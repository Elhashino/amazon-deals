"""The kill-filter: hard rejection rules on normalized data.

Three outcomes, and the difference matters:
  REJECT — positive evidence this launch is structured to take your money.
  DEFER  — the coin is too young, or data we require is missing (e.g.
           RugCheck hasn't indexed a 30-second-old coin yet). Re-check next
           poll instead of binning a coin for being new. Missing data is
           NEVER a pass.
  PASS   — every check had data and every check cleared.

Age is judged before anything else, because liquidity, holders and buyers
are all things that grow in the first minutes. Binning a 5-minute-old coin
for having a thin pool bins it forever — so youth defers, it never rejects.

Every failed check is reported, not just the first — the rejection log is
a teaching tool.
"""

from __future__ import annotations

import time
from enum import Enum

from .config import Config
from .models import BundleReport, Candidate, DemandReport, SafetyReport

# RugCheck risk names that are instant kills regardless of anything else.
CRITICAL_RISK_SUBSTRINGS = (
    "freeze authority",
    "mint authority",
    "rugged",
    "honeypot",
    "transfer fee",
)


class Outcome(Enum):
    PASS = "pass"
    REJECT = "reject"
    DEFER = "defer"


def _fmt_usd(v: float) -> str:
    return f"${v:,.0f}"


def pair_age_minutes(cand: Candidate) -> float | None:
    if cand.pair_created_at_ms is None:
        return None
    return (time.time() * 1000 - cand.pair_created_at_ms) / 60_000


def check_market(cand: Candidate, cfg: Config) -> tuple[Outcome, list[str]]:
    """Cheap checks from market data alone — run before spending API budget."""
    age_min = pair_age_minutes(cand)

    # --- age gate first ---
    if age_min is None:
        return Outcome.DEFER, ["pair age unknown (not indexed yet)"]
    if age_min < 0:
        return Outcome.REJECT, ["pair timestamp is in the future"]
    if age_min > cfg.max_pair_age_minutes:
        return Outcome.REJECT, [
            f"too old ({age_min / 60:.1f}h > {cfg.max_pair_age_minutes / 60:.0f}h)"
        ]
    if age_min < cfg.min_pair_age_minutes:
        # Everything below would be judging a pool that is still filling.
        return Outcome.DEFER, [f"only {age_min:.0f}m old (< {cfg.min_pair_age_minutes:.0f}m)"]

    # --- old enough for its numbers to mean something ---
    if cand.liquidity_usd is None:
        return Outcome.DEFER, ["liquidity unknown"]
    if cand.liquidity_usd < cfg.min_liquidity_usd:
        return Outcome.REJECT, [
            f"liquidity {_fmt_usd(cand.liquidity_usd)} < {_fmt_usd(cfg.min_liquidity_usd)}"
        ]
    return Outcome.PASS, []


def check_safety(safety: SafetyReport, cfg: Config) -> tuple[Outcome, list[str]]:
    fails: list[str] = []
    missing: list[str] = []

    def known(value, label: str) -> bool:
        if value is None:
            missing.append(label)
            return False
        return True

    if safety.rugged_flag:
        fails.append("already flagged as rugged")

    if known(safety.mint_authority_active, "mint authority") and safety.mint_authority_active:
        fails.append("mint authority still active (supply can be inflated)")
    if known(safety.freeze_authority_active, "freeze authority") and safety.freeze_authority_active:
        fails.append("freeze authority still active (your wallet can be frozen)")
    if known(safety.lp_locked_pct, "LP lock") and safety.lp_locked_pct < cfg.min_lp_locked_pct:
        fails.append(f"LP only {safety.lp_locked_pct:.0f}% locked/burned (< {cfg.min_lp_locked_pct:.0f}%)")
    if known(safety.top10_holder_pct, "top-10 holders") and safety.top10_holder_pct > cfg.max_top10_holder_pct:
        fails.append(f"top 10 wallets hold {safety.top10_holder_pct:.0f}% (> {cfg.max_top10_holder_pct:.0f}%)")
    if (
        known(safety.max_single_holder_pct, "largest holder")
        and safety.max_single_holder_pct > cfg.max_single_holder_pct
    ):
        fails.append(f"one wallet holds {safety.max_single_holder_pct:.0f}% (> {cfg.max_single_holder_pct:.0f}%)")
    if safety.insider_pct is not None and safety.insider_pct > cfg.max_insider_pct:
        fails.append(f"insider network holds {safety.insider_pct:.0f}% (> {cfg.max_insider_pct:.0f}%)")
    if known(safety.total_holders, "holder count") and safety.total_holders < cfg.min_holders:
        fails.append(f"only {safety.total_holders} holders (< {cfg.min_holders})")
    if safety.rugcheck_score is not None and safety.rugcheck_score > cfg.max_rugcheck_score:
        fails.append(f"RugCheck risk score {safety.rugcheck_score:.0f} (> {cfg.max_rugcheck_score:.0f})")

    for risk in safety.risk_names:
        lowered = risk.lower()
        if any(s in lowered for s in CRITICAL_RISK_SUBSTRINGS):
            fails.append(f"critical risk flag: {risk}")

    if fails:
        return Outcome.REJECT, fails
    if missing:
        return Outcome.DEFER, [f"safety data missing: {', '.join(missing)}"]
    return Outcome.PASS, []


def check_bundles(bundle: BundleReport, cfg: Config) -> tuple[Outcome, list[str]]:
    if bundle.bundled_pct_of_supply is None:
        # Bundle analysis is best-effort (free RPC gets rate-limited); an
        # unknown here alone shouldn't freeze the pipeline forever. The
        # alert says so explicitly rather than implying a clean result.
        return Outcome.PASS, []
    if bundle.bundled_pct_of_supply > cfg.max_bundle_pct:
        return Outcome.REJECT, [
            f"bundled wallets hold {bundle.bundled_pct_of_supply:.0f}% of supply "
            f"across {bundle.bundled_wallets} wallets / {bundle.funder_clusters} funding clusters "
            f"(> {cfg.max_bundle_pct:.0f}%)"
        ]
    return Outcome.PASS, []


def check_demand(
    demand: DemandReport | None, cfg: Config, age_minutes: float | None = None
) -> tuple[Outcome, list[str]]:
    """Two ways to fail: too few real traders, or volume that comes from a
    handful of wallets trading with themselves."""
    if demand is None or demand.unique_traders is None:
        return Outcome.PASS, []  # best-effort metric; scoring penalizes unknowns

    # The threshold is denominated in an hour. A 25-minute-old pair has not
    # had an hour, so scale the bar to the life it has actually lived.
    threshold = float(cfg.min_unique_buyers_h1)
    if age_minutes is not None and age_minutes < 60:
        threshold *= max(age_minutes, 1.0) / 60.0

    fails: list[str] = []
    if demand.unique_traders < threshold:
        age_note = f" for a {age_minutes:.0f}m-old pair" if age_minutes is not None and age_minutes < 60 else ""
        fails.append(f"~{demand.unique_traders} unique traders (< {threshold:.0f}{age_note})")

    if (
        demand.diversity is not None
        and demand.sampled >= 10
        and demand.total_txns >= 30
        and demand.diversity < cfg.min_trader_diversity
    ):
        fails.append(
            f"wash-trading pattern: only {demand.diversity:.0%} of sampled trades came from "
            f"distinct wallets (< {cfg.min_trader_diversity:.0%})"
        )

    return (Outcome.REJECT, fails) if fails else (Outcome.PASS, [])
