"""The kill-filter: hard rejection rules on normalized data.

Three outcomes, and the difference matters:
  REJECT — positive evidence this launch is structured to take your money.
  DEFER  — data we require is missing (e.g. RugCheck hasn't indexed a
           30-second-old coin yet). Re-check next poll instead of binning
           a coin for being young. Missing data is NEVER a pass.
  PASS   — every check had data and every check cleared.

Every failed check is reported, not just the first — the rejection log is
a teaching tool.
"""

from __future__ import annotations

import time
from enum import Enum

from .config import Config
from .models import BundleReport, Candidate, SafetyReport

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


def check_market(cand: Candidate, cfg: Config) -> tuple[Outcome, list[str]]:
    """Cheap checks from market data alone — run before spending API budget."""
    fails: list[str] = []
    defer = False

    if cand.liquidity_usd is None:
        defer = True
    elif cand.liquidity_usd < cfg.min_liquidity_usd:
        fails.append(f"liquidity {_fmt_usd(cand.liquidity_usd)} < {_fmt_usd(cfg.min_liquidity_usd)}")

    if cand.pair_created_at_ms is None:
        defer = True
    else:
        age_min = (time.time() * 1000 - cand.pair_created_at_ms) / 60_000
        if age_min < 0:
            fails.append("pair timestamp in the future")
        elif age_min > cfg.max_pair_age_minutes:
            fails.append(f"too old ({age_min / 60:.1f}h > {cfg.max_pair_age_minutes / 60:.0f}h)")
        elif age_min < cfg.min_pair_age_minutes:
            # Too young is a DEFER, not a reject — it'll grow up in minutes.
            defer = True

    if fails:
        return Outcome.REJECT, fails
    if defer:
        return Outcome.DEFER, ["market data incomplete or pair too young"]
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
        # unknown here alone shouldn't freeze the pipeline forever.
        return Outcome.PASS, []
    if bundle.bundled_pct_of_supply > cfg.max_bundle_pct:
        return Outcome.REJECT, [
            f"bundled wallets hold {bundle.bundled_pct_of_supply:.0f}% of supply "
            f"across {bundle.bundled_wallets} wallets / {bundle.funder_clusters} funding clusters "
            f"(> {cfg.max_bundle_pct:.0f}%)"
        ]
    return Outcome.PASS, []


def check_demand(unique_buyers_h1: int | None, cfg: Config) -> tuple[Outcome, list[str]]:
    if unique_buyers_h1 is None:
        return Outcome.PASS, []  # best-effort metric; scoring penalizes unknowns
    if unique_buyers_h1 < cfg.min_unique_buyers_h1:
        return Outcome.REJECT, [f"only {unique_buyers_h1} unique buyers in 1h (< {cfg.min_unique_buyers_h1})"]
    return Outcome.PASS, []
