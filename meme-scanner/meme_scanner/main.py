"""The scan loop: discover -> kill-filter -> score -> alert -> rug-watch.

Cheap checks run before expensive ones so the API budget goes on the few
coins that deserve it. Every rejection is logged with its reason — the
rejection log is the scanner's classroom.
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback

from . import __version__
from .alerts import format_alert, format_rug_warning
from .analysis import bundles
from .apis import dexscreener, rugcheck, solana_rpc
from .config import Config
from .filters import Outcome, check_bundles, check_demand, check_market, check_safety
from .models import BundleReport
from .rejection_log import RejectionLog
from .scoring import score
from .state import State
from .telegram import Alerter


def process_mint(mint: str, boosted: bool, cfg: Config, state: State, log: RejectionLog, alerter: Alerter) -> None:
    """Run one token through the full pipeline. DEFER = stay quiet, retry next poll."""
    cand = dexscreener.get_candidates([mint]).get(mint)
    if cand is None:
        return  # not on DexScreener yet; it'll come around again
    cand.boosted = cand.boosted or boosted
    sym = cand.symbol

    # Stage 1: market hard-filter (free — already have the data).
    outcome, reasons = check_market(cand, cfg)
    if outcome is Outcome.REJECT:
        state.mark_seen(mint)
        log.reject(mint, sym, "market", "; ".join(reasons))
        return
    if outcome is Outcome.DEFER:
        return

    # Stage 2: contract & holder safety (RugCheck, ~1 req/sec budget).
    safety = rugcheck.get_safety(mint)
    if safety is None:
        return  # not indexed yet — defer, don't bin
    if safety.mint_authority_active is None or safety.freeze_authority_active is None:
        # RugCheck didn't say — read the ground truth off-chain data can't fake.
        try:
            mint_active, freeze_active = solana_rpc.get_mint_authorities(mint, cfg)
            if safety.mint_authority_active is None:
                safety.mint_authority_active = mint_active
            if safety.freeze_authority_active is None:
                safety.freeze_authority_active = freeze_active
        except Exception as exc:
            print(f"  [WARN] RPC authority check failed for {sym}: {exc}")
    outcome, reasons = check_safety(safety, cfg)
    if outcome is Outcome.REJECT:
        state.mark_seen(mint)
        log.reject(mint, sym, "safety", "; ".join(reasons))
        return
    if outcome is Outcome.DEFER:
        return

    # Stage 3: bundled-wallet analysis (Solana RPC, most expensive — last).
    try:
        bundle = bundles.analyze(mint, cfg)
    except Exception as exc:  # best-effort: RPC hiccups never kill a candidate
        print(f"  [WARN] bundle analysis failed for {sym}: {exc}")
        bundle = BundleReport(note=f"analysis failed: {exc}")
    outcome, reasons = check_bundles(bundle, cfg)
    if outcome is Outcome.REJECT:
        state.mark_seen(mint)
        log.reject(mint, sym, "bundles", "; ".join(reasons))
        return

    # Stage 4: unique-buyer demand.
    try:
        unique_buyers = solana_rpc.unique_buyers_h1(cand.pair_address, cfg)
    except Exception as exc:
        print(f"  [WARN] unique-buyer count failed for {sym}: {exc}")
        unique_buyers = None
    outcome, reasons = check_demand(unique_buyers, cfg)
    if outcome is Outcome.REJECT:
        state.mark_seen(mint)
        log.reject(mint, sym, "demand", "; ".join(reasons))
        return

    # Survivor: score it.
    state.mark_seen(mint)
    verdict = score(cand, safety, unique_buyers)
    if verdict.score < cfg.min_alert_score:
        log.reject(mint, sym, "score", f"momentum score {verdict.score:.0f} < {cfg.min_alert_score:.0f}",
                   str(verdict.components))
        return

    print(f"  [SURVIVOR] {sym} scored {verdict.score:.0f} — alerting")
    if alerter.send(format_alert(cand, safety, bundle, unique_buyers, verdict, time.time() * 1000)):
        state.mark_alerted(mint)
        if cand.liquidity_usd:
            state.watch(mint, {"liquidity_usd": cand.liquidity_usd, "symbol": sym})


def check_watched(cfg: Config, state: State, alerter: Alerter) -> None:
    """Re-check alerted coins; warn once if liquidity is being pulled."""
    for mint, info in state.watched().items():
        if time.time() - info.get("since", 0) > cfg.watch_hours * 3600:
            state.unwatch(mint)
            continue
        cand = dexscreener.get_candidate(mint)
        if cand is None or cand.liquidity_usd is None:
            continue
        liq_at_alert = info.get("liquidity_usd") or 0
        threshold = liq_at_alert * (1 - cfg.rug_liquidity_drop_pct / 100.0)
        if liq_at_alert and cand.liquidity_usd < threshold:
            alerter.send(format_rug_warning(cand, liq_at_alert, cand.liquidity_usd))
            state.unwatch(mint)  # one warning, not spam


def run_cycle(cfg: Config, state: State, log: RejectionLog, alerter: Alerter) -> int:
    """One poll cycle. Returns number of new mints evaluated."""
    discovered = dexscreener.discover_mints()
    for mint in rugcheck.new_token_mints():
        discovered.setdefault(mint, False)

    fresh = {m: b for m, b in discovered.items() if not state.is_seen(m) and not state.is_alerted(m)}
    print(f"[cycle] {len(discovered)} discovered, {len(fresh)} new to evaluate")

    for mint, boosted in fresh.items():
        try:
            process_mint(mint, boosted, cfg, state, log, alerter)
        except Exception:
            print(f"  [ERROR] pipeline crashed on {mint[:8]} — skipping this cycle")
            traceback.print_exc()

    check_watched(cfg, state, alerter)
    state.prune_and_save()
    return len(fresh)


def main() -> None:
    # Windows: with output redirected (e.g. Task Scheduler -> log file) stdout
    # falls back to the legacy codepage and emoji in alerts would crash us.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Solana meme-coin launch scanner with a kill-filter")
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args()

    cfg = Config.load()
    state = State(cfg.data_dir)
    log = RejectionLog(cfg.data_dir)
    alerter = Alerter(cfg.telegram_bot_token, cfg.telegram_chat_id)

    print(f"meme-scanner v{__version__} | poll every {cfg.poll_seconds}s | "
          f"telegram {'ON' if alerter.enabled else 'OFF (console mode)'}")
    print(f"filters: liq>=${cfg.min_liquidity_usd:,.0f} lp_lock>={cfg.min_lp_locked_pct:.0f}% "
          f"top10<={cfg.max_top10_holder_pct:.0f}% bundle<={cfg.max_bundle_pct:.0f}% "
          f"holders>={cfg.min_holders} buyers>={cfg.min_unique_buyers_h1}")

    while True:
        started = time.monotonic()
        try:
            run_cycle(cfg, state, log, alerter)
        except Exception:
            print("[ERROR] cycle failed, will retry next poll")
            traceback.print_exc()
        if args.once:
            break
        elapsed = time.monotonic() - started
        time.sleep(max(5.0, cfg.poll_seconds - elapsed))


if __name__ == "__main__":
    main()
