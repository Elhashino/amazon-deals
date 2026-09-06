"""The scan loop: discover -> kill-filter -> score -> alert -> rug-watch.

Cheap checks run before expensive ones so the API budget goes on the few
coins that deserve it. Every rejection is logged with its reason — the
rejection log is the scanner's classroom.

Two ordering rules earn their keep on an unattended box:
  - a coin is only marked "seen" once we are done with it for good; a
    transient failure (Telegram down, RugCheck not indexed) leaves it
    eligible for the next cycle rather than binning it silently;
  - state is saved as soon as an alert lands, not at the end of the
    cycle, so a crash can't replay yesterday's alerts on restart.
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback

from . import __version__
from .alerts import format_alert, format_rug_warning
from .analysis import bundles
from .apis import dexscreener, pumpportal, rugcheck, solana_rpc
from .config import Config
from .filters import (
    Outcome,
    check_bundles,
    check_demand,
    check_market,
    check_safety,
    pair_age_minutes,
)
from .models import BundleReport, Candidate, DemandReport
from .rejection_log import RejectionLog
from .scoring import score
from .state import State
from .telegram import Alerter

# Bound the per-cycle RPC spend. Deferred coins come back next cycle, and
# a truncated cycle says so out loud rather than quietly covering less.
#
# The first gate reads batched DexScreener data — one request per 30 mints —
# and rejects or defers the large majority, so raising this mostly buys cheap
# coverage of the pending queue. The paid stages behind it are paced by the
# RPC throttle rather than by this number, which puts a hard ceiling on spend
# however high this goes. Coins graduate off the bonding curve hours after
# launch, so a queue that takes longer to walk than that loses real
# candidates to the pending TTL rather than to any filter.
MAX_EVALUATIONS_PER_CYCLE = 120

# How long a --once shakedown run waits for the live feed to receive its
# first launches. Only used by --once; the continuous loop fills the feed
# between cycles for free.
FEED_WARMUP_SECONDS = 20.0


def process_mint(
    cand: Candidate, cfg: Config, state: State, log: RejectionLog, alerter: Alerter
) -> None:
    """Run one candidate through the full pipeline.

    DEFER at any stage = stay quiet and leave it unseen, so the next cycle
    tries again. Only REJECT and a delivered alert mark a coin as seen.
    """
    mint, sym = cand.mint, cand.symbol

    # Stage 1: market hard-filter (free — already have the data).
    outcome, reasons = check_market(cand, cfg)
    if outcome is Outcome.REJECT:
        state.mark_seen(mint)
        log.reject(mint, sym, "market", "; ".join(reasons))
        return
    if outcome is Outcome.DEFER:
        return
    age_min = pair_age_minutes(cand)

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

    # Stage 4: trader diversity — real demand vs bots trading with themselves.
    try:
        demand = solana_rpc.unique_traders_h1(cand.pair_address, cfg)
    except Exception as exc:
        print(f"  [WARN] trader count failed for {sym}: {exc}")
        demand = DemandReport()
    outcome, reasons = check_demand(demand, cfg, age_min)
    if outcome is Outcome.REJECT:
        state.mark_seen(mint)
        log.reject(mint, sym, "demand", "; ".join(reasons))
        return

    # Survivor: score it.
    verdict = score(cand, safety, demand.unique_traders)
    if verdict.score < cfg.min_alert_score:
        state.mark_seen(mint)
        log.reject(mint, sym, "score", f"momentum score {verdict.score:.0f} < {cfg.min_alert_score:.0f}",
                   str(verdict.components))
        return

    print(f"  [SURVIVOR] {sym} scored {verdict.score:.0f} — alerting")
    delivered = alerter.send(
        format_alert(cand, safety, bundle, demand, verdict, time.time() * 1000)
    )
    if not delivered:
        # Telegram is down or refused the message. Leave the mint unseen so
        # the next cycle retries: a dropped alert is the one failure this
        # tool cannot afford to be quiet about.
        print(f"  [WARN] alert for {sym} not delivered — will retry next cycle")
        return

    state.mark_seen(mint)
    state.mark_alerted(mint)
    if cand.liquidity_usd:
        state.watch(mint, {"liquidity_usd": cand.liquidity_usd, "symbol": sym})
    # Persist immediately: a crash after this point must not replay the alert.
    state.prune_and_save()


def check_watched(cfg: Config, state: State, alerter: Alerter) -> None:
    """Re-check alerted coins; warn once if liquidity is being pulled."""
    for mint, info in state.watched().items():
        if time.time() - info.get("since", 0) > cfg.watch_hours * 3600:
            state.unwatch(mint)
            continue
        try:
            cand = dexscreener.get_candidate(mint)
        except Exception as exc:
            print(f"  [WARN] rug-watch lookup failed for {mint[:8]}: {exc}")
            continue
        liq_at_alert = info.get("liquidity_usd") or 0
        if cand is None or cand.liquidity_usd is None:
            # The pair vanishing from the market feed is itself a red flag,
            # so say so rather than treating silence as "still fine".
            if cand is None:
                alerter.send(
                    f"⚠️ <b>{info.get('symbol') or mint[:8]}</b> has dropped off DexScreener "
                    f"since the alert (pool pulled, or delisted). Treat as gone."
                )
                state.unwatch(mint)
            continue
        threshold = liq_at_alert * (1 - cfg.rug_liquidity_drop_pct / 100.0)
        if liq_at_alert and cand.liquidity_usd < threshold:
            alerter.send(format_rug_warning(cand, liq_at_alert, cand.liquidity_usd))
            state.unwatch(mint)  # one warning, not spam


def run_cycle(cfg: Config, state: State, log: RejectionLog, alerter: Alerter,
              feed: pumpportal.LaunchFeed | None = None) -> int:
    """One poll cycle. Returns number of new mints evaluated."""
    try:
        # Live launches arrive seconds after creation — far too young to
        # judge — so they queue up and get promoted once they've aged into
        # the window where their numbers mean something.
        new_live = 0
        if feed is not None:
            now = time.time()
            for mint in feed.drain():
                if state.add_pending(mint, now):
                    new_live += 1

        discovered = dexscreener.discover_mints()
        for mint in rugcheck.new_token_mints():
            discovered.setdefault(mint, False)

        polled = [m for m in discovered if not state.is_seen(m) and not state.is_alerted(m)]
        # Ripened live launches go first: they are the freshest real leads.
        # Keep them in the order ripe_pending sorted them — oldest first.
        # Inserting each at the head instead would reverse that into newest
        # first, and once the queue is deeper than one cycle's cap, a coin
        # would then be looked at only in the minutes right after it ripens,
        # which is exactly when DexScreener has a pair for it but no
        # liquidity figure yet. It defers, newer arrivals push it down, and
        # nothing ever re-checks it once its data fills in. Draining oldest
        # first means every pending coin comes back around.
        ripe = state.ripe_pending(cfg.min_pair_age_minutes * 60)
        already = set(polled)
        ripe_first = []
        for mint in ripe:
            if mint not in already:
                ripe_first.append(mint)
                discovered.setdefault(mint, False)
        fresh = ripe_first + polled

        # Split the budget between the two discovery paths instead of letting
        # either take all of it. Most pump.fun launches never leave the bonding
        # curve: DexScreener lists them with no liquidity figure at all, so they
        # defer every cycle and pile up until they expire. That queue reaches
        # many times the cycle cap within an hour, and taking from it first
        # leaves nothing for the DexScreener and RugCheck polling feeds — the
        # path that catches launches from everywhere else. Each side gets a
        # reserved share, and whatever one side doesn't use goes to the other.
        reserve = MAX_EVALUATIONS_PER_CYCLE // 2
        take_polled = polled[:MAX_EVALUATIONS_PER_CYCLE - min(reserve, len(ripe_first))]
        take_ripe = ripe_first[:MAX_EVALUATIONS_PER_CYCLE - len(take_polled)]
        evaluating = take_ripe + take_polled
        skipped = len(fresh) - len(evaluating)
        live_note = ""
        if feed is not None:
            live_note = (f", live feed {feed.status}: +{new_live} new, "
                         f"{state.pending_count()} ripening, {len(ripe)} ready")
        print(f"[cycle] {len(discovered)} discovered, {len(fresh)} to check{live_note}"
              + (f" ({skipped} held over — per-cycle cap)" if skipped else ""))

        # One batched enrichment call per 30 mints, not one call per mint.
        candidates = dexscreener.get_candidates(evaluating)
        for mint in evaluating:
            cand = candidates.get(mint)
            if cand is None:
                continue  # not on DexScreener yet; it'll come around again
            cand.boosted = cand.boosted or discovered.get(mint, False)
            try:
                process_mint(cand, cfg, state, log, alerter)
            except Exception:
                print(f"  [ERROR] pipeline crashed on {mint[:8]} — skipping this cycle")
                traceback.print_exc()

        try:
            check_watched(cfg, state, alerter)
        except Exception:
            print("  [ERROR] rug-watch pass failed")
            traceback.print_exc()
        return len(evaluating)
    finally:
        # Whatever happened above, what we learned this cycle gets written.
        try:
            state.prune_and_save()
        except OSError as exc:
            print(f"  [WARN] could not save state: {exc}")


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

    feed = None
    if cfg.enable_pumpportal:
        feed = pumpportal.LaunchFeed(cfg.pumpportal_url)
        if not feed.start():
            feed = None  # library missing — polling feeds still work

    print(f"meme-scanner v{__version__} | poll every {cfg.poll_seconds}s | "
          f"telegram {'ON' if alerter.enabled else 'OFF (console mode)'} | "
          f"live feed {'ON' if feed else 'OFF'}")
    print(f"filters: liq>=${cfg.min_liquidity_usd:,.0f} lp_lock>={cfg.min_lp_locked_pct:.0f}% "
          f"top10<={cfg.max_top10_holder_pct:.0f}% bundle<={cfg.max_bundle_pct:.0f}% "
          f"holders>={cfg.min_holders} traders>={cfg.min_unique_buyers_h1}")

    # A single shakedown cycle would otherwise run before the websocket has
    # had a chance to receive anything, always reporting "0 launches seen" and
    # never exercising the live-feed path. Give it a moment so --once actually
    # tests what it is meant to test. The continuous loop needs no such pause:
    # the feed fills between cycles.
    if args.once and feed is not None:
        print(f"[once] letting the live feed fill for {FEED_WARMUP_SECONDS:.0f}s before the cycle...")
        time.sleep(FEED_WARMUP_SECONDS)

    # Ctrl+C is the documented way to stop a manually-started scanner, so it is
    # an ordinary exit, not a fault. Report it as one — a traceback here reads
    # as a crash and invites the reader to go looking for a bug that isn't
    # there. Whatever the cycle learned is already on disk: run_cycle saves
    # state in a finally block before the interrupt reaches this far.
    try:
        while True:
            started = time.monotonic()
            try:
                run_cycle(cfg, state, log, alerter, feed)
            except Exception:
                print("[ERROR] cycle failed, will retry next poll")
                traceback.print_exc()
            if args.once:
                break
            elapsed = time.monotonic() - started
            time.sleep(max(5.0, cfg.poll_seconds - elapsed))
    except KeyboardInterrupt:
        print("\n[stopped] scanner shut down — state and rejection log saved.")
    finally:
        if feed is not None:
            feed.stop()


if __name__ == "__main__":
    main()
