"""Follow already-alerted coins and record what happens to them.

The scanner only snapshots a coin while the scanner itself is running, so
closing the window ends the record. That is the wrong half of the timeline
to lose: alerted coins have consistently looked healthy for the first hour
and then collapsed some hours later, so a session's worth of snapshots
captures the part that never varies and misses the part that decides the
outcome.

This follows them independently. It reads data/alerts.csv, asks DexScreener
where each coin stands now, and appends to data/outcomes.csv — the same file
the scanner writes, so the two interleave harmlessly and the analysis sees
one series per coin.

It costs nothing to run. DexScreener needs no API key and this makes no
Solana RPC calls at all, so it cannot touch the Helius credit budget however
long it runs. One request covers 30 coins. Leave it running; run the scanner
when you feel like it.

    python track.py                 # follow coins alerted in the last 24h
    python track.py --hours 48      # widen the window
    python track.py --every 120     # check every 2 minutes instead of 5
    python track.py --once          # single pass, then exit
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import time
from pathlib import Path

from meme_scanner.apis import dexscreener
from meme_scanner.config import Config
from meme_scanner.outcomes import OutcomeLog

BATCH = 30  # DexScreener resolves this many mints per request


def _num(value: str | None) -> float | None:
    """Parse a CSV cell written by the outcome log.

    Values are quoted with a leading apostrophe when they would otherwise
    look like a spreadsheet formula, so a negative number arrives as '-18.47.
    """
    if value in (None, "", "None"):
        return None
    try:
        return float(str(value).lstrip("'"))
    except ValueError:
        return None


def load_alerts(data_dir: str, hours: float) -> list[dict]:
    """Alerts raised within the window, newest first, one row per coin."""
    path = Path(data_dir) / "alerts.csv"
    if not path.is_file():
        return []
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    out: dict[str, dict] = {}
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                when = dt.datetime.strptime(row["utc_time"], "%Y-%m-%d %H:%M:%S")
            except (ValueError, KeyError, TypeError):
                continue  # a torn row must not stop the pass
            when = when.replace(tzinfo=dt.timezone.utc)
            if when < cutoff:
                continue
            mint = row.get("mint") or ""
            if not mint:
                continue
            # Keep the first alert for a mint: that is the decision point the
            # outcome is measured against.
            out.setdefault(mint, {
                "mint": mint,
                "symbol": (row.get("symbol") or "").lstrip("'"),
                "at": when,
                "liq": _num(row.get("liquidity_usd")),
                "price": _num(row.get("price_usd")),
            })
    return sorted(out.values(), key=lambda r: r["at"], reverse=True)


def pass_once(alerts: list[dict], outcomes: OutcomeLog) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    found: dict = {}
    for i in range(0, len(alerts), BATCH):
        chunk = [a["mint"] for a in alerts[i:i + BATCH]]
        try:
            found.update(dexscreener.get_candidates(chunk))
        except Exception as exc:
            print(f"  [WARN] lookup failed for a batch of {len(chunk)}: {exc}")

    print(f"\n[{now.strftime('%H:%M:%S')}] following {len(alerts)} coins")
    print(f"  {'coin':16}{'age':>7}{'liq now':>12}{'liq %':>8}{'price %':>9}")
    for a in alerts:
        cand = found.get(a["mint"])
        mins = (now - a["at"]).total_seconds() / 60.0
        outcomes.snapshot(a["mint"], a["symbol"], mins, cand, a["liq"], a["price"])
        age = f"{mins/60:.1f}h" if mins >= 60 else f"{mins:.0f}m"
        if cand is None or cand.liquidity_usd is None:
            print(f"  {a['symbol']:16}{age:>7}{'GONE':>12}{'':>8}{'':>9}")
            continue
        lp = 100.0 * cand.liquidity_usd / a["liq"] if a["liq"] else None
        pp = 100.0 * cand.price_usd / a["price"] if a["price"] and cand.price_usd else None
        mark = "  <-- collapsed" if lp is not None and lp <= 40 else ""
        print(f"  {a['symbol']:16}{age:>7}${cand.liquidity_usd:>11,.0f}"
              f"{(f'{lp:.0f}%' if lp is not None else '-'):>8}"
              f"{(f'{pp:.0f}%' if pp is not None else '-'):>9}{mark}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Follow alerted coins and log what happens to them")
    ap.add_argument("--hours", type=float, default=24.0, help="how far back to follow alerts (default 24)")
    ap.add_argument("--every", type=float, default=300.0, help="seconds between passes (default 300)")
    ap.add_argument("--once", action="store_true", help="single pass, then exit")
    args = ap.parse_args()

    cfg = Config.load()
    outcomes = OutcomeLog(cfg.data_dir)
    print(f"outcome tracker | following alerts from the last {args.hours:.0f}h "
          f"| every {args.every:.0f}s | free (DexScreener only, no RPC, no Helius credits)")

    try:
        while True:
            alerts = load_alerts(cfg.data_dir, args.hours)
            if not alerts:
                print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] no alerts in the last "
                      f"{args.hours:.0f}h yet — run the scanner to produce some")
            else:
                pass_once(alerts, outcomes)
            if args.once:
                break
            time.sleep(args.every)
    except KeyboardInterrupt:
        print("\n[stopped] tracker shut down — outcomes.csv saved.")


if __name__ == "__main__":
    main()
