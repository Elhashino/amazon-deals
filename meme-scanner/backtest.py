"""Did the alerts ever contain a tradeable edge? Answer it from the log.

Every alerted coin so far went up before it went down, so the only version of
this that could make money is exiting before the drop. That is a precise
question rather than a matter of opinion: buy at the alert, sell a fixed
number of minutes later, and see what each holding period returns across
every coin ever alerted.

This reads data/alerts.csv (the decision point) and data/outcomes.csv (what
happened afterwards), joins them on mint, and reports each rule's return, win
rate and worst case. It also reports the peak each coin reached and when —
the ceiling a perfect exit would have caught, which is the number to compare
any rule against.

Three things it does deliberately, because each is a way these analyses lie:

  Costs are charged. A round trip on a thin pool is not free, and a strategy
  that only works at zero cost does not work. --cost sets the assumption.

  Coverage is printed per horizon. A "+60 min" rule tested on the three coins
  that happened to be tracked that long is not evidence, and the output says
  so rather than quietly averaging whatever it has.

  Unfinished coins are marked. A coin still being tracked has not had its
  outcome yet, and its current price is not its final one.

    python backtest.py                 # every alert, default cost assumption
    python backtest.py --cost 0        # gross, before fees and slippage
    python backtest.py --hours 12      # only coins tracked at least 12h
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path

from meme_scanner.config import Config

HORIZONS = [1, 2, 5, 10, 15, 30, 45, 60, 120, 240, 480, 720]
DEFAULT_COST_PCT = 5.0  # round trip: swap fees both ways plus slippage on a thin pool


def _num(value) -> float | None:
    """Parse a cell from the outcome logs, which quote formula-looking values."""
    if value in (None, "", "None"):
        return None
    try:
        return float(str(value).lstrip("'"))
    except ValueError:
        return None


def _time(value: str) -> dt.datetime | None:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
    except (ValueError, TypeError):
        return None


def load(data_dir: str) -> dict:
    """Join alerts to their outcome snapshots. Returns {mint: {...}}."""
    alerts_path = Path(data_dir) / "alerts.csv"
    out_path = Path(data_dir) / "outcomes.csv"
    if not alerts_path.is_file():
        return {}

    coins: dict[str, dict] = {}
    with alerts_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            mint = row.get("mint") or ""
            when = _time(row.get("utc_time", ""))
            price = _num(row.get("price_usd"))
            if not mint or when is None or not price:
                continue
            # First alert per mint: that is the decision being measured.
            coins.setdefault(mint, {
                "symbol": (row.get("symbol") or "").lstrip("'"),
                "at": when, "price": price,
                "liq": _num(row.get("liquidity_usd")),
                "score": _num(row.get("score")),
                "boosted": (row.get("boosted") or "").strip() == "True",
                "points": [],
            })

    if out_path.is_file():
        with out_path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                coin = coins.get(row.get("mint") or "")
                if coin is None:
                    continue
                mins, price = _num(row.get("minutes_since_alert")), _num(row.get("price_usd"))
                if mins is None or not price:
                    continue
                coin["points"].append((mins, price))

    for coin in coins.values():
        coin["points"].sort()
        # Return in percent at each observed minute.
        coin["curve"] = [(m, 100.0 * (p - coin["price"]) / coin["price"]) for m, p in coin["points"]]
        coin["tracked_min"] = coin["points"][-1][0] if coin["points"] else 0.0
    return coins


def at_horizon(coin: dict, minutes: float) -> float | None:
    """Return at `minutes`, using the last observation at or before it.

    Nothing is interpolated forward: a coin whose tracking stopped at 14
    minutes has no 60-minute outcome, and inventing one by carrying its last
    price forward would score a collapse that happened off-camera as a hold.
    """
    best = None
    for m, ret in coin["curve"]:
        if m <= minutes:
            best = ret
        else:
            break
    if best is None:
        return None
    # Require the coin to have actually been watched to roughly that age.
    if coin["tracked_min"] < minutes * 0.9:
        return None
    return best


def main() -> None:
    ap = argparse.ArgumentParser(description="Test whether any holding period beat the alerts")
    ap.add_argument("--cost", type=float, default=DEFAULT_COST_PCT,
                    help=f"round-trip cost %% charged to every trade (default {DEFAULT_COST_PCT})")
    ap.add_argument("--hours", type=float, default=0.0,
                    help="only include coins tracked at least this long")
    ap.add_argument("--stake", type=float, default=100.0, help="stake per alert (default 100)")
    args = ap.parse_args()

    cfg = Config.load()
    coins = load(cfg.data_dir)
    if args.hours:
        coins = {k: v for k, v in coins.items() if v["tracked_min"] >= args.hours * 60}
    if not coins:
        print("No alerts logged yet. Run the scanner to produce some, and leave track.py running.")
        return

    print("=" * 74)
    print("  DID ANY EXIT RULE WORK?")
    print(f"  {len(coins)} alerts | £{args.stake:.0f} each | {args.cost:.1f}% round-trip cost charged")
    print("=" * 74)

    print(f"\n{'coin':16}{'tracked':>9}{'peak':>9}{'at':>7}{'final':>9}")
    for c in sorted(coins.values(), key=lambda c: c["at"]):
        if not c["curve"]:
            print(f"{c['symbol']:16}{'no data':>9}")
            continue
        peak_m, peak = max(c["curve"], key=lambda p: p[1])
        final = c["curve"][-1][1]
        tracked = f"{c['tracked_min']/60:.1f}h" if c["tracked_min"] >= 60 else f"{c['tracked_min']:.0f}m"
        print(f"{c['symbol']:16}{tracked:>9}{peak:>+8.0f}%{peak_m:>6.0f}m{final:>+8.0f}%")

    print(f"\n{'exit at':>10}{'coins':>7}{'return':>10}{'wins':>8}{'best':>9}{'worst':>9}")
    print("-" * 74)
    best_rule = None
    for h in HORIZONS:
        rets = [r for c in coins.values() if (r := at_horizon(c, h)) is not None]
        if not rets:
            continue
        net = [r - args.cost for r in rets]
        avg = sum(net) / len(net)
        wins = sum(1 for r in net if r > 0)
        label = f"+{h}m" if h < 60 else f"+{h//60}h"
        flag = "  <-- too few" if len(rets) < 10 else ""
        print(f"{label:>10}{len(rets):>7}{avg:>+9.0f}%{wins:>4}/{len(rets):<3}"
              f"{max(net):>+8.0f}%{min(net):>+8.0f}%{flag}")
        if len(rets) >= 10 and (best_rule is None or avg > best_rule[1]):
            best_rule = (label, avg, len(rets))

    print("-" * 74)
    finals = [c["curve"][-1][1] - args.cost for c in coins.values() if c["curve"]]
    if finals:
        avg = sum(finals) / len(finals)
        pot = args.stake * len(finals)
        print(f"{'hold':>10}{len(finals):>7}{avg:>+9.0f}%"
              f"{sum(1 for r in finals if r > 0):>4}/{len(finals):<3}"
              f"{max(finals):>+8.0f}%{min(finals):>+8.0f}%")
        print(f"\n  Buying every alert and holding: £{pot:,.0f} in -> "
              f"£{pot * (1 + avg/100):,.0f} out")

    peaks = [max(c['curve'], key=lambda p: p[1]) for c in coins.values() if c["curve"]]
    if peaks:
        avg_peak = sum(p[1] for p in peaks) / len(peaks)
        print(f"  Selling every coin at its exact peak: {avg_peak:+.0f}% average")
        print("    (the ceiling a perfect exit would catch — not achievable, "
              "it needs the future)")

    print()
    if best_rule:
        verdict = ("no exit rule tested is profitable after costs"
                   if best_rule[1] <= 0 else
                   f"best rule is {best_rule[0]} at {best_rule[1]:+.0f}% on {best_rule[2]} coins")
        print(f"  VERDICT: {verdict}.")
    else:
        print("  VERDICT: not enough coins tracked long enough to judge any rule yet.")
        print("  Leave track.py running. ~50 alerts each followed 12h makes this answerable.")


if __name__ == "__main__":
    main()
