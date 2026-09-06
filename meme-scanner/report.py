"""Read data/rejections.csv and tell you what the market has been doing.

The scanner writes a line every time it bins a coin. Over a week that file
becomes the most useful thing in this project: a record of how the launches
you *didn't* see were actually built. This turns it into a readable summary.

Run it with:  python report.py            (or: python report.py --days 3)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
from collections import Counter
from pathlib import Path

from meme_scanner.config import Config

STAGE_LABELS = {
    "market": "Market   (liquidity / age)",
    "safety": "Safety   (contract & holders)",
    "bundles": "Bundles  (one entity, many wallets)",
    "demand":  "Demand   (real buyers vs bots)",
    "score":   "Momentum (clean but going nowhere)",
}

# Collapse "liquidity $3,412 < $20,000" into "liquidity below minimum" so the
# same failure doesn't appear 200 times with 200 different numbers.
NORMALIZERS = [
    (r"liquidity \$[\d,]+ < \$[\d,]+", "liquidity below minimum"),
    (r"too old \([\d.]+h > \d+h\)", "older than the 24h window"),
    (r"LP only \d+% locked/burned.*", "LP not locked — dev can pull the pool"),
    (r"top 10 wallets hold \d+%.*", "top 10 wallets hold too much"),
    (r"one wallet holds \d+%.*", "a single wallet holds too much"),
    (r"only \d+ holders.*", "too few holders"),
    (r"insider network holds \d+%.*", "insider network holds too much"),
    (r"RugCheck risk score \d+.*", "RugCheck risk score too high"),
    (r"bundled wallets hold \d+% of supply.*", "bundled wallets — one entity, many wallets"),
    (r"~?\d+ unique traders \(<.*", "too few real traders"),
    (r"wash-trading pattern.*", "wash trading — a few wallets, all the volume"),
    (r"momentum score \d+ < \d+", "no momentum worth alerting on"),
    (r"critical risk flag: (.*)", r"critical flag: \1"),
]


def normalize(reason: str) -> str:
    text = reason.strip()
    for pattern, replacement in NORMALIZERS:
        new, n = re.subn(pattern, replacement, text)
        if n:
            return new
    return text


def bar(count: int, total: int, width: int = 28) -> str:
    filled = round(width * count / total) if total else 0
    return "█" * filled + "·" * (width - filled)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize the scanner's rejection log")
    parser.add_argument("--days", type=float, default=7.0, help="how far back to look")
    parser.add_argument("--top", type=int, default=12, help="how many reasons to list")
    args = parser.parse_args()

    path = Path(Config.load().data_dir) / "rejections.csv"
    if not path.is_file():
        print(f"No log yet at {path} — run the scanner first (or: python demo.py --week)")
        return

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
    rows = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                when = dt.datetime.strptime(row["utc_time"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=dt.timezone.utc
                )
            except (ValueError, KeyError, TypeError):
                continue
            if when >= cutoff:
                rows.append((when, row))

    if not rows:
        print(f"Nothing binned in the last {args.days:g} days.")
        return

    total = len(rows)
    first, last = min(r[0] for r in rows), max(r[0] for r in rows)
    span_days = max((last - first).total_seconds() / 86400, 0.04)

    print()
    print("=" * 62)
    print("  WHAT THE SCANNER BINNED")
    print(f"  {first:%d %b %H:%M} → {last:%d %b %H:%M} UTC   ({span_days:.1f} days)")
    print("=" * 62)
    print(f"\n  {total:,} coins binned  ·  ~{total / span_days:,.0f} per day\n")

    print("  WHERE THEY DIED")
    print("  " + "-" * 58)
    stages = Counter(r[1]["stage"] for r in rows)
    for stage, count in stages.most_common():
        label = STAGE_LABELS.get(stage, stage)
        print(f"  {label:36s} {bar(count, total)} {count:>5,} ({count / total:4.0%})")

    print("\n  WHY — the specific failures, most common first")
    print("  " + "-" * 58)
    reasons = Counter()
    for _, row in rows:
        # A coin can fail several checks at once; count each one.
        for part in (row.get("reason") or "").split(";"):
            if part.strip():
                reasons[normalize(part)] += 1
    top = reasons.most_common(args.top)
    widest = max((len(r) for r, _ in top), default=10)
    for reason, count in top:
        print(f"  {reason:<{min(widest, 46)}.46s} {count:>5,}")

    print("\n  " + "-" * 58)
    print("  Read that list once a week. It is what people who lose money")
    print("  on these coins never see — the shape of what they bought.\n")


if __name__ == "__main__":
    main()
