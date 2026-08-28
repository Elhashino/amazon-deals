"""Rejection log: every binned coin, why, and at which stage.

This file is half the product. Watching WHY coins get rejected in real
time teaches you the market faster than any caller channel — and it's the
evidence trail when tuning thresholds.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

FIELDS = ["utc_time", "mint", "symbol", "stage", "reason", "detail"]


class RejectionLog:
    def __init__(self, data_dir: str) -> None:
        self.path = Path(data_dir) / "rejections.csv"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.is_file():
            with self.path.open("w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow(FIELDS)

    def reject(self, mint: str, symbol: str, stage: str, reason: str, detail: str = "") -> None:
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with self.path.open("a", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow([now, mint, symbol, stage, reason, detail])
        print(f"  [BINNED] {symbol or mint[:8]} @ {stage}: {reason}" + (f" ({detail})" if detail else ""))
