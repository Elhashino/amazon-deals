"""Rejection log: every binned coin, why, and at which stage.

This file is half the product. Watching WHY coins get rejected in real
time teaches you the market faster than any caller channel — and it's the
evidence trail when tuning thresholds.

Two hardening details, because this file is written from attacker-supplied
token names and read in Excel: cells that look like formulas are quoted so
a token called "=cmd|..." cannot execute on open, and the log rotates so a
24/7 box never fills its disk.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

FIELDS = ["utc_time", "mint", "symbol", "stage", "reason", "detail"]
MAX_BYTES = 5 * 1024 * 1024  # rotate at ~5 MB, keep one previous file


def _defuse(value: str) -> str:
    """Neutralize spreadsheet formula injection from token names."""
    text = str(value)
    if text[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + text
    return text


class RejectionLog:
    def __init__(self, data_dir: str) -> None:
        self.path = Path(data_dir) / "rejections.csv"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.is_file():
            with self.path.open("w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow(FIELDS)

    def _rotate_if_large(self) -> None:
        try:
            if self.path.stat().st_size < MAX_BYTES:
                return
        except OSError:
            return
        self.path.replace(self.path.with_suffix(".prev.csv"))
        self._ensure_header()

    def reject(self, mint: str, symbol: str, stage: str, reason: str, detail: str = "") -> None:
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [now, mint, symbol, stage, reason, detail]
        try:
            self._rotate_if_large()
            with self.path.open("a", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow([_defuse(c) for c in row])
        except OSError as exc:
            # A full or locked disk must never stop the scan loop.
            print(f"  [WARN] could not write rejection log: {exc}")
        print(f"  [BINNED] {symbol or mint[:8]} @ {stage}: {reason}" + (f" ({detail})" if detail else ""))
