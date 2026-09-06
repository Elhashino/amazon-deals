"""Persistent scanner state: what we've seen, alerted, and are watching.

A single JSON file so the scanner survives restarts without re-alerting
the same coins. Everything is pruned by age so the file never grows
unbounded on a 24/7 box.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

SEEN_TTL_SECONDS = 48 * 3600
ALERTED_TTL_SECONDS = 14 * 24 * 3600
PENDING_TTL_SECONDS = 26 * 3600  # a shade past the 24h max pair age


class State:
    def __init__(self, data_dir: str) -> None:
        self.path = Path(data_dir) / "state.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict = {"seen": {}, "alerted": {}, "watching": {}, "pending": {}}
        if self.path.is_file():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    for key in self._data:
                        if isinstance(loaded.get(key), dict):
                            self._data[key] = loaded[key]
            except (json.JSONDecodeError, OSError):
                pass  # corrupt state file -> start fresh rather than crash

    # -- seen: every mint we've already evaluated (pass or fail) --
    def is_seen(self, mint: str) -> bool:
        return mint in self._data["seen"]

    def mark_seen(self, mint: str) -> None:
        self._data["seen"][mint] = time.time()

    # -- alerted: mints we pushed to Telegram --
    def is_alerted(self, mint: str) -> bool:
        return mint in self._data["alerted"]

    def mark_alerted(self, mint: str) -> None:
        self._data["alerted"][mint] = time.time()

    # -- pending: launches seen live but still too young to judge --
    def add_pending(self, mint: str, launched_at: float) -> bool:
        """Remember a live launch. Returns True if it's new to us."""
        if mint in self._data["pending"] or self.is_seen(mint) or self.is_alerted(mint):
            return False
        self._data["pending"][mint] = launched_at
        return True

    def ripe_pending(self, min_age_seconds: float) -> list[str]:
        """Pending mints old enough to have numbers worth reading, oldest first."""
        now = time.time()
        ready = [
            (ts, mint) for mint, ts in self._data["pending"].items()
            if now - ts >= min_age_seconds and not self.is_seen(mint) and not self.is_alerted(mint)
        ]
        ready.sort()
        return [mint for _, mint in ready]

    def pending_count(self) -> int:
        return len(self._data["pending"])

    # -- watching: alerted mints we re-check for a post-alert rug --
    def watch(self, mint: str, info: dict) -> None:
        self._data["watching"][mint] = {"since": time.time(), **info}

    def unwatch(self, mint: str) -> None:
        self._data["watching"].pop(mint, None)

    def watched(self) -> dict[str, dict]:
        return dict(self._data["watching"])

    def prune_and_save(self) -> None:
        now = time.time()
        self._data["seen"] = {
            m: ts for m, ts in self._data["seen"].items() if now - ts < SEEN_TTL_SECONDS
        }
        self._data["alerted"] = {
            m: ts for m, ts in self._data["alerted"].items() if now - ts < ALERTED_TTL_SECONDS
        }
        # Drop pending entries that aged out or have since been judged.
        self._data["pending"] = {
            m: ts for m, ts in self._data["pending"].items()
            if now - ts < PENDING_TTL_SECONDS and m not in self._data["seen"]
            and m not in self._data["alerted"]
        }
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._data), encoding="utf-8")
        tmp.replace(self.path)
