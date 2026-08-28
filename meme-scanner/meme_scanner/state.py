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


class State:
    def __init__(self, data_dir: str) -> None:
        self.path = Path(data_dir) / "state.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict = {"seen": {}, "alerted": {}, "watching": {}}
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
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._data), encoding="utf-8")
        tmp.replace(self.path)
