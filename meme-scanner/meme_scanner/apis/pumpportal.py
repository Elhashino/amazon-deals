"""PumpPortal live launch feed (free websocket, no key, no account).

Why this exists: DexScreener has no public "new pairs" endpoint, so the
polling feeds only surface coins whose teams paid for a profile or boost,
minutes after the fact. PumpPortal pushes a message the instant a pump.fun
token is created. Same coins, seconds instead of minutes, and it catches
the ones nobody paid to promote.

Honest caveat, from the research: PumpPortal's docs do not specify the
message fields. The shape below was verified from real captured traffic,
so every field is read defensively and only `mint` is treated as required.
A frame we don't understand is skipped, never guessed at.

This runs on a background thread and is strictly additive: if the socket
is down, unreachable, or the library isn't installed, the scanner carries
on with its polling feeds and simply says so.
"""

from __future__ import annotations

import json
import threading
import time

WS_URL = "wss://pumpportal.fun/api/data"
SUBSCRIBE = {"method": "subscribeNewToken"}

# Don't let an outage or a burst grow the buffer without bound.
MAX_BUFFERED = 5_000
BACKOFF_START = 2.0
BACKOFF_MAX = 120.0


def parse_new_token_frame(raw: str | bytes) -> dict | None:
    """Extract a launch from one websocket frame, or None if it isn't one.

    Skips the subscription acknowledgement (no txType), trade/migration
    frames, malformed JSON, and anything without a usable mint address.
    """
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("txType") != "create":
        return None
    mint = data.get("mint")
    if not isinstance(mint, str) or not mint.strip():
        return None
    return {
        "mint": mint.strip(),
        "symbol": str(data.get("symbol") or ""),
        "name": str(data.get("name") or ""),
        "pool": str(data.get("pool") or "pump"),
    }


class LaunchFeed:
    """Background listener. Call start(), then drain() each cycle."""

    def __init__(self, url: str = WS_URL) -> None:
        self.url = url
        self._lock = threading.Lock()
        self._buffer: dict[str, dict] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._connected = False
        self._launches_seen = 0
        self._last_error = ""

    # --- state a caller can report on ---
    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def status(self) -> str:
        if self._connected:
            return f"live ({self._launches_seen} launches seen)"
        return f"offline ({self._last_error})" if self._last_error else "starting"

    def start(self) -> bool:
        """Begin listening. Returns False if the feed can't run at all."""
        try:
            import websocket  # noqa: F401  (websocket-client)
        except ImportError:
            self._last_error = "websocket-client not installed"
            print("  [WARN] PumpPortal feed off: pip install websocket-client")
            return False
        self._thread = threading.Thread(target=self._run, name="pumpportal", daemon=True)
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()

    def drain(self) -> dict[str, dict]:
        """Take everything buffered since the last call."""
        with self._lock:
            found, self._buffer = self._buffer, {}
        return found

    def _record(self, launch: dict) -> None:
        with self._lock:
            if len(self._buffer) >= MAX_BUFFERED:
                # Keep the newest; a backlog this deep means the scanner is
                # far behind anyway and stale launches are worthless.
                self._buffer.pop(next(iter(self._buffer)))
            self._buffer[launch["mint"]] = launch
            self._launches_seen += 1

    def _run(self) -> None:
        import websocket

        backoff = BACKOFF_START
        while not self._stop.is_set():
            try:
                conn = websocket.create_connection(self.url, timeout=30)
                conn.send(json.dumps(SUBSCRIBE))
                self._connected = True
                self._last_error = ""
                backoff = BACKOFF_START  # a good connection resets the backoff
                while not self._stop.is_set():
                    try:
                        frame = conn.recv()
                    except websocket.WebSocketTimeoutException:
                        continue  # quiet minute, not a failure
                    launch = parse_new_token_frame(frame)
                    if launch:
                        self._record(launch)
                conn.close()
            except Exception as exc:  # any network/protocol failure: retry
                self._connected = False
                self._last_error = f"{type(exc).__name__}: {exc}"[:120]
                if self._stop.wait(backoff):
                    break
                backoff = min(backoff * 2, BACKOFF_MAX)
        self._connected = False
