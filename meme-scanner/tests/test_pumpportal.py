"""Live launch feed: frame parsing, buffering, and the ripening queue.

The websocket itself isn't exercised here (no network in tests); what is
tested is everything that decides what the scanner *does* with a frame —
which is where the bugs would be.
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import meme_scanner.main as main_mod
from meme_scanner.apis import pumpportal
from meme_scanner.apis.pumpportal import LaunchFeed, parse_new_token_frame
from meme_scanner.config import Config
from meme_scanner.rejection_log import RejectionLog
from meme_scanner.state import State

# A real 'create' frame shape, as captured from the live feed.
CREATE_FRAME = json.dumps({
    "signature": "5xY...", "mint": "So1anaMint111pump", "traderPublicKey": "Dev111",
    "txType": "create", "initialBuy": 42_000_000, "marketCapSol": 31.2,
    "bondingCurveKey": "BC111", "vSolInBondingCurve": 30.9,
    "vTokensInBondingCurve": 1_070_000_000, "name": "Dog Wif Cap",
    "symbol": "WIFCAP", "uri": "https://ipfs.io/x", "pool": "pump",
})


def test_parses_a_launch():
    launch = parse_new_token_frame(CREATE_FRAME)
    assert launch == {"mint": "So1anaMint111pump", "symbol": "WIFCAP",
                      "name": "Dog Wif Cap", "pool": "pump"}


def test_minimal_frame_still_parses():
    """Fields beyond `mint` are undocumented, so none may be required."""
    launch = parse_new_token_frame(json.dumps({"txType": "create", "mint": "M1"}))
    assert launch["mint"] == "M1" and launch["symbol"] == "" and launch["pool"] == "pump"


def test_non_launch_frames_ignored():
    # subscription ack (no txType), trades, migrations, junk
    assert parse_new_token_frame(json.dumps({"message": "Successfully subscribed"})) is None
    assert parse_new_token_frame(json.dumps({"txType": "buy", "mint": "M"})) is None
    assert parse_new_token_frame(json.dumps({"txType": "migrate", "mint": "M"})) is None
    assert parse_new_token_frame(json.dumps({"txType": "create"})) is None       # no mint
    assert parse_new_token_frame(json.dumps({"txType": "create", "mint": " "})) is None
    assert parse_new_token_frame(json.dumps(["not", "a", "dict"])) is None
    assert parse_new_token_frame("<html>502 Bad Gateway</html>") is None
    assert parse_new_token_frame(b"") is None


def test_buffer_drains_once_and_dedupes():
    feed = LaunchFeed()
    feed._record({"mint": "A"})
    feed._record({"mint": "B"})
    feed._record({"mint": "A"})          # duplicate frame
    assert set(feed.drain()) == {"A", "B"}
    assert feed.drain() == {}            # draining clears


def test_buffer_is_bounded(monkeypatch):
    """An outage must not let the backlog grow without limit."""
    monkeypatch.setattr(pumpportal, "MAX_BUFFERED", 10)
    feed = LaunchFeed()
    for i in range(50):
        feed._record({"mint": f"M{i}"})
    drained = feed.drain()
    assert len(drained) == 10
    assert "M49" in drained and "M0" not in drained  # newest kept


def test_status_reports_offline_reason():
    feed = LaunchFeed()
    assert feed.status == "starting"
    feed._last_error = "ConnectionRefusedError: nope"
    assert "offline" in feed.status and "ConnectionRefused" in feed.status


def test_missing_library_is_survivable(monkeypatch):
    """No websocket-client installed => feed off, scanner still runs."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "websocket":
            raise ImportError("no module named websocket")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert LaunchFeed().start() is False


# ---------- the ripening queue ----------

class FakeFeed:
    def __init__(self, mints):
        self._mints = mints

    status = "live (test)"

    def drain(self):
        found, self._mints = {m: {"mint": m} for m in self._mints}, []
        return found


def test_new_launches_ripen_before_being_judged(tmp_path, monkeypatch, capsys):
    """A 10-second-old coin has nothing worth reading yet: it must queue,
    not be evaluated and binned."""
    cfg = Config(data_dir=str(tmp_path))
    state, log = State(cfg.data_dir), RejectionLog(cfg.data_dir)
    monkeypatch.setattr(main_mod.dexscreener, "discover_mints", lambda: {})
    monkeypatch.setattr(main_mod.rugcheck, "new_token_mints", lambda: [])
    called = []
    monkeypatch.setattr(main_mod.dexscreener, "get_candidates",
                        lambda mints: called.append(list(mints)) or {})

    main_mod.run_cycle(cfg, state, log, object(), FakeFeed(["FRESH1", "FRESH2"]))
    assert state.pending_count() == 2
    assert called == [[]]                      # nothing evaluated yet
    assert "2 ripening" in capsys.readouterr().out


def test_ripe_launch_is_promoted_for_evaluation(tmp_path, monkeypatch):
    cfg = Config(data_dir=str(tmp_path))
    state, log = State(cfg.data_dir), RejectionLog(cfg.data_dir)
    # a launch first seen 40 minutes ago (past the 20-minute minimum)
    state.add_pending("RIPE1", time.time() - 40 * 60)
    state.add_pending("YOUNG1", time.time() - 60)
    monkeypatch.setattr(main_mod.dexscreener, "discover_mints", lambda: {})
    monkeypatch.setattr(main_mod.rugcheck, "new_token_mints", lambda: [])
    asked = []
    monkeypatch.setattr(main_mod.dexscreener, "get_candidates",
                        lambda mints: asked.append(list(mints)) or {})

    main_mod.run_cycle(cfg, state, log, object(), FakeFeed([]))
    assert asked == [["RIPE1"]]                # ripe promoted, young held back


def test_pending_is_durable_and_pruned(tmp_path):
    st = State(str(tmp_path))
    st.add_pending("KEEP", time.time() - 60)
    st.add_pending("STALE", time.time() - 30 * 3600)   # past the 26h TTL
    st.add_pending("JUDGED", time.time() - 60)
    st.mark_seen("JUDGED")
    st.prune_and_save()

    reloaded = State(str(tmp_path))
    assert reloaded.pending_count() == 1
    assert reloaded.ripe_pending(0) == ["KEEP"]


def test_pending_ignores_already_judged_mints():
    st = State("/tmp/does-not-matter-unused")
    st.mark_seen("SEEN1")
    st.mark_alerted("ALERTED1")
    assert st.add_pending("SEEN1", time.time()) is False
    assert st.add_pending("ALERTED1", time.time()) is False
    assert st.add_pending("NEW1", time.time()) is True
    assert st.add_pending("NEW1", time.time()) is False  # no duplicates
