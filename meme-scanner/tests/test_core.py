"""Tests for the research-independent core: config, state, telegram, rejection log."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meme_scanner.config import Config, _load_dotenv
from meme_scanner.rejection_log import RejectionLog
from meme_scanner.state import State
from meme_scanner.telegram import Alerter, escape_html


def test_dotenv_parsing(tmp_path):
    env = tmp_path / ".env"
    env.write_text(
        "# comment\n"
        "TELEGRAM_BOT_TOKEN=123:abc\n"
        'MIN_LIQUIDITY_USD="35000"\n'
        "BROKEN LINE\n"
        "POLL_SECONDS = 90\n",
        encoding="utf-8",
    )
    values = _load_dotenv(env)
    assert values["TELEGRAM_BOT_TOKEN"] == "123:abc"
    assert values["MIN_LIQUIDITY_USD"] == "35000"
    assert values["POLL_SECONDS"] == "90"
    assert "BROKEN LINE" not in values


def test_config_env_precedence(monkeypatch):
    monkeypatch.setenv("MIN_LIQUIDITY_USD", "50000")
    monkeypatch.setenv("POLL_SECONDS", "30")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    cfg = Config.load()
    assert cfg.min_liquidity_usd == 50000.0
    assert cfg.poll_seconds == 30
    assert cfg.telegram_bot_token == "tok"
    # untouched values keep defaults
    assert cfg.max_top10_holder_pct == 30.0


def test_state_roundtrip(tmp_path):
    st = State(str(tmp_path))
    assert not st.is_seen("mintA")
    st.mark_seen("mintA")
    st.mark_alerted("mintB")
    st.watch("mintB", {"liquidity_usd": 50000})
    st.prune_and_save()

    st2 = State(str(tmp_path))
    assert st2.is_seen("mintA")
    assert st2.is_alerted("mintB")
    assert "mintB" in st2.watched()
    assert st2.watched()["mintB"]["liquidity_usd"] == 50000
    st2.unwatch("mintB")
    assert "mintB" not in st2.watched()


def test_state_survives_corrupt_file(tmp_path):
    (tmp_path / "state.json").write_text("{not json", encoding="utf-8")
    st = State(str(tmp_path))
    assert not st.is_seen("anything")
    st.mark_seen("anything")
    st.prune_and_save()
    assert json.loads((tmp_path / "state.json").read_text())["seen"]


def test_escape_html():
    assert escape_html("<$PEPE> & co") == "&lt;$PEPE&gt; &amp; co"


def test_alerter_console_mode_and_truncation(capsys):
    alerter = Alerter("", "")
    assert not alerter.enabled
    assert alerter.send("x" * 5000)  # console mode always "succeeds"
    out = capsys.readouterr().out
    assert "[truncated]" in out


def test_rejection_log_writes_csv(tmp_path, capsys):
    log = RejectionLog(str(tmp_path))
    log.reject("mint123", "PEPE", "hard_filter", "liquidity too low", "$3k < $20k")
    lines = (tmp_path / "rejections.csv").read_text().strip().splitlines()
    assert len(lines) == 2
    assert "liquidity too low" in lines[1]
    assert "BINNED" in capsys.readouterr().out
