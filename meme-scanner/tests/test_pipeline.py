"""End-to-end pipeline wiring with every external call mocked.

Proves: a clean coin flows discovery -> filters -> score -> alert -> watch;
a dirty coin is binned at the right stage with the reason logged; a
missing RugCheck report defers instead of binning; an RPC crash never
kills the cycle; a failed Telegram send leaves the coin retryable; state
is durable across a crash; and a watched coin whose liquidity vanishes
fires exactly one rug warning.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import meme_scanner.main as main_mod
from meme_scanner.config import Config
from meme_scanner.models import BundleReport, Candidate, DemandReport, SafetyReport
from meme_scanner.rejection_log import RejectionLog
from meme_scanner.state import State


class FakeAlerter:
    enabled = True

    def __init__(self, deliver=True):
        self.sent = []
        self.deliver = deliver

    def send(self, text):
        self.sent.append(text)
        return self.deliver


def good_candidate(mint="GOODMINT") -> Candidate:
    return Candidate(
        mint=mint, symbol="GOOD", name="Good Coin", pair_address="PAIRG",
        dex_id="pumpswap", url="https://dexscreener.com/solana/PAIRG",
        price_usd=0.001, liquidity_usd=70_000, fdv_usd=400_000, market_cap_usd=400_000,
        pair_created_at_ms=int((time.time() - 3600) * 1000),
        vol_m5=12_000, vol_h1=85_000, vol_h24=85_000,
        buys_m5=100, sells_m5=35, buys_h1=800, sells_h1=320,
        has_website=True, has_socials=True,
    )


def clean_safety() -> SafetyReport:
    return SafetyReport(
        mint_authority_active=False, freeze_authority_active=False,
        lp_locked_pct=100.0, top10_holder_pct=15.0, max_single_holder_pct=4.0,
        insider_pct=1.0, total_holders=700, rugcheck_score=8.0,
    )


def healthy_demand() -> DemandReport:
    return DemandReport(unique_traders=300, diversity=0.85, sampled=40, total_txns=900)


def wire(monkeypatch, tmp_path, *, candidate, safety, bundle=None,
         demand=None, deliver=True):
    cfg = Config(data_dir=str(tmp_path))
    state = State(cfg.data_dir)
    log = RejectionLog(cfg.data_dir)
    alerter = FakeAlerter(deliver=deliver)
    monkeypatch.setattr(main_mod.dexscreener, "discover_mints",
                        lambda: {candidate.mint: False})
    monkeypatch.setattr(main_mod.dexscreener, "get_candidates",
                        lambda mints: {candidate.mint: candidate})
    monkeypatch.setattr(main_mod.dexscreener, "get_candidate", lambda mint: candidate)
    monkeypatch.setattr(main_mod.rugcheck, "new_token_mints", lambda: [])
    monkeypatch.setattr(main_mod.rugcheck, "get_safety", lambda mint: safety)
    monkeypatch.setattr(main_mod.bundles, "analyze",
                        lambda mint, c: bundle or BundleReport(checked_wallets=15, bundled_wallets=0,
                                                               bundled_pct_of_supply=2.0))
    monkeypatch.setattr(main_mod.solana_rpc, "unique_traders_h1",
                        lambda pair, c: demand or healthy_demand())
    return cfg, state, log, alerter


def read_log(tmp_path) -> str:
    return (tmp_path / "rejections.csv").read_text()


def test_clean_coin_alerts_and_watches(monkeypatch, tmp_path):
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=clean_safety())
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1
    assert "GOOD" in alerter.sent[0] and "Score" in alerter.sent[0]
    assert state.is_alerted("GOODMINT")
    assert "GOODMINT" in state.watched()
    main_mod.run_cycle(cfg, state, log, alerter)  # already seen, no duplicate
    assert len(alerter.sent) == 1


def test_alert_state_is_durable_immediately(monkeypatch, tmp_path):
    """Regression: state was only written at the end of a cycle, so a crash
    after alerting replayed that alert on the next start."""
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=clean_safety())
    monkeypatch.setattr(main_mod, "check_watched",
                        lambda c, s, a: (_ for _ in ()).throw(RuntimeError("crash after alert")))
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1
    reloaded = State(cfg.data_dir)  # what a restart would read from disk
    assert reloaded.is_alerted("GOODMINT")


def test_failed_telegram_send_leaves_coin_retryable(monkeypatch, tmp_path):
    """Regression: a survivor was marked seen before the send, so a network
    blip dropped the day's one alert permanently."""
    cfg, state, log, alerter = wire(monkeypatch, tmp_path, candidate=good_candidate(),
                                    safety=clean_safety(), deliver=False)
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1          # attempted
    assert not state.is_alerted("GOODMINT")
    assert not state.is_seen("GOODMINT")   # still eligible next cycle

    alerter.deliver = True
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 2 and state.is_alerted("GOODMINT")


def test_dirty_coin_binned_at_safety_with_reason(monkeypatch, tmp_path):
    dirty = clean_safety()
    dirty.mint_authority_active = True
    dirty.lp_locked_pct = 0.0
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=dirty)
    main_mod.run_cycle(cfg, state, log, alerter)
    assert not alerter.sent
    logged = read_log(tmp_path)
    assert "safety" in logged and "mint authority" in logged
    assert state.is_seen("GOODMINT")


def test_no_rugcheck_report_defers_not_bins(monkeypatch, tmp_path):
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=None)
    main_mod.run_cycle(cfg, state, log, alerter)
    assert not alerter.sent
    assert not state.is_seen("GOODMINT")
    assert "GOODMINT" not in read_log(tmp_path)


def test_bundled_coin_binned(monkeypatch, tmp_path):
    heavy = BundleReport(checked_wallets=20, bundled_wallets=14,
                         bundled_pct_of_supply=55.0, funder_clusters=3)
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=clean_safety(),
                                    bundle=heavy)
    main_mod.run_cycle(cfg, state, log, alerter)
    assert not alerter.sent
    assert "bundles" in read_log(tmp_path)


def test_wash_traded_coin_binned(monkeypatch, tmp_path):
    washed = DemandReport(unique_traders=3, diversity=0.05, sampled=40, total_txns=900)
    cfg, state, log, alerter = wire(monkeypatch, tmp_path, candidate=good_candidate(),
                                    safety=clean_safety(), demand=washed)
    main_mod.run_cycle(cfg, state, log, alerter)
    assert not alerter.sent
    assert "demand" in read_log(tmp_path)


def test_rpc_crash_does_not_kill_candidate(monkeypatch, tmp_path):
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=clean_safety())

    def boom(mint, c):
        raise RuntimeError("RPC 429")

    monkeypatch.setattr(main_mod.bundles, "analyze", boom)
    monkeypatch.setattr(main_mod.solana_rpc, "unique_traders_h1",
                        lambda pair, c: (_ for _ in ()).throw(RuntimeError("RPC down")))
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1
    # and the alert says which checks could not be completed
    assert "Not verified" in alerter.sent[0]


def test_rug_watch_fires_once(monkeypatch, tmp_path):
    cand = good_candidate()
    cfg, state, log, alerter = wire(monkeypatch, tmp_path, candidate=cand, safety=clean_safety())
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1

    rugged = good_candidate()
    rugged.liquidity_usd = 7_000
    monkeypatch.setattr(main_mod.dexscreener, "get_candidate", lambda mint: rugged)
    main_mod.check_watched(cfg, state, alerter)
    assert len(alerter.sent) == 2 and "RUG WARNING" in alerter.sent[1]
    main_mod.check_watched(cfg, state, alerter)  # no repeat spam
    assert len(alerter.sent) == 2


def test_rug_watch_warns_when_pair_disappears(monkeypatch, tmp_path):
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=clean_safety())
    main_mod.run_cycle(cfg, state, log, alerter)
    monkeypatch.setattr(main_mod.dexscreener, "get_candidate", lambda mint: None)
    main_mod.check_watched(cfg, state, alerter)
    assert "dropped off DexScreener" in alerter.sent[-1]
    assert "GOODMINT" not in state.watched()


def test_cycle_uses_batched_enrichment(monkeypatch, tmp_path):
    """Regression: one DexScreener call per mint burned the rate limit that
    the 30-mint batch endpoint exists to protect."""
    cands = {f"M{i}": good_candidate(f"M{i}") for i in range(25)}
    calls = []
    cfg = Config(data_dir=str(tmp_path))
    state, log, alerter = State(cfg.data_dir), RejectionLog(cfg.data_dir), FakeAlerter()
    monkeypatch.setattr(main_mod.dexscreener, "discover_mints",
                        lambda: {m: False for m in cands})
    monkeypatch.setattr(main_mod.rugcheck, "new_token_mints", lambda: [])

    def fake_batch(mints):
        calls.append(list(mints))
        return {m: cands[m] for m in mints if m in cands}

    monkeypatch.setattr(main_mod.dexscreener, "get_candidates", fake_batch)
    monkeypatch.setattr(main_mod.rugcheck, "get_safety", lambda mint: None)  # defer early
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(calls) == 1 and len(calls[0]) == 25


def test_per_cycle_cap_is_reported_not_silent(monkeypatch, tmp_path, capsys):
    many = {f"M{i}": good_candidate(f"M{i}") for i in range(80)}
    cfg = Config(data_dir=str(tmp_path))
    state, log, alerter = State(cfg.data_dir), RejectionLog(cfg.data_dir), FakeAlerter()
    monkeypatch.setattr(main_mod.dexscreener, "discover_mints", lambda: {m: False for m in many})
    monkeypatch.setattr(main_mod.rugcheck, "new_token_mints", lambda: [])
    monkeypatch.setattr(main_mod.dexscreener, "get_candidates",
                        lambda mints: {m: many[m] for m in mints})
    monkeypatch.setattr(main_mod.rugcheck, "get_safety", lambda mint: None)
    evaluated = main_mod.run_cycle(cfg, state, log, alerter)
    assert evaluated == main_mod.MAX_EVALUATIONS_PER_CYCLE
    assert "held over" in capsys.readouterr().out
