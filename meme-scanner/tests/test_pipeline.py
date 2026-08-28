"""End-to-end pipeline wiring with every external call mocked.

Proves: a clean coin flows discovery -> filters -> score -> alert -> watch;
a dirty coin is binned at the right stage with the reason logged; a
missing RugCheck report defers instead of binning; an RPC crash never
kills the cycle; a watched coin whose liquidity vanishes fires exactly
one rug warning.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import meme_scanner.main as main_mod
from meme_scanner.config import Config
from meme_scanner.models import BundleReport, Candidate, SafetyReport
from meme_scanner.rejection_log import RejectionLog
from meme_scanner.state import State


class FakeAlerter:
    enabled = True

    def __init__(self):
        self.sent = []

    def send(self, text):
        self.sent.append(text)
        return True


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


def wire(monkeypatch, tmp_path, *, candidate, safety, bundle=None, buyers=300):
    cfg = Config(data_dir=str(tmp_path))
    state = State(cfg.data_dir)
    log = RejectionLog(cfg.data_dir)
    alerter = FakeAlerter()
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
    monkeypatch.setattr(main_mod.solana_rpc, "unique_buyers_h1", lambda pair, c: buyers)
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
    # second cycle: already seen, no duplicate alert
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1


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
    assert state.is_seen("GOODMINT")  # binned for good, won't re-fetch


def test_no_rugcheck_report_defers_not_bins(monkeypatch, tmp_path):
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=None)
    main_mod.run_cycle(cfg, state, log, alerter)
    assert not alerter.sent
    assert not state.is_seen("GOODMINT")  # eligible again next cycle
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


def test_rpc_crash_does_not_kill_candidate(monkeypatch, tmp_path):
    cfg, state, log, alerter = wire(monkeypatch, tmp_path,
                                    candidate=good_candidate(), safety=clean_safety())

    def boom(mint, c):
        raise RuntimeError("RPC 429")

    monkeypatch.setattr(main_mod.bundles, "analyze", boom)
    monkeypatch.setattr(main_mod.solana_rpc, "unique_buyers_h1",
                        lambda pair, c: (_ for _ in ()).throw(RuntimeError("RPC down")))
    main_mod.run_cycle(cfg, state, log, alerter)
    # bundle + buyers unknown -> best-effort pass, scored with partial credit
    assert len(alerter.sent) == 1


def test_rug_watch_fires_once(monkeypatch, tmp_path):
    cand = good_candidate()
    cfg, state, log, alerter = wire(monkeypatch, tmp_path, candidate=cand, safety=clean_safety())
    main_mod.run_cycle(cfg, state, log, alerter)
    assert len(alerter.sent) == 1

    # liquidity collapses 90%
    rugged = good_candidate()
    rugged.liquidity_usd = 7_000
    monkeypatch.setattr(main_mod.dexscreener, "get_candidate", lambda mint: rugged)
    main_mod.check_watched(cfg, state, alerter)
    assert len(alerter.sent) == 2 and "RUG WARNING" in alerter.sent[1]
    main_mod.check_watched(cfg, state, alerter)  # no repeat spam
    assert len(alerter.sent) == 2
