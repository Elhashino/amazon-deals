"""Kill-filter behaviour: reject on evidence, defer on missing data, pass only when clean."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meme_scanner.config import Config
from meme_scanner.filters import Outcome, check_bundles, check_demand, check_market, check_safety
from meme_scanner.models import BundleReport, Candidate, SafetyReport

CFG = Config()


def ms_ago(minutes: float) -> int:
    return int((time.time() - minutes * 60) * 1000)


def clean_safety() -> SafetyReport:
    return SafetyReport(
        mint_authority_active=False,
        freeze_authority_active=False,
        lp_locked_pct=100.0,
        top10_holder_pct=18.0,
        max_single_holder_pct=4.0,
        insider_pct=2.0,
        total_holders=450,
        rugcheck_score=5.0,
        risk_names=[],
    )


# --- market ---

def test_market_pass():
    cand = Candidate(mint="M", liquidity_usd=50_000, pair_created_at_ms=ms_ago(60))
    assert check_market(cand, CFG)[0] is Outcome.PASS


def test_market_low_liquidity_rejects():
    cand = Candidate(mint="M", liquidity_usd=3_000, pair_created_at_ms=ms_ago(60))
    outcome, fails = check_market(cand, CFG)
    assert outcome is Outcome.REJECT and "liquidity" in fails[0]


def test_market_too_young_defers_not_rejects():
    cand = Candidate(mint="M", liquidity_usd=50_000, pair_created_at_ms=ms_ago(2))
    assert check_market(cand, CFG)[0] is Outcome.DEFER


def test_market_too_old_rejects():
    cand = Candidate(mint="M", liquidity_usd=50_000, pair_created_at_ms=ms_ago(60 * 48))
    assert check_market(cand, CFG)[0] is Outcome.REJECT


def test_market_missing_liquidity_defers():
    cand = Candidate(mint="M", pair_created_at_ms=ms_ago(60))
    assert check_market(cand, CFG)[0] is Outcome.DEFER


# --- safety ---

def test_safety_clean_passes():
    assert check_safety(clean_safety(), CFG)[0] is Outcome.PASS


def test_safety_missing_data_defers_never_passes():
    outcome, reasons = check_safety(SafetyReport(), CFG)
    assert outcome is Outcome.DEFER
    assert "missing" in reasons[0]


def test_safety_active_mint_authority_rejects():
    s = clean_safety()
    s.mint_authority_active = True
    outcome, fails = check_safety(s, CFG)
    assert outcome is Outcome.REJECT and any("mint authority" in f for f in fails)


def test_safety_unlocked_lp_rejects():
    s = clean_safety()
    s.lp_locked_pct = 10.0
    assert check_safety(s, CFG)[0] is Outcome.REJECT


def test_safety_concentrated_holders_rejects():
    s = clean_safety()
    s.top10_holder_pct = 55.0
    assert check_safety(s, CFG)[0] is Outcome.REJECT


def test_safety_reports_every_failure_not_just_first():
    s = clean_safety()
    s.mint_authority_active = True
    s.lp_locked_pct = 0.0
    s.top10_holder_pct = 80.0
    _, fails = check_safety(s, CFG)
    assert len(fails) >= 3


def test_safety_critical_risk_name_rejects_even_when_numbers_clean():
    s = clean_safety()
    s.risk_names = ["Freeze Authority still enabled"]
    outcome, fails = check_safety(s, CFG)
    assert outcome is Outcome.REJECT and "critical risk" in fails[0]


def test_safety_rugged_flag_rejects():
    s = clean_safety()
    s.rugged_flag = True
    assert check_safety(s, CFG)[0] is Outcome.REJECT


# --- bundles & demand ---

def test_bundled_supply_rejects():
    b = BundleReport(checked_wallets=20, bundled_wallets=12, bundled_pct_of_supply=45.0, funder_clusters=2)
    outcome, fails = check_bundles(b, CFG)
    assert outcome is Outcome.REJECT and "bundled" in fails[0]


def test_bundle_unknown_is_best_effort_pass():
    assert check_bundles(BundleReport(), CFG)[0] is Outcome.PASS


def test_low_unique_buyers_rejects():
    assert check_demand(5, CFG)[0] is Outcome.REJECT
    assert check_demand(200, CFG)[0] is Outcome.PASS
    assert check_demand(None, CFG)[0] is Outcome.PASS
