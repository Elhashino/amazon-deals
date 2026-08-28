"""Scoring maths: strong candidates score high, weak ones low, missing data never crashes."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from meme_scanner.models import Candidate, SafetyReport
from meme_scanner.scoring import score


def strong_candidate() -> Candidate:
    return Candidate(
        mint="M1", symbol="GOOD",
        liquidity_usd=80_000, fdv_usd=500_000,
        vol_m5=15_000, vol_h1=90_000,
        buys_m5=120, sells_m5=40, buys_h1=900, sells_h1=350,
        has_website=True, has_socials=True,
    )


def test_strong_candidate_scores_high():
    v = score(strong_candidate(), SafetyReport(total_holders=800), unique_buyers_h1=400)
    assert v.score >= 70, v.components
    assert abs(sum(v.components.values()) - v.score) < 0.5


def test_weak_candidate_scores_low():
    weak = Candidate(
        mint="M2", symbol="MEH",
        liquidity_usd=21_000, fdv_usd=2_000_000,
        vol_m5=200, vol_h1=30_000,          # volume dying
        buys_m5=5, sells_m5=25, buys_h1=100, sells_h1=250,  # sell pressure
    )
    v = score(weak, SafetyReport(total_holders=110), unique_buyers_h1=30)
    assert v.score < 35, v.components


def test_all_missing_data_is_zero_not_crash():
    v = score(Candidate(mint="M3"), SafetyReport(), unique_buyers_h1=None)
    assert v.score == 0.0


def test_unknown_unique_buyers_gets_partial_credit_from_txns():
    cand = strong_candidate()
    with_known = score(cand, SafetyReport(total_holders=800), unique_buyers_h1=400)
    with_unknown = score(cand, SafetyReport(total_holders=800), unique_buyers_h1=None)
    assert 0 < with_unknown.components["unique_demand"] < with_known.components["unique_demand"]


def test_boost_alone_is_nearly_worthless():
    boosted = Candidate(mint="M4", boosted=True)
    organic = Candidate(mint="M5", has_website=True, has_socials=True)
    vb = score(boosted, SafetyReport(), None)
    vo = score(organic, SafetyReport(), None)
    assert vo.components["identity"] > vb.components["identity"] * 5
