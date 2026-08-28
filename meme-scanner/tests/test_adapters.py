"""Adapter normalization against response shapes verified from API docs/clients.

These fixtures mirror the documented quirks: priceUsd as a string, epoch-ms
pairCreatedAt, bare-array envelopes, per-market LP lock, AMM vaults inside
topHolders, null-means-renounced authorities.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import meme_scanner.apis.dexscreener as dex
import meme_scanner.apis.rugcheck as rc
from meme_scanner.apis.http import ApiError

FULL_PAIR = {
    "chainId": "solana",
    "dexId": "pumpswap",
    "url": "https://dexscreener.com/solana/PAIR1",
    "pairAddress": "PAIR1",
    "baseToken": {"address": "MINT1", "name": "Dog Wif Laser", "symbol": "DWL"},
    "quoteToken": {"address": "So11111111111111111111111111111111111111112", "symbol": "SOL"},
    "priceUsd": "0.0004123",
    "priceNative": "0.0000021",
    "txns": {"m5": {"buys": 80, "sells": 30}, "h1": {"buys": 700, "sells": 300}},
    "volume": {"m5": 9000, "h1": 80000, "h24": 80000},
    "priceChange": {"m5": 4.2, "h1": 61.0},
    "liquidity": {"usd": 65000.5, "base": 1, "quote": 2},
    "fdv": 410000,
    "marketCap": 410000,
    "pairCreatedAt": 1756389000000,
    "info": {"websites": [{"label": "Website", "url": "https://x.example"}],
             "socials": [{"type": "twitter", "url": "https://x.com/x"}]},
    "boosts": {"active": 500},
}

# A minutes-old pair: nearly everything optional is absent.
BARE_PAIR = {
    "chainId": "solana",
    "dexId": "pumpfun",
    "pairAddress": "PAIR2",
    "baseToken": {"address": "MINT2", "name": "n", "symbol": "s"},
    "txns": {"m5": {"buys": 3, "sells": 0}},
    "volume": {"m5": 50},
}


def test_pair_to_candidate_full():
    c = dex.pair_to_candidate(FULL_PAIR)
    assert c.mint == "MINT1" and c.symbol == "DWL"
    assert c.price_usd == 0.0004123          # parsed from string
    assert c.liquidity_usd == 65000.5
    assert c.pair_created_at_ms == 1756389000000
    assert c.buys_h1 == 700 and c.sells_h1 == 300
    assert c.has_website and c.has_socials and c.boosted


def test_pair_to_candidate_bare_new_pair():
    c = dex.pair_to_candidate(BARE_PAIR)
    assert c.mint == "MINT2"
    assert c.price_usd is None and c.liquidity_usd is None
    assert c.pair_created_at_ms is None and c.buys_h1 is None
    assert not c.boosted


def test_pair_to_candidate_no_base_address_is_none():
    assert dex.pair_to_candidate({"baseToken": {}}) is None


def test_get_candidates_picks_deepest_pair(monkeypatch):
    small = dict(FULL_PAIR, pairAddress="PAIRX", liquidity={"usd": 100})
    monkeypatch.setattr(dex, "get_json", lambda url: [small, FULL_PAIR, BARE_PAIR])
    best = dex.get_candidates(["MINT1", "MINT2"])
    assert best["MINT1"].pair_address == "PAIR1"  # 65000 beats 100
    assert best["MINT2"].pair_address == "PAIR2"


def test_discover_mints_filters_chain_and_merges_boost(monkeypatch):
    def fake(url):
        if "profiles" in url:
            return [{"chainId": "solana", "tokenAddress": "M1"},
                    {"chainId": "ethereum", "tokenAddress": "0xno"}]
        return [{"chainId": "solana", "tokenAddress": "M1", "amount": 30},
                {"chainId": "solana", "tokenAddress": "M2", "amount": 10}]
    monkeypatch.setattr(dex, "get_json", fake)
    found = dex.discover_mints()
    assert found == {"M1": True, "M2": True}


RUGCHECK_REPORT = {
    "mint": "MINT1",
    "token": {"mintAuthority": None, "freezeAuthority": None, "supply": 10**15, "decimals": 6},
    "tokenMeta": {"name": "Dog Wif Laser", "symbol": "DWL", "mutable": False},
    "topHolders": [
        {"address": "VAULT_A", "owner": "RAY_AUTH", "pct": 40.0, "insider": False},
        {"address": "W1", "owner": "W1o", "pct": 6.0, "insider": True},
        {"address": "W2", "owner": "W2o", "pct": 3.0, "insider": False},
        {"address": "W3", "owner": "W3o", "pct": 2.0, "insider": False},
    ],
    "knownAccounts": {"VAULT_A": {"name": "Raydium Pool", "type": "AMM"}},
    "risks": [{"name": "Top 10 holders high ownership", "value": "", "description": "d",
               "score": 9296, "level": "warn"}],
    "score": 9296,
    "score_normalised": 22,
    "rugged": False,
    "totalHolders": 640,
    "markets": [
        {"lp": {"lpLockedPct": 100.0, "baseUSD": 30000, "quoteUSD": 30000}},
        {"lp": {"lpLockedPct": 5.0, "baseUSD": 10, "quoteUSD": 10}},
    ],
}


def test_rugcheck_normalization(monkeypatch):
    monkeypatch.setattr(rc, "_throttled_get", lambda url: RUGCHECK_REPORT)
    s = rc.get_safety("MINT1")
    assert s.mint_authority_active is False       # null = renounced
    assert s.freeze_authority_active is False
    assert s.lp_locked_pct == 100.0               # deepest market wins, not the $20 one
    assert s.top10_holder_pct == 11.0             # AMM vault excluded (6+3+2)
    assert s.max_single_holder_pct == 6.0
    assert s.insider_pct == 6.0
    assert s.total_holders == 640
    assert s.rugcheck_score == 22.0               # normalised, NOT raw 9296
    assert s.risk_names == ["Top 10 holders high ownership"]
    assert not s.rugged_flag


def test_rugcheck_active_authority_detected(monkeypatch):
    bad = dict(RUGCHECK_REPORT, token={"mintAuthority": "SomeBase58Key", "freezeAuthority": None})
    monkeypatch.setattr(rc, "_throttled_get", lambda url: bad)
    assert rc.get_safety("M").mint_authority_active is True


def test_rugcheck_unindexed_mint_returns_none(monkeypatch):
    def boom(url):
        raise ApiError("GET url -> HTTP 400: token not found")
    monkeypatch.setattr(rc, "_throttled_get", boom)
    assert rc.get_safety("UNKNOWN") is None


def test_rugcheck_empty_report_all_unknown(monkeypatch):
    monkeypatch.setattr(rc, "_throttled_get", lambda url: {"mint": "M"})
    s = rc.get_safety("M")
    assert s.mint_authority_active is None
    assert s.lp_locked_pct is None and s.top10_holder_pct is None


def test_new_tokens_feed_defensive(monkeypatch):
    monkeypatch.setattr(rc, "_throttled_get",
                        lambda url: [{"mint": "A"}, {"nope": 1}, "junk", {"mint": "B"}])
    assert rc.new_token_mints() == ["A", "B"]
