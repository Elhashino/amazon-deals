"""DexScreener adapter.

Discovery reality check (verified 2026): there is NO public "newest pairs"
endpoint — the new-pairs page is fed by a private websocket. The standard
free approach is polling the token-profiles and token-boosts feeds (tokens
whose teams paid DexScreener — which for meme coins is nearly all serious
launches) and enriching mints in batches of 30 via /tokens/v1.

Envelope quirks that bite: /tokens/v1 returns a BARE ARRAY, /latest/dex/pairs
wraps in {schemaVersion, pairs} where pairs can be null. priceUsd is a
STRING. pairCreatedAt is epoch MILLISECONDS. Almost every field is optional
on a minutes-old pair, so it's .get() all the way down.
"""

from __future__ import annotations

from ..models import Candidate
from .http import ApiError, get_json

BASE = "https://api.dexscreener.com"
BATCH = 30  # documented max mints per /tokens/v1 call


def discover_mints() -> dict[str, bool]:
    """Poll the discovery feeds. Returns {mint: boosted} for Solana tokens."""
    found: dict[str, bool] = {}
    for path, boosted in (("/token-profiles/latest/v1", False), ("/token-boosts/latest/v1", True)):
        try:
            items = get_json(BASE + path)
        except ApiError as exc:
            print(f"  [WARN] DexScreener {path}: {exc}")
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict) or item.get("chainId") != "solana":
                continue
            mint = item.get("tokenAddress")
            if isinstance(mint, str) and mint:
                found[mint] = found.get(mint, False) or boosted
    return found


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pair_to_candidate(pair: dict) -> Candidate | None:
    base_token = pair.get("baseToken") or {}
    mint = base_token.get("address")
    if not mint:
        return None
    txns = pair.get("txns") or {}
    m5, h1 = txns.get("m5") or {}, txns.get("h1") or {}
    volume = pair.get("volume") or {}
    change = pair.get("priceChange") or {}
    liquidity = pair.get("liquidity") or {}
    info = pair.get("info") or {}
    boosts = pair.get("boosts") or {}
    return Candidate(
        mint=mint,
        symbol=base_token.get("symbol") or "",
        name=base_token.get("name") or "",
        pair_address=pair.get("pairAddress") or "",
        dex_id=pair.get("dexId") or "",
        url=pair.get("url") or "",
        price_usd=_to_float(pair.get("priceUsd")),  # string in the API
        liquidity_usd=_to_float(liquidity.get("usd")),
        fdv_usd=_to_float(pair.get("fdv")),
        market_cap_usd=_to_float(pair.get("marketCap")),
        pair_created_at_ms=_to_int(pair.get("pairCreatedAt")),  # epoch ms
        vol_m5=_to_float(volume.get("m5")),
        vol_h1=_to_float(volume.get("h1")),
        vol_h24=_to_float(volume.get("h24")),
        buys_m5=_to_int(m5.get("buys")),
        sells_m5=_to_int(m5.get("sells")),
        buys_h1=_to_int(h1.get("buys")),
        sells_h1=_to_int(h1.get("sells")),
        price_change_m5=_to_float(change.get("m5")),
        price_change_h1=_to_float(change.get("h1")),
        has_website=bool(info.get("websites")),
        has_socials=bool(info.get("socials")),
        boosted=bool(boosts.get("active")),
    )


def get_candidates(mints: list[str]) -> dict[str, Candidate]:
    """Batch-enrich mints -> best Candidate per mint (highest-liquidity pair)."""
    best: dict[str, Candidate] = {}
    for i in range(0, len(mints), BATCH):
        chunk = mints[i : i + BATCH]
        try:
            pairs = get_json(f"{BASE}/tokens/v1/solana/{','.join(chunk)}")
        except ApiError as exc:
            print(f"  [WARN] DexScreener tokens/v1 batch failed: {exc}")
            continue
        if not isinstance(pairs, list):  # bare array, no wrapper
            continue
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            cand = pair_to_candidate(pair)
            if cand is None:
                continue
            current = best.get(cand.mint)
            if current is None or (cand.liquidity_usd or 0) > (current.liquidity_usd or 0):
                best[cand.mint] = cand
    return best


def get_candidate(mint: str) -> Candidate | None:
    """Fresh look at one token (used by the rug-watch re-checks)."""
    return get_candidates([mint]).get(mint)
