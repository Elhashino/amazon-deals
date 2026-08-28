"""RugCheck adapter.

Verified quirks (2026): anonymous limit is ~1 req/sec, so calls are
throttled locally. An unindexed mint returns HTTP 400 (not 404). Scores:
score_normalised is 0-100 and HIGHER = MORE RISK; the raw `score` field is
an unbounded point sum — never gate on it. LP lock lives top-level on the
summary endpoint but per-market (markets[i].lp.lpLockedPct) on the full
report. Nearly every field can be null on a brand-new token.
"""

from __future__ import annotations

import time

from ..models import SafetyReport
from .http import ApiError, get_json

BASE = "https://api.rugcheck.xyz/v1"
MIN_INTERVAL = 1.2  # seconds between calls; anonymous tier is ~1 req/sec

_last_call = 0.0


def _throttled_get(url: str):
    global _last_call
    wait = MIN_INTERVAL - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()
    return get_json(url)


def new_token_mints() -> list[str]:
    """Discovery feed #3: RugCheck's recently-detected launches."""
    try:
        items = _throttled_get(f"{BASE}/stats/new_tokens")
    except ApiError as exc:
        print(f"  [WARN] RugCheck new_tokens: {exc}")
        return []
    mints = []
    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("mint"), str):
                mints.append(item["mint"])
    return mints


def _authority_active(report: dict, key: str) -> bool | None:
    """null = renounced (safe), string = still active, missing = unknown."""
    token = report.get("token")
    if isinstance(token, dict) and key in token:
        return token[key] is not None
    if key in report:  # duplicated top-level in the full report
        return report[key] is not None
    return None


def _lp_locked_pct(report: dict) -> float | None:
    """Take the LP lock of the deepest market — that's where a rug would exit."""
    markets = report.get("markets")
    if not isinstance(markets, list) or not markets:
        return None
    best_pct, best_depth = None, -1.0
    for market in markets:
        lp = (market or {}).get("lp") or {}
        pct = lp.get("lpLockedPct")
        if pct is None:
            continue
        try:
            depth = float(lp.get("baseUSD") or 0) + float(lp.get("quoteUSD") or 0)
            pct = float(pct)
        except (TypeError, ValueError):
            continue
        if depth > best_depth:
            best_pct, best_depth = pct, depth
    return best_pct


def _is_infra_account(entry: dict, known_accounts: dict) -> bool:
    """AMM vaults / pool accounts shouldn't count as 'holders' for
    concentration — a healthy pool legitimately holds a big slice."""
    for addr in (entry.get("address"), entry.get("owner")):
        meta = known_accounts.get(addr) if isinstance(addr, str) else None
        if isinstance(meta, dict):
            text = f"{meta.get('name', '')} {meta.get('type', '')}".lower()
            if any(word in text for word in ("amm", "pool", "vault", "raydium", "meteora", "orca", "pumpswap", "lp")):
                return True
    return False


def get_safety(mint: str) -> SafetyReport | None:
    """Fetch and normalize the full report. None = not indexed yet (defer)."""
    try:
        report = _throttled_get(f"{BASE}/tokens/{mint}/report")
    except ApiError as exc:
        if "HTTP 400" in str(exc):  # unknown/unindexed mint
            return None
        print(f"  [WARN] RugCheck report {mint[:8]}: {exc}")
        return None
    if not isinstance(report, dict):
        return None

    known_accounts = report.get("knownAccounts") or {}
    holders = [h for h in (report.get("topHolders") or []) if isinstance(h, dict)]
    real_holders = [h for h in holders if not _is_infra_account(h, known_accounts)]

    top10 = max_single = None
    pcts = sorted((float(h.get("pct") or 0) for h in real_holders), reverse=True)
    if pcts:
        top10 = sum(pcts[:10])
        max_single = pcts[0]

    insider_pct = None
    insider_from_holders = sum(float(h.get("pct") or 0) for h in real_holders if h.get("insider"))
    if insider_from_holders > 0:
        insider_pct = insider_from_holders

    risks = [r for r in (report.get("risks") or []) if isinstance(r, dict)]
    score_norm = report.get("score_normalised")

    return SafetyReport(
        mint_authority_active=_authority_active(report, "mintAuthority"),
        freeze_authority_active=_authority_active(report, "freezeAuthority"),
        lp_locked_pct=_lp_locked_pct(report),
        top10_holder_pct=top10,
        max_single_holder_pct=max_single,
        insider_pct=insider_pct,
        total_holders=report.get("totalHolders"),
        rugcheck_score=float(score_norm) if score_norm is not None else None,
        risk_names=[str(r.get("name")) for r in risks if r.get("name")],
        rugged_flag=bool(report.get("rugged")),
    )
