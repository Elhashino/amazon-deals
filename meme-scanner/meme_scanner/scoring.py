"""Momentum scoring for coins that survived the kill-filters.

0-100, built from observable behaviour, not vibes. The philosophy: reward
DISTRIBUTED interest (many unique buyers, growing holder base, balanced
buy pressure) over raw volume, which wash-trading bots fake for pennies.
A high score still isn't a prediction — it means "if any of today's
launches has legs, it looks like this one."
"""

from __future__ import annotations

import math

from .models import BundleReport, Candidate, SafetyReport, Verdict

# Component weights — must sum to 100.
W_BUY_PRESSURE = 25.0
W_VOL_ACCEL = 15.0
W_UNIQUE_DEMAND = 25.0
W_HOLDER_BASE = 15.0
W_LIQ_HEALTH = 10.0
W_IDENTITY = 10.0


def _saturating(value: float, target: float) -> float:
    """0..1, hits ~0.63 at target, asymptotes to 1. Smooth, no cliffs."""
    if target <= 0 or value <= 0:
        return 0.0
    return 1.0 - math.exp(-value / target)


def score(cand: Candidate, safety: SafetyReport, unique_buyers_h1: int | None) -> Verdict:
    parts: dict[str, float] = {}

    # 1. Buy pressure: fraction of trades that are buys, h1-weighted with m5.
    def pressure(buys, sells):
        if buys is None or sells is None or buys + sells < 10:
            return None
        return buys / (buys + sells)

    p_h1 = pressure(cand.buys_h1, cand.sells_h1)
    p_m5 = pressure(cand.buys_m5, cand.sells_m5)
    vals = [v for v in (p_h1, p_h1, p_m5) if v is not None]  # h1 counts double
    if vals:
        avg = sum(vals) / len(vals)
        # 50% buys = neutral (0 pts), 70%+ = strong (full pts), <50% = 0.
        parts["buy_pressure"] = W_BUY_PRESSURE * max(0.0, min(1.0, (avg - 0.5) / 0.2))
    else:
        parts["buy_pressure"] = 0.0

    # 2. Volume acceleration: is the last 5 min running hotter than the hour?
    if cand.vol_m5 and cand.vol_h1 and cand.vol_h1 > 0:
        accel = (cand.vol_m5 * 12.0) / cand.vol_h1  # >1 = accelerating
        parts["vol_accel"] = W_VOL_ACCEL * max(0.0, min(1.0, (accel - 0.8) / 1.2))
    else:
        parts["vol_accel"] = 0.0

    # 3. Unique demand: distinct buyers beats volume — bots fake volume,
    #    faking hundreds of distinct funded wallets costs real money.
    if unique_buyers_h1 is not None:
        parts["unique_demand"] = W_UNIQUE_DEMAND * _saturating(unique_buyers_h1, 150.0)
    else:
        # Unknown (RPC budget ran out) — give half credit from tx counts.
        txs = (cand.buys_h1 or 0) + (cand.sells_h1 or 0)
        parts["unique_demand"] = 0.5 * W_UNIQUE_DEMAND * _saturating(txs, 400.0)

    # 4. Holder base size.
    if safety.total_holders is not None:
        parts["holder_base"] = W_HOLDER_BASE * _saturating(safety.total_holders, 500.0)
    else:
        parts["holder_base"] = 0.0

    # 5. Liquidity health: liq/FDV in a sane band. Too thin = exit door is
    #    tiny; absurdly high = probably nothing else real about the token.
    if cand.liquidity_usd and cand.fdv_usd and cand.fdv_usd > 0:
        ratio = cand.liquidity_usd / cand.fdv_usd
        parts["liq_health"] = W_LIQ_HEALTH if 0.05 <= ratio <= 0.60 else W_LIQ_HEALTH * 0.3
    else:
        parts["liq_health"] = 0.0

    # 6. Identity: a real site/socials is weak evidence of effort. A paid
    #    boost is NOT identity — it's marketing spend, worth a nod at most.
    ident = 0.0
    if cand.has_website:
        ident += 0.5
    if cand.has_socials:
        ident += 0.4
    if cand.boosted:
        ident += 0.1
    parts["identity"] = W_IDENTITY * min(1.0, ident)

    total = round(sum(parts.values()), 1)
    strongest = max(parts, key=parts.get)
    summary = f"strongest signal: {strongest.replace('_', ' ')}"
    return Verdict(score=total, components={k: round(v, 1) for k, v in parts.items()}, summary=summary)
