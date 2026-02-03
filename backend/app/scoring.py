from __future__ import annotations

"""Deal + demand scoring.

We compute two related scores:
  - score: price-centric "how good is the deal" score (0-100).
  - hot_score: blended score that also prioritises "most wanted" items (0-100).

The demand signals are derived from Keepa fields we already request in `keepa_client.py`:
  - SALES (sales rank history)
  - RATING (0-50, e.g. 45 == 4.5 stars)
  - COUNT_REVIEWS (review count history)
"""

from dataclasses import dataclass
from typing import Any, Iterable
import math
import numpy as np


@dataclass
class DealMetrics:
    # Price-centric
    price_current: float | None
    price_median_90d: float | None
    discount_pct_90d: float | None
    confidence: float | None
    score: float | None

    # Demand-centric ("most wanted")
    sales_rank_current: float | None
    sales_rank_median_30d: float | None
    sales_rank_trend_30d: float | None  # positive == improving (rank getting lower)
    rank_drops_7d: int | None
    rating: float | None  # stars (0-5)
    review_count: int | None
    demand_score: float | None
    hot_score: float | None


def _as_float_array(x: Any) -> np.ndarray:
    if x is None:
        return np.asarray([], dtype=float)
    try:
        return np.asarray(x, dtype=float)
    except Exception:
        return np.asarray([], dtype=float)


def _as_time_array(x: Any) -> np.ndarray:
    # keepa library returns numpy.datetime64 array when converted via np.asarray
    if x is None:
        return np.asarray([])
    try:
        return np.asarray(x)
    except Exception:
        return np.asarray([])


def _last_valid(series: np.ndarray, *, invalid_leq: float | None = None) -> float | None:
    if series.size == 0:
        return None
    s = series[~np.isnan(series)]
    if s.size == 0:
        return None
    if invalid_leq is not None:
        s = s[s > invalid_leq]
        if s.size == 0:
            return None
    return float(s[-1])


def _subset_last_days(values: np.ndarray, times: np.ndarray, days: int) -> np.ndarray:
    if values.size == 0 or times.size == 0:
        return np.asarray([], dtype=float)
    end = times[-1]
    try:
        start = end - np.timedelta64(days, "D")
    except Exception:
        return np.asarray([], dtype=float)
    mask = times >= start
    v = values[mask]
    v = v[~np.isnan(v)]
    return v


def _median_last_days(values: np.ndarray, times: np.ndarray, days: int, *, invalid_leq: float | None = None) -> float | None:
    v = _subset_last_days(values, times, days)
    if invalid_leq is not None:
        v = v[v > invalid_leq]
    if v.size == 0:
        return None
    return float(np.median(v))


def _volatility(values: np.ndarray, times: np.ndarray, days: int = 90) -> float | None:
    """Robust relative volatility using MAD/median on the last N days."""
    v = _subset_last_days(values, times, days)
    if v.size < 5:
        return None
    med = float(np.median(v))
    if med <= 0:
        return None
    mad = float(np.median(np.abs(v - med)))
    return mad / med


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return float(max(lo, min(hi, x)))


def _rank_component(rank: float | None) -> float:
    """Map sales rank (lower is better) to a 0-100 signal using a log scale."""
    if rank is None or rank <= 0:
        return 0.0
    # Examples:
    # 1 -> 100, 100 -> 60, 1k -> 40, 10k -> 20, 100k -> 0
    return _clamp(100.0 - 20.0 * math.log10(rank + 1.0))


def _drops_component(drops_7d: int | None) -> float:
    if drops_7d is None or drops_7d <= 0:
        return 0.0
    # 10+ meaningful drops in 7d == very strong movement
    return _clamp(drops_7d * 10.0)


def _reviews_component(reviews: int | None) -> float:
    if reviews is None or reviews <= 0:
        return 0.0
    # log scaling: 10 -> ~20, 100 -> 40, 1k -> 60, 10k -> 80, 100k -> 100
    return _clamp(20.0 * math.log10(reviews + 1.0))


def _rating_component(rating_stars: float | None) -> float:
    if rating_stars is None or rating_stars <= 0:
        return 0.0
    # 3.5 -> 0, 5.0 -> 100
    return _clamp(((rating_stars - 3.5) / 1.5) * 100.0)


def _count_rank_drops_last_days(ranks: np.ndarray, times: np.ndarray, days: int) -> int | None:
    """Count number of times rank improves (numeric value decreases) in last N days."""
    if ranks.size == 0 or times.size == 0:
        return None
    end = times[-1]
    start = end - np.timedelta64(days, "D")
    mask = times >= start
    r = ranks[mask]
    if r.size < 2:
        return 0
    # Remove NaNs and invalid ranks (<= 0)
    r = r[~np.isnan(r)]
    r = r[r > 0]
    if r.size < 2:
        return 0
    diffs = np.diff(r)
    # a "drop" means rank got smaller -> negative diff
    return int(np.sum(diffs < 0))


def compute_deal_metrics(product: dict[str, Any]) -> DealMetrics:
    data = product.get("data", {}) or {}

    # ----- Price-centric (existing logic) -----
    candidates = [
        ("NEW", data.get("NEW"), data.get("NEW_time")),
        ("AMAZON", data.get("AMAZON"), data.get("AMAZON_time")),
    ]
    values = None
    times = None
    for _key, v, t in candidates:
        if v is not None and t is not None and len(v) > 0 and len(t) > 0:
            values = _as_float_array(v)
            times = _as_time_array(t)
            break
    if values is None or times is None:
        return DealMetrics(
            None, None, None, None, None,
            None, None, None, None, None, None, None, None, None
        )

    price_current = _last_valid(values, invalid_leq=0)
    median_90d = _median_last_days(values, times, 90, invalid_leq=0)

    discount = None
    if price_current is not None and median_90d is not None and median_90d > 0:
        discount = (median_90d - price_current) / median_90d

    vol = _volatility(values, times, 90)
    confidence = None
    if vol is not None:
        # 0% vol => 100 confidence, 30%+ vol => ~0 confidence
        confidence = float(np.clip(1.0 - (vol / 0.30), 0.0, 1.0) * 100.0)

    score = None
    if discount is not None and confidence is not None:
        score = float(np.clip(discount, 0.0, 1.0) * 70.0 + (confidence / 100.0) * 30.0)

    # ----- Demand-centric (new) -----
    sales = _as_float_array(data.get("SALES"))
    sales_t = _as_time_array(data.get("SALES_time"))
    sales_rank_current = _last_valid(sales, invalid_leq=0)
    sales_rank_median_30d = _median_last_days(sales, sales_t, 30, invalid_leq=0)

    sales_rank_trend_30d = None
    if sales_rank_current is not None and sales_rank_median_30d is not None and sales_rank_median_30d > 0:
        # positive => improving (rank number lower than typical)
        sales_rank_trend_30d = float((sales_rank_median_30d - sales_rank_current) / sales_rank_median_30d)
        sales_rank_trend_30d = float(np.clip(sales_rank_trend_30d, -1.0, 1.0))

    rank_drops_7d = _count_rank_drops_last_days(sales, sales_t, 7)

    rating_raw = _as_float_array(data.get("RATING"))
    rating_t = _as_time_array(data.get("RATING_time"))
    rating_current_raw = _last_valid(rating_raw, invalid_leq=0)
    rating = (rating_current_raw / 10.0) if rating_current_raw is not None else None

    reviews_raw = _as_float_array(data.get("COUNT_REVIEWS"))
    reviews_t = _as_time_array(data.get("COUNT_REVIEWS_time"))
    reviews_current = _last_valid(reviews_raw, invalid_leq=-1)
    review_count = int(reviews_current) if reviews_current is not None and reviews_current >= 0 else None

    # demand score on 0-100 scale
    demand_score = None
    if any(x is not None for x in (sales_rank_current, rank_drops_7d, rating, review_count)):
        demand_score = (
            0.45 * _rank_component(sales_rank_current)
            + 0.20 * _drops_component(rank_drops_7d)
            + 0.20 * _reviews_component(review_count)
            + 0.15 * _rating_component(rating)
        )
        demand_score = _clamp(demand_score)

    hot_score = None
    if score is not None and confidence is not None and demand_score is not None:
        blended = (0.60 * score) + (0.40 * demand_score)
        # keep "deal confidence" relevant: low confidence is less trustworthy
        hot_score = float(np.clip(blended * (0.50 + 0.50 * (confidence / 100.0)), 0.0, 100.0))

    return DealMetrics(
        price_current,
        median_90d,
        discount,
        confidence,
        score,
        sales_rank_current,
        sales_rank_median_30d,
        sales_rank_trend_30d,
        rank_drops_7d,
        rating,
        review_count,
        demand_score,
        hot_score,
    )
