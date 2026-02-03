from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import select, desc
from .db import SessionLocal
from .models import Deal, Product
from .config import settings

router = APIRouter(prefix="/api")

def amazon_url(asin: str) -> str:
    base = f"https://www.amazon.co.uk/dp/{asin}"
    return f"{base}?tag={settings.AMAZON_ASSOC_TAG}" if settings.AMAZON_ASSOC_TAG else base

@router.get("/deals")
def list_deals(
    category: str | None = None,
    sort: str = Query("hot", pattern="^(hot|deal)$"),
    limit: int = Query(50, ge=1, le=200),
):
    with SessionLocal() as db:
        q = select(Deal, Product).join(Product, Product.asin == Deal.asin).where(Deal.is_active == True)
        if category:
            q = q.where(Deal.category_slug == category)
        if sort == "deal":
            q = q.order_by(Deal.score.desc().nullslast())
        else:
            # "hot" == blend of deal quality + demand
            q = q.order_by(Deal.hot_score.desc().nullslast(), Deal.score.desc().nullslast())
        q = q.limit(limit)
        rows = db.execute(q).all()
        items = []
        for d, p in rows:
            items.append({
                "asin": d.asin,
                "category": d.category_slug,
                "title": p.title,
                "brand": p.brand,
                "price_current": d.price_current,
                "price_median_90d": d.price_median_90d,
                "discount_pct_90d": d.discount_pct_90d,
                "confidence": d.confidence,
                "score": d.score,
                "demand_score": d.demand_score,
                "hot_score": d.hot_score,
                "sales_rank_current": d.sales_rank_current,
                "sales_rank_trend_30d": d.sales_rank_trend_30d,
                "rank_drops_7d": d.rank_drops_7d,
                "rating": d.rating,
                "review_count": d.review_count,
                "published_at": d.published_at.isoformat(),
                "amazon_url": amazon_url(d.asin),
            })
        return {"items": items}

@router.get("/deal/{asin}")
def get_deal(asin: str):
    if len(asin) != 10:
        raise HTTPException(status_code=400, detail="Invalid ASIN.")
    with SessionLocal() as db:
        q = (select(Deal, Product)
             .join(Product, Product.asin == Deal.asin)
             .where(Deal.asin == asin, Deal.is_active == True)
             .order_by(desc(Deal.score))
             .limit(1))
        row = db.execute(q).first()
        if not row:
            raise HTTPException(status_code=404, detail="Deal not found.")
        d, p = row
        return {
            "asin": d.asin,
            "title": p.title,
            "brand": p.brand,
            "category": d.category_slug,
            "price_current": d.price_current,
            "price_median_90d": d.price_median_90d,
            "discount_pct_90d": d.discount_pct_90d,
            "confidence": d.confidence,
            "score": d.score,
            "demand_score": d.demand_score,
            "hot_score": d.hot_score,
            "sales_rank_current": d.sales_rank_current,
            "sales_rank_trend_30d": d.sales_rank_trend_30d,
            "rank_drops_7d": d.rank_drops_7d,
            "rating": d.rating,
            "review_count": d.review_count,
            "published_at": d.published_at.isoformat(),
            "amazon_url": amazon_url(d.asin),
        }
