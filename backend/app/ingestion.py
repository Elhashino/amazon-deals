from __future__ import annotations

import os
from datetime import datetime, timezone
from sqlalchemy import select, text

from .db import SessionLocal, engine, Base
from .models import Product, PriceSnapshot, Deal
from .keepa_client import KeepaClient
from .scoring import compute_deal_metrics
from .config import settings


def _norm(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "") if ch.isalnum() or ch.isspace()).strip()


def resolve_root_category_ids(roots: dict[str, dict]) -> dict[str, int]:
    """
    roots appears to be a dict keyed by catId (as string) -> { catId, name, ... }
    We locate a curated set of root categories so we do NOT explode runtime/tokens.
    """
    def find_id(keywords: list[str]) -> int | None:
        for v in roots.values():
            name = _norm(str(v.get("name", "")))
            if all(k in name for k in keywords):
                try:
                    return int(v.get("catId"))
                except Exception:
                    continue
        return None

    targets = {
        # existing behaviour
        "home_kitchen": ["home", "kitchen"],
        "diy_tools": ["diy", "tools"],
        "toys_games": ["toys"],
        "electronics": ["electronics"],

        # expanded coverage (matches your config additions)
        "beauty": ["beauty"],
        "health": ["health"],
        "grocery": ["grocery"],
        "pet": ["pet"],
        "sports": ["sports"],
        "baby": ["baby"],
        "automotive": ["automotive"],
        "garden": ["garden", "outdoor"],
    }

    out: dict[str, int] = {}
    for key, kws in targets.items():
        cid = find_id(kws)
        if cid:
            out[key] = cid
    return out


def categorize(product: dict, root_name: str) -> str:
    root = _norm(root_name)

    # Expanded buckets
    if "beauty" in root:
        return "beauty"
    if "health" in root:
        return "health"
    if "grocery" in root or "food" in root:
        return "grocery"
    if "pet" in root:
        return "pet"
    if "garden" in root or "lawn" in root or ("outdoor" in root and "sports" not in root):
        return "garden"
    if "sports" in root or "outdoors" in root:
        return "sports"
    if "baby" in root:
        return "baby"
    if "automotive" in root:
        return "automotive"

    # Existing behaviour
    if "home" in root and "kitchen" in root:
        tree = product.get("categoryTree") or []
        names = " ".join(_norm(n.get("name", "")) for n in tree if isinstance(n, dict))
        if any(k in names for k in ["kitchen", "dining", "cookware", "bakeware", "utensils", "appliances"]):
            return "kitchen"
        return "home"

    if "diy" in root or "tools" in root:
        return "diy"

    if "toys" in root or "games" in root:
        return "toys"

    if "electronics" in root:
        return "electrical"

    return "home"


def min_discount_for_category(cat: str) -> float:
    return {
        "home": settings.MIN_DISCOUNT_HOME,
        "kitchen": settings.MIN_DISCOUNT_KITCHEN,
        "diy": settings.MIN_DISCOUNT_DIY,
        "electrical": settings.MIN_DISCOUNT_ELECTRICAL,
        "toys": settings.MIN_DISCOUNT_TOYS,

        # New slugs (you said you changed/added these)
        "grocery": settings.MIN_DISCOUNT_GROCERY,
        "health": settings.MIN_DISCOUNT_HEALTH,
        "beauty": settings.MIN_DISCOUNT_BEAUTY,
        "pet": settings.MIN_DISCOUNT_PET,
        "sports": settings.MIN_DISCOUNT_SPORTS,
        "baby": settings.MIN_DISCOUNT_BABY,
        "automotive": settings.MIN_DISCOUNT_AUTOMOTIVE,
        "garden": settings.MIN_DISCOUNT_HOME,  # Use same threshold as home
    }.get(cat, settings.MIN_DISCOUNT_HOME)


def _extract_image_url(p: dict) -> str:
    """
    Keepa commonly provides imagesCSV. Build a usable Amazon image URL.
    If not available, return empty string (NOT NULL safe due to model default).
    """
    images_csv = p.get("imagesCSV") or ""
    if isinstance(images_csv, str) and images_csv.strip():
        first = images_csv.split(",")[0].strip()
        if first:
            return f"https://m.media-amazon.com/images/I/{first}"
    # Some clients may provide direct fields
    for key in ("imageUrl", "imageURL", "image", "image_url"):
        v = p.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _extract_asin(obj: dict) -> str | None:
    """Best-effort ASIN extraction (Keepa can be inconsistent across endpoints)."""
    if not isinstance(obj, dict):
        return None
    for key in ("asin", "ASIN", "Asin", "productCode"):
        v = obj.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if len(s) == 10:
            return s
    return None


def run_ingestion_once():
    Base.metadata.create_all(bind=engine)
    client = KeepaClient()

    roots = client.uk_root_categories() or {}
    root_ids = resolve_root_category_ids(roots)

    # keep runtime bounded: only ingest the curated root ids we found
    unique_root_ids = sorted(set(root_ids.values()))
    include_lists: list[list[int]]
    if unique_root_ids:
        include_lists = [[cid] for cid in unique_root_ids]
    else:
        # fallback: old behaviour if roots parsing fails
        include_lists = [[int(v.get("catId"))] for v in roots.values() if v.get("catId")]

    # CRITICAL FIX: Record the run start time BEFORE opening DB session
    # We use published_at (timestamp without tz, naive) to track which deals belong to this run
    # The ingested_at field is managed by a DB trigger and will have unique microsecond timestamps
    run_started_at = datetime.now(timezone.utc).replace(tzinfo=None)  # naive for timestamp without tz
    
    print(f"Starting ingestion run at: {run_started_at}")

    with SessionLocal() as db:
        # Deal retention policy
        # Recommended:
        #   PURGE_DEALS_ON_START=0  (keep old deals while this run is executing)
        #   PURGE_DEALS_ON_END=1    (after a successful run, delete all older deals)
        #
        # This prevents the "empty DB" problem if ingestion crashes mid-run.
        purge_start = os.getenv("PURGE_DEALS_ON_START", "0").strip().lower() not in {"0", "false", "no"}
        purge_end = os.getenv("PURGE_DEALS_ON_END", "1").strip().lower() not in {"0", "false", "no"}

        if purge_start:
            print("Purging all deals at start (PURGE_DEALS_ON_START=1)")
            db.execute(text("DELETE FROM deals;"))
            db.commit()
        else:
            print("Keeping existing deals during run (PURGE_DEALS_ON_START=0)")

        deals_processed = 0

        for include_cats in include_lists:
            for page in range(settings.DEALS_PAGES_PER_ROOT_CATEGORY):
                deals = client.deals(include_categories=include_cats, page=page)
                deal_rows = deals.get("dr", []) or deals.get("deals", []) or []

                asins: list[str] = []
                for d in deal_rows:
                    asin = d.get("asin") or d.get("ASIN") or d.get("productCode")
                    if isinstance(asin, str) and len(asin) == 10:
                        asins.append(asin)

                if not asins:
                    continue

                # Preserve order but remove duplicates (can happen across pages)
                asins = list(dict.fromkeys(asins))

                products = client.products(asins) or []

                # Keepa can return products out-of-order or omit some ASINs. Map by ASIN.
                product_by_asin: dict[str, dict] = {}
                for p in products:
                    if not isinstance(p, dict):
                        continue
                    pasin = _extract_asin(p)
                    if pasin:
                        product_by_asin[pasin] = p

                for asin in asins:
                    p = product_by_asin.get(asin)
                    if p is None:
                        continue

                    title = (p.get("title") or "")[:600]
                    brand = (p.get("brand") or "")[:200]
                    image_url = _extract_image_url(p)

                    root_cat_id = p.get("rootCategory")
                    root_cat_name = ""
                    if root_cat_id and str(root_cat_id) in roots:
                        root_cat_name = str(roots[str(root_cat_id)].get("name", ""))[:200]
                    elif p.get("rootCategoryName"):
                        root_cat_name = str(p.get("rootCategoryName"))[:200]

                    cat_slug = categorize(p, root_cat_name)

                    metrics = compute_deal_metrics(p)
                    if metrics.discount_pct_90d is None or metrics.score is None:
                        continue

                    if metrics.discount_pct_90d < min_discount_for_category(cat_slug):
                        continue

                    # Ensure Product exists first (FK safety)
                    prod = db.get(Product, asin)
                    is_new_prod = prod is None
                    if is_new_prod:
                        prod = Product(asin=asin)
                        db.add(prod)

                    prod.title = title
                    prod.brand = brand
                    prod.image_url = image_url or ""  # NOT NULL safe
                    prod.root_category_id = int(root_cat_id) if root_cat_id else None
                    prod.root_category_name = root_cat_name
                    prod.last_seen_at = run_started_at

                    # If it's new, flush so FK is satisfied immediately
                    if is_new_prod:
                        db.flush()

                    # Snapshot (captured_at is the correct field)
                    db.add(
                        PriceSnapshot(
                            asin=asin,
                            captured_at=run_started_at,
                            price_current=metrics.price_current,
                            price_median_90d=metrics.price_median_90d,
                            discount_pct_90d=metrics.discount_pct_90d,
                            confidence=metrics.confidence,
                            score=metrics.score,
                        )
                    )

                    existing = db.execute(
                        select(Deal).where(
                            Deal.asin == asin,
                            Deal.category_slug == cat_slug,
                            Deal.is_active == True,
                        )
                    ).scalar_one_or_none()

                    if existing:
                        # CRITICAL FIX: Use run_started_at for published_at to track this run
                        existing.published_at = run_started_at
                        existing.price_current = metrics.price_current
                        existing.price_median_90d = metrics.price_median_90d
                        existing.discount_pct_90d = metrics.discount_pct_90d
                        existing.confidence = metrics.confidence
                        existing.score = metrics.score

                        existing.sales_rank_current = metrics.sales_rank_current
                        existing.sales_rank_median_30d = metrics.sales_rank_median_30d
                        existing.sales_rank_trend_30d = metrics.sales_rank_trend_30d
                        existing.rank_drops_7d = metrics.rank_drops_7d
                        existing.rating = metrics.rating
                        existing.review_count = metrics.review_count

                        existing.demand_score = metrics.demand_score
                        existing.hot_score = metrics.hot_score
                        existing.is_active = True

                        # NOTE: ingested_at will be automatically set by DB trigger
                    else:
                        db.add(
                            Deal(
                                asin=asin,
                                category_slug=cat_slug,
                                published_at=run_started_at,
                                price_current=metrics.price_current,
                                price_median_90d=metrics.price_median_90d,
                                discount_pct_90d=metrics.discount_pct_90d,
                                confidence=metrics.confidence,
                                score=metrics.score,
                                sales_rank_current=metrics.sales_rank_current,
                                sales_rank_median_30d=metrics.sales_rank_median_30d,
                                sales_rank_trend_30d=metrics.sales_rank_trend_30d,
                                rank_drops_7d=metrics.rank_drops_7d,
                                rating=metrics.rating,
                                review_count=metrics.review_count,
                                demand_score=metrics.demand_score,
                                hot_score=metrics.hot_score,
                                is_active=True,
                                # ingested_at will be set by DB trigger
                            )
                        )
                    
                    deals_processed += 1

        print(f"\nProcessed {deals_processed} deals in this run")

        # CRITICAL FIX: After a successful ingestion, delete deals from PREVIOUS runs
        # We use published_at (which we control) instead of ingested_at (which has unique timestamps from trigger)
        if purge_end:
            print(f"Purging deals from previous runs (published_at < {run_started_at})")
            result = db.execute(
                text("DELETE FROM deals WHERE published_at IS NULL OR published_at < :run_ts"),
                {"run_ts": run_started_at},
            )
            deleted_count = result.rowcount
            print(f"Deleted {deleted_count} old deals")
        else:
            print("Keeping deals from previous runs (PURGE_DEALS_ON_END=0)")

        db.commit()

    print("Ingestion complete.")


if __name__ == "__main__":
    run_ingestion_once()
