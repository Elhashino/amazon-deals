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

    resolved = list(out.keys())
    missing = [k for k in targets if k not in out]
    print(f"Resolved {len(resolved)}/{len(targets)} categories: {resolved}")
    if missing:
        print(f"WARNING: Could not resolve categories (will get 0 deals from these): {missing}")

    return out


def categorize(product: dict, root_name: str) -> str:
    root = _norm(root_name)
    
    # Get full category tree and title for better categorization
    category_tree = product.get("categoryTree") or []
    tree_names = " ".join(_norm(n.get("name", "")) for n in category_tree if isinstance(n, dict))
    title = _norm(product.get("title", ""))
    
    # Combine all text for keyword matching
    all_text = f"{root} {tree_names} {title}"

    # BEAUTY — all beauty-root products stay as beauty
    if "beauty" in root:
        return "beauty"

    # HEALTH — exclude sports/fitness equipment that belongs in sports
    if "health" in root:
        non_health = [
            "treadmill", "exercise bike", "rowing machine", "cross trainer",
            "spin bike", "dumbbell", "barbell", "weight plate", "kettlebell",
            "pull up bar", "resistance band", "yoga mat", "gym bench",
            "punch bag", "boxing glove", "sports shoe", "running shoe",
            "football boot", "cycling shoe", "skipping rope", "ab roller"
        ]
        if not any(kw in all_text for kw in non_health):
            return "health"
        # Falls through to sports check below

    # GROCERY
    if "grocery" in root or "food" in root:
        return "grocery"

    # PET
    if "pet" in root:
        return "pet"

    # BABY — only if product is actually a baby product
    if "baby" in root:
        baby_keywords = [
            "baby", "infant", "toddler", "newborn", "nappy", "diaper",
            "pram", "pushchair", "buggy", "cot", "nursery", "teething",
            "dummy", "bottle feed", "breast pump", "maternity", "stroller",
            "weaning", "highchair", "car seat", "baby monitor", "baby wipe",
            "neonatal", "babygrow", "onesie", "bib", "moses basket"
        ]
        non_baby = [
            "elbow guard", "elbow pad", "knee guard", "knee pad", "helmet",
            "glove", "shin guard", "protective gear", "body armour",
            "cycling", "ski", "snowboard", "mtb", "motocross",
            "adult", "men", "women", "xl", "xxl", "large", "medium"
        ]
        if any(kw in all_text for kw in baby_keywords) and not any(kw in all_text for kw in non_baby):
            return "baby"
        # Falls through to correct category below

    # AUTOMOTIVE — avoid matching "care", "card", "carpet" etc.
    if "automotive" in root or "motoring" in root or "vehicle" in root:
        return "automotive"
    
    # GARDEN - Very strict: require "garden" in text OR garden-specific words only
    # Step 1: Check if "garden" or "gardening" appears anywhere
    has_garden_word = "garden" in all_text or "gardening" in all_text
    
    # Step 2: Garden-specific keywords that are unambiguous
    garden_specific_keywords = [
        "lawn mower", "lawnmower", "lawn feed", "lawn seed", "lawn care",
        "greenhouse", "compost", "fertiliser", "fertilizer", "weed killer",
        "hedge trimmer", "strimmer", "lawn edger", "secateurs", "trowel",
        "watering can", "grow bag", "potting soil", "potting mix",
        "plant pot", "flower pot", "planter box",
        "garden hose", "garden fork", "garden spade", "garden rake",
        "bird feeder", "bird bath", "seed packet", "vegetable seed"
    ]
    
    has_garden_specific = any(kw in all_text for kw in garden_specific_keywords)
    
    # Only categorize as garden if either condition is true
    if has_garden_word or has_garden_specific:
        # Exclude if clearly a non-garden category
        non_garden = [
            "football", "tennis", "cricket", "golf", "fitness", "gym",
            "bike", "cycling", "makeup", "skincare", "shampoo", "vitamin",
            "supplement", "kitchen", "cookware", "food", "snack", "drink",
            "toy", "game", "puzzle", "doll", "action figure",
            "clothing", "shirt", "trouser", "dress", "jacket",
            "phone", "tablet", "laptop", "camera",
            "book", "dvd", "cd", "music"
        ]
        if not any(kw in all_text for kw in non_garden):
            return "garden"
    
    # SPORTS - After garden check
    if "sports" in root or "outdoors" in root or "fitness" in root:
        return "sports"

    # Existing behaviour
    if "home" in root and "kitchen" in root:
        names = " ".join(_norm(n.get("name", "")) for n in category_tree if isinstance(n, dict))
        if any(k in names for k in ["kitchen", "dining", "cookware", "bakeware", "utensils", "appliances"]):
            return "kitchen"
        return "misc"

    if "diy" in root or "tools" in root:
        return "diy"

    if "toys" in root or "games" in root:
        return "toys"

    if "electronics" in root:
        return "electrical"

    # --- Keyword-based fallbacks for items rejected from their root category ---
    # Catches e.g. fitness gear rejected from health, or electrical items rejected from beauty

    sports_fallback = [
        "treadmill", "exercise bike", "rowing machine", "cross trainer", "spin bike",
        "dumbbell", "barbell", "weight plate", "kettlebell", "pull up bar",
        "resistance band", "yoga mat", "gym bench", "punch bag", "boxing glove",
        "running shoe", "football boot", "cycling shoe", "skipping rope", "ab roller",
        "elbow guard", "elbow pad", "knee guard", "knee pad", "shin guard",
        "ski boot", "snowboard", "cycling helmet", "mtb helmet", "motocross helmet"
    ]
    if any(kw in all_text for kw in sports_fallback):
        return "sports"

    electrical_fallback = [
        "hair dryer", "hair straightener", "straighteners", "hair curler",
        "curling tong", "electric shaver", "electric razor", "epilator",
        "electric toothbrush", "water flosser", "facial steamer",
        "led mask", "led light therapy", "sonic cleaner", "massager"
    ]
    if any(kw in all_text for kw in electrical_fallback):
        return "electrical"

    return "misc"


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
        "misc": settings.MIN_DISCOUNT_MISC,
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


def _clean_title(title: str) -> str:
    """Remove common Amazon spam from product titles."""
    if not title:
        return title
    
    # Remove common spam patterns
    spam_patterns = [
        r'\(Pack of \d+\)',
        r'\[\d+ Pack\]',
        r'Compatible with ',
        r'Works with ',
        r'- \d+ Count',
        r'\d+ Count',
        r'Prime Eligible',
        r'FREE Delivery',
        r'Subscribe & Save',
    ]
    
    import re
    cleaned = title
    for pattern in spam_patterns:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Remove extra spaces and trim
    cleaned = ' '.join(cleaned.split())
    return cleaned[:600]  # Keep DB limit


def run_ingestion_once():
    Base.metadata.create_all(bind=engine)
    client = KeepaClient()

    roots = client.uk_root_categories() or {}
    root_ids = resolve_root_category_ids(roots)

    # Optional: restrict to a subset of categories via CATEGORY_GROUP env var.
    # A = home/electronics/beauty/health side; B = grocery/pet/sports/baby/auto/garden side.
    # Leave unset (or empty) to run all categories (original behaviour).
    category_group = os.getenv("CATEGORY_GROUP", "").strip().upper()
    GROUP_A = {"home_kitchen", "diy_tools", "toys_games", "electronics", "beauty", "health"}
    GROUP_B = {"grocery", "pet", "sports", "baby", "automotive", "garden"}
    if category_group == "A":
        root_ids = {k: v for k, v in root_ids.items() if k in GROUP_A}
        print(f"CATEGORY_GROUP=A — running: {list(root_ids.keys())}")
    elif category_group == "B":
        root_ids = {k: v for k, v in root_ids.items() if k in GROUP_B}
        print(f"CATEGORY_GROUP=B — running: {list(root_ids.keys())}")
    else:
        print(f"CATEGORY_GROUP not set — running all: {list(root_ids.keys())}")

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

                    title = _clean_title(p.get("title") or "")
                    
                    # Skip unwanted products (underwear, erotic, etc.)
                    exclude_keywords = [
                        "underwear", "lingerie", "bra", "panties", "thong",
                        "g-string", "sexy", "erotic", "adult", "knickers",
                        "briefs", "boxers", "intimate"
                    ]
                    title_lower = title.lower()
                    if any(keyword in title_lower for keyword in exclude_keywords):
                        continue
                    
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
                    if metrics.discount_pct_90d is None:
                        continue

                    if metrics.discount_pct_90d < min_discount_for_category(cat_slug):
                        continue

                    # If score still couldn't be computed, synthesise a simple fallback
                    # so the deal isn't silently dropped (score = discount * 70, confidence = 0)
                    if metrics.score is None:
                        metrics.score = float(min(metrics.discount_pct_90d, 1.0) * 70.0)
                        if metrics.confidence is None:
                            metrics.confidence = 0.0

                    # Quality filters — balance between volume and not showing junk
                    # Require at least 15 reviews — products with no review data are also excluded
                    if metrics.review_count is None or metrics.review_count < 15:
                        continue

                    # Decent rating — filters out poor quality without being too strict
                    if metrics.rating is not None and metrics.rating < 3.5:
                        continue
                    
                    # Skip suspiciously cheap products (likely pricing errors or scams)
                    if metrics.price_current is not None and metrics.price_current < 0.50:
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
                    ).scalars().first()

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

        # Flush ORM changes before raw SQL deletes to avoid StaleDataError
        db.flush()

        # CRITICAL FIX: Remove any duplicate deals (same ASIN + category_slug)
        # Keep only the most recently ingested one
        print("Removing duplicate deals...")
        dedup_result = db.execute(text("""
            DELETE FROM deals a USING deals b
            WHERE a.id < b.id
            AND a.asin = b.asin
            AND a.category_slug = b.category_slug
            AND a.is_active = true
            AND b.is_active = true
        """))
        dedup_count = dedup_result.rowcount
        if dedup_count > 0:
            print(f"Removed {dedup_count} duplicate deals")

        # CRITICAL FIX: After a successful ingestion, delete deals from PREVIOUS runs
        # We use published_at (which we control) instead of ingested_at (which has unique timestamps from trigger)
        if purge_end:
            # Delete deals older than 25 hours. This is safe for both single-run and
            # two-run setups: a 2am Group A run and a 2pm Group B run are only 12 hours
            # apart, so both stay within the 25-hour window and neither wipes the other.
            print("Purging deals older than 25 hours...")
            result = db.execute(
                text("DELETE FROM deals WHERE published_at IS NULL OR published_at < (NOW() - INTERVAL '25 hours')")
            )
            deleted_count = result.rowcount
            print(f"Deleted {deleted_count} old deals")
        else:
            print("Keeping deals from previous runs (PURGE_DEALS_ON_END=0)")

        db.commit()

    print("Ingestion complete.")


if __name__ == "__main__":
    run_ingestion_once()
