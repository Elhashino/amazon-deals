"""
Daily Social Media Post Generator for Sunblessed Savings
Run from the backend/ folder.

Reads DATABASE_URL from (in priority order):
  1. $env:DATABASE_URL environment variable (set in PowerShell)
  2. backend/.env file via project config

Usage:
    python generate_posts.py           # 15 Twitter posts → daily_posts.txt + post_images/
    python generate_posts.py --quick   # 5 Twitter posts printed to screen only
    python generate_posts.py --count 20  # custom number of posts
    python generate_posts.py --hukd    # 10 HotUKDeals posts → hukd_posts.txt
    python generate_posts.py --hukd --count 5  # custom number of HUKD posts
    python generate_posts.py --pick    # browse all deals, pick ones you want → hukd_posts.txt
    python generate_posts.py --reset   # clear posted history so all deals are available again
"""

import os
import sys
import pathlib
import urllib.request
from datetime import datetime
from typing import Optional
from sqlalchemy import create_engine, text

# Use DATABASE_URL env var if set (e.g. via $env:DATABASE_URL in PowerShell),
# otherwise fall back to whatever is in the project .env / config.py
_db_url = os.getenv("DATABASE_URL")
if _db_url:
    engine = create_engine(_db_url, pool_pre_ping=True)
else:
    from app.db import engine

IMAGES_DIR = pathlib.Path("post_images")
POSTED_ASINS_FILE = pathlib.Path("posted_asins.txt")

# ---------------------------------------------------------------------------
# Posted ASIN tracking — prevents re-posting the same deal locally
# ---------------------------------------------------------------------------

def load_posted_asins() -> set:
    if not POSTED_ASINS_FILE.exists():
        return set()
    return {line.strip() for line in POSTED_ASINS_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}

def save_posted_asins(asins: list[str]) -> None:
    with POSTED_ASINS_FILE.open("a", encoding="utf-8") as f:
        for asin in asins:
            f.write(asin + "\n")

def reset_posted_asins() -> None:
    POSTED_ASINS_FILE.write_text("", encoding="utf-8")
    print("✅ Posted ASIN history cleared — all deals available again.")

# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------

CATEGORY_URLS = {
    "beauty":     "https://sunblessedsavings.co.uk/category/beauty",
    "pet":        "https://sunblessedsavings.co.uk/category/pet",
    "health":     "https://sunblessedsavings.co.uk/category/health",
    "baby":       "https://sunblessedsavings.co.uk/category/baby",
    "kitchen":    "https://sunblessedsavings.co.uk/category/kitchen",
    "garden":     "https://sunblessedsavings.co.uk/category/garden",
    "diy":        "https://sunblessedsavings.co.uk/category/diy",
    "toys":       "https://sunblessedsavings.co.uk/category/toys",
    "electrical": "https://sunblessedsavings.co.uk/category/electrical",
    "grocery":    "https://sunblessedsavings.co.uk/category/grocery",
    "sports":     "https://sunblessedsavings.co.uk/category/sports",
    "automotive": "https://sunblessedsavings.co.uk/category/automotive",
    "misc":       "https://sunblessedsavings.co.uk",
}

CATEGORY_EMOJIS = {
    "beauty":     "💄",
    "pet":        "🐾",
    "health":     "⚕️",
    "baby":       "👶",
    "kitchen":    "🍳",
    "garden":     "🌱",
    "diy":        "🔨",
    "toys":       "🧸",
    "electrical": "⚡",
    "grocery":    "🛒",
    "sports":     "⚽",
    "automotive": "🚗",
    "misc":       "🔥",
}

CATEGORY_HASHTAGS = {
    "beauty":     "#BeautyDeals",
    "pet":        "#PetDeals",
    "health":     "#HealthDeals",
    "baby":       "#BabyDeals",
    "kitchen":    "#HomeDeals",
    "garden":     "#GardenDeals",
    "diy":        "#DIYDeals",
    "toys":       "#ToyDeals",
    "electrical": "#TechDeals",
    "grocery":    "#GroceryDeals",
    "sports":     "#SportsDeals",
    "automotive": "#AutomotiveDeals",
    "misc":       "#AmazonDeals",
}

# ---------------------------------------------------------------------------
# Image downloader
# ---------------------------------------------------------------------------

def download_image(image_url: str, asin: str, index: int) -> Optional[str]:
    """Download product image to post_images/ and return the local file path."""
    if not image_url:
        return None
    IMAGES_DIR.mkdir(exist_ok=True)
    # Strip query params to get extension, default to jpg
    clean = image_url.split("?")[0]
    ext = clean.rsplit(".", 1)[-1] if "." in clean else "jpg"
    filename = IMAGES_DIR / f"{index:02d}_{asin}.{ext}"
    try:
        urllib.request.urlretrieve(image_url, filename)
        return str(filename)
    except Exception as e:
        print(f"  ⚠️  Could not download image for {asin}: {e}")
        return None

# ---------------------------------------------------------------------------
# Post builder
# ---------------------------------------------------------------------------

def build_post(deal) -> Optional[str]:
    price_current = deal["price_current"]
    price_median  = deal["price_median_90d"]
    discount_pct  = deal["discount_pct_90d"]

    if not price_current or not price_median:
        return None

    savings          = price_median - price_current
    discount_pct_int = int(discount_pct * 100)
    category         = deal["category_slug"]
    emoji            = CATEGORY_EMOJIS.get(category, "🔥")
    category_hashtag = CATEGORY_HASHTAGS.get(category, "")
    short_title      = deal["title"][:60] + "..." if len(deal["title"]) > 60 else deal["title"]
    url              = CATEGORY_URLS.get(category, "https://sunblessedsavings.co.uk")

    rating       = deal.get("rating")
    review_count = deal.get("review_count")
    social_proof = ""
    if rating and review_count:
        social_proof = f"⭐ {rating:.1f}/5 ({review_count:,} reviews)\n\n"
    elif rating:
        social_proof = f"⭐ {rating:.1f}/5\n\n"

    return (
        f"🔥 {discount_pct_int}% OFF — Save £{savings:.2f}! 🔥\n"
        f"\n"
        f"{short_title}\n"
        f"\n"
        f"{social_proof}"
        f"Was: £{price_median:.2f} → NOW: £{price_current:.2f} 💰\n"
        f"\n"
        f"👉 {url}\n"
        f"\n"
        f"#AmazonUK #UKDeals {category_hashtag}"
    )

# ---------------------------------------------------------------------------
# Query — wrapped subquery so LIMIT picks by hot_score, not asin alphabetically
# ---------------------------------------------------------------------------

DEALS_QUERY = text("""
    SELECT title, price_current, price_median_90d, discount_pct_90d,
           category_slug, hot_score, asin, image_url, review_count, rating
    FROM (
        SELECT DISTINCT ON (d.asin)
            p.title,
            d.price_current,
            d.price_median_90d,
            d.discount_pct_90d,
            d.category_slug,
            d.hot_score,
            d.asin,
            p.image_url,
            d.review_count,
            d.rating
        FROM deals d
        JOIN products p ON p.asin = d.asin
        WHERE d.is_active = true
          AND d.discount_pct_90d >= 0.30
          AND d.price_current    IS NOT NULL
          AND d.price_median_90d IS NOT NULL
          AND (d.price_median_90d - d.price_current) >= 10.0
          AND d.rating           >= 4.0
          AND d.review_count     >= 500
        ORDER BY d.asin, d.hot_score DESC NULLS LAST
    ) sub
    ORDER BY hot_score DESC NULLS LAST
    LIMIT 100
""")

BROWSE_QUERY = text("""
    SELECT title, price_current, price_median_90d, discount_pct_90d,
           category_slug, hot_score, asin,
           rating, review_count, image_url
    FROM (
        SELECT DISTINCT ON (d.asin)
            p.title,
            d.price_current,
            d.price_median_90d,
            d.discount_pct_90d,
            d.category_slug,
            d.hot_score,
            d.asin,
            d.rating,
            d.review_count,
            p.image_url
        FROM deals d
        JOIN products p ON p.asin = d.asin
        WHERE d.is_active = true
          AND d.discount_pct_90d >= 0.19
          AND d.price_current    IS NOT NULL
          AND d.price_median_90d IS NOT NULL
        ORDER BY d.asin, d.hot_score DESC NULLS LAST
    ) sub
    ORDER BY discount_pct_90d DESC NULLS LAST
    LIMIT 200
""")

# ---------------------------------------------------------------------------
# HotUKDeals post builder
# ---------------------------------------------------------------------------

def build_hukd_post(deal) -> Optional[str]:
    price_current = deal["price_current"]
    price_median  = deal["price_median_90d"]
    discount_pct  = deal["discount_pct_90d"]
    asin          = deal["asin"]

    if not price_current or not price_median:
        return None

    savings          = price_median - price_current
    discount_pct_int = int(discount_pct * 100)
    title            = deal["title"]
    short_title      = title[:80] + "..." if len(title) > 80 else title
    amazon_url       = f"https://www.amazon.co.uk/dp/{asin}?tag=sunblessedsav-21"

    hukd_title = f"{short_title} — {discount_pct_int}% off — £{price_current:.2f} (was £{price_median:.2f})"

    body = (
        f"Amazon UK has {short_title} for £{price_current:.2f}, down from £{price_median:.2f} "
        f"— that's {discount_pct_int}% off and a saving of £{savings:.2f}."
    )

    return (
        f"TITLE (paste into HotUKDeals title field):\n"
        f"{hukd_title}\n"
        f"\n"
        f"DEAL LINK (paste into HotUKDeals deal URL field):\n"
        f"{amazon_url}\n"
        f"\n"
        f"DESCRIPTION (paste into HotUKDeals description box):\n"
        f"{body}"
    )


def generate_hukd_posts(num_posts: int = 10) -> list[str]:
    posted = load_posted_asins()
    print(f"🌞 Fetching deals for HotUKDeals... ({len(posted)} already posted, skipping those)\n")

    with engine.connect() as conn:
        rows = conn.execute(DEALS_QUERY).mappings().all()

    if not rows:
        print("❌ No deals found — check DB has active deals.")
        return []

    fresh = [d for d in rows if d["asin"] not in posted]
    if not fresh:
        print("⚠️  All top deals already posted. Run with --reset to start over.")
        return []

    selected = fresh[:num_posts]
    posts = []
    new_asins = []
    for i, deal in enumerate(selected, 1):
        post = build_hukd_post(deal)
        if post:
            posts.append(post)
            new_asins.append(deal["asin"])
            print(f"--- DEAL {i} ---")
            print(post)
            print("\n" + "=" * 60 + "\n")

    if new_asins:
        save_posted_asins(new_asins)

    return posts


def save_hukd_to_file(posts: list[str], filename: str = "hukd_posts.txt") -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"SUNBLESSED SAVINGS — HOTUKDEALS POSTS\n")
        f.write(f"Generated : {timestamp}\n")
        f.write(f"Total     : {len(posts)} deals\n")
        f.write("=" * 60 + "\n\n")

        for i, post in enumerate(posts, 1):
            f.write(f"DEAL {i}\n")
            f.write("-" * 60 + "\n")
            f.write(post)
            f.write("\n\n" + "=" * 60 + "\n\n")

    print(f"✅ Saved {len(posts)} deals → {filename}")
    print("📋 Open hukd_posts.txt and post each deal at hotukdeals.com")


# ---------------------------------------------------------------------------
# Main Twitter post generation
# ---------------------------------------------------------------------------

def generate_posts(num_posts: int = 15) -> list[tuple[str, Optional[str]]]:
    """Returns list of (tweet_text, local_image_path) tuples."""
    posted = load_posted_asins()
    print(f"🌞 Fetching deals from DB... ({len(posted)} already posted, skipping those)\n")

    with engine.connect() as conn:
        rows = conn.execute(DEALS_QUERY).mappings().all()

    if not rows:
        print("❌ No deals found — check DB has active deals.")
        return []

    # Filter out already-posted ASINs
    fresh = [d for d in rows if d["asin"] not in posted]
    if not fresh:
        print("⚠️  All top deals already posted. Run with --reset to start over.")
        return []

    selected = fresh[:num_posts]
    results = []
    new_asins = []
    for i, deal in enumerate(selected, 1):
        post = build_post(deal)
        if post:
            image_path = download_image(deal.get("image_url", ""), deal["asin"], i)
            results.append((post, image_path))
            new_asins.append(deal["asin"])
            print(f"--- POST {i} ---")
            print(post)
            if image_path:
                print(f"📸 IMAGE: {image_path}")
            else:
                print("📸 IMAGE: (not available)")
            print("\n" + "=" * 60 + "\n")

    if new_asins:
        save_posted_asins(new_asins)

    return results


def save_to_file(posts: list[tuple[str, Optional[str]]], filename: str = "daily_posts.txt") -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"SUNBLESSED SAVINGS — DAILY POSTS\n")
        f.write(f"Generated : {timestamp}\n")
        f.write(f"Total     : {len(posts)} posts\n")
        f.write("=" * 60 + "\n\n")

        for i, (post, image_path) in enumerate(posts, 1):
            f.write(f"POST {i}\n")
            f.write("-" * 60 + "\n")
            f.write(post)
            if image_path:
                f.write(f"\n\n📸 IMAGE: {image_path}")
            else:
                f.write(f"\n\n📸 IMAGE: (not available)")
            f.write("\n\n" + "=" * 60 + "\n\n")

    print(f"✅ Saved {len(posts)} posts → {filename}")
    print(f"📁 Images saved in: {IMAGES_DIR}/")
    print("📋 Open daily_posts.txt — copy tweet text, attach the image file, post!")


def interactive_hukd_picker() -> None:
    """Browse all active deals and manually pick which ones to format for HUKD."""
    print("🌞 Fetching deals from DB...\n")

    with engine.connect() as conn:
        rows = conn.execute(BROWSE_QUERY).mappings().all()

    if not rows:
        print("❌ No deals found.")
        return

    # Print browsable table
    print(f"{'#':<4} {'DISC':>5}  {'NOW':>7}  {'WAS':>7}  {'RATING':>7}  {'REVIEWS':>8}  TITLE")
    print("-" * 100)
    for i, d in enumerate(rows, 1):
        discount  = int(d["discount_pct_90d"] * 100)
        rating    = f"{d['rating']:.1f}★" if d["rating"] else "  -  "
        reviews   = str(d["review_count"]) if d["review_count"] else "-"
        title     = d["title"][:65] + "..." if len(d["title"]) > 65 else d["title"]
        print(f"{i:<4} {discount:>4}%  £{d['price_current']:>6.2f}  £{d['price_median_90d']:>6.2f}  {rating:>7}  {reviews:>8}  {title}")

    print(f"\n{len(rows)} deals shown, sorted by biggest discount first.")
    print("\nEnter the numbers of deals you want to post (e.g. 3 7 12), then press Enter:")

    raw = input("> ").strip()
    if not raw:
        print("No deals selected.")
        return

    try:
        picks = [int(x) for x in raw.split()]
    except ValueError:
        print("❌ Invalid input — enter space-separated numbers.")
        return

    selected = []
    for n in picks:
        if 1 <= n <= len(rows):
            selected.append(rows[n - 1])
        else:
            print(f"⚠️  #{n} out of range, skipping.")

    if not selected:
        print("No valid deals selected.")
        return

    posts = []
    print("\n" + "=" * 60 + "\n")
    for i, deal in enumerate(selected, 1):
        post = build_hukd_post(deal)
        if post:
            posts.append(post)
            print(f"--- DEAL {i} ---")
            print(post)
            print("\n" + "=" * 60 + "\n")

    if posts:
        save_hukd_to_file(posts)
        print(f"\n✅ Done! {len(posts)} HUKD posts saved to hukd_posts.txt")
        print("\n📝 NEXT STEPS:")
        print("  1. Open 'hukd_posts.txt'")
        print("  2. Go to hotukdeals.com → Submit a deal")
        print("  3. Paste TITLE, DEAL LINK, and DESCRIPTION into the form")
        print("  4. Set category and submit!")


if __name__ == "__main__":
    args = sys.argv[1:]
    quick = "--quick" in args
    hukd  = "--hukd" in args
    pick  = "--pick" in args
    reset = "--reset" in args

    if "--count" in args:
        idx = args.index("--count")
        try:
            num_posts = int(args[idx + 1])
        except (IndexError, ValueError):
            print("⚠️  --count needs a number, e.g. --count 10. Using default.")
            num_posts = None
    else:
        num_posts = None

    print("🌞 SUNBLESSED SAVINGS — POST GENERATOR 🌞")
    print("=" * 60)

    if reset:
        reset_posted_asins()
        sys.exit(0)

    if pick:
        interactive_hukd_picker()
    elif hukd:
        count = num_posts or 10
        posts = generate_hukd_posts(num_posts=count)
        if posts:
            save_hukd_to_file(posts)
            print(f"\n✅ Done! {len(posts)} HUKD posts saved.")
            print("\n📝 NEXT STEPS:")
            print("  1. Open 'hukd_posts.txt'")
            print("  2. Go to hotukdeals.com → Submit a deal")
            print("  3. Paste TITLE, DEAL LINK, and DESCRIPTION into the form")
            print("  4. Set category and submit — post the best deals first!")
        else:
            print("\n❌ No posts generated.")
    else:
        count = num_posts or (5 if quick else 15)
        posts = generate_posts(num_posts=count)

        if posts and not quick:
            save_to_file(posts)
            print(f"\n✅ Done! {len(posts)} posts saved.")
            print("\n📝 NEXT STEPS:")
            print("  1. Open 'daily_posts.txt' — each post has its image file path")
            print("  2. For each post: copy the tweet text into Twitter/X")
            print("  3. Click the image icon in Twitter, attach the matching image from post_images/")
            print("  4. Post at UK peak times: 7:30 / 9 / 11 / 12:30 / 3 / 5:30 / 7 / 9pm")
        elif posts and quick:
            print(f"\n✅ Done! {len(posts)} posts printed above — copy/paste now!")
        else:
            print("\n❌ No posts generated.")
