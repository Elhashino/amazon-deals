"""
Daily Social Media Post Generator for Sunblessed Savings
Run from the backend/ folder.

Reads DATABASE_URL from (in priority order):
  1. $env:DATABASE_URL environment variable (set in PowerShell)
  2. backend/.env file via project config

Usage:
    python generate_posts.py           # 15 posts → daily_posts.txt
    python generate_posts.py --quick   # 5 posts printed to screen only
    python generate_posts.py --count 20  # custom number of posts
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

# Use DATABASE_URL env var if set (e.g. via $env:DATABASE_URL in PowerShell),
# otherwise fall back to whatever is in the project .env / config.py
_db_url = os.getenv("DATABASE_URL")
if _db_url:
    engine = create_engine(_db_url, pool_pre_ping=True)
else:
    from app.db import engine

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
}

# ---------------------------------------------------------------------------
# Post builder
# ---------------------------------------------------------------------------

def build_post(deal) -> str | None:
    price_current = deal["price_current"]
    price_median  = deal["price_median_90d"]
    discount_pct  = deal["discount_pct_90d"]

    if not price_current or not price_median:
        return None

    savings            = price_median - price_current
    discount_pct_int   = int(discount_pct * 100)
    category           = deal["category_slug"]
    emoji              = CATEGORY_EMOJIS.get(category, "🔥")
    category_hashtag   = CATEGORY_HASHTAGS.get(category, "")
    short_title        = deal["title"][:60] + "..." if len(deal["title"]) > 60 else deal["title"]

    url = (
        "https://sunblessedsavings.co.uk/category/sunniest-savings"
        if discount_pct >= 0.30
        else CATEGORY_URLS.get(category, "https://sunblessedsavings.co.uk")
    )

    return (
        f"{emoji} {discount_pct_int}% OFF! {emoji}\n"
        f"\n"
        f"{short_title}\n"
        f"\n"
        f"Was: £{price_median:.2f}\n"
        f"NOW: £{price_current:.2f} 💰\n"
        f"\n"
        f"Save £{savings:.2f}!\n"
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
           category_slug, hot_score, asin
    FROM (
        SELECT DISTINCT ON (d.asin)
            p.title,
            d.price_current,
            d.price_median_90d,
            d.discount_pct_90d,
            d.category_slug,
            d.hot_score,
            d.asin
        FROM deals d
        JOIN products p ON p.asin = d.asin
        WHERE d.is_active = true
          AND d.discount_pct_90d >= 0.19
          AND d.price_current    IS NOT NULL
          AND d.price_median_90d IS NOT NULL
        ORDER BY d.asin, d.hot_score DESC NULLS LAST
    ) sub
    ORDER BY hot_score DESC NULLS LAST
    LIMIT :limit
""")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_posts(num_posts: int = 15) -> list[str]:
    print(f"🌞 Fetching top {num_posts} deals from DB...\n")

    with engine.connect() as conn:
        rows = conn.execute(DEALS_QUERY, {"limit": num_posts}).mappings().all()

    if not rows:
        print("❌ No deals found — check DB has active deals.")
        return []

    posts = []
    for i, deal in enumerate(rows, 1):
        post = build_post(deal)
        if post:
            posts.append(post)
            print(f"--- POST {i} ---")
            print(post)
            print("\n" + "=" * 60 + "\n")

    return posts


def save_to_file(posts: list[str], filename: str = "daily_posts.txt") -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"🌞 SUNBLESSED SAVINGS — DAILY POSTS\n")
        f.write(f"Generated : {timestamp}\n")
        f.write(f"Total     : {len(posts)} posts\n")
        f.write("=" * 60 + "\n\n")

        for i, post in enumerate(posts, 1):
            f.write(f"POST {i}\n")
            f.write("-" * 60 + "\n")
            f.write(post)
            f.write("\n\n" + "=" * 60 + "\n\n")

    print(f"✅ Saved {len(posts)} posts → {filename}")
    print("📋 Open the file and copy/paste throughout the day!")


if __name__ == "__main__":
    args = sys.argv[1:]
    quick = "--quick" in args

    num_posts = 5 if quick else 15
    if "--count" in args:
        idx = args.index("--count")
        try:
            num_posts = int(args[idx + 1])
        except (IndexError, ValueError):
            print("⚠️  --count needs a number, e.g. --count 20. Using 15.")
            num_posts = 15

    print("🌞 SUNBLESSED SAVINGS — POST GENERATOR 🌞")
    print("=" * 60)

    posts = generate_posts(num_posts=num_posts)

    if posts and not quick:
        save_to_file(posts)
        print(f"\n✅ Done! {len(posts)} posts saved.")
        print("\n📝 NEXT STEPS:")
        print("  1. Open 'daily_posts.txt'")
        print("  2. Copy each post into Twitter/X")
        print("  3. Post at UK peak times: 7:30 / 9 / 11 / 12:30 / 3 / 5:30 / 7 / 9pm")
    elif posts and quick:
        print(f"\n✅ Done! {len(posts)} posts printed above — copy/paste now!")
    else:
        print("\n❌ No posts generated.")
