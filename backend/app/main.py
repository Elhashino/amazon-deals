"""
FastAPI web application for UK Deals Alert
"""
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import os
from pathlib import Path

# Initialize FastAPI app
app = FastAPI(title="UK Deals Alert")

# Setup directories
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Create directories if they don't exist
TEMPLATES_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)
(STATIC_DIR / "css").mkdir(exist_ok=True)
(STATIC_DIR / "images").mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Setup templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Add custom Jinja filter for time ago
def time_ago(dt):
    """Convert datetime to human readable 'time ago' string."""
    if dt is None:
        return "Unknown"
    
    # Ensure we have a timezone-aware datetime
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "Just now"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}m ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}h ago"
    else:
        days = int(seconds / 86400)
        return f"{days}d ago"

templates.env.filters["time_ago"] = time_ago

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

engine = create_engine(DATABASE_URL)

# Amazon affiliate tag - UPDATE THIS WITH YOUR TAG!
AFFILIATE_TAG = os.getenv("AMAZON_AFFILIATE_TAG", "yoursite-21")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Homepage showing sunniest savings (30%+ discounts)"""
    with engine.connect() as conn:
        # Get top 3 overall for featured section
        featured_deals = conn.execute(text("""
            SELECT * FROM (
                SELECT DISTINCT ON (d.asin)
                    p.title,
                    p.image_url,
                    p.asin,
                    d.discount_pct_90d,
                    d.price_current,
                    d.price_median_90d,
                    d.category_slug,
                    d.hot_score,
                    d.rating,
                    d.review_count,
                    d.score,
                    d.ingested_at
                FROM deals d
                JOIN products p ON p.asin = d.asin
                WHERE d.is_active = true
                AND d.discount_pct_90d >= 0.25
                ORDER BY d.asin, d.hot_score DESC NULLS LAST
            ) sub
            ORDER BY hot_score DESC NULLS LAST
            LIMIT 3
        """)).mappings().all()
        
        # Get top 3 from each category ordered by hot_score (most purchased)
        categories_with_deals = []
        category_names_map = {
            "beauty": "Beauty",
            "pet": "Pet",
            "health": "Health",
            "baby": "Baby",
            "kitchen": "Home and Kitchen",
            "garden": "Garden",
            "diy": "DIY",
            "toys": "Toys",
            "electrical": "Electronics",
            "grocery": "Grocery",
            "sports": "Sports",
            "automotive": "Automotive",
            "misc": "More Deals",
        }
        
        for slug, name in category_names_map.items():
            category_deals = conn.execute(text("""
                SELECT * FROM (
                    SELECT DISTINCT ON (d.asin)
                        p.title,
                        p.image_url,
                        p.asin,
                        d.discount_pct_90d,
                        d.price_current,
                        d.price_median_90d,
                        d.category_slug,
                        d.hot_score,
                        d.rating,
                        d.review_count,
                        d.score,
                        d.ingested_at
                    FROM deals d
                    JOIN products p ON p.asin = d.asin
                    WHERE d.is_active = true
                    AND d.category_slug = :slug
                    AND d.discount_pct_90d >= 0.25
                    ORDER BY d.asin,
                        (d.discount_pct_90d * 0.5 +
                         COALESCE(d.rating, 0) / 5.0 * 0.3 +
                         LEAST(LN(GREATEST(d.review_count, 1)) / 10.0, 1.0) * 0.2) DESC
                ) sub
                ORDER BY (discount_pct_90d * 0.5 +
                          COALESCE(rating, 0) / 5.0 * 0.3 +
                          LEAST(LN(GREATEST(review_count, 1)) / 10.0, 1.0) * 0.2) DESC
                LIMIT 3
            """), {"slug": slug}).mappings().all()
            
            if category_deals:  # Only add if category has deals
                categories_with_deals.append({
                    "slug": slug,
                    "name": name,
                    "deals": category_deals
                })

        last_updated = conn.execute(text(
            "SELECT MAX(ingested_at) FROM deals WHERE is_active = true"
        )).scalar()

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "featured_deals": featured_deals,
            "categories_with_deals": categories_with_deals,
            "affiliate_tag": AFFILIATE_TAG,
            "last_updated": last_updated
        }
    )


@app.get("/category/sunniest-savings", response_class=HTMLResponse)
async def sunniest_savings(request: Request):
    """Sunniest Savings - Best discounts across all categories"""
    with engine.connect() as conn:
        # Get all sunniest deals
        deals = conn.execute(text("""
            SELECT 
                p.title, 
                p.image_url, 
                p.asin, 
                d.discount_pct_90d, 
                d.price_current, 
                d.price_median_90d, 
                d.category_slug,
                d.hot_score, 
                d.rating, 
                d.review_count,
                d.score,
                d.ingested_at
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true
            AND d.discount_pct_90d >= 0.25
            ORDER BY d.hot_score DESC NULLS LAST
            LIMIT 100
        """)).mappings().all()
        
        # Get top 3 for featured section (highest discount)
        featured_deals = conn.execute(text("""
            SELECT
                p.title,
                p.image_url,
                p.asin,
                d.discount_pct_90d,
                d.price_current,
                d.price_median_90d,
                d.category_slug,
                d.hot_score,
                d.rating,
                d.review_count,
                d.score,
                d.ingested_at
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true
            AND d.discount_pct_90d >= 0.25
            ORDER BY d.discount_pct_90d DESC NULLS LAST
            LIMIT 3
        """)).mappings().all()

        last_updated = conn.execute(text(
            "SELECT MAX(ingested_at) FROM deals WHERE is_active = true"
        )).scalar()

    return templates.TemplateResponse(
        "category.html",
        {
            "request": request,
            "deals": deals,
            "featured_deals": featured_deals,
            "category": "sunniest-savings",
            "category_name": "Sunniest Savings",
            "affiliate_tag": AFFILIATE_TAG,
            "last_updated": last_updated
        }
    )


@app.get("/category/{slug}", response_class=HTMLResponse)
async def category(request: Request, slug: str):
    """Category page showing filtered deals"""
    
    # Category display names
    category_names = {
        "beauty": "Beauty",
        "pet": "Pet",
        "health": "Health",
        "baby": "Baby",
        "grocery": "Grocery",
        "sports": "Sports",
        "automotive": "Automotive",
        "kitchen": "Home and Kitchen",
        "garden": "Garden",
        "diy": "DIY",
        "toys": "Toys",
        "electrical": "Electronics"
    }
    
    with engine.connect() as conn:
        # Get all deals for this category
        deals = conn.execute(text("""
            SELECT * FROM (
                SELECT DISTINCT ON (d.asin)
                    p.title,
                    p.image_url,
                    p.asin,
                    d.discount_pct_90d,
                    d.price_current,
                    d.price_median_90d,
                    d.category_slug,
                    d.hot_score,
                    d.rating,
                    d.review_count,
                    d.score,
                    d.ingested_at
                FROM deals d
                JOIN products p ON p.asin = d.asin
                WHERE d.is_active = true
                  AND d.category_slug = :slug
                ORDER BY d.asin,
                    (d.discount_pct_90d * 0.5 +
                     COALESCE(d.rating, 0) / 5.0 * 0.3 +
                     LEAST(LN(GREATEST(d.review_count, 1)) / 10.0, 1.0) * 0.2) DESC
            ) sub
            ORDER BY (discount_pct_90d * 0.5 +
                      COALESCE(rating, 0) / 5.0 * 0.3 +
                      LEAST(LN(GREATEST(review_count, 1)) / 10.0, 1.0) * 0.2) DESC
        """), {"slug": slug}).mappings().all()

        # Get top 3 for featured section (highest discount)
        featured_deals = conn.execute(text("""
            SELECT * FROM (
                SELECT DISTINCT ON (d.asin)
                    p.title,
                    p.image_url,
                    p.asin,
                    d.discount_pct_90d,
                    d.price_current,
                    d.price_median_90d,
                    d.category_slug,
                    d.hot_score,
                    d.rating,
                    d.review_count,
                    d.score,
                    d.ingested_at
                FROM deals d
                JOIN products p ON p.asin = d.asin
                WHERE d.is_active = true
                  AND d.category_slug = :slug
                ORDER BY d.asin, d.discount_pct_90d DESC NULLS LAST
            ) sub
            ORDER BY discount_pct_90d DESC NULLS LAST
            LIMIT 3
        """), {"slug": slug}).mappings().all()

        last_updated = conn.execute(text(
            "SELECT MAX(ingested_at) FROM deals WHERE is_active = true"
        )).scalar()

    category_name = category_names.get(slug, slug.title())

    return templates.TemplateResponse(
        "category.html",
        {
            "request": request,
            "deals": deals,
            "featured_deals": featured_deals,
            "category": slug,
            "category_name": category_name,
            "affiliate_tag": AFFILIATE_TAG,
            "last_updated": last_updated
        }
    )


@app.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = ""):
    """Search deals by product title across all categories"""
    deals = []
    if q.strip():
        with engine.connect() as conn:
            deals = conn.execute(text("""
                SELECT p.title, p.image_url, p.asin,
                    d.discount_pct_90d, d.price_current, d.price_median_90d,
                    d.category_slug, d.hot_score, d.rating, d.review_count,
                    d.score, d.ingested_at
                FROM deals d
                JOIN products p ON p.asin = d.asin
                WHERE d.is_active = true
                AND p.title ILIKE :query
                ORDER BY d.hot_score DESC NULLS LAST
                LIMIT 100
            """), {"query": f"%{q}%"}).mappings().all()

    return templates.TemplateResponse("search.html", {
        "request": request,
        "deals": deals,
        "query": q,
        "affiliate_tag": AFFILIATE_TAG,
    })


@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    """About page"""
    return templates.TemplateResponse("about.html", {"request": request})


@app.get("/affiliate-disclosure", response_class=HTMLResponse)
async def affiliate_disclosure(request: Request):
    """Affiliate Disclosure page"""
    return templates.TemplateResponse("affiliate-disclosure.html", {"request": request})


@app.get("/privacy", response_class=HTMLResponse)
async def privacy(request: Request):
    """Privacy policy page"""
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.get("/contact", response_class=HTMLResponse)
async def contact(request: Request):
    """Contact page"""
    return templates.TemplateResponse("contact.html", {"request": request})


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    return {"status": "healthy"}


@app.get("/debug/db")
async def debug_db():
    """Temporary diagnostic endpoint - check DB state"""
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM deals")).scalar()
        active = conn.execute(text("SELECT COUNT(*) FROM deals WHERE is_active = true")).scalar()
        passing_filter = conn.execute(text("SELECT COUNT(*) FROM deals WHERE is_active = true AND discount_pct_90d >= 0.25")).scalar()
        null_published = conn.execute(text("SELECT COUNT(*) FROM deals WHERE published_at IS NULL")).scalar()
        oldest = conn.execute(text("SELECT MIN(published_at) FROM deals")).scalar()
        newest = conn.execute(text("SELECT MAX(published_at) FROM deals")).scalar()
        by_category = conn.execute(text("""
            SELECT category_slug, COUNT(*) as cnt, MIN(discount_pct_90d) as min_disc, MAX(discount_pct_90d) as max_disc
            FROM deals WHERE is_active = true
            GROUP BY category_slug ORDER BY cnt DESC
        """)).mappings().all()
        total_products = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
        newest_product = conn.execute(text("SELECT MAX(last_seen_at) FROM products")).scalar()
        total_snapshots = conn.execute(text("SELECT COUNT(*) FROM price_snapshots")).scalar()
        newest_snapshot = conn.execute(text("SELECT MAX(captured_at) FROM price_snapshots")).scalar()
        db_now = conn.execute(text("SELECT NOW()")).scalar()
    return {
        "db_now_utc": str(db_now),
        "total_deals": total,
        "active_deals": active,
        "passing_discount_filter": passing_filter,
        "null_published_at": null_published,
        "oldest_published_at": str(oldest),
        "newest_published_at": str(newest),
        "by_category": [dict(r) for r in by_category],
        "total_products": total_products,
        "newest_product_last_seen": str(newest_product),
        "total_price_snapshots": total_snapshots,
        "newest_snapshot_captured": str(newest_snapshot),
    }


@app.get("/sitemap.xml")
async def sitemap():
    """Generate XML sitemap for search engines"""
    from fastapi.responses import Response
    
    categories = ["beauty", "pet", "health", "baby", "grocery", "sports",
                  "automotive", "kitchen", "garden", "diy", "toys", "electrical",
                  "misc", "sunniest-savings"]
    
    sitemap = '<?xml version="1.0" encoding="UTF-8"?>\n'
    sitemap += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    
    # Homepage
    sitemap += '  <url>\n'
    sitemap += '    <loc>https://sunblessedsavings.co.uk/</loc>\n'
    sitemap += '    <changefreq>daily</changefreq>\n'
    sitemap += '    <priority>1.0</priority>\n'
    sitemap += '  </url>\n'
    
    # Categories
    for cat in categories:
        sitemap += '  <url>\n'
        sitemap += f'    <loc>https://sunblessedsavings.co.uk/category/{cat}</loc>\n'
        sitemap += '    <changefreq>daily</changefreq>\n'
        sitemap += '    <priority>0.8</priority>\n'
        sitemap += '  </url>\n'
    
    # Static pages
    for page in ["about", "privacy", "contact"]:
        sitemap += '  <url>\n'
        sitemap += f'    <loc>https://sunblessedsavings.co.uk/{page}</loc>\n'
        sitemap += '    <changefreq>monthly</changefreq>\n'
        sitemap += '    <priority>0.5</priority>\n'
        sitemap += '  </url>\n'
    
    sitemap += '</urlset>'
    
    return Response(content=sitemap, media_type="application/xml")


@app.get("/robots.txt")
async def robots():
    """Robots.txt for search engines"""
    from fastapi.responses import Response
    
    content = """User-agent: *
Allow: /
Sitemap: https://sunblessedsavings.co.uk/sitemap.xml
"""
    return Response(content=content, media_type="text/plain")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
