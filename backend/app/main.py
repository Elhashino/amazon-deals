"""
FastAPI web application for UK Deals Alert
"""
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text
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
                d.score
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true
            AND d.discount_pct_90d >= 0.30
            ORDER BY d.discount_pct_90d DESC NULLS LAST
            LIMIT 3
        """)).mappings().all()
        
        # Get top 3 from each category ordered by hot_score (most purchased)
        categories_with_deals = []
        category_names_map = {
            "beauty": "Beauty",
            "pet": "Pet",
            "health": "Health",
            "baby": "Baby",
            "kitchen": "Kitchen",
            "garden": "Garden",
            "diy": "DIY",
            "toys": "Toys",
            "electrical": "Electronics",
            "grocery": "Grocery",
            "sports": "Sports",
            "automotive": "Automotive",
        }
        
        for slug, name in category_names_map.items():
            category_deals = conn.execute(text("""
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
                    d.score
                FROM deals d
                JOIN products p ON p.asin = d.asin
                WHERE d.is_active = true
                AND d.category_slug = :slug
                AND d.discount_pct_90d >= 0.30
                ORDER BY d.hot_score DESC NULLS LAST
                LIMIT 3
            """), {"slug": slug}).mappings().all()
            
            if category_deals:  # Only add if category has deals
                categories_with_deals.append({
                    "slug": slug,
                    "name": name,
                    "deals": category_deals
                })
    
    return templates.TemplateResponse(
        "home.html", 
        {
            "request": request, 
            "featured_deals": featured_deals,
            "categories_with_deals": categories_with_deals,
            "affiliate_tag": AFFILIATE_TAG
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
                d.score
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true
            AND d.discount_pct_90d >= 0.30
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
                d.score
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true
            AND d.discount_pct_90d >= 0.30
            ORDER BY d.discount_pct_90d DESC NULLS LAST
            LIMIT 3
        """)).mappings().all()
    
    return templates.TemplateResponse(
        "category.html", 
        {
            "request": request, 
            "deals": deals,
            "featured_deals": featured_deals,
            "category": "sunniest-savings",
            "category_name": "Sunniest Savings",
            "affiliate_tag": AFFILIATE_TAG
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
        "kitchen": "Kitchen",
        "garden": "Garden",
        "diy": "DIY",
        "toys": "Toys",
        "electrical": "Electronics"
    }
    
    with engine.connect() as conn:
        # Get all deals for this category
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
                d.score
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true 
              AND d.category_slug = :slug
            ORDER BY d.hot_score DESC NULLS LAST
        """), {"slug": slug}).mappings().all()
        
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
                d.score
            FROM deals d
            JOIN products p ON p.asin = d.asin
            WHERE d.is_active = true 
              AND d.category_slug = :slug
            ORDER BY d.discount_pct_90d DESC NULLS LAST
            LIMIT 3
        """), {"slug": slug}).mappings().all()
    
    category_name = category_names.get(slug, slug.title())
    
    return templates.TemplateResponse(
        "category.html", 
        {
            "request": request, 
            "deals": deals, 
            "featured_deals": featured_deals,
            "category": slug,
            "category_name": category_name,
            "affiliate_tag": AFFILIATE_TAG
        }
    )


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


@app.get("/sitemap.xml")
async def sitemap():
    """Generate XML sitemap for search engines"""
    from fastapi.responses import Response
    
    categories = ["beauty", "pet", "health", "baby", "grocery", "sports", 
                  "automotive", "kitchen", "garden", "diy", "toys", "electrical", 
                  "sunniest-savings"]
    
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
