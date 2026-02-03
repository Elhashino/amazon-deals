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
    """Homepage showing top deals"""
    with engine.connect() as conn:
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
            ORDER BY d.hot_score DESC NULLS LAST
            LIMIT 500
        """)).mappings().all()
    
    return templates.TemplateResponse(
        "home.html", 
        {
            "request": request, 
            "deals": deals,
            "affiliate_tag": AFFILIATE_TAG
        }
    )


@app.get("/category/{slug}", response_class=HTMLResponse)
async def category(request: Request, slug: str):
    """Category page showing filtered deals"""
    
    # Category display names
    category_names = {
        "beauty": "Beauty & Personal Care",
        "pet": "Pet Supplies",
        "health": "Health & Household",
        "baby": "Baby Products",
        "grocery": "Grocery & Gourmet",
        "sports": "Sports & Outdoors",
        "automotive": "Automotive",
        "home": "Home & Garden",
        "kitchen": "Kitchen & Dining",
        "diy": "DIY & Tools",
        "toys": "Toys & Games",
        "electrical": "Electronics"
    }
    
    with engine.connect() as conn:
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
    
    category_name = category_names.get(slug, slug.title())
    
    return templates.TemplateResponse(
        "category.html", 
        {
            "request": request, 
            "deals": deals, 
            "category": slug,
            "category_name": category_name,
            "affiliate_tag": AFFILIATE_TAG
        }
    )


@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    """About page"""
    return templates.TemplateResponse("about.html", {"request": request})


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
