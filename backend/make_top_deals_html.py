import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Generate a simple static HTML file you can open in your browser to sanity-check results.
# Usage (from backend folder, with venv active):
#   python .\make_top_deals_html.py

load_dotenv(".env")

db_url = os.environ.get("DATABASE_URL")
if not db_url:
    raise SystemExit("DATABASE_URL not set. Put it in backend/.env")

engine = create_engine(db_url)

# Pull more rows so percentile-based "wildcard" logic has enough data
q = text(
    """
    select
      d.asin,
      p.title,
      p.image_url,
      d.category_slug,
      d.hot_score,
      d.score,
      d.demand_score,
      d.discount_pct_90d,
      d.price_current,
      d.price_median_90d,
      d.sales_rank_current,
      d.rank_drops_7d,
      d.rating,
      d.review_count,
      d.ingested_at,
      d.published_at
    from deals d
    join products p on p.asin = d.asin
    where d.is_active = true
    order by d.hot_score desc nulls last, d.score desc nulls last
    limit 2000
    """
)

with engine.connect() as conn:
    rows = conn.execute(q).mappings().all()

# ----------------------------
# Formatting helpers
# ----------------------------
def _pct(x):
    return "" if x is None else f"{float(x) * 100:.1f}%"


def _f(x, nd=2):
    if x is None:
        return ""
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return str(x)


def _as_float(x):
    try:
        return None if x is None else float(x)
    except Exception:
        return None


def _as_int(x):
    try:
        return None if x is None else int(x)
    except Exception:
        return None


def _rating5(x):
    """
    Normalise rating to a 0–5 scale.
    If stored as 0.0–1.0, multiply by 10 (because your max_raw is ~0.50 -> 5.0),
    otherwise assume already 0–5.
    """
    x = _as_float(x)
    if x is None:
        return None
    return x * 10.0 if x <= 1.0 else x


def _dt_str(dt):
    """
    Render a datetime consistently.
    - If timezone-aware -> show UTC
    - If naive -> show as "naive" (still useful)
    """
    if dt is None:
        return ""
    try:
        # SQLAlchemy returns datetime objects here
        if getattr(dt, "tzinfo", None) is not None:
            dt_utc = dt.astimezone(timezone.utc)
            return dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)


def _max_dt(rows_list, key):
    dts = [r.get(key) for r in rows_list if r.get(key) is not None]
    if not dts:
        return None
    try:
        return max(dts)
    except Exception:
        # if types mismatch, fallback to string comparison
        return sorted(dts, key=lambda x: str(x))[-1]


# ----------------------------
# Publish rules (tune these)
# ----------------------------
MIN_TRUSTED_REVIEWS = 100
MIN_TRUSTED_RATING = 4.0

# minimum reviews for wildcards to prevent low-review noise
MIN_WILDCARD_REVIEWS = 25  # try 10 / 25 / 50

# Wildcard exceptions: keep weird bargains only if they have strong signals
EXC_DISCOUNT_PCT_90D = 0.60  # 60% (note discount_pct_90d is stored as 0.60, not 60)
EXC_RANK_DROPS_7D = 1000     # adjust to your scale
HOT_SCORE_TOP_PERCENT = 10   # top 10% hot_score becomes a wildcard exception

# Mega bargains: "stupidly good" price drops
MEGA_MIN_DISCOUNT_PCT_90D = 0.70   # 70%+
MEGA_MIN_SAVINGS_GBP = 20.0        # or save at least £20 vs median 90d (if median present)
MEGA_MIN_REVIEWS = 10              # keep some social proof; set 0 to allow everything

# Optional: minimum price to publish (set to 0 to allow everything)
MIN_PRICE_GBP = 0.0

# Title junk filters (keep conservative; expand as you observe patterns)
EXCLUDE_TITLE_KEYWORDS = [
    "refurbished",
    "renewed",
    "used",
    "replacement",
    "compatible",
    "spare",
    "spares",
    "part",
    "parts",
]


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def is_junk_title(title: str) -> bool:
    t = _norm(title)
    return any(k in t for k in EXCLUDE_TITLE_KEYWORDS)


def hot_score_cutoff(rows_list):
    hs = [_as_float(r.get("hot_score")) for r in rows_list]
    hs = [x for x in hs if x is not None]
    if not hs:
        return None
    hs.sort()  # ascending
    # 90th percentile for "top 10%"
    p = 1.0 - (HOT_SCORE_TOP_PERCENT / 100.0)
    idx = int(round(p * (len(hs) - 1)))
    idx = max(0, min(idx, len(hs) - 1))
    return hs[idx]


HOT_CUTOFF = hot_score_cutoff(rows)


def passes_price_floor(r) -> bool:
    price = _as_float(r.get("price_current"))
    if price is None:
        return False
    return price >= MIN_PRICE_GBP


def is_exception(r) -> bool:
    # Wildcard gate: only allow if at least one strong signal is true
    disc = _as_float(r.get("discount_pct_90d")) or 0.0
    drops = _as_int(r.get("rank_drops_7d")) or 0
    hot = _as_float(r.get("hot_score"))

    hot_ok = (HOT_CUTOFF is not None) and (hot is not None) and (hot >= HOT_CUTOFF)
    return (disc >= EXC_DISCOUNT_PCT_90D) or (drops >= EXC_RANK_DROPS_7D) or hot_ok


def is_trusted(r) -> bool:
    # Main publish list: rating + review_count required
    if not passes_price_floor(r):
        return False

    title = r.get("title") or ""
    if is_junk_title(title):
        return False

    rating = _rating5(r.get("rating"))
    reviews = _as_int(r.get("review_count"))

    if rating is None or reviews is None:
        return False

    return (rating >= MIN_TRUSTED_RATING) and (reviews >= MIN_TRUSTED_REVIEWS)


def is_wildcard(r) -> bool:
    # Wildcards: allowed only if "exception" is true.
    # Can include missing rating, but require a minimum review count to avoid noise.
    if not passes_price_floor(r):
        return False

    title = r.get("title") or ""
    if is_junk_title(title):
        return False

    if is_trusted(r):
        return False

    reviews = _as_int(r.get("review_count")) or 0
    if reviews < MIN_WILDCARD_REVIEWS:
        return False

    return is_exception(r)


def is_mega_bargain(r) -> bool:
    """
    Mega bargains: not strict on rating/reviews (but we do enforce a small floor),
    and focus on big discount / big savings.
    This is meant to catch "too good to miss" deals even if not trusted.
    """
    if not passes_price_floor(r):
        return False

    title = r.get("title") or ""
    if is_junk_title(title):
        return False

    # Don't duplicate trusted/wildcards
    if is_trusted(r) or is_wildcard(r):
        return False

    disc = _as_float(r.get("discount_pct_90d")) or 0.0
    cur = _as_float(r.get("price_current"))
    med = _as_float(r.get("price_median_90d"))
    savings = None
    if cur is not None and med is not None:
        savings = max(0.0, med - cur)

    reviews = _as_int(r.get("review_count")) or 0
    if reviews < MEGA_MIN_REVIEWS:
        return False

    return (disc >= MEGA_MIN_DISCOUNT_PCT_90D) or ((savings is not None) and (savings >= MEGA_MIN_SAVINGS_GBP))


trusted = [r for r in rows if is_trusted(r)]
wildcards = [r for r in rows if is_wildcard(r)]
mega = [r for r in rows if is_mega_bargain(r)]

# ----------------------------
# HTML
# ----------------------------
generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

html = [
    "<html><head><meta charset='utf-8'><title>Top Deals</title>",
    "<style>"
    "body{font-family:Arial,sans-serif;margin:24px;background:#f5f5f5}"
    ".card{border:1px solid #ddd;border-radius:10px;padding:16px;margin:12px 0;background:white;display:flex;gap:16px;align-items:flex-start;box-shadow:0 2px 4px rgba(0,0,0,0.08)}"
    ".card-image{flex-shrink:0;width:140px;height:140px;object-fit:contain;border-radius:8px;background:white;border:1px solid #e0e0e0}"
    ".card-content{flex-grow:1;min-width:0}"
    ".card-title{font-size:16px;font-weight:600;margin:0 0 12px 0;color:#111;line-height:1.4}"
    ".meta{color:#555;font-size:13px;margin-top:8px;line-height:1.6}"
    ".k{display:inline-block;min-width:140px;color:#333;font-weight:500}"
    ".section{margin-top:32px;background:white;padding:20px;border-radius:10px;box-shadow:0 2px 4px rgba(0,0,0,0.08)}"
    ".tag{display:inline-block;padding:4px 12px;border-radius:999px;border:1px solid #bbb;font-size:12px;margin-right:8px;margin-bottom:8px;font-weight:600}"
    ".tag.trusted{border-color:#2e7d32;background:#e8f5e9;color:#2e7d32}"
    ".tag.wild{border-color:#ef6c00;background:#fff3e0;color:#ef6c00}"
    ".tag.mega{border-color:#1565c0;background:#e3f2fd;color:#1565c0}"
    ".small{font-size:12px;color:#666;margin-top:4px}"
    ".amazon-link{display:inline-block;margin-top:10px;padding:10px 18px;background:#ff9900;color:white;text-decoration:none;border-radius:6px;font-size:14px;font-weight:600;transition:background 0.2s}"
    ".amazon-link:hover{background:#e68a00}"
    "h1{color:#111;margin-bottom:8px}"
    "h2{color:#111;margin:0 0 12px 0;font-size:20px}"
    "</style>",
    "</head><body>",
    "<h1>Top Deals (sanity check)</h1>",
    f"<p class='small'><b>Generated:</b> {generated_at}</p>",
    "<p>Sorted by Hot Score (deal + demand). Open items in Amazon to verify prices.</p>",
    f"<p><b>Rules:</b> Trusted = rating ≥ {MIN_TRUSTED_RATING} and reviews ≥ {MIN_TRUSTED_REVIEWS}. "
    f"Wildcards = exception (discount ≥ {int(EXC_DISCOUNT_PCT_90D*100)}% or rank drops ≥ {EXC_RANK_DROPS_7D} "
    f"or hot_score in top {HOT_SCORE_TOP_PERCENT}%), plus reviews ≥ {MIN_WILDCARD_REVIEWS}. "
    f"Mega = discount ≥ {int(MEGA_MIN_DISCOUNT_PCT_90D*100)}% OR savings ≥ £{_f(MEGA_MIN_SAVINGS_GBP,0)}, plus reviews ≥ {MEGA_MIN_REVIEWS}.</p>",
    f"<p><b>Counts:</b> Trusted {len(trusted)} | Wildcards {len(wildcards)} | Mega {len(mega)} | Total pulled {len(rows)} "
    f"| Hot cutoff (top {HOT_SCORE_TOP_PERCENT}%): {_f(HOT_CUTOFF) if HOT_CUTOFF is not None else 'n/a'}</p>",
]


def _badge(section: str) -> str:
    if section == "TRUSTED":
        return "<span class='tag trusted'>TRUSTED</span>"
    if section == "WILDCARD":
        return "<span class='tag wild'>WILDCARD</span>"
    if section == "MEGA":
        return "<span class='tag mega'>MEGA</span>"
    return ""


def render_cards(section_title: str, rows_list, section_tag: str):
    last_ing = _max_dt(rows_list, "ingested_at")
    last_pub = _max_dt(rows_list, "published_at")

    html.append("<div class='section'>")
    html.append(f"<h2>{section_title}</h2>")
    html.append(
        "<div class='small'>"
        f"<b>Latest ingested in this section:</b> {_dt_str(last_ing) if last_ing else 'n/a'}"
        f" &nbsp; | &nbsp; <b>Latest published:</b> {_dt_str(last_pub) if last_pub else 'n/a'}"
        "</div>"
    )
    html.append("</div>")

    if not rows_list:
        html.append("<p><i>No items matched.</i></p>")
        return

    for r in rows_list:
        title = (r.get("title") or r["asin"]).replace("<", "&lt;").replace(">", "&gt;")
        asin = r["asin"]
        url = f"https://www.amazon.co.uk/dp/{asin}"
        
        # Get image URL - use placeholder if not available
        image_url = r.get("image_url") or ""
        if not image_url:
            # Use Amazon's no-image placeholder
            image_url = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='140' height='140'%3E%3Crect fill='%23f0f0f0' width='140' height='140'/%3E%3Ctext x='50%25' y='50%25' text-anchor='middle' dy='.3em' fill='%23999' font-family='Arial' font-size='14'%3ENo Image%3C/text%3E%3C/svg%3E"

        ing = r.get("ingested_at")
        pub = r.get("published_at")

        cur = _as_float(r.get("price_current"))
        med = _as_float(r.get("price_median_90d"))
        savings = ""
        if cur is not None and med is not None:
            savings = f"£{_f(max(0.0, med - cur),2)}"

        html.append("<div class='card'>")
        html.append(f"<img src='{image_url}' alt='Product image' class='card-image' onerror=\"this.src='data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 width=%27140%27 height=%27140%27%3E%3Crect fill=%27%23f0f0f0%27 width=%27140%27 height=%27140%27/%3E%3Ctext x=%2750%25%27 y=%2750%25%27 text-anchor=%27middle%27 dy=%27.3em%27 fill=%27%23999%27 font-family=%27Arial%27 font-size=%2714%27%3ENo Image%3C/text%3E%3C/svg%3E'\">")
        html.append("<div class='card-content'>")
        html.append(f"<div>{_badge(section_tag)}</div>")
        html.append(f"<div class='card-title'>{title}</div>")
        html.append(
            "<div class='meta'>"
            f"<span class='k'>ASIN</span>{asin} | "
            f"<span class='k'>Category</span>{r.get('category_slug','')} | "
            f"<span class='k'>Hot</span>{_f(r.get('hot_score'))} | "
            f"<span class='k'>Deal</span>{_f(r.get('score'))} | "
            f"<span class='k'>Demand</span>{_f(r.get('demand_score'))}"
            "</div>"
        )
        html.append(
            "<div class='meta'>"
            f"<span class='k'>Current</span>£{_f(r.get('price_current'),2)} | "
            f"<span class='k'>90d median</span>£{_f(r.get('price_median_90d'),2)} | "
            f"<span class='k'>Savings vs 90d</span>{savings} | "
            f"<span class='k'>90d discount</span>{_pct(r.get('discount_pct_90d'))}"
            "</div>"
        )
        html.append(
            "<div class='meta'>"
            f"<span class='k'>Rank</span>{_f(r.get('sales_rank_current'),0)} | "
            f"<span class='k'>Rank drops 7d</span>{r.get('rank_drops_7d','')} | "
            f"<span class='k'>Rating</span>{_f(_rating5(r.get('rating')),1)} | "
            f"<span class='k'>Reviews</span>{r.get('review_count','')}"
            "</div>"
        )
        html.append(
            "<div class='meta'>"
            f"<span class='k'>Ingested at</span>{_dt_str(ing) or 'n/a'} | "
            f"<span class='k'>Published at</span>{_dt_str(pub) or 'n/a'}"
            "</div>"
        )
        html.append(f"<a href='{url}' target='_blank' class='amazon-link'>Open on Amazon</a>")
        html.append("</div>")  # close card-content
        html.append("</div>")  # close card


render_cards("Trusted (publishable)", trusted, "TRUSTED")
render_cards("Wildcards (manual review)", wildcards, "WILDCARD")
render_cards("Mega bargains (big drops; looser trust)", mega, "MEGA")

html.append("</body></html>")

out_path = os.path.abspath("top_deals.html")
with open(out_path, "w", encoding="utf-8") as f:
    f.write("\n".join(html))

print(out_path)
