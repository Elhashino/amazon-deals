"""Alert message formatting (Telegram HTML)."""

from __future__ import annotations

from .models import BundleReport, Candidate, SafetyReport, Verdict
from .telegram import escape_html


def _usd(v: float | None) -> str:
    if v is None:
        return "?"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}k"
    return f"${v:.0f}"


def _age(cand: Candidate, now_ms: float) -> str:
    if cand.pair_created_at_ms is None:
        return "?"
    minutes = (now_ms - cand.pair_created_at_ms) / 60_000
    return f"{minutes / 60:.1f}h" if minutes >= 90 else f"{minutes:.0f}m"


def format_alert(
    cand: Candidate,
    safety: SafetyReport,
    bundle: BundleReport,
    unique_buyers_h1: int | None,
    verdict: Verdict,
    now_ms: float,
) -> str:
    sym = escape_html(cand.symbol or "?")
    name = escape_html(cand.name or "")
    buyers = str(unique_buyers_h1) if unique_buyers_h1 is not None else "n/a"
    bundled = (
        f"{bundle.bundled_pct_of_supply:.0f}%" if bundle.bundled_pct_of_supply is not None else "n/a"
    )
    lp = f"{safety.lp_locked_pct:.0f}%" if safety.lp_locked_pct is not None else "?"
    top10 = f"{safety.top10_holder_pct:.0f}%" if safety.top10_holder_pct is not None else "?"

    lines = [
        f"🔎 <b>{sym}</b> — {name}",
        f"Score: <b>{verdict.score:.0f}/100</b> ({escape_html(verdict.summary)})",
        "",
        f"💧 Liq {_usd(cand.liquidity_usd)} | MC {_usd(cand.market_cap_usd or cand.fdv_usd)} | Age {_age(cand, now_ms)}",
        f"📊 1h vol {_usd(cand.vol_h1)} | buys/sells {cand.buys_h1 or 0}/{cand.sells_h1 or 0} | buyers {buyers}",
        f"🔒 LP locked {lp} | top10 {top10} | holders {safety.total_holders or '?'} | bundled {bundled}",
        f"🏷 {escape_html(cand.dex_id)}"
        + (" | 🌐 site" if cand.has_website else "")
        + (" | 📣 socials" if cand.has_socials else "")
        + (" | 💰 boosted" if cand.boosted else ""),
        "",
        f'<a href="{escape_html(cand.url)}">chart</a> | <code>{escape_html(cand.mint)}</code>',
        "",
        "⚠️ Passed filters ≠ will pump. Never risk money you can't lose.",
    ]
    return "\n".join(lines)


def format_rug_warning(cand: Candidate, liq_at_alert: float, liq_now: float) -> str:
    drop = 100.0 * (1 - liq_now / liq_at_alert) if liq_at_alert else 0.0
    return (
        f"🚨 <b>RUG WARNING: {escape_html(cand.symbol or cand.mint[:8])}</b>\n"
        f"Liquidity {_usd(liq_at_alert)} → {_usd(liq_now)} (−{drop:.0f}%) since alert.\n"
        f"If you're in, decide NOW.\n"
        f'<a href="{escape_html(cand.url)}">chart</a> | <code>{escape_html(cand.mint)}</code>'
    )
