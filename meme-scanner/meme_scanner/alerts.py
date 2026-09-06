"""Alert message formatting (Telegram HTML).

Rule for this file: never let a missing measurement look like a clean one.
Anything the scanner could not determine is printed as "n/a" and listed
under an explicit "not verified" line, because a blank where a check
should be is exactly how someone talks themselves into a bad buy.
"""

from __future__ import annotations

from .models import BundleReport, Candidate, DemandReport, SafetyReport, Verdict
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
    demand: DemandReport,
    verdict: Verdict,
    now_ms: float,
) -> str:
    sym = escape_html(cand.symbol or "?")
    name = escape_html(cand.name or "")

    traders = f"{demand.unique_traders}" if demand.unique_traders is not None else "n/a"
    bundled = f"{bundle.bundled_pct_of_supply:.0f}%" if bundle.bundled_pct_of_supply is not None else "n/a"
    lp = f"{safety.lp_locked_pct:.0f}%" if safety.lp_locked_pct is not None else "n/a"
    top10 = f"{safety.top10_holder_pct:.0f}%" if safety.top10_holder_pct is not None else "n/a"
    holders = f"{safety.total_holders}" if safety.total_holders is not None else "n/a"

    # Say plainly which checks could not be completed.
    gaps = []
    if bundle.bundled_pct_of_supply is None:
        gaps.append("bundled wallets")
    if demand.unique_traders is None:
        gaps.append("unique traders")
    if safety.lp_locked_pct is None:
        gaps.append("LP lock")
    if safety.total_holders is None:
        gaps.append("holder count")

    lines = [
        f"🔎 <b>{sym}</b> — {name}",
        f"Score: <b>{verdict.score:.0f}/100</b> ({escape_html(verdict.summary)})",
        "",
        f"💧 Liq {_usd(cand.liquidity_usd)} | MC {_usd(cand.market_cap_usd or cand.fdv_usd)} | Age {_age(cand, now_ms)}",
        f"📊 1h vol {_usd(cand.vol_h1)} | trades {(cand.buys_h1 or 0) + (cand.sells_h1 or 0)} | traders ~{traders}",
        f"🔒 LP locked {lp} | top10 {top10} | holders {holders} | bundled {bundled}",
        f"🏷 {escape_html(cand.dex_id)}"
        + (" | 🌐 site" if cand.has_website else "")
        + (" | 📣 socials" if cand.has_socials else "")
        + (" | 💰 boosted" if cand.boosted else ""),
    ]
    if gaps:
        lines.append(f"❔ <b>Not verified:</b> {escape_html(', '.join(gaps))} — treat as unknown, not clean.")
    lines += [
        "",
        f'<a href="{escape_html(cand.url)}">chart</a> | <code>{escape_html(cand.mint)}</code>',
        "",
        "⚠️ Passed filters ≠ will pump. Most launches go to zero. Never risk money you can't lose.",
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
