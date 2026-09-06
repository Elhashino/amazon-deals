"""Demo mode: watch the scanner work, without touching the live APIs.

Everything here is REAL scanner code — the same filters, the same scoring,
the same alert text you'd get on your phone. Only the market data is fake:
six invented launches, each built to look like a pattern that actually
shows up on Solana every day.

Run it with:  python demo.py
"""

from __future__ import annotations

import re
import tempfile
import time

from meme_scanner import main as scanner
from meme_scanner.config import Config
from meme_scanner.models import BundleReport, Candidate, DemandReport, SafetyReport
from meme_scanner.rejection_log import RejectionLog
from meme_scanner.state import State
from meme_scanner.telegram import Alerter


def mins_ago(m: float) -> int:
    return int((time.time() - m * 60) * 1000)


def coin(mint, symbol, name, **kw) -> Candidate:
    base = dict(
        pair_address=f"PAIR_{symbol}", dex_id="pumpswap",
        url=f"https://dexscreener.com/solana/{mint}",
        pair_created_at_ms=mins_ago(90),
    )
    base.update(kw)
    return Candidate(mint=mint, symbol=symbol, name=name, **base)


def safe(**kw) -> SafetyReport:
    base = dict(
        mint_authority_active=False, freeze_authority_active=False,
        lp_locked_pct=100.0, top10_holder_pct=16.0, max_single_holder_pct=4.0,
        insider_pct=2.0, total_holders=620, rugcheck_score=9.0, risk_names=[],
    )
    base.update(kw)
    return SafetyReport(**base)


# --- six launches, each a pattern you'd meet for real ---------------------
SCENARIOS = {
    "RUGPULL1": {
        "why": "the classic: dev can still mint more supply, LP not locked",
        "cand": coin("RUGPULL1", "MOONX", "MoonShot X", liquidity_usd=45_000,
                     fdv_usd=300_000, market_cap_usd=300_000, vol_m5=8_000, vol_h1=60_000,
                     buys_m5=90, sells_m5=20, buys_h1=600, sells_h1=200),
        "safety": safe(mint_authority_active=True, lp_locked_pct=0.0,
                       risk_names=["Mint Authority still enabled", "Large Amount of LP Unlocked"]),
        "bundle": BundleReport(checked_wallets=12, bundled_wallets=0, bundled_pct_of_supply=3.0),
        "demand": DemandReport(unique_traders=180, diversity=0.8, sampled=40, total_txns=700),
    },
    "BUNDLED1": {
        "why": "looks clean on paper — until you trace who funded the wallets",
        "cand": coin("BUNDLED1", "SAFEDOG", "Safe Dog", liquidity_usd=80_000,
                     fdv_usd=500_000, market_cap_usd=500_000, vol_m5=14_000, vol_h1=95_000,
                     buys_m5=110, sells_m5=30, buys_h1=780, sells_h1=260,
                     has_website=True, has_socials=True),
        "safety": safe(top10_holder_pct=27.0),   # under the 30% cap!
        "bundle": BundleReport(checked_wallets=12, bundled_wallets=7,
                               bundled_pct_of_supply=41.0, funder_clusters=2),
        "demand": DemandReport(unique_traders=210, diversity=0.75, sampled=40, total_txns=800),
    },
    "WASHED01": {
        "why": "huge volume, but it's a few bots trading with themselves",
        "cand": coin("WASHED01", "PUMPKING", "Pump King", liquidity_usd=60_000,
                     fdv_usd=400_000, market_cap_usd=400_000, vol_m5=40_000, vol_h1=350_000,
                     buys_m5=300, sells_m5=280, buys_h1=1900, sells_h1=1800),
        "safety": safe(),
        "bundle": BundleReport(checked_wallets=12, bundled_wallets=1, bundled_pct_of_supply=6.0),
        "demand": DemandReport(unique_traders=4, diversity=0.06, sampled=40, total_txns=3700),
    },
    "TOOYOUNG": {
        "why": "6 minutes old — too early to judge, so we wait, we don't bin it",
        "cand": coin("TOOYOUNG", "FRESH", "Fresh Launch", liquidity_usd=9_000,
                     pair_created_at_ms=mins_ago(6), vol_m5=3_000, buys_m5=40, sells_m5=8),
        "safety": safe(total_holders=45),
        "bundle": BundleReport(),
        "demand": DemandReport(),
    },
    "MEHCOIN1": {
        "why": "nothing wrong with it — just no momentum worth waking you up for",
        "cand": coin("MEHCOIN1", "MEHCOIN", "Meh Coin", liquidity_usd=25_000,
                     fdv_usd=1_800_000, market_cap_usd=1_800_000, vol_m5=300, vol_h1=22_000,
                     buys_m5=6, sells_m5=20, buys_h1=140, sells_h1=260),
        "safety": safe(total_holders=150),
        "bundle": BundleReport(checked_wallets=12, bundled_wallets=0, bundled_pct_of_supply=1.0),
        "demand": DemandReport(unique_traders=70, diversity=0.7, sampled=40, total_txns=400),
    },
    "SURVIVOR": {
        "why": "passes every check with real, distributed buying — this is the alert",
        "cand": coin("SURVIVOR", "WIFCAP", "Dog Wif Cap", liquidity_usd=110_000,
                     fdv_usd=620_000, market_cap_usd=620_000, vol_m5=26_000, vol_h1=140_000,
                     buys_m5=190, sells_m5=55, buys_h1=1150, sells_h1=380,
                     has_website=True, has_socials=True, boosted=True),
        "safety": safe(total_holders=1400, top10_holder_pct=12.0, max_single_holder_pct=3.0),
        "bundle": BundleReport(checked_wallets=12, bundled_wallets=0,
                               bundled_pct_of_supply=2.0, funder_clusters=0),
        "demand": DemandReport(unique_traders=540, diversity=0.92, sampled=40, total_txns=1500),
    },
}


class PhonePreviewAlerter(Alerter):
    """Prints the alert the way it reads on your phone, not as raw HTML."""

    def send(self, html_text: str) -> bool:
        text = re.sub(r"<a href=\"([^\"]*)\">([^<]*)</a>", r"\2: \1", html_text)
        text = re.sub(r"</?(b|i|code)>", "", text)
        text = (text.replace("&amp;", "&").replace("&lt;", "<")
                    .replace("&gt;", ">").replace("&quot;", '"').replace("&#39;", "'"))
        print("\n   ┌─── 📱 TELEGRAM ALERT " + "─" * 32)
        for line in text.splitlines():
            print("   │ " + line)
        print("   └" + "─" * 54 + "\n")
        return True


def install_fake_market() -> None:
    """Point the scanner's data sources at the fixtures above."""
    scanner.dexscreener.discover_mints = lambda: {m: False for m in SCENARIOS}
    scanner.dexscreener.get_candidates = lambda mints: {
        m: SCENARIOS[m]["cand"] for m in mints if m in SCENARIOS
    }
    scanner.dexscreener.get_candidate = lambda m: SCENARIOS.get(m, {}).get("cand")
    scanner.rugcheck.new_token_mints = lambda: []
    scanner.rugcheck.get_safety = lambda m: SCENARIOS[m]["safety"]
    scanner.bundles.analyze = lambda m, cfg: SCENARIOS[m]["bundle"]
    scanner.solana_rpc.unique_traders_h1 = lambda pair, cfg: next(
        s["demand"] for s in SCENARIOS.values() if s["cand"].pair_address == pair
    )


def main() -> None:
    print("=" * 68)
    print("  MEME-SCANNER DEMO — real filters, invented coins")
    print("  Nothing here is a live market. This shows you what the tool DOES.")
    print("=" * 68)
    print("\nSix launches just appeared. Watch what happens to each:\n")
    for mint, s in SCENARIOS.items():
        print(f"  • {s['cand'].symbol:9s} — {s['why']}")
    print("\n" + "-" * 68 + "\n")

    install_fake_market()
    with tempfile.TemporaryDirectory() as tmp:
        cfg = Config(data_dir=tmp)
        state = State(tmp)
        scanner.run_cycle(cfg, state, RejectionLog(tmp), PhonePreviewAlerter("", ""))

        # Anything neither binned nor alerted was DEFERRED — the scanner is
        # holding off, not ignoring it. It gets re-checked every minute.
        deferred = [m for m in SCENARIOS if not state.is_seen(m)]
        for mint in deferred:
            print(f"  [WAITING] {SCENARIOS[mint]['cand'].symbol} — too young to judge; "
                  f"re-checked next cycle, not binned")

        binned = sum(1 for m in SCENARIOS if state.is_seen(m) and not state.is_alerted(m))
        alerted = sum(1 for m in SCENARIOS if state.is_alerted(m))

    print("\n" + "-" * 68)
    print(f"\n  {binned} binned (each with a written reason)  |  "
          f"{len(deferred)} waiting  |  {alerted} alert\n")
    print("On your PC that alert goes to your phone instead of the screen,")
    print("and every binned coin is appended to data/rejections.csv — that")
    print("file is the bit that teaches you how these launches are built.\n")


if __name__ == "__main__":
    main()
