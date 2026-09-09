"""Alert outcome log: what every alerted coin looked like, and what happened next.

The rejection log records the coins that were binned. This records the ones
that got through, and then keeps watching them — which is the only way to
find out whether the filters select for anything that survives.

Two files, because they answer different questions:

  alerts.csv    one row per alert, holding every feature the scanner had at
                the moment it fired. Written once, never revised.
  outcomes.csv  a liquidity and price snapshot per alerted coin per cycle,
                for as long as the rug-watch holds it.

Kept apart so the feature row stays a clean record of what was known at
decision time. Joining them on mint is what turns "9 of 10 collapsed" into
"and here is what the 9 had in common that the 1 did not".

Same two hardening details as the rejection log: token names are
attacker-supplied and these files get opened in Excel, so cells that look
like formulas are quoted; and both files rotate so a box left running does
not fill its disk.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

ALERT_FIELDS = [
    "utc_time", "mint", "symbol", "name",
    # verdict
    "score", "score_components",
    # market shape at alert
    "dex_id", "price_usd", "liquidity_usd", "market_cap_usd", "fdv_usd",
    "pair_age_minutes",
    "vol_m5", "vol_h1", "vol_h24",
    "buys_m5", "sells_m5", "buys_h1", "sells_h1",
    "price_change_m5", "price_change_h1",
    # identity / promotion
    "has_website", "has_socials", "social_types", "has_telegram", "boosted",
    # derived, for the risk features the literature points at
    "avg_buy_usd_h1",
    # safety
    "lp_locked_pct", "mint_authority_active", "freeze_authority_active",
    "top10_holder_pct", "max_single_holder_pct", "insider_pct",
    "total_holders", "rugcheck_score", "risk_names",
    # bundles
    "bundled_pct_of_supply", "bundled_wallets", "funder_clusters",
    "checked_wallets", "bundle_note",
    # demand
    "unique_traders", "trader_diversity", "trades_sampled", "total_txns",
]

OUTCOME_FIELDS = [
    "utc_time", "mint", "symbol", "minutes_since_alert",
    "liquidity_usd", "price_usd", "market_cap_usd",
    "liquidity_pct_of_alert", "price_pct_of_alert",
]

MAX_BYTES = 5 * 1024 * 1024


def _defuse(value) -> str:
    """Neutralize spreadsheet formula injection from token names."""
    text = "" if value is None else str(value)
    if text[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + text
    return text


class _Csv:
    def __init__(self, path: Path, fields: list[str]) -> None:
        self.path = path
        self.fields = fields
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.is_file():
            with self.path.open("w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow(self.fields)
            return
        self._migrate_header()

    def _migrate_header(self) -> None:
        """Bring an existing file up to the current columns.

        Adding a field to FIELDS would otherwise append rows carrying more
        values than the header on disk names, silently misaligning every
        column after the insertion point for anything reading the file back.
        Rows already written are rewritten against the new header with the
        columns they predate left empty, which keeps history readable instead
        of trading it for the new fields.
        """
        try:
            with self.path.open(encoding="utf-8", newline="") as fh:
                rows = list(csv.reader(fh))
        except OSError as exc:
            print(f"  [WARN] could not read {self.path.name} to migrate: {exc}")
            return
        if not rows or rows[0] == self.fields:
            return

        old = rows[0]
        merged = [dict(zip(old, r)) for r in rows[1:] if r]
        tmp = self.path.with_suffix(".migrating.csv")
        try:
            # Write beside the original and swap, so an interruption cannot
            # leave the log half-rewritten.
            with tmp.open("w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(self.fields)
                for record in merged:
                    w.writerow([record.get(f, "") for f in self.fields])
            tmp.replace(self.path)
            added = [f for f in self.fields if f not in old]
            dropped = [f for f in old if f not in self.fields]
            note = (f"+{len(added)}" if added else "") + (f" -{len(dropped)}" if dropped else "")
            print(f"  [log] {self.path.name}: columns updated ({note.strip()}), "
                  f"{len(merged)} existing rows kept")
        except OSError as exc:
            print(f"  [WARN] could not migrate {self.path.name}: {exc}")
            try:
                tmp.unlink()
            except OSError:
                pass

    def append(self, row: dict) -> None:
        try:
            try:
                if self.path.stat().st_size >= MAX_BYTES:
                    self.path.replace(self.path.with_suffix(".prev.csv"))
                    self._ensure_header()
            except OSError:
                pass
            with self.path.open("a", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow([_defuse(row.get(f)) for f in self.fields])
        except OSError as exc:
            # A full or locked disk must never stop the scan loop.
            print(f"  [WARN] could not write {self.path.name}: {exc}")


class OutcomeLog:
    def __init__(self, data_dir: str) -> None:
        self.alerts = _Csv(Path(data_dir) / "alerts.csv", ALERT_FIELDS)
        self.outcomes = _Csv(Path(data_dir) / "outcomes.csv", OUTCOME_FIELDS)

    @staticmethod
    def _now() -> str:
        return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def alert(self, cand, safety, bundle, demand, verdict, age_minutes) -> None:
        """Record everything known about a coin at the moment it alerted."""
        self.alerts.append({
            "utc_time": self._now(),
            "mint": cand.mint, "symbol": cand.symbol, "name": cand.name,
            "score": f"{verdict.score:.0f}" if verdict else None,
            "score_components": verdict.components if verdict else None,
            "dex_id": cand.dex_id,
            "price_usd": cand.price_usd,
            "liquidity_usd": cand.liquidity_usd,
            "market_cap_usd": cand.market_cap_usd,
            "fdv_usd": cand.fdv_usd,
            "pair_age_minutes": f"{age_minutes:.1f}" if age_minutes is not None else None,
            "vol_m5": cand.vol_m5, "vol_h1": cand.vol_h1, "vol_h24": cand.vol_h24,
            "buys_m5": cand.buys_m5, "sells_m5": cand.sells_m5,
            "buys_h1": cand.buys_h1, "sells_h1": cand.sells_h1,
            "price_change_m5": cand.price_change_m5,
            "price_change_h1": cand.price_change_h1,
            "has_website": cand.has_website, "has_socials": cand.has_socials,
            "social_types": "|".join(cand.social_types),
            "has_telegram": cand.has_telegram,
            "boosted": cand.boosted,
            "avg_buy_usd_h1": (f"{cand.avg_buy_usd_h1:.2f}"
                               if cand.avg_buy_usd_h1 is not None else None),
            "lp_locked_pct": getattr(safety, "lp_locked_pct", None),
            "mint_authority_active": getattr(safety, "mint_authority_active", None),
            "freeze_authority_active": getattr(safety, "freeze_authority_active", None),
            "top10_holder_pct": getattr(safety, "top10_holder_pct", None),
            "max_single_holder_pct": getattr(safety, "max_single_holder_pct", None),
            "insider_pct": getattr(safety, "insider_pct", None),
            "total_holders": getattr(safety, "total_holders", None),
            "rugcheck_score": getattr(safety, "rugcheck_score", None),
            "risk_names": "|".join(getattr(safety, "risk_names", None) or []),
            "bundled_pct_of_supply": getattr(bundle, "bundled_pct_of_supply", None),
            "bundled_wallets": getattr(bundle, "bundled_wallets", None),
            "funder_clusters": getattr(bundle, "funder_clusters", None),
            "checked_wallets": getattr(bundle, "checked_wallets", None),
            "bundle_note": getattr(bundle, "note", None),
            "unique_traders": getattr(demand, "unique_traders", None),
            "trader_diversity": getattr(demand, "diversity", None),
            "trades_sampled": getattr(demand, "sampled", None),
            "total_txns": getattr(demand, "total_txns", None),
        })

    def snapshot(self, mint: str, symbol: str, minutes: float, cand,
                 liq_at_alert: float | None, price_at_alert: float | None) -> None:
        """Record where an alerted coin stands now, relative to where it alerted."""
        liq = getattr(cand, "liquidity_usd", None) if cand else None
        price = getattr(cand, "price_usd", None) if cand else None

        def pct(now_value, then_value):
            if now_value is None or not then_value:
                return None
            return f"{100.0 * now_value / then_value:.1f}"

        self.outcomes.append({
            "utc_time": self._now(),
            "mint": mint, "symbol": symbol,
            "minutes_since_alert": f"{minutes:.0f}",
            "liquidity_usd": liq,
            "price_usd": price,
            "market_cap_usd": getattr(cand, "market_cap_usd", None) if cand else None,
            "liquidity_pct_of_alert": pct(liq, liq_at_alert),
            "price_pct_of_alert": pct(price, price_at_alert),
        })
