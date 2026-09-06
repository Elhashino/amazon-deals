"""Internal data model. API clients normalize their responses into these
shapes; filters and scoring only ever see these — so an API changing a
field name breaks one adapter, not the whole pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Candidate:
    """A new pair pulled from market data, normalized."""

    mint: str                      # base token mint address
    symbol: str = ""
    name: str = ""
    pair_address: str = ""
    dex_id: str = ""               # e.g. raydium, pumpswap, meteora
    url: str = ""                  # DexScreener page
    price_usd: float | None = None
    liquidity_usd: float | None = None
    fdv_usd: float | None = None
    market_cap_usd: float | None = None
    pair_created_at_ms: int | None = None
    vol_m5: float | None = None
    vol_h1: float | None = None
    vol_h24: float | None = None
    buys_m5: int | None = None
    sells_m5: int | None = None
    buys_h1: int | None = None
    sells_h1: int | None = None
    price_change_m5: float | None = None
    price_change_h1: float | None = None
    has_website: bool = False
    has_socials: bool = False
    boosted: bool = False          # paid DexScreener boost — a flag, not a virtue


@dataclass
class SafetyReport:
    """Contract/holder safety, normalized from RugCheck + on-chain checks."""

    mint_authority_active: bool | None = None   # None = unknown
    freeze_authority_active: bool | None = None
    lp_locked_pct: float | None = None
    top10_holder_pct: float | None = None       # excluding LP/AMM accounts
    max_single_holder_pct: float | None = None  # excluding LP/AMM accounts
    insider_pct: float | None = None
    total_holders: int | None = None
    rugcheck_score: float | None = None         # normalised 0-100, higher = worse
    risk_names: list[str] = field(default_factory=list)
    rugged_flag: bool = False                   # RugCheck says already rugged


@dataclass
class BundleReport:
    """Bundled-wallet analysis of top holders."""

    checked_wallets: int = 0
    bundled_wallets: int = 0
    bundled_pct_of_supply: float | None = None  # supply held by bundled cluster
    funder_clusters: int = 0                    # distinct common-funder groups
    note: str = ""


@dataclass
class DemandReport:
    """Trading-wallet diversity over the last hour, from a sampled estimate.

    unique_traders counts distinct wallets that traded (buyers AND sellers —
    the fee payer of a swap is the trader either way), so it is honestly
    named: it is not a buyer-only count.
    """

    unique_traders: int | None = None   # None = couldn't determine
    diversity: float | None = None      # distinct wallets / sampled txs, 0..1
    sampled: int = 0                    # transactions actually fetched
    total_txns: int = 0                 # pool transactions in the hour


@dataclass
class Verdict:
    score: float
    components: dict[str, float]
    summary: str
