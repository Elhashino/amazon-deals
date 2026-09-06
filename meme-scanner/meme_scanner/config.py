"""Configuration: environment variables with a tiny .env loader.

Every threshold the filters and scorer use lives here so tuning never
means editing logic. Values come from (in order of precedence):
process environment > .env file next to the project root > defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> dict[str, str]:
    """Minimal .env parser: KEY=VALUE lines, # comments, no quoting games."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    # utf-8-sig: Windows Notepad writes a BOM, which would otherwise glue
    # itself to the first key name and silently disable that setting.
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values


@dataclass
class Config:
    # --- Telegram alerts (empty token = console-only mode) ---
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # --- Polling ---
    poll_seconds: int = 60

    # --- Hard kill-filters (fail any one of these -> binned) ---
    min_liquidity_usd: float = 20_000.0
    min_pair_age_minutes: float = 20.0     # let launch-block chaos settle first
    max_pair_age_minutes: float = 1_440.0  # only look at coins < 24h old
    max_top10_holder_pct: float = 30.0     # top 10 non-LP holders, % of supply
    max_single_holder_pct: float = 15.0    # any one non-LP wallet, % of supply
    min_lp_locked_pct: float = 80.0        # LP locked or burned, %
    max_insider_pct: float = 25.0          # RugCheck insider-network holdings, %
    max_bundle_pct: float = 20.0           # supply bought by bundled wallets, %
    max_rugcheck_score: float = 40.0       # RugCheck normalised risk score, higher = worse
    min_holders: int = 100
    min_unique_buyers_h1: int = 50        # distinct trading wallets in 1h
    min_trader_diversity: float = 0.30    # distinct wallets / sampled trades

    # --- Momentum scoring (survivors only) ---
    min_alert_score: float = 60.0          # 0-100; below this we log but stay quiet

    # --- Rug watch on alerted coins ---
    watch_hours: float = 12.0
    rug_liquidity_drop_pct: float = 60.0   # liq down this much since alert -> rug warning

    # --- Optional upgrades ---
    helius_api_key: str = ""               # better RPC limits for bundle checks
    solana_rpc_url: str = "https://api.mainnet-beta.solana.com"

    # --- Storage ---
    data_dir: str = str(PROJECT_ROOT / "data")

    @classmethod
    def load(cls) -> "Config":
        dotenv = _load_dotenv(PROJECT_ROOT / ".env")
        kwargs = {}
        for f in fields(cls):
            env_key = f.name.upper()
            raw = os.environ.get(env_key, dotenv.get(env_key))
            if raw is None or raw == "":
                continue
            if f.type in ("int", int):
                kwargs[f.name] = int(raw)
            elif f.type in ("float", float):
                kwargs[f.name] = float(raw)
            else:
                kwargs[f.name] = raw
        return cls(**kwargs)
