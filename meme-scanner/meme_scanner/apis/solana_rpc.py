"""Solana JSON-RPC adapter — filled in from on-chain research pass."""

from __future__ import annotations

from ..config import Config


def unique_buyers_h1(pair_address: str, cfg: Config) -> int | None:
    """Distinct buying wallets in the last hour. None = couldn't determine."""
    raise NotImplementedError  # replaced after on-chain research lands
