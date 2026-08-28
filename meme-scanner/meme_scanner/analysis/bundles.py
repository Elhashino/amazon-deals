"""Bundled-wallet detection — filled in from on-chain research pass."""

from __future__ import annotations

from ..config import Config
from ..models import BundleReport


def analyze(mint: str, cfg: Config) -> BundleReport:
    raise NotImplementedError  # replaced after on-chain research lands
