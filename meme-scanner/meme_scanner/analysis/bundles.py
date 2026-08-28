"""Bundled-wallet detection.

The scam this catches: one entity splits its bag across dozens of wallets
before or at launch so every holder-concentration check looks clean, then
dumps all of them inside the same hour. The wallets can be made to LOOK
independent; making them BE independent is expensive. Two on-chain
fingerprints survive the disguise:

  1. Common funding parent — the wallets were topped up with SOL from the
     same source (directly, or one top holder funded another).
  2. Launch-window birth — the wallet's first-ever transaction lands
     within a couple of slots of the token's creation: pre-arranged
     sniping, usually via the same Jito bundle as the launch itself.

Wallets sharing either fingerprint form a cluster; supply held by
multi-wallet clusters is the bundled percentage.

Cost: ~4 RPC calls per analyzed holder, bounded, throttled. Only coins
that already passed the market and safety filters get this far, so the
budget lands on maybe a handful of tokens per hour.
"""

from __future__ import annotations

from collections import defaultdict

from ..config import Config
from ..models import BundleReport
from ..apis import solana_rpc as rpc

MAX_HOLDERS_ANALYZED = 12   # top non-infra holders traced (4 RPC calls each)
CREATION_WINDOW_SLOTS = 3   # first activity within this many slots of launch = sniper


def analyze(mint: str, cfg: Config) -> BundleReport:
    supply = rpc.get_supply_raw(mint, cfg)
    if not supply:
        return BundleReport(note="could not read token supply")

    largest = rpc.get_largest_token_accounts(mint, cfg)
    if not largest:
        return BundleReport(note="could not read largest accounts")

    # Resolve token accounts -> owner wallets, drop program-owned addresses
    # (bonding-curve PDAs, AMM vaults — infrastructure, not holders).
    holders: list[dict] = []  # {wallet, pct}
    seen_wallets: set[str] = set()
    for entry in largest:
        if len(holders) >= MAX_HOLDERS_ANALYZED:
            break
        owner = rpc.resolve_owner(entry["token_account"], cfg)
        if not owner or owner in seen_wallets:
            continue
        if rpc.is_plain_wallet(owner, cfg) is False:
            continue
        seen_wallets.add(owner)
        holders.append({"wallet": owner, "pct": 100.0 * entry["amount"] / supply})

    if not holders:
        return BundleReport(note="no plain-wallet holders resolved")

    launch_slot = rpc.creation_slot(mint, cfg)

    # Trace each holder's origin.
    funder_groups: dict[str, list[dict]] = defaultdict(list)
    launch_snipers: list[dict] = []
    holder_wallets = {h["wallet"] for h in holders}
    for holder in holders:
        origin = rpc.wallet_origin(holder["wallet"], cfg)
        if origin["too_active"]:
            continue  # an old, busy wallet is not a fresh bundle wallet
        funder = origin["funder"]
        if funder:
            # A holder funded by another top holder is the same entity.
            key = funder if funder not in holder_wallets else f"holder:{funder}"
            funder_groups[key].append(holder)
        if (
            launch_slot is not None
            and origin["first_slot"] is not None
            and 0 <= origin["first_slot"] - launch_slot <= CREATION_WINDOW_SLOTS
        ):
            launch_snipers.append(holder)

    bundled: dict[str, dict] = {}
    clusters = 0
    for group in funder_groups.values():
        if len(group) >= 2:
            clusters += 1
            for holder in group:
                bundled[holder["wallet"]] = holder
    if len(launch_snipers) >= 2:
        clusters += 1
        for holder in launch_snipers:
            bundled[holder["wallet"]] = holder

    return BundleReport(
        checked_wallets=len(holders),
        bundled_wallets=len(bundled),
        bundled_pct_of_supply=round(sum(h["pct"] for h in bundled.values()), 2),
        funder_clusters=clusters,
        note=f"launch_slot={'unknown' if launch_slot is None else launch_slot}",
    )
