"""Solana JSON-RPC adapter (free public RPC, or Helius with a key).

Verified constraints this code respects:
- Public RPC: ~100 req/10s per IP and 40 req/10s per method — one global
  throttle at ~3.3 req/s keeps us under both with margin. Helius free (10 RPS) uses the
  same pacing; it's the reliability that improves, not our appetite.
- getTokenLargestAccounts returns TOKEN ACCOUNT addresses, not wallets —
  each needs a getAccountInfo to resolve parsed.info.owner.
- getTransaction errors on version-0 transactions (most of them) unless
  maxSupportedTransactionVersion: 0 is passed.
- Amounts are strings; do integer math, never float.
- On a jsonParsed mint, mintAuthority/freezeAuthority keys are always
  present; null means revoked (safe).
"""

from __future__ import annotations

import time
from collections import Counter

from ..config import Config
from ..models import DemandReport
from .http import ApiError, post_json

SYSTEM_PROGRAM = "11111111111111111111111111111111"

_MIN_INTERVAL = 0.3  # ~3.3 req/s — margin under the 40-req/10s per-method cap
_last_call = 0.0


def _rpc_url(cfg: Config) -> str:
    if cfg.helius_api_key:
        return f"https://mainnet.helius-rpc.com/?api-key={cfg.helius_api_key}"
    return cfg.solana_rpc_url


def _call(method: str, params: list, cfg: Config):
    global _last_call
    wait = _MIN_INTERVAL - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = post_json(_rpc_url(cfg), body)
    if not isinstance(resp, dict) or "error" in resp:
        err = (resp or {}).get("error", "malformed response")
        raise ApiError(f"RPC {method}: {err}")
    return resp.get("result")


# ---------- basic reads ----------

def get_mint_authorities(mint: str, cfg: Config) -> tuple[bool | None, bool | None]:
    """(mint_authority_active, freeze_authority_active); None = couldn't read.
    Ground truth straight from chain — used when RugCheck hasn't indexed yet."""
    result = _call("getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}], cfg)
    value = (result or {}).get("value")
    if not value:
        return None, None
    parsed = ((value.get("data") or {}).get("parsed") or {})
    if parsed.get("type") != "mint":
        return None, None
    info = parsed.get("info") or {}
    # Keys are always present on a parsed mint; null == revoked.
    return info.get("mintAuthority") is not None, info.get("freezeAuthority") is not None


def get_supply_raw(mint: str, cfg: Config) -> int | None:
    result = _call("getTokenSupply", [mint], cfg)
    amount = ((result or {}).get("value") or {}).get("amount")
    try:
        return int(amount)
    except (TypeError, ValueError):
        return None


def get_largest_token_accounts(mint: str, cfg: Config) -> list[dict]:
    """Top-20 token accounts: [{'token_account': str, 'amount': int}, ...]."""
    result = _call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}], cfg)
    out = []
    for entry in (result or {}).get("value") or []:
        try:
            out.append({"token_account": entry["address"], "amount": int(entry["amount"])})
        except (KeyError, TypeError, ValueError):
            continue
    return out


def resolve_owner(token_account: str, cfg: Config) -> str | None:
    """Token account -> owning wallet (parsed.info.owner)."""
    result = _call("getAccountInfo", [token_account, {"encoding": "jsonParsed"}], cfg)
    value = (result or {}).get("value")
    if not value:
        return None
    parsed = ((value.get("data") or {}).get("parsed") or {})
    if parsed.get("type") != "account":
        return None
    return (parsed.get("info") or {}).get("owner")


def is_plain_wallet(pubkey: str, cfg: Config) -> bool | None:
    """True if the account is a system-owned wallet. Program-owned addresses
    (bonding-curve PDAs, AMM pool authorities) are infrastructure, not holders."""
    result = _call("getAccountInfo", [pubkey, {"encoding": "jsonParsed"}], cfg)
    value = (result or {}).get("value")
    if value is None:
        return True  # no account data at all: an unfunded wallet, still a wallet
    return value.get("owner") == SYSTEM_PROGRAM


def get_signatures(address: str, cfg: Config, limit: int = 1000, before: str | None = None) -> list[dict]:
    opts: dict = {"limit": limit, "commitment": "confirmed"}
    if before:
        opts["before"] = before
    result = _call("getSignaturesForAddress", [address, opts], cfg)
    return result if isinstance(result, list) else []


def get_transaction(signature: str, cfg: Config) -> dict | None:
    return _call(
        "getTransaction",
        [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                     "commitment": "confirmed"}],
        cfg,
    )


# ---------- derived reads ----------

def fee_payer(tx: dict | None) -> str | None:
    keys = (((tx or {}).get("transaction") or {}).get("message") or {}).get("accountKeys") or []
    if keys and isinstance(keys[0], dict):
        return keys[0].get("pubkey")
    return None


def _iter_parsed_instructions(tx: dict):
    msg = ((tx.get("transaction") or {}).get("message") or {})
    for ins in msg.get("instructions") or []:
        if isinstance(ins, dict):
            yield ins
    for inner in (tx.get("meta") or {}).get("innerInstructions") or []:
        for ins in (inner or {}).get("instructions") or []:
            if isinstance(ins, dict):
                yield ins


def find_sol_funder(tx: dict | None, wallet: str) -> str | None:
    """Who sent SOL to `wallet` in this transaction (system transfer/createAccount)."""
    if not tx:
        return None
    for ins in _iter_parsed_instructions(tx):
        if ins.get("program") != "system":
            continue
        parsed = ins.get("parsed") or {}
        info = parsed.get("info") or {}
        kind = parsed.get("type")
        if kind in ("transfer", "transferChecked") and info.get("destination") == wallet:
            return info.get("source")
        if kind == "createAccount" and info.get("newAccount") == wallet:
            return info.get("source")
    return None


def wallet_origin(wallet: str, cfg: Config) -> dict:
    """The wallet's oldest transaction: who funded it, and in which slot it
    first acted. Returns {funder, first_slot, too_active}.
    A wallet with >=1000 signatures is an old/busy wallet — funding trace is
    skipped (and such wallets are poor bundle candidates anyway)."""
    sigs = get_signatures(wallet, cfg, limit=1000)
    if not sigs:
        return {"funder": None, "first_slot": None, "too_active": False}
    if len(sigs) >= 1000:
        return {"funder": None, "first_slot": None, "too_active": True}
    oldest = sigs[-1]  # newest-first ordering
    tx = get_transaction(oldest.get("signature", ""), cfg)
    return {
        "funder": find_sol_funder(tx, wallet),
        "first_slot": oldest.get("slot"),
        "too_active": False,
    }


def creation_slot(mint: str, cfg: Config, max_pages: int = 3) -> int | None:
    """Slot of the mint's oldest (creation) transaction, or None if its
    history is too deep to walk within max_pages (1000 sigs/page)."""
    before = None
    for _ in range(max_pages):
        page = get_signatures(mint, cfg, limit=1000, before=before)
        if not page:
            return None
        if len(page) < 1000:
            return page[-1].get("slot")
        before = page[-1].get("signature")
    return None  # too deep — signal unavailable rather than wrong


def unique_traders_h1(pair_address: str, cfg: Config, sample_size: int = 40) -> DemandReport:
    """Estimate distinct trading wallets on a pool over the last hour.

    Counting every trader exactly would cost one RPC call per transaction,
    so we sample. The estimator matters: naively scaling
    (distinct in sample / sampled) x (total txs) is exactly wrong for the
    adversary this metric exists to catch — two bots printing 1000 trades
    would sample as 2 distinct in 40, and scale back up to 50 "traders".

    So repeats within the sample are treated as the signal they are:
      - saw every sampled wallet exactly once -> the pool is unsaturated,
        we have not seen its size, extrapolate linearly;
      - saw repeats -> use the Chao1 richness lower bound, which stays
        small precisely when a few wallets do all the trading.
    The diversity ratio is reported alongside so the caller can reject
    wash-trading patterns outright.
    """
    if not pair_address:
        return DemandReport()
    now = time.time()
    sigs = get_signatures(pair_address, cfg, limit=1000)
    in_hour = [
        s for s in sigs
        if s.get("err") is None
        and isinstance(s.get("blockTime"), (int, float))
        and now - s["blockTime"] <= 3600
    ]
    total = len(in_hour)
    if total == 0:
        return DemandReport(unique_traders=0, diversity=None, sampled=0, total_txns=0)

    take = in_hour if total <= sample_size else in_hour[:: max(1, total // sample_size)][:sample_size]
    payers: list[str] = []
    fetched = 0
    for sig in take:
        try:
            payer = fee_payer(get_transaction(sig.get("signature", ""), cfg))
        except ApiError:
            continue
        fetched += 1
        if payer:
            payers.append(payer)

    if fetched < min(10, total):  # too little signal to estimate from
        return DemandReport(unique_traders=None, diversity=None, sampled=fetched, total_txns=total)

    counts = Counter(payers)
    distinct = len(counts)
    diversity = distinct / fetched if fetched else None

    if fetched >= total:
        estimate = distinct  # we saw the whole hour; no estimation needed
    elif distinct == fetched:
        # No wallet repeated: the sample never saturated, so the population
        # is larger than what we saw — linear extrapolation is the honest
        # read, capped by the number of trades that actually happened.
        estimate = min(total, round(distinct * total / fetched))
    else:
        singles = sum(1 for c in counts.values() if c == 1)
        doubles = sum(1 for c in counts.values() if c == 2)
        chao1 = distinct + (singles * singles) / (2 * doubles) if doubles else distinct
        estimate = int(min(total, chao1))

    return DemandReport(
        unique_traders=estimate, diversity=diversity, sampled=fetched, total_txns=total
    )
