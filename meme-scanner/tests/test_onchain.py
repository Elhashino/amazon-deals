"""Solana RPC helpers and bundle clustering, with the RPC layer mocked."""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import meme_scanner.analysis.bundles as bundles
import meme_scanner.apis.solana_rpc as rpc
from meme_scanner.config import Config

CFG = Config()


# ---------- solana_rpc parsing helpers ----------

def mint_account(mint_auth, freeze_auth):
    return {"value": {"data": {"parsed": {"type": "mint",
            "info": {"mintAuthority": mint_auth, "freezeAuthority": freeze_auth,
                     "supply": "1000", "decimals": 6, "isInitialized": True}},
            "program": "spl-token"}, "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}}


def test_mint_authorities_null_means_renounced(monkeypatch):
    monkeypatch.setattr(rpc, "_call", lambda m, p, c: mint_account(None, None))
    assert rpc.get_mint_authorities("M", CFG) == (False, False)


def test_mint_authorities_string_means_active(monkeypatch):
    monkeypatch.setattr(rpc, "_call", lambda m, p, c: mint_account("Key111", None))
    assert rpc.get_mint_authorities("M", CFG) == (True, False)


def test_mint_authorities_missing_account_is_unknown(monkeypatch):
    monkeypatch.setattr(rpc, "_call", lambda m, p, c: {"value": None})
    assert rpc.get_mint_authorities("M", CFG) == (None, None)


def test_fee_payer_extraction():
    tx = {"transaction": {"message": {"accountKeys": [
        {"pubkey": "PAYER", "signer": True}, {"pubkey": "OTHER", "signer": False}]}}}
    assert rpc.fee_payer(tx) == "PAYER"
    assert rpc.fee_payer(None) is None
    assert rpc.fee_payer({}) is None


def test_find_sol_funder_transfer_and_create():
    def tx_with(parsed_type, info):
        return {"transaction": {"message": {"instructions": [
            {"program": "system", "parsed": {"type": parsed_type, "info": info}}]}},
            "meta": {}}
    t1 = tx_with("transfer", {"source": "RICH", "destination": "W1", "lamports": 5})
    assert rpc.find_sol_funder(t1, "W1") == "RICH"
    assert rpc.find_sol_funder(t1, "W2") is None
    t2 = tx_with("createAccount", {"source": "RICH", "newAccount": "W1"})
    assert rpc.find_sol_funder(t2, "W1") == "RICH"


def test_find_sol_funder_inner_instructions():
    tx = {"transaction": {"message": {"instructions": []}},
          "meta": {"innerInstructions": [{"instructions": [
              {"program": "system",
               "parsed": {"type": "transfer", "info": {"source": "S", "destination": "W"}}}]}]}}
    assert rpc.find_sol_funder(tx, "W") == "S"


def test_wallet_origin_too_active(monkeypatch):
    monkeypatch.setattr(rpc, "get_signatures", lambda a, c, limit=1000, before=None:
                        [{"signature": f"s{i}", "slot": i} for i in range(1000)])
    origin = rpc.wallet_origin("W", CFG)
    assert origin["too_active"] and origin["funder"] is None


def test_creation_slot_shallow_history(monkeypatch):
    monkeypatch.setattr(rpc, "get_signatures", lambda a, c, limit=1000, before=None:
                        [{"signature": "new", "slot": 500}, {"signature": "create", "slot": 100}])
    assert rpc.creation_slot("M", CFG) == 100


def test_creation_slot_too_deep_returns_none(monkeypatch):
    monkeypatch.setattr(rpc, "get_signatures", lambda a, c, limit=1000, before=None:
                        [{"signature": f"s{i}", "slot": i} for i in range(1000)])
    assert rpc.creation_slot("M", CFG, max_pages=2) is None


def test_unique_buyers_estimates_diversity(monkeypatch):
    now = time.time()
    sigs = [{"signature": f"s{i}", "err": None, "blockTime": now - 60} for i in range(80)]
    monkeypatch.setattr(rpc, "get_signatures", lambda a, c, limit=1000, before=None: sigs)
    # wash-trading: 80 txs but only 2 wallets cycling
    monkeypatch.setattr(rpc, "get_transaction", lambda s, c: {
        "transaction": {"message": {"accountKeys": [{"pubkey": f"BOT{hash(s) % 2}"}]}}})
    est = rpc.unique_buyers_h1("PAIR", CFG)
    assert est is not None and est <= 8  # low ratio -> low estimate despite 80 txs


def test_unique_buyers_zero_and_empty(monkeypatch):
    monkeypatch.setattr(rpc, "get_signatures", lambda a, c, limit=1000, before=None: [])
    assert rpc.unique_buyers_h1("PAIR", CFG) == 0
    assert rpc.unique_buyers_h1("", CFG) is None


# ---------- bundle clustering ----------

def wire_bundles(monkeypatch, *, origins, owners=None, plain=None, launch_slot=100):
    monkeypatch.setattr(bundles.rpc, "get_supply_raw", lambda m, c: 1_000_000)
    largest = [{"token_account": f"TA_{w}", "amount": amt}
               for w, amt in owners.items()]
    monkeypatch.setattr(bundles.rpc, "get_largest_token_accounts", lambda m, c: largest)
    monkeypatch.setattr(bundles.rpc, "resolve_owner", lambda ta, c: ta.removeprefix("TA_"))
    monkeypatch.setattr(bundles.rpc, "is_plain_wallet",
                        lambda w, c: (plain or {}).get(w, True))
    monkeypatch.setattr(bundles.rpc, "creation_slot", lambda m, c, max_pages=3: launch_slot)
    monkeypatch.setattr(bundles.rpc, "wallet_origin", lambda w, c: origins[w])


def test_common_funder_cluster_detected(monkeypatch):
    # W1-W3 all funded by the same parent (the 47-wallet scam, miniaturized);
    # W4 is organic.
    owners = {"W1": 80_000, "W2": 70_000, "W3": 50_000, "W4": 60_000}
    origins = {
        "W1": {"funder": "PARENT", "first_slot": 5000, "too_active": False},
        "W2": {"funder": "PARENT", "first_slot": 6000, "too_active": False},
        "W3": {"funder": "PARENT", "first_slot": 7000, "too_active": False},
        "W4": {"funder": "CEX", "first_slot": 9000, "too_active": False},
    }
    wire_bundles(monkeypatch, owners=owners, origins=origins)
    report = bundles.analyze("MINT", CFG)
    assert report.bundled_wallets == 3
    assert report.funder_clusters == 1
    assert report.bundled_pct_of_supply == 20.0  # (80+70+50)k / 1M


def test_holder_funding_holder_is_same_entity(monkeypatch):
    owners = {"W1": 100_000, "W2": 100_000}
    origins = {
        "W1": {"funder": "CEX1", "first_slot": 5000, "too_active": False},
        "W2": {"funder": "W1", "first_slot": 6000, "too_active": False},
    }
    wire_bundles(monkeypatch, owners=owners, origins=origins)
    # W2 funded by W1 -> the 'holder:W1' group has only W2 in it (size 1),
    # so no cluster forms from just this pair-edge... unless another wallet
    # shares it.
    owners3 = dict(owners, W3=50_000)
    origins3 = dict(origins, W3={"funder": "W1", "first_slot": 7000, "too_active": False})
    wire_bundles(monkeypatch, owners=owners3, origins=origins3)
    report = bundles.analyze("MINT", CFG)
    assert report.bundled_wallets == 2  # W2 + W3, both funded by holder W1


def test_launch_snipers_cluster(monkeypatch):
    owners = {"W1": 90_000, "W2": 90_000, "W3": 30_000}
    origins = {
        # first-ever activity within 3 slots of token creation (slot 100)
        "W1": {"funder": "A", "first_slot": 101, "too_active": False},
        "W2": {"funder": "B", "first_slot": 102, "too_active": False},
        "W3": {"funder": "C", "first_slot": 99_999, "too_active": False},
    }
    wire_bundles(monkeypatch, owners=owners, origins=origins, launch_slot=100)
    report = bundles.analyze("MINT", CFG)
    assert report.bundled_wallets == 2
    assert report.bundled_pct_of_supply == 18.0


def test_amm_vault_excluded(monkeypatch):
    owners = {"VAULT": 500_000, "W1": 50_000, "W2": 40_000}
    origins = {
        "W1": {"funder": "X", "first_slot": 5000, "too_active": False},
        "W2": {"funder": "Y", "first_slot": 6000, "too_active": False},
    }
    wire_bundles(monkeypatch, owners=owners, origins=origins,
                 plain={"VAULT": False, "W1": True, "W2": True})
    report = bundles.analyze("MINT", CFG)
    assert report.checked_wallets == 2  # vault never analyzed
    assert report.bundled_wallets == 0


def test_veteran_wallets_not_bundled(monkeypatch):
    owners = {"W1": 100_000, "W2": 100_000}
    origins = {
        "W1": {"funder": None, "first_slot": None, "too_active": True},
        "W2": {"funder": None, "first_slot": None, "too_active": True},
    }
    wire_bundles(monkeypatch, owners=owners, origins=origins)
    report = bundles.analyze("MINT", CFG)
    assert report.bundled_wallets == 0 and report.bundled_pct_of_supply == 0.0
