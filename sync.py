"""
Plaid sync logic for the Flask UI server.

Extracted into a standalone module with lazy Plaid initialization so that
ui.py can import it without triggering the MCP server's module-level setup.

Orchestrates the full pipeline per sync:
  Plaid → upsert → auto_categorization → rules → Claude categorization
"""

from dotenv import load_dotenv
load_dotenv()

import hashlib
import logging
import os

import plaid
from plaid.api import plaid_api
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.country_code import CountryCode
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from cryptography.fernet import Fernet

import classifier
import db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy Plaid client initialization
# ---------------------------------------------------------------------------

_plaid_client: plaid_api.PlaidApi | None = None
_fernet_inst: Fernet | None = None


def _ensure_client() -> tuple[plaid_api.PlaidApi, Fernet]:
    global _plaid_client, _fernet_inst
    if _plaid_client is None:
        client_id = os.environ["PLAID_CLIENT_ID"]
        secret = os.environ["PLAID_SECRET"]
        env_name = os.environ.get("PLAID_ENV", "production").lower()
        env_map = {
            "sandbox": plaid.Environment.Sandbox,
            "production": plaid.Environment.Production,
        }
        if env_name not in env_map:
            raise ValueError(f"PLAID_ENV must be 'sandbox' or 'production', got: {env_name!r}")
        configuration = plaid.Configuration(
            host=env_map[env_name],
            api_key={"clientId": client_id, "secret": secret},
        )
        api_client = plaid.ApiClient(configuration)
        _plaid_client = plaid_api.PlaidApi(api_client)
        _fernet_inst = Fernet(os.environ["PLAID_ENCRYPTION_KEY"].encode())
    return _plaid_client, _fernet_inst


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------

def _encrypt_token(token: str) -> str:
    _, fernet = _ensure_client()
    return fernet.encrypt(token.encode()).decode()


def _decrypt_token(token_enc: str) -> str:
    _, fernet = _ensure_client()
    return fernet.decrypt(token_enc.encode()).decode()


def _get_all_tokens() -> list[tuple[str, str]]:
    """Return [(token_hash, plaintext_token)] for all stored items."""
    return [
        (h, _decrypt_token(db.get_encrypted_token(h)))
        for h in db.get_all_token_hashes()
    ]


def _get_institution_name(token: str) -> str:
    client, _ = _ensure_client()
    item = client.item_get(ItemGetRequest(access_token=token))["item"]
    inst = client.institutions_get_by_id(
        InstitutionsGetByIdRequest(
            institution_id=item["institution_id"],
            country_codes=[CountryCode("US")],
        )
    )["institution"]
    return inst["name"]


def _bootstrap_tokens() -> None:
    """
    On first run: read PLAID_ACCESS_TOKENS (or PLAID_ACCESS_TOKEN) from env,
    encrypt each, and store in the items table.
    """
    if db.get_all_token_hashes():
        return
    raw = os.environ.get("PLAID_ACCESS_TOKENS") or os.environ.get("PLAID_ACCESS_TOKEN", "")
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    for token in tokens:
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        institution = _get_institution_name(token)
        db.upsert_item(token_hash, _encrypt_token(token), institution)


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------

def sync_and_store() -> dict:
    """
    Incrementally sync Plaid transactions for all linked accounts.

    Pipeline:
      1. Fetch new/modified/removed transactions from Plaid
      2. Upsert to DB; collect added_ids
      3. Apply plaid_category_map auto-categorization
      4. Snapshot categories for added_ids (pre-rules)
      5. Apply manual rules
      6. Snapshot categories again (post-rules)
      7. Identify rule-matched ids (category changed between snapshots)
      8. Send (added_ids - rule_matched) to Claude for validation/assignment
      9. Write audit log entries for Claude decisions

    Returns:
      {added, modified, removed, auto_categorized, rules_applied, claude_categorized}
    """
    _ensure_client()
    _bootstrap_tokens()

    def _row(t) -> dict:
        return {
            "transaction_id": t["transaction_id"],
            "date": str(t["date"]),
            "name": t["name"],
            "merchant_name": t.get("merchant_name") or "",
            "amount": t["amount"],
            "plaid_category": (
                t["personal_finance_category"]["primary"]
                if t.get("personal_finance_category")
                else (t.get("category") or [""])[0]
            ),
            "account_id": t["account_id"],
            "pending": t["pending"],
        }

    client, _ = _ensure_client()
    totals = {"added": 0, "modified": 0, "removed": 0, "snapshots_taken": 0}
    all_added_ids: list[str] = []

    for token_hash, token in _get_all_tokens():
        # 1. Upsert account metadata
        institution = _get_institution_name(token)
        acct_resp = client.accounts_balance_get(
            AccountsBalanceGetRequest(access_token=token)
        )
        db.upsert_accounts([
            {
                "account_id": a["account_id"],
                "name": a["name"],
                "official_name": a.get("official_name") or "",
                "type": str(a["type"]),
                "subtype": str(a["subtype"]),
                "mask": a.get("mask") or "",
                "institution": institution,
                "token_hash": token_hash,
            }
            for a in acct_resp["accounts"]
        ])

        # Snapshot balances from the already-fetched acct_resp (zero extra API calls)
        for a in acct_resp["accounts"]:
            balances = a["balances"]
            db.upsert_balance_snapshot(
                account_id=a["account_id"],
                balance=balances["current"] or 0.0,
                available=balances.get("available"),
                currency=balances.get("iso_currency_code") or "USD",
                account_type=str(a["type"]),
                account_subtype=str(a["subtype"]),
            )
            totals["snapshots_taken"] += 1

        # 2. Sync transactions
        cursor = db.get_cursor(token_hash)
        all_added: list = []
        all_modified: list = []
        all_removed: list = []
        next_cursor = cursor

        try:
            while True:
                kwargs: dict = {"access_token": token}
                if next_cursor:
                    kwargs["cursor"] = next_cursor
                response = client.transactions_sync(TransactionsSyncRequest(**kwargs))
                all_added.extend(response["added"])
                all_modified.extend(response["modified"])
                all_removed.extend(response["removed"])
                next_cursor = response["next_cursor"]
                if not response["has_more"]:
                    break
        except plaid.ApiException as e:
            body = str(e)
            if "TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION" in body:
                next_cursor = ""
                all_added, all_modified, all_removed = [], [], []
                while True:
                    kwargs = {"access_token": token}
                    response = client.transactions_sync(TransactionsSyncRequest(**kwargs))
                    all_added.extend(response["added"])
                    all_modified.extend(response["modified"])
                    all_removed.extend(response["removed"])
                    next_cursor = response["next_cursor"]
                    if not response["has_more"]:
                        break
            else:
                raise

        db.upsert_transactions([_row(t) for t in all_added])
        db.upsert_transactions([_row(t) for t in all_modified])
        db.delete_transactions([t["transaction_id"] for t in all_removed])
        db.save_cursor(token_hash, next_cursor)

        totals["added"] += len(all_added)
        totals["modified"] += len(all_modified)
        totals["removed"] += len(all_removed)
        all_added_ids.extend(t["transaction_id"] for t in all_added)

    # 3. Snapshot BEFORE any categorization — new transactions start as NULL
    pre_snap = db.get_custom_categories_for_ids(all_added_ids)

    # 4. Apply manual rules FIRST (user-defined, highest priority)
    totals["rules_applied"] = db.apply_rules_to_new_transactions()

    # 5. Snapshot after rules to detect which transactions were rule-matched
    post_rules_snap = db.get_custom_categories_for_ids(all_added_ids)
    rule_matched_ids = {
        tx_id for tx_id in all_added_ids
        if pre_snap.get(tx_id) != post_rules_snap.get(tx_id)
    }

    # 6. Apply plaid_category_map to fill remaining NULL (those not matched by rules)
    totals["auto_categorized"] = db.apply_auto_categorization()

    # 7. Build Claude candidate list (new transactions minus rule-matched and hidden)
    hidden_ids = db.get_hidden_transaction_ids(all_added_ids)
    claude_candidate_ids = [
        tx_id for tx_id in all_added_ids
        if tx_id not in rule_matched_ids and tx_id not in hidden_ids
    ]

    candidates: list[dict] = []
    if claude_candidate_ids:
        # Fetch full transaction rows (just upserted, so they exist in DB)
        added_tx_map = _fetch_transactions_by_ids(claude_candidate_ids)
        # Use post-auto_cat categories (auto_cat may have filled some NULLs)
        post_autocat_snap = db.get_custom_categories_for_ids(claude_candidate_ids)
        for tx_id in claude_candidate_ids:
            tx = added_tx_map.get(tx_id)
            if tx:
                candidates.append({
                    "transaction_id": tx_id,
                    "name": tx.get("name") or "",
                    "merchant_name": tx.get("merchant_name") or "",
                    "custom_name": tx.get("custom_name"),
                    "custom_merchant_name": tx.get("custom_merchant_name"),
                    "amount": tx.get("amount", 0.0),
                    "current_category": post_autocat_snap.get(tx_id),
                })

    # 9. Claude categorization
    totals["claude_categorized"] = classifier.apply_claude_categorization(candidates)

    return totals


def _fetch_transactions_by_ids(transaction_ids: list[str]) -> dict[str, dict]:
    """Fetch full transaction rows for the given IDs. Returns {tx_id: row_dict}."""
    if not transaction_ids:
        return {}
    import sqlite3
    from pathlib import Path
    placeholders = ",".join("?" * len(transaction_ids))
    conn = sqlite3.connect(db.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"SELECT * FROM transactions WHERE transaction_id IN ({placeholders})",
            transaction_ids,
        ).fetchall()
    finally:
        conn.close()
    return {r["transaction_id"]: dict(r) for r in rows}
