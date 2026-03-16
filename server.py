"""
Plaid MCP server — exposes Plaid data as tools for Claude Desktop.

Tools:
  - sync_transactions(start_date, end_date)  → list of transactions
  - get_account_balances()                   → list of account balances
  - list_items()                             → info about the connected bank

Register in Claude Desktop config:
  "plaid": {
    "command": "/Users/calvinkorver/Code/plaid-mpc/.venv/bin/python",
    "args": ["/Users/calvinkorver/Code/plaid-mpc/server.py"],
    "env": {
      "PLAID_CLIENT_ID": "...",
      "PLAID_SECRET": "...",
      "PLAID_ACCESS_TOKEN": "...",
      "PLAID_ENV": "production"
    }
  }
"""

from dotenv import load_dotenv
load_dotenv()

import hashlib
import os
from datetime import date

import plaid
from plaid.api import plaid_api
from plaid.model.transactions_sync_request import TransactionsSyncRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.country_code import CountryCode
from cryptography.fernet import Fernet

from mcp.server.fastmcp import FastMCP
import db
import categories

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]
PLAID_ENV = os.environ.get("PLAID_ENV", "production").lower()

ENV_MAP = {
    "sandbox": plaid.Environment.Sandbox,
    "production": plaid.Environment.Production,
}

if PLAID_ENV not in ENV_MAP:
    raise ValueError(f"PLAID_ENV must be 'sandbox' or 'production', got: {PLAID_ENV!r}")

configuration = plaid.Configuration(
    host=ENV_MAP[PLAID_ENV],
    api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
)
api_client = plaid.ApiClient(configuration)
plaid_client = plaid_api.PlaidApi(api_client)

_fernet = Fernet(os.environ["PLAID_ENCRYPTION_KEY"].encode())


def _encrypt_token(token: str) -> str:
    return _fernet.encrypt(token.encode()).decode()


def _decrypt_token(token_enc: str) -> str:
    return _fernet.decrypt(token_enc.encode()).decode()


def _get_institution_name(token: str) -> str:
    """Fetch institution name for a token (2 Plaid API calls)."""
    item = plaid_client.item_get(ItemGetRequest(access_token=token))["item"]
    inst = plaid_client.institutions_get_by_id(
        InstitutionsGetByIdRequest(
            institution_id=item["institution_id"],
            country_codes=[CountryCode("US")],
        )
    )["institution"]
    return inst["name"]


def _get_all_tokens() -> list[tuple[str, str]]:
    """Return [(token_hash, plaintext_token)] for all stored items."""
    return [
        (h, _decrypt_token(db.get_encrypted_token(h)))
        for h in db.get_all_token_hashes()
    ]


def _bootstrap_tokens() -> None:
    """
    On first run: read PLAID_ACCESS_TOKENS (or PLAID_ACCESS_TOKEN) from env,
    encrypt each, and store in the items table.
    After first run the env var is no longer needed.
    """
    if db.get_all_token_hashes():
        return  # already seeded
    raw = os.environ.get("PLAID_ACCESS_TOKENS") or os.environ.get("PLAID_ACCESS_TOKEN", "")
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    for token in tokens:
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        institution = _get_institution_name(token)
        db.upsert_item(token_hash, _encrypt_token(token), institution)


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP("plaid")

db.init_db()
_bootstrap_tokens()


@mcp.tool()
def sync_transactions(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch transactions between start_date and end_date from all linked accounts.

    Uses Plaid's /transactions/sync endpoint with full pagination across all
    stored tokens. Filters results to the requested date range after fetching.

    Returns a list of transaction dicts with keys:
      transaction_id, date, name, merchant_name, amount, category,
      account_id, pending
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    all_filtered: list = []
    for _token_hash, token in _get_all_tokens():
        all_added: list = []
        cursor = ""
        while True:
            kwargs: dict = {"access_token": token}
            if cursor:
                kwargs["cursor"] = cursor
            response = plaid_client.transactions_sync(TransactionsSyncRequest(**kwargs))
            all_added.extend(response["added"])
            if not response["has_more"]:
                break
            cursor = response["next_cursor"]

        all_filtered.extend([
            {
                "transaction_id": t["transaction_id"],
                "date": str(t["date"]),
                "name": t["name"],
                "merchant_name": t.get("merchant_name") or "",
                "amount": t["amount"],
                "category": (
                    t["personal_finance_category"]["primary"]
                    if t.get("personal_finance_category")
                    else (t.get("category") or [""])[0]
                ),
                "account_id": t["account_id"],
                "pending": t["pending"],
            }
            for t in all_added
            if start <= t["date"] <= end
        ])

    return all_filtered


@mcp.tool()
def get_account_balances() -> list[dict]:
    """
    Fetch real-time balances for all accounts across all linked tokens.

    Returns a list of account dicts with keys:
      account_id, name, official_name, type, subtype,
      current_balance, available_balance, currency
    """
    result = []
    for _token_hash, token in _get_all_tokens():
        response = plaid_client.accounts_balance_get(
            AccountsBalanceGetRequest(access_token=token)
        )
        result.extend([
            {
                "account_id": acct["account_id"],
                "name": acct["name"],
                "official_name": acct.get("official_name") or "",
                "type": str(acct["type"]),
                "subtype": str(acct["subtype"]),
                "current_balance": acct["balances"]["current"],
                "available_balance": acct["balances"]["available"],
                "currency": acct["balances"]["iso_currency_code"],
            }
            for acct in response["accounts"]
        ])
    return result


@mcp.tool()
def list_items() -> list[dict]:
    """
    Return info about all connected bank items (one per linked token).

    Makes two Plaid API calls per token:
      1. /item/get — to get the item and institution_id
      2. /institutions/get_by_id — to resolve the institution name

    Returns a list of dicts with keys:
      item_id, institution_id, institution_name,
      available_products, billed_products, consent_expiration
    """
    result = []
    for _token_hash, token in _get_all_tokens():
        item_response = plaid_client.item_get(ItemGetRequest(access_token=token))
        item = item_response["item"]
        institution_id = item["institution_id"]

        inst_response = plaid_client.institutions_get_by_id(
            InstitutionsGetByIdRequest(
                institution_id=institution_id,
                country_codes=[CountryCode("US")],
            )
        )
        institution = inst_response["institution"]

        result.append({
            "item_id": item["item_id"],
            "institution_id": institution_id,
            "institution_name": institution["name"],
            "available_products": [str(p) for p in item.get("available_products") or []],
            "billed_products": [str(p) for p in item.get("billed_products") or []],
            "consent_expiration": str(item.get("consent_expiration_time") or "N/A"),
        })
    return result


@mcp.tool()
def sync_and_store() -> dict:
    """
    Incrementally sync Plaid transactions for all linked accounts into the local ledger.

    Uses a saved cursor per token so only new/changed transactions are fetched.
    On the first run (no cursor saved) fetches full history.
    Handles added, modified, and removed transactions from Plaid.

    Returns: {"added": N, "modified": N, "removed": N, "auto_categorized": N}
    """
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

    totals = {"added": 0, "modified": 0, "removed": 0}

    for token_hash, token in _get_all_tokens():
        # 1. Upsert account metadata for this token
        institution = _get_institution_name(token)
        acct_resp = plaid_client.accounts_balance_get(
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
                response = plaid_client.transactions_sync(TransactionsSyncRequest(**kwargs))
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
                    response = plaid_client.transactions_sync(
                        TransactionsSyncRequest(**kwargs)
                    )
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

    # First apply legacy plaid_category_map-based categorization to fill in
    # obvious defaults, then run manual rules which can further specialize.
    totals["auto_categorized"] = db.apply_auto_categorization()
    totals["rules_applied"] = db.apply_rules_to_new_transactions()
    # Finally, use Claude to validate/assign categories for anything still uncategorized
    import classifier
    totals["claude_categorized"] = classifier.apply_claude_categorization_from_db()
    return totals


@mcp.tool()
def get_stored_transactions(
    start_date: str, end_date: str, category: str = "", account_id: str = ""
) -> list[dict]:
    """
    Query the local ledger. Makes no Plaid API call.

    Args:
        start_date: YYYY-MM-DD
        end_date:   YYYY-MM-DD
        category:   Optional. Filters by custom_category (if set) or plaid_category.
        account_id: Optional. Filters by Plaid account_id.

    Returns a list of transaction dicts including account_name.
    """
    return db.query_transactions(start_date, end_date, category, account_id)


@mcp.tool()
def get_all_accounts() -> list[dict]:
    """
    Return all synced accounts from the local ledger. Makes no Plaid API call.

    Returns a list of account dicts with keys:
      account_id, name, official_name, type, subtype, mask, institution, token_hash
    """
    return db.get_all_accounts()


@mcp.tool()
def update_category(transaction_id: str, category: str) -> dict:
    """
    Set a custom category for a transaction, overriding Plaid's built-in category.

    Use consistent labels: Food & Dining, Groceries, Transport, Shopping,
    Entertainment, Health, Utilities, Rent, Income, Transfer, Other

    Returns: {"ok": true/false, "transaction_id": ..., "category": ...}
    """
    ok = db.set_custom_category(transaction_id, category)
    return {"ok": ok, "transaction_id": transaction_id, "category": category}


@mcp.tool()
def get_uncategorized(limit: int = 50) -> list[dict]:
    """
    Return transactions where custom_category has not been set.

    Useful for reviewing edge cases where the auto-mapping produced no result
    (e.g., unknown plaid_category values not in the map).

    Args:
        limit: Max rows to return (default 50)
    """
    return db.get_uncategorized(limit)


@mcp.tool()
def get_spending_summary(month: str) -> list[dict]:
    """
    Aggregate spending by category for a given month.

    Args:
        month: YYYY-MM format, e.g. "2026-02"

    Returns list of {category, total, count} sorted by total descending.
    Only includes posted (non-pending) debits (positive amounts).
    """
    return db.get_spending_summary(month)


@mcp.tool()
def set_budget(category: str, monthly_amount: float) -> dict:
    """
    Set or update the monthly spending budget for a category.

    Args:
        category:       Category name, e.g. "Food & Dining"
        monthly_amount: Target spending limit in dollars

    Returns: {"ok": True, "category": ..., "monthly_amount": ...}
    """
    db.set_budget(category, monthly_amount)
    return {"ok": True, "category": category, "monthly_amount": monthly_amount}


@mcp.tool()
def get_budget_status(month: str) -> list[dict]:
    """
    Compare actual spending to budgets for a given month.

    Args:
        month: YYYY-MM format, e.g. "2026-03"

    Returns [{category, budgeted, spent, remaining, over_budget}].
    Over-budget categories appear first. Categories with spending but no
    budget are also included (budgeted=0).
    """
    return db.get_budget_status(month)


@mcp.tool()
def set_category_mapping(plaid_category: str, custom_category: str) -> dict:
    """
    Override the default Plaid→custom mapping for a specific Plaid primary category.

    Takes effect immediately: also retroactively applies to any existing
    transactions that still have custom_category IS NULL.

    Args:
        plaid_category:  Plaid primary value, e.g. "GENERAL_SERVICES"
        custom_category: Your label, e.g. "Subscriptions"

    Returns: {"ok": true, "plaid_category": ..., "custom_category": ..., "retroactively_applied": N}
    """
    db.set_category_mapping(plaid_category, custom_category)
    retroactively_applied = db.apply_auto_categorization()
    return {
        "ok": True,
        "plaid_category": plaid_category,
        "custom_category": custom_category,
        "retroactively_applied": retroactively_applied,
    }


@mcp.tool()
def add_category(name: str, parent: str = "") -> dict:
    """
    Add a new custom category or sub-category to the taxonomy.

    Sub-categories are one level deep only. If parent is provided,
    it must already exist as a top-level category.

    Args:
        name:   New category label, e.g. "Coffee"
        parent: Parent category name, e.g. "Food & Dining" (optional)

    Returns: {"ok": true/false, "name": ..., "parent": ..., "error": ...}
    """
    try:
        ok = db.add_category(name, parent)
        return {"ok": ok, "name": name, "parent": parent}
    except ValueError as e:
        return {"ok": False, "error": str(e), "name": name, "parent": parent}


if __name__ == "__main__":
    mcp.run()
