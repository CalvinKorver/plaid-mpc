"""
SQLite helpers for the Plaid ledger.

Database is created at the same directory as this file (ledger.db),
so the path is stable regardless of working directory.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "ledger.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id  TEXT PRIMARY KEY,
                date            TEXT NOT NULL,
                name            TEXT,
                merchant_name   TEXT,
                amount          REAL,
                plaid_category  TEXT,
                custom_category TEXT,
                account_id      TEXT,
                pending         INTEGER,
                imported_at     TEXT
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                token_hash   TEXT PRIMARY KEY,
                cursor       TEXT,
                last_synced  TEXT
            );

            CREATE TABLE IF NOT EXISTS categories (
                name    TEXT PRIMARY KEY,
                parent  TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS plaid_category_map (
                plaid_category   TEXT PRIMARY KEY,
                custom_category  TEXT NOT NULL
            );

            INSERT OR IGNORE INTO categories (name, parent) VALUES
                ('Food & Dining',''),('Groceries','Food & Dining'),
                ('Transport',''),('Shopping',''),('Entertainment',''),
                ('Health',''),('Utilities',''),('Rent',''),
                ('Income',''),('Transfer',''),('Other','');

            INSERT OR IGNORE INTO plaid_category_map (plaid_category, custom_category) VALUES
                ('FOOD_AND_DRINK','Food & Dining'),('TRANSPORTATION','Transport'),
                ('GENERAL_MERCHANDISE','Shopping'),('ENTERTAINMENT','Entertainment'),
                ('GENERAL_SERVICES','Other'),('GOVERNMENT_AND_NON_PROFIT','Other'),
                ('HOME_IMPROVEMENT','Other'),('INCOME','Income'),
                ('LOAN_PAYMENTS','Other'),('MEDICAL','Health'),
                ('PERSONAL_CARE','Other'),('RENT_AND_UTILITIES','Utilities'),
                ('TRANSFER_IN','Transfer'),('TRANSFER_OUT','Transfer'),
                ('TRAVEL','Transport'),('BANK_FEES','Other'),('OTHER','Other');

            CREATE TABLE IF NOT EXISTS budgets (
                category       TEXT PRIMARY KEY,
                monthly_amount REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS items (
                token_hash   TEXT PRIMARY KEY,
                token_enc    TEXT NOT NULL,
                institution  TEXT,
                added_at     TEXT
            );

            CREATE TABLE IF NOT EXISTS accounts (
                account_id     TEXT PRIMARY KEY,
                name           TEXT NOT NULL,
                official_name  TEXT,
                type           TEXT,
                subtype        TEXT,
                mask           TEXT,
                institution    TEXT,
                token_hash     TEXT
            );

            CREATE TABLE IF NOT EXISTS rules (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern      TEXT NOT NULL,
                account_id   TEXT,
                category     TEXT NOT NULL,
                new_payee    TEXT,
                old_payee    TEXT,
                old_category TEXT,
                created_at   TEXT
            );
        """)

        # For existing installations created before old_payee/old_category were added,
        # try to add the columns; ignore errors if they already exist.
        try:
            conn.execute("ALTER TABLE rules ADD COLUMN old_payee TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE rules ADD COLUMN old_category TEXT")
        except sqlite3.OperationalError:
            pass


def get_cursor(token_hash: str) -> str:
    """Return the saved sync cursor for this token, or '' if none."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT cursor FROM sync_state WHERE token_hash = ?", (token_hash,)
        ).fetchone()
    return row["cursor"] if row else ""


def save_cursor(token_hash: str, cursor: str) -> None:
    """Upsert the sync cursor and record the sync timestamp."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO sync_state (token_hash, cursor, last_synced)
            VALUES (?, ?, ?)
            ON CONFLICT(token_hash) DO UPDATE SET
                cursor = excluded.cursor,
                last_synced = excluded.last_synced
            """,
            (token_hash, cursor, now),
        )


def upsert_transactions(rows: list[dict]) -> None:
    """
    Insert or update transactions. custom_category is preserved on update
    so user overrides aren't wiped by a re-sync.
    """
    if not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as conn:
        conn.executemany(
            """
            INSERT INTO transactions
                (transaction_id, date, name, merchant_name, amount,
                 plaid_category, account_id, pending, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(transaction_id) DO UPDATE SET
                date          = excluded.date,
                name          = excluded.name,
                merchant_name = excluded.merchant_name,
                amount        = excluded.amount,
                plaid_category = excluded.plaid_category,
                account_id    = excluded.account_id,
                pending       = excluded.pending
            -- custom_category intentionally omitted: preserve user overrides
            """,
            [
                (
                    r["transaction_id"],
                    r["date"],
                    r["name"],
                    r.get("merchant_name") or "",
                    r["amount"],
                    r.get("plaid_category") or "",
                    r["account_id"],
                    1 if r["pending"] else 0,
                    now,
                )
                for r in rows
            ],
        )


def delete_transactions(ids: list[str]) -> None:
    """Remove transactions by ID (handles Plaid 'removed' events)."""
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _conn() as conn:
        conn.execute(
            f"DELETE FROM transactions WHERE transaction_id IN ({placeholders})", ids
        )


def query_transactions(
    start_date: str, end_date: str, category: str = "", account_id: str = ""
) -> list[dict]:
    """
    Query the ledger by date range.

    category   (optional): matches custom_category if set, otherwise plaid_category.
    account_id (optional): filters by account_id.

    Returns rows with an extra `account_name` field (empty string if unknown).
    """
    sql = """
        WITH ranked AS (
            SELECT t.*, COALESCE(NULLIF(a.official_name, ''), a.name, '') AS account_name,
                ROW_NUMBER() OVER (
                    PARTITION BY t.date, t.amount, t.name
                    ORDER BY CASE WHEN a.account_id IS NOT NULL THEN 0 ELSE 1 END,
                             t.transaction_id
                ) AS _rn
            FROM transactions t
            LEFT JOIN accounts a ON t.account_id = a.account_id
            WHERE t.date BETWEEN ? AND ?
    """
    params: list = [start_date, end_date]

    if category:
        sql += """
            AND (
                (t.custom_category IS NOT NULL AND t.custom_category = ?)
                OR (t.custom_category IS NULL AND t.plaid_category = ?)
            )
        """
        params += [category, category]

    if account_id:
        sql += " AND t.account_id = ?"
        params.append(account_id)

    sql += " ) SELECT * FROM ranked WHERE _rn = 1 ORDER BY date DESC"

    with _conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [dict(r) for r in rows]


def set_custom_category(transaction_id: str, category: str) -> bool:
    """
    Set custom_category for a transaction.
    Returns True if the transaction existed, False if not found.
    """
    with _conn() as conn:
        cursor = conn.execute(
            "UPDATE transactions SET custom_category = ? WHERE transaction_id = ?",
            (category, transaction_id),
        )
    return cursor.rowcount > 0


def get_category_map() -> dict[str, str]:
    """Return the full plaid_category_map table as a plain dict."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT plaid_category, custom_category FROM plaid_category_map"
        ).fetchall()
    return {r["plaid_category"]: r["custom_category"] for r in rows}


def set_category_mapping(plaid_category: str, custom_category: str) -> None:
    """Upsert a single Plaid→custom override in the map table."""
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO plaid_category_map (plaid_category, custom_category)
            VALUES (?, ?)
            ON CONFLICT(plaid_category) DO UPDATE SET
                custom_category = excluded.custom_category
            """,
            (plaid_category, custom_category),
        )


def apply_auto_categorization() -> int:
    """
    For every transaction with custom_category IS NULL, look up its
    plaid_category in plaid_category_map and set custom_category.

    Idempotent: safe to call multiple times.
    Returns the number of rows updated.
    """
    with _conn() as conn:
        cursor = conn.execute(
            """
            UPDATE transactions
            SET custom_category = (
                SELECT custom_category
                FROM plaid_category_map
                WHERE plaid_category_map.plaid_category = transactions.plaid_category
            )
            WHERE custom_category IS NULL
              AND plaid_category IS NOT NULL
              AND plaid_category != ''
              AND EXISTS (
                  SELECT 1 FROM plaid_category_map
                  WHERE plaid_category_map.plaid_category = transactions.plaid_category
              )
            """
        )
    return cursor.rowcount


def get_all_categories() -> list[dict]:
    """Return all categories ordered by parent then name."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT name, parent FROM categories ORDER BY parent, name"
        ).fetchall()
    return [dict(r) for r in rows]


def add_category(name: str, parent: str = "") -> bool:
    """
    Insert a new category. Returns False if the name already exists.
    Raises ValueError if parent is non-empty and doesn't exist.
    """
    with _conn() as conn:
        if parent:
            exists = conn.execute(
                "SELECT 1 FROM categories WHERE name = ?", (parent,)
            ).fetchone()
            if not exists:
                raise ValueError(f"Parent category '{parent}' does not exist")
        try:
            conn.execute(
                "INSERT INTO categories (name, parent) VALUES (?, ?)",
                (name, parent),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def remove_category(name: str) -> bool:
    """
    Delete a sub-level category (one with a non-empty parent).
    Raises ValueError if the category doesn't exist or is top-level.
    Clears custom_category on transactions using it and deletes any budget for it.
    Returns True if deleted.
    """
    with _conn() as conn:
        row = conn.execute(
            "SELECT parent FROM categories WHERE name = ?", (name,)
        ).fetchone()
        if not row:
            raise ValueError(f"Category '{name}' does not exist")
        if not row["parent"]:
            raise ValueError(f"Category '{name}' is a top-level category and cannot be removed here")
        conn.execute(
            "UPDATE transactions SET custom_category = NULL WHERE custom_category = ?",
            (name,),
        )
        conn.execute("DELETE FROM budgets WHERE category = ?", (name,))
        conn.execute("DELETE FROM categories WHERE name = ?", (name,))
    return True


def get_uncategorized(limit: int = 50) -> list[dict]:
    """Return transactions where custom_category IS NULL."""
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM transactions
            WHERE custom_category IS NULL
            ORDER BY date DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_item(token_hash: str, token_enc: str, institution: str = "") -> None:
    """Store an encrypted Plaid access token. Safe to call repeatedly."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO items (token_hash, token_enc, institution, added_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(token_hash) DO UPDATE SET
                token_enc   = excluded.token_enc,
                institution = excluded.institution
            """,
            (token_hash, token_enc, institution, now),
        )


def get_all_token_hashes() -> list[str]:
    """Return all token_hash values from the items table."""
    with _conn() as conn:
        rows = conn.execute("SELECT token_hash FROM items").fetchall()
    return [r["token_hash"] for r in rows]


def get_encrypted_token(token_hash: str) -> str | None:
    """Return the encrypted token for a given hash, or None if not found."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT token_enc FROM items WHERE token_hash = ?", (token_hash,)
        ).fetchone()
    return row["token_enc"] if row else None


def upsert_accounts(rows: list[dict]) -> None:
    """Insert or replace account metadata. Safe to call on every sync."""
    if not rows:
        return
    with _conn() as conn:
        conn.executemany(
            """
            INSERT INTO accounts
                (account_id, name, official_name, type, subtype, mask, institution, token_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                name          = excluded.name,
                official_name = excluded.official_name,
                type          = excluded.type,
                subtype       = excluded.subtype,
                mask          = excluded.mask,
                institution   = excluded.institution,
                token_hash    = excluded.token_hash
            """,
            [
                (
                    r["account_id"], r["name"], r.get("official_name") or "",
                    r.get("type") or "", r.get("subtype") or "",
                    r.get("mask") or "", r.get("institution") or "",
                    r.get("token_hash") or "",
                )
                for r in rows
            ],
        )


def get_all_accounts() -> list[dict]:
    """Return all accounts ordered by institution then name."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM accounts ORDER BY institution, name"
        ).fetchall()
    return [dict(r) for r in rows]


def set_budget(category: str, monthly_amount: float) -> None:
    """Upsert a monthly budget for a category."""
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO budgets (category, monthly_amount) VALUES (?, ?)
            ON CONFLICT(category) DO UPDATE SET monthly_amount = excluded.monthly_amount
            """,
            (category, monthly_amount),
        )


def get_all_budgets() -> list[dict]:
    """Return all budgets ordered by category."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT category, monthly_amount FROM budgets ORDER BY category"
        ).fetchall()
    return [dict(r) for r in rows]


def get_budget_status(month: str) -> list[dict]:
    """
    Compute spending vs budget for every category that has either a budget or
    spending in the given YYYY-MM month.

    Returns [{category, budgeted, spent, remaining, over_budget}] sorted by:
      1. over_budget DESC (overages first)
      2. budgeted DESC
    """
    spending = {r["category"]: r["total"] for r in get_spending_summary(month)}
    budgets  = {r["category"]: r["monthly_amount"] for r in get_all_budgets()}
    cats = sorted(set(spending) | set(budgets))
    result = []
    for cat in cats:
        budgeted = budgets.get(cat, 0.0)
        spent    = round(spending.get(cat, 0.0), 2)
        result.append({
            "category":   cat,
            "budgeted":   budgeted,
            "spent":      spent,
            "remaining":  round(budgeted - spent, 2),
            "over_budget": budgeted > 0 and spent > budgeted,
        })
    result.sort(key=lambda r: (-r["over_budget"], -r["budgeted"]))
    return result


def get_spending_summary(month: str) -> list[dict]:
    """
    Aggregate spending by category for a given YYYY-MM.
    Only includes posted (non-pending) debits (amount > 0).
    Returns [{category, total, count}] sorted by total descending.
    """
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(custom_category, plaid_category, 'Uncategorized') AS category,
                ROUND(SUM(amount), 2) AS total,
                COUNT(*) AS count
            FROM transactions
            WHERE date LIKE ?
              AND amount > 0
              AND pending = 0
            GROUP BY category
            ORDER BY total DESC
            """,
            (f"{month}-%",),
        ).fetchall()
    return [dict(r) for r in rows]


def insert_rule(
    pattern: str,
    account_id: str | None,
    category: str,
    new_payee: str | None = None,
    old_payee: str | None = None,
    old_category: str | None = None,
) -> int:
    """
    Insert a new manual rule and return its id.

    pattern:    Case-insensitive substring matched against name/merchant_name.
    account_id: Optional Plaid account_id to scope the rule (None = all).
    category:   Custom category label to assign when the rule matches.
    new_payee:  Optional new payee/display name to assign.
    """
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as conn:
        cursor = conn.execute(
            """
            INSERT INTO rules (pattern, account_id, category, new_payee, old_payee, old_category, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (pattern, account_id, category, new_payee, old_payee, old_category, now),
        )
        return int(cursor.lastrowid)


def delete_rule(rule_id: int) -> None:
    """Delete a rule by id. Does not alter existing transactions."""
    with _conn() as conn:
        conn.execute("DELETE FROM rules WHERE id = ?", (rule_id,))


def get_all_rules() -> list[dict]:
    """Return all rules ordered by creation time (newest first)."""
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT id, pattern, account_id, category, new_payee, old_payee, old_category, created_at
            FROM rules
            ORDER BY datetime(created_at) DESC, id DESC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def find_rule_matches(pattern: str, account_id: str | None) -> list[dict]:
    """
    Return transactions that would match a rule with the given pattern/account.

    Matching is case-insensitive on name/merchant_name and optionally scoped
    to a specific account_id.
    """
    like = f"%{pattern.lower()}%"
    sql = """
        SELECT
            t.*,
            COALESCE(NULLIF(a.official_name, ''), a.name, '') AS account_name
        FROM transactions t
        LEFT JOIN accounts a ON t.account_id = a.account_id
        WHERE (LOWER(t.name) LIKE ? OR LOWER(t.merchant_name) LIKE ?)
    """
    params: list = [like, like]
    if account_id:
        sql += " AND t.account_id = ?"
        params.append(account_id)

    with _conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _apply_rule_row(rule: sqlite3.Row) -> int:
    """
    Apply a single rule row to matching transactions.

    Only updates custom_category when it is currently NULL so manual overrides
    are preserved. Payee renames are applied by updating the name field when
    new_payee is non-empty.
    """
    pattern = rule["pattern"]
    account_id = rule["account_id"]
    category = rule["category"]
    new_payee = rule["new_payee"] or ""

    like = f"%{pattern.lower()}%"

    total = 0
    # 1) Set custom_category for all matches (rules are user-preferred).
    sql_cat = """
        UPDATE transactions
        SET custom_category = ?
        WHERE (LOWER(name) LIKE ? OR LOWER(merchant_name) LIKE ?)
    """
    params_cat: list = [category, like, like]
    if account_id:
        sql_cat += " AND account_id = ?"
        params_cat.append(account_id)

    with _conn() as conn:
        cur_cat = conn.execute(sql_cat, params_cat)
        total += cur_cat.rowcount

        # 2) Apply payee rename independently of custom_category.
        if new_payee:
            sql_payee = """
                UPDATE transactions
                SET name = ?
                WHERE (LOWER(name) LIKE ? OR LOWER(merchant_name) LIKE ?)
            """
            params_payee: list = [new_payee, like, like]
            if account_id:
                sql_payee += " AND account_id = ?"
                params_payee.append(account_id)
            cur_payee = conn.execute(sql_payee, params_payee)
            total += cur_payee.rowcount

    return total


def apply_rule(rule_id: int) -> int:
    """
    Apply a single rule to all matching existing transactions.

    Returns the number of rows updated.
    """
    with _conn() as conn:
        rule = conn.execute(
            "SELECT * FROM rules WHERE id = ?", (rule_id,)
        ).fetchone()
    if not rule:
        return 0
    return _apply_rule_row(rule)


def apply_rules_to_new_transactions() -> int:
    """
    Apply all rules across the ledger.

    This is safe to call repeatedly: rules only fill in custom_category when
    it is NULL, so subsequent runs are mostly no-ops.
    """
    with _conn() as conn:
        rules = conn.execute("SELECT * FROM rules ORDER BY id").fetchall()

    total = 0
    for rule in rules:
        total += _apply_rule_row(rule)
    return total
