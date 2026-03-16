# Personal Finance Automation — Master Plan

## Goal
Build a local personal finance system where Claude can:
1. Pull live transaction data from Plaid
2. Maintain a persistent, categorized ledger on disk
3. Auto-assign categories using Claude Cowork
4. Generate monthly spending summaries and reports

All production API keys stay local — never sent to Anthropic's servers.

---

## Architecture Overview

```
Plaid API  ←──→  MCP Server (server.py)  ←──→  Claude Desktop / Cowork
                        ↕
                  SQLite ledger (Phase 2+)
                        ↕
               ~/Documents/Finance/ (reports)
```

---

## Phase 1 — MCP Foundation ✅ (current)

**Goal:** Get Plaid data into Claude Desktop via MCP tools.

### Files
| File | Purpose |
|---|---|
| `server.py` | FastMCP server — 3 Plaid tools |
| `setup_link.py` | One-time Flask app to get `access_token` via Plaid Link |
| `requirements.txt` | Python dependencies |
| `.env.example` | Credential template |

### MCP Tools
| Tool | Endpoint | Description |
|---|---|---|
| `sync_transactions(start_date, end_date)` | `/transactions/sync` | Fetch all transactions in a date range |
| `get_account_balances()` | `/accounts/balance/get` | Real-time balances for all accounts |
| `list_items()` | `/item/get` + `/institutions/get_by_id` | Info about the connected bank |

### Setup Steps
```bash
# 1. Create venv and install deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env
# Fill in PLAID_CLIENT_ID and PLAID_SECRET in .env

# 3. Get your access_token (one-time)
python3 setup_link.py
# Open http://localhost:8080 → connect your bank → copy printed token to .env

# 4. Smoke test
python3 server.py     # should start without errors, Ctrl+C
mcp dev server.py     # opens inspector UI at http://localhost:5173
```

### Claude Desktop Registration
Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
and **merge** (do not replace) the following into `"mcpServers"`:

```json
"plaid": {
  "command": "/Users/calvinkorver/Code/plaid-mpc/.venv/bin/python",
  "args": ["/Users/calvinkorver/Code/plaid-mpc/server.py"],
  "env": {
    "PLAID_CLIENT_ID": "your_client_id",
    "PLAID_SECRET": "your_secret",
    "PLAID_ACCESS_TOKEN": "access-production-xxxx",
    "PLAID_ENV": "production"
  }
}
```

Restart Claude Desktop after saving.

### Known Limitations
- No persistence: every `sync_transactions` call re-fetches all history
- Categories come from Plaid's `personal_finance_category` — no custom overrides yet
- No scheduled runs

**Phase 1 is done when:** Claude can answer "What are my balances?" and "Show my transactions from last month."

---

## Phase 2 — Persistent Ledger + Incremental Sync

**Goal:** Store transactions in a local SQLite database. Only fetch new ones on each run.

### New Files
| File | Purpose |
|---|---|
| `db.py` | SQLite schema + helper functions |
| `ledger.db` | Auto-created database (add to .gitignore) |

### Schema
```sql
CREATE TABLE transactions (
  transaction_id  TEXT PRIMARY KEY,
  date            TEXT NOT NULL,
  name            TEXT,
  merchant_name   TEXT,
  amount          REAL,
  plaid_category  TEXT,   -- Plaid's built-in category
  custom_category TEXT,   -- Claude/user-assigned override
  account_id      TEXT,
  pending         INTEGER,
  imported_at     TEXT
);

CREATE TABLE sync_state (
  access_token_hash TEXT PRIMARY KEY,
  cursor            TEXT,
  last_synced       TEXT
);
```

### New / Updated MCP Tools
| Tool | Description |
|---|---|
| `sync_and_store()` | Incremental sync using saved cursor; upserts into DB; saves new cursor |
| `get_stored_transactions(start_date, end_date, category?)` | Query ledger (no Plaid call) |
| `update_category(transaction_id, category)` | Set `custom_category` for one transaction |

**Phase 2 is done when:** Running `sync_and_store` twice only fetches new transactions the second time. Custom categories survive restarts.

---

## Phase 3 — Auto-Categorization with Claude Cowork

**Goal:** Claude Cowork reads uncategorized transactions, assigns categories, and writes monthly reports.

### New MCP Tools
| Tool | Description |
|---|---|
| `get_uncategorized(limit?)` | Transactions where `custom_category IS NULL` |
| `bulk_update_categories(updates)` | Batch-write categories `[{transaction_id, category}]` |
| `get_spending_summary(month)` | Aggregate totals by `custom_category` for a given month (YYYY-MM) |

### Cowork Workflow Prompt
```
1. Call sync_and_store to pull latest transactions.
2. Call get_uncategorized to get up to 100 pending items.
3. For each transaction, assign a category based on merchant name and Plaid category.
   Use consistent labels: Food & Dining, Groceries, Transport, Shopping,
   Entertainment, Health, Utilities, Rent, Income, Transfer, Other.
4. Call bulk_update_categories with your assignments.
5. Call get_spending_summary for the current month.
6. Write a markdown report to ~/Documents/Finance/YYYY-MM-report.md
   with a summary table and notable transactions.
```

**Phase 3 is done when:** Claude can run "Categorize and summarize last month's spending" end-to-end and produce a report file.

---

## Phase 4 — Export + Automation (optional)

| Feature | Description |
|---|---|
| `export_to_csv(start_date, end_date)` | Write filtered transactions to `~/Documents/Finance/` |
| Scheduled Cowork task | Auto-sync + categorize on a weekly schedule |
| Multi-account support | Accept a list of `access_token`s; add an `accounts` lookup table (account_id → name); tag transactions by account; add account filter to `ui.py` |
| Budget tracking | Define monthly budgets per category; flag overages in reports |

---

## Category Taxonomy (Phase 3+)

| Label | Examples |
|---|---|
| Food & Dining | Restaurants, cafes, fast food |
| Groceries | Whole Foods, Trader Joe's, supermarkets |
| Transport | Uber, Lyft, gas, parking, transit |
| Shopping | Amazon, clothing, electronics |
| Entertainment | Netflix, Spotify, movies, events |
| Health | Pharmacy, gym, doctor |
| Utilities | Electric, internet, phone |
| Rent | Monthly rent or mortgage |
| Income | Payroll, transfers in |
| Transfer | Internal account moves (filter from spending) |
| Other | Everything else |
