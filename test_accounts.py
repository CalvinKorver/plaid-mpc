"""
Unit tests for items and accounts tables in db.py.

Uses a temporary SQLite DB for each test — no side effects on ledger.db.

Run:
    python3 -m unittest test_accounts -v
"""

import os
import tempfile
import unittest
from pathlib import Path

import db as db_module


class TestItems(unittest.TestCase):
    """Tests for encrypted token storage in the items table."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_upsert_and_get_token_hashes(self):
        db_module.upsert_item("hash1", "enc_token_abc", "Chase")
        self.assertEqual(db_module.get_all_token_hashes(), ["hash1"])

    def test_upsert_item_is_idempotent(self):
        db_module.upsert_item("hash1", "enc1", "Chase")
        db_module.upsert_item("hash1", "enc2", "Chase")
        self.assertEqual(len(db_module.get_all_token_hashes()), 1)

    def test_upsert_item_updates_enc_value(self):
        db_module.upsert_item("hash1", "enc_old", "Chase")
        db_module.upsert_item("hash1", "enc_new", "Chase")
        self.assertEqual(db_module.get_encrypted_token("hash1"), "enc_new")

    def test_get_encrypted_token_returns_value(self):
        db_module.upsert_item("hash1", "enc_abc", "AmEx")
        self.assertEqual(db_module.get_encrypted_token("hash1"), "enc_abc")

    def test_get_encrypted_token_returns_none_for_missing(self):
        self.assertIsNone(db_module.get_encrypted_token("nonexistent"))

    def test_multiple_items_stored(self):
        db_module.upsert_item("h1", "enc1", "Chase")
        db_module.upsert_item("h2", "enc2", "AmEx")
        self.assertEqual(len(db_module.get_all_token_hashes()), 2)

    def test_empty_items_returns_empty_list(self):
        self.assertEqual(db_module.get_all_token_hashes(), [])


class TestAccounts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _account(self, account_id="acc1", name="Sapphire Preferred",
                 institution="Chase", token_hash="abc"):
        return {
            "account_id": account_id, "name": name, "official_name": "",
            "type": "credit", "subtype": "credit card",
            "mask": "1234", "institution": institution, "token_hash": token_hash,
        }

    def _tx(self, tx_id, account_id="acc1", date="2026-03-01"):
        return {
            "transaction_id": tx_id, "date": date, "name": "Test",
            "merchant_name": "", "amount": 10.0, "plaid_category": "FOOD_AND_DRINK",
            "account_id": account_id, "pending": False,
        }

    def test_upsert_and_get_accounts(self):
        db_module.upsert_accounts([self._account()])
        accounts = db_module.get_all_accounts()
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[0]["name"], "Sapphire Preferred")

    def test_upsert_accounts_is_idempotent(self):
        db_module.upsert_accounts([self._account()])
        db_module.upsert_accounts([self._account()])
        self.assertEqual(len(db_module.get_all_accounts()), 1)

    def test_upsert_accounts_updates_name(self):
        db_module.upsert_accounts([self._account(name="Old Name")])
        db_module.upsert_accounts([self._account(name="New Name")])
        self.assertEqual(db_module.get_all_accounts()[0]["name"], "New Name")

    def test_upsert_accounts_empty_list_no_op(self):
        db_module.upsert_accounts([])
        self.assertEqual(db_module.get_all_accounts(), [])

    def test_query_transactions_includes_account_name(self):
        db_module.upsert_accounts([self._account(account_id="acc1", name="Sapphire Preferred")])
        db_module.upsert_transactions([self._tx("tx1", account_id="acc1")])
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["account_name"], "Sapphire Preferred")

    def test_query_transactions_account_name_empty_when_no_match(self):
        db_module.upsert_transactions([self._tx("tx1", account_id="unknown")])
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["account_name"], "")

    def test_query_transactions_account_filter(self):
        db_module.upsert_transactions([
            self._tx("tx1", account_id="acc1"),
            self._tx("tx2", account_id="acc2"),
        ])
        rows = db_module.query_transactions("2026-03-01", "2026-03-01", account_id="acc1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["transaction_id"], "tx1")

    def test_query_transactions_no_account_filter_returns_all(self):
        db_module.upsert_transactions([
            {**self._tx("tx1", account_id="acc1"), "amount": 10.0},
            {**self._tx("tx2", account_id="acc2"), "amount": 20.0},
        ])
        self.assertEqual(len(db_module.query_transactions("2026-03-01", "2026-03-01")), 2)

    def test_query_transactions_account_and_category_filter_combined(self):
        db_module.upsert_transactions([
            self._tx("tx1", account_id="acc1"),
            self._tx("tx2", account_id="acc2"),
        ])
        db_module.apply_auto_categorization()
        rows = db_module.query_transactions(
            "2026-03-01", "2026-03-01",
            category="Food & Dining", account_id="acc1",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["transaction_id"], "tx1")

    def test_accounts_ordered_by_institution_then_name(self):
        db_module.upsert_accounts([
            self._account("a1", "Sapphire Preferred", "Chase"),
            self._account("a2", "Business", "Chase"),
            self._account("a3", "Platinum", "AmEx"),
        ])
        institutions = [a["institution"] for a in db_module.get_all_accounts()]
        self.assertEqual(institutions, ["AmEx", "Chase", "Chase"])

    def test_multiple_accounts_multiple_institutions(self):
        db_module.upsert_accounts([
            self._account("a1", "Checking", "BECU", "h1"),
            self._account("a2", "Platinum", "AmEx", "h2"),
            self._account("a3", "Sapphire Preferred", "Chase", "h3"),
            self._account("a4", "Business", "Chase", "h3"),
        ])
        self.assertEqual(len(db_module.get_all_accounts()), 4)

    def test_duplicate_transactions_deduped_prefers_known_account(self):
        db_module.upsert_accounts([self._account(account_id="acc1", name="Robinhood Credit Card")])
        db_module.upsert_transactions([
            self._tx("tx1", account_id="acc1"),       # known account
            self._tx("tx2", account_id="orphaned"),   # unknown account, same date/amount/name
        ])
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["account_name"], "Robinhood Credit Card")

    def test_duplicate_transactions_deduped_returns_one_when_both_unknown(self):
        db_module.upsert_transactions([
            self._tx("tx1", account_id="orphan1"),
            self._tx("tx2", account_id="orphan2"),
        ])
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(len(rows), 1)

    def test_get_all_accounts_includes_official_name_and_mask(self):
        db_module.upsert_accounts([self._account()])
        acct = db_module.get_all_accounts()[0]
        self.assertIn("official_name", acct)
        self.assertIn("mask", acct)

    def test_accounts_with_duplicate_names_have_distinct_masks(self):
        db_module.upsert_accounts([
            {**self._account("a1", "CREDIT CARD", "Chase"), "mask": "1111"},
            {**self._account("a2", "CREDIT CARD", "Chase"), "mask": "2222"},
        ])
        accounts = db_module.get_all_accounts()
        masks = {a["account_id"]: a["mask"] for a in accounts}
        self.assertEqual(masks["a1"], "1111")
        self.assertEqual(masks["a2"], "2222")

    def test_query_transactions_prefers_official_name_for_account_name(self):
        # When an account has a generic name like "CREDIT CARD" but a more
        # specific official_name (e.g. "Chase Slate Edge"), the UI should
        # show the official_name everywhere, including in the transactions
        # table.
        db_module.upsert_accounts([
            {
                **self._account(account_id="chase1", name="CREDIT CARD", institution="Chase"),
                "official_name": "Chase Slate Edge",
            }
        ])
        db_module.upsert_transactions([
            self._tx("tx1", account_id="chase1"),
        ])
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["account_name"], "Chase Slate Edge")


if __name__ == "__main__":
    unittest.main()
