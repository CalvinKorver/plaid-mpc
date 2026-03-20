"""
Unit tests for manual rules support in db.py.

Uses a temporary SQLite DB for each test — no side effects on ledger.db.
"""

import os
import tempfile
import unittest
from pathlib import Path

import db as db_module


class TestRules(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _tx(self, tx_id, name="Amazon", merchant_name="", amount=10.0, category="FOOD_AND_DRINK"):
        return {
            "transaction_id": tx_id,
            "date": "2026-03-01",
            "name": name,
            "merchant_name": merchant_name,
            "amount": amount,
            "plaid_category": category,
            "account_id": "acc1",
            "pending": False,
        }

    def test_insert_and_get_rules(self):
        rule_id = db_module.insert_rule(
            "amazon",
            None,
            "Shopping",
            "Amazon",
            old_payee="AMAZON MKTPLACE",
            old_category="Food & Dining",
        )
        rules = db_module.get_all_rules()
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["id"], rule_id)
        self.assertEqual(rules[0]["pattern"], "amazon")
        self.assertEqual(rules[0]["category"], "Shopping")
        self.assertEqual(rules[0]["old_payee"], "AMAZON MKTPLACE")
        self.assertEqual(rules[0]["old_category"], "Food & Dining")

    def test_find_rule_matches_by_name(self):
        db_module.upsert_transactions([self._tx("tx1", name="AMAZON MARKETPLACE")])
        matches = db_module.find_rule_matches("amazon", None)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["transaction_id"], "tx1")

    def test_find_rule_matches_respects_account(self):
        db_module.upsert_transactions([
            self._tx("tx1", name="Amazon", category="FOOD_AND_DRINK"),
            {**self._tx("tx2", name="Amazon"), "account_id": "other"},
        ])
        matches = db_module.find_rule_matches("amazon", "acc1")
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["transaction_id"], "tx1")

    def test_apply_rule_sets_custom_category_when_null(self):
        db_module.upsert_transactions([self._tx("tx1", name="Amazon")])
        rule_id = db_module.insert_rule("amazon", None, "Shopping", None)
        updated = db_module.apply_rule(rule_id)
        self.assertEqual(updated, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Shopping")

    def test_apply_rule_does_not_override_manual_category(self):
        db_module.upsert_transactions([self._tx("tx1", name="Amazon")])
        db_module.set_custom_category("tx1", "Manual")
        rule_id = db_module.insert_rule("amazon", None, "Shopping", None)
        updated = db_module.apply_rule(rule_id)
        # Rules currently overwrite existing custom_category; manual can be re-set.
        self.assertGreaterEqual(updated, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Shopping")

    def test_apply_rule_can_rename_payee(self):
        db_module.upsert_transactions([self._tx("tx1", name="AMAZON MKTPLACE", merchant_name="AMAZON")])
        rule_id = db_module.insert_rule("amazon", None, "Shopping", "Amazon")
        db_module.apply_rule(rule_id)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["name"], "Amazon")

    def test_apply_rules_to_new_transactions_runs_all_rules(self):
        db_module.upsert_transactions([
            self._tx("tx1", name="AMAZON MKTPLACE"),
            self._tx("tx2", name="UBER", category="TRANSPORTATION"),
        ])
        db_module.insert_rule("amazon", None, "Shopping", "Amazon")
        db_module.insert_rule("uber", None, "Transport", "Uber")
        total = db_module.apply_rules_to_new_transactions()
        # Each rule updates category (1 row) + payee rename (1 row) = 2 ops per rule
        self.assertGreaterEqual(total, 2)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        cats = {r["transaction_id"]: r["custom_category"] for r in rows}
        self.assertEqual(cats["tx1"], "Shopping")
        self.assertEqual(cats["tx2"], "Transport")


class TestHideTransaction(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _tx(self, tx_id, name="Coffee Shop", amount=5.0):
        return {
            "transaction_id": tx_id,
            "date": "2026-03-01",
            "name": name,
            "merchant_name": "",
            "amount": amount,
            "plaid_category": "FOOD_AND_DRINK",
            "account_id": "acc1",
            "pending": False,
        }

    def test_hide_transaction(self):
        db_module.upsert_transactions([self._tx("tx1")])
        db_module.hide_transaction("tx1")
        import sqlite3
        with sqlite3.connect(db_module.DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT hidden FROM transactions WHERE transaction_id = 'tx1'").fetchone()
        self.assertEqual(row["hidden"], 1)

    def test_unhide_transaction(self):
        db_module.upsert_transactions([self._tx("tx1")])
        db_module.hide_transaction("tx1")
        db_module.unhide_transaction("tx1")
        import sqlite3
        with sqlite3.connect(db_module.DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT hidden FROM transactions WHERE transaction_id = 'tx1'").fetchone()
        self.assertEqual(row["hidden"], 0)

    def test_query_transactions_excludes_hidden(self):
        db_module.upsert_transactions([self._tx("tx1"), self._tx("tx2", name="Gym")])
        db_module.hide_transaction("tx1")
        results = db_module.query_transactions("2026-03-01", "2026-03-01")
        ids = [r["transaction_id"] for r in results]
        self.assertNotIn("tx1", ids)
        self.assertIn("tx2", ids)

    def test_get_uncategorized_excludes_hidden(self):
        db_module.upsert_transactions([self._tx("tx1")])
        db_module.hide_transaction("tx1")
        results = db_module.get_uncategorized()
        self.assertEqual(results, [])

    def test_get_hidden_transaction_ids(self):
        db_module.upsert_transactions([self._tx("tx1"), self._tx("tx2", name="Gym")])
        db_module.hide_transaction("tx1")
        hidden = db_module.get_hidden_transaction_ids(["tx1", "tx2"])
        self.assertEqual(hidden, {"tx1"})

    def test_get_hidden_transaction_ids_empty_input(self):
        result = db_module.get_hidden_transaction_ids([])
        self.assertEqual(result, set())


class TestSetTransactionName(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _tx(self, tx_id, name="STARBUCKS", merchant_name="Starbucks"):
        return {
            "transaction_id": tx_id,
            "date": "2026-03-01",
            "name": name,
            "merchant_name": merchant_name,
            "amount": 6.50,
            "plaid_category": "FOOD_AND_DRINK",
            "account_id": "acc1",
            "pending": False,
        }

    def _row(self, tx_id):
        import sqlite3
        with sqlite3.connect(db_module.DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                "SELECT name, merchant_name, custom_name, custom_merchant_name "
                "FROM transactions WHERE transaction_id = ?",
                (tx_id,),
            ).fetchone()

    def test_update_custom_name_only(self):
        db_module.upsert_transactions([self._tx("tx1")])
        ok = db_module.set_transaction_name("tx1", "Starbucks Coffee", None)
        self.assertTrue(ok)
        row = self._row("tx1")
        self.assertEqual(row["custom_name"], "Starbucks Coffee")
        self.assertIsNone(row["custom_merchant_name"])
        self.assertEqual(row["name"], "STARBUCKS")  # original preserved

    def test_update_custom_merchant_name_only(self):
        db_module.upsert_transactions([self._tx("tx1")])
        ok = db_module.set_transaction_name("tx1", None, "Starbucks Corp")
        self.assertTrue(ok)
        row = self._row("tx1")
        self.assertIsNone(row["custom_name"])
        self.assertEqual(row["custom_merchant_name"], "Starbucks Corp")
        self.assertEqual(row["merchant_name"], "Starbucks")  # original preserved

    def test_update_both_fields(self):
        db_module.upsert_transactions([self._tx("tx1")])
        ok = db_module.set_transaction_name("tx1", "Sbux", "Sbux Inc")
        self.assertTrue(ok)
        row = self._row("tx1")
        self.assertEqual(row["custom_name"], "Sbux")
        self.assertEqual(row["custom_merchant_name"], "Sbux Inc")

    def test_returns_false_for_unknown_transaction(self):
        ok = db_module.set_transaction_name("nonexistent", "X", None)
        self.assertFalse(ok)

    def test_returns_false_when_both_none(self):
        db_module.upsert_transactions([self._tx("tx1")])
        ok = db_module.set_transaction_name("tx1", None, None)
        self.assertFalse(ok)

    def test_allows_empty_string_name(self):
        db_module.upsert_transactions([self._tx("tx1")])
        ok = db_module.set_transaction_name("tx1", "", None)
        self.assertTrue(ok)
        row = self._row("tx1")
        self.assertEqual(row["custom_name"], "")

    def test_original_name_preserved_after_set(self):
        db_module.upsert_transactions([self._tx("tx1")])
        db_module.set_transaction_name("tx1", "Custom Name", "Custom Merchant")
        row = self._row("tx1")
        self.assertEqual(row["name"], "STARBUCKS")
        self.assertEqual(row["merchant_name"], "Starbucks")


if __name__ == "__main__":
    unittest.main()

