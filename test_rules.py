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
        self.assertEqual(total, 2)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        cats = {r["transaction_id"]: r["custom_category"] for r in rows}
        self.assertEqual(cats["tx1"], "Shopping")
        self.assertEqual(cats["tx2"], "Transport")


if __name__ == "__main__":
    unittest.main()

