"""
Unit tests for categories.py and db.py helpers.

Uses a temporary SQLite DB for each test — no side effects on ledger.db.

Run:
    python3 -m pytest test_categories.py -v
    # or:
    python3 -m unittest test_categories -v
"""

import os
import tempfile
import unittest
from pathlib import Path

from categories import DEFAULT_PLAID_MAP, DEFAULT_CATEGORIES, resolve_category
import db as db_module


# ---------------------------------------------------------------------------
# categories.py — pure function tests
# ---------------------------------------------------------------------------

class TestResolveCategory(unittest.TestCase):
    def test_known_plaid_category(self):
        self.assertEqual(resolve_category("FOOD_AND_DRINK", {}), "Food & Dining")

    def test_transportation(self):
        self.assertEqual(resolve_category("TRANSPORTATION", {}), "Transport")

    def test_unknown_falls_back_to_other(self):
        self.assertEqual(resolve_category("SOME_FUTURE_PLAID_TYPE", {}), "Other")

    def test_empty_string_falls_back_to_other(self):
        self.assertEqual(resolve_category("", {}), "Other")

    def test_override_takes_precedence_over_default(self):
        self.assertEqual(
            resolve_category("FOOD_AND_DRINK", {"FOOD_AND_DRINK": "Restaurants"}),
            "Restaurants",
        )

    def test_override_for_unknown_plaid_cat(self):
        self.assertEqual(
            resolve_category("FUTURE_CAT", {"FUTURE_CAT": "Custom"}),
            "Custom",
        )

    def test_all_default_map_values_are_non_empty(self):
        for plaid_cat, label in DEFAULT_PLAID_MAP.items():
            self.assertNotEqual(label, "", f"{plaid_cat} maps to empty string")

    def test_default_categories_has_expected_labels(self):
        names = {name for name, _ in DEFAULT_CATEGORIES}
        for expected in ["Food & Dining", "Groceries", "Transport", "Other"]:
            self.assertIn(expected, names)

    def test_groceries_is_subcategory_of_food(self):
        parent_map = {name: parent for name, parent in DEFAULT_CATEGORIES}
        self.assertEqual(parent_map["Groceries"], "Food & Dining")


# ---------------------------------------------------------------------------
# db.py — integration tests using a temp DB
# ---------------------------------------------------------------------------

class TestDb(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _tx(self, tx_id, plaid_cat="FOOD_AND_DRINK", amount=10.0, pending=False, date="2026-03-01"):
        return {
            "transaction_id": tx_id,
            "date": date,
            "name": "Test",
            "merchant_name": "",
            "amount": amount,
            "plaid_category": plaid_cat,
            "account_id": "acc1",
            "pending": pending,
        }

    # --- init / seeding ---

    def test_init_db_seeds_categories(self):
        cats = db_module.get_all_categories()
        self.assertEqual(len(cats), 11)
        names = {c["name"] for c in cats}
        self.assertIn("Food & Dining", names)
        self.assertIn("Groceries", names)
        self.assertIn("Other", names)

    def test_init_db_seeds_plaid_map(self):
        m = db_module.get_category_map()
        self.assertEqual(m["FOOD_AND_DRINK"], "Food & Dining")
        self.assertEqual(m["MEDICAL"], "Health")
        self.assertEqual(m["TRANSPORTATION"], "Transport")
        self.assertEqual(len(m), 17)

    def test_init_db_is_idempotent(self):
        db_module.init_db()  # second call should not fail or duplicate
        self.assertEqual(len(db_module.get_all_categories()), 11)
        self.assertEqual(len(db_module.get_category_map()), 17)

    # --- apply_auto_categorization ---

    def test_auto_categorization_sets_custom(self):
        db_module.upsert_transactions([self._tx("tx1", "FOOD_AND_DRINK")])
        count = db_module.apply_auto_categorization()
        self.assertEqual(count, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Food & Dining")

    def test_auto_categorization_is_idempotent(self):
        db_module.upsert_transactions([self._tx("tx2", "TRANSPORTATION")])
        db_module.apply_auto_categorization()
        second_run = db_module.apply_auto_categorization()
        self.assertEqual(second_run, 0)

    def test_auto_categorization_does_not_overwrite_manual(self):
        db_module.upsert_transactions([self._tx("tx3", "GENERAL_MERCHANDISE")])
        db_module.set_custom_category("tx3", "Subscriptions")
        db_module.apply_auto_categorization()
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Subscriptions")

    def test_auto_categorization_skips_empty_plaid_category(self):
        db_module.upsert_transactions([self._tx("tx4", "")])
        count = db_module.apply_auto_categorization()
        self.assertEqual(count, 0)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertIsNone(rows[0]["custom_category"])

    def test_auto_categorization_unknown_plaid_cat_stays_null(self):
        db_module.upsert_transactions([self._tx("tx5", "TOTALLY_UNKNOWN_CAT")])
        count = db_module.apply_auto_categorization()
        self.assertEqual(count, 0)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertIsNone(rows[0]["custom_category"])

    # --- add_category ---

    def test_add_top_level_category(self):
        ok = db_module.add_category("Subscriptions")
        self.assertTrue(ok)
        names = {c["name"] for c in db_module.get_all_categories()}
        self.assertIn("Subscriptions", names)

    def test_add_subcategory(self):
        ok = db_module.add_category("Coffee", "Food & Dining")
        self.assertTrue(ok)
        cats = {c["name"]: c["parent"] for c in db_module.get_all_categories()}
        self.assertEqual(cats["Coffee"], "Food & Dining")

    def test_add_duplicate_returns_false(self):
        self.assertFalse(db_module.add_category("Transport"))

    def test_add_subcategory_with_invalid_parent_raises(self):
        with self.assertRaises(ValueError):
            db_module.add_category("SubFoo", "NonExistentParent")

    # --- remove_category ---

    def test_remove_subcategory(self):
        ok = db_module.remove_category("Groceries")
        self.assertTrue(ok)
        names = {c["name"] for c in db_module.get_all_categories()}
        self.assertNotIn("Groceries", names)

    def test_remove_subcategory_clears_transactions(self):
        db_module.upsert_transactions([self._tx("tx_groc", "FOOD_AND_DRINK")])
        db_module.set_custom_category("tx_groc", "Groceries")
        db_module.remove_category("Groceries")
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertIsNone(rows[0]["custom_category"])

    def test_remove_nonexistent_category_raises(self):
        with self.assertRaises(ValueError):
            db_module.remove_category("DoesNotExist")

    def test_remove_top_level_category_raises(self):
        with self.assertRaises(ValueError):
            db_module.remove_category("Food & Dining")

    def test_remove_subcategory_then_readd_as_top_level(self):
        db_module.remove_category("Groceries")
        ok = db_module.add_category("Groceries")
        self.assertTrue(ok)
        cats = {c["name"]: c["parent"] for c in db_module.get_all_categories()}
        self.assertEqual(cats["Groceries"], "")

    # --- set_category_mapping ---

    def test_set_category_mapping_overrides(self):
        db_module.set_category_mapping("GENERAL_SERVICES", "Subscriptions")
        m = db_module.get_category_map()
        self.assertEqual(m["GENERAL_SERVICES"], "Subscriptions")

    def test_set_category_mapping_then_auto_categorize(self):
        db_module.upsert_transactions([self._tx("tx6", "GENERAL_SERVICES")])
        db_module.set_category_mapping("GENERAL_SERVICES", "Subscriptions")
        count = db_module.apply_auto_categorization()
        self.assertEqual(count, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Subscriptions")

    # --- get_uncategorized ---

    def test_get_uncategorized_returns_null_custom_category(self):
        db_module.upsert_transactions([self._tx("tx7", "")])
        uncategorized = db_module.get_uncategorized()
        self.assertTrue(any(r["transaction_id"] == "tx7" for r in uncategorized))

    def test_get_uncategorized_excludes_categorized(self):
        db_module.upsert_transactions([self._tx("tx8", "FOOD_AND_DRINK")])
        db_module.apply_auto_categorization()
        uncategorized = db_module.get_uncategorized()
        self.assertFalse(any(r["transaction_id"] == "tx8" for r in uncategorized))

    def test_get_uncategorized_respects_limit(self):
        txs = [self._tx(f"txL{i}", "") for i in range(10)]
        db_module.upsert_transactions(txs)
        result = db_module.get_uncategorized(limit=3)
        self.assertEqual(len(result), 3)

    # --- get_spending_summary ---

    def test_get_spending_summary(self):
        db_module.upsert_transactions([
            self._tx("s1", "FOOD_AND_DRINK", amount=40.0, date="2026-03-05"),
            self._tx("s2", "TRANSPORTATION", amount=15.0, date="2026-03-10"),
        ])
        db_module.apply_auto_categorization()
        summary = db_module.get_spending_summary("2026-03")
        totals = {r["category"]: r["total"] for r in summary}
        self.assertEqual(totals["Food & Dining"], 40.0)
        self.assertEqual(totals["Transport"], 15.0)

    def test_get_spending_summary_excludes_pending(self):
        db_module.upsert_transactions([
            self._tx("p1", "FOOD_AND_DRINK", amount=99.0, pending=True, date="2026-03-01"),
        ])
        db_module.apply_auto_categorization()
        summary = db_module.get_spending_summary("2026-03")
        totals = {r["category"]: r["total"] for r in summary}
        self.assertNotIn("Food & Dining", totals)

    def test_get_spending_summary_excludes_credits(self):
        db_module.upsert_transactions([
            self._tx("c1", "INCOME", amount=-1000.0, date="2026-03-01"),
        ])
        db_module.apply_auto_categorization()
        summary = db_module.get_spending_summary("2026-03")
        totals = {r["category"]: r["total"] for r in summary}
        self.assertNotIn("Income", totals)

    def test_get_spending_summary_excludes_other_months(self):
        db_module.upsert_transactions([
            self._tx("m1", "FOOD_AND_DRINK", amount=50.0, date="2026-02-15"),
        ])
        db_module.apply_auto_categorization()
        summary = db_module.get_spending_summary("2026-03")
        totals = {r["category"]: r["total"] for r in summary}
        self.assertNotIn("Food & Dining", totals)


if __name__ == "__main__":
    unittest.main()
