"""
Unit tests for budget tracking helpers in db.py.

Uses a temporary SQLite DB for each test — no side effects on ledger.db.

Run:
    python3 -m unittest test_budgets -v
"""

import os
import tempfile
import unittest
from pathlib import Path

import db as db_module


class TestBudgets(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _tx(self, tx_id, plaid_cat="FOOD_AND_DRINK", amount=10.0,
            pending=False, date="2026-03-01"):
        return {
            "transaction_id": tx_id, "date": date, "name": "Test",
            "merchant_name": "", "amount": amount,
            "plaid_category": plaid_cat, "account_id": "acc1", "pending": pending,
        }

    # --- set_budget / get_all_budgets ---

    def test_set_and_get_budget(self):
        db_module.set_budget("Food & Dining", 300.0)
        budgets = db_module.get_all_budgets()
        self.assertEqual(len(budgets), 1)
        self.assertEqual(budgets[0]["category"], "Food & Dining")
        self.assertEqual(budgets[0]["monthly_amount"], 300.0)

    def test_set_budget_updates_existing(self):
        db_module.set_budget("Food & Dining", 300.0)
        db_module.set_budget("Food & Dining", 400.0)
        budgets = db_module.get_all_budgets()
        self.assertEqual(len(budgets), 1)
        self.assertEqual(budgets[0]["monthly_amount"], 400.0)

    def test_multiple_budgets(self):
        db_module.set_budget("Food & Dining", 300.0)
        db_module.set_budget("Transport", 150.0)
        db_module.set_budget("Shopping", 200.0)
        self.assertEqual(len(db_module.get_all_budgets()), 3)

    def test_get_all_budgets_ordered_by_category(self):
        db_module.set_budget("Transport", 100.0)
        db_module.set_budget("Food & Dining", 300.0)
        names = [b["category"] for b in db_module.get_all_budgets()]
        self.assertEqual(names, sorted(names))

    def test_get_all_budgets_empty(self):
        self.assertEqual(db_module.get_all_budgets(), [])

    # --- get_budget_status ---

    def test_get_budget_status_under_budget(self):
        db_module.set_budget("Food & Dining", 500.0)
        db_module.upsert_transactions([self._tx("t1", "FOOD_AND_DRINK", 200.0)])
        db_module.apply_auto_categorization()
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        row = status["Food & Dining"]
        self.assertEqual(row["spent"], 200.0)
        self.assertEqual(row["budgeted"], 500.0)
        self.assertEqual(row["remaining"], 300.0)
        self.assertFalse(row["over_budget"])

    def test_get_budget_status_over_budget(self):
        db_module.set_budget("Food & Dining", 100.0)
        db_module.upsert_transactions([self._tx("t1", "FOOD_AND_DRINK", 150.0)])
        db_module.apply_auto_categorization()
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        row = status["Food & Dining"]
        self.assertTrue(row["over_budget"])
        self.assertEqual(row["remaining"], -50.0)

    def test_get_budget_status_no_spending_shows_budget(self):
        db_module.set_budget("Transport", 200.0)
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        self.assertIn("Transport", status)
        self.assertEqual(status["Transport"]["spent"], 0.0)
        self.assertEqual(status["Transport"]["budgeted"], 200.0)
        self.assertFalse(status["Transport"]["over_budget"])

    def test_get_budget_status_no_budget_shows_spending(self):
        db_module.upsert_transactions([self._tx("t1", "FOOD_AND_DRINK", 50.0)])
        db_module.apply_auto_categorization()
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        self.assertIn("Food & Dining", status)
        self.assertEqual(status["Food & Dining"]["budgeted"], 0.0)
        self.assertEqual(status["Food & Dining"]["spent"], 50.0)
        self.assertFalse(status["Food & Dining"]["over_budget"])

    def test_get_budget_status_over_budget_sorted_first(self):
        db_module.set_budget("Food & Dining", 100.0)
        db_module.set_budget("Transport", 500.0)
        db_module.upsert_transactions([
            self._tx("t1", "FOOD_AND_DRINK", 150.0),
            self._tx("t2", "TRANSPORTATION", 100.0),
        ])
        db_module.apply_auto_categorization()
        result = db_module.get_budget_status("2026-03")
        self.assertTrue(result[0]["over_budget"])
        self.assertEqual(result[0]["category"], "Food & Dining")

    def test_get_budget_status_excludes_pending(self):
        db_module.set_budget("Food & Dining", 100.0)
        db_module.upsert_transactions([
            self._tx("t1", "FOOD_AND_DRINK", 200.0, pending=True)
        ])
        db_module.apply_auto_categorization()
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        self.assertEqual(status["Food & Dining"]["spent"], 0.0)
        self.assertFalse(status["Food & Dining"]["over_budget"])

    def test_get_budget_status_wrong_month_shows_zero_spent(self):
        db_module.set_budget("Food & Dining", 300.0)
        db_module.upsert_transactions([
            self._tx("t1", "FOOD_AND_DRINK", 100.0, date="2026-02-15")
        ])
        db_module.apply_auto_categorization()
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        self.assertEqual(status["Food & Dining"]["spent"], 0.0)
        self.assertEqual(status["Food & Dining"]["remaining"], 300.0)

    def test_get_budget_status_empty_returns_empty(self):
        self.assertEqual(db_module.get_budget_status("2026-03"), [])

    def test_get_budget_status_exactly_at_budget_not_over(self):
        db_module.set_budget("Food & Dining", 100.0)
        db_module.upsert_transactions([self._tx("t1", "FOOD_AND_DRINK", 100.0)])
        db_module.apply_auto_categorization()
        status = {r["category"]: r for r in db_module.get_budget_status("2026-03")}
        self.assertFalse(status["Food & Dining"]["over_budget"])
        self.assertEqual(status["Food & Dining"]["remaining"], 0.0)


if __name__ == "__main__":
    unittest.main()
