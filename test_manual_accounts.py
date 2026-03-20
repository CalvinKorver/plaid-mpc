"""
Unit tests for manual account functionality in db.py.

Uses a temporary SQLite DB — no Plaid API calls, no real DB mutations.
"""

import os
import tempfile
import unittest
from pathlib import Path

import db as db_module


class TestManualAccounts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    # ------------------------------------------------------------------
    # create_manual_account
    # ------------------------------------------------------------------

    def test_create_returns_manual_prefixed_id(self):
        account_id = db_module.create_manual_account("Rollover IRA", "investment", "ira", 48015.81, "Fidelity")
        self.assertTrue(account_id.startswith("manual_"))

    def test_create_stores_account_with_is_manual_flag(self):
        account_id = db_module.create_manual_account("Workday 401k", "investment", "401k", 134711.42, "Fidelity")
        accounts = db_module.get_all_accounts()
        match = next((a for a in accounts if a["account_id"] == account_id), None)
        self.assertIsNotNone(match)
        self.assertEqual(match["is_manual"], 1)
        self.assertEqual(match["name"], "Workday 401k")
        self.assertEqual(match["institution"], "Fidelity")

    def test_create_inserts_initial_balance_snapshot(self):
        account_id = db_module.create_manual_account("HSA", "investment", "hsa", 14386.28)
        accounts = db_module.get_manual_accounts()
        match = next((a for a in accounts if a["account_id"] == account_id), None)
        self.assertIsNotNone(match)
        self.assertAlmostEqual(match["balance"], 14386.28)

    def test_create_multiple_accounts_all_appear(self):
        db_module.create_manual_account("Workday 401k", "investment", "401k", 134711.42, "Fidelity")
        db_module.create_manual_account("Roblox 401k", "investment", "401k", 22733.52, "Fidelity")
        db_module.create_manual_account("Rollover IRA", "investment", "ira", 48015.81, "Fidelity")
        db_module.create_manual_account("HSA", "investment", "hsa", 14386.28, "Fidelity")
        accounts = db_module.get_manual_accounts()
        self.assertEqual(len(accounts), 4)

    # ------------------------------------------------------------------
    # update_manual_account_balance
    # ------------------------------------------------------------------

    def test_update_balance_returns_true_for_valid_account(self):
        account_id = db_module.create_manual_account("Rollover IRA", "investment", "ira", 48015.81)
        ok = db_module.update_manual_account_balance(account_id, 49000.00)
        self.assertTrue(ok)

    def test_update_balance_reflects_new_value_in_get_manual_accounts(self):
        account_id = db_module.create_manual_account("Rollover IRA", "investment", "ira", 48015.81)
        db_module.update_manual_account_balance(account_id, 49000.00)
        accounts = db_module.get_manual_accounts()
        match = next(a for a in accounts if a["account_id"] == account_id)
        self.assertAlmostEqual(match["balance"], 49000.00)

    def test_update_balance_returns_false_for_nonexistent(self):
        ok = db_module.update_manual_account_balance("manual_doesnotexist", 100.0)
        self.assertFalse(ok)

    def test_update_balance_returns_false_for_plaid_account(self):
        db_module.upsert_accounts([{
            "account_id": "plaid_acc_1",
            "name": "Checking",
            "official_name": "",
            "type": "depository",
            "subtype": "checking",
            "mask": "1234",
            "institution": "Chase",
            "token_hash": "tok1",
        }])
        ok = db_module.update_manual_account_balance("plaid_acc_1", 500.0)
        self.assertFalse(ok)

    # ------------------------------------------------------------------
    # delete_manual_account
    # ------------------------------------------------------------------

    def test_delete_removes_account(self):
        account_id = db_module.create_manual_account("HSA", "investment", "hsa", 14386.28)
        db_module.delete_manual_account(account_id)
        accounts = db_module.get_manual_accounts()
        self.assertFalse(any(a["account_id"] == account_id for a in accounts))

    def test_delete_removes_balance_snapshots(self):
        account_id = db_module.create_manual_account("HSA", "investment", "hsa", 14386.28)
        db_module.delete_manual_account(account_id)
        # Net worth should not include this account
        nw = db_module.get_net_worth_history("2026-03-01", "2026-03-19")
        self.assertIsNone(nw["current_net_worth"])

    def test_delete_returns_false_for_nonexistent(self):
        ok = db_module.delete_manual_account("manual_ghost")
        self.assertFalse(ok)

    def test_delete_returns_false_for_plaid_account(self):
        db_module.upsert_accounts([{
            "account_id": "plaid_acc_2",
            "name": "Savings",
            "official_name": "",
            "type": "depository",
            "subtype": "savings",
            "mask": "5678",
            "institution": "BofA",
            "token_hash": "tok2",
        }])
        ok = db_module.delete_manual_account("plaid_acc_2")
        self.assertFalse(ok)

    # ------------------------------------------------------------------
    # Net worth includes manual accounts
    # ------------------------------------------------------------------

    def test_manual_account_balance_included_in_net_worth(self):
        db_module.create_manual_account("Workday 401k", "investment", "401k", 134711.42, "Fidelity")
        db_module.create_manual_account("Roblox 401k", "investment", "401k", 22733.52, "Fidelity")
        db_module.create_manual_account("Rollover IRA", "investment", "ira", 48015.81, "Fidelity")
        db_module.create_manual_account("HSA", "investment", "hsa", 14386.28, "Fidelity")
        nw = db_module.get_net_worth_history("2026-03-13", "2026-03-19")
        expected = 134711.42 + 22733.52 + 48015.81 + 14386.28
        self.assertIsNotNone(nw["current_net_worth"])
        self.assertAlmostEqual(nw["current_net_worth"], expected, places=2)

    # ------------------------------------------------------------------
    # upsert_accounts does not overwrite manual accounts
    # ------------------------------------------------------------------

    def test_plaid_sync_does_not_overwrite_manual_account(self):
        account_id = db_module.create_manual_account("HSA", "investment", "hsa", 14386.28, "Fidelity")
        # Simulate Plaid returning an account with the same ID (edge case)
        db_module.upsert_accounts([{
            "account_id": account_id,
            "name": "OVERWRITTEN",
            "official_name": "should not appear",
            "type": "depository",
            "subtype": "checking",
            "mask": "",
            "institution": "Evil Bank",
            "token_hash": "tok3",
        }])
        accounts = db_module.get_all_accounts()
        match = next(a for a in accounts if a["account_id"] == account_id)
        self.assertEqual(match["name"], "HSA")
        self.assertEqual(match["is_manual"], 1)


if __name__ == "__main__":
    unittest.main()
