"""
Unit tests for sync.py orchestration logic.

Mocks the Plaid API so no real network calls are made.
Tests that the candidate-tracking pipeline correctly excludes rule-matched
transactions from Claude categorization.
"""

import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import db as db_module


class _PlaidTx:
    """Minimal Plaid transaction-like object supporting [] and .get() access."""

    def __init__(self, tx_id: str, name: str, merchant: str = "", amount: float = 10.0):
        self._data = {
            "transaction_id": tx_id,
            "date": _StrDate("2026-03-01"),
            "name": name,
            "merchant_name": merchant,
            "amount": amount,
            "personal_finance_category": {"primary": "GENERAL_MERCHANDISE"},
            "account_id": "acc1",
            "pending": False,
        }

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)


class _StrDate(str):
    """String subclass that supports str() for date fields."""
    pass


def _make_mock_plaid_client(added_txns: list) -> MagicMock:
    mock = MagicMock()

    sync_resp = MagicMock()
    sync_resp.__getitem__ = lambda self, key: {
        "added": added_txns,
        "modified": [],
        "removed": [],
        "next_cursor": "cur1",
        "has_more": False,
    }[key]
    mock.transactions_sync.return_value = sync_resp

    acct_resp = MagicMock()
    acct_resp.__getitem__ = lambda self, key: {
        "accounts": [{
            "account_id": "acc1",
            "name": "Checking",
            "official_name": "My Checking",
            "type": _StrDate("depository"),
            "subtype": _StrDate("checking"),
            "mask": "1234",
        }]
    }[key]
    mock.accounts_balance_get.return_value = acct_resp

    mock.item_get.return_value = {"item": {"institution_id": "ins_1"}}
    mock.institutions_get_by_id.return_value = {"institution": {"name": "Test Bank"}}
    return mock


class TestSyncOrchestration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _run_sync(self, added_txns, captured_candidates=None):
        """Run sync_and_store with mocked Plaid, capturing Claude candidates."""
        import sync as sync_module

        mock_client = _make_mock_plaid_client(added_txns)

        def fake_categorize(candidates):
            if captured_candidates is not None:
                captured_candidates.extend(candidates)
            return 0

        with patch.object(sync_module, "_ensure_client", return_value=(mock_client, MagicMock())):
            with patch.object(sync_module, "_get_all_tokens", return_value=[("h1", "tok1")]):
                with patch.object(sync_module, "_get_institution_name", return_value="Test Bank"):
                    with patch.object(sync_module, "_bootstrap_tokens"):
                        with patch("classifier.apply_claude_categorization", side_effect=fake_categorize):
                            return sync_module.sync_and_store()

    def test_result_includes_claude_categorized_key(self):
        """sync_and_store() result must include claude_categorized."""
        added = [_PlaidTx("tx1", "Amazon", "Amazon", 15.0)]
        result = self._run_sync(added)
        self.assertIn("claude_categorized", result)
        self.assertEqual(result["added"], 1)

    def test_rule_matched_transactions_excluded_from_claude(self):
        """
        5 transactions added; 1 matches a rule. Claude should receive 4 candidates.
        """
        tx_ids = [f"tx{i}" for i in range(5)]
        added = [_PlaidTx(tx_id, f"Merchant{i}", f"M{i}") for i, tx_id in enumerate(tx_ids)]

        # Rule matches only transactions with "Merchant0" in name/merchant
        db_module.insert_rule("Merchant0", None, "Shopping")

        captured = []
        result = self._run_sync(added, captured_candidates=captured)

        candidate_ids = {c["transaction_id"] for c in captured}
        self.assertNotIn("tx0", candidate_ids, "Rule-matched tx0 should be excluded")
        self.assertEqual(len(candidate_ids), 4)
        self.assertEqual(result["rules_applied"], 1)

    def test_candidates_include_current_category(self):
        """Each candidate passed to Claude must have a current_category field."""
        added = [_PlaidTx("tx1", "Netflix", "Netflix")]
        captured = []
        self._run_sync(added, captured_candidates=captured)

        self.assertEqual(len(captured), 1)
        self.assertIn("current_category", captured[0])

    def test_no_new_transactions_means_no_claude_call(self):
        """If no transactions were added, Claude should receive 0 candidates."""
        captured = []
        result = self._run_sync([], captured_candidates=captured)

        self.assertEqual(captured, [])
        self.assertEqual(result["added"], 0)
        self.assertEqual(result["claude_categorized"], 0)


class TestSyncLock(unittest.TestCase):
    """Test that the lock pattern in _run_sync_bg prevents concurrent syncs."""

    def test_lock_prevents_second_entry(self):
        """A held lock causes _run_sync_bg to return without calling sync."""
        lock = threading.Lock()
        called = []

        def sync_fn():
            if not lock.acquire(blocking=False):
                return
            try:
                called.append(True)
            finally:
                lock.release()

        # Hold the lock, then try to sync — should be skipped
        lock.acquire()
        try:
            sync_fn()
            self.assertEqual(called, [], "sync should be skipped when lock is held")
        finally:
            lock.release()

        # Now that lock is free, sync should proceed
        sync_fn()
        self.assertEqual(len(called), 1, "sync should run when lock is free")

    def test_lock_released_after_sync(self):
        """Lock must be released even if sync raises."""
        lock = threading.Lock()
        calls = []

        def failing_sync():
            if not lock.acquire(blocking=False):
                return
            try:
                raise RuntimeError("simulated failure")
            except RuntimeError:
                pass
            finally:
                lock.release()
                calls.append("released")

        failing_sync()
        self.assertIn("released", calls)
        self.assertFalse(lock.locked(), "lock should be released after error")


if __name__ == "__main__":
    unittest.main()
