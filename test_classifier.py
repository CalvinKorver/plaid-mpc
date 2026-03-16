"""
Unit tests for classifier.py.

Uses a temporary SQLite DB and a mocked anthropic.Anthropic client.
No real API calls are made.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import db as db_module
import classifier as classifier_module


def _make_mock_client(responses: list[dict]) -> MagicMock:
    """
    Build a mock anthropic.Anthropic client whose messages.create() returns
    successive responses from the provided list of {transaction_id: {category, reason}} dicts.
    """
    client = MagicMock()
    call_count = [0]

    def fake_create(**kwargs):
        idx = call_count[0]
        call_count[0] += 1
        payload = responses[idx] if idx < len(responses) else {}
        msg = MagicMock()
        msg.content = [MagicMock(text=json.dumps(payload))]
        return msg

    client.messages.create.side_effect = fake_create
    return client


class TestApplyClaudeCategorization(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _tx(self, tx_id, name="Coffee Shop", merchant_name="Starbucks", amount=5.50, category=None):
        db_module.upsert_transactions([{
            "transaction_id": tx_id,
            "date": "2026-03-01",
            "name": name,
            "merchant_name": merchant_name,
            "amount": amount,
            "plaid_category": "FOOD_AND_DRINK",
            "account_id": "acc1",
            "pending": False,
        }])
        if category:
            db_module.set_custom_category(tx_id, category)

    def _candidates(self, *args):
        """Build candidate dicts from (tx_id, current_category) tuples."""
        rows = []
        for tx_id, current_cat in args:
            rows.append({
                "transaction_id": tx_id,
                "name": "Coffee Shop",
                "merchant_name": "Starbucks",
                "amount": 5.50,
                "current_category": current_cat,
            })
        return rows

    def test_noop_when_no_candidates(self):
        count = classifier_module.apply_claude_categorization([])
        self.assertEqual(count, 0)

    def test_skips_when_api_key_missing(self):
        self._tx("tx1")
        candidates = self._candidates(("tx1", None))
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            count = classifier_module.apply_claude_categorization(candidates)
        self.assertEqual(count, 0)

    def test_assigns_category_to_uncategorized(self):
        self._tx("tx1")
        candidates = self._candidates(("tx1", None))
        mock_client = _make_mock_client([
            {"tx1": {"category": "Food & Dining", "reason": "Starbucks is a coffee chain."}}
        ])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                count = classifier_module.apply_claude_categorization(candidates)
        self.assertEqual(count, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Food & Dining")

    def test_validates_and_keeps_auto_label(self):
        """Claude agrees with the existing auto-label."""
        self._tx("tx1", category="Shopping")
        candidates = self._candidates(("tx1", "Shopping"))
        mock_client = _make_mock_client([
            {"tx1": {"category": "Shopping", "reason": "AMAZON is a retail marketplace."}}
        ])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                count = classifier_module.apply_claude_categorization(candidates)
        self.assertEqual(count, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Shopping")

    def test_overrides_wrong_auto_label(self):
        """Claude overrides an incorrectly auto-labeled transaction."""
        self._tx("tx1", category="Shopping")
        candidates = self._candidates(("tx1", "Shopping"))
        mock_client = _make_mock_client([
            {"tx1": {"category": "Groceries", "reason": "Whole Foods is a grocery store."}}
        ])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                count = classifier_module.apply_claude_categorization(candidates)
        self.assertEqual(count, 1)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertEqual(rows[0]["custom_category"], "Groceries")

    def test_audit_log_written_for_each_transaction(self):
        self._tx("tx1")
        self._tx("tx2", name="Uber", merchant_name="Uber")
        candidates = self._candidates(("tx1", None), ("tx2", "Transport"))
        mock_client = _make_mock_client([{
            "tx1": {"category": "Food & Dining", "reason": "Coffee shop."},
            "tx2": {"category": "Transport", "reason": "Uber is a ride-share."},
        }])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                classifier_module.apply_claude_categorization(candidates)

        import sqlite3
        conn = sqlite3.connect(db_module.DB_PATH)
        conn.row_factory = sqlite3.Row
        logs = conn.execute(
            "SELECT * FROM categorization_log WHERE source = 'claude' ORDER BY id"
        ).fetchall()
        conn.close()

        self.assertEqual(len(logs), 2)
        ids = {r["transaction_id"] for r in logs}
        self.assertIn("tx1", ids)
        self.assertIn("tx2", ids)
        for row in logs:
            self.assertEqual(row["source"], "claude")
            self.assertIsNotNone(row["reason"])
            self.assertEqual(row["model"], classifier_module.MODEL)

    def test_handles_api_error_gracefully(self):
        self._tx("tx1")
        candidates = self._candidates(("tx1", None))
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API error")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                count = classifier_module.apply_claude_categorization(candidates)
        self.assertEqual(count, 0)

    def test_batches_large_transaction_set(self):
        """120 candidates → 3 API calls (batches of BATCH_SIZE=50)."""
        batch_size = classifier_module.BATCH_SIZE
        n = batch_size * 2 + 20  # 3 batches
        tx_ids = [f"tx{i}" for i in range(n)]
        for tx_id in tx_ids:
            self._tx(tx_id)

        candidates = self._candidates(*[(tx_id, None) for tx_id in tx_ids])

        # Build a response for each batch
        def make_response(start, end):
            return {
                tx_ids[i]: {"category": "Other", "reason": "Misc."}
                for i in range(start, min(end, n))
            }

        responses = [
            make_response(0, batch_size),
            make_response(batch_size, batch_size * 2),
            make_response(batch_size * 2, n),
        ]
        mock_client = _make_mock_client(responses)
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                count = classifier_module.apply_claude_categorization(candidates)

        self.assertEqual(mock_client.messages.create.call_count, 3)
        self.assertEqual(count, n)

    def test_skips_unknown_category_in_response(self):
        """Claude returns a category not in the taxonomy — transaction is skipped."""
        self._tx("tx1")
        candidates = self._candidates(("tx1", None))
        mock_client = _make_mock_client([
            {"tx1": {"category": "NotARealCategory", "reason": "Some reason."}}
        ])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                count = classifier_module.apply_claude_categorization(candidates)
        self.assertEqual(count, 0)
        rows = db_module.query_transactions("2026-03-01", "2026-03-01")
        self.assertIsNone(rows[0]["custom_category"])


class TestApplyClaudeCategorizationFromDb(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_noop_when_nothing_uncategorized(self):
        # All transactions already have custom_category
        db_module.upsert_transactions([{
            "transaction_id": "tx1",
            "date": "2026-03-01",
            "name": "Amazon",
            "merchant_name": "Amazon",
            "amount": 20.0,
            "plaid_category": "GENERAL_MERCHANDISE",
            "account_id": "acc1",
            "pending": False,
        }])
        db_module.set_custom_category("tx1", "Shopping")
        count = classifier_module.apply_claude_categorization_from_db()
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
