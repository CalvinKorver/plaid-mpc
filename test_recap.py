"""
Unit tests for recap.py.

Uses a temporary SQLite DB and a mocked anthropic.Anthropic client.
No real API or Plaid calls are made.
"""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import db as db_module
import recap as recap_module


def _make_mock_narrative_client(text: str) -> MagicMock:
    """Build a mock anthropic.Anthropic client whose messages.create() returns `text`."""
    client = MagicMock()
    msg = MagicMock()
    msg.content = [MagicMock(text=text)]
    client.messages.create.return_value = msg
    return client


def _tx(tx_id, tx_date, name, amount, category=None, pending=False, account_id="acc1"):
    """Helper — build a transaction dict and insert it into the DB."""
    db_module.upsert_transactions([{
        "transaction_id": tx_id,
        "date": tx_date,
        "name": name,
        "merchant_name": name,
        "amount": amount,
        "plaid_category": "FOOD_AND_DRINK",
        "account_id": account_id,
        "pending": pending,
    }])
    if category:
        db_module.set_custom_category(tx_id, category)


def _snapshot(account_id, balance, snapped_at, acct_type="depository"):
    """Insert a balance snapshot directly."""
    db_module.upsert_balance_snapshot(
        account_id=account_id,
        balance=balance,
        available=balance,
        currency="USD",
        account_type=acct_type,
        account_subtype="checking",
        snapped_at=snapped_at,
    )


class TestGetWeekSpendingData(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_total_sums_only_debits_in_range(self):
        # 3 debits in range, 1 outside range, 1 credit
        _tx("t1", "2026-03-09", "Coffee", 10.0, "Food & Dining")
        _tx("t2", "2026-03-10", "Lunch", 20.0, "Food & Dining")
        _tx("t3", "2026-03-11", "Dinner", 30.0, "Food & Dining")
        _tx("t4", "2026-03-16", "Outside", 99.0, "Other")   # outside week
        _tx("t5", "2026-03-09", "Payroll", -1000.0, "Income")  # credit

        data = recap_module.get_week_spending_data("2026-03-09")
        self.assertAlmostEqual(data["total"], 60.0)

    def test_by_day_zero_fills_missing_days(self):
        _tx("t1", "2026-03-09", "Coffee", 10.0)  # Monday
        _tx("t2", "2026-03-11", "Lunch", 20.0)   # Wednesday

        data = recap_module.get_week_spending_data("2026-03-09")
        by_day = data["by_day"]

        # All 7 days should be present
        self.assertEqual(len(by_day), 7)
        self.assertAlmostEqual(by_day["2026-03-09"], 10.0)
        self.assertAlmostEqual(by_day["2026-03-10"], 0.0)
        self.assertAlmostEqual(by_day["2026-03-11"], 20.0)
        for d in ["2026-03-12", "2026-03-13", "2026-03-14", "2026-03-15"]:
            self.assertAlmostEqual(by_day[d], 0.0)

    def test_by_category_top_merchants(self):
        # 4 Food & Dining transactions; top 3 should be included, sorted desc
        _tx("t1", "2026-03-09", "Fancy Restaurant", 80.0, "Food & Dining")
        _tx("t2", "2026-03-09", "Coffee Shop", 5.0, "Food & Dining")
        _tx("t3", "2026-03-09", "Lunch Place", 15.0, "Food & Dining")
        _tx("t4", "2026-03-09", "Diner", 30.0, "Food & Dining")

        data = recap_module.get_week_spending_data("2026-03-09")
        cats = {c["category"]: c for c in data["by_category"]}
        self.assertIn("Food & Dining", cats)
        txs = cats["Food & Dining"]["transactions"]
        self.assertLessEqual(len(txs), 3)
        # Should be sorted by amount descending
        amounts = [t["amount"] for t in txs]
        self.assertEqual(amounts, sorted(amounts, reverse=True))
        # Top entry should be the $80 one
        self.assertAlmostEqual(txs[0]["amount"], 80.0)

    def test_income_total_captures_credits(self):
        _tx("t1", "2026-03-09", "Payroll", -3500.0, "Income")
        _tx("t2", "2026-03-09", "Refund", -50.0, "Shopping")

        data = recap_module.get_week_spending_data("2026-03-09")
        self.assertAlmostEqual(data["income_total"], 3550.0)
        self.assertAlmostEqual(data["total"], 0.0)

    def test_pending_excluded(self):
        _tx("t1", "2026-03-09", "Pending charge", 50.0, pending=True)
        _tx("t2", "2026-03-09", "Posted charge", 25.0)

        data = recap_module.get_week_spending_data("2026-03-09")
        self.assertAlmostEqual(data["total"], 25.0)


class TestComputeSpendingComparison(unittest.TestCase):
    def test_direction_down_when_current_less_than_prior(self):
        current = {"total": 100.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        prior = {"total": 500.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        result = recap_module.compute_spending_comparison(current, prior)
        self.assertEqual(result["direction"], "down")
        self.assertAlmostEqual(result["change_pct"], -80.0)
        self.assertIn("down", result["description"])

    def test_direction_up_when_current_more_than_prior(self):
        current = {"total": 300.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        prior = {"total": 200.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        result = recap_module.compute_spending_comparison(current, prior)
        self.assertEqual(result["direction"], "up")
        self.assertAlmostEqual(result["change_pct"], 50.0)

    def test_direction_same_when_equal(self):
        current = {"total": 100.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        prior = {"total": 100.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        result = recap_module.compute_spending_comparison(current, prior)
        self.assertEqual(result["direction"], "same")
        self.assertAlmostEqual(result["change_pct"], 0.0)

    def test_change_pct_none_when_prior_is_zero(self):
        current = {"total": 50.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        prior = {"total": 0.0, "income_total": 0.0, "by_day": {}, "by_category": []}
        result = recap_module.compute_spending_comparison(current, prior)
        self.assertIsNone(result["change_pct"])


class TestGetNetWorthHistory(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_net_worth_sums_depository_minus_credit(self):
        _snapshot("dep1", 10000.0, "2026-03-09T12:00:00+00:00", acct_type="depository")
        _snapshot("cred1", 3000.0, "2026-03-09T12:00:00+00:00", acct_type="credit")

        result = db_module.get_net_worth_history("2026-03-09", "2026-03-15")
        self.assertIsNotNone(result["current_net_worth"])
        self.assertAlmostEqual(result["current_net_worth"], 7000.0)

    def test_uses_most_recent_snapshot_before_date(self):
        # Two snapshots for same account on Mon and Wed; query for Tue should use Mon
        _snapshot("dep1", 10000.0, "2026-03-09T12:00:00+00:00")  # Mon
        _snapshot("dep1", 9000.0, "2026-03-11T12:00:00+00:00")   # Wed

        result = db_module.get_net_worth_history("2026-03-09", "2026-03-15")
        by_day = result["by_day"]
        # Mon should use the Mon snapshot
        self.assertAlmostEqual(by_day.get("2026-03-09", 0.0), 10000.0)
        # Tue should still use the Mon snapshot (most recent at-or-before Tue)
        self.assertAlmostEqual(by_day.get("2026-03-10", 0.0), 10000.0)
        # Wed and later should use the Wed snapshot
        self.assertAlmostEqual(by_day.get("2026-03-11", 0.0), 9000.0)

    def test_returns_none_when_no_snapshots(self):
        result = db_module.get_net_worth_history("2026-03-09", "2026-03-15")
        self.assertIsNone(result["current_net_worth"])
        self.assertIsNone(result["week_start_net_worth"])
        self.assertEqual(result["by_day"], {})


class TestGenerateNarrative(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _sample_recap(self):
        return {
            "week_start": "2026-03-09",
            "week_end": "2026-03-15",
            "spending": {
                "total": 200.0,
                "prior_week_total": 400.0,
                "change_amount": -200.0,
                "change_pct": -50.0,
                "direction": "down",
                "description": "down 50%",
                "by_day": {},
                "by_category": [],
                "income_total": 3000.0,
            },
            "net_worth": {
                "current": 50000.0,
                "prior_week": 49000.0,
                "change_amount": 1000.0,
                "change_pct": 2.04,
                "by_day": {},
                "breakdown": {},
            },
            "narrative": "",
        }

    def test_returns_empty_string_when_api_key_missing(self):
        recap_data = self._sample_recap()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.generate_narrative(recap_data)
        self.assertEqual(result, "")

    def test_returns_narrative_on_success(self):
        recap_data = self._sample_recap()
        mock_client = _make_mock_narrative_client("Great week! Spending was down significantly.")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.generate_narrative(recap_data)
        self.assertEqual(result, "Great week! Spending was down significantly.")

    def test_returns_empty_string_on_api_error(self):
        recap_data = self._sample_recap()
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API error")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.generate_narrative(recap_data)
        self.assertEqual(result, "")


class TestBuildWeeklyRecap(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_returns_correct_week_bounds(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.build_weekly_recap("2026-03-09")
        self.assertEqual(result["week_start"], "2026-03-09")
        self.assertEqual(result["week_end"], "2026-03-15")

    def test_spending_section_present_and_structured(self):
        _tx("t1", "2026-03-09", "Coffee", 15.0, "Food & Dining")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.build_weekly_recap("2026-03-09")
        s = result["spending"]
        self.assertIn("total", s)
        self.assertIn("by_day", s)
        self.assertIn("by_category", s)
        self.assertIn("income_total", s)
        self.assertIn("direction", s)
        self.assertAlmostEqual(s["total"], 15.0)

    def test_net_worth_section_present(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.build_weekly_recap("2026-03-09")
        nw = result["net_worth"]
        self.assertIn("current", nw)
        self.assertIn("by_day", nw)
        self.assertIn("breakdown", nw)

    def test_no_crash_when_no_transactions_or_snapshots(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.build_weekly_recap("2026-03-09")
        self.assertAlmostEqual(result["spending"]["total"], 0.0)
        self.assertIsNone(result["net_worth"]["current"])
        self.assertEqual(result["narrative"], "")

    def test_narrative_included_when_api_available(self):
        mock_client = _make_mock_narrative_client("A quiet spending week.")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.build_weekly_recap("2026-03-09")
        self.assertEqual(result["narrative"], "A quiet spending week.")


class TestBuildNarrativePromptWithMarketContext(unittest.TestCase):
    def _sample_recap(self, market_context=None):
        base = {
            "week_start": "2026-03-09",
            "week_end": "2026-03-15",
            "spending": {
                "total": 200.0,
                "prior_week_total": 400.0,
                "change_amount": -200.0,
                "change_pct": -50.0,
                "direction": "down",
                "description": "down 50%",
                "by_day": {},
                "by_category": [],
                "income_total": 3000.0,
            },
            "net_worth": {
                "current": 50000.0,
                "prior_week": 49000.0,
                "change_amount": 1000.0,
                "change_pct": 2.04,
                "by_day": {},
                "breakdown": {},
            },
            "narrative": "",
        }
        if market_context is not None:
            base["market_context"] = market_context
        return base

    def test_market_prompt_includes_market_data_when_present(self):
        recap_data = self._sample_recap(market_context={
            "snapshot": {"S&P 500": {"start": 5000.0, "end": 5075.0, "change_pct": 1.5}},
            "finance_headlines": [{"title": "Fed holds rates", "published": "2026-03-10"}],
            "top_headlines": [{"title": "Tech earnings beat", "published": "2026-03-12"}],
        })
        prompt = recap_module.build_market_narrative_prompt(recap_data)
        self.assertIn("S&P 500", prompt)
        self.assertIn("Fed holds rates", prompt)
        self.assertIn("Tech earnings beat", prompt)

    def test_prompt_omits_market_section_when_context_empty(self):
        recap_data = self._sample_recap(market_context={
            "snapshot": {},
            "finance_headlines": [],
            "top_headlines": [],
        })
        prompt = recap_module.build_narrative_prompt(recap_data)
        self.assertNotIn("Market performance", prompt)
        self.assertNotIn("finance headlines", prompt.lower())

    def test_prompt_omits_market_section_when_key_missing(self):
        # Backward compat — no market_context key at all
        recap_data = self._sample_recap()
        # Should not raise KeyError
        prompt = recap_module.build_narrative_prompt(recap_data)
        self.assertNotIn("Market performance", prompt)


class TestBuildWeeklyRecapMarketContext(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _patch_market(self, snapshot=None, finance_headlines=None, top_headlines=None):
        """Return a context manager that patches all three market functions."""
        from unittest.mock import patch as _patch
        import contextlib

        @contextlib.contextmanager
        def _cm():
            with _patch("recap.market.get_market_snapshot", return_value=snapshot or {}), \
                 _patch("recap.market.get_finance_headlines", return_value=finance_headlines or []), \
                 _patch("recap.market.get_top_news_headlines", return_value=top_headlines or []):
                yield

        return _cm()

    def test_market_context_key_present_in_result(self):
        with self._patch_market(
            snapshot={"S&P 500": {"start": 5000.0, "end": 5050.0, "change_pct": 1.0}},
            finance_headlines=[{"title": "Finance news", "published": "2026-03-10"}],
            top_headlines=[{"title": "Top news", "published": "2026-03-11"}],
        ):
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("ANTHROPIC_API_KEY", None)
                result = recap_module.build_weekly_recap("2026-03-09")
        ctx = result["market_context"]
        self.assertIn("snapshot", ctx)
        self.assertIn("finance_headlines", ctx)
        self.assertIn("top_headlines", ctx)

    def test_week_dates_passed_to_headline_functions(self):
        with patch("recap.market.get_market_snapshot", return_value={}), \
             patch("recap.market.get_finance_headlines", return_value=[]) as mock_fin, \
             patch("recap.market.get_top_news_headlines", return_value=[]) as mock_top:
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("ANTHROPIC_API_KEY", None)
                recap_module.build_weekly_recap("2026-03-09")
        # Both wrappers must receive the week dates as first two positional args
        fin_args = mock_fin.call_args[0]
        top_args = mock_top.call_args[0]
        self.assertEqual(fin_args[0], "2026-03-09")
        self.assertEqual(fin_args[1], "2026-03-15")
        self.assertEqual(top_args[0], "2026-03-09")
        self.assertEqual(top_args[1], "2026-03-15")

    def test_market_context_empty_on_all_failures(self):
        with self._patch_market():
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("ANTHROPIC_API_KEY", None)
                result = recap_module.build_weekly_recap("2026-03-09")
        self.assertEqual(result["market_context"]["snapshot"], {})
        self.assertEqual(result["market_context"]["finance_headlines"], [])
        self.assertEqual(result["market_context"]["top_headlines"], [])

    def test_narrative_generated_with_market_context(self):
        mock_client = _make_mock_narrative_client("Great week with steady markets.")
        with self._patch_market(
            snapshot={"S&P 500": {"start": 5000.0, "end": 5050.0, "change_pct": 1.0}},
            finance_headlines=[{"title": "Markets up", "published": "2026-03-10"}],
            top_headlines=[],
        ):
            with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
                with patch("anthropic.Anthropic", return_value=mock_client):
                    result = recap_module.build_weekly_recap("2026-03-09")
        self.assertEqual(result["narrative"], "Great week with steady markets.")


class TestGenerateMarketNarrative(unittest.TestCase):
    def _sample_recap(self):
        return {
            "week_start": "2026-03-09",
            "week_end": "2026-03-15",
            "spending": {
                "total": 200.0, "prior_week_total": 400.0, "change_amount": -200.0,
                "change_pct": -50.0, "direction": "down", "description": "down 50%",
                "by_day": {}, "by_category": [], "income_total": 3000.0,
            },
            "net_worth": {
                "current": 50000.0, "prior_week": 49000.0,
                "change_amount": 1000.0, "change_pct": 2.04, "by_day": {}, "breakdown": {},
            },
            "market_context": {"snapshot": {}, "finance_headlines": [], "top_headlines": []},
            "narrative": "",
        }

    def test_returns_headline_and_narrative_from_valid_json(self):
        payload = '{"headline": "Markets fell on rate fears", "narrative": "The S&P dropped 2%."}'
        mock_client = _make_mock_narrative_client(payload)
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.generate_market_narrative(self._sample_recap())
        self.assertEqual(result["headline"], "Markets fell on rate fears")
        self.assertEqual(result["narrative"], "The S&P dropped 2%.")

    def test_strips_markdown_code_fences(self):
        payload = '```json\n{"headline": "Markets rose on strong jobs data", "narrative": "Indexes up."}\n```'
        mock_client = _make_mock_narrative_client(payload)
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.generate_market_narrative(self._sample_recap())
        self.assertEqual(result["headline"], "Markets rose on strong jobs data")
        self.assertEqual(result["narrative"], "Indexes up.")

    def test_falls_back_gracefully_when_json_invalid(self):
        plain_text = "Markets were volatile this week amid uncertainty."
        mock_client = _make_mock_narrative_client(plain_text)
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.generate_market_narrative(self._sample_recap())
        self.assertEqual(result["headline"], "")
        self.assertEqual(result["narrative"], plain_text)

    def test_returns_empty_when_api_key_not_set(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.generate_market_narrative(self._sample_recap())
        self.assertEqual(result, {"headline": "", "narrative": ""})

    def test_returns_empty_on_api_exception(self):
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API error")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            with patch("anthropic.Anthropic", return_value=mock_client):
                result = recap_module.generate_market_narrative(self._sample_recap())
        self.assertEqual(result, {"headline": "", "narrative": ""})


class TestBuildWeeklyRecapDualNarratives(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db_module.DB_PATH = Path(self.tmp.name)
        db_module.init_db()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_result_has_market_sentiment_headline_key(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.build_weekly_recap("2026-03-09")
        self.assertIn("market_sentiment_headline", result)

    def test_result_has_market_narrative_key(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            result = recap_module.build_weekly_recap("2026-03-09")
        self.assertIn("market_narrative", result)

    def test_both_narrative_functions_called(self):
        with patch("recap.generate_narrative", return_value="spending text") as mock_spend, \
             patch("recap.generate_market_narrative", return_value={"headline": "h", "narrative": "m"}) as mock_market:
            result = recap_module.build_weekly_recap("2026-03-09")
        mock_spend.assert_called_once()
        mock_market.assert_called_once()
        self.assertEqual(result["narrative"], "spending text")
        self.assertEqual(result["market_sentiment_headline"], "h")
        self.assertEqual(result["market_narrative"], "m")


if __name__ == "__main__":
    unittest.main()
