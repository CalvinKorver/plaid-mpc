"""
Unit tests for market.py.

No real network calls are made — yfinance and feedparser are fully mocked.
"""

import sys
import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, call, patch

import market as market_module


def _make_multindex_df(ticker_closes: dict):
    """
    Build a mock DataFrame mimicking yfinance multi-ticker output with group_by='ticker'.

    ticker_closes: {ticker_symbol: [close_day1, close_day2]}
    Returns a MagicMock whose .get(ticker).get("Close").dropna() returns the values.
    """
    df = MagicMock()

    def df_get(ticker, default=None):
        if ticker not in ticker_closes:
            return default
        closes = ticker_closes[ticker]
        ticker_df = MagicMock()

        def ticker_df_get(field, default2=None):
            if field != "Close":
                return default2
            series = MagicMock()
            series.dropna.return_value = series
            series.empty = len(closes) == 0
            if closes:
                series.iloc.__getitem__ = lambda self, idx: closes[idx]
            return series

        ticker_df.get.side_effect = ticker_df_get
        return ticker_df

    df.get.side_effect = df_get
    return df


class TestGetMarketSnapshot(unittest.TestCase):
    def test_returns_all_three_indices_on_success(self):
        mock_df = _make_multindex_df({
            "^GSPC": [5000.0, 5100.0],
            "^IXIC": [15000.0, 14700.0],
            "^DJI": [40000.0, 40400.0],
        })
        with patch("yfinance.download", return_value=mock_df):
            result = market_module.get_market_snapshot("2026-03-09", "2026-03-15")
        self.assertIn("S&P 500", result)
        self.assertIn("NASDAQ", result)
        self.assertIn("Dow Jones", result)

    def test_change_pct_calculation_correct(self):
        mock_df = _make_multindex_df({"^GSPC": [5000.0, 5100.0]})
        # Patch the other tickers to return None (not present)
        original_side_effect = mock_df.get.side_effect

        def patched_get(ticker, default=None):
            if ticker in ("^IXIC", "^DJI"):
                return default
            return original_side_effect(ticker, default)

        mock_df.get.side_effect = patched_get
        with patch("yfinance.download", return_value=mock_df):
            result = market_module.get_market_snapshot("2026-03-09", "2026-03-15")
        self.assertAlmostEqual(result["S&P 500"]["change_pct"], 2.0)

    def test_returns_empty_dict_when_yfinance_not_installed(self):
        with patch.dict(sys.modules, {"yfinance": None}):
            # Re-import to hit the ImportError branch
            import importlib
            mod = importlib.import_module("market")
            result = mod.get_market_snapshot("2026-03-09", "2026-03-15")
        self.assertEqual(result, {})

    def test_returns_empty_dict_on_download_exception(self):
        with patch("yfinance.download", side_effect=Exception("network error")):
            result = market_module.get_market_snapshot("2026-03-09", "2026-03-15")
        self.assertEqual(result, {})

    def test_skips_index_with_empty_close_series(self):
        mock_df = _make_multindex_df({"^GSPC": []})  # empty close list

        def patched_get(ticker, default=None):
            if ticker in ("^IXIC", "^DJI"):
                return default
            ticker_df = MagicMock()
            series = MagicMock()
            series.dropna.return_value = series
            series.empty = True
            ticker_df.get.return_value = series
            return ticker_df

        mock_df.get.side_effect = patched_get
        with patch("yfinance.download", return_value=mock_df):
            result = market_module.get_market_snapshot("2026-03-09", "2026-03-15")
        self.assertNotIn("S&P 500", result)

    def test_download_end_date_is_one_day_after_week_end(self):
        mock_df = MagicMock()
        mock_df.get.return_value = None
        with patch("yfinance.download", return_value=mock_df) as mock_dl:
            market_module.get_market_snapshot("2026-03-09", "2026-03-15")
        _, kwargs = mock_dl.call_args
        expected_end = (date.fromisoformat("2026-03-15") + timedelta(days=1)).isoformat()
        self.assertEqual(kwargs.get("end"), expected_end)


class TestGetHeadlines(unittest.TestCase):
    def _make_feed(self, titles):
        """Build a mock feedparser result with the given titles."""
        entries = []
        for title in titles:
            entry = MagicMock()
            entry.get.side_effect = lambda key, default="": {
                "title": title,
                "published": "Mon, 09 Mar 2026 12:00:00 +0000",
            }.get(key, default)
            entries.append(entry)
        feed = {"entries": entries}
        return feed

    def test_returns_headlines_up_to_limit(self):
        feed = self._make_feed([f"Headline {i}" for i in range(10)])
        with patch("feedparser.parse", return_value=feed):
            result = market_module.get_headlines("http://example.com/rss", limit=3)
        self.assertEqual(len(result), 3)

    def test_returns_empty_list_when_feedparser_not_installed(self):
        with patch.dict(sys.modules, {"feedparser": None}):
            import importlib
            mod = importlib.import_module("market")
            result = mod.get_headlines("http://example.com/rss")
        self.assertEqual(result, [])

    def test_returns_empty_list_when_feed_empty(self):
        with patch("feedparser.parse", return_value={"entries": []}):
            result = market_module.get_headlines("http://example.com/rss")
        self.assertEqual(result, [])

    def test_returns_empty_list_on_parse_exception(self):
        with patch("feedparser.parse", side_effect=Exception("connection error")):
            result = market_module.get_headlines("http://example.com/rss")
        self.assertEqual(result, [])

    def test_headline_dict_has_title_and_published_keys(self):
        feed = self._make_feed(["Test Headline"])
        with patch("feedparser.parse", return_value=feed):
            result = market_module.get_headlines("http://example.com/rss", limit=1)
        self.assertEqual(len(result), 1)
        self.assertIn("title", result[0])
        self.assertIn("published", result[0])
        self.assertIsInstance(result[0]["title"], str)
        self.assertIsInstance(result[0]["published"], str)

    def test_get_finance_headlines_url_contains_week_dates(self):
        with patch("feedparser.parse", return_value={"entries": []}) as mock_parse:
            market_module.get_finance_headlines("2026-03-09", "2026-03-15")
        url = mock_parse.call_args[0][0]
        self.assertIn("after:2026-03-09", url)
        self.assertIn("before:2026-03-15", url)

    def test_get_top_news_headlines_url_contains_week_dates(self):
        with patch("feedparser.parse", return_value={"entries": []}) as mock_parse:
            market_module.get_top_news_headlines("2026-03-09", "2026-03-15")
        url = mock_parse.call_args[0][0]
        self.assertIn("after:2026-03-09", url)
        self.assertIn("before:2026-03-15", url)

    def test_truncates_long_headline_title(self):
        long_title = "A" * 200
        feed = self._make_feed([long_title])
        with patch("feedparser.parse", return_value=feed):
            result = market_module.get_headlines("http://example.com/rss", limit=1)
        self.assertLessEqual(len(result[0]["title"]), 83)  # 80 chars + "..."
        self.assertTrue(result[0]["title"].endswith("..."))


class TestFormatMarketContext(unittest.TestCase):
    def test_returns_empty_string_when_all_empty(self):
        result = market_module.format_market_context({}, [], [])
        self.assertEqual(result, "")

    def test_positive_change_formatted_with_plus_sign(self):
        snapshot = {"S&P 500": {"start": 5000.0, "end": 5075.0, "change_pct": 1.5}}
        result = market_module.format_market_context(snapshot, [], [])
        self.assertIn("+1.5%", result)

    def test_negative_change_formatted_correctly(self):
        snapshot = {"NASDAQ": {"start": 15000.0, "end": 14880.0, "change_pct": -0.8}}
        result = market_module.format_market_context(snapshot, [], [])
        self.assertIn("-0.8%", result)

    def test_omits_finance_headlines_section_when_empty(self):
        snapshot = {"S&P 500": {"start": 5000.0, "end": 5050.0, "change_pct": 1.0}}
        result = market_module.format_market_context(snapshot, [], [])
        self.assertNotIn("finance headlines", result.lower())

    def test_omits_top_headlines_section_when_empty(self):
        snapshot = {"S&P 500": {"start": 5000.0, "end": 5050.0, "change_pct": 1.0}}
        result = market_module.format_market_context(snapshot, [], [])
        self.assertNotIn("top news headlines", result.lower())

    def test_omits_snapshot_section_when_empty(self):
        headlines = [{"title": "Fed holds rates", "published": "2026-03-10"}]
        result = market_module.format_market_context({}, headlines, [])
        self.assertNotIn("Market performance", result)
        self.assertIn("Fed holds rates", result)

    def test_both_headline_sections_present_when_both_provided(self):
        finance_hl = [{"title": "Markets rally on earnings", "published": "2026-03-10"}]
        top_hl = [{"title": "Election results in", "published": "2026-03-11"}]
        result = market_module.format_market_context({}, finance_hl, top_hl)
        self.assertIn("Top finance headlines", result)
        self.assertIn("Top news headlines", result)
        self.assertIn("Markets rally on earnings", result)
        self.assertIn("Election results in", result)

    def test_snapshot_index_name_and_pct_in_output(self):
        snapshot = {"Dow Jones": {"start": 40000.0, "end": 40400.0, "change_pct": 1.0}}
        result = market_module.format_market_context(snapshot, [], [])
        self.assertIn("Dow Jones", result)
        self.assertIn("+1.0%", result)


if __name__ == "__main__":
    unittest.main()
