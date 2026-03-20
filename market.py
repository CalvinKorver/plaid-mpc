"""
Market data and news headline fetching for weekly recap context.

Provides:
- get_market_snapshot(week_start, week_end) -> weekly % change for major indices
- get_finance_headlines(week_start, week_end) -> finance news scoped to the week
- get_top_news_headlines(week_start, week_end) -> general news scoped to the week
- format_market_context(snapshot, finance_headlines, top_headlines) -> prompt-ready str

All functions degrade gracefully — if a library is missing or a network call fails,
they return empty dicts/lists rather than raising. Callers do not need try/except.
"""

import logging
from datetime import date, timedelta
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)

MARKET_INDICES = {"S&P 500": "^GSPC", "NASDAQ": "^IXIC", "Dow Jones": "^DJI"}

# Google News RSS search supports date-scoped queries via after:/before: operators.
GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search?hl=en-US&gl=US&ceid=US:en&q={query}"

_HEADLINE_MAX_TITLE_LEN = 80


def _news_feed_url(query: str, week_start: str, week_end: str) -> str:
    """Build a date-scoped Google News RSS search URL."""
    scoped_query = f"{query}+after:{week_start}+before:{week_end}"
    return GOOGLE_NEWS_RSS_BASE.format(query=scoped_query)


def get_market_snapshot(week_start: str, week_end: str) -> dict:
    """
    Fetch weekly % change for major US indices using yfinance.

    Args:
        week_start: YYYY-MM-DD — first day of the target week.
        week_end:   YYYY-MM-DD — last day of the target week (inclusive).

    Returns:
        {
            "S&P 500":   {"start": float, "end": float, "change_pct": float},
            "NASDAQ":    {"start": float, "end": float, "change_pct": float},
            "Dow Jones": {"start": float, "end": float, "change_pct": float},
        }
        Returns {} if yfinance is unavailable or all downloads fail.
        Individual indices are omitted if their data is unavailable.
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[market] yfinance not installed — skipping market snapshot")
        return {}

    try:
        download_end = (date.fromisoformat(week_end) + timedelta(days=1)).isoformat()
        tickers = list(MARKET_INDICES.values())
        df = yf.download(
            tickers,
            start=week_start,
            end=download_end,
            progress=False,
            auto_adjust=True,
            group_by="ticker",
        )
    except Exception as e:
        logger.warning("[market] yfinance download failed: %s", e)
        return {}

    result = {}
    for name, ticker in MARKET_INDICES.items():
        try:
            ticker_df = df.get(ticker)
            if ticker_df is None:
                continue
            close_series = ticker_df.get("Close")
            if close_series is None:
                continue
            clean = close_series.dropna()
            if clean.empty:
                continue
            start_price = float(clean.iloc[0])
            end_price = float(clean.iloc[-1])
            if start_price == 0:
                continue
            change_pct = round((end_price - start_price) / start_price * 100, 2)
            result[name] = {
                "start": round(start_price, 2),
                "end": round(end_price, 2),
                "change_pct": change_pct,
            }
        except Exception as e:
            logger.warning("[market] failed to extract data for %s (%s): %s", name, ticker, e)
            continue

    return result


def get_headlines(feed_url: str, limit: int = 5) -> list:
    """
    Fetch headlines from an RSS feed URL via feedparser.

    Args:
        feed_url: Full RSS feed URL (may include date-scoped query params).
        limit:    Maximum number of headlines to return.

    Returns:
        [{"title": str, "published": str}, ...]
        Returns [] if feedparser is unavailable or the fetch fails.
    """
    try:
        import feedparser
    except ImportError:
        logger.warning("[market] feedparser not installed — skipping headlines")
        return []

    try:
        feed = feedparser.parse(feed_url)
        entries = feed.get("entries", [])[:limit]
        headlines = []
        for entry in entries:
            title = entry.get("title", "").strip()
            if len(title) > _HEADLINE_MAX_TITLE_LEN:
                title = title[:_HEADLINE_MAX_TITLE_LEN] + "..."
            raw_published = entry.get("published", "")
            try:
                published = parsedate_to_datetime(raw_published).strftime("%Y-%m-%d")
            except Exception:
                published = raw_published
            headlines.append({"title": title, "published": published})
        return headlines
    except Exception as e:
        logger.warning("[market] headline fetch failed for %s: %s", feed_url, e)
        return []


def get_finance_headlines(week_start: str, week_end: str, limit: int = 5) -> list:
    """Finance/markets headlines scoped to the recap week via Google News RSS."""
    url = _news_feed_url("finance+economy+markets", week_start, week_end)
    return get_headlines(url, limit)


def get_top_news_headlines(week_start: str, week_end: str, limit: int = 5) -> list:
    """General top headlines scoped to the recap week via Google News RSS."""
    url = _news_feed_url("news", week_start, week_end)
    return get_headlines(url, limit)


def format_market_context(
    snapshot: dict,
    finance_headlines: list,
    top_headlines: list,
) -> str:
    """
    Format market snapshot and headlines into a prompt-ready string.

    Pure formatting function — no I/O. Returns "" if all inputs are empty.
    Omits a section entirely if its data is empty.
    """
    sections = []

    if snapshot:
        lines = ["Market performance for the week:"]
        for name, data in snapshot.items():
            pct = data.get("change_pct")
            if pct is not None:
                lines.append(f"  - {name}: {pct:+.1f}%")
        if len(lines) > 1:
            sections.append("\n".join(lines))

    if finance_headlines:
        lines = ["Top finance headlines:"]
        for h in finance_headlines:
            lines.append(f'  - "{h["title"]}" ({h["published"]})')
        sections.append("\n".join(lines))

    if top_headlines:
        lines = ["Top news headlines this week:"]
        for h in top_headlines:
            lines.append(f'  - "{h["title"]}" ({h["published"]})')
        sections.append("\n".join(lines))

    return "\n\n".join(sections)
