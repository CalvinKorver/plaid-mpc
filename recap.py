"""
Weekly Recap: data aggregation and narrative generation.

Produces a structured summary of the user's spending and net worth for a
given week, plus a Claude-generated narrative description.

Entry point: build_weekly_recap(week_start_date) -> dict
"""

import logging
import os
from datetime import date, timedelta

import anthropic

import db

logger = logging.getLogger(__name__)

NARRATIVE_MODEL = "claude-haiku-4-5-20251001"


def _week_bounds(week_start: str) -> tuple[str, str]:
    """Return (week_start, week_end) for a 7-day window starting on week_start."""
    start = date.fromisoformat(week_start)
    end = start + timedelta(days=6)
    return week_start, end.isoformat()


def get_week_spending_data(week_start: str) -> dict:
    """Aggregate spending for the 7-day window starting on week_start."""
    week_end = (date.fromisoformat(week_start) + timedelta(days=6)).isoformat()
    return db.get_week_spending(week_start, week_end)


def get_prior_week_spending_data(week_start: str) -> dict:
    """Aggregate spending for the 7 days immediately before week_start."""
    start = date.fromisoformat(week_start)
    prior_start = (start - timedelta(days=7)).isoformat()
    prior_end = (start - timedelta(days=1)).isoformat()
    return db.get_week_spending(prior_start, prior_end)


def compute_spending_comparison(current: dict, prior: dict) -> dict:
    """
    Compare current week spending to prior week.

    Returns:
        {
            "current_total": float,
            "prior_total": float,
            "change_amount": float,       # positive = spent more
            "change_pct": float | None,   # None if prior_total == 0
            "direction": "up" | "down" | "same",
            "description": str,           # e.g. "down 12%" or "up $45.00 (22%)"
        }
    """
    current_total = current["total"]
    prior_total = prior["total"]
    change_amount = round(current_total - prior_total, 2)

    if prior_total == 0:
        change_pct = None
        direction = "same" if change_amount == 0 else ("up" if change_amount > 0 else "down")
        description = f"${current_total:.2f} this week (no prior week data)"
    else:
        change_pct = round((change_amount / prior_total) * 100, 1)
        if change_amount > 0:
            direction = "up"
            description = f"up {abs(change_pct):.0f}%"
        elif change_amount < 0:
            direction = "down"
            description = f"down {abs(change_pct):.0f}%"
        else:
            direction = "same"
            description = "unchanged"

    return {
        "current_total": current_total,
        "prior_total": prior_total,
        "change_amount": change_amount,
        "change_pct": change_pct,
        "direction": direction,
        "description": description,
    }


def build_narrative_prompt(recap_data: dict) -> str:
    """Build the Claude prompt for narrative generation."""
    spending = recap_data["spending"]
    net_worth = recap_data["net_worth"]

    top_cats = spending["by_category"][:3]
    cat_lines = []
    for cat in top_cats:
        merchants = ", ".join(
            t["name"] for t in cat["transactions"][:3] if t["name"]
        )
        line = f"  - {cat['category']}: ${cat['total']:.2f}"
        if merchants:
            line += f" ({merchants})"
        cat_lines.append(line)
    top_cats_text = "\n".join(cat_lines) if cat_lines else "  - No spending data"

    if net_worth["current"] is not None and net_worth["prior_week"] is not None:
        nw_change = round(net_worth["current"] - net_worth["prior_week"], 2)
        nw_text = f"${net_worth['current']:,.2f} (change: ${nw_change:+,.2f})"
    elif net_worth["current"] is not None:
        nw_text = f"${net_worth['current']:,.2f} (prior week data unavailable)"
    else:
        nw_text = "not available"

    return (
        f"You are a personal finance assistant. Summarize this week's financial activity "
        f"in 2-3 friendly, conversational sentences. Be specific about notable numbers. "
        f"Mention if spending went up or down compared to last week. If net worth data is "
        f"available, briefly mention the change. Respond with ONLY the summary sentences — "
        f"no headers, no bullet points, no JSON.\n\n"
        f"Week: {recap_data['week_start']} to {recap_data['week_end']}\n"
        f"Total spending: ${spending['total']:.2f} ({spending['description']} vs last week)\n"
        f"Top spending categories:\n{top_cats_text}\n"
        f"Income/deposits: ${spending['income_total']:.2f}\n"
        f"Net worth: {nw_text}"
    )


def generate_narrative(recap_data: dict) -> str:
    """
    Call Claude Haiku to produce a 2-3 sentence narrative summary.
    Returns empty string if ANTHROPIC_API_KEY is not set or any API error occurs.
    Mirrors the graceful-degradation pattern from classifier.py.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        logger.warning("[recap] ANTHROPIC_API_KEY not set — skipping narrative generation")
        return ""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        prompt = build_narrative_prompt(recap_data)
        message = client.messages.create(
            model=NARRATIVE_MODEL,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except Exception as e:
        logger.warning("[recap] narrative generation failed: %s", e)
        return ""


def build_weekly_recap(week_start_date: str) -> dict:
    """
    Generate a complete weekly recap.

    Args:
        week_start_date: YYYY-MM-DD — the first day of the target week.

    Returns:
        {
            "week_start": str,
            "week_end": str,
            "spending": {
                "total": float,
                "prior_week_total": float,
                "change_amount": float,
                "change_pct": float | None,
                "direction": str,
                "description": str,
                "by_day": {date: float},
                "by_category": [{category, total, transactions}],
                "income_total": float,
            },
            "net_worth": {
                "current": float | None,
                "prior_week": float | None,
                "change_amount": float | None,
                "change_pct": float | None,
                "by_day": {date: float},
                "breakdown": {depository, investment, credit, loan},
            },
            "narrative": str,
        }
    """
    week_start, week_end = _week_bounds(week_start_date)

    current_spending = get_week_spending_data(week_start)
    prior_spending = get_prior_week_spending_data(week_start)
    comparison = compute_spending_comparison(current_spending, prior_spending)

    nw_history = db.get_net_worth_history(week_start, week_end)
    current_nw = nw_history["current_net_worth"]
    prior_nw = nw_history["week_start_net_worth"]

    if current_nw is not None and prior_nw is not None:
        nw_change_amount = round(current_nw - prior_nw, 2)
        nw_change_pct = round((nw_change_amount / prior_nw) * 100, 2) if prior_nw != 0 else None
    else:
        nw_change_amount = None
        nw_change_pct = None

    recap = {
        "week_start": week_start,
        "week_end": week_end,
        "spending": {
            "total": comparison["current_total"],
            "prior_week_total": comparison["prior_total"],
            "change_amount": comparison["change_amount"],
            "change_pct": comparison["change_pct"],
            "direction": comparison["direction"],
            "description": comparison["description"],
            "by_day": current_spending["by_day"],
            "by_category": current_spending["by_category"],
            "income_total": current_spending["income_total"],
        },
        "net_worth": {
            "current": current_nw,
            "prior_week": prior_nw,
            "change_amount": nw_change_amount,
            "change_pct": nw_change_pct,
            "by_day": nw_history["by_day"],
            "breakdown": nw_history["breakdown"],
        },
        "narrative": "",
    }

    recap["narrative"] = generate_narrative(recap)
    return recap
