"""
Claude API-based transaction categorizer.

Processes batches of new transactions that weren't matched by manual rules.
Two modes per transaction:
  - assigned_category is set: Claude validates and may override
  - assigned_category is None: Claude assigns the best category from scratch

All decisions are written to categorization_log for auditability.
"""

import json
import logging
import os

import anthropic

import db

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 50


def _build_prompt(candidates: list[dict], categories: list[str]) -> str:
    category_list = ", ".join(categories)
    tx_list = json.dumps(
        [
            {
                "id": c["transaction_id"],
                "name": c["name"],
                "merchant_name": c["merchant_name"],
                "amount": c["amount"],
                "assigned_category": c["current_category"],
            }
            for c in candidates
        ],
        indent=2,
    )
    return f"""You are a financial transaction categorizer. Available categories:
{category_list}

For each transaction below:
- If assigned_category is set, it was auto-labeled by a heuristic system. Prefer that label but \
override it if the merchant name and amount clearly indicate a different category.
- If assigned_category is null, assign the most appropriate category from the list above.

In both cases, provide a brief reason (1 sentence).

Transactions:
{tx_list}

Respond ONLY with a valid JSON object mapping each transaction id to its result:
{{
  "<id>": {{"category": "<category from the list>", "reason": "<1-sentence reason>"}},
  ...
}}"""


def _categorize_batch(
    client: anthropic.Anthropic,
    candidates: list[dict],
    categories: list[str],
) -> dict[str, dict]:
    """
    Send one batch to Claude. Returns {transaction_id: {category, reason}}.
    Raises on API or parse error — caller handles.
    """
    prompt = _build_prompt(candidates, categories)
    message = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


def apply_claude_categorization(candidates: list[dict]) -> int:
    """
    Categorize a list of candidate transactions using Claude API.

    Each candidate must have:
        transaction_id, name, merchant_name, amount, current_category (str | None)

    Skips gracefully if ANTHROPIC_API_KEY is not set or if any API error occurs.
    Writes results to DB and categorization_log.
    Returns the number of transactions categorized.
    """
    if not candidates:
        return 0

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        logger.warning("[classifier] ANTHROPIC_API_KEY not set — skipping Claude categorization")
        return 0

    try:
        client = anthropic.Anthropic(api_key=api_key)
        all_categories = [c["name"] for c in db.get_all_categories()]
        total = 0

        for i in range(0, len(candidates), BATCH_SIZE):
            batch = candidates[i : i + BATCH_SIZE]
            try:
                results = _categorize_batch(client, batch, all_categories)
            except Exception as e:
                logger.warning("[classifier] batch %d failed: %s", i // BATCH_SIZE, e)
                continue

            # Index batch by id for O(1) lookup
            batch_map = {c["transaction_id"]: c for c in batch}

            for tx_id, result in results.items():
                if tx_id not in batch_map:
                    continue
                candidate = batch_map[tx_id]
                category = result.get("category", "").strip()
                reason = result.get("reason", "").strip()

                if not category or category not in all_categories:
                    logger.warning(
                        "[classifier] tx %s got unknown category %r — skipping", tx_id, category
                    )
                    continue

                if db.set_custom_category(tx_id, category):
                    db.insert_categorization_log(
                        transaction_id=tx_id,
                        source="claude",
                        old_category=candidate["current_category"],
                        new_category=category,
                        reason=reason,
                        model=MODEL,
                    )
                    total += 1

        return total

    except Exception as e:
        logger.warning("[classifier] Claude categorization failed: %s", e)
        return 0


def apply_claude_categorization_from_db() -> int:
    """
    Convenience wrapper: load all uncategorized transactions from DB and categorize.
    Used by the MCP server's sync_and_store tool (no candidate tracking needed there).
    """
    uncategorized = db.get_uncategorized(limit=200)
    if not uncategorized:
        return 0
    candidates = [
        {
            "transaction_id": t["transaction_id"],
            "name": t["name"] or "",
            "merchant_name": t["merchant_name"] or "",
            "amount": t["amount"],
            "current_category": t.get("custom_category"),
        }
        for t in uncategorized
    ]
    return apply_claude_categorization(candidates)
