"""
Category taxonomy and Plaid → custom mapping.

DEFAULT_PLAID_MAP: maps Plaid's personal_finance_category.primary values to our labels.
DEFAULT_CATEGORIES: the canonical taxonomy as (name, parent) tuples.
resolve_category(): pure function — no DB access.
"""

DEFAULT_PLAID_MAP: dict[str, str] = {
    "FOOD_AND_DRINK":            "Food & Dining",
    "TRANSPORTATION":            "Transport",
    "GENERAL_MERCHANDISE":       "Shopping",
    "ENTERTAINMENT":             "Entertainment",
    "GENERAL_SERVICES":          "Other",
    "GOVERNMENT_AND_NON_PROFIT": "Other",
    "HOME_IMPROVEMENT":          "Other",
    "INCOME":                    "Income",
    "LOAN_PAYMENTS":             "Other",
    "MEDICAL":                   "Health",
    "PERSONAL_CARE":             "Other",
    "RENT_AND_UTILITIES":        "Utilities",
    "TRANSFER_IN":               "Transfer",
    "TRANSFER_OUT":              "Transfer",
    "TRAVEL":                    "Transport",
    "BANK_FEES":                 "Other",
    "OTHER":                     "Other",
}

# (name, parent) — parent="" means top-level
DEFAULT_CATEGORIES: list[tuple[str, str]] = [
    ("Food & Dining", ""),
    ("Groceries",     "Food & Dining"),
    ("Transport",     ""),
    ("Shopping",      ""),
    ("Entertainment", ""),
    ("Health",        ""),
    ("Utilities",     ""),
    ("Rent",          ""),
    ("Income",        ""),
    ("Transfer",      ""),
    ("Other",         ""),
]


def resolve_category(plaid_cat: str, overrides: dict[str, str]) -> str:
    """
    Map a Plaid primary category string to a custom label.

    Resolution order:
      1. overrides (from plaid_category_map DB table)
      2. DEFAULT_PLAID_MAP
      3. "Other" if no match
    """
    return overrides.get(plaid_cat) or DEFAULT_PLAID_MAP.get(plaid_cat, "Other")
