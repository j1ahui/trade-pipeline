"""
Reconciliation rules

Tier 1 ships exactly one rule: "does this fills price roughly match what the market was actually
doing at that moment?" This rule covers 2 most important break types:

    - no market data near the fill at all -> "unmatched fill"
    - market data exists, but the price is way off -> "price drift"

Tier 2 will add more rules (tick sequence gaps, position-level drift).
"""

from datetime import timedelta

import pandas as pd

from domain.models import Fill, Break

DEFAULT_WINDOW = timedelta(seconds=5)       # how far around a fills timestamp we'll look for matching market ticks

DEFAULT_PRICE_TOLERANCE = 0.01              # how far a fills price can drift from the nearest market price before its flagged as a break

def check_fill_against_market(
        fill: Fill,
        ticks_df: pd.DataFrame,
        window: timedelta = DEFAULT_WINDOW,
        price_tolerance: float = DEFAULT_PRICE_TOLERANCE,
) -> Break | None:
    """
    Check a single fill against market ticks for the same symbol.

    Returns a Break if somethings wrong, or None if the fill looks fine.
    This is the function pytest suite targets directly 
    """
    window_start = fill.timestamp - window
    window_end = fill.timestamp + window

    