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

DEFAULT_WINDOW = timedelta(seconds=5)       # how far around a fills timestamp we'll look for matching market ticks. timedelta = reps a duration of time

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

    nearby = ticks_df[                              # makes nearby a new df containing rows that satisfy the conditions (which produce a boolean series)
        (ticks_df["symbol"] == fill.symbol)         # checking tick against nearby ticks WITH SAME SYMBOL AS FIRST CONDITION !! (checks all eths) to see what was up in that instance -> was fills price reasonable?          
        & (ticks_df["timestamp"] >= window_start)
        & (ticks_df["timestamp"] <= window_end)
    ]

    if nearby.empty:
        return Break(
            fill_id = fill.fill_id,
            rule="unmatched_fill",
            description=(f"No market ticks for {fill.symbol} within" f"{window.total_seconds():.0f}s of fill at {fill.timestamp}"),         # total_seconds() = converts time into secs 
            severity="critical",
        )
    
    nearby = nearby.copy()         # compare against nearest tick in time to see what was market doing at that instance 
    nearby["time_delta"] = (nearby["timestamp"] - fill.timestamp).abs()     # adding new col
    closest = nearby.loc[nearby["time_delta"].idxmin()]         # .loc = df property. returns closest tick row through its index

    market_price = closest["price"]             # gets price from that row 
    pct_diff = abs(fill.price - market_price) / market_price

    if pct_diff > price_tolerance:
        return Break(
            fill_id=fill.fill_id,
            rule="price_drift",
            description=(f"Fill price {fill.price} for {fill.symbol} is" f"{pct_diff:.2%} away from market price {market_price:.2f}" f"at {closest['timestamp']}"),
            severity="warning"
        )

    return None


def reconcile_all(fills: list[Fill], ticks_df: pd.DataFrame) -> list[Break]:
    """
    Run every fill through the rule set, return only breaks found.
    """
    breaks = []

    for fill in fills:
        result = check_fill_against_market(fill, ticks_df)
        if result is not None:
            breaks.append(result)
    return breaks