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
            description=(f"Fill price {fill.price} for {fill.symbol} is " f"{pct_diff:.2%} away from market price {market_price:.2f}"  f"at {closest['timestamp']}"),
            severity="warning"
        )

    return None


DEFAULT_MAX_GAP = timedelta(seconds=10)
DEFAULT_MAX_POSITION = 1.0

def check_tick_sequence_gaps(ticks_df: pd.DataFrame, max_gap: timedelta = DEFAULT_MAX_GAP) -> list[Break]:
    """
    Flag stretches where a symbols ticks go quiet for too long.

    A gap usually means the ingest side dropped a connection, a worker fell behind or the exchange itself paused trading.
    Worth surfacing than having a hole in the data.
    These breaks arent tied to any one fill so we use "GAP: <symbol>" as reference.
    """
    breaks = []

    for symbol, group in ticks_df.groupby("symbol"):                    # groupby() returns ("BTC-USD", <DataFrame containing BTC-USD rows>)
        sorted_ticks = group.sort_values("timestamp")
        gaps = sorted_ticks["timestamp"].diff()                         # diff() produces a series object (single column of a pandas DataFrame)

        for idx in gaps[gaps > max_gap].index:                          # returns index of gaps[gaps > max_gap]. > produces a boolean series, .index allows resulting series to remember original df indices
            gap = gaps.loc[idx]
            ts = sorted_ticks.loc[idx, "timestamp"]                     # returns timestamp col from row idx
            breaks.append(
                Break(
                    fill_id=f"GAP: {symbol}",
                    rule="tick_sequence_gap",
                    description=(f"{gap.total_seconds():.1f}s gap in {symbol}ticks, " f"ending at {ts}"),
                    severity="warning",
                )
            )

    return breaks


def compute_net_positions(fills: list[Fill]) -> dict[str, float]:       # [] in type hint dict[str, float] mean generic type parameters (what types does this container hold?)
    """
    Takes all individual Fill objects and adds them together to calculate current running net position for each symbol.
    """
    positions: dict[str, float] = {}
    for fill in fills:
        sign = 1 if fill.side == "buy" else -1
        positions[fill.symbol] = positions.get(fill.symbol, 0.0) + sign * fill.size

    return positions


def check_position_drift(fills: list[Fill], max_position: float = DEFAULT_MAX_POSITION) -> list[Break]:
    """
    Flag any symbol whose net position has drifted past max_position.
    """
    breaks = []
    for symbol, position in compute_net_positions(fills).items():           # items() gives key-value pairs in positions dict and then does tuple unpacking
        if abs(position) > max_position:
            breaks.append(
                Break(
                    fill_id=f"POSITION: {symbol}",
                    rule="position_drift",
                    description=(f"Net position for {symbol} drifted to {position:.4f}, "f"limit is {max_position}"),
                    severity="critical",
            )
        )
            
    return breaks


def reconcile_all(fills: list[Fill], ticks_df: pd.DataFrame, max_gap: timedelta = DEFAULT_MAX_GAP, max_position: float = DEFAULT_MAX_POSITION,) -> list[Break]:
    """
    Run every fill through the rule set, return only breaks found.
    """
    breaks = []

    for fill in fills:
        result = check_fill_against_market(fill, ticks_df)
        if result is not None:
            breaks.append(result)

    breaks.extend(check_tick_sequence_gaps(ticks_df, max_gap=max_gap))
    breaks.extend(check_position_drift(fills, max_position=max_position))

    return breaks