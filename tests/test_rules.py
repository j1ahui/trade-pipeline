"""
Tests for reconciliation rules

Tests below do not touch network or a live feed - reconciliation logic is pure. 
Given some ticks and a fill, does it flag correctly? 
Built tiny fake DataFrames by hand so tests are fast and deterministic
"""

from datetime import datetime, timedelta, timezone              # timedelta lets you move around BASE_TIME (create timestamps around BASE_TIME)

import pandas as pd
import pytest

from domain.models import Fill
from reconciliation.rules import check_fill_against_market

def make_ticks_df(rows):
    """
    Helper function: build minimal ticks DataFrame from (symbol, price, timestamp) tuples.
    """
    return pd.DataFrame(
        [{"symbol": s, "price": p, "timestamp": t} for s, p, t in rows]         # s, p, t in rows = tuple unpacking (assigns and unpacks in order)
    )

BASE_TIME = datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc)            # BASE_TIME = dependent variable (constant). datetime class reps a specific point in time


def test_fill_matching_market_price_has_no_breaks():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
        ("BTC-USD", 68010.0, BASE_TIME + timedelta(seconds=1)),
    ])
    fill = Fill("f1", "BTC-USD", 68005.0, 0.1, "buy", BASE_TIME + timedelta(seconds=1))     # positional passing (can pass with keywords too)

    result = check_fill_against_market(fill, ticks)

    assert result is None       # expecting None


def test_fill_with_no_nearby_ticks_is_unmatched():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
    ])
    fill = Fill("f2", "BTC-USD", 68000.0, 0.1, "buy", BASE_TIME + timedelta(hours=1))       # fill happens outside 5s window

    result = check_fill_against_market(fill, ticks)     # result contains Break object (can access its properties like .rule, .severity)

    assert result is not None                       # assert checks by comparing return value of check_fill_against_market (which is stored in result) against condition
    assert result.rule == "unmatched_fill"
    assert result.severity == "critical"


def test_fill_with_price_far_from_market_price_is_price_drift():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
    ])
    fill = Fill("f3", "BTC-USD", 71400.0, 0.1, "buy", BASE_TIME)        # 5% above market 

    result = check_fill_against_market(fill, ticks)

    assert result is not None
    assert result.rule == "price_drift"
    assert result.severity == "warning"


def test_fill_within_tolerance_has_no_break():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME)                             # should pass
    ])
    fill = Fill("f4", "BTC-USD", 68340.0, 0.1, "buy", BASE_TIME)

    result = check_fill_against_market(fill, ticks)

    assert result is None


def test_only_matches_same_symbol():
    ticks = make_ticks_df([
        ("ETH-USD", 68000.0, BASE_TIME)                             # wrong symbol
    ])
    fill = Fill("f5", "BTC-USD", 68000.0, 0.1, "buy", BASE_TIME)

    result = check_fill_against_market(fill, ticks)

    assert result is not None
    assert result.rule == "unmatched_fill"


@pytest.mark.parametrize("tolerance, expect_break", [                   # similar to tuple unpacking. produces a decorator that pytest uses to run same test with diff inputs
    (0.001, True),      # tuple. very strict - 0.5% diff should trip
    (0.05, False),      # very loose - 0.5% should pass
])
def test_price_tolerance_is_configurable(tolerance, expect_break):      # passes in values from above - def test_price_tolerance_is_configurable(0.001, True)
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME)
    ])
    fill = Fill("f6", "BTC-USD", 68340.0, 0.1, "buy", BASE_TIME)        # 0.5% off

    result = check_fill_against_market(fill, ticks, price_tolerance=tolerance)

    assert (result is not None) == expect_break         # compares against pytest values