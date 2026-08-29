"""
Test for check_tick_sequence_gaps.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd

from reconciliation.rules import check_tick_sequence_gaps

BASE_TIME = datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc)

def make_ticks_df(rows):
    return pd.DataFrame(
        [{"symbol": s, "price": p, "timestamp": t} for s, p, t in rows]
    )


def test_no_gap_when_ticks_are_close_together():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
        ("BTC-USD", 68001.0, BASE_TIME + timedelta(seconds=1)),
        ("BTC-USD", 68002.0, BASE_TIME + timedelta(seconds=2)),
    ])

    breaks = check_tick_sequence_gaps(ticks, max_gap=timedelta(seconds=10))

    assert breaks == []


def test_gap_larger_than_threshold_is_flagged():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
        ("BTC-USD", 68050.0, BASE_TIME + timedelta(seconds=30)),        # 30s later
    ])

    breaks = check_tick_sequence_gaps(ticks, max_gap=timedelta(seconds=10))

    assert len(breaks) == 1
    assert breaks[0].rule == "tick_sequence_gap"
    assert "BTC-USD" in breaks[0].fill_id


def test_gaps_are_checked_per_symbol_independently():                   # BTC has a gap, ETH doesnt (only BTC should be flagged)
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
        ("BTC-USD", 68050.0, BASE_TIME + timedelta(seconds=30)),
        ("ETH-USD", 3000.0, BASE_TIME),
        ("ETH-USD", 3001.0, BASE_TIME + timedelta(seconds=1)),
    ])

    breaks = check_tick_sequence_gaps(ticks, max_gap=timedelta(seconds=10))

    assert len(breaks) == 1
    assert "BTC-USD" in breaks[0].fill_id


def test_multiple_gaps_in_same_symbol_all_flagged():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
        ("BTC-USD", 68010.0, BASE_TIME + timedelta(seconds=20)),
        ("BTC-USD", 68020.0, BASE_TIME + timedelta(seconds=45)),
    ])

    breaks = check_tick_sequence_gaps(ticks, max_gap=timedelta(seconds=10))

    assert len(breaks) == 2
