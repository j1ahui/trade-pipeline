"""
Tests for compute_net_position and check_position_drift.
"""

from datetime import datetime, timezone

import pytest

from domain.models import Fill
from reconciliation.rules import compute_net_positions, check_position_drift

BASE_TIME = datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc)


def make_fill(symbol, price, size, side, fill_id="f1"):
    return Fill(fill_id, symbol, price, size, side, BASE_TIME)


def test_buys_and_sells_net_out_correctly():
    fills = [
        make_fill("BTC-USD", 68000.0, 0.5, "buy", "f1"),
        make_fill("BTC-USD", 68000, 0.3, "sell", "f2"),
    ]

    positions = compute_net_positions(fills)

    assert positions["BTC-USD"] == pytest.approx(0.2)                   # approx() = used to compare floating-point numbers approximately (floating-point calculations can produce tiny inaccuracies)


def test_position_within_limit_has_no_break():
    fills = [
        make_fill("BTC-USD", 68000, 0.5, "buy")
    ]

    breaks = check_position_drift(fills, max_position=1.0)

    assert breaks == []


def test_position_beyond_limit_is_flagged():
    fills = [
        make_fill("BTC-USD", 68000, 0.8, "buy", "f1"),
        make_fill("BTC-USD", 68000, 0.5, "buy", "f2"),
    ]

    breaks = check_position_drift(fills, max_position=1.0)

    assert len(breaks) == 1
    assert breaks[0].rule == "position_drift"
    assert breaks[0].severity == "critical"


def test_symbols_tracked_independently():
    fills = [
        make_fill("BTC-USD", 68000, 2.0, "buy", "f1"),
        make_fill("ETH-USD", 3000, 0.1, "buy", "f2"),
    ]

    breaks = check_position_drift(fills, max_position=1.0)

    assert len(breaks) == 1
    assert "BTC-USD" in breaks[0].fill_id