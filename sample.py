"""
Domain objects for the post-trade pipeline.

Tier 1 goal: stop passing raw dicts around. Every tick and fill that
moves through the system is a typed object, not a dict with string keys
that can silently typo or drift.
"""
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class Tick:
    """A single normalized market data observation."""
    symbol: str          # e.g. "BTC-USD"
    price: float
    size: float
    timestamp: datetime  # UTC, when the trade happened on the exchange
    received_at: datetime  # UTC, when *we* saw it (useful later for lag)

    @classmethod
    def from_coinbase_ticker(cls, msg: dict) -> "Tick":
        """
        Build a Tick from a raw Coinbase 'ticker' channel message.

        Coinbase's ticker messages look like:
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "68123.45",
            "last_size": "0.001",
            "time": "2026-08-12T10:00:00.123456Z",
            ...
        }

        Keeping this parsing logic in one place means if Coinbase changes
        their message shape, there's exactly one function to fix.
        """
        return cls(
            symbol=msg["product_id"],
            price=float(msg["price"]),
            size=float(msg.get("last_size", 0.0)),
            timestamp=datetime.fromisoformat(msg["time"].replace("Z", "+00:00")),
            received_at=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class Fill:
    """A single simulated trade execution from our fictional trade book."""
    fill_id: str
    symbol: str
    price: float
    size: float
    side: str            # "buy" or "sell"
    timestamp: datetime  # UTC


@dataclass(frozen=True)
class Break:
    """A reconciliation problem: something that didn't match up."""
    fill_id: str
    rule: str             # which reconciliation rule flagged this
    description: str
    severity: str = "warning"  # "warning" or "critical"

"""
SQLite-backed tick storage.

Tier 1 keeps this deliberately simple: one table, one connection, no
concurrency to worry about yet (that comes in Tier 2 when multiple
worker processes write at once). The point right now is just: ticks
go in, and you can query them back out with SQL or pandas.
"""
import sqlite3
from pathlib import Path

from domain.models import Tick

DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "ticks.db"


class TickStore:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._create_table()

    def _create_table(self):
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticks (
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                size REAL NOT NULL,
                timestamp TEXT NOT NULL,
                received_at TEXT NOT NULL
            )
            """
        )
        # An index on (symbol, timestamp) is what makes "find the market
        # price for BTC-USD around 10:00:03" fast instead of a full scan -
        # exactly the query reconciliation will run constantly.
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_time ON ticks(symbol, timestamp)"
        )
        self._conn.commit()

    def write_tick(self, tick: Tick):
        self._conn.execute(
            "INSERT INTO ticks (symbol, price, size, timestamp, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                tick.symbol,
                tick.price,
                tick.size,
                tick.timestamp.isoformat(),
                tick.received_at.isoformat(),
            ),
        )
        self._conn.commit()

    def write_ticks(self, ticks: list[Tick]):
        """Batch insert - useful once you're writing faster than one at a time."""
        self._conn.executemany(
            "INSERT INTO ticks (symbol, price, size, timestamp, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (t.symbol, t.price, t.size, t.timestamp.isoformat(), t.received_at.isoformat())
                for t in ticks
            ],
        )
        self._conn.commit()

    def read_all_as_dataframe(self):
        """Pull everything back out as a pandas DataFrame for reconciliation."""
        import pandas as pd
        return pd.read_sql_query(
            "SELECT * FROM ticks ORDER BY timestamp", self._conn,
            parse_dates=["timestamp", "received_at"],
        )

    def close(self):
        self._conn.close()

"""
A fake trade book.

In real life this would be "the fills our trading desk actually made,"
pulled from an execution system. We don't have a trading desk, so we
simulate one: pick a few random moments and pretend we bought/sold at
roughly the market price (with some fills intentionally off, so
reconciliation has something to catch).
"""
import random
import uuid
from datetime import timedelta

import pandas as pd

from domain.models import Fill


def simulate_fills(
    ticks_df: pd.DataFrame,
    n_fills: int = 20,
    break_rate: float = 0.25,
    seed: int | None = None,
) -> list[Fill]:
    """
    Generate fake fills by sampling real ticks and perturbing some of them.

    break_rate: fraction of fills that get deliberately corrupted (price
    moved away from market, or timestamp shifted) so the reconciliation
    suite has real breaks to find. Without this, every test run would
    have nothing to report - not a useful demo.
    """
    rng = random.Random(seed)
    if ticks_df.empty:
        return []

    sample = ticks_df.sample(n=min(n_fills, len(ticks_df)), random_state=seed)

    fills = []
    for _, row in sample.iterrows():
        is_break = rng.random() < break_rate
        price = row["price"]
        timestamp = row["timestamp"]

        if is_break:
            # Nudge the price 2-5% away from the real market price at that
            # moment - big enough that a "price drift" rule should catch it.
            price = price * (1 + rng.choice([-1, 1]) * rng.uniform(0.02, 0.05))

        fills.append(
            Fill(
                fill_id=str(uuid.uuid4())[:8],
                symbol=row["symbol"],
                price=round(price, 2),
                size=round(rng.uniform(0.001, 0.5), 4),
                side=rng.choice(["buy", "sell"]),
                timestamp=timestamp,
            )
        )

    # Also throw in a couple of fills with NO matching market data at all -
    # this is the "unmatched fill" break case.
    for _ in range(max(1, n_fills // 10)):
        fills.append(
            Fill(
                fill_id=str(uuid.uuid4())[:8],
                symbol=rng.choice(["BTC-USD", "ETH-USD"]),
                price=round(rng.uniform(100, 100000), 2),
                size=round(rng.uniform(0.001, 0.5), 4),
                side=rng.choice(["buy", "sell"]),
                # Timestamp way outside the range of ticks we actually have.
                timestamp=ticks_df["timestamp"].min() - timedelta(hours=1),
            )
        )

    return fills

"""
Reconciliation rules.

Tier 1 ships exactly one rule: "does this fill's price roughly match
what the market was actually doing at that moment?" That single rule
already covers the two most important break types:

  - no market data near the fill at all  -> "unmatched fill"
  - market data exists, but price is way off -> "price drift"

Tier 2 will add more rules (tick sequence gaps, position-level drift).
Keeping rules as small, independent functions is what makes the pytest
suite in tests/ possible - each rule can be tested with made-up data,
no live feed required.
"""
from datetime import timedelta

import pandas as pd

from domain.models import Fill, Break

# How far around a fill's timestamp we'll look for matching market ticks.
DEFAULT_WINDOW = timedelta(seconds=5)

# How far a fill's price can drift from the nearest market price before
# we flag it as a break, expressed as a fraction (0.01 = 1%).
DEFAULT_PRICE_TOLERANCE = 0.01


def check_fill_against_market(
    fill: Fill,
    ticks_df: pd.DataFrame,
    window: timedelta = DEFAULT_WINDOW,
    price_tolerance: float = DEFAULT_PRICE_TOLERANCE,
) -> Break | None:
    """
    Check a single fill against market ticks for the same symbol.

    Returns a Break if something's wrong, or None if the fill looks fine.
    This is the function the pytest suite targets directly - no need to
    spin up a whole pipeline to test reconciliation logic.
    """
    window_start = fill.timestamp - window
    window_end = fill.timestamp + window

    nearby = ticks_df[
        (ticks_df["symbol"] == fill.symbol)
        & (ticks_df["timestamp"] >= window_start)
        & (ticks_df["timestamp"] <= window_end)
    ]

    if nearby.empty:
        return Break(
            fill_id=fill.fill_id,
            rule="unmatched_fill",
            description=(
                f"No market ticks for {fill.symbol} within "
                f"{window.total_seconds():.0f}s of fill at {fill.timestamp}"
            ),
            severity="critical",
        )

    # Compare against the nearest tick in time, not just any tick in the
    # window - that's the closest thing we have to "what was the market
    # doing at the instant of this fill."
    nearby = nearby.copy()
    nearby["time_delta"] = (nearby["timestamp"] - fill.timestamp).abs()
    closest = nearby.loc[nearby["time_delta"].idxmin()]

    market_price = closest["price"]
    pct_diff = abs(fill.price - market_price) / market_price

    if pct_diff > price_tolerance:
        return Break(
            fill_id=fill.fill_id,
            rule="price_drift",
            description=(
                f"Fill price {fill.price} for {fill.symbol} is "
                f"{pct_diff:.2%} away from market price {market_price:.2f} "
                f"at {closest['timestamp']}"
            ),
            severity="warning",
        )

    return None


def reconcile_all(fills: list[Fill], ticks_df: pd.DataFrame) -> list[Break]:
    """Run every fill through the rule set, return only the breaks found."""
    breaks = []
    for fill in fills:
        result = check_fill_against_market(fill, ticks_df)
        if result is not None:
            breaks.append(result)
    return breaks

"""
Tests for the reconciliation rules.

Notice none of these tests touch the network or a live feed - that's
the whole point. Reconciliation logic is pure: given some ticks and a
fill, does it flag correctly? We build tiny fake DataFrames by hand so
the tests are fast and deterministic.
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from domain.models import Fill
from reconciliation.rules import check_fill_against_market


def make_ticks_df(rows):
    """Helper: build a minimal ticks DataFrame from (symbol, price, timestamp) tuples."""
    return pd.DataFrame(
        [{"symbol": s, "price": p, "timestamp": t} for s, p, t in rows]
    )


BASE_TIME = datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc)


def test_fill_matching_market_price_has_no_break():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
        ("BTC-USD", 68010.0, BASE_TIME + timedelta(seconds=1)),
    ])
    fill = Fill("f1", "BTC-USD", 68005.0, 0.1, "buy", BASE_TIME + timedelta(seconds=1))

    result = check_fill_against_market(fill, ticks)

    assert result is None


def test_fill_with_no_nearby_ticks_is_unmatched():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
    ])
    # Fill happens an hour later - way outside the default 5s window.
    fill = Fill("f2", "BTC-USD", 68000.0, 0.1, "buy", BASE_TIME + timedelta(hours=1))

    result = check_fill_against_market(fill, ticks)

    assert result is not None
    assert result.rule == "unmatched_fill"
    assert result.severity == "critical"


def test_fill_with_price_far_from_market_is_price_drift():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
    ])
    # 5% above market - default tolerance is 1%, so this should trip.
    fill = Fill("f3", "BTC-USD", 71400.0, 0.1, "buy", BASE_TIME)

    result = check_fill_against_market(fill, ticks)

    assert result is not None
    assert result.rule == "price_drift"
    assert result.severity == "warning"


def test_fill_within_tolerance_has_no_break():
    ticks = make_ticks_df([
        ("BTC-USD", 68000.0, BASE_TIME),
    ])
    # 0.5% above market - under the 1% tolerance, should pass.
    fill = Fill("f4", "BTC-USD", 68340.0, 0.1, "buy", BASE_TIME)

    result = check_fill_against_market(fill, ticks)

    assert result is None


def test_only_matches_same_symbol():
    ticks = make_ticks_df([
        ("ETH-USD", 68000.0, BASE_TIME),  # same price, WRONG symbol
    ])
    fill = Fill("f5", "BTC-USD", 68000.0, 0.1, "buy", BASE_TIME)

    result = check_fill_against_market(fill, ticks)

    # No BTC-USD ticks exist, so this should be unmatched, not a false match.
    assert result is not None
    assert result.rule == "unmatched_fill"


@pytest.mark.parametrize("tolerance,expect_break", [
    (0.001, True),   # very strict - 0.5% diff should trip
    (0.05, False),   # very loose - 0.5% diff should pass
])
def test_price_tolerance_is_configurable(tolerance, expect_break):
    ticks = make_ticks_df([("BTC-USD", 68000.0, BASE_TIME)])
    fill = Fill("f6", "BTC-USD", 68340.0, 0.1, "buy", BASE_TIME)  # 0.5% off

    result = check_fill_against_market(fill, ticks, price_tolerance=tolerance)

    assert (result is not None) == expect_break