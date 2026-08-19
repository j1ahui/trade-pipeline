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