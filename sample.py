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


"""
Async ingest from Coinbase's public WebSocket feed.

This is the piece that proves "non-blocking ingest of a live streaming
feed." No auth needed for market data - Coinbase's ticker channel is
public. We connect once, subscribe to a few products, and yield a
Tick object every time a trade prints.
"""
import asyncio
import json
import logging
from typing import AsyncIterator, Sequence

import websockets

from domain.models import Tick

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


async def stream_ticks(symbols: Sequence[str]) -> AsyncIterator[Tick]:
    """
    Connect to Coinbase, subscribe to the ticker channel for the given
    symbols, and yield a normalized Tick for every trade that prints.

    This is an async generator: callers do `async for tick in stream_ticks(...)`.
    That's the asyncio idiom for "give me items as they arrive, don't
    block the rest of the program while waiting."
    """
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": list(symbols),
        "channels": ["ticker"],
    }

    async with websockets.connect(COINBASE_WS_URL) as ws:
        await ws.send(json.dumps(subscribe_msg))                # converting a python dict into json
        logger.info("Subscribed to ticker channel for %s", symbols)

        async for raw_message in ws:
            msg = json.loads(raw_message)           # converting json received from coinbase to python dict 

            # Coinbase sends a few message types on this channel:
            # "subscriptions" (ack), "ticker" (the actual trades we want),
            # and occasionally error messages. We only care about ticker.
            if msg.get("type") != "ticker":
                continue

            # Some ticker messages arrive before a trade has actually
            # happened (e.g. an initial snapshot) and won't have a price.
            # Skip anything malformed rather than crashing the pipeline.
            try:
                yield Tick.from_coinbase_ticker(msg)
            except (KeyError, ValueError) as e:
                logger.warning("Skipping malformed ticker message: %s", e)
                continue


async def _demo():
    """Quick manual test: print ticks for 15 seconds then stop."""
    logging.basicConfig(level=logging.INFO)

    async def _run():
        async for tick in stream_ticks(["BTC-USD", "ETH-USD"]):         # stream_ticks(["BTC-USD", "ETH-USD"]) is producing ticks. async for tick in = consuming ticks
            print(tick)

    try:
        await asyncio.wait_for(_run(), timeout=15)
    except asyncio.TimeoutError:
        print("\nDemo finished (15s elapsed).")


if __name__ == "__main__":
    asyncio.run(_demo())

"""
Tier 1 entry point.

Flow: connect to the live feed -> normalize ticks -> write to SQLite ->
(after collecting for a while) simulate some fills -> reconcile ->
print a report.

Run it directly:
    python main.py

By default it listens for 30 seconds before moving to reconciliation.
Adjust LISTEN_SECONDS below if you want more data before reconciling.
"""
import asyncio
import logging

from ingest.coinbase_ws import stream_ticks
from storage.sqlite_store import TickStore
from reconciliation.trade_book import simulate_fills
from reconciliation.rules import reconcile_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SYMBOLS = ["BTC-USD", "ETH-USD"]
LISTEN_SECONDS = 30


async def collect_ticks(store: TickStore, seconds: int):
    """Listen to the live feed for a fixed window, writing every tick to storage."""
    count = 0

    async def _run():
        nonlocal count
        async for tick in stream_ticks(SYMBOLS):
            store.write_tick(tick)
            count += 1
            if count % 25 == 0:
                logger.info("Ingested %d ticks so far...", count)

    try:
        await asyncio.wait_for(_run(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

    logger.info("Done listening. Ingested %d ticks total.", count)


def run_reconciliation(store: TickStore):
    """Simulate a trade book against the ticks we just collected and report breaks."""
    ticks_df = store.read_all_as_dataframe()

    if ticks_df.empty:
        logger.warning("No ticks were collected - nothing to reconcile against.")
        return

    fills = simulate_fills(ticks_df, n_fills=20, break_rate=0.25, seed=42)
    breaks = reconcile_all(fills, ticks_df)

    print("\n" + "=" * 60)
    print(f"RECONCILIATION REPORT — {len(fills)} fills checked, {len(breaks)} breaks found")
    print("=" * 60)

    if not breaks:
        print("No breaks found. All fills reconciled cleanly.")
    else:
        for b in breaks:
            print(f"[{b.severity.upper():8}] {b.rule:15} fill={b.fill_id}  {b.description}")

    print("=" * 60 + "\n")


async def main():
    store = TickStore()
    try:
        logger.info("Listening to live feed for %ds...", LISTEN_SECONDS)
        await collect_ticks(store, LISTEN_SECONDS)
        run_reconciliation(store)
    finally:
        store.close()


if __name__ == "__main__":
    asyncio.run(main())


"""
Redis Streams queue layer.

This is the boundary the JD calls out specifically: "a message queue -
the boundary that makes this distributed instead of one script." Before
Tier 2, ingest wrote directly to SQLite. Now ingest only knows how to
publish a Tick onto a stream, and workers only know how to read from
it - neither side knows the other exists. That's what lets you scale
producers and consumers independently, and it's why swapping this for
Kafka later wouldn't require touching ingest or storage at all.

Consumer groups (not plain XREAD) are used deliberately: a group lets
multiple worker processes split the same stream between them instead of
each worker seeing every message, and it gives each message an
explicit ack step - if a worker dies mid-processing, the message stays
"pending" and can be claimed by another worker instead of being lost.
"""
import logging

import redis

from domain.models import Tick

logger = logging.getLogger(__name__)

STREAM_NAME = "ticks_stream"
GROUP_NAME = "tick_workers"

# Cap the stream length so it doesn't grow forever if workers fall
# behind - Tier 3's load harness is exactly the tool that will tell us
# whether this cap is being hit.
MAX_STREAM_LENGTH = 100_000


def get_redis_client(host: str = "localhost", port: int = 6379) -> redis.Redis:
    """One place to build a Redis connection, so config lives in one spot."""
    return redis.Redis(host=host, port=port, decode_responses=True)


def publish_tick(client: redis.Redis, tick: Tick):
    """Publish one Tick onto the stream. Called from the async ingest side."""
    client.xadd(
        STREAM_NAME,
        tick.to_redis_fields(),
        maxlen=MAX_STREAM_LENGTH,
        approximate=True,  # exact trimming is expensive; approximate is fine here
    )


def ensure_consumer_group(client: redis.Redis):
    """
    Create the consumer group if it doesn't exist yet.

    mkstream=True means this also creates the stream itself if no ticks
    have been published yet - lets workers start up before ingest does
    without erroring.
    """
    try:
        client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        logger.info("Created consumer group '%s' on stream '%s'", GROUP_NAME, STREAM_NAME)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            # Group already exists - fine, another worker (or a previous
            # run) already created it.
            pass
        else:
            raise


def read_batch(client: redis.Redis, consumer_name: str, count: int = 10, block_ms: int = 2000):
    """
    Read up to `count` new messages for this consumer within the group.

    Returns a list of (message_id, Tick) pairs. Blocks up to block_ms
    waiting for messages if none are immediately available - this is
    what lets a worker sit idle without busy-looping the CPU.
    """
    response = client.xreadgroup(
        GROUP_NAME, consumer_name, {STREAM_NAME: ">"}, count=count, block=block_ms
    )

    if not response:
        return []

    # response shape: [(stream_name, [(message_id, fields_dict), ...])]
    _, messages = response[0]
    return [(msg_id, Tick.from_redis_fields(fields)) for msg_id, fields in messages]


def ack(client: redis.Redis, message_id: str):
    """Acknowledge a message as processed, removing it from the pending list."""
    client.xack(STREAM_NAME, GROUP_NAME, message_id)

"""
Worker processes.

Each worker is a standalone process (not a thread - real multiprocessing,
each with its own Python interpreter and no shared memory) that pulls
ticks off the Redis stream and writes them to SQLite. This is the piece
that proves "distributed" rather than "one script with an async loop":
you can run 1 worker or 10, and the queue is what makes that a config
change instead of a rewrite.

A worker doesn't know or care that ingest is written with asyncio, or
that there's a WebSocket involved at all - it only speaks the queue's
language (read a batch, write it, ack it). That decoupling is the point.
"""
import logging
import multiprocessing

from mq.redis_stream import get_redis_client, ensure_consumer_group, read_batch, ack
from storage.sqlite_store import TickStore

logger = logging.getLogger(__name__)


def run_worker(worker_id: int, stop_event: multiprocessing.Event, redis_host: str = "localhost"):
    """
    The function each worker process runs. Loops reading batches from the
    stream, writing them to storage, and acking, until told to stop.

    stop_event is a multiprocessing.Event - the standard way to signal
    "shut down" across process boundaries, since worker processes don't
    share memory with the parent and can't just check a regular variable.
    """
    consumer_name = f"worker-{worker_id}"
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [worker-{worker_id}] %(message)s")

    client = get_redis_client(host=redis_host)
    ensure_consumer_group(client)
    store = TickStore()  # each worker opens its own SQLite connection

    written = 0
    logger.info("Worker %d started, consumer name '%s'", worker_id, consumer_name)

    try:
        while not stop_event.is_set():
            # block_ms=1000 means: wait up to 1s for new messages, then
            # loop back around and check stop_event again. Without this
            # periodic wake-up, a worker could block forever past when
            # it was told to stop.
            batch = read_batch(client, consumer_name, count=10, block_ms=1000)

            if not batch:
                continue

            for message_id, tick in batch:
                store.write_tick(tick)
                ack(client, message_id)
                written += 1

            if written % 50 < len(batch):
                logger.info("Written %d ticks so far", written)
    finally:
        store.close()
        logger.info("Worker %d shutting down, wrote %d ticks total", worker_id, written)


def spawn_workers(n_workers: int, redis_host: str = "localhost"):
    """
    Start n_workers worker processes and return (processes, stop_event).

    Caller is responsible for calling stop_event.set() and then joining
    the processes when it's time to shut down - see main.py.
    """
    stop_event = multiprocessing.Event()                # creating event object. each worker is intended to be a separate python process
    processes = []

    for worker_id in range(n_workers):
        p = multiprocessing.Process(
            target=run_worker, args=(worker_id, stop_event, redis_host), daemon=False
        )
        p.start()
        processes.append(p)

    return processes, stop_event


def stop_workers(processes: list[multiprocessing.Process], stop_event: multiprocessing.Event, timeout: int = 5):
    """Signal all workers to stop and wait for them to exit cleanly."""
    stop_event.set()
    for p in processes:
        p.join(timeout=timeout)
        if p.is_alive():
            logger.warning("Worker %s did not exit cleanly, terminating", p.name)
            p.terminate()

