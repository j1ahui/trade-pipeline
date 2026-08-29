"""
Tier 1 entry point.

Flow: connect to the live feed, normalise ticks, write to SQLite (after collecting for a while), simulate some fills,
      reconcile, print a report

Run it directly: python3 main.py 

By default, it listens for 30s before moving to reconciliation (adjust LISTEN_SECONDS below if you want more data before reconciling).

Tier 2 entry point.

Flow: spawn N workers processes, run the async producer (which streams live ticks) and publishes them to Redis, stop the 
      workers once the producer finishes, simulate fills, reconcile what workers wrote to SQLite.
"""

import asyncio
import logging
import time

from ingest.coinbase_ws import stream_ticks
from ingest.producer import run_producer
from workers.worker import spawn_workers, stop_workers
from storage.sqlite_store import TickStore
from reconciliation.trade_book import simulate_fills
from reconciliation.rules import reconcile_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SYMBOLS = ["BTC-USD", "ETH-USD"]
LISTEN_SECONDS = 30
N_WORKERS = 3
WORKER_DRAIN_SECONDS = 3        # give workers a moment to finish last batch


async def collect_ticks(store: TickStore, seconds: int):        # expecting a TickStore object
    """
    Listen to the live feed for a fixed window, writing every tick to storage.
    """
    count = 0

    async def _run():               # nested function
        nonlocal count 
        async for tick in stream_ticks(SYMBOLS):
            store.write_tick(tick)  # .write_tick() is a method
            count += 1              # belongs to outer function
            if count % 25 == 0:     # logger runs in 25s (prints 25 ticks, 50 ticks, 75 ticks)
                logger.info("Ingested %d ticks so far...", count)

    try:
        await asyncio.wait_for(_run(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

    logger.info("Done Listening. Ingested %d ticks in total.", count)


def run_reconciliation(store: TickStore):
    """
    Simulate a trade book against the ticks we just collected and report breaks.
    """
    ticks_df = store.read_all_as_dataframe()

    if ticks_df.empty:
        logger.warning("No ticks were collected - nothing to reconcile against.")
        return
    
    fills = simulate_fills(ticks_df, n_fills=20, break_rate=0.25, seed=42)
    breaks = reconcile_all(fills, ticks_df)

    print("\n" + "=" * 60)
    print(f"RECONCILIATION REPORT - {len(fills)} fills checked, {len(breaks)} breaks found")
    print("=" * 60)

    if not breaks:
        print("No breaks found. All files reconciled cleanly.")
    else:
        by_rule = {}
        for b in breaks:
            by_rule.setdefault(b.rule, []).append(b)
        for rule, rule_breaks in by_rule.items():
            print(f"\n{rule} ({len(rule_breaks)}):")
            for b in rule_breaks:
                print(f"[{b.severity.upper():8}] {b.rule:15} fill={b.fill_id} {b.description}")

    print("=" * 60 + "\n")

async def main():
    logger.info("Spawning %d worker processes...", N_WORKERS)
    processes, stop_event = spawn_workers(N_WORKERS)

    time.sleep(1)           # give workers a moment to create the consumer group before the producer starts publishing (no early ticks land before the group exists)

    try: 
        logger.info("Listening to live feed for %ds...", LISTEN_SECONDS)
        published = await run_producer(SYMBOLS, LISTEN_SECONDS)
        logger.info("Producer published %d ticks. Draining workers...", published)
        time.sleep(WORKER_DRAIN_SECONDS)

    finally:
        stop_workers(processes, stop_event)
        logger.info("All workers have stopped.")

    store = TickStore()
    try:
        run_reconciliation(store)
    finally:
        store.close()


if __name__ == "__main__":
    asyncio.run(main())           # asyncio.run() runs coroutine

