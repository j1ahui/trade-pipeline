"""
Tier 1 entry point.

Flow: connect to the live feed, normalise ticks, write to SQLite (after collecting for a while), simulate some fills,
      reconcile, print a report

Run it directly: python3 main.py 

By default, it listens for 30s before moving to reconciliation (adjust LISTEN_SECONDS below if you want more data before reconciling).
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
        for b in breaks:
            print(f"[{b.severity.upper():8}] {b.rule:15} fill={b.fill_id} {b.description}")

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
    asyncio.run(main())           # asyncio.run() runs coroutine

