"""
Ingest producer.

Tier 1 had ingest write straight to SQLite. 
Tier 2 splits that in two: this module still does the async WebSocket listening but now it 
publishes each Tick onto the redis stream instead of touching storage directly.

Storage becomes the workers job, not ingest's.
Ingests only responsibility is to get ticks from Coinbase and put onto redis stream
"""

import asyncio
import logging

from ingest.coinbase_ws import stream_ticks
from mq.redis_stream import get_redis_client, publish_tick

logger = logging.getLogger(__name__)                            # function that returns a Logger object


async def run_producer(symbols: list[str], seconds: int, redis_host: str="localhost"):
    """
    Listen to the live feed for a fixed window, publishing every tick to the Redis stream (mirrors collect_ticks).
    """
    client = get_redis_client(host=redis_host)                  # host=redis_host not required but useful since youre passing workers configured redis host into redis client
    count = 0

    async def _run():
        nonlocal count
        async for tick in stream_ticks(symbols):
            publish_tick(client, tick)
            count += 1
            if count % 25 == 0:
                logger.info("Published %d ticks to the stream so far...", count)

    try:
        await asyncio.wait_for(_run(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

    logger.info("Producer done. Published %d ticks total.", count)
    return count