"""
Redis Streams queue layer.

Implementing consumer-group functionality, instead of using XREAD (redis command for reading messages from Redis stream).
Redis: creates a boundary between producing ticks and processing ticks 
Distribution implementation introduced - if you need for more processing capacity, you add more workers without changing producer
"""
import logging

import redis

from domain.models import Tick

logger = logging.getLogger(__name__)

STREAM_NAME = "ticks_stream"
GROUP_NAME = "tick_workers"

MAX_STREAM_LENGTH = 100_000             # cap stream length so it doesnt grow forever if workers fall behind 

def get_redis_client(host: str = "localhost", port: int = 6379) -> redis.Redis:             # host: str = "localhost" (type hints and default values)
    """
    One place to build a Redis connection (config lives in one spot).
    """
    return redis.Redis(host=host, port=port, decode_response=True)              # creating redis client object


def publish_tick(client: redis.Redis, tick: Tick):
    """
    Publish one Tick onto the stream. Called from the async ingest side.
    """
    client.xadd(
        STREAM_NAME, 
        tick.to_redis_fields(),
        maxlen=MAX_STREAM_LENGTH,
        approximate=True,                   # exact trimming is expensive
    )
    