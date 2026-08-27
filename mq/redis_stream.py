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
    return redis.Redis(host=host, port=port, decode_response=True)              # creating redis client object, providing communication methods with Redis server


def publish_tick(client: redis.Redis, tick: Tick):
    """
    Publish one Tick onto the stream. Called from the async ingest side.
    """
    client.xadd(                            # redis command that adds a new entry/message with fields to redis stream
        STREAM_NAME, 
        tick.to_redis_fields(),             # param that stores new stream entry
        maxlen=MAX_STREAM_LENGTH,
        approximate=True,                   # exact trimming (trimming = removing old entries from stream (NOT SQLITE WHICH IS PERSISTENT HISTORICAL STORAGE !!!) e.g - tick 100,001) is expensive (better performance as redis doesnt have to perform expensive exact trimming on every XADD)
    )
    

def ensure_consumer_groups(client: redis.Redis):
    """
    Create the consumer group if it doesnt exist yet.
    mkstream=True creates the stream itself if no ticks have been published yet (allows workers to start before cb_ws starts)
    """
    try:
        client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)                # id=0 (where the group starts reading from. 0 means the consumer group can process existing messages from the beginning. $ = process messages after group has been created)
        logger.info("Created consumer group '%s' on stream '%s'", GROUP_NAME, STREAM_NAME)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):                   # group already exists
            pass
        else:
            raise


def read_batch(client: redis.Redis, consumer_name: str, count: int=10, block_ms: int=2000):                 # block_ms = if there are no messages available rn, wait up to 2ms (2000), for a new message to arrive 
    """
    Read up to 'count' messages for this consumer within the group.
    Return a list of (message_id, Tick) pairs. 
    Blocks up to 'block_ms' waiting for messages if none are immediately available (allows workers to sit idle without busy-looping CPU). 
    """
    response = client.xreadgroup(                                                           # XREADGROUP = gets messages out of stream using consumer group (XADD puts messages onto stream)
        GROUP_NAME, consumer_name, {STREAM_NAME: ">"}, count=count, block=block_ms          # {STREAM_NAME: ">"} = python dict
    )

    if not response:
        return []
    
    _, messages = response[0]           # response shape of redis return is [(stream_name, [(message_id, fields_dict), ...])]
    return [(msg_id, Tick.from_redis_fields(fields)) for msg_id, fields in messages]        # returns (message ID, Tick object)


def ack(client: redis.Redis, message_id: str):
    """
    Acknowledge a message as processed, removing it from the pending list.
    """
    client.xack(STREAM_NAME, GROUP_NAME, message_id)