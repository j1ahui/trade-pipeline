"""
Async ingest from Coinbases public Web Socket feed.

This piece proves "non-blocking ingest of a live streaming feed".
No auth needed for market data (Coinbases ticker channel is public).

We connect once, sub to a few products and yield a Tick object every time a trade prints.
"""

import asyncio
import json
import logging
from typing import AsyncIterator, Sequence          # used for type hints

import websockets

from domain.models import Tick

logger = logging.getLogger(__name__)                # logger = logging object to send messages to Pythons logging system. __name__ = name of current module (coinbase_ws (no .py)) without file type

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


async def stream_ticks(symbols: Sequence[str]) -> AsyncIterator[Tick]:          # AsyncIterator[Tick] = returns an asynchronous iterator producing Tick objects
    """
    Connect to Coinbase, sub to the ticker channel for the given symbols, yield 
    a normalised Tick for every trade that prints.

    This is an async generator: callers do 'async for tick in stream_ticks(...)'.
    Thats the asyncio idiom for "give me items as they arrive, dont block rest if program while waiting".
    """
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": list(symbols),
        "channels": ["ticker"],
    }

    async with websockets.connect(COINBASE_WS_URL) as ws:               # ws object. uses pythons context manager mechanism to automatically close connection (async handles asynchronous setup and cleanup. websockets.connect() is designed to work with "async with"). 
        await ws.send(json.dumps(subscribe_msg))                        # converting python dict into json
        logger.info("Subscribed to ticker channel for %s", symbols)

        async for raw_message in ws:
            msg = json.loads(raw_message)                               # converting json received from coinbase into python dict

            if msg.get("type") != "ticker":                             # coinbase sends a few message types on this channel ("subscriptions" (acks), "ticker" (trades), error messages)
                continue

            try:
                yield Tick.from_coinbase_ticker(msg)        # yield keeps generator alive -> stream of coinbase messages continue to flow in. 
            except (KeyError, ValueError) as e:
                logger.warning("Skipping malformed ticker message: %s", e)
                continue

async def _demo():
    """
    Quick manual test: print ticks for 15s then stop.
    """
    logging.basicConfig(level=logging.INFO)             # show info messages

    async def _run():
        async for tick in stream_ticks(["BTC-USD", "ETH-USD"]):     # stream_ticks(["BTC-USD", "ETH-USD"]) is producing ticks. async for tick in = consuming ticks
            print(tick)

    try:
        await asyncio.wait_for(_run(), timeout=15)
    except asyncio.TimeoutError:
        print("\nDemo finished (15s elapsed).")


if __name__ == "__main__":
    asyncio.run(_demo())