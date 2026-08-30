"""
Synthetic tick generator for load testing.

How fast can this pipeline absorb messages?

The load harness needs a large batch of tick-shaped data to replay at controlled rates.
Using the live Coinbase feed for this would tie load test results to what market was doing.

A simple random-walk generator gives repeatable, on-demand data instead 
"""

import random
from datetime import datetime, timedelta, timezone

from domain.models import Tick

DEFAULT_START_PRICES = {"BTC-USD": 68000.0, "ETH-USD": 3000.0}


def generate_synthetic_ticks(n_per_symbol: int, symbols: tuple = ("BTC-USD", "ETH-USD"), seed: int | None = None,) -> list[Tick]:       #n_per_symbol = objects per symbol
    """
    Generate n_per_symbol ticks for each symbol via a simple random walk around a starting price, then shuffle so symbols
    interleave the way a real feed would. 
    """
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)

    ticks = []
    for symbol in symbols:
        price = DEFAULT_START_PRICES.get(symbol, 100.0)         
        for i in range(n_per_symbol):
            price = max(price + rng.uniform(-5, 5), 1.0)            # price can not go below 1.0
            ts = now + timedelta(milliseconds=i * 50)
            ticks.append(
                Tick(
                    symbol=symbol,
                    price=round(price, 2),
                    size=round(rng.uniform(0.001, 0.5), 4),
                    timestamp=ts,
                    received_at=ts
                )
            )

    rng.shuffle(ticks)
    return ticks