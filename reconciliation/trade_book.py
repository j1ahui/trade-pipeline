"""
A fake trade book.

Irl, this would be the "fills our trading desk actually made", pulled from an execution system.
We dont have a trading desk so we simulate one: pick a few random moments and pretend we 
bought/sold at roughly the market price (with fills intentionally off so 
reconciliation has something to catch)
"""

import random
import uuid             
from datetime import timedelta

import pandas as pd

from domain.models import Fill

def simulate_fills(
    ticks_df: pd.DataFrame, 
    n_fills: int=20,
    break_rate: float=0.25,                         # 25% of fills to be wrong
    seed: int | None = None,) -> list[Fill]:        # seed controls how that generator produces its random numbers. prevents results from changing between runs (fake trade generations must be reproducible)
    """
    Generate fake fills by sampling real ticks and perturbing some of them.

    break_rate: fraction of fills that get deliberately corrupted (price moved
    away from market, or timestamp shifted) so the reconciliation suite has 
    real breaks to find. Without this, every test run would have nothing to 
    report (not a useful demo).
    """

    rng = random.Random(seed)           # creates instance. same seed as pandas random sampling below 
    if ticks_df.empty:
        return []
    
    sample = ticks_df.sample(n=min(n_fills, len(ticks_df)), random_state=seed)      # same seed as pythons random generator     # sample() = pandas df method that randomly selects rows from DataFrame. might not always have 20 fills (df might only have 10 ticks -> use smallest)

    fills = []
    for _, row in sample.iterrows():            # sample = is the df. _ = rows index. iterrows = iterates through rows
        is_break = rng.random() < break_rate    # returns a boolean
        price = row["price"]                    # row = iterator (current row). reps entire current row (just accessing "price" col)
        timestamp = row["timestamp"]            # get timestamp from current row 

        if is_break:
            price = price * (1 + rng.choice([-1, 1]) * rng.uniform(0.02, 0.05))     # choice() = randomly chooses from provided list. [-1, 1] to decide whether price moves up or down. uniform() = generates random decimal between 0.02 and 0.05 (choosing a price adjustment between 2% and 5%). (essentially creates fills)

        fills.append(
            Fill(                               # creating fill instance (constructs an object of type Fill)
                fill_id=str(uuid.uuid4())[:8],  # creating an instance. creates smt like 550e8400-e29b-41d4-a716-446655440000
                symbol=row["symbol"],
                price=round(price, 2),
                size=round(rng.uniform(0.001, 0.5), 4),
                side=rng.choice(["buy", "sell"]),
                timestamp=timestamp,
            )
        )

    for _ in range(max(1, n_fills // 10)):      # fills with no matching market data ("unmatched fill" break case)
        fills.append(
            Fill(
                fill_id=str(uuid.uuid4())[:8],
                symbol=rng.choice(["BTC-USD", "ETH-USD"]),
                price=round(rng.uniform(100, 10000), 2),
                size=round(rng.uniform(0.001, 0.5), 4),
                side=rng.choice(["buy", "sell"]),
                timestamp=ticks_df["timestamp"].min() - timedelta(hours=1),     # going back 1 hour
            )
        )

    return fills