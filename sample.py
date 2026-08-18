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