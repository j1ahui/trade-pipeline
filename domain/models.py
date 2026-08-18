"""
Domain objects for the post-trade pipeline
"""

from dataclasses import dataclass                   # helps create classes that are primarily used to store data
from datetime import datetime, timezone

@dataclass(frozen=True)                             # frozen=True means object fields cant be modified once created
class Tick :
    """A single normalised market data observation"""
    symbol: str
    price: float
    size: float
    timestamp: datetime
    received_at: datetime

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

        Change function below if Coinbase message shape changes
        
        """
        return cls(
            symbol=msg["product_id"],
            price=float(msg["price"]),
            size=float(msg.get["last_size", 0.0]),
            timestamp=datetime.fromisoformat(msg["time"].replace("Z", "+00:00")),
            received_at=datetime.now(timezone.utc),
        )