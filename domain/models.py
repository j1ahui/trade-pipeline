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

    @classmethod                                                    # can create a Tick without having a tick object like Tick.from_coinbase_ticker(msg) -> can be directly called on class 
    def from_coinbase_ticker(cls, msg: dict) -> "Tick":             # cls = the class itself (Tick), passed as a param
        """
        Build a Tick from a raw Coinbase 'ticker' channel message.

        Coinbase's ticker messages look like:
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "68123.45",
            "last_size": "0.001",
            "time": "2026-08-12T10:00:00.123456Z",              # ISO 8601 formatting
            ...
        }

        Change function below if Coinbase message shape changes
        
        """
        return cls(                                                 # cls = Tick. creating a new object of Tick class
            symbol=msg["product_id"],
            price=float(msg["price"]),
            size=float(msg.get("last_size", 0.0)),
            timestamp=datetime.fromisoformat(msg["time"].replace("Z", "+00:00")),
            received_at=datetime.now(timezone.utc),                 # .utc = in UTC
        )


@dataclass(frozen=True)
class Fill:
    """A single simulated trade execution from fictional trade book."""
    fill_id: str
    symbol: str
    price: float
    size: float
    side: str
    timestamp: datetime


@dataclass(frozen=True)
class Break:
    """A reconciliation problem: something that didnt match up."""
    fill_id: str
    rule: str                       # which reconciliation rule detected the problem
    description: str
    severity: str = "warning"       # warning or critical