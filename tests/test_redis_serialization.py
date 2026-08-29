"""
Test the Tick <-> Redis field serialization round-trip.

This doesnt need a running Redis server, it just checks that to_redis_fields and from_redis_fields are inverses of each other 
(float that loses precision going through str(), or a timestamp that loses its timezone).
"""

from datetime import datetime, timezone

from domain.models import Tick


def test_tick_survives_redis_round_trip():
    original = Tick(
        symbol="BTC-USD",
        price=68123.45,
        size=0.001,
        timestamp=datetime(2026, 8, 12, 10, 0, 0, 123456, tzinfo=timezone.utc),
        received_at=datetime(2026, 8, 12, 10, 0, 0, 200000, tzinfo=timezone.utc),

    )

    fields = original.to_redis_fields()
    rebuilt = Tick.from_redis_fields(fields)

    assert rebuilt == original


def test_redis_fields_are_all_strings():                    # xadd required string/bytes values (stray float or datetime would cause an error)
    tick = Tick(
        symbol="ETH-USD",
        price=3000.5,
        size=1.25,
        timestamp=datetime.now(timezone.utc),
        received_at=datetime.now(timezone.utc),
    )

    fields = tick.to_redis_fields()

    assert all(isinstance(v, str) for v in fields.values())     # check every value in fields dictionary is a string. .values() gets all values from dictionary. isinstance() checks whether v is a string. all() checks whether if every item is True
