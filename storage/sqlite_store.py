"""
SQLite backed tick storage

As of rn: 1 table, 1 connection, no concurrency
Ticks go in, can query them back out with SQL or pandas
"""

import sqlite3
from pathlib import Path

from domain.models import Tick

DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "ticks.db"            # Path(__file__) = turns file into a Path object (makes working with paths easier)

class TickStore:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH):        # type hint. also gives db_path a default value (can be overriden)
        self.db_path = db_path                                  # self.db_path points to data/ticks.db
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)              # opens a connection. self._conn points to connection to database (ticks.db). use self._conn to tell sqlite to run sql against ticks.db database
        self._create_table()

    def _create_table(self):
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticks (
            symbol TEXT NOT NULL,
            price REAL NOT NULL,                    
            size REAL NOT NULL,
            timestamp TEXT NOT NULL,
            received_at TEXT NOT NULL)
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol ON ticks(symbol, timestamp)"
        )
        self._conn.commit()


    def write_tick(self, tick: Tick):
        self._conn.execute(
            "INSERT INTO ticks (symbol, price, size, timestamp, received_at)"
            "VALUES (?, ?, ?, ?, ?)",
            (
                tick.symbol,
                tick.price,
                tick.size,
                tick.timestamp.isoformat(),
                tick.received_at.isoformat(),
            ),
        )
        self._conn.commit()

    
    def write_ticks(self, ticks: list[Tick]):               # executemany() processes ticks together as opposed just one tick (execute())
        """Batch insert - useful once youre writing faster than one at a time."""
        self._conn.executemany(
            "INSERT INTO ticks (symbol, price, size, timestamp, received_at)"
            "VALUES (?, ?, ?, ?, ?)",
            [
                (t.symbol, t.price, t.size, t.timestamp.isoformat(), t.received_at.isoformat())
                for t in ticks
            ],
        )
        self._conn.commit()


    def read_all_as_dataframe(self):                        # parse_dates converts dates (which are currently in TEXT) into actual datetime values. read_sql_query() turns ticks into pandas df (giant table)
        """Pull everything back out as a pandas DataFrame for reconciliation."""
        import pandas as pd                                 # df - loaded into python memory so you can analyse/manipulate easily
        return pd.read_sql_query(
            "SELECT * FROM ticks ORDER BY timestamp", self._conn,
            parse_dates=["timestamp", "received_at"],
        )


    def close(self):
        self._conn.close()
