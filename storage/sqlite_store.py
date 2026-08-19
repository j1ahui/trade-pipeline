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
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._create_table()

    def _create_table(self):
        self._conn.execute()