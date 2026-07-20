from __future__ import annotations

import sqlite3
from pathlib import Path

from .migrations import migrate
from .search_text import normalize_search_text


class PersistenceDatabase:
    """Factoría de conexiones. Los decimales se guardan como texto canónico."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        # Keep accent-insensitive member searches in SQLite so LIMIT is applied
        # only after the textual predicate has selected the matching rows.
        conn.create_function("NORMALIZE_SEARCH_TEXT", 1, normalize_search_text)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            migrate(conn)
