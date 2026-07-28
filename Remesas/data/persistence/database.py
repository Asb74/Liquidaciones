from __future__ import annotations

import sqlite3
import logging
import threading
from contextlib import contextmanager
from pathlib import Path

from .migrations import migrate
from .search_text import normalize_search_text


class PersistenceDatabase:
    """Factoría de conexiones. Los decimales se guardan como texto canónico."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    @contextmanager
    def connect(self):
        """Yield a connection owned by, and closed in, the calling thread."""
        conn = self.open_connection()
        try:
            yield conn
        finally:
            self.close_connection(conn)

    def open_connection(self) -> sqlite3.Connection:
        """Open a manually managed connection for an explicit transaction."""
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
        logging.getLogger(__name__).info(
            "[SQLiteConnection] database=%s thread_id=%s action=OPENED",
            self.path, threading.get_ident(),
        )
        return conn

    def close_connection(self, conn: sqlite3.Connection) -> None:
        conn.close()
        logging.getLogger(__name__).info(
            "[SQLiteConnection] database=%s thread_id=%s action=CLOSED",
            self.path, threading.get_ident(),
        )

    def initialize(self) -> None:
        with self.connect() as conn:
            migrate(conn)
