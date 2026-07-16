"""Base SQLite local, de escritura, exclusiva de liquidaciones definitivas."""

from .database import PersistenceDatabase

__all__ = ["PersistenceDatabase"]
