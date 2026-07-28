"""Compatibilidad de construcción para la persistencia PostgreSQL central."""
from __future__ import annotations

from contextlib import contextmanager

from data.postgres_repository import PostgresRepository, PostgreSQLSettings
from .migrations import migrate


class PersistenceDatabase(PostgresRepository):
    """Repositorio PostgreSQL de persistencia (el argumento antiguo se ignora)."""

    def __init__(self, _obsolete_path: str | None = None, *, settings: PostgreSQLSettings | None = None, connection=None) -> None:
        super().__init__(settings, connection=connection)

    @property
    def path(self) -> str:
        return "postgresql://liquidaciones"

    @contextmanager
    def connect(self):
        if self._connection is not None:
            yield self
            return
        with super().connect() as repository:
            yield type(self)(settings=self.settings, connection=repository._connection)

    def open_connection(self):
        connection = self.pool(self.settings).getconn()
        connection.execute("SET search_path TO liquidaciones, legacy_dbfruta, legacy_eepp, integracion, informes, public")
        return type(self)(settings=self.settings, connection=connection)

    def close_connection(self, connection) -> None:
        raw_connection = getattr(connection, "_connection", connection)
        self.pool(self.settings).putconn(raw_connection)

    def initialize(self) -> None:
        with self.transaction() as repository:
            migrate(repository)
