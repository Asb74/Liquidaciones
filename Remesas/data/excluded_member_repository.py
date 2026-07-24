"""Read-only access to the member exclusion flag in DBEEPPL."""
from __future__ import annotations

import sqlite3
from pathlib import Path


class ExcludedMemberRepository:
    """The only repository that knows how ``DSocio.Tipo`` is stored."""

    def __init__(self, db_path: str | Path | None = None, connection: sqlite3.Connection | None = None) -> None:
        if db_path is None and connection is None:
            raise ValueError("Se requiere DBEEPPL o una conexión con eepp adjunta")
        self.db_path = str(db_path) if db_path is not None else None
        self.connection = connection

    def list_members_with_type_other(self) -> frozenset[int]:
        """Return members whose normalized ``Tipo`` is ``OTROS``.

        Column names are discovered from the real table schema rather than being
        assumed.  The query intentionally remains read-only.
        """
        if self.connection is not None:
            schemas = {row[1] for row in self.connection.execute("PRAGMA database_list")}
            return self._query(self.connection, "eepp." if "eepp" in schemas else "")
        uri = f"file:{Path(self.db_path).as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
            return self._query(conn, "")

    @staticmethod
    def _query(conn: sqlite3.Connection, schema: str) -> frozenset[int]:
        columns = {str(row[1]).casefold(): str(row[1]) for row in conn.execute(f"PRAGMA {schema}table_info(DSocio)")}
        member_column = columns.get("idsocio")
        type_column = columns.get("tipo")
        # Older local copies can predate Tipo.  They contain no demonstrable
        # OTROS rows, so preserve availability until the next synchronization.
        if not member_column or not type_column:
            return frozenset()
        quote = lambda value: '"' + value.replace('"', '""') + '"'
        sql = (
            f"SELECT DISTINCT {quote(member_column)} FROM {schema}DSocio "
            f"WHERE UPPER(TRIM(COALESCE({quote(type_column)}, ''))) = 'OTROS'"
        )
        result = set()
        for row in conn.execute(sql):
            try:
                result.add(int(row[0]))
            except (TypeError, ValueError):
                continue
        return frozenset(result)
