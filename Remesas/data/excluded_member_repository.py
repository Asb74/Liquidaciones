"""Read-only access to the member exclusion flag in DBEEPPL."""
from __future__ import annotations

from data.repository import IDataRepository


class ExcludedMemberRepository:
    """The only repository that knows how ``DSocio.Tipo`` is stored."""

    def __init__(self, db_path=None, connection: IDataRepository | None = None) -> None:
        if connection is None:
            raise ValueError("Se requiere una conexión PostgreSQL")
        self.connection = connection

    def list_members_with_type_other(self) -> frozenset[int]:
        """Return members whose normalized ``Tipo`` is ``OTROS``.

        Column names are discovered from the real table schema rather than being
        assumed.  The query intentionally remains read-only.
        """
        return self._query(self.connection, "legacy_eepp.")

    @staticmethod
    def _query(conn: IDataRepository, schema: str) -> frozenset[int]:
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
