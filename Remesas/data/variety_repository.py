from __future__ import annotations

import logging
import sqlite3

from domain.varieties import VarietyGroup, normalize_variety_text

logger = logging.getLogger(__name__)


class VarietyRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._log_schema()

    def _log_schema(self) -> None:
        rows = self.conn.execute("PRAGMA eepp.table_info('MVariedad')").fetchall()
        cols = [str(r[1]) for r in rows]
        logger.info("[Variedades] PRAGMA table_info('MVariedad') columnas=%s", ",".join(cols))

    def list_varieties(self, crop: str) -> tuple[str, ...]:
        sql = """
        SELECT DISTINCT TRIM(Variedad) AS Variedad
        FROM eepp.MVariedad
        WHERE UPPER(TRIM(CULTIVO)) = UPPER(TRIM(?))
          AND Variedad IS NOT NULL AND TRIM(Variedad) <> ''
        ORDER BY UPPER(TRIM(Variedad))
        """
        return tuple(str(r[0]) for r in self.conn.execute(sql, (crop,)).fetchall())

    def list_groups(self, crop: str) -> tuple[VarietyGroup, ...]:
        sql = """
        SELECT DISTINCT TRIM(GRUPO) AS Grupo, TRIM(SUBGRUPO) AS Subgrupo
        FROM eepp.MVariedad
        WHERE UPPER(TRIM(CULTIVO)) = UPPER(TRIM(?))
          AND GRUPO IS NOT NULL AND TRIM(GRUPO) <> ''
          AND SUBGRUPO IS NOT NULL AND TRIM(SUBGRUPO) <> ''
        ORDER BY UPPER(TRIM(GRUPO)), UPPER(TRIM(SUBGRUPO))
        """
        return tuple(VarietyGroup(crop, str(r[0]), str(r[1])) for r in self.conn.execute(sql, (crop,)).fetchall())

    def resolve_group(self, crop: str, group: str, subgroup: str) -> tuple[str, ...]:
        sql = """
        SELECT DISTINCT TRIM(Variedad) AS Variedad
        FROM eepp.MVariedad
        WHERE UPPER(TRIM(CULTIVO)) = UPPER(TRIM(?))
          AND UPPER(TRIM(GRUPO)) = UPPER(TRIM(?))
          AND UPPER(TRIM(SUBGRUPO)) = UPPER(TRIM(?))
          AND Variedad IS NOT NULL AND TRIM(Variedad) <> ''
        ORDER BY UPPER(TRIM(Variedad))
        """
        return tuple(str(r[0]) for r in self.conn.execute(sql, (crop, group, subgroup)).fetchall())

    def find_variety(self, crop: str, value: str) -> str | None:
        wanted = normalize_variety_text(value)
        for variety in self.list_varieties(crop):
            if normalize_variety_text(variety) == wanted:
                return variety
        return None
