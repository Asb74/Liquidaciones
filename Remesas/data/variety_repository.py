from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterable

from dataclasses import dataclass

from domain.varieties import VarietyGroup, normalize_variety_text

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VarietalGroup:
    crop: str
    group: str
    subgroup: str

    @property
    def label(self) -> str:
        return f"{self.group} {self.subgroup}".strip()


@dataclass(frozen=True)
class VarietyMatch:
    crop: str
    variety: str


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


    def find_exact_variety(self, crop: str, normalized_variety: str) -> str | None:
        sql = """
        SELECT TRIM(Variedad) AS Variedad
        FROM eepp.MVariedad
        WHERE UPPER(TRIM(CULTIVO)) = ?
          AND UPPER(TRIM(Variedad)) = ?
          AND Variedad IS NOT NULL AND TRIM(Variedad) <> ''
        ORDER BY TRIM(Variedad)
        LIMIT 1
        """
        row = self.conn.execute(sql, (normalize_variety_text(crop), normalized_variety)).fetchone()
        return str(row[0]) if row else None

    def find_exact_varieties(self, master_crops: Iterable[str], normalized_value: str) -> tuple[VarietyMatch, ...]:
        """Find exact normalized varieties while retaining the matching master crop."""
        matches: list[VarietyMatch] = []
        for crop in dict.fromkeys(normalize_variety_text(crop) for crop in master_crops):
            for variety in self.list_varieties(crop):
                if normalize_variety_text(variety) == normalized_value:
                    matches.append(VarietyMatch(crop, variety))
        return tuple(matches)

    def find_group_by_label(self, crop: str, normalized_label: str) -> VarietalGroup | None:
        sql = """
        SELECT TRIM(GRUPO) AS Grupo, TRIM(SUBGRUPO) AS Subgrupo
        FROM eepp.MVariedad
        WHERE UPPER(TRIM(CULTIVO)) = ?
          AND GRUPO IS NOT NULL AND TRIM(GRUPO) <> ''
          AND SUBGRUPO IS NOT NULL AND TRIM(SUBGRUPO) <> ''
          AND UPPER(TRIM(GRUPO)) || ' ' || UPPER(TRIM(SUBGRUPO)) = ?
        ORDER BY UPPER(TRIM(GRUPO)), UPPER(TRIM(SUBGRUPO))
        LIMIT 1
        """
        row = self.conn.execute(sql, (normalize_variety_text(crop), normalized_label)).fetchone()
        return VarietalGroup(crop, str(row[0]), str(row[1])) if row else None

    def find_groups_by_label(self, master_crops: Iterable[str], normalized_value: str) -> tuple[VarietalGroup, ...]:
        """Find group labels across candidate master crops."""
        matches: list[VarietalGroup] = []
        for crop in dict.fromkeys(normalize_variety_text(crop) for crop in master_crops):
            matches.extend(group for group in self.list_groups(crop) if normalize_variety_text(group.label) == normalized_value)
        return tuple(matches)

    def list_group_varieties(self, crop: str, group: str, subgroup: str) -> tuple[str, ...]:
        return self.resolve_group(crop, group, subgroup)
