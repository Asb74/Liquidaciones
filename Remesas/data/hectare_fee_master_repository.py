from __future__ import annotations

import sqlite3


class HectareFeeCropRepository:
    """Read-only queries for crop options used by the hectare fee master."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def list_surface_crop_options(self) -> list[str]:
        sql = """
        SELECT DISTINCT
            UPPER(TRIM(CULTIVO)) AS CULTIVO
        FROM eepp.DEEPP
        WHERE CULTIVO IS NOT NULL
          AND TRIM(CULTIVO) <> ''
        ORDER BY UPPER(TRIM(CULTIVO))
        """
        return [str(row[0]) for row in self.conn.execute(sql)]

    def list_delivery_crop_options(self) -> list[str]:
        sql = """
        SELECT DISTINCT
            UPPER(TRIM(CULTIVO)) AS CULTIVO
        FROM PesosFres
        WHERE CULTIVO IS NOT NULL
          AND TRIM(CULTIVO) <> ''
        ORDER BY UPPER(TRIM(CULTIVO))
        """
        return [str(row[0]) for row in self.conn.execute(sql)]
