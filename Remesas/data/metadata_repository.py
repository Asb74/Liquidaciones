from __future__ import annotations

import sqlite3


class MetadataRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def table_exists(self, table: str, schema: str = "main") -> bool:
        sql = f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?"
        return self.conn.execute(sql, (table,)).fetchone() is not None

    def columns(self, table: str, schema: str = "main") -> set[str]:
        return {row[1] for row in self.conn.execute(f"PRAGMA {schema}.table_info({table})")}

    def campaigns(self) -> list[str]:
        return [str(r[0]) for r in self.conn.execute('SELECT DISTINCT CAMPAÑA FROM PesosFres WHERE CAMPAÑA IS NOT NULL ORDER BY CAMPAÑA DESC')]

    def empresas(self, campana: str) -> list[str]:
        return [str(r[0]) for r in self.conn.execute('SELECT DISTINCT EMPRESA FROM PesosFres WHERE CAMPAÑA=? AND EMPRESA IS NOT NULL ORDER BY EMPRESA', (campana,))]

    def cultivos(self, campana: str, empresa: str) -> list[str]:
        return [str(r[0]) for r in self.conn.execute('SELECT DISTINCT CULTIVO FROM PesosFres WHERE CAMPAÑA=? AND EMPRESA=? AND CULTIVO IS NOT NULL ORDER BY CULTIVO', (campana, empresa))]

    def variedades(self, campana: str, empresa: str, cultivo: str) -> list[str]:
        sql = 'SELECT DISTINCT Variedad FROM eepp.DEEPP WHERE CAMPAÑA=? AND EMPRESA=? AND CULTIVO=? AND Variedad IS NOT NULL AND TRIM(Variedad) <> "" ORDER BY Variedad'
        return [str(r[0]) for r in self.conn.execute(sql, (campana, empresa, cultivo))]
