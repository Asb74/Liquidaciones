from __future__ import annotations

import sqlite3
from datetime import date


SPECIAL_ECONOMIC_CROPS = {"DIRECTO", "DIRECTOCHF", "INDUSTRIA"}


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

    def variedades(self, campana: str, empresa: str, cultivo: str, desde: date | None = None, hasta: date | None = None) -> list[str]:
        if (cultivo or "").strip().upper() in SPECIAL_ECONOMIC_CROPS:
            return self._variedades_entregas(campana, empresa, cultivo, desde, hasta)
        sql = 'SELECT DISTINCT Variedad FROM eepp.DEEPP WHERE CAMPAÑA=? AND EMPRESA=? AND CULTIVO=? AND Variedad IS NOT NULL AND TRIM(Variedad) <> "" ORDER BY Variedad'
        return [str(r[0]) for r in self.conn.execute(sql, (campana, empresa, cultivo))]

    def _variedades_entregas(self, campana: str, empresa: str, cultivo: str, desde: date | None, hasta: date | None) -> list[str]:
        tables = ["PesosFres"]
        if self.table_exists("PesosFresCon") and "Variedad" in self.columns("PesosFresCon"):
            tables.append("PesosFresCon")
        selects: list[str] = []
        params: list[object] = []
        for table in tables:
            where = ['CAMPAÑA=?', 'EMPRESA=?', 'CULTIVO=?']
            table_params: list[object] = [campana, empresa, cultivo]
            if desde and hasta and "Fcarga" in self.columns(table):
                where.append('date(substr(Fcarga, 1, 10)) BETWEEN date(?) AND date(?)')
                table_params.extend([desde.isoformat(), hasta.isoformat()])
            selects.append(f"SELECT Variedad FROM {table} WHERE " + " AND ".join(where))
            params.extend(table_params)
        sql = "SELECT DISTINCT Variedad FROM (" + " UNION ".join(selects) + ") WHERE Variedad IS NOT NULL AND TRIM(Variedad) <> '' ORDER BY Variedad"
        return [str(r[0]) for r in self.conn.execute(sql, params)]


    def surface_crops_for_hectare_master(self) -> list[str]:
        return [str(r[0]).strip().upper() for r in self.conn.execute("SELECT DISTINCT CULTIVO FROM eepp.DEEPP WHERE CULTIVO IS NOT NULL AND TRIM(CULTIVO) <> '' ORDER BY UPPER(TRIM(CULTIVO))")]

    def delivery_crops_for_hectare_master(self) -> list[str]:
        return [str(r[0]).strip().upper() for r in self.conn.execute("SELECT DISTINCT CULTIVO FROM PesosFres WHERE CULTIVO IS NOT NULL AND TRIM(CULTIVO) <> '' ORDER BY UPPER(TRIM(CULTIVO))")]
