from __future__ import annotations

import re
import sqlite3


class LegacyPersistenceRepository:
    """Consultas exclusivamente SELECT sobre las copias de Perceco."""
    def __init__(self, conn: sqlite3.Connection, schema: str = "eepp") -> None:
        self.conn, self.schema = conn, schema

    def max_liquidation_id(self, pattern_prefix: str) -> str | None:
        # La validación Python descarta ids de forma distinta al formato confirmado.
        sql=f"SELECT IdLiq FROM {self.schema}.DLiquidaciones WHERE IdLiq LIKE ?"
        try: rows=self.conn.execute(sql,(pattern_prefix+"%",)).fetchall()
        except sqlite3.OperationalError: rows=self.conn.execute(sql.replace(f"{self.schema}.",""),(pattern_prefix+"%",)).fetchall()
        rx=re.compile(re.escape(pattern_prefix)+r"\d{4}$")
        valid=[str(r[0]) for r in rows if r[0] is not None and rx.fullmatch(str(r[0]))]
        return max(valid, key=lambda value:int(value[-4:]), default=None)

    def member_name(self, member_id: int) -> str | None:
        for name_col in ("Socio", "Nombre", "NombreSocio"):
            try:
                row=self.conn.execute(f"SELECT {name_col} FROM {self.schema}.DSocio WHERE IdSocio=?",(member_id,)).fetchone()
                if row: return str(row[0] or "").strip()
            except sqlite3.OperationalError: continue
        return None

    def article_code(self, crop: str, variety: str, aliases: dict[str,str] | None=None) -> int | None:
        compatible=(aliases or {}).get(crop.upper(),crop).upper()
        sql=f"SELECT ARTICULO FROM {self.schema}.MVariedad WHERE UPPER(TRIM(CULTIVO))=? AND UPPER(TRIM(Variedad))=?"
        try: row=self.conn.execute(sql,(compatible,variety.strip().upper())).fetchone()
        except sqlite3.OperationalError: row=self.conn.execute(sql.replace(f"{self.schema}.",""),(compatible,variety.strip().upper())).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def historical_split_rows(self):
        sql=f"SELECT * FROM {self.schema}.DDividirLiq"
        try: return self.conn.execute(sql).fetchall()
        except sqlite3.OperationalError: return self.conn.execute(sql.replace(f"{self.schema}.","")).fetchall()
