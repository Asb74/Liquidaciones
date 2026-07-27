from __future__ import annotations

import re
import sqlite3
import logging


logger = logging.getLogger(__name__)


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

    def member_is_self_billed(self, member_id: int) -> bool:
        """Return whether the legacy member must be omitted from accounting CSVs.

        Do not turn database errors into a false answer: exporting an incomplete
        accounting file is worse than stopping the operation.
        """
        sql = f"SELECT FacSoc FROM {self.schema}.DSocio WHERE IdSocio=?"
        try:
            row = self.conn.execute(sql, (member_id,)).fetchone()
        except sqlite3.OperationalError:
            row = self.conn.execute(sql.replace(f"{self.schema}.", ""), (member_id,)).fetchone()
        if row is None:
            return False
        return str(row[0] or "").strip().upper() == "SI"

    def article_code(self, crop: str, variety: str, aliases: dict[str,str] | None=None) -> str | None:
        normalized_crop=crop.strip().upper()
        normalized_aliases = {str(key).strip().upper(): str(value).strip().upper()
                              for key, value in (aliases or {}).items()}
        compatible=normalized_aliases.get(normalized_crop,normalized_crop)
        normalized_variety=variety.strip().upper()
        sql=f"SELECT ARTICULO FROM {self.schema}.MVariedad WHERE UPPER(TRIM(CULTIVO))=? AND UPPER(TRIM(Variedad))=?"
        try: row=self.conn.execute(sql,(compatible,normalized_variety)).fetchone()
        except sqlite3.OperationalError: row=self.conn.execute(sql.replace(f"{self.schema}.",""),(compatible,normalized_variety)).fetchone()
        if not row or row[0] is None:
            logger.warning("[MVariedadArticulo]\ncrop=%s\nvariety=%s\narticle=\nstatus=not_found", compatible, normalized_variety)
            return None
        # ARTICULO is a business identifier, not a quantity: legacy data contains
        # both numeric codes (for example 3984) and alphanumeric ones (B391).
        code = str(row[0]).strip()
        if not code:
            logger.warning("[MVariedadArticulo]\ncrop=%s\nvariety=%s\narticle=\nsource=MVariedades\nstatus=not_found", compatible, normalized_variety)
            return None
        logger.info("[MVariedadArticulo]\ncrop=%s\nvariety=%s\narticle=%s\nsource=MVariedades\nstatus=resolved", compatible, normalized_variety, code)
        return code

    def historical_split_rows(self):
        sql=f"SELECT * FROM {self.schema}.DDividirLiq"
        try: return self.conn.execute(sql).fetchall()
        except sqlite3.OperationalError: return self.conn.execute(sql.replace(f"{self.schema}.","")).fetchall()
