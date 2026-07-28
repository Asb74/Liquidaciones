from __future__ import annotations

import re
from data.repository import IDataRepository
import logging
import threading
from contextlib import contextmanager
from collections.abc import Callable


logger = logging.getLogger(__name__)


class PostgresLegacyRepository:
    """Consultas exclusivamente SELECT sobre las esquemas legacy de PostgreSQL."""
    def __init__(self, conn: IDataRepository | Callable[[], IDataRepository], schema: str = "eepp") -> None:
        self._connection_source, self.schema = conn, schema

    @contextmanager
    def _connect(self):
        """Use a fresh connection for factory-backed (cross-thread) repositories."""
        if not isinstance(self._connection_source, IDataRepository):
            connection = self._connection_source()
            database = "legacy"
            try:
                database = connection.execute("PRAGMA database_list").fetchone()[2] or database
            except Exception:
                pass
            logger.info("[PostgreSQLConnection] database=%s thread_id=%s action=OPENED", database, threading.get_ident())
            try:
                yield connection
            finally:
                connection.close()
                logger.info("[PostgreSQLConnection] database=%s thread_id=%s action=CLOSED", database, threading.get_ident())
        else:
            # Backwards compatibility for short-lived, same-thread persistence jobs.
            yield self._connection_source

    def max_liquidation_id(self, pattern_prefix: str) -> str | None:
        # La validación Python descarta ids de forma distinta al formato confirmado.
        sql=f"SELECT IdLiq FROM {self.schema}.DLiquidaciones WHERE IdLiq LIKE ?"
        with self._connect() as conn:
            try: rows=conn.execute(sql,(pattern_prefix+"%",)).fetchall()
            except Exception: rows=conn.execute(sql.replace(f"{self.schema}.",""),(pattern_prefix+"%",)).fetchall()
        rx=re.compile(re.escape(pattern_prefix)+r"\d{4}$")
        valid=[str(r[0]) for r in rows if r[0] is not None and rx.fullmatch(str(r[0]))]
        return max(valid, key=lambda value:int(value[-4:]), default=None)

    def member_name(self, member_id: int) -> str | None:
        with self._connect() as conn:
            for name_col in ("Socio", "Nombre", "NombreSocio"):
                try:
                    row=conn.execute(f"SELECT {name_col} FROM {self.schema}.DSocio WHERE IdSocio=?",(member_id,)).fetchone()
                    if row: return str(row[0] or "").strip()
                except Exception: continue
        return None

    def member_is_self_billed(self, member_id: int) -> bool:
        """Return whether the legacy member must be omitted from accounting CSVs.

        Do not turn database errors into a false answer: exporting an incomplete
        accounting file is worse than stopping the operation.
        """
        sql = f"SELECT FacSoc FROM {self.schema}.DSocio WHERE IdSocio=?"
        try:
            with self._connect() as conn:
                try:
                    row = conn.execute(sql, (member_id,)).fetchone()
                except Exception:
                    row = conn.execute(sql.replace(f"{self.schema}.", ""), (member_id,)).fetchone()
            logger.info("[FacSocCheck] member_id=%s thread_id=%s status=OK", member_id, threading.get_ident())
        except Exception:
            logger.exception("[FacSocCheck] member_id=%s thread_id=%s status=FAILED", member_id, threading.get_ident())
            raise
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
        with self._connect() as conn:
            try: row=conn.execute(sql,(compatible,normalized_variety)).fetchone()
            except Exception: row=conn.execute(sql.replace(f"{self.schema}.",""),(compatible,normalized_variety)).fetchone()
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
        with self._connect() as conn:
            try: return conn.execute(sql).fetchall()
            except Exception: return conn.execute(sql.replace(f"{self.schema}.","")).fetchall()
