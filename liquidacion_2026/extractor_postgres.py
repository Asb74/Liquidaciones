"""Extracción de datos desde PostgreSQL para liquidación KAKIS."""

from __future__ import annotations

import logging
from Remesas.data.postgres_repository import PostgresRepository

import pandas as pd

from .config import CALIBRES, DESTRIOS
from .utils import parse_decimal

LOGGER = logging.getLogger(__name__)


class PostgresExtractorError(RuntimeError):
    """Error de extracción de datos."""


class PostgresExtractor:
    def __init__(self, fruta_db: str, calidad_db: str, eeppl_db: str) -> None:
        self.repository = PostgresRepository()
        self.fruta_db = "legacy_dbfruta"
        self.calidad_db = "legacy_calidad"
        self.eeppl_db = "legacy_eepp"

    def fetch_pesosfres(self, campana: int, empresa: int, cultivo: str) -> pd.DataFrame:
        cal_select = [f"Cal{i} AS cal{i}" for i in range(12)]
        query = f"""
            SELECT
                CAMPAÑA AS campaña,
                EMPRESA AS empresa,
                CULTIVO AS cultivo,
                Apodo AS apodo,
                Boleta AS boleta,
                IDSocio AS idsocio,
                {', '.join(cal_select)},
                DesLinea AS deslinea,
                DesMesa AS desmesa,
                Podrido AS podrido
            FROM legacy_dbfruta.PesosFres
            WHERE CAMPAÑA = ? AND EMPRESA = ? AND CULTIVO = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, empresa, cultivo))
        df.columns = df.columns.str.strip().str.lower()
        if df.empty:
            raise PostgresExtractorError("No hay datos en PesosFres para los filtros indicados.")

        for col in [*CALIBRES, *DESTRIOS]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["kilos_comerciales"] = df[CALIBRES].sum(axis=1)

        df["semana"] = pd.to_numeric(df["apodo"], errors="coerce").astype("Int64")
        invalid_mask = df["semana"].isna()
        if invalid_mask.any():
            invalid_rows = df.loc[invalid_mask, ["apodo", "boleta"]].head(5).to_dict("records")
            raise PostgresExtractorError(
                "Semana inválida: "
                f"{int(invalid_mask.sum())} filas tienen apodo no numérico en PesosFres. "
                f"Ejemplos: {invalid_rows}"
            )
        return df

    def fetch_correspondencias_calibres(self) -> pd.DataFrame:
        return self._read_sql(self.calidad_db, "SELECT BASE, KAKIS FROM legacy_calidad.CorrespondenciasCalibres")

    def fetch_deepp(self) -> pd.DataFrame:
        df = self._read_sql(
            self.eeppl_db,
            """
            SELECT
                Boleta AS boleta,
                IDSocio AS idsocio,
                Certificacion AS certificacion,
                NivelGlobal AS nivelglobal,
                CAMPAÑA AS campaña,
                CULTIVO AS cultivo,
                EMPRESA AS empresa
            FROM legacy_eepp.DEEPP
            """,
        )
        df.columns = df.columns.str.strip().str.lower()
        return df

    def fetch_mnivel_global(self) -> pd.DataFrame:
        df = self._read_sql(self.eeppl_db, "SELECT Nivel AS nivel, Indice AS indice FROM legacy_eepp.MNivelGlobal")
        df.columns = df.columns.str.strip().str.lower()
        if not df.empty:
            df["indice"] = df["indice"].map(parse_decimal)
        return df

    def fetch_bon_global(self, campana: int, cultivo: str, empresa: int) -> pd.DataFrame:
        cols = self._table_columns(self.fruta_db, "BonGlobal")
        has_categoria = "CATEGORIA" in cols
        categoria_sql = "CATEGORIA AS categoria," if has_categoria else "'' AS categoria,"

        query = f"""
            SELECT
                CAMPAÑA AS campaña,
                CULTIVO AS cultivo,
                EMPRESA AS empresa,
                {categoria_sql}
                Bonificacion AS bonificacion
            FROM legacy_dbfruta.BonGlobal
            WHERE CAMPAÑA = ? AND CULTIVO = ? AND EMPRESA = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, cultivo, empresa))
        if df.empty:
            raise PostgresExtractorError("No existe registro en BonGlobal para campaña/cultivo/empresa.")
        df.columns = df.columns.str.strip().str.lower()
        df["bonificacion"] = df["bonificacion"].map(parse_decimal)
        if len(df) > 1:
            LOGGER.info(
                "BonGlobal devolvió %s filas para campaña=%s cultivo=%s empresa=%s (posibles categorías múltiples).",
                len(df),
                campana,
                cultivo,
                empresa,
            )
        return df

    def _read_sql(self, schema: str, query: str, params: tuple | None = None) -> pd.DataFrame:
        try:
            with self.repository.connect() as repository:
                cursor = repository.execute(query, params)
                rows = cursor.fetchall()
                columns = [item.name for item in cursor.description]
                return pd.DataFrame(rows, columns=columns)
        except Exception as exc:
            raise PostgresExtractorError(f"Error consultando PostgreSQL: {exc}") from exc

    def _table_columns(self, schema: str, table: str) -> set[str]:
        with self.repository.connect() as repository:
            rows = repository.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=? AND lower(table_name)=lower(?)",
                (schema, table),
            ).fetchall()
        return {str(row[0]).upper() for row in rows}
