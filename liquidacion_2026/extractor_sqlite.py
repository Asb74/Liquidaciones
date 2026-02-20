"""Extracción de datos desde SQLite para liquidación KAKIS."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing

import pandas as pd

from .config import CALIBRES, DESTRIOS
from .utils import parse_decimal

LOGGER = logging.getLogger(__name__)


class SQLiteExtractorError(RuntimeError):
    """Error de extracción de datos."""


class SQLiteExtractor:
    def __init__(self, fruta_db: str, calidad_db: str, eeppl_db: str) -> None:
        self.fruta_db = fruta_db
        self.calidad_db = calidad_db
        self.eeppl_db = eeppl_db

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
            FROM PesosFres
            WHERE CAMPAÑA = ? AND EMPRESA = ? AND CULTIVO = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, empresa, cultivo))
        df.columns = df.columns.str.strip().str.lower()
        if df.empty:
            raise SQLiteExtractorError("No hay datos en PesosFres para los filtros indicados.")

        for col in [*CALIBRES, *DESTRIOS]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["kilos_comerciales_float"] = df[CALIBRES].sum(axis=1)
        df["kilos_comerciales"] = df["kilos_comerciales_float"].map(parse_decimal)
        df["kilos_total"] = df.apply(
            lambda row: row["kilos_comerciales"] + parse_decimal(row["deslinea"]) + parse_decimal(row["desmesa"]) + parse_decimal(row["podrido"]),
            axis=1,
        )

        df["semana"] = pd.to_numeric(df["apodo"], errors="coerce").astype("Int64")
        invalid_mask = df["semana"].isna()
        if invalid_mask.any():
            invalid_rows = df.loc[invalid_mask, ["apodo", "boleta"]].head(5).to_dict("records")
            raise SQLiteExtractorError(
                "Semana inválida: "
                f"{int(invalid_mask.sum())} filas tienen apodo no numérico en PesosFres. "
                f"Ejemplos: {invalid_rows}"
            )
        return df

    def fetch_correspondencias_calibres(self) -> pd.DataFrame:
        return self._read_sql(self.calidad_db, "SELECT BASE, KAKIS FROM CorrespondenciasCalibres")

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
            FROM DEEPP
            """,
        )
        df.columns = df.columns.str.strip().str.lower()
        return df

    def fetch_mnivel_global(self) -> pd.DataFrame:
        df = self._read_sql(self.eeppl_db, "SELECT Nivel AS nivel, Indice AS indice FROM MNivelGlobal")
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
            FROM BonGlobal
            WHERE CAMPAÑA = ? AND CULTIVO = ? AND EMPRESA = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, cultivo, empresa))
        if df.empty:
            raise SQLiteExtractorError("No existe registro en BonGlobal para campaña/cultivo/empresa.")
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

    @staticmethod
    def _read_sql(db_path: str, query: str, params: tuple | None = None) -> pd.DataFrame:
        LOGGER.info("Abriendo SQLite en ruta: %s", db_path)
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                return pd.read_sql_query(query, conn, params=params)
        except sqlite3.Error as exc:
            raise SQLiteExtractorError(f"Error SQLite en {db_path}: {exc}") from exc

    @staticmethod
    def _table_columns(db_path: str, table: str) -> set[str]:
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.Error as exc:
            raise SQLiteExtractorError(f"Error leyendo esquema de tabla {table} en {db_path}: {exc}") from exc
        return {str(row[1]).upper() for row in rows}
