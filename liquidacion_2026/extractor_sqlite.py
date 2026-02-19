"""Extracción de datos desde SQLite."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pandas as pd

from .config import CALIBRES, COLUMNS_KILOS, DESTRIOS


class SQLiteExtractorError(RuntimeError):
    """Error de extracción de datos."""


class SQLiteExtractor:
    """Encapsula consultas hacia las BDs del proceso."""

    def __init__(self, fruta_db: str, calidad_db: str, eeppl_db: str) -> None:
        self.fruta_db = fruta_db
        self.calidad_db = calidad_db
        self.eeppl_db = eeppl_db

    def fetch_pesosfres(self, campana: int, empresa: str, cultivo: str) -> pd.DataFrame:
        cols = ["CAMPAÑA", "EMPRESA", "CULTIVO", "Neto", "NetoPartida", "Apodo", "Boleta", *COLUMNS_KILOS]
        query = f"""
            SELECT {', '.join(cols)}
            FROM PesosFres
            WHERE CAMPAÑA = ? AND EMPRESA = ? AND CULTIVO = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, empresa, cultivo))
        if df.empty:
            raise SQLiteExtractorError("No se encontraron registros en PesosFres con los filtros indicados.")

        for col in ["Neto", "NetoPartida", *COLUMNS_KILOS]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["semana"] = pd.to_numeric(df["Apodo"], errors="coerce")
        if df["semana"].isna().any():
            raise SQLiteExtractorError("Existen valores de Apodo no numéricos; no se puede determinar semana.")

        df["kilos_base"] = df.apply(
            lambda r: r["Neto"] if r["NetoPartida"] == 0 else r["NetoPartida"],
            axis=1,
        )
        return df

    def fetch_correspondencias_calibres(self) -> pd.DataFrame:
        query = "SELECT BASE, KAKIS FROM CorrespondenciasCalibres"
        return self._read_sql(self.calidad_db, query)

    def fetch_deepp(self) -> pd.DataFrame:
        query = "SELECT Boleta, NivelGlobal FROM DEEPP"
        return self._read_sql(self.eeppl_db, query)

    def fetch_mnivel_global(self) -> pd.DataFrame:
        query = "SELECT Nivel, Indice FROM MNivelGlobal"
        df = self._read_sql(self.eeppl_db, query)
        if not df.empty:
            df["Indice"] = pd.to_numeric(df["Indice"], errors="coerce")
        return df

    def fetch_bon_global(self, campana: int, cultivo: str, empresa: str) -> pd.DataFrame:
        query = """
            SELECT CAMPAÑA, CULTIVO, EMPRESA, Bonificacion
            FROM BonGlobal
            WHERE CAMPAÑA = ? AND CULTIVO = ? AND EMPRESA = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, cultivo, empresa))
        if not df.empty:
            df["Bonificacion"] = pd.to_numeric(df["Bonificacion"], errors="coerce").fillna(0)
        return df

    @staticmethod
    def _read_sql(db_path: str, query: str, params: tuple | None = None) -> pd.DataFrame:
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                return pd.read_sql_query(query, conn, params=params)
        except sqlite3.Error as exc:
            raise SQLiteExtractorError(f"Error SQLite en {db_path}: {exc}") from exc
