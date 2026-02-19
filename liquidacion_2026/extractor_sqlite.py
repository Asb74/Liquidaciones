"""Extracción de datos desde SQLite para liquidación KAKIS."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pandas as pd

from .config import CALIBRES, DESTRIOS


class SQLiteExtractorError(RuntimeError):
    """Error de extracción de datos."""


class SQLiteExtractor:
    def __init__(self, fruta_db: str, calidad_db: str, eeppl_db: str) -> None:
        self.fruta_db = fruta_db
        self.calidad_db = calidad_db
        self.eeppl_db = eeppl_db

    def fetch_pesosfres(self, campana: int, empresa: int, cultivo: str) -> pd.DataFrame:
        cols = ["CAMPAÑA", "EMPRESA", "CULTIVO", "Apodo", "Boleta", "IDSocio", *CALIBRES, *DESTRIOS]
        query = f"""
            SELECT {', '.join(cols)}
            FROM PesosFres
            WHERE CAMPAÑA = ? AND EMPRESA = ? AND CULTIVO = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, empresa, cultivo))
        if df.empty:
            raise SQLiteExtractorError("No hay datos en PesosFres para los filtros indicados.")

        for col in [*CALIBRES, *DESTRIOS]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["semana"] = pd.to_numeric(df["Apodo"], errors="coerce").astype("Int64")
        invalid_mask = df["semana"].isna()
        if invalid_mask.any():
            invalid_rows = df.loc[invalid_mask, ["Apodo", "Boleta"]].head(5).to_dict("records")
            raise SQLiteExtractorError(
                "Semana inválida: "
                f"{int(invalid_mask.sum())} filas tienen Apodo no numérico en PesosFres. "
                f"Ejemplos: {invalid_rows}"
            )
        return df

    def fetch_correspondencias_calibres(self) -> pd.DataFrame:
        return self._read_sql(self.calidad_db, "SELECT BASE, KAKIS FROM CorrespondenciasCalibres")

    def fetch_deepp(self) -> pd.DataFrame:
        return self._read_sql(self.eeppl_db, "SELECT Boleta, IDSocio, NivelGlobal FROM DEEPP")

    def fetch_mnivel_global(self) -> pd.DataFrame:
        df = self._read_sql(self.eeppl_db, "SELECT Nivel, Indice FROM MNivelGlobal")
        if not df.empty:
            df["Indice"] = pd.to_numeric(df["Indice"], errors="coerce").fillna(0)
        return df

    def fetch_bon_global(self, campana: int, cultivo: str, empresa: int) -> pd.DataFrame:
        query = """
            SELECT CAMPAÑA, CULTIVO, EMPRESA, Bonificacion
            FROM BonGlobal
            WHERE CAMPAÑA = ? AND CULTIVO = ? AND EMPRESA = ?
        """
        df = self._read_sql(self.fruta_db, query, (campana, cultivo, empresa))
        if df.empty:
            raise SQLiteExtractorError("No existe registro en BonGlobal para campaña/cultivo/empresa.")
        df["Bonificacion"] = pd.to_numeric(df["Bonificacion"], errors="coerce").fillna(0)
        return df

    @staticmethod
    def _read_sql(db_path: str, query: str, params: tuple | None = None) -> pd.DataFrame:
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                return pd.read_sql_query(query, conn, params=params)
        except sqlite3.Error as exc:
            raise SQLiteExtractorError(f"Error SQLite en {db_path}: {exc}") from exc
