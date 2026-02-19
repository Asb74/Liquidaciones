"""Normalización de correspondencias de calibres a grupos económicos."""

from __future__ import annotations

import pandas as pd

from .config import CALIBRES


def build_calibre_mapping(correspondencias_df: pd.DataFrame) -> pd.DataFrame:
    """Construye mapping Cal0..Cal11 -> grupo económico AAA/AA/A usando columna KAKIS."""
    normalized = correspondencias_df.copy()
    normalized["BASE"] = normalized["BASE"].astype(str).str.strip().str.upper()
    normalized["grupo"] = normalized["KAKIS"].astype(str).str.strip().str.upper()
    normalized = normalized[["BASE", "grupo"]].dropna()

    cal_df = pd.DataFrame({"calibre": CALIBRES})
    cal_df["BASE"] = [f"C{i}" for i in range(12)]

    mapped = cal_df.merge(normalized, on="BASE", how="left", validate="m:1")
    return mapped[["calibre", "grupo"]]
