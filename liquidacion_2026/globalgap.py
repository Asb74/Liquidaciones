"""Cálculos del fondo GlobalGAP."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd

from .config import CALIBRES


def calcular_bonificacion_globalgap(
    pesos_df: pd.DataFrame,
    deepp_df: pd.DataFrame,
    mnivel_df: pd.DataFrame,
    bon_global_df: pd.DataFrame,
    audit_path: Path,
) -> tuple[pd.DataFrame, Decimal]:
    """Calcula fondo globalgap total y devuelve detalle por boleta."""
    bonificacion_base = Decimal("0")
    if not bon_global_df.empty:
        bonificacion_base = Decimal(str(bon_global_df["Bonificacion"].iloc[0]))

    pesos_boleta = pesos_df[["Boleta", *CALIBRES]].copy()
    pesos_boleta["kilos_bonificables"] = pesos_boleta[CALIBRES].sum(axis=1)

    merged = pesos_boleta.merge(deepp_df, on="Boleta", how="left", validate="m:1")
    missing_nivel = merged[merged["NivelGlobal"].isna()].copy()
    if not missing_nivel.empty:
        missing_nivel[["Boleta"]].drop_duplicates().to_csv(audit_path, index=False)

    with_indice = merged.merge(
        mnivel_df,
        left_on="NivelGlobal",
        right_on="Nivel",
        how="left",
        validate="m:1",
    )
    with_indice["Indice"] = pd.to_numeric(with_indice["Indice"], errors="coerce").fillna(0)

    with_indice["bonificacion_real_kg"] = with_indice["Indice"].map(
        lambda x: Decimal(str(x)) * bonificacion_base
    )
    with_indice["fondo_boleta"] = with_indice.apply(
        lambda row: Decimal(str(row["kilos_bonificables"])) * row["bonificacion_real_kg"],
        axis=1,
    )

    total_fondo = sum(with_indice["fondo_boleta"], Decimal("0"))
    return with_indice, total_fondo
