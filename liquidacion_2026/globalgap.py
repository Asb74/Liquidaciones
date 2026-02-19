"""Cálculo de Fondo GlobalGAP por boleta."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from .config import CALIBRES


def calcular_fondo_globalgap(
    pesos_df: pd.DataFrame,
    deepp_df: pd.DataFrame,
    mnivel_df: pd.DataFrame,
    bon_global_df: pd.DataFrame,
) -> tuple[Decimal, pd.DataFrame]:
    bon_base = Decimal(str(bon_global_df["Bonificacion"].iloc[0]))

    kilos_boleta = pesos_df[["Boleta", *CALIBRES]].copy()
    kilos_boleta["kilos_bonificables"] = kilos_boleta[CALIBRES].sum(axis=1)

    merged = kilos_boleta.merge(deepp_df, on="Boleta", how="left", validate="m:1")
    merged = merged.merge(mnivel_df, left_on="NivelGlobal", right_on="Nivel", how="left", validate="m:1")
    merged["Indice"] = pd.to_numeric(merged["Indice"], errors="coerce")

    audit_rows: list[dict[str, object]] = []

    def resolve_indice(row: pd.Series) -> Decimal:
        if pd.isna(row.get("NivelGlobal")):
            audit_rows.append({"boleta": row["Boleta"], "motivo": "boleta_sin_deepp", "nivelglobal": "", "indice_asignado": 0})
            return Decimal("0")
        if pd.isna(row.get("Indice")):
            audit_rows.append(
                {
                    "boleta": row["Boleta"],
                    "motivo": "nivel_sin_indice",
                    "nivelglobal": row.get("NivelGlobal", ""),
                    "indice_asignado": 0,
                }
            )
            return Decimal("0")
        return Decimal(str(row["Indice"]))

    merged["indice_decimal"] = merged.apply(resolve_indice, axis=1)
    merged["fondo_boleta"] = merged.apply(
        lambda r: Decimal(str(r["kilos_bonificables"])) * bon_base * r["indice_decimal"], axis=1
    )

    total = sum(merged["fondo_boleta"], Decimal("0"))
    audit_df = pd.DataFrame(audit_rows).drop_duplicates() if audit_rows else pd.DataFrame(
        columns=["boleta", "motivo", "nivelglobal", "indice_asignado"]
    )
    return total, audit_df
