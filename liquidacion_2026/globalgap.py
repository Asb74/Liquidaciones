"""Cálculo de Fondo GlobalGAP por socio."""

from __future__ import annotations

from decimal import Decimal
import logging

import pandas as pd

from .config import CALIBRES
from .utils import parse_decimal


LOGGER = logging.getLogger(__name__)


def calcular_fondo_globalgap(
    pesos_df: pd.DataFrame,
    deepp_df: pd.DataFrame,
    mnivel_df: pd.DataFrame,
    bon_global_df: pd.DataFrame,
) -> tuple[Decimal, pd.DataFrame, pd.DataFrame]:
    pesos_df = pesos_df.copy()
    deepp_df = deepp_df.copy()
    mnivel_df = mnivel_df.copy()

    pesos_df.columns = pesos_df.columns.str.strip().str.lower()
    deepp_df.columns = deepp_df.columns.str.strip().str.lower()
    mnivel_df.columns = mnivel_df.columns.str.strip().str.lower()

    calibres_cols = CALIBRES
    if "kg_comercial" not in pesos_df.columns:
        pesos_df["kg_comercial"] = pesos_df[calibres_cols].sum(axis=1)

    bon_base = parse_decimal(bon_global_df["bonificacion"].iloc[0])

    kilos_socio = pesos_df.groupby("idsocio", as_index=False).agg({"kg_comercial": "sum"})
    kilos_socio = kilos_socio.rename(columns={"kg_comercial": "kilos_gg"})

    deepp_unique = deepp_df.drop_duplicates(subset=["idsocio"])[["idsocio", "nivelglobal"]]

    merged = kilos_socio.merge(
        deepp_unique,
        on="idsocio",
        how="left",
        validate="m:1",
    )
    merged = merged.merge(
        mnivel_df[["nivel", "indice"]],
        left_on="nivelglobal",
        right_on="nivel",
        how="left",
        validate="m:1",
    )
    merged["indice"] = pd.to_numeric(merged["indice"], errors="coerce")

    audit_rows: list[dict[str, object]] = []

    def resolve_indice(row: pd.Series) -> Decimal:
        if pd.isna(row.get("nivelglobal")):
            audit_rows.append({"boleta": row["idsocio"], "motivo": "boleta_sin_deepp", "nivelglobal": "", "indice_asignado": 0})
            return Decimal("0")
        if pd.isna(row.get("indice")):
            audit_rows.append(
                {
                    "boleta": row["idsocio"],
                    "motivo": "nivel_sin_indice",
                    "nivelglobal": row.get("nivelglobal", ""),
                    "indice_asignado": 0,
                }
            )
            return Decimal("0")
        return parse_decimal(row["indice"])

    merged["indice_decimal"] = merged.apply(resolve_indice, axis=1)
    merged["bonif_eur_kg"] = bon_base
    merged["fondo_gg_eur"] = merged.apply(
        lambda r: parse_decimal(r["kilos_gg"]) * r["bonif_eur_kg"] * r["indice_decimal"], axis=1
    )

    total = sum(merged["fondo_gg_eur"], Decimal("0"))
    audit_inc_df = pd.DataFrame(audit_rows).drop_duplicates() if audit_rows else pd.DataFrame(
        columns=["boleta", "motivo", "nivelglobal", "indice_asignado"]
    )

    audit_globalgap_socios_df = merged[
        ["idsocio", "kilos_gg", "nivelglobal", "indice_decimal", "bonif_eur_kg", "fondo_gg_eur"]
    ].rename(columns={"indice_decimal": "indice"})
    LOGGER.info("Total kilos GG (comerciales): %s", sum(audit_globalgap_socios_df["kilos_gg"], Decimal("0")))
    LOGGER.info("Fondo GG total: %s", total)

    return total, audit_globalgap_socios_df, audit_inc_df
