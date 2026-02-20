"""Cálculo de Fondo GlobalGAP por socio."""

from __future__ import annotations

from decimal import Decimal
import logging

import pandas as pd

from .config import CALIBRES
from .utils import parse_decimal


LOGGER = logging.getLogger(__name__)


def _first_non_empty(values: pd.Series) -> str:
    for value in values:
        normalized = str(value).strip()
        if normalized:
            return normalized
    return ""


def calcular_fondo_globalgap(
    pesos_df: pd.DataFrame,
    deepp_df: pd.DataFrame,
    mnivel_df: pd.DataFrame,
    bon_global_df: pd.DataFrame,
) -> tuple[Decimal, pd.DataFrame, pd.DataFrame]:
    pesos_df = pesos_df.copy()
    deepp_df = deepp_df.copy()
    mnivel_df = mnivel_df.copy()
    bon_global_df = bon_global_df.copy()

    pesos_df.columns = pesos_df.columns.str.strip().str.lower()
    deepp_df.columns = deepp_df.columns.str.strip().str.lower()
    mnivel_df.columns = mnivel_df.columns.str.strip().str.lower()
    bon_global_df.columns = bon_global_df.columns.str.strip().str.lower()

    for col in CALIBRES:
        pesos_df[col] = pesos_df[col].map(parse_decimal)

    pesos_df["kilos_comerciales"] = pesos_df[CALIBRES].sum(axis=1)
    pesos_df["idsocio"] = pesos_df["idsocio"].astype(str).str.strip()

    kilos_socios_df = pesos_df.groupby("idsocio", as_index=False)["kilos_comerciales"].sum()

    deepp_df["idsocio"] = deepp_df["idsocio"].astype(str).str.strip()
    deepp_df["boleta"] = deepp_df["boleta"].astype(str).str.strip()
    deepp_df["certificacion"] = deepp_df["certificacion"].fillna("").astype(str).str.strip().str.upper()
    deepp_gg = deepp_df[deepp_df["certificacion"] == "GLOBAL GAP"].copy()

    inconsistencias = (
        deepp_gg.assign(nivelglobal_norm=deepp_gg["nivelglobal"].fillna("").astype(str).str.strip())
        .groupby("idsocio")["nivelglobal_norm"]
        .nunique()
    )
    socios_niveles_inconsistentes = inconsistencias[inconsistencias > 1].index.tolist()
    if socios_niveles_inconsistentes:
        LOGGER.info("Socios GG con más de un NivelGlobal en DEEPP: %s", socios_niveles_inconsistentes)

    deepp_socios = (
        deepp_gg.groupby("idsocio", as_index=False)
        .agg(
            nivelglobal=("nivelglobal", _first_non_empty),
            boleta_ref=("boleta", _first_non_empty),
        )
    )

    mnivel_df["nivel"] = mnivel_df["nivel"].fillna("").astype(str).str.strip()
    mnivel_df["indice"] = mnivel_df["indice"].map(parse_decimal)

    merged_all = kilos_socios_df.merge(deepp_socios, on="idsocio", how="left")
    merged_all = merged_all.merge(mnivel_df[["nivel", "indice"]], left_on="nivelglobal", right_on="nivel", how="left")
    merged_all["indice"] = merged_all["indice"].map(parse_decimal)

    bonificaciones = bon_global_df["bonificacion"].map(parse_decimal)
    if bonificaciones.empty:
        bon_eur = Decimal("0")
    elif len(bonificaciones) > 1:
        LOGGER.info("BonGlobal tiene %s filas; se usa media de bonificación para GG.", len(bonificaciones))
        bon_eur = sum(bonificaciones, Decimal("0")) / Decimal(len(bonificaciones))
    else:
        bon_eur = bonificaciones.iloc[0]

    merged_all["bon_eur"] = bon_eur
    merged_all["fondo_eur"] = merged_all.apply(
        lambda row: parse_decimal(row["kilos_comerciales"]) * parse_decimal(row["bon_eur"]) * parse_decimal(row["indice"]),
        axis=1,
    )

    merged = merged_all[merged_all["idsocio"].isin(deepp_socios["idsocio"])].copy()
    fondo_total = sum(merged["fondo_eur"], Decimal("0"))

    boletas_pesos = set(pesos_df["boleta"].astype(str).str.strip())
    boletas_gg = set(deepp_gg["boleta"].astype(str).str.strip())
    boletas_no_match = sorted(boletas_pesos - boletas_gg)
    socios_no_match = sorted(set(kilos_socios_df["idsocio"]) - set(deepp_socios["idsocio"]))

    audit_rows: list[dict[str, str]] = []
    audit_rows.extend({"tipo": "boleta_no_match", "id": boleta, "detalle": "boleta en pesos sin GLOBAL GAP"} for boleta in boletas_no_match)
    audit_rows.extend({"tipo": "socio_no_match", "id": socio, "detalle": "socio en pesos sin GLOBAL GAP"} for socio in socios_no_match)
    audit_rows.extend(
        {"tipo": "socio_nivel_inconsistente", "id": socio, "detalle": "más de un nivelglobal en DEEPP"}
        for socio in socios_niveles_inconsistentes
    )
    audit_df = pd.DataFrame(audit_rows, columns=["tipo", "id", "detalle"])

    audit_globalgap_socios_df = merged[["idsocio", "nivelglobal", "indice", "bon_eur", "kilos_comerciales", "fondo_eur"]].copy()
    audit_globalgap_socios_df["kilos_comerciales"] = audit_globalgap_socios_df["kilos_comerciales"].map(parse_decimal)
    audit_globalgap_socios_df["indice"] = audit_globalgap_socios_df["indice"].map(parse_decimal)
    audit_globalgap_socios_df["bon_eur"] = audit_globalgap_socios_df["bon_eur"].map(parse_decimal)
    audit_globalgap_socios_df["fondo_eur"] = audit_globalgap_socios_df["fondo_eur"].map(parse_decimal)

    LOGGER.info("Socios GG considerados: %s", len(audit_globalgap_socios_df))
    LOGGER.info("Fondo GG total calculado: %s", fondo_total)

    return fondo_total, audit_globalgap_socios_df, audit_df
