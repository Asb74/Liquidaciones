"""Cálculo de Fondo GlobalGAP por socio."""

from __future__ import annotations

from decimal import Decimal
import logging

import pandas as pd

from .config import CALIBRES
from .utils import parse_decimal


LOGGER = logging.getLogger(__name__)


def _distinct_non_empty(values: pd.Series) -> list[str]:
    normalizados = values.fillna("").astype(str).str.strip()
    return sorted({value for value in normalizados if value})


def _build_inconsistencias_df(deepp_df: pd.DataFrame) -> pd.DataFrame:
    if deepp_df.empty:
        return pd.DataFrame(columns=["idsocio", "campo", "valores_distintos"])

    niveles = deepp_df.groupby("idsocio")["nivelglobal"].apply(_distinct_non_empty)
    certs = deepp_df.groupby("idsocio")["certificacion_norm"].apply(_distinct_non_empty)

    rows: list[dict[str, str]] = []
    for idsocio, valores in niveles.items():
        if len(valores) > 1:
            rows.append({"idsocio": idsocio, "campo": "nivelglobal", "valores_distintos": " | ".join(valores)})

    for idsocio, valores in certs.items():
        if len(valores) > 1:
            rows.append({"idsocio": idsocio, "campo": "certificacion_norm", "valores_distintos": " | ".join(valores)})

    return pd.DataFrame(rows, columns=["idsocio", "campo", "valores_distintos"])


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

    campana = pesos_df["campaña"].iloc[0] if "campaña" in pesos_df.columns and not pesos_df.empty else None
    empresa = pesos_df["empresa"].iloc[0] if "empresa" in pesos_df.columns and not pesos_df.empty else None
    cultivo = pesos_df["cultivo"].iloc[0] if "cultivo" in pesos_df.columns and not pesos_df.empty else None

    deepp_df["idsocio"] = deepp_df["idsocio"].astype(str).str.strip()
    deepp_df["certificacion_norm"] = deepp_df["certificacion"].fillna("").astype(str).str.strip().str.upper()
    deepp_df["nivelglobal"] = deepp_df["nivelglobal"].fillna("").astype(str).str.strip()
    deepp_filtrado = deepp_df.copy()
    if campana is not None and "campaña" in deepp_filtrado.columns:
        deepp_filtrado = deepp_filtrado[deepp_filtrado["campaña"] == campana]
    if empresa is not None and "empresa" in deepp_filtrado.columns:
        deepp_filtrado = deepp_filtrado[deepp_filtrado["empresa"] == empresa]
    if cultivo is not None and "cultivo" in deepp_filtrado.columns:
        deepp_filtrado = deepp_filtrado[deepp_filtrado["cultivo"] == cultivo]

    inconsistencias_df = _build_inconsistencias_df(deepp_filtrado)
    inconsistencias_nivel_df = inconsistencias_df[inconsistencias_df["campo"] == "nivelglobal"]
    if not inconsistencias_nivel_df.empty:
        LOGGER.error(
            "NivelGlobal inconsistente para el mismo socio. Socios afectados: %s",
            inconsistencias_nivel_df["idsocio"].tolist(),
        )
        raise ValueError("NivelGlobal inconsistente para el mismo socio")

    maestro_socios = (
        deepp_filtrado.groupby("idsocio", as_index=False)
        .agg(
            certificacion_norm=("certificacion_norm", "first"),
            nivelglobal=("nivelglobal", "first"),
        )
    )
    socios_gg = maestro_socios[maestro_socios["certificacion_norm"] == "GLOBAL GAP"].copy()

    if not inconsistencias_df.empty:
        LOGGER.warning("Se detectaron inconsistencias en DEEPP para certificación/nivel por socio.")

    mnivel_df["nivel"] = mnivel_df["nivel"].fillna("").astype(str).str.strip()
    mnivel_df["indice"] = mnivel_df["indice"].map(parse_decimal)

    merged_all = kilos_socios_df.merge(socios_gg[["idsocio", "nivelglobal"]], on="idsocio", how="left")
    merged_all = merged_all.merge(mnivel_df[["nivel", "indice"]], left_on="nivelglobal", right_on="nivel", how="left")
    merged_all["indice"] = merged_all["indice"].map(parse_decimal)

    bonificaciones = bon_global_df["bonificacion"].map(parse_decimal)
    if bonificaciones.empty:
        bon_eur = Decimal("0")
    elif len(bonificaciones) > 1:
        LOGGER.info("BonGlobal tiene %s filas; se toma la primera bonificación para GG.", len(bonificaciones))
        bon_eur = bonificaciones.iloc[0]
    else:
        bon_eur = bonificaciones.iloc[0]

    merged_all["bonificacion_eur"] = bon_eur
    merged_all["fondo_soc"] = merged_all.apply(
        lambda row: parse_decimal(row["kilos_comerciales"]) * parse_decimal(row["bonificacion_eur"]) * parse_decimal(row["indice"]),
        axis=1,
    )

    merged = merged_all[merged_all["idsocio"].isin(socios_gg["idsocio"])].copy()
    fondo_total = sum(merged["fondo_soc"], Decimal("0"))

    socios_no_gg = sorted(set(kilos_socios_df["idsocio"]) - set(socios_gg["idsocio"]))

    audit_rows: list[dict[str, str]] = []
    audit_rows.extend(
        {"tipo": "socio_sin_gg", "id": socio, "detalle": "socio en pesos sin certificación GLOBAL GAP"}
        for socio in socios_no_gg
    )
    audit_rows.extend(
        {
            "tipo": f"inconsistencia_{row['campo']}",
            "id": row["idsocio"],
            "detalle": row["valores_distintos"],
        }
        for _, row in inconsistencias_df.iterrows()
    )
    audit_df = pd.DataFrame(audit_rows, columns=["tipo", "id", "detalle"])

    audit_globalgap_socios_df = merged[["idsocio", "nivelglobal", "indice", "bonificacion_eur", "kilos_comerciales", "fondo_soc"]].copy()
    audit_globalgap_socios_df["kilos_comerciales"] = audit_globalgap_socios_df["kilos_comerciales"].map(parse_decimal)
    audit_globalgap_socios_df["indice"] = audit_globalgap_socios_df["indice"].map(parse_decimal)
    audit_globalgap_socios_df["bonificacion_eur"] = audit_globalgap_socios_df["bonificacion_eur"].map(parse_decimal)
    audit_globalgap_socios_df["fondo_soc"] = audit_globalgap_socios_df["fondo_soc"].map(parse_decimal)

    LOGGER.info("Socios GG considerados: %s", len(audit_globalgap_socios_df))
    LOGGER.info("Fondo GG total calculado: %s", fondo_total)

    return fondo_total, audit_globalgap_socios_df, audit_df
