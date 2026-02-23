"""Cálculo de Fondo GlobalGAP replicando consulta Access."""

from __future__ import annotations

from decimal import Decimal
import logging

import pandas as pd

from .utils import parse_decimal


LOGGER = logging.getLogger(__name__)


def _normalizar_texto(value: object) -> str:
    return str(value).strip()


def _normalizar_certificacion(value: object) -> str:
    return _normalizar_texto(value).upper()


def _build_bonificacion_map(bon_global_df: pd.DataFrame, mnivel_df: pd.DataFrame) -> tuple[dict[str, Decimal], Decimal]:
    bg = bon_global_df.copy()
    bg.columns = bg.columns.str.strip().str.lower()
    if "bonificacion" in bg.columns:
        bg["bonificacion"] = bg["bonificacion"].map(parse_decimal)

    nivel_to_eurokg: dict[str, Decimal] = {}

    if {"nivel", "bonificacion"}.issubset(bg.columns):
        for _, row in bg.iterrows():
            nivel_to_eurokg[_normalizar_texto(row["nivel"])] = parse_decimal(row["bonificacion"])
        return nivel_to_eurokg, Decimal("0")

    if {"indice", "bonificacion"}.issubset(bg.columns):
        idx_to_bonus = {
            parse_decimal(row["indice"]): parse_decimal(row["bonificacion"])
            for _, row in bg.iterrows()
        }
        for _, row in mnivel_df.iterrows():
            nivel_to_eurokg[_normalizar_texto(row["nivel"])] = idx_to_bonus.get(parse_decimal(row["indice"]), Decimal("0"))
        return nivel_to_eurokg, Decimal("0")

    bon_base = parse_decimal(bg["bonificacion"].iloc[0]) if "bonificacion" in bg.columns and not bg.empty else Decimal("0")
    return {}, bon_base


def calcular_fondo_globalgap(
    pesos_df: pd.DataFrame,
    deepp_df: pd.DataFrame,
    mnivel_df: pd.DataFrame,
    bon_global_df: pd.DataFrame,
) -> tuple[Decimal, pd.DataFrame, pd.DataFrame]:
    pesos = pesos_df.copy()
    deepp = deepp_df.copy()
    mnivel = mnivel_df.copy()

    pesos.columns = pesos.columns.str.strip().str.lower()
    deepp.columns = deepp.columns.str.strip().str.lower()
    mnivel.columns = mnivel.columns.str.strip().str.lower()

    for col in ["campaña", "cultivo", "boleta", "idsocio"]:
        if col not in pesos.columns:
            pesos[col] = ""
        if col not in deepp.columns:
            deepp[col] = ""
        pesos[col] = pesos[col].astype(str).str.strip()
        deepp[col] = deepp[col].astype(str).str.strip()

    for col in [*(f"cal{i}" for i in range(12)), "podrido", "deslinea", "desmesa"]:
        pesos[col] = pesos[col].map(parse_decimal)

    pesos["comercializado"] = pesos[[f"cal{i}" for i in range(12)]].sum(axis=1)
    pesos["destrio"] = pesos[["podrido", "deslinea", "desmesa"]].sum(axis=1)

    deepp["certificacion_norm"] = deepp["certificacion"].map(_normalizar_certificacion)
    deepp["nivelglobal"] = deepp["nivelglobal"].map(_normalizar_texto)

    join_cols = ["campaña", "cultivo", "boleta", "idsocio"]
    joined = pesos.merge(
        deepp[join_cols + ["certificacion", "certificacion_norm", "nivelglobal"]],
        on=join_cols,
        how="inner",
    )

    gg_joined = joined[joined["certificacion_norm"] == "GLOBAL GAP"].copy()

    if gg_joined.empty:
        empty_cols = ["idsocio", "certificacion", "nivelglobal", "indice", "kilos_comerciales_gg", "kilos_destrio", "euro_kg", "importe_gg"]
        return Decimal("0"), pd.DataFrame(columns=empty_cols), pd.DataFrame(columns=["tipo", "id", "detalle"])

    grouped = (
        gg_joined.groupby(["idsocio", "nivelglobal"], as_index=False)
        .agg(
            certificacion=("certificacion", "first"),
            kilos_comerciales_gg=("comercializado", lambda s: sum(s.map(parse_decimal), Decimal("0"))),
            kilos_destrio=("destrio", lambda s: sum(s.map(parse_decimal), Decimal("0"))),
        )
    )

    mnivel["nivel"] = mnivel["nivel"].map(_normalizar_texto)
    mnivel["indice"] = mnivel["indice"].map(parse_decimal)

    grouped = grouped.merge(mnivel[["nivel", "indice"]], left_on="nivelglobal", right_on="nivel", how="left")
    grouped["indice"] = grouped["indice"].map(parse_decimal)

    nivel_to_eurokg, bon_base = _build_bonificacion_map(bon_global_df, mnivel)

    def _euro_kg(row: pd.Series) -> Decimal:
        nivel = _normalizar_texto(row["nivelglobal"])
        if nivel_to_eurokg:
            return parse_decimal(nivel_to_eurokg.get(nivel, Decimal("0")))
        return parse_decimal(bon_base) * parse_decimal(row["indice"])

    grouped["euro_kg"] = grouped.apply(_euro_kg, axis=1)
    grouped["importe_gg"] = grouped.apply(
        lambda row: parse_decimal(row["kilos_comerciales_gg"]) * parse_decimal(row["euro_kg"]),
        axis=1,
    )

    fondo_gg_total = sum(grouped["importe_gg"].map(parse_decimal), Decimal("0"))

    LOGGER.info("GlobalGAP grupos considerados (idsocio+nivel): %s", len(grouped))
    LOGGER.info("GlobalGAP fondo total: %s", fondo_gg_total)

    audit_globalgap_socios_df = grouped[
        ["idsocio", "certificacion", "nivelglobal", "indice", "kilos_comerciales_gg", "kilos_destrio", "euro_kg", "importe_gg"]
    ].copy()

    socios_pesos = set(pesos["idsocio"].astype(str).str.strip())
    socios_certificados = set(audit_globalgap_socios_df["idsocio"].astype(str).str.strip())
    socios_no_gg = sorted(socios_pesos - socios_certificados)

    audit_df = pd.DataFrame(
        [
            {
                "tipo": "socio_sin_gg",
                "id": socio,
                "detalle": "socio en pesos sin certificación GLOBAL GAP",
            }
            for socio in socios_no_gg
        ],
        columns=["tipo", "id", "detalle"],
    )

    return fondo_gg_total, audit_globalgap_socios_df, audit_df
