"""Cálculo de Fondo GlobalGAP por socio."""

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


def _resolver_nivel_mode(series: pd.Series) -> str:
    niveles = series.fillna("").astype(str).str.strip()
    niveles = niveles[niveles != ""]
    if niveles.empty:
        return ""
    freq = niveles.value_counts()
    top = freq[freq == freq.max()].index.tolist()
    return sorted(top)[0]


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

    pesos["idsocio"] = pesos["idsocio"].astype(str).str.strip()
    if "kilos_comerciales" not in pesos.columns:
        cal_cols = [f"cal{i}" for i in range(12)]
        pesos["kilos_comerciales"] = pesos[cal_cols].sum(axis=1)
    pesos["kilos_comerciales"] = pesos["kilos_comerciales"].map(parse_decimal)

    kilos_socios_df = pesos.groupby("idsocio", as_index=False)["kilos_comerciales"].sum()

    deepp["idsocio"] = deepp["idsocio"].astype(str).str.strip()
    deepp["certificacion_norm"] = deepp["certificacion"].map(_normalizar_certificacion)
    deepp["nivelglobal"] = deepp["nivelglobal"].map(_normalizar_texto)
    gg_deepp = deepp[deepp["certificacion_norm"] == "GLOBAL GAP"].copy()

    conflictos_rows: list[dict[str, str]] = []
    if not gg_deepp.empty:
        for idsocio, sdf in gg_deepp.groupby("idsocio"):
            valores = sorted({v for v in sdf["nivelglobal"].tolist() if v})
            if len(valores) > 1:
                conflictos_rows.append(
                    {
                        "tipo": "conflicto_nivelglobal",
                        "id": idsocio,
                        "detalle": " | ".join(valores),
                    }
                )

    socios_gg = (
        gg_deepp.groupby("idsocio", as_index=False)
        .agg(
            certificacion=("certificacion_norm", "first"),
            nivelglobal=("nivelglobal", _resolver_nivel_mode),
        )
    )

    mnivel["nivel"] = mnivel["nivel"].map(_normalizar_texto)
    mnivel["indice"] = mnivel["indice"].map(parse_decimal)
    socios_gg = socios_gg.merge(mnivel[["nivel", "indice"]], left_on="nivelglobal", right_on="nivel", how="left")
    socios_gg["indice"] = socios_gg["indice"].map(parse_decimal)

    nivel_to_eurokg, bon_base = _build_bonificacion_map(bon_global_df, mnivel)

    def _euro_kg(row: pd.Series) -> Decimal:
        nivel = _normalizar_texto(row["nivelglobal"])
        if nivel_to_eurokg:
            return parse_decimal(nivel_to_eurokg.get(nivel, Decimal("0")))
        return parse_decimal(bon_base) * parse_decimal(row["indice"])

    socios_gg["euro_kg"] = socios_gg.apply(_euro_kg, axis=1)

    audit_globalgap_socios_df = socios_gg.merge(kilos_socios_df, on="idsocio", how="left")
    audit_globalgap_socios_df["kilos_comerciales_gg"] = audit_globalgap_socios_df["kilos_comerciales"].map(parse_decimal)
    audit_globalgap_socios_df["importe_gg"] = audit_globalgap_socios_df.apply(
        lambda row: parse_decimal(row["kilos_comerciales_gg"]) * parse_decimal(row["euro_kg"]),
        axis=1,
    )

    fondo_gg_total = sum(audit_globalgap_socios_df["importe_gg"], Decimal("0"))

    socios_pesos = set(kilos_socios_df["idsocio"])
    socios_certificados = set(audit_globalgap_socios_df["idsocio"])
    socios_no_gg = sorted(socios_pesos - socios_certificados)

    audit_rows = conflictos_rows + [
        {
            "tipo": "socio_sin_gg",
            "id": socio,
            "detalle": "socio en pesos sin certificación GLOBAL GAP",
        }
        for socio in socios_no_gg
    ]
    audit_df = pd.DataFrame(audit_rows, columns=["tipo", "id", "detalle"])

    if conflictos_rows:
        LOGGER.warning(
            "Conflictos de NivelGlobal resueltos por mode determinista para socios: %s",
            [row["id"] for row in conflictos_rows],
        )

    kilos_gg_total = sum(audit_globalgap_socios_df["kilos_comerciales_gg"], Decimal("0"))
    LOGGER.info("GlobalGAP socios considerados: %s", len(audit_globalgap_socios_df))
    LOGGER.info("GlobalGAP kilos comerciales totales: %s", kilos_gg_total)
    LOGGER.info("GlobalGAP fondo total: %s", fondo_gg_total)

    audit_globalgap_socios_df = audit_globalgap_socios_df[
        ["idsocio", "certificacion", "nivelglobal", "indice", "kilos_comerciales_gg", "euro_kg", "importe_gg"]
    ].copy()

    return fondo_gg_total, audit_globalgap_socios_df, audit_df
