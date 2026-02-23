"""Modelo económico final de liquidación KAKIS por campaña."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from .config import DESTRIOS
from .utils import parse_decimal
from .utils_debug import debug_write
from .validaciones import (
    ValidationError,
    validar_columnas_minimas_pesosfres,
    validar_cuadre,
    validar_referencia,
    validar_semanas_kilos_vs_anecop,
    validar_total_rel,
)

logger = logging.getLogger(__name__)

Q5 = Decimal("0.00001")


def round_final(value: Decimal) -> Decimal:
    return value.quantize(Q5, rounding=ROUND_HALF_UP)


@dataclass
class ResultadoCalculo:
    precios_df: pd.DataFrame
    resumen_df: pd.DataFrame
    resumen_metricas: dict[str, Decimal | int]


def _normalizar_categoria(value: object) -> str:
    token = str(value).strip().upper()
    if "II" in token or "2" in token:
        return "II"
    if "I" in token or "1" in token:
        return "I"
    return token


def calcular_modelo_final(
    pesos_df: pd.DataFrame,
    calibre_map: pd.DataFrame,
    anecop_df: pd.DataFrame,
    precios_destrio: dict[str, Decimal],
    bruto_campana: Decimal,
    otros_fondos: Decimal,
    fondo_gg_total: Decimal,
    ratio_categoria_ii: Decimal,
) -> ResultadoCalculo:
    validar_columnas_minimas_pesosfres(pesos_df)

    long = pesos_df.melt(id_vars=["semana", "boleta"], value_vars=[f"cal{i}" for i in range(12)], var_name="calibre", value_name="kilos")
    long["kilos"] = pd.to_numeric(long["kilos"], errors="coerce").fillna(0).map(parse_decimal)
    long = long.merge(calibre_map, on="calibre", how="inner", validate="m:1")
    long["categoria"] = long["categoria"].map(_normalizar_categoria)

    kilos_group = long.groupby(["semana", "grupo", "categoria"], as_index=False)["kilos"].sum()
    kilos_group = kilos_group[kilos_group["kilos"] > Decimal("0")]

    anecop = anecop_df.copy()
    anecop["precio_base"] = anecop["precio_base"].map(parse_decimal)

    kilos_semanas = set(kilos_group["semana"].astype(int).unique().tolist())
    anecop_semanas = set(anecop["semana"].astype(int).unique().tolist())
    validar_semanas_kilos_vs_anecop(kilos_semanas, anecop_semanas)

    semana_ref = None
    for sem in sorted(anecop_semanas):
        if sem in kilos_semanas:
            semana_ref = sem
            break
    if semana_ref is None:
        raise ValueError("No existe semana de referencia: no hay cruce entre ANECOP y kilos comerciales.")

    ref = anecop[(anecop["semana"] == semana_ref) & (anecop["grupo"] == "AAA")]["precio_base"].iloc[0]
    validar_referencia(ref, semana_ref)

    anecop["rel"] = anecop["precio_base"].map(lambda p: parse_decimal(p) / ref)

    rel_i = anecop[["semana", "grupo", "rel"]].copy()
    rel_i["categoria"] = "I"
    rel_i = rel_i.rename(columns={"rel": "rel_final"})

    rel_ii = rel_i.copy()
    rel_ii["categoria"] = "II"
    rel_ii["rel_final"] = rel_ii["rel_final"].map(lambda rel: parse_decimal(rel) * ratio_categoria_ii)

    rel_df = pd.concat([rel_i, rel_ii], ignore_index=True)

    merged = kilos_group.merge(rel_df, on=["semana", "grupo", "categoria"], how="left", validate="m:1")
    merged["rel_final"] = merged["rel_final"].map(parse_decimal)
    merged["kilos_dec"] = merged["kilos"].map(parse_decimal)
    merged["rel_kilos"] = merged.apply(lambda r: parse_decimal(r["kilos_dec"]) * parse_decimal(r["rel_final"]), axis=1)

    destrios_long = pesos_df.melt(id_vars=["semana"], value_vars=DESTRIOS, var_name="destrio", value_name="kilos")
    destrios_long["kilos"] = pd.to_numeric(destrios_long["kilos"], errors="coerce").fillna(0).map(parse_decimal)
    destrios_long["importe"] = destrios_long.apply(
        lambda r: parse_decimal(r["kilos"]) * parse_decimal(precios_destrio[r["destrio"]]),
        axis=1,
    )
    importe_destrios = sum(destrios_long["importe"], Decimal("0"))

    neto_comercial = bruto_campana - fondo_gg_total - otros_fondos - importe_destrios

    base_relativa = sum(merged["rel_kilos"], Decimal("0"))
    validar_total_rel(base_relativa)

    coef = parse_decimal(neto_comercial) / parse_decimal(base_relativa)

    final_i = rel_i.copy()
    final_i["precio_raw"] = final_i["rel_final"].map(lambda rel: parse_decimal(rel) * parse_decimal(coef))
    final_i["precio_final"] = final_i["precio_raw"].map(round_final)
    final_i = final_i.rename(columns={"grupo": "calibre"})[["semana", "calibre", "categoria", "precio_raw", "precio_final"]]

    final_ii = final_i[final_i["categoria"] == "I"].copy()
    final_ii["categoria"] = "II"
    final_ii["precio_raw_i"] = final_ii["precio_raw"].map(parse_decimal)
    final_ii["precio_raw"] = final_ii["precio_raw_i"].map(lambda p: parse_decimal(p) * parse_decimal(ratio_categoria_ii))
    final_ii["precio_final"] = final_ii["precio_raw"].map(round_final)

    invalid = final_ii[final_ii["precio_raw"] > final_ii["precio_raw_i"]]
    if not invalid.empty:
        detail = invalid[["semana", "calibre", "precio_raw_i", "precio_final"]].to_dict("records")
        raise ValidationError(f"Precio categoría II superior a categoría I detectado: {detail}")

    final_ii = final_ii.drop(columns=["precio_raw_i"])
    precios_det_df = pd.concat([final_i, final_ii], ignore_index=True).sort_values(["semana", "calibre", "categoria"]).reset_index(drop=True)
    precios_df = precios_det_df.drop(columns=["precio_raw"])

    recon_det = merged.merge(
        precios_det_df.rename(columns={"calibre": "grupo"}),
        on=["semana", "grupo", "categoria"],
        how="left",
        validate="m:1",
    )
    recon = sum(recon_det.apply(lambda r: parse_decimal(r["kilos_dec"]) * parse_decimal(r["precio_raw"]), axis=1), Decimal("0")) + importe_destrios
    objetivo_validacion = bruto_campana - fondo_gg_total - otros_fondos
    logger.info(f"Recon sin redondeo: {recon}")
    logger.info(f"Objetivo: {objetivo_validacion}")
    logger.info(f"Descuadre: {recon-objetivo_validacion}")
    descuadre = validar_cuadre(recon, objetivo_validacion, tolerancia=Decimal("0.05"))

    sem_kilos = merged.groupby("semana", as_index=False)["kilos"].sum().rename(columns={"kilos": "total_kg_comercial_sem"})

    df_rel = anecop[["semana", "grupo", "rel"]].pivot(index="semana", columns="grupo", values="rel").reset_index()
    debug_write("RIGHT DATASET UNIQUE CHECK", df_rel.groupby("semana").size())

    table = sem_kilos.merge(df_rel, on="semana", how="left")

    missing_rel_weeks = sorted(table.loc[table[["AAA", "AA", "A"]].isna().any(axis=1), "semana"].astype(int).tolist())
    if missing_rel_weeks:
        raise ValueError(f"Hay semanas con kilos comerciales sin ANECOP: {missing_rel_weeks}")

    table[["AAA", "AA", "A"]] = table[["AAA", "AA", "A"]].apply(
        lambda col: col.map(lambda v: parse_decimal(v) * parse_decimal(coef))
    )
    table["coef_global"] = coef
    table["ref_semana"] = semana_ref
    table = table.rename(columns={"AAA": "precio_aaa_i", "AA": "precio_aa_i", "A": "precio_a_i"})

    metricas = {
        "total_kg_comerciales": parse_decimal(merged["kilos"].sum()),
        "ingreso_destrios_total": importe_destrios,
        "fondo_gg_total": fondo_gg_total,
        "neto_obj": neto_comercial,
        "total_rel": base_relativa,
        "coef": coef,
        "num_semanas_con_kilos": int(len(kilos_semanas)),
        "descuadre": descuadre,
        "recon": recon,
        "semana_ref": int(semana_ref),
    }
    logger.info(
        "Importe destríos=%s | neto_comercial=%s | base_relativa=%s | coef=%s | descuadre=%s",
        importe_destrios,
        neto_comercial,
        base_relativa,
        coef,
        descuadre,
    )
    return ResultadoCalculo(precios_df=precios_df, resumen_df=table.sort_values("semana"), resumen_metricas=metricas)
