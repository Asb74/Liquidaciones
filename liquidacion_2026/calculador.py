"""Modelo económico final de liquidación KAKIS por campaña."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from .config import DESTRIOS, q4
from .utils import parse_decimal
from .utils_debug import debug_write
from .validaciones import (
    validar_columnas_minimas_pesosfres,
    validar_cuadre,
    validar_referencia,
    validar_semanas_kilos_vs_anecop,
    validar_total_rel,
)

logger = logging.getLogger(__name__)


@dataclass
class ResultadoCalculo:
    precios_df: pd.DataFrame
    resumen_df: pd.DataFrame
    resumen_metricas: dict[str, Decimal | int]


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
    long["kilos"] = pd.to_numeric(long["kilos"], errors="coerce").fillna(0)
    long = long.merge(calibre_map, on="calibre", how="inner", validate="m:1")

    kilos_group = long.groupby(["semana", "grupo", "categoria"], as_index=False)["kilos"].sum()
    kilos_group = kilos_group[kilos_group["kilos"] > 0]

    logging.info("----- KILOS DATAFRAME -----")
    logging.info(f"Columnas: {kilos_group.columns.tolist()}")
    logging.info(f"Shape: {kilos_group.shape}")
    logging.info(f"Tipos:\n{kilos_group.dtypes}")
    logging.info(f"Primeras filas:\n{kilos_group.head(5)}")
    if "semana" in kilos_group.columns:
        dup_kilos = kilos_group.groupby("semana").size()
        logging.info(f"Registros por semana KILOS:\n{dup_kilos}")
        logging.info(f"Semanas duplicadas KILOS:\n{dup_kilos[dup_kilos > 1]}")
    debug_write("KILOS COLUMNS", kilos_group.columns.tolist())
    debug_write("KILOS SHAPE", kilos_group.shape)
    debug_write("KILOS HEAD", kilos_group.head())
    if "semana" in kilos_group.columns:
        debug_write("KILOS GROUPBY SEMANA", kilos_group.groupby("semana").size())

    anecop = anecop_df.copy()
    anecop["precio_base"] = anecop["precio_base"].map(parse_decimal)

    logging.info("----- ANECOP DATAFRAME -----")
    logging.info(f"Columnas: {anecop.columns.tolist()}")
    logging.info(f"Shape: {anecop.shape}")
    logging.info(f"Tipos:\n{anecop.dtypes}")
    logging.info(f"Primeras filas:\n{anecop.head(5)}")
    if "semana" in anecop.columns:
        dup_anecop = anecop.groupby("semana").size()
        logging.info(f"Registros por semana ANECOP:\n{dup_anecop}")
        logging.info(f"Semanas duplicadas ANECOP:\n{dup_anecop[dup_anecop > 1]}")
    debug_write("ANECOP COLUMNS", anecop.columns.tolist())
    debug_write("ANECOP SHAPE", anecop.shape)
    debug_write("ANECOP HEAD", anecop.head())
    if "semana" in anecop.columns:
        debug_write("ANECOP GROUPBY SEMANA", anecop.groupby("semana").size())

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

    anecop["rel"] = anecop["precio_base"].map(lambda p: q4(parse_decimal(p) / ref))

    rel_rows = []
    for _, row in anecop.iterrows():
        rel_rows.append({"semana": int(row["semana"]), "grupo": row["grupo"], "categoria": "I", "rel_final": row["rel"]})
        rel_rows.append(
            {
                "semana": int(row["semana"]),
                "grupo": row["grupo"],
                "categoria": "II",
                "rel_final": q4(row["rel"] * ratio_categoria_ii),
            }
        )
    rel_df = pd.DataFrame(rel_rows)

    try:
        logging.info("Intentando merge de kilos_group con rel_df por ['semana', 'grupo', 'categoria']")
        merged = kilos_group.merge(rel_df, on=["semana", "grupo", "categoria"], how="left", validate="m:1")
    except Exception as e:
        logging.error("ERROR EN MERGE")
        logging.error(str(e))
        logging.error("Claves únicas en derecha (semana, grupo, categoria):")
        logging.error(rel_df.groupby(["semana", "grupo", "categoria"]).size())
        raise
    merged["rel_final"] = merged["rel_final"].map(parse_decimal)
    merged["kilos_dec"] = merged["kilos"].map(parse_decimal)
    merged["rel_kilos"] = merged.apply(lambda r: q4(r["kilos_dec"] * r["rel_final"]), axis=1)

    destrios_long = pesos_df.melt(id_vars=["semana"], value_vars=DESTRIOS, var_name="destrio", value_name="kilos")
    destrios_long["kilos"] = pd.to_numeric(destrios_long["kilos"], errors="coerce").fillna(0)
    destrios_long["importe"] = destrios_long.apply(
        lambda r: q4(parse_decimal(r["kilos"]) * precios_destrio[r["destrio"]]), axis=1
    )
    importe_destrios = sum(destrios_long["importe"], Decimal("0"))

    neto_comercial = q4(bruto_campana - fondo_gg_total - otros_fondos - importe_destrios)

    base_relativa = sum(merged["rel_kilos"], Decimal("0"))
    validar_total_rel(base_relativa)

    coef = neto_comercial / base_relativa

    logger.info(f"Importe destríos: {importe_destrios}")
    logger.info(f"Neto comercial: {neto_comercial}")
    logger.info(f"Base relativa: {base_relativa}")
    logger.info(f"Coef global sin redondear: {coef}")

    final_rows = []
    for _, row in rel_df.iterrows():
        precio = parse_decimal(row["rel_final"]) * coef
        final_rows.append(
            {
                "semana": int(row["semana"]),
                "calibre": row["grupo"],
                "categoria": row["categoria"],
                "precio_final": precio,
            }
        )
    precios_df = pd.DataFrame(final_rows).sort_values(["semana", "calibre", "categoria"])

    recon_det = merged.merge(
        precios_df.rename(columns={"calibre": "grupo"}),
        on=["semana", "grupo", "categoria"],
        how="left",
        validate="m:1",
    )
    recon = sum(recon_det.apply(lambda r: r["kilos_dec"] * r["precio_final"], axis=1), Decimal("0")) + importe_destrios
    objetivo_validacion = bruto_campana - fondo_gg_total - otros_fondos
    descuadre = validar_cuadre(recon, objetivo_validacion)

    sem_kilos = merged.groupby("semana", as_index=False)["kilos"].sum().rename(columns={"kilos": "total_kg_comercial_sem"})

    df_rel = (
        anecop[["semana", "grupo", "rel"]]
        .pivot(index="semana", columns="grupo", values="rel")
        .reset_index()
    )
    if not df_rel["semana"].is_unique:
        logging.error("ANECOP no tiene semanas únicas")
        logging.error(df_rel.groupby("semana").size())
        raise ValueError("Semanas duplicadas en ANECOP")
    debug_write("RIGHT DATASET UNIQUE CHECK", df_rel.groupby("semana").size())

    try:
        logging.info("Intentando merge por 'semana'")
        table = sem_kilos.merge(df_rel, on="semana", how="left")
    except Exception as e:
        debug_write("MERGE ERROR", str(e))
        logging.error("ERROR EN MERGE")
        logging.error(str(e))
        logging.error("Claves únicas en derecha:")
        if "semana" in df_rel.columns:
            logging.error(df_rel.groupby("semana").size())
        raise

    missing_rel_weeks = sorted(table.loc[table[["AAA", "AA", "A"]].isna().any(axis=1), "semana"].astype(int).tolist())
    if missing_rel_weeks:
        raise ValueError(f"Hay semanas con kilos comerciales sin ANECOP: {missing_rel_weeks}")

    table[["AAA", "AA", "A"]] = table[["AAA", "AA", "A"]].applymap(lambda v: parse_decimal(v) * coef)
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
    return ResultadoCalculo(precios_df=precios_df, resumen_df=table.sort_values("semana"), resumen_metricas=metricas)
