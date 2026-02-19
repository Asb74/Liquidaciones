"""Modelo económico final de liquidación KAKIS por campaña."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from .config import DESTRIOS, q4
from .validaciones import validar_cuadre, validar_referencia, validar_semanas_kilos_vs_anecop, validar_total_rel


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
    long = pesos_df.melt(id_vars=["semana", "Boleta"], value_vars=[f"Cal{i}" for i in range(12)], var_name="calibre", value_name="kilos")
    long["kilos"] = pd.to_numeric(long["kilos"], errors="coerce").fillna(0)
    long = long.merge(calibre_map, on="calibre", how="inner", validate="m:1")

    kilos_group = long.groupby(["semana", "grupo", "categoria"], as_index=False)["kilos"].sum()
    kilos_group = kilos_group[kilos_group["kilos"] > 0]

    anecop = anecop_df.copy()
    anecop["precio_base"] = anecop["precio_base"].map(lambda v: Decimal(str(v)))

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

    anecop["rel"] = anecop["precio_base"].map(lambda p: q4(Decimal(str(p)) / ref))

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

    merged = kilos_group.merge(rel_df, on=["semana", "grupo", "categoria"], how="left", validate="m:1")
    merged["rel_final"] = merged["rel_final"].map(lambda x: Decimal(str(x)))
    merged["kilos_dec"] = merged["kilos"].map(lambda k: Decimal(str(k)))
    merged["rel_kilos"] = merged.apply(lambda r: q4(r["kilos_dec"] * r["rel_final"]), axis=1)

    total_rel = sum(merged["rel_kilos"], Decimal("0"))
    validar_total_rel(total_rel)

    destrios_long = pesos_df.melt(id_vars=["semana"], value_vars=DESTRIOS, var_name="destrio", value_name="kilos")
    destrios_long["kilos"] = pd.to_numeric(destrios_long["kilos"], errors="coerce").fillna(0)
    destrios_long["importe"] = destrios_long.apply(
        lambda r: q4(Decimal(str(r["kilos"])) * precios_destrio[r["destrio"]]), axis=1
    )
    ingreso_destrios_total = sum(destrios_long["importe"], Decimal("0"))

    neto_obj = q4(bruto_campana - fondo_gg_total - otros_fondos - ingreso_destrios_total)
    coef = q4(neto_obj / total_rel)

    final_rows = []
    for _, row in rel_df.iterrows():
        precio = q4(Decimal(str(row["rel_final"])) * coef)
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
    recon = sum(recon_det.apply(lambda r: q4(r["kilos_dec"] * r["precio_final"]), axis=1), Decimal("0")) + ingreso_destrios_total
    objetivo_validacion = bruto_campana - fondo_gg_total - otros_fondos
    descuadre = validar_cuadre(recon, objetivo_validacion)

    sem_kilos = merged.groupby("semana", as_index=False)["kilos"].sum().rename(columns={"kilos": "total_kg_comercial_sem"})
    table = sem_kilos.merge(
        precios_df[precios_df["categoria"] == "I"].pivot(index="semana", columns="calibre", values="precio_final").reset_index(),
        on="semana",
        how="left",
    )
    table["coef_global"] = coef
    table["ref_semana"] = semana_ref
    table = table.rename(columns={"AAA": "precio_aaa_i", "AA": "precio_aa_i", "A": "precio_a_i"})

    metricas = {
        "total_kg_comerciales": Decimal(str(merged["kilos"].sum())),
        "ingreso_destrios_total": ingreso_destrios_total,
        "fondo_gg_total": fondo_gg_total,
        "neto_obj": neto_obj,
        "total_rel": total_rel,
        "coef": coef,
        "num_semanas_con_kilos": int(len(kilos_semanas)),
        "descuadre": descuadre,
        "recon": recon,
        "semana_ref": int(semana_ref),
    }
    return ResultadoCalculo(precios_df=precios_df, resumen_df=table.sort_values("semana"), resumen_metricas=metricas)
