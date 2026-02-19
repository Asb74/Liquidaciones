"""Motor de cálculo económico de liquidación."""

from __future__ import annotations

import logging
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from .config import CALIBRES, DECIMAL_INTERNAL, DESTRIOS
from .validaciones import validar_cuadre_final, validar_ingreso_teorico_no_cero

LOGGER = logging.getLogger(__name__)


def _q4(value: Decimal) -> Decimal:
    return value.quantize(DECIMAL_INTERNAL, rounding=ROUND_HALF_UP)


def calcular_precios_finales(
    pesos_df: pd.DataFrame,
    calibre_map: pd.DataFrame,
    precios_orientativos_semana: dict[int, dict[str, Decimal]],
    precios_destrio: dict[str, Decimal],
    fondo_globalgap_total: Decimal,
) -> pd.DataFrame:
    """Calcula precios finales por semana/calibre/categoría."""
    calibres_long = pesos_df.melt(
        id_vars=["semana"],
        value_vars=CALIBRES,
        var_name="calibre",
        value_name="kilos",
    )
    calibres_long["kilos"] = pd.to_numeric(calibres_long["kilos"], errors="coerce").fillna(0)

    calibres_grouped = calibres_long.groupby(["semana", "calibre"], as_index=False)["kilos"].sum()
    calibres_grouped = calibres_grouped.merge(calibre_map, on="calibre", how="left", validate="m:1")

    destrios_long = pesos_df.melt(
        id_vars=["semana"],
        value_vars=DESTRIOS,
        var_name="destrio",
        value_name="kilos",
    )
    destrios_long["kilos"] = pd.to_numeric(destrios_long["kilos"], errors="coerce").fillna(0)

    salida: list[dict[str, object]] = []
    semanas = sorted(pesos_df["semana"].astype(int).unique().tolist())

    total_kilos_anecop = Decimal(str(calibres_grouped["kilos"].sum()))
    fondo_rate = Decimal("0") if total_kilos_anecop == 0 else _q4(fondo_globalgap_total / total_kilos_anecop)

    for semana in semanas:
        week_cal = calibres_grouped[calibres_grouped["semana"] == semana].copy()
        week_des = destrios_long[destrios_long["semana"] == semana].copy()
        precios_sem = precios_orientativos_semana[semana]

        ingreso_teorico_anecop = Decimal("0")
        for _, row in week_cal.iterrows():
            grupo = str(row["grupo"])
            orientativo = precios_sem[grupo]
            kilos = Decimal(str(row["kilos"]))
            ingreso_teorico_anecop += _q4(kilos * orientativo)

        validar_ingreso_teorico_no_cero(ingreso_teorico_anecop, semana)

        ingreso_destrios = Decimal("0")
        for _, row in week_des.iterrows():
            precio_des = precios_destrio[row["destrio"]]
            kilos = Decimal(str(row["kilos"]))
            ingreso_destrios += _q4(kilos * precio_des)

        fondo_week = _q4(Decimal(str(week_cal["kilos"].sum())) * fondo_rate)
        ingreso_real = _q4(ingreso_teorico_anecop + ingreso_destrios + fondo_week)
        factor = _q4((ingreso_real - ingreso_destrios) / ingreso_teorico_anecop)

        LOGGER.info(
            "Semana %s | ingreso_anecop=%s ingreso_destrios=%s fondo=%s ingreso_real=%s factor=%s",
            semana,
            ingreso_teorico_anecop,
            ingreso_destrios,
            fondo_week,
            ingreso_real,
            factor,
        )

        ingreso_reconstruido = Decimal("0")
        for _, row in week_cal.iterrows():
            grupo = str(row["grupo"])
            orientativo = precios_sem[grupo]
            precio_cat1 = _q4(orientativo * factor)
            precio_cat2 = _q4(precio_cat1 * Decimal("0.5"))
            kilos = Decimal(str(row["kilos"]))
            salida.append(
                {
                    "semana": int(semana),
                    "calibre": row["calibre"],
                    "categoria": "CAT1",
                    "precio_final": precio_cat1,
                    "kilos": kilos,
                }
            )
            salida.append(
                {
                    "semana": int(semana),
                    "calibre": row["calibre"],
                    "categoria": "CAT2",
                    "precio_final": precio_cat2,
                    "kilos": Decimal("0"),
                }
            )
            ingreso_reconstruido += _q4(kilos * precio_cat1)

        ingreso_reconstruido += ingreso_destrios + fondo_week
        validar_cuadre_final(ingreso_real, ingreso_reconstruido)

    return pd.DataFrame(salida)
