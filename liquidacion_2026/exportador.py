"""Exportación de salidas de liquidación."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

from .utils import format_kg_es, parse_decimal



def format_decimal_es(value: Decimal) -> str:
    dec = parse_decimal(value)
    return format(dec, "f").replace(".", ",")


def _to_es_dataframe(df: pd.DataFrame, decimals: int) -> pd.DataFrame:
    out = df.copy()
    quant = Decimal("1").scaleb(-decimals)
    for col in out.columns:
        if out[col].map(lambda x: isinstance(x, Decimal)).any():
            out[col] = out[col].map(lambda x: format_decimal_es(parse_decimal(x).quantize(quant, rounding=ROUND_HALF_UP)))
    return out


def _format_kilos_for_export(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        col_norm = str(col).lower()
        if "kilo" in col_norm or "_kg" in col_norm or col_norm.startswith("kg"):
            out[col] = out[col].map(lambda value: format_kg_es(parse_decimal(value)))
    return out


def _build_precios_finales_pivot(precios_df: pd.DataFrame) -> pd.DataFrame:
    salida = precios_df.copy()

    salida["calibre"] = salida["calibre"].astype(str).str.strip().str.upper()
    salida["categoria"] = salida["categoria"].astype(str).str.strip().str.upper()

    pivot = salida.pivot_table(
        index="semana",
        columns=["calibre", "categoria"],
        values="precio_final",
        aggfunc="first",
    )

    columnas_objetivo = {
        "AAAI": ("AAA", "I"),
        "AAI": ("AA", "I"),
        "AI": ("A", "I"),
        "AAAII": ("AAA", "II"),
        "AAII": ("AA", "II"),
        "AII": ("A", "II"),
    }

    resultado = pd.DataFrame(index=pivot.index)

    for columna_salida, origen in columnas_objetivo.items():
        resultado[columna_salida] = pivot.get(origen)

    resultado = resultado.reset_index()

    # Asegurar orden numérico real antes de convertir a texto
    resultado["semana"] = resultado["semana"].astype(int)
    resultado = resultado.sort_values("semana").reset_index(drop=True)

    resultado = resultado.rename(columns={"semana": "Semana"})
    resultado["Semana"] = "Sem " + resultado["Semana"].astype(str)

    columnas_finales = ["Semana", "AAAI", "AAI", "AI", "AAAII", "AAII", "AII"]

    return resultado[columnas_finales]


def exportar_todo(
    *,
    precios_df: pd.DataFrame,
    campana: int,
    cultivo: str,
    audit_df: pd.DataFrame,
    audit_globalgap_socios_df: pd.DataFrame,
    audit_kilos_semana_df: pd.DataFrame,
    resumen_df: pd.DataFrame,
    resumen_metricas: dict[str, Decimal | int],
    output_dir: Path,
    export_decimals: int,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    perceco_path = output_dir / f"precios_perceco_{campana}_{cultivo}.csv"
    precios_finales_path = output_dir / "precios_finales.csv"
    perceco = precios_df.copy()
    perceco.insert(0, "campaña", campana)
    perceco_es = _to_es_dataframe(perceco, export_decimals)
    perceco_es["precio_final"] = perceco["precio_final"].map(lambda x: f"{parse_decimal(x):.5f}".replace(".", ","))
    perceco_es.to_csv(perceco_path, index=False, sep=";", decimal=",", encoding="utf-8-sig")

    precios_finales = _build_precios_finales_pivot(precios_df)
    for columna in precios_finales.columns[1:]:
        precios_finales[columna] = precios_finales[columna].map(
            lambda value: "" if pd.isna(value)
            else f"{parse_decimal(value):.5f}".replace(".", ",")
        )
    precios_finales.to_csv(
        precios_finales_path,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
    )

    audit_path = output_dir / "auditoria_gg_boletas_no_match.csv"
    audit_df.to_csv(audit_path, index=False, sep=";", decimal=",", encoding="utf-8-sig")

    audit_gg_socios_path = output_dir / "auditoria_globalgap_socios.csv"
    audit_globalgap_socios_export = _format_kilos_for_export(_to_es_dataframe(audit_globalgap_socios_df, export_decimals))
    audit_globalgap_socios_export.to_csv(
        audit_gg_socios_path,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
    )

    audit_kilos_semana_path = output_dir / "auditoria_kilos_semana.csv"
    audit_kilos_semana_export = _format_kilos_for_export(_to_es_dataframe(audit_kilos_semana_df, export_decimals))
    audit_kilos_semana_export.to_csv(
        audit_kilos_semana_path,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
    )

    resumen_semana_path = output_dir / "resumen_semana.csv"
    _to_es_dataframe(resumen_df, export_decimals).to_csv(
        resumen_semana_path,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
    )

    resumen_path = output_dir / "resumen_campania.csv"
    resumen = pd.DataFrame(
        [
            {
                "bruto": resumen_metricas.get("bruto", ""),
                "fondo_gg": resumen_metricas["fondo_gg_total"],
                "otros_fondos": resumen_metricas.get("otros_fondos", ""),
                "destrios": resumen_metricas["ingreso_destrios_total"],
                "neto_obj": resumen_metricas["neto_obj"],
                "total_rel": resumen_metricas["total_rel"],
                "coef": resumen_metricas["coef"],
                "recon": resumen_metricas["recon"],
                "descuadre": resumen_metricas["descuadre"],
            }
        ]
    )
    _to_es_dataframe(resumen, export_decimals).to_csv(resumen_path, index=False, sep=";", decimal=",", encoding="utf-8-sig")

    return {
        "perceco": perceco_path,
        "precios_finales": precios_finales_path,
        "audit": audit_path,
        "audit_globalgap_socios": audit_gg_socios_path,
        "audit_kilos_semana": audit_kilos_semana_path,
        "resumen": resumen_path,
        "resumen_semana": resumen_semana_path,
    }
