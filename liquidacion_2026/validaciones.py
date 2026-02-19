"""Validaciones de consistencia del proceso."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd


class ValidationError(ValueError):
    """Error de validación de negocio."""


def validar_duplicados(df: pd.DataFrame, cols: list[str], context: str) -> None:
    if df.duplicated(subset=cols).any():
        dupes = df[df.duplicated(subset=cols, keep=False)][cols].drop_duplicates().to_dict("records")
        raise ValidationError(f"Duplicados detectados en {context}: {dupes}")


def validar_semanas_sin_precio(semanas: pd.Series, precios_por_semana: dict[int, dict[str, Decimal]]) -> None:
    faltantes = sorted(set(semanas.astype(int).tolist()) - set(precios_por_semana.keys()))
    if faltantes:
        raise ValidationError(f"Semanas sin precio orientativo: {faltantes}")


def validar_calibres_sin_mapping(calibre_map: pd.DataFrame) -> None:
    missing = calibre_map[calibre_map["grupo"].isna() | (calibre_map["grupo"] == "")]
    if not missing.empty:
        raise ValidationError(
            f"Calibres sin mapping económico: {missing['calibre'].tolist()}"
        )


def validar_ingreso_teorico_no_cero(ingreso_teorico_anecop: Decimal, semana: int) -> None:
    if ingreso_teorico_anecop == Decimal("0"):
        raise ValidationError(f"Ingreso teórico ANECOP es 0 para semana {semana}")


def validar_cuadre_final(ingreso_real: Decimal, ingreso_reconstruido: Decimal, tolerancia: Decimal = Decimal("0.01")) -> None:
    if abs(ingreso_real - ingreso_reconstruido) > tolerancia:
        raise ValidationError(
            f"Descuadre final superior a tolerancia. Ingreso real={ingreso_real}, "
            f"reconstruido={ingreso_reconstruido}, tolerancia={tolerancia}"
        )
