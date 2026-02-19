"""Exportación de resultados de liquidación."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

from .config import DECIMAL_EXPORT


def _to_export_decimal(value: object) -> Decimal:
    return Decimal(str(value)).quantize(DECIMAL_EXPORT, rounding=ROUND_HALF_UP)


def exportar_resultado(df: pd.DataFrame, campana: int, output_path: Path) -> None:
    """Exporta CSV final con formato compatible Perceco y columnas de resumen."""
    out = df[
        [
            "semana",
            "calibre",
            "categoria",
            "precio_final",
            "ingreso_teorico",
            "fondo_gg",
            "ingreso_real",
            "factor",
        ]
    ].copy()
    out.insert(0, "campaña", campana)
    for column in ["precio_final", "ingreso_teorico", "fondo_gg", "ingreso_real", "factor"]:
        out[column] = out[column].map(_to_export_decimal)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
