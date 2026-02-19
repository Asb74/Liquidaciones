"""Exportación de resultados de liquidación."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

from .config import DECIMAL_EXPORT


def exportar_resultado(df: pd.DataFrame, campana: int, output_path: Path) -> None:
    """Exporta CSV final con formato compatible Perceco."""
    out = df[["semana", "calibre", "categoria", "precio_final"]].copy()
    out.insert(0, "campaña", campana)
    out["precio_final"] = out["precio_final"].map(
        lambda x: Decimal(str(x)).quantize(DECIMAL_EXPORT, rounding=ROUND_HALF_UP)
    )
    out.to_csv(output_path, index=False)
