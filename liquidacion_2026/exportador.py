"""Exportación de salidas de liquidación."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

from .utils import parse_decimal


def _round(value: object, decimals: int) -> Decimal:
    quant = Decimal("1").scaleb(-decimals)
    return parse_decimal(value).quantize(quant, rounding=ROUND_HALF_UP)


def exportar_todo(
    *,
    precios_df: pd.DataFrame,
    campana: int,
    cultivo: str,
    audit_df: pd.DataFrame,
    resumen_metricas: dict[str, Decimal | int],
    output_dir: Path,
    export_decimals: int,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    perceco_path = output_dir / f"precios_perceco_{campana}_{cultivo}.csv"
    perceco = precios_df.copy()
    perceco.insert(0, "campaña", campana)
    perceco["precio_final"] = perceco["precio_final"].map(lambda x: _round(x, export_decimals))
    perceco.to_csv(perceco_path, index=False)

    audit_path = output_dir / "auditoria_gg_boletas_no_match.csv"
    audit_df.to_csv(audit_path, index=False)

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
    resumen.to_csv(resumen_path, index=False)

    return {"perceco": perceco_path, "audit": audit_path, "resumen": resumen_path}
