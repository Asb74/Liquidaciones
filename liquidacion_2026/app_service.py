"""Servicios de aplicación para ejecutar la liquidación sin acoplarla a la UI."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from .calculador import calcular_precios_finales
from .config import DBPaths, LiquidacionConfig, PriceConfig
from .correspondencia_calibres import build_calibre_mapping
from .exportador import exportar_resultado
from .extractor_sqlite import SQLiteExtractor
from .globalgap import calcular_bonificacion_globalgap
from .validaciones import (
    validar_calibres_sin_mapping,
    validar_duplicados,
    validar_semanas_sin_precio,
)

LOGGER = logging.getLogger(__name__)
SALIDAS_DIR = Path("salidas")


def parse_price_json(raw: str) -> dict[str, Any]:
    """Convierte JSON de precios en diccionario validado."""
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Se esperaba un objeto JSON.")
    return parsed


def default_output_path(campana: int) -> Path:
    """Genera ruta por defecto para salida de liquidación."""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return SALIDAS_DIR / f"liquidacion_{campana}_{ts}.csv"


def default_audit_path(campana: int) -> Path:
    """Genera ruta por defecto para auditoría de GlobalGAP."""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return SALIDAS_DIR / f"auditoria_globalgap_{campana}_{ts}.csv"


def build_config(
    *,
    campana: int,
    empresa: str,
    cultivo: str,
    db_fruta: Path,
    db_calidad: Path,
    db_eeppl: Path,
    precios_anecop_raw: str,
    precios_destrio: dict[str, Decimal],
    output: Path | None = None,
    audit_globalgap: Path | None = None,
) -> LiquidacionConfig:
    """Construye configuración de ejecución a partir de datos de UI/CLI."""
    if not empresa.strip():
        raise ValueError("El campo empresa es obligatorio.")
    if not cultivo.strip():
        raise ValueError("El campo cultivo es obligatorio.")

    anecop_raw = parse_price_json(precios_anecop_raw)
    anecop: dict[int, dict[str, Decimal]] = {
        int(semana): {grupo.upper(): Decimal(str(valor)) for grupo, valor in grupos.items()}
        for semana, grupos in anecop_raw.items()
    }

    SALIDAS_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = output if output else default_output_path(campana)
    audit_csv = audit_globalgap if audit_globalgap else default_audit_path(campana)

    return LiquidacionConfig(
        campana=campana,
        empresa=empresa,
        cultivo=cultivo,
        db_paths=DBPaths(fruta=db_fruta, calidad=db_calidad, eeppl=db_eeppl),
        prices=PriceConfig(anecop=anecop, destrios=precios_destrio),
        output_csv=output_csv,
        audit_globalgap_csv=audit_csv,
        log_file=SALIDAS_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log",
    )


def configurar_logging(log_file: Path) -> None:
    """Configura logging estándar a consola y archivo."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )


def run(config: LiquidacionConfig) -> Path:
    """Ejecuta el flujo de liquidación completo y exporta CSV."""
    extractor = SQLiteExtractor(
        fruta_db=str(config.db_paths.fruta),
        calidad_db=str(config.db_paths.calidad),
        eeppl_db=str(config.db_paths.eeppl),
    )

    pesos_df = extractor.fetch_pesosfres(config.campana, config.empresa, config.cultivo)
    validar_semanas_sin_precio(pesos_df["semana"], config.prices.anecop)

    corres_df = extractor.fetch_correspondencias_calibres()
    calibre_map = build_calibre_mapping(corres_df)
    validar_calibres_sin_mapping(calibre_map)
    validar_duplicados(calibre_map, ["calibre"], "calibre mapping")

    deepp = extractor.fetch_deepp()
    validar_duplicados(deepp, ["Boleta"], "DEEPP")
    mnivel = extractor.fetch_mnivel_global()
    validar_duplicados(mnivel, ["Nivel"], "MNivelGlobal")

    bon_global = extractor.fetch_bon_global(config.campana, config.cultivo, config.empresa)
    _, fondo_total = calcular_bonificacion_globalgap(
        pesos_df,
        deepp_df=deepp,
        mnivel_df=mnivel,
        bon_global_df=bon_global,
        audit_path=config.audit_globalgap_csv,
    )

    precios_df = calcular_precios_finales(
        pesos_df=pesos_df,
        calibre_map=calibre_map,
        precios_orientativos_semana=config.prices.anecop,
        precios_destrio=config.prices.destrios,
        fondo_globalgap_total=fondo_total,
    )

    exportar_resultado(precios_df, config.campana, config.output_csv)
    LOGGER.info("Proceso finalizado. Archivo generado: %s", config.output_csv)
    return config.output_csv


def mostrar_resumen(csv_path: Path) -> pd.DataFrame:
    """Genera resumen semanal a partir del CSV exportado."""
    if not csv_path.exists():
        raise FileNotFoundError(f"No existe el CSV de resultado: {csv_path}")

    df = pd.read_csv(csv_path)
    required_columns = {"semana", "ingreso_teorico", "fondo_gg", "ingreso_real", "factor"}
    if not required_columns.issubset(df.columns):
        missing = required_columns.difference(df.columns)
        raise ValueError(f"El CSV no contiene columnas para resumen: {sorted(missing)}")

    return (
        df.groupby("semana", as_index=False)[["ingreso_teorico", "fondo_gg", "ingreso_real", "factor"]]
        .max()
        .sort_values(by="semana")
    )
