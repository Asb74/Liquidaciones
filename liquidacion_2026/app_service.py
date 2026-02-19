"""Orquestación del proceso de liquidación para UI."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from .calculador import ResultadoCalculo, calcular_modelo_final
from .config import DBPaths, LiquidacionConfig
from .correspondencia_calibres import build_calibre_mapping
from .exportador import exportar_todo
from .extractor_sqlite import SQLiteExtractor
from .globalgap import calcular_fondo_globalgap
from .normalizador_anecop import cargar_anecop

LOGGER = logging.getLogger(__name__)


@dataclass
class RunOutput:
    resultado: ResultadoCalculo
    files: dict[str, Path]


def build_config(
    *,
    campana: int,
    empresa: int,
    cultivo: str,
    bruto_campana: Decimal,
    otros_fondos: Decimal,
    ratio_categoria_ii: Decimal,
    anecop_path: Path,
    db_fruta: Path,
    db_calidad: Path,
    db_eeppl: Path,
    precios_destrio: dict[str, Decimal],
) -> LiquidacionConfig:
    if campana != 2025:
        raise ValueError("Validación configurada para campaña 2025.")
    if empresa != 1:
        raise ValueError("Empresa válida para este modelo: 1.")
    if cultivo.strip().upper() != "KAKIS":
        raise ValueError("Cultivo válido para este modelo: KAKIS.")

    return LiquidacionConfig(
        campana=campana,
        empresa=empresa,
        cultivo=cultivo.strip().upper(),
        bruto_campana=bruto_campana,
        otros_fondos=otros_fondos,
        ratio_categoria_ii=ratio_categoria_ii,
        precios_destrio=precios_destrio,
        anecop_path=anecop_path,
        db_paths=DBPaths(fruta=db_fruta, calidad=db_calidad, eeppl=db_eeppl),
        output_dir=Path("salidas"),
    )


def configurar_logging(output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )
    return log_file


def run(config: LiquidacionConfig) -> RunOutput:
    extractor = SQLiteExtractor(str(config.db_paths.fruta), str(config.db_paths.calidad), str(config.db_paths.eeppl))

    anecop_df = cargar_anecop(config.anecop_path)
    pesos_df = extractor.fetch_pesosfres(config.campana, config.empresa, config.cultivo)
    calibre_map = build_calibre_mapping(extractor.fetch_correspondencias_calibres())

    deepp_df = extractor.fetch_deepp()
    mnivel_df = extractor.fetch_mnivel_global()
    bon_global_df = extractor.fetch_bon_global(config.campana, config.cultivo, config.empresa)

    fondo_gg_total, audit_df = calcular_fondo_globalgap(pesos_df, deepp_df, mnivel_df, bon_global_df)

    resultado = calcular_modelo_final(
        pesos_df=pesos_df,
        calibre_map=calibre_map,
        anecop_df=anecop_df,
        precios_destrio=config.precios_destrio,
        bruto_campana=config.bruto_campana,
        otros_fondos=config.otros_fondos,
        fondo_gg_total=fondo_gg_total,
        ratio_categoria_ii=config.ratio_categoria_ii,
    )
    resultado.resumen_metricas["bruto"] = config.bruto_campana
    resultado.resumen_metricas["otros_fondos"] = config.otros_fondos

    files = exportar_todo(
        precios_df=resultado.precios_df,
        campana=config.campana,
        cultivo=config.cultivo,
        audit_df=audit_df,
        resumen_metricas=resultado.resumen_metricas,
        output_dir=config.output_dir,
        export_decimals=config.export_decimals,
    )

    LOGGER.info("Liquidación completada. Archivo Perceco: %s", files["perceco"])
    return RunOutput(resultado=resultado, files=files)
