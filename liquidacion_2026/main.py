"""Punto de entrada del proceso de liquidación 2026."""

from __future__ import annotations

import argparse
import json
import logging
from decimal import Decimal
from pathlib import Path

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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Liquidación campaña 2026 - Kakis")
    parser.add_argument("--campana", type=int, required=True)
    parser.add_argument("--empresa", required=True)
    parser.add_argument("--cultivo", required=True)
    parser.add_argument("--db-fruta", required=True)
    parser.add_argument("--db-calidad", required=True)
    parser.add_argument("--db-eeppl", required=True)
    parser.add_argument("--precios-anecop", required=True, help="JSON: {semana: {AAA: 0.4, AA: 0.3, A: 0.2}}")
    parser.add_argument("--precios-destrio", required=True, help="JSON: {DesLinea: -0.01, DesMesa: -0.02, Podrido: -0.03}")
    parser.add_argument("--output", default="liquidacion_2026_resultado.csv")
    parser.add_argument("--audit-globalgap", default="auditoria_globalgap_no_match.csv")
    return parser.parse_args()


def _parse_price_json(raw: str) -> dict:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Se esperaba un objeto JSON.")
    return parsed


def _build_config(args: argparse.Namespace) -> LiquidacionConfig:
    anecop_raw = _parse_price_json(args.precios_anecop)
    destrio_raw = _parse_price_json(args.precios_destrio)

    anecop: dict[int, dict[str, Decimal]] = {
        int(semana): {k.upper(): Decimal(str(v)) for k, v in grupos.items()}
        for semana, grupos in anecop_raw.items()
    }
    destrios = {k: Decimal(str(v)) for k, v in destrio_raw.items()}

    return LiquidacionConfig(
        campana=args.campana,
        empresa=args.empresa,
        cultivo=args.cultivo,
        db_paths=DBPaths(
            fruta=Path(args.db_fruta),
            calidad=Path(args.db_calidad),
            eeppl=Path(args.db_eeppl),
        ),
        prices=PriceConfig(anecop=anecop, destrios=destrios),
        output_csv=Path(args.output),
        audit_globalgap_csv=Path(args.audit_globalgap),
    )


def run(config: LiquidacionConfig) -> None:
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


def main() -> None:
    args = parse_args()
    config = _build_config(args)
    run(config)


if __name__ == "__main__":
    main()
