"""Punto de entrada del proceso de liquidación 2026."""

from __future__ import annotations

import argparse
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Liquidación campaña 2026 - Caquis")
    parser.add_argument("--campana", type=int)
    parser.add_argument("--empresa")
    parser.add_argument("--cultivo")
    parser.add_argument("--db-fruta")
    parser.add_argument("--db-calidad")
    parser.add_argument("--db-eeppl")
    parser.add_argument("--precios-anecop", help="JSON: {semana: {AAA: 0.4, AA: 0.3, A: 0.2}}")
    parser.add_argument("--precios-destrio", help="JSON: {DesLinea: -0.01, DesMesa: -0.02, Podrido: -0.03}")
    parser.add_argument("--output")
    parser.add_argument("--audit-globalgap")
    return parser.parse_args()


def _parse_price_json(raw: str) -> dict[str, Any]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Se esperaba un objeto JSON.")
    return parsed


def _default_output_path(campana: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return SALIDAS_DIR / f"liquidacion_{campana}_{ts}.csv"


def _default_audit_path(campana: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return SALIDAS_DIR / f"auditoria_globalgap_{campana}_{ts}.csv"


def _build_config(args: argparse.Namespace) -> LiquidacionConfig:
    required_fields = [
        "campana",
        "empresa",
        "cultivo",
        "db_fruta",
        "db_calidad",
        "db_eeppl",
        "precios_anecop",
        "precios_destrio",
    ]
    missing = [field for field in required_fields if getattr(args, field) in (None, "")]
    if missing:
        missing_msg = ", ".join(sorted(missing))
        raise ValueError(f"Faltan argumentos obligatorios para modo CLI: {missing_msg}")

    anecop_raw = _parse_price_json(args.precios_anecop)
    destrio_raw = _parse_price_json(args.precios_destrio)
    anecop: dict[int, dict[str, Decimal]] = {
        int(semana): {grupo.upper(): Decimal(str(valor)) for grupo, valor in grupos.items()}
        for semana, grupos in anecop_raw.items()
    }
    destrios = {key: Decimal(str(value)) for key, value in destrio_raw.items()}

    SALIDAS_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = Path(args.output) if args.output else _default_output_path(args.campana)
    audit_csv = Path(args.audit_globalgap) if args.audit_globalgap else _default_audit_path(args.campana)
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
        output_csv=output_csv,
        audit_globalgap_csv=audit_csv,
        log_file=SALIDAS_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M')}.log",
    )


def configurar_logging(log_file: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )


def run(config: LiquidacionConfig) -> Path:
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
    if not csv_path.exists():
        raise FileNotFoundError(f"No existe el CSV de resultado: {csv_path}")

    df = pd.read_csv(csv_path)
    required_columns = {"semana", "ingreso_teorico", "fondo_gg", "ingreso_real", "factor"}
    if not required_columns.issubset(df.columns):
        missing = required_columns.difference(df.columns)
        raise ValueError(f"El CSV no contiene columnas para resumen: {sorted(missing)}")

    summary = (
        df.groupby("semana", as_index=False)[["ingreso_teorico", "fondo_gg", "ingreso_real", "factor"]]
        .max()
        .sort_values(by="semana")
    )
    print("\n=== RESUMEN SEMANAL ===")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:,.2f}"))
    return summary


def _pedir_input(label: str) -> str:
    return input(label).strip()


def _pedir_json(label: str) -> str:
    while True:
        raw = _pedir_input(label)
        try:
            _parse_price_json(raw)
            return raw
        except json.JSONDecodeError:
            print("JSON inválido. Inténtalo de nuevo.")


def _interactive_config() -> LiquidacionConfig:
    print("\n=== Ejecutar liquidación ===")
    campana = int(_pedir_input("Campaña (ej. 2026): "))
    empresa = _pedir_input("Empresa: ")
    cultivo = _pedir_input("Cultivo (ej. CAQUIS): ")
    db_fruta = _pedir_input("Ruta DB fruta (.sqlite): ")
    db_calidad = _pedir_input("Ruta DB calidad (.sqlite): ")
    db_eeppl = _pedir_input("Ruta DB EEPPL (.sqlite): ")
    precios_anecop = _pedir_json("Precios ANECOP (JSON): ")
    precios_destrio = _pedir_json("Precios destrío (JSON): ")

    args = argparse.Namespace(
        campana=campana,
        empresa=empresa,
        cultivo=cultivo,
        db_fruta=db_fruta,
        db_calidad=db_calidad,
        db_eeppl=db_eeppl,
        precios_anecop=precios_anecop,
        precios_destrio=precios_destrio,
        output=None,
        audit_globalgap=None,
    )
    return _build_config(args)


def _interactive_menu() -> None:
    ultimo_csv: Path | None = None
    while True:
        print("\n=== LIQUIDACIÓN CAQUI 2026 ===")
        print("1) Ejecutar liquidación")
        print("2) Ver resumen semanal")
        print("3) Salir")
        opcion = _pedir_input("Selecciona opción: ")

        if opcion == "1":
            config = _interactive_config()
            configurar_logging(config.log_file)
            ultimo_csv = run(config)
            print(f"Liquidación completada. CSV: {ultimo_csv}")
        elif opcion == "2":
            if ultimo_csv is None:
                ruta = _pedir_input("Ruta CSV de liquidación: ")
                mostrar_resumen(Path(ruta))
            else:
                mostrar_resumen(ultimo_csv)
        elif opcion == "3":
            print("Hasta luego.")
            return
        else:
            print("Opción inválida. Elige 1, 2 o 3.")


def _has_cli_args(args: argparse.Namespace) -> bool:
    return any(value not in (None, "") for value in vars(args).values())


def main() -> None:
    try:
        args = parse_args()
        if _has_cli_args(args):
            config = _build_config(args)
            configurar_logging(config.log_file)
            output = run(config)
            print(f"Liquidación completada. CSV: {output}")
            mostrar_resumen(output)
        else:
            _interactive_menu()
    except KeyboardInterrupt:
        print("\nProceso cancelado por el usuario.")
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).exception("Error durante la ejecución")
        print(f"Error: {exc}")


if __name__ == "__main__":
    main()
