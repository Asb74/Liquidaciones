"""Configuración y constantes de liquidación KAKIS 2025."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


LOG_PATH = Path("log_ejecucion.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


@dataclass(frozen=True)
class DBPaths:
    fruta: Path
    calidad: Path
    eeppl: Path


@dataclass(frozen=True)
class LiquidacionConfig:
    campana: int
    empresa: int
    cultivo: str
    bruto_campana: Decimal
    otros_fondos: Decimal
    ratio_categoria_ii: Decimal
    precios_destrio: dict[str, Decimal]
    anecop_path: Path
    db_paths: DBPaths
    output_dir: Path
    export_decimals: int = 2


CALIBRES = [f"Cal{i}" for i in range(12)]
DESTRIOS = ["DesLinea", "DesMesa", "Podrido"]
GRUPOS_COMERCIALES = ["AAA", "AA", "A"]
CATEGORIAS_COMERCIALES = ["I", "II"]

ROUND_INTERNAL = Decimal("0.0001")


def q4(value: Decimal) -> Decimal:
    return value.quantize(ROUND_INTERNAL, rounding=ROUND_HALF_UP)
