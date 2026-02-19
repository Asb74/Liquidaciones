"""Configuración del proceso de liquidación 2026."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


@dataclass(frozen=True)
class DBPaths:
    """Rutas de bases de datos SQLite."""

    fruta: Path
    calidad: Path
    eeppl: Path


@dataclass(frozen=True)
class PriceConfig:
    """Precios orientativos por grupo ANECOP y destríos."""

    anecop: dict[str, Decimal]
    destrios: dict[str, Decimal]


@dataclass(frozen=True)
class LiquidacionConfig:
    """Parámetros de ejecución de la liquidación."""

    campana: int
    empresa: str
    cultivo: str
    db_paths: DBPaths
    prices: PriceConfig
    output_csv: Path
    audit_globalgap_csv: Path


DECIMAL_INTERNAL = Decimal("0.0001")
DECIMAL_EXPORT = Decimal("0.01")

CALIBRES = [f"Cal{i}" for i in range(12)]
DESTRIOS = ["DesLinea", "DesMesa", "Podrido"]
COLUMNS_KILOS = CALIBRES + DESTRIOS
