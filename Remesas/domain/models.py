from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class AppConfig:
    db_fruta: str
    db_eepp: str
    app_name: str
    mode: str
    window_width: int
    window_height: int
    log_file: str
    log_level: str
    hectare_fee_price_per_hectare: Decimal = Decimal("195")
    hectare_fee_delivery_crops: tuple[str, ...] = ("CITRICOS", "MANDARINA", "DIRECTO", "DIRECTOCHF", "INDUSTRIA")
    hectare_fee_applicable_remittance_crops: tuple[str, ...] = ("CITRICOS", "MANDARINA", "DIRECTO", "DIRECTOCHF", "INDUSTRIA")

    @property
    def hectare_fee_applicable_crops(self) -> tuple[str, ...]:
        return self.hectare_fee_applicable_remittance_crops


@dataclass(frozen=True)
class WorkContext:
    campana: str
    empresa: str
    cultivo: str


@dataclass(frozen=True)
class Period:
    start: date
    end: date


@dataclass
class DeliveryFilter:
    context: WorkContext
    period: Period
    varieties: list[str] = field(default_factory=list)
    socio: str | None = None
    categoria: str | None = None
    limit: int = 5000


@dataclass
class Delivery:
    fecha: str | None
    registro: Any
    socio: Any
    nombre_socio: str | None
    variedad: str | None
    categoria: str | None
    neto: Decimal
    albaran: Any
    boleta: Any
    plataforma: Any
    liquidado: Any
    batch_net_kg: Decimal = Decimal("0")
    precalibrado: Any = None
    collection_cost: Decimal = Decimal("0")
    social_security_collection: Decimal = Decimal("0")
    foreman_cost: Decimal = Decimal("0")
    transport_cost: Decimal = Decimal("0")
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def net_kg(self) -> Decimal:
        return self.neto

    @property
    def effective_net_kg(self) -> Decimal:
        from domain.financial_rules import effective_net_kg
        return effective_net_kg(self.neto, self.batch_net_kg)


@dataclass
class Summary:
    total_entregas: int = 0
    socios: int = 0
    variedades: int = 0
    kilos_netos: float = 0.0
    primera_fecha: str = ""
    ultima_fecha: str = ""
    liquidadas: int = 0
    sin_variedad: int = 0
    sin_socio_valido: int = 0
    sin_categoria: int = 0
    warnings: list[str] = field(default_factory=list)


@dataclass
class Remesa:
    values: dict[str, Any]

    @property
    def prices(self) -> dict[str, Any]:
        keys = [f"P{i}" for i in range(12)] + ["PDESTRIO", "PDMESA", "PPODRIDO"]
        return {key: self.values.get(key) for key in keys}
