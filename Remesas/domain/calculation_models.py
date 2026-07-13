from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class LiquidationHeader:
    remesa_id: Any
    remesa_name: str
    campana: str
    empresa: str
    cultivo: str
    fecha_pago: str
    periodo_desde: str
    periodo_hasta: str
    tipo_liquidacion: str
    categoria: str
    socio: str
    variedades: list[str]
    options: dict[str, bool]
    prices: dict[str, Decimal]
    generated_at: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class GradeLiquidation:
    code: str
    label: str
    kg: Decimal
    price: Decimal
    amount: Decimal


@dataclass(frozen=True)
class MemberLiquidation:
    member_id: int
    member_name: str
    variety: str
    delivery_count: int
    net_kg: Decimal
    commercial_kg: Decimal
    destruction_kg: Decimal
    table_destruction_kg: Decimal
    rotten_kg: Decimal
    grades: list[GradeLiquidation]
    commercial_amount: Decimal
    destruction_amount: Decimal
    table_destruction_amount: Decimal
    rotten_amount: Decimal
    gross_amount: Decimal
    collection_amount: Decimal
    transport_amount: Decimal
    quality_amount: Decimal
    globalgap_amount: Decimal
    hectare_fee_amount: Decimal
    taxable_base: Decimal
    vat_percent: Decimal
    vat_amount: Decimal
    withholding_percent: Decimal
    withholding_amount: Decimal
    total_amount: Decimal

    @property
    def final_average_price(self) -> Decimal:
        return self.total_amount / self.net_kg if self.net_kg else Decimal("0")

    @property
    def commercial_average_price(self) -> Decimal:
        return self.commercial_amount / self.commercial_kg if self.commercial_kg else Decimal("0")


@dataclass(frozen=True)
class LiquidationTotals:
    net_kg: Decimal
    commercial_amount: Decimal
    gross_amount: Decimal
    taxable_base: Decimal
    vat_amount: Decimal
    withholding_amount: Decimal
    total_amount: Decimal


@dataclass(frozen=True)
class LiquidationResult:
    header: LiquidationHeader
    member_results: list[MemberLiquidation]
    totals: LiquidationTotals
    warnings: list[str]


@dataclass(frozen=True)
class LiquidationLine:
    member_id: int
    member_name: str
    variety: str
    delivery_count: int
    net_kg: Decimal
    commercial_amount: Decimal
    collection_amount: Decimal | None
    transport_amount: Decimal | None
    quality_amount: Decimal | None
    globalgap_amount: Decimal | None
    hectare_fee_amount: Decimal | None
    taxable_base: Decimal | None
    vat_amount: Decimal | None
    withholding_amount: Decimal | None
    total_amount: Decimal | None


@dataclass(frozen=True)
class LiquidationCalculationResult:
    lines: list[LiquidationLine]
    delivery_count: int
    member_count: int
    variety_count: int
    net_kg: Decimal
    commercial_amount: Decimal
    warnings: list[str]
    result: LiquidationResult | None = None
