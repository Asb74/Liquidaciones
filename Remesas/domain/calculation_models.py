from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


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
