from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, TYPE_CHECKING
from domain.hectare_fee_master import HectareFeeMaster

if TYPE_CHECKING:
    from domain.models import Delivery


class CalculationStatus(Enum):
    CALCULATED = "calculated"
    NOT_APPLICABLE = "not_applicable"
    DISABLED = "disabled"
    PENDING = "pending"
    ERROR = "error"


@dataclass(frozen=True)
class MoneyConcept:
    amount: Decimal | None
    status: CalculationStatus
    warning: str = ""


@dataclass(frozen=True)
class FiscalRegime:
    name: str
    vat_rate: Decimal
    withholding_rate: Decimal


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
class GradeBreakdown:
    code: str
    label: str
    kilograms: Decimal
    price: Decimal
    amount: Decimal

    @property
    def kg(self) -> Decimal:
        return self.kilograms


GradeLiquidation = GradeBreakdown


@dataclass(frozen=True)
class HectareFeeAuditData:
    surface_crops: tuple[str, ...]
    delivery_crops: tuple[str, ...]
    price_per_hectare: Decimal
    applicable_hectares: Decimal
    total_theoretical_fee: Decimal
    total_effective_kg: Decimal
    rate_per_kg: Decimal | None
    line_effective_kg: Decimal
    line_fee: Decimal | None
    status: CalculationStatus
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class GlobalGapCertificationResult:
    certified: bool
    inconsistent: bool
    certified_crops: tuple[str, ...]
    non_certified_crops: tuple[str, ...]
    raw_values: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class GlobalGapLevelResult:
    level: str | None
    index: Decimal | None
    status: CalculationStatus
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class GlobalGapRate:
    bonus_rate: Decimal | None
    category: int | None
    source_description: str
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class GlobalGapAuditData:
    certified: bool
    certification_inconsistent: bool
    certified_crops: tuple[str, ...]
    non_certified_crops: tuple[str, ...]
    level: str | None
    index: Decimal | None
    bonus_rate: Decimal | None
    category: int | None
    base_type: str | None
    effective_net_kg: Decimal
    commercial_net_kg: Decimal
    base_kg: Decimal | None
    detected_amount: Decimal | None
    applied_amount: Decimal | None
    status: CalculationStatus
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class MemberLiquidation:
    member_id: int
    member_name: str
    variety: str
    delivery_count: int
    net_deliveries: Decimal
    net_commercial: Decimal
    net_waste: Decimal
    net_rotten: Decimal
    grades: tuple[GradeBreakdown, ...]
    commercial_amount: Decimal
    destruction_amount: Decimal = Decimal("0")
    table_destruction_amount: Decimal = Decimal("0")
    rotten_amount: Decimal = Decimal("0")
    gross_amount: Decimal = Decimal("0")
    detected_collection_amount: Decimal = Decimal("0")
    collection_amount: Decimal | None = None
    detected_transport_amount: Decimal = Decimal("0")
    transport_amount: Decimal | None = None
    quality_amount: Decimal | None = None
    globalgap_amount: Decimal | None = None
    globalgap_audit: GlobalGapAuditData | None = None
    hectare_fee_amount: Decimal | None = None
    effective_net_kg: Decimal = Decimal("0")
    quality_rate: Decimal = Decimal("0")
    quality_source: str = "not_found"
    applicable_hectares: Decimal = Decimal("0")
    hectare_fee_price: Decimal = Decimal("0")
    hectare_fee_total_member: Decimal = Decimal("0")
    hectare_fee_total_effective_kg: Decimal = Decimal("0")
    hectare_fee_rate_per_kg: Decimal | None = None
    hectare_fee_status: CalculationStatus = CalculationStatus.NOT_APPLICABLE
    hectare_fee_rounding_adjustment: Decimal = Decimal("0")
    hectare_fee_parcels: tuple[dict, ...] = ()
    hectare_fee_audit: HectareFeeAuditData | None = None
    taxable_base: Decimal | None = None
    vat_rate: Decimal | None = None
    vat_amount: Decimal | None = None
    withholding_rate: Decimal | None = None
    withholding_amount: Decimal | None = None
    total_amount: Decimal | None = None
    commercial_average_price: Decimal | None = None
    final_average_price: Decimal | None = None
    warnings: tuple[str, ...] = ()
    statuses: dict[str, CalculationStatus] = field(default_factory=dict)
    source_deliveries: tuple["Delivery", ...] = ()

    @property
    def net_kg(self) -> Decimal:
        return self.effective_net_kg or self.net_deliveries

    @property
    def commercial_kg(self) -> Decimal:
        return self.net_commercial

    @property
    def destruction_kg(self) -> Decimal:
        return self.net_waste

    @property
    def table_destruction_kg(self) -> Decimal:
        return Decimal("0")

    @property
    def rotten_kg(self) -> Decimal:
        return self.net_rotten

    @property
    def vat_percent(self) -> Decimal:
        return self.vat_rate or Decimal("0")

    @property
    def withholding_percent(self) -> Decimal:
        return self.withholding_rate or Decimal("0")


@dataclass(frozen=True)
class LiquidationTotals:
    net_kg: Decimal
    commercial_amount: Decimal
    gross_amount: Decimal
    detected_collection_amount: Decimal
    collection_amount: Decimal | None
    detected_transport_amount: Decimal
    transport_amount: Decimal | None
    quality_amount: Decimal | None
    globalgap_amount: Decimal | None
    hectare_fee_amount: Decimal | None
    taxable_base: Decimal | None
    vat_amount: Decimal | None
    withholding_amount: Decimal | None
    total_amount: Decimal | None


@dataclass(frozen=True)
class LiquidationResult:
    header: LiquidationHeader
    member_results: tuple[MemberLiquidation, ...]
    totals: LiquidationTotals
    warnings: tuple[str, ...]
    hectare_fee_master: HectareFeeMaster | None = None
    hectare_fee_master_fingerprint: str = ""

    @property
    def members(self) -> tuple[MemberLiquidation, ...]:
        return self.member_results

    @property
    def delivery_count(self) -> int:
        return sum(m.delivery_count for m in self.member_results)

    @property
    def member_count(self) -> int:
        return len({m.member_id for m in self.member_results})

    @property
    def variety_count(self) -> int:
        return len({m.variety for m in self.member_results if m.variety})

    @property
    def net_kg(self) -> Decimal:
        return self.totals.net_kg

    @property
    def commercial_amount(self) -> Decimal:
        return self.totals.commercial_amount


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
