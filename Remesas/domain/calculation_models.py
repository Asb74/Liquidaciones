from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, TYPE_CHECKING, Protocol
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
class FiscalCalculation:
    vat_rate: Decimal
    withholding_rate: Decimal
    vat_factor: Decimal
    withholding_factor: Decimal
    raw_amount_after_vat: Decimal
    amount_after_vat: Decimal
    vat_amount: Decimal
    raw_total_amount: Decimal
    withholding_amount: Decimal
    total_amount: Decimal
    final_average_price: Decimal | None


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
    eligible_crops: tuple[str, ...]
    price_per_hectare: Decimal
    applicable_hectares: Decimal
    total_theoretical_fee: Decimal
    total_effective_kg: Decimal
    rate_per_kg: Decimal | None
    line_effective_kg: Decimal
    line_fee: Decimal | None
    status: CalculationStatus
    warnings: tuple[str, ...] = ()
    candidate_boletas: int = 0
    active_cha_boletas: int = 0
    candidate_parcels: int = 0
    included_parcels: int = 0
    excluded_parcels: int = 0
    young_parcels: int = 0
    inactive_parcels: int = 0
    kg_by_crop: tuple[tuple[str, Decimal], ...] = ()
    reason: str = ""
    already_applied_fee: Decimal = Decimal("0")
    projected_applied_fee: Decimal = Decimal("0")
    remaining_fee: Decimal = Decimal("0")
    balance_status: str = "OPEN"

    @property
    def surface_crops(self) -> tuple[str, ...]:
        return self.eligible_crops

    @property
    def delivery_crops(self) -> tuple[str, ...]:
        return self.eligible_crops


@dataclass(frozen=True)
class HectareFeeBalance:
    member_id: int
    annual_fee: Decimal
    already_applied_fee: Decimal
    current_liquidation_fee: Decimal
    projected_applied_fee: Decimal
    remaining_fee: Decimal
    closed: bool
    warnings: tuple[str, ...] = ()

    @property
    def status(self) -> str:
        if self.remaining_fee < Decimal("-0.01"):
            return "OVER_APPLIED"
        if abs(self.remaining_fee) <= Decimal("0.01"):
            return "CLOSED"
        return "OPEN"


class HectareFeeAppliedRepository(Protocol):
    def get_applied_fee(self, member_id: int, campaign: str, company: str, eligible_crops: tuple[str, ...], exclude_current_remittance_id: str | int | None = None) -> Decimal:
        ...


@dataclass(frozen=True)
class MemberHectareFeeContext:
    member_id: int
    applicable_hectares: Decimal
    total_member_fee: Decimal
    total_effective_kg: Decimal
    rate_per_kg: Decimal | None
    status: CalculationStatus
    warnings: tuple[str, ...]
    parcel_audit: tuple[dict, ...]
    delivery_audit: tuple[dict, ...] = ()
    balance: HectareFeeBalance | None = None


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
    line_effective_kg: Decimal = Decimal("0")
    hectare_fee_parcels: tuple[dict, ...] = ()
    hectare_fee_delivery_audit: tuple[dict, ...] = ()
    hectare_fee_audit: HectareFeeAuditData | None = None
    hectare_fee_balance: HectareFeeBalance | None = None
    taxable_base: Decimal | None = None
    fiscal_regime_name: str = ""
    vat_rate: Decimal | None = None
    vat_factor: Decimal | None = None
    vat_amount: Decimal | None = None
    amount_after_vat: Decimal | None = None
    raw_amount_after_vat: Decimal | None = None
    withholding_rate: Decimal | None = None
    withholding_factor: Decimal | None = None
    withholding_amount: Decimal | None = None
    raw_total_amount: Decimal | None = None
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
    variety_audit: tuple[object, ...] = ()

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
