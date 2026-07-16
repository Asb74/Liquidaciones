from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class SplitRecipient:
    recipient_member_id: int
    recipient_member_name: str = ""
    value: Decimal = Decimal("0")
    is_residual: bool = False
    sort_order: int = 0

@dataclass(frozen=True)
class SplitRule:
    id: int | None
    source_member_id: int
    split_type: str
    recipients: tuple[SplitRecipient, ...]
    source_member_name: str = ""
    campaign: str | None = None
    crop: str | None = None
    variety: str | None = None
    remittance_id: int | None = None
    priority: int = 100

@dataclass(frozen=True)
class ResolvedSplitRule:
    rule: SplitRule | None
    factors: tuple[tuple[SplitRecipient, Decimal], ...]

@dataclass(frozen=True)
class SplitAllocation:
    recipient_member_id: int
    factor: Decimal
    values: dict[str, Decimal]

@dataclass(frozen=True)
class SplitPreviewLine:
    source_member_id: int
    source_member_name: str
    recipient_member_id: int
    recipient_name: str
    variety: str
    split_factor: Decimal
    net_kg: Decimal
    gross_amount: Decimal
    collection_amount: Decimal
    hectare_fee_amount: Decimal
    quality_amount: Decimal
    transport_amount: Decimal
    globalgap_amount: Decimal
    taxable_base: Decimal
    vat_rate: Decimal
    withholding_rate: Decimal
    vat_amount: Decimal
    withholding_amount: Decimal
    total_amount: Decimal
    commercial_price: Decimal | None
    final_average_price: Decimal | None
    cod_art: int | None = None
    split_rule_id: int | None = None
    split_type: str | None = None
    warnings: tuple[str, ...] = ()

@dataclass(frozen=True)
class PersistencePreview:
    header: object
    lines: tuple[SplitPreviewLine, ...]
    fingerprint: str
    original_line_count: int
    warnings: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return bool(self.lines)

@dataclass(frozen=True)
class PendingRemittancePersistence:
    remittance: object
    calculation_result: object
    persistence_preview: PersistencePreview
    valid: bool
    warnings: tuple[str, ...] = ()
    output_directory: object | None = None

@dataclass(frozen=True)
class PendingBatchPersistence:
    batch_execution_id: str
    campaign: str
    company: str
    crop: str
    remittances: tuple[PendingRemittancePersistence, ...]
    total_original_lines: int
    total_final_lines: int
    warnings: tuple[str, ...]
    valid: bool
    excluded_remittances: tuple[object, ...] = ()

@dataclass(frozen=True)
class RemittancePersistenceSaveResult:
    remittance: object
    saved: bool
    batch: object | None = None
    error: str | None = None
    pdf_paths: tuple[object, ...] = ()

@dataclass(frozen=True)
class BatchPersistenceSaveResult:
    requested: int
    saved: int
    failed: int
    remittance_results: tuple[RemittancePersistenceSaveResult, ...]
    warnings: tuple[str, ...] = ()

@dataclass(frozen=True)
class PersistedLiquidation:
    id_liq: str
    recipient_member_id: int
    total_amount: Decimal

@dataclass(frozen=True)
class PersistenceBatch:
    batch_id: str
    status: str
    liquidations: tuple[PersistedLiquidation, ...]

@dataclass(frozen=True)
class SequenceState:
    crop: str
    campaign: str
    company: str
    prefix: str
    last_sequence: int
    initialized_from: str
    legacy_last_idliq: str | None = None
