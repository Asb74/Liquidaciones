"""Canonical business identity and duplicate-conflict results."""
from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class LiquidationScope:
    campaign: str
    company: str
    crop: str
    remittance_id: int

    def normalized(self):
        return LiquidationScope(str(self.campaign).strip(), str(self.company).strip(),
                                str(self.crop).strip().upper(), int(self.remittance_id))


class LiquidationConflictType(str, Enum):
    NONE = "NONE"
    ACTIVE_NOT_EXPORTED = "ACTIVE_NOT_EXPORTED"
    ACTIVE_EXPORTED = "ACTIVE_EXPORTED"
    MULTIPLE_ACTIVE = "MULTIPLE_ACTIVE"
    RECTIFICATION_IN_PROGRESS = "RECTIFICATION_IN_PROGRESS"
    INVALID_STATE = "INVALID_STATE"


@dataclass(frozen=True)
class LiquidationConflict:
    scope: LiquidationScope
    conflict_type: LiquidationConflictType
    existing_batch_id: str | None = None
    existing_status: str | None = None
    operation_type: str | None = None
    exported_to_accounting: bool = False
    export_ids: tuple[int, ...] = ()
    warnings: tuple[str, ...] = ()
