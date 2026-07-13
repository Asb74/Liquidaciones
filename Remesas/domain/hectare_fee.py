from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from domain.utils import round_money


class CalculationStatus(Enum):
    CALCULATED = "calculated"
    NOT_APPLICABLE = "not_applicable"
    DISABLED = "disabled"
    PENDING = "pending"
    ERROR = "error"


@dataclass(frozen=True)
class HectareFeeCalculation:
    member_id: int
    applicable_hectares: Decimal
    price_per_hectare: Decimal
    total_fee: Decimal
    total_effective_kg: Decimal
    rate_per_kg: Decimal | None
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class HectareFeeResult:
    status: CalculationStatus
    applicable_hectares: Decimal
    price_per_hectare: Decimal
    total_fee: Decimal
    total_effective_kg: Decimal
    rate_per_kg: Decimal | None
    applied_amount: Decimal
    detected_amount: Decimal = Decimal("0")
    rounding_adjustment: Decimal = Decimal("0")
    warnings: tuple[str, ...] = ()


def allocate_hectare_fees(total_fee: Decimal, rate_per_kg: Decimal, lines: list[tuple[int, Decimal]]) -> tuple[dict[int, Decimal], Decimal]:
    amounts = {idx: round_money(kg * rate_per_kg) for idx, kg in lines}
    difference = total_fee - sum(amounts.values(), Decimal("0"))
    if difference and lines:
        target_idx = max(lines, key=lambda item: (item[1], -item[0]))[0]
        amounts[target_idx] = amounts[target_idx] + difference
    return amounts, difference
