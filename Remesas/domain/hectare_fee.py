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
    member_id: int
    applicable_hectares: Decimal
    price_per_hectare: Decimal
    total_member_fee: Decimal
    total_effective_kg: Decimal
    rate_per_kg: Decimal | None
    line_effective_net_kg: Decimal
    line_fee: Decimal
    status: CalculationStatus
    warnings: tuple[str, ...] = ()


def calculate_line_hectare_fee(line_effective_net_kg: Decimal, rate_per_kg: Decimal) -> Decimal:
    return round_money(line_effective_net_kg * rate_per_kg)


def allocate_hectare_fees(total_fee: Decimal, rate_per_kg: Decimal, lines: list[tuple[int, Decimal]]) -> tuple[dict[int, Decimal], Decimal]:
    """Backward-compatible helper: calculate partial line fees without annual balancing."""
    amounts = {idx: calculate_line_hectare_fee(kg, rate_per_kg) for idx, kg in lines}
    difference = total_fee - sum(amounts.values(), Decimal("0"))
    return amounts, difference
