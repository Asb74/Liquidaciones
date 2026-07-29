"""Pure, shared arithmetic for every varietal benchmark producer."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable


def positive_decimal(value) -> Decimal | None:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() and result > 0 else None


def kilograms_per_hectare(kilograms, hectares) -> Decimal | None:
    """Canonical kg/ha formula; callers must not implement a second division."""
    kg, ha = positive_decimal(kilograms), positive_decimal(hectares)
    return kg / ha if kg is not None and ha is not None else None


@dataclass(frozen=True)
class MetricStatistics:
    maximum: Decimal | None
    average: Decimal | None
    minimum: Decimal | None
    count: int


def metric_statistics(values: Iterable[object]) -> MetricStatistics:
    valid = tuple(value for raw in values if (value := positive_decimal(raw)) is not None)
    if not valid:
        return MetricStatistics(None, None, None, 0)
    return MetricStatistics(max(valid), sum(valid, Decimal(0)) / len(valid), min(valid), len(valid))
