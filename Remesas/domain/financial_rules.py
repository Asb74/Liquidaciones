from __future__ import annotations

from decimal import Decimal
from typing import Any

from domain.utils import decimal_or_zero, round_money

EFFECTIVE_NET_SQL = "CASE WHEN COALESCE({alias}.NetoPartida, 0) = 0 THEN COALESCE({alias}.Neto, 0) ELSE {alias}.NetoPartida END"


def effective_net_kg(net_kg: Any, batch_net_kg: Any | None) -> Decimal:
    batch_value = decimal_or_zero(batch_net_kg)
    if batch_value == Decimal("0"):
        return decimal_or_zero(net_kg)
    return batch_value


def calculate_quality_adjustment(effective_net: Decimal, quality_rate: Decimal, apply_quality: bool) -> Decimal:
    if not apply_quality:
        return Decimal("0")
    return effective_net * quality_rate


def calculate_total_hectare_fee(applicable_hectares: Decimal, price_per_hectare: Decimal) -> Decimal:
    return round_money(applicable_hectares * price_per_hectare)
