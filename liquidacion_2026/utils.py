"""Utilidades compartidas para normalización de valores."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


def parse_decimal(value: object) -> Decimal:
    """Convierte valores europeos o numéricos en ``Decimal`` robusto."""
    if value is None:
        return Decimal("0")

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        return Decimal(str(value))

    value = str(value).strip()
    if value == "":
        return Decimal("0")

    value = value.replace(".", "")
    value = value.replace(",", ".")

    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"No se puede convertir a Decimal: {value}") from exc

