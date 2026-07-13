from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

_TRUE_LIQUIDATED = {"S", "SI", "SÍ", "Y", "YES", "1", "TRUE", "LIQUIDADO"}
_FALSE_LIQUIDATED = {"", "N", "NO", "0", "FALSE"}


def is_liquidated(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value)) != 0
        except InvalidOperation:
            return False
    text = str(value).strip().upper()
    if text in _TRUE_LIQUIDATED:
        return True
    if text in _FALSE_LIQUIDATED:
        return False
    return False


def format_file_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M")


def format_display_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text[:19] if "%H" in fmt else text[:10], fmt).strftime("%d/%m/%Y")
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).strftime("%d/%m/%Y")
    except ValueError:
        return text


def to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value).replace(",", "."))
    except InvalidOperation:
        return Decimal("0")
