from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
import unicodedata

_TRUE_LIQUIDATED = {"S", "SI", "SÍ", "Y", "YES", "1", "TRUE", "LIQUIDADO"}
_FALSE_LIQUIDATED = {"", "N", "NO", "0", "FALSE"}
_TRUE_YES_NO = {"S", "SI", "Y", "YES", "1", "TRUE", "X"}
_FALSE_YES_NO = {"", "N", "NO", "0", "FALSE", "NULL", "NONE"}


def _normalize_token(value: object) -> str:
    text = str(value or "").strip().upper()
    return "".join(ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn")


def parse_yes_no(value: object) -> bool:
    """Parse Access/VB yes-no flags stored with heterogeneous values."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, (float, Decimal)):
        try:
            return Decimal(str(value)) != 0
        except InvalidOperation:
            return False
    token = _normalize_token(value)
    if token in _TRUE_YES_NO:
        return True
    if token in _FALSE_YES_NO:
        return False
    return False


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


def decimal_or_zero(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal("1") if value else Decimal("0")
    text = str(value).strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text.replace(",", "."))
    except InvalidOperation as exc:
        raise ValueError(f"Valor numérico no válido: {value!r}") from exc


def to_decimal(value: Any) -> Decimal:
    return decimal_or_zero(value)


def format_integer_es(value: Decimal | int) -> str:
    amount = Decimal(str(value or 0)).quantize(Decimal("1"), ROUND_HALF_UP)
    return f"{int(amount):,}".replace(",", ".")


def format_decimal_es(value: Decimal, decimals: int = 2) -> str:
    q = Decimal("1").scaleb(-decimals)
    text = f"{Decimal(str(value or 0)).quantize(q, ROUND_HALF_UP):,.{decimals}f}"
    return text.replace(",", "_").replace(".", ",").replace("_", ".")


def format_currency_es(value: Decimal) -> str:
    return f"{format_decimal_es(value, 2)} €"


def format_price_es(value: Decimal, decimals: int = 5) -> str:
    return format_decimal_es(value, decimals)


def round_money(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def round_price(value: Decimal) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)


def format_percentage_es(value: Decimal, decimals: int = 2) -> str:
    return f"{format_decimal_es(value, decimals)} %"


def get_price_labels(crop: str) -> list[str]:
    normalized = _normalize_token(crop)
    if normalized in {"MANDARINA", "CLEMENTINA", "NARANJA", "CITRICOS", "CITRICO"}:
        return ["I AAA", "I AA", "I A", "I B", "I C", "I D", "II AAA", "II AA", "II A", "II B", "II C", "II D"]
    return [f"P{i}" for i in range(12)]


def get_grade_labels(crop: str) -> list[str]:
    return get_price_labels(crop)


def safe_path_part(value: object) -> str:
    text = str(value or "sin_nombre").strip().replace(" ", "_")
    invalid = '<>:"/\\|?*'
    for ch in invalid:
        text = text.replace(ch, "_")
    return text.strip("._") or "sin_nombre"
