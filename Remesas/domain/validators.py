from __future__ import annotations

from datetime import date, datetime

from .models import Period, WorkContext

_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S")


def parse_user_date(value: str) -> date:
    text = (value or "").strip()
    if not text:
        raise ValueError("La fecha es obligatoria.")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date()
    except ValueError as exc:
        raise ValueError(f"Formato de fecha no válido: {value}") from exc


def validate_context(context: WorkContext) -> None:
    missing = [name for name, val in (("campaña", context.campana), ("empresa", context.empresa), ("cultivo", context.cultivo)) if not str(val).strip()]
    if missing:
        raise ValueError("Debe seleccionar " + ", ".join(missing) + ".")


def validate_period(period: Period) -> None:
    if period.start > period.end:
        raise ValueError("La fecha inicial es posterior a la fecha final.")
