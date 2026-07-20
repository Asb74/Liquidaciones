"""Central business rules for members that are not real recipients."""
from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

SYSTEM_MEMBER_ID = 0
EXCLUDED_MEMBER_IDS = frozenset({SYSTEM_MEMBER_ID})
SYSTEM_MEMBER_EXCLUDED_MESSAGE = "El socio 0 es un registro técnico excluido."


def is_excluded_member(member_id: object) -> bool:
    """Recognise zero in safe numeric representations, but not blank values."""
    if member_id is None or (isinstance(member_id, str) and not member_id.strip()):
        return False
    try:
        value = Decimal(str(member_id).strip())
    except (InvalidOperation, ValueError):
        return False
    return value.is_finite() and value == value.to_integral_value() and int(value) in EXCLUDED_MEMBER_IDS


def log_system_member_excluded(logger: logging.Logger, *, origin: str, count: int,
                               net_kg=0, amount=0, batch_id=None, remesa_id=None) -> None:
    """Log a single audit event for an operation rather than one per row."""
    logger.warning("[SYSTEM_MEMBER_EXCLUDED] origin=%s records=%s batch_id=%s remesa_id=%s net_kg_excluded=%s amount_excluded=%s",
                   origin, count, batch_id, remesa_id, net_kg, amount)
