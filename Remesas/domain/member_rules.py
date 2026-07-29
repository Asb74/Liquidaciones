"""Central, defensive eligibility rule for liquidation members."""
from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from data.excluded_member_repository import ExcludedMemberRepository

SYSTEM_MEMBER_ID = 0
SYSTEM_MEMBER_EXCLUDED_MESSAGE = "Socio excluido por configuración de DSocio.Tipo = OTROS."
SYSTEM_MEMBER_ZERO_REASON = "SYSTEM_MEMBER_ZERO"
DSOCIO_TIPO_OTROS_REASON = "DSOCIO_TIPO_OTROS"


def _member_number(member_id: object) -> int | None:
    if member_id is None or (isinstance(member_id, str) and not member_id.strip()):
        return None
    try:
        value = Decimal(str(member_id).strip())
    except (InvalidOperation, ValueError):
        return None
    return int(value) if value.is_finite() and value == value.to_integral_value() else None


class ExcludedMemberService:
    """Cached application-wide rule: technical member zero and ``Tipo=OTROS``."""
    def __init__(self, repository: ExcludedMemberRepository | None = None) -> None:
        self.repository = repository
        self._excluded_member_ids: frozenset[int] = frozenset()
        self.logger = logging.getLogger(__name__)
        if repository is not None:
            self.refresh_excluded_members()

    def set_repository(self, repository: ExcludedMemberRepository, *, refresh: bool = True) -> None:
        self.repository = repository
        if refresh:
            self.refresh_excluded_members()

    def refresh_excluded_members(self) -> frozenset[int]:
        self._excluded_member_ids = self.repository.list_members_with_type_other() if self.repository else frozenset()
        self.logger.info("[EXCLUDED_MEMBER_RULE_REFRESHED] reason=%s excluded_members=%s", DSOCIO_TIPO_OTROS_REASON, len(self._excluded_member_ids))
        return self.get_excluded_member_ids()

    def invalidate_cache(self) -> None:
        self._excluded_member_ids = frozenset()

    def get_excluded_member_ids(self) -> frozenset[int]:
        return frozenset({SYSTEM_MEMBER_ID, *self._excluded_member_ids})

    def is_excluded_member(self, member_id: object) -> bool:
        value = _member_number(member_id)
        return value is not None and value in self.get_excluded_member_ids()

    def filter_eligible_member_ids(self, member_ids: Iterable[object]) -> tuple[int, ...]:
        return tuple(value for item in member_ids if (value := _member_number(item)) is not None and not self.is_excluded_member(value))

    def reason_for_exclusion(self, member_id: object) -> str | None:
        value = _member_number(member_id)
        if value == SYSTEM_MEMBER_ID:
            return SYSTEM_MEMBER_ZERO_REASON
        if value in self._excluded_member_ids:
            return DSOCIO_TIPO_OTROS_REASON
        return None


excluded_member_service = ExcludedMemberService()


def configure_excluded_members(*, db_path: str | Path | None = None, connection=None, refresh: bool = True) -> ExcludedMemberService:
    """Configure/reload the central cache after opening or synchronizing DBEEPPL."""
    if db_path is not None or connection is not None:
        excluded_member_service.set_repository(ExcludedMemberRepository(db_path, connection), refresh=refresh)
    elif refresh:
        excluded_member_service.refresh_excluded_members()
    return excluded_member_service


def refresh_excluded_members() -> frozenset[int]:
    return excluded_member_service.refresh_excluded_members()


def is_excluded_member(member_id: object) -> bool:
    return excluded_member_service.is_excluded_member(member_id)


def log_system_member_excluded(logger: logging.Logger, *, origin: str, count: int, net_kg=0, amount=0, batch_id=None, remesa_id=None) -> None:
    """One grouped audit log event per operation, never one per delivery."""
    logger.warning("[EXCLUDED_MEMBER_DELIVERIES_SKIPPED] origin=%s records=%s batch_id=%s remesa_id=%s net_kg_excluded=%s amount_excluded=%s", origin, count, batch_id, remesa_id, net_kg, amount)
