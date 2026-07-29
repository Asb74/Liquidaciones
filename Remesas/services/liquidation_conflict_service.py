"""One shared policy for individual and mass liquidation persistence."""
from __future__ import annotations

import logging
from domain.liquidation_conflicts import LiquidationConflict, LiquidationConflictType, LiquidationScope

logger = logging.getLogger(__name__)


class LiquidationConflictService:
    def __init__(self, repository):
        self.repository = repository

    @staticmethod
    def scope_from_header(header) -> LiquidationScope:
        return LiquidationScope(header.campana, header.empresa, header.cultivo,
                                int(header.remesa_id)).normalized()

    def inspect(self, scope: LiquidationScope) -> LiquidationConflict:
        scope = scope.normalized()
        batches = self.repository.list_active_batches_for_scope(scope)
        if not batches:
            kind = LiquidationConflictType.NONE
            result = LiquidationConflict(scope, kind)
        elif len(batches) > 1:
            kind = LiquidationConflictType.MULTIPLE_ACTIVE
            result = LiquidationConflict(scope, kind, warnings=(
                "Existen varias liquidaciones activas para esta remesa. Debe resolver la incidencia antes de continuar.",))
        else:
            batch = batches[0]
            exports = self.repository.list_accounting_exports_for_batch(batch["batch_id"])
            exported = bool(exports)
            kind = LiquidationConflictType.ACTIVE_EXPORTED if exported else LiquidationConflictType.ACTIVE_NOT_EXPORTED
            result = LiquidationConflict(scope, kind, batch["batch_id"], batch["status"],
                batch["operation_type"], exported, tuple(row["id"] for row in exports))
        logger.info("[LiquidationConflict] scope=%s existing_batch_id=%s exported=%s conflict_type=%s",
                    scope, result.existing_batch_id, result.exported_to_accounting, result.conflict_type.value)
        return result

    def classify_many(self, scopes):
        return tuple(self.inspect(scope) for scope in scopes)
