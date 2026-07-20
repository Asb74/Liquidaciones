from __future__ import annotations

from datetime import datetime, timezone


class LiquidationHistoryService:
    """Fachada de consulta posterior al guardado; la UI nunca accede a SQLite."""

    def __init__(self, repository, document_service, modification_service=None):
        self.repository = repository
        self.document_service = document_service
        self.modification_service = modification_service

    def list_batches(self, filters=None):
        return self.repository.list_batches(**(filters or {}))

    def get_batch_detail(self, batch_id):
        return {"batch": self.repository.get_batch(batch_id), "lines": self.repository.list_batch_liquidations(batch_id),
                "chain": self.repository.list_modification_chain(batch_id)}

    def list_documents(self, batch_id):
        return self.repository.list_batch_documents(batch_id)

    def list_recipient_documents(self, batch_id):
        return tuple(self.repository.list_latest_batch_documents(batch_id))

    def void_batch(self, batch_id, reason, user=None):
        reason = str(reason or "").strip()
        if not reason: raise ValueError("El motivo de anulación es obligatorio")
        if self.modification_service:
            return self.modification_service.void(batch_id, reason, user=user)
        now = datetime.now(timezone.utc).isoformat()
        if not self.repository.mark_batch_voided(batch_id, reason=reason, user=user, voided_at=now):
            raise ValueError("El batch no existe o ya está anulado")
        self.repository.mark_lines_voided(batch_id, reason=reason, user=user, voided_at=now)
        self.repository.supersede_batch_documents(batch_id)
        self.repository.audit(batch_id, "VOID", '{"reason": %r}' % reason, user)

    def modify_batch(self, batch_id, replacement_preview, user=None):
        if not self.modification_service:
            raise RuntimeError("El servicio de rectificación no está configurado")
        return self.modification_service.modify(batch_id, replacement_preview, user=user)

    def regenerate_documents(self, batch_id, recipient_member_id=None):
        return self.document_service.regenerate_documents(batch_id, recipient_member_id=recipient_member_id)
