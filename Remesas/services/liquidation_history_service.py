from __future__ import annotations

from datetime import datetime, timezone
import json


class LiquidationHistoryService:
    """Fachada de consulta posterior al guardado; la UI nunca accede a SQLite."""

    def __init__(self, repository, document_service, modification_service=None, csv_export_service=None):
        self.repository = repository
        self.document_service = document_service
        self.modification_service = modification_service
        self.csv_export_service = csv_export_service

    def list_batches(self, filters=None):
        return self.repository.list_batches(**(filters or {}))

    def list_history_filter_options(
        self,
        *,
        campaign=None,
        company=None,
        crop=None,
        remittance_id=None,
        member_id=None,
        status=None,
        date_from=None,
        date_to=None,
    ):
        return self.repository.list_history_filter_options(
            campaign=campaign,
            company=company,
            crop=crop,
            remittance_id=remittance_id,
            member_id=member_id,
            status=status,
            date_from=date_from,
            date_to=date_to,
        )

    def search_liquidation_members(self, text, **filters):
        return self.repository.search_liquidation_members(text, **filters)

    def history_summary(self, filters=None):
        return self.repository.history_summary(**(filters or {}))

    def filtered_batch_ids(self, filters=None):
        """Deliberately queries SQLite again: this is not the paged Treeview scope."""
        return tuple(row["batch_id"] for row in self.list_batches(filters))

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

    def export_csv(self, batch_id, *, member_id=None, user=None, force=False):
        if not self.csv_export_service: raise RuntimeError("La exportación CSV no está configurada")
        batch=self.repository.get_batch(batch_id)
        if batch and batch["modification_group_id"] and batch["operation_type"] in ("REVERSAL", "REPLACEMENT"):
            return self.csv_export_service.export_modification(batch["modification_group_id"], user=user, force=force)
        return self.csv_export_service.export_batch(batch_id, member_id=member_id, user=user, force=force)

    def export_csv_batches(self, batch_ids, *, user=None, force=False):
        if not self.csv_export_service: raise RuntimeError("La exportación CSV no está configurada")
        return self.csv_export_service.export_batches(batch_ids, user=user, force=force)

    def list_csv_exports(self, batch_id):
        return self.repository.list_csv_exports(batch_id=batch_id)

    def last_csv_export(self, batch_ids):
        selected = set(batch_ids)
        exports = self.repository.list_csv_exports()
        for export in exports:
            if export["status"] != "GENERATED":
                continue
            ids = set(json.loads(export["batch_ids_json"] or "[]")) if export["export_type"] == "MASS" else {export["batch_id"]}
            if ids == selected:
                return export
        return None

    def regenerate_csv_export(self, export_id, *, user=None):
        if not self.csv_export_service: raise RuntimeError("La exportación CSV no está configurada")
        return self.csv_export_service.regenerate_export(export_id, user=user)
