"""Immutable accounting rectifications backed exclusively by persisted rows."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from domain.member_rules import SYSTEM_MEMBER_ID


MONEY_COLUMNS = ("neto", "imp_bruto", "recoleccion", "cuota_ha", "bp_calidad",
                 "b_transporte", "b_global", "base_i", "importe_total")


class LiquidationModificationService:
    """Creates reversal movements without ever recalculating the original batch.

    The replacement is deliberately supplied as a newly calculated persistence
    preview.  This keeps calculation rules outside this accounting workflow.
    """
    def __init__(self, persistence_service) -> None:
        self.persistence = persistence_service
        self.database = persistence_service.database

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat()

    def _audit(self, conn, batch_id, action, group_id, user):
        conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",
                     (batch_id, action, "BATCH", batch_id, json.dumps({"modification_group_id": group_id}), self._now(), user))

    def _create_reversal(self, original_batch_id, group_id, user, *, replacement_batch_id=None):
        """Copy the stored records, changing only amount fields and identity."""
        conn = self.database.connect(); reversal_id = str(uuid.uuid4()); now = self._now()
        try:
            conn.execute("BEGIN IMMEDIATE")
            original = conn.execute("SELECT * FROM liquidation_batches WHERE batch_id=?", (original_batch_id,)).fetchone()
            if not original: raise ValueError("El batch original no existe")
            conn.execute("""INSERT INTO liquidation_batches
                (batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,calculation_fingerprint,original_line_count,final_line_count,status,created_at,created_by,operation_type,original_batch_id,replacement_batch_id,modification_group_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (reversal_id, original["remesa_id"], original["remesa_name"], original["campaign"], original["company"], original["crop"], original["payment_date"],
                 f"REVERSAL:{group_id}", original["original_line_count"], original["final_line_count"], "ACTIVE", now, user, "REVERSAL", original_batch_id, replacement_batch_id, group_id))
            rows = conn.execute("SELECT * FROM liquidaciones WHERE batch_id=? AND recipient_member_id<>? AND id_socio<>? ORDER BY id", (original_batch_id, SYSTEM_MEMBER_ID, SYSTEM_MEMBER_ID)).fetchall()
            for row in rows:
                data = dict(row)
                new_id = self.persistence._next_id(conn, data["cultivo"], data["campana"], data["empresa"], user, reversal_id)
                for name in MONEY_COLUMNS:
                    data[name] = format(-Decimal(data[name]), "f")
                data.update(id=None, id_liq=new_id, batch_id=reversal_id, status="ACTIVE", created_at=now,
                            created_by=user, calculation_fingerprint=f"REVERSAL:{group_id}", operation_type="REVERSAL",
                            original_batch_id=original_batch_id, original_id_liq=row["id_liq"],
                            replacement_batch_id=replacement_batch_id, replacement_id_liq=None, modification_group_id=group_id,
                            voided_at=None, voided_by=None, void_reason=None)
                columns = tuple(data)
                conn.execute("INSERT INTO liquidaciones(" + ",".join(columns) + ") VALUES(" + ",".join("?" for _ in columns) + ")",
                             tuple(data[c] for c in columns))
            self._audit(conn, reversal_id, "REVERSAL_CREATED", group_id, user)
            conn.commit()
        except Exception:
            conn.rollback(); raise
        finally:
            conn.close()
        return reversal_id

    def modify(self, original_batch_id, replacement_preview, *, user=None):
        """Supersede an active batch with a reversal and a freshly calculated batch."""
        group_id = str(uuid.uuid4()); now = self._now()
        with self.database.connect() as conn:
            if not conn.execute("SELECT 1 FROM liquidation_batches WHERE batch_id=? AND status='ACTIVE'", (original_batch_id,)).fetchone():
                raise ValueError("Solo se puede rectificar un batch ACTIVE")
            self._audit(conn, original_batch_id, "MODIFICATION_STARTED", group_id, user)
            conn.execute("UPDATE liquidation_batches SET status='SUPERSEDED' WHERE batch_id=?", (original_batch_id,))
            conn.execute("UPDATE liquidaciones SET status='SUPERSEDED' WHERE batch_id=? AND status='ACTIVE'", (original_batch_id,))
            conn.execute("UPDATE generated_documents SET status='SUPERSEDED' WHERE batch_id=? AND status='GENERATED'", (original_batch_id,))
        replacement = self.persistence.save(replacement_preview, user=user)
        reversal_id = self._create_reversal(original_batch_id, group_id, user, replacement_batch_id=replacement.batch_id)
        with self.database.connect() as conn:
            conn.execute("UPDATE liquidation_batches SET operation_type='REPLACEMENT',original_batch_id=?,modification_group_id=? WHERE batch_id=?", (original_batch_id, group_id, replacement.batch_id))
            conn.execute("UPDATE liquidaciones SET operation_type='REPLACEMENT',original_batch_id=?,replacement_batch_id=?,modification_group_id=? WHERE batch_id=?", (original_batch_id, replacement.batch_id, group_id, replacement.batch_id))
            conn.execute("UPDATE liquidation_batches SET replacement_batch_id=? WHERE batch_id=?", (replacement.batch_id, original_batch_id))
            conn.execute("UPDATE liquidaciones SET replacement_batch_id=? WHERE batch_id=?", (replacement.batch_id, original_batch_id))
            conn.execute("UPDATE liquidaciones SET replacement_id_liq=(SELECT p.id_liq FROM liquidaciones p WHERE p.batch_id=? AND p.recipient_member_id=liquidaciones.recipient_member_id LIMIT 1) WHERE batch_id=?", (replacement.batch_id, reversal_id))
            self._audit(conn, replacement.batch_id, "REPLACEMENT_CREATED", group_id, user)
            self._audit(conn, original_batch_id, "ORIGINAL_SUPERSEDED", group_id, user)
        return {"original_batch_id": original_batch_id, "reversal_batch_id": reversal_id, "replacement_batch_id": replacement.batch_id, "modification_group_id": group_id}

    def void(self, original_batch_id, reason, *, user=None):
        """Cancellation is an immutable reversal only (no replacement batch)."""
        if not str(reason).strip(): raise ValueError("El motivo de anulación es obligatorio")
        group_id = str(uuid.uuid4()); now = self._now()
        with self.database.connect() as conn:
            if conn.execute("UPDATE liquidation_batches SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'", (now,user,reason.strip(),original_batch_id)).rowcount != 1:
                raise ValueError("El batch no existe o no está activo")
            conn.execute("UPDATE liquidaciones SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'", (now,user,reason.strip(),original_batch_id))
            conn.execute("UPDATE generated_documents SET status='SUPERSEDED' WHERE batch_id=? AND status='GENERATED'", (original_batch_id,))
        return self._create_reversal(original_batch_id, group_id, user)
