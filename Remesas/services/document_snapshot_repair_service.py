"""Idempotent repair for malformed v4 document snapshots."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging

from domain.utils import round_price

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotRepairReport:
    invalid: int = 0
    repaired: int = 0
    unresolved: int = 0


def repair_invalid_v4_snapshots(database, remittance_connection) -> SnapshotRepairReport:
    """Recover fixed prices exclusively from PagosCIT, never from amount/kg."""
    invalid = repaired = unresolved = 0
    with database.connect() as conn:
        rows = conn.execute("""SELECT s.batch_id,s.recipient_member_id,s.payload_json,b.remesa_id
            FROM liquidation_document_snapshots s JOIN liquidation_batches b ON b.batch_id=s.batch_id
            WHERE s.schema_version>=4""").fetchall()
        for row in rows:
            try:
                raw = json.loads(row["payload_json"]); model = raw.get("model", {})
            except (TypeError, ValueError, json.JSONDecodeError):
                raw, model = {"schema_version": 4, "model": {}}, {}
            if model.get("national_market_price") is not None and model.get("rotten_leaves_price") is not None:
                continue
            invalid += 1; status = "UNRECOVERABLE"; national = rotten = None
            details = "No se encontraron precios válidos en la remesa original"
            try:
                source = remittance_connection.execute(
                    "SELECT PDESTRIO,PDMESA,PPODRIDO FROM PagosCIT WHERE IdREMESA=?", (row["remesa_id"],)
                ).fetchone()
                if source is None or any(value is None for value in source): raise ValueError(details)
                national = round_price(Decimal(str(source[0])))
                table = round_price(Decimal(str(source[1])))
                rotten = round_price(Decimal(str(source[2])))
                if national != table: raise ValueError("PDESTRIO y PDMESA no coinciden")
                model.update(national_market_price=format(national, "f"), destruction_price=format(national, "f"),
                             secondary_price=format(national, "f"), rotten_leaves_price=format(rotten, "f"),
                             rotten_price=format(rotten, "f"), waste_price=format(rotten, "f"))
                raw["model"] = model
                conn.execute("UPDATE liquidation_document_snapshots SET payload_json=? WHERE batch_id=? AND recipient_member_id=?",
                    (json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")), row["batch_id"], row["recipient_member_id"]))
                status, details = "REPAIRED", "Precios recuperados de PagosCIT"; repaired += 1
            except Exception as exc:
                details = str(exc); unresolved += 1
            conn.execute("""INSERT INTO document_snapshot_repair_incidents
                (batch_id,recipient_member_id,remesa_id,status,national_market_price,rotten_leaves_price,details,updated_at)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(batch_id,recipient_member_id) DO UPDATE SET
                remesa_id=excluded.remesa_id,status=excluded.status,national_market_price=excluded.national_market_price,
                rotten_leaves_price=excluded.rotten_leaves_price,details=excluded.details,updated_at=excluded.updated_at""",
                (row["batch_id"], row["recipient_member_id"], row["remesa_id"], status,
                 None if national is None else format(national, "f"), None if rotten is None else format(rotten, "f"), details,
                 datetime.now(timezone.utc).isoformat()))
            logger.log(logging.INFO if status == "REPAIRED" else logging.ERROR,
                "[DocumentSnapshotRepair] batch_id=%s recipient_member_id=%s remesa_id=%s estado=%s national_market_price=%s rotten_leaves_price=%s details=%s",
                row["batch_id"], row["recipient_member_id"], row["remesa_id"], status, national, rotten, details)
    return SnapshotRepairReport(invalid, repaired, unresolved)
