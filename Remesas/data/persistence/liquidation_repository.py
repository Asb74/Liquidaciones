from __future__ import annotations

import logging

from data.persistence.database import PersistenceDatabase
from data.persistence.search_text import normalize_search_text
from domain.member_rules import SYSTEM_MEMBER_ID, is_excluded_member


logger = logging.getLogger(__name__)


class LiquidationRepository:
    """Único punto de lectura/escritura documental de la SQLite local."""

    def __init__(self, database: PersistenceDatabase) -> None:
        self.database = database

    def get_batch(self, batch_id: str):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidation_batches WHERE batch_id=?", (batch_id,)).fetchone()

    def save_document_snapshot(self, *, batch_id: str, recipient_member_id: int, payload_json: str, schema_version: int, calculation_fingerprint: str, created_at: str, created_by: str | None = None) -> None:
        with self.database.connect() as conn:
            conn.execute("INSERT OR REPLACE INTO liquidation_document_snapshots(batch_id,recipient_member_id,payload_json,schema_version,calculation_fingerprint,created_at,created_by) VALUES(?,?,?,?,?,?,?)", (batch_id,recipient_member_id,payload_json,schema_version,calculation_fingerprint,created_at,created_by))

    def get_document_snapshot(self, batch_id: str, recipient_member_id: int):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidation_document_snapshots WHERE batch_id=? AND recipient_member_id=?", (batch_id,recipient_member_id)).fetchone()

    def list_batch_liquidations(self, batch_id: str):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidaciones WHERE batch_id=? AND recipient_member_id<>? AND id_socio<>? ORDER BY recipient_member_id,id", (batch_id, SYSTEM_MEMBER_ID, SYSTEM_MEMBER_ID)).fetchall()

    _CSV_COLUMNS = "id,id_liq,fecha,cultivo,campana,empresa,id_socio,socio,cod_art,variedad,neto,imp_bruto,precio_comer,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,precio_medio,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo"

    def list_csv_rows_for_batch(self, batch_id: str, member_id: int | None = None):
        clauses = ["batch_id=?", "status NOT IN ('VOIDED','SUPERSEDED')", "recipient_member_id<>?", "id_socio<>?"]
        args = [batch_id, SYSTEM_MEMBER_ID, SYSTEM_MEMBER_ID]
        if member_id is not None:
            clauses.append("id_socio=?"); args.append(member_id)
        with self.database.connect() as conn:
            return conn.execute(f"SELECT {self._CSV_COLUMNS} FROM liquidaciones WHERE {' AND '.join(clauses)} ORDER BY id ASC", args).fetchall()

    def export_batches(self, batch_ids):
        """Return active CSV rows for the selected batches in accounting order."""
        batch_ids = tuple(dict.fromkeys(batch_ids))
        if not batch_ids:
            return ()
        placeholders = ",".join("?" for _ in batch_ids)
        with self.database.connect() as conn:
            return conn.execute(
                f"""SELECT {self._CSV_COLUMNS}, b.remesa_id AS remittance_id FROM liquidaciones l
                JOIN liquidation_batches b ON b.batch_id=l.batch_id
                WHERE l.batch_id IN ({placeholders})
                  AND l.status NOT IN ('VOIDED','SUPERSEDED')
                  AND b.status IN ('ACTIVE','PARTIAL')
                  AND l.recipient_member_id <> {SYSTEM_MEMBER_ID} AND l.id_socio <> {SYSTEM_MEMBER_ID}
                ORDER BY b.campaign, b.company, b.crop, b.remesa_id,
                  CASE l.operation_type WHEN 'REVERSAL' THEN 0 WHEN 'REPLACEMENT' THEN 1 ELSE 2 END, l.id""",
                batch_ids,
            ).fetchall()

    def list_csv_rows_for_modification(self, modification_group_id: str):
        with self.database.connect() as conn:
            return conn.execute(f"SELECT {self._CSV_COLUMNS},operation_type,batch_id FROM liquidaciones WHERE modification_group_id=? AND operation_type IN ('REVERSAL','REPLACEMENT') AND recipient_member_id<>? AND id_socio<>? ORDER BY CASE operation_type WHEN 'REVERSAL' THEN 0 ELSE 1 END,id ASC", (modification_group_id, SYSTEM_MEMBER_ID, SYSTEM_MEMBER_ID)).fetchall()

    def get_csv_export(self, export_id: int):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM accounting_exports WHERE id=?", (export_id,)).fetchone()

    def list_csv_exports(self, batch_id=None, modification_group_id=None):
        clauses=[]; args=[]
        if batch_id is not None: clauses.append("batch_id=?"); args.append(batch_id)
        if modification_group_id is not None: clauses.append("modification_group_id=?"); args.append(modification_group_id)
        sql="SELECT * FROM accounting_exports" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY id DESC"
        with self.database.connect() as conn: return conn.execute(sql,args).fetchall()

    def find_generated_csv_export(self, *, batch_id, modification_group_id, member_id, export_type, source_fingerprint):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM accounting_exports WHERE batch_id IS ? AND modification_group_id IS ? AND member_id IS ? AND export_type=? AND source_fingerprint=? AND status='GENERATED' ORDER BY id DESC LIMIT 1", (batch_id, modification_group_id, member_id, export_type, source_fingerprint)).fetchone()

    def record_csv_export(self, **values) -> int:
        from data.persistence.migrations import utcnow
        with self.database.connect() as conn:
            attempt = conn.execute("SELECT COALESCE(MAX(generation_attempt),0) FROM accounting_exports WHERE batch_id IS ? AND modification_group_id IS ? AND member_id IS ? AND export_type=?", (values.get("batch_id"),values.get("modification_group_id"),values.get("member_id"),values["export_type"])).fetchone()[0] + 1
            cursor=conn.execute("INSERT INTO accounting_exports(batch_id,modification_group_id,remittance_id,member_id,export_type,file_path,info_file_path,status,line_count,excluded_line_count,net_total,amount_total,file_hash,source_fingerprint,generated_at,created_at,created_by,error_message,generation_attempt,supersedes_export_id,batch_ids_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (values.get("batch_id"),values.get("modification_group_id"),values.get("remittance_id"),values.get("member_id"),values["export_type"],values.get("file_path", ""),values.get("info_file_path"),values["status"],values.get("line_count",0),values.get("excluded_line_count",0),values.get("net_total"),values.get("amount_total"),values.get("file_hash"),values.get("source_fingerprint"),values.get("generated_at"),utcnow(),values.get("created_by"),values.get("error_message"),attempt,values.get("supersedes_export_id"),values.get("batch_ids_json")))
            return cursor.lastrowid

    def mark_csv_export_superseded(self, export_id: int) -> None:
        with self.database.connect() as conn: conn.execute("UPDATE accounting_exports SET status='SUPERSEDED' WHERE id=? AND status='GENERATED'", (export_id,))

    def list_modification_chain(self, batch_id: str):
        """All persisted movements linked to the selected batch, for history/detail UI."""
        with self.database.connect() as conn:
            batch = conn.execute("SELECT * FROM liquidation_batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not batch: return ()
            root = batch["original_batch_id"] or batch_id
            return conn.execute("""SELECT * FROM liquidation_batches
              WHERE batch_id=? OR original_batch_id=? OR replacement_batch_id=?
              ORDER BY created_at""", (root, root, root)).fetchall()

    def list_active_batches_for_remittance(self, remittance_id: int):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidation_batches WHERE remesa_id=? AND status='ACTIVE' ORDER BY created_at DESC", (remittance_id,)).fetchall()

    def list_recipient_lines(self, batch_id: str, recipient_member_id: int):
        with self.database.connect() as conn:
            if is_excluded_member(recipient_member_id): return ()
            return conn.execute("SELECT * FROM liquidaciones WHERE batch_id=? AND recipient_member_id=? AND id_socio<>? ORDER BY id", (batch_id, recipient_member_id, SYSTEM_MEMBER_ID)).fetchall()

    def list_batches(self, **filters):
        clauses=[]; args=[]
        mapping={"status":"b.status","campaign":"b.campaign","company":"b.company","crop":"b.crop","remittance_id":"b.remesa_id"}
        for key,column in mapping.items():
            if filters.get(key) not in (None, ""):
                clauses.append(f"{column}=?"); args.append(filters[key])
        if filters.get("member_id") not in (None, ""):
            if is_excluded_member(filters["member_id"]): return ()
            clauses.append("EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id=? AND l.id_socio<>?)"); args.extend((filters["member_id"], SYSTEM_MEMBER_ID))
        clauses.append("(NOT EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id=b.batch_id) OR EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id<>0 AND l.id_socio<>0))")
        if filters.get("date_from"): clauses.append("substr(b.payment_date,1,10)>=?"); args.append(filters["date_from"])
        if filters.get("date_to"): clauses.append("substr(b.payment_date,1,10)<=?"); args.append(filters["date_to"])
        sql = """SELECT b.*,
          (SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id<>0 AND l.id_socio<>0) line_count,
          (SELECT COUNT(DISTINCT recipient_member_id) FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id<>0 AND l.id_socio<>0) recipient_count,
          (SELECT COUNT(*) FROM generated_documents d WHERE d.batch_id=b.batch_id AND d.status='GENERATED' AND d.recipient_member_id<>0) document_count
          FROM liquidation_batches b"""
        if clauses: sql += " WHERE " + " AND ".join(clauses)
        with self.database.connect() as conn:
            return conn.execute(sql + " ORDER BY b.created_at DESC", tuple(args)).fetchall()

    @staticmethod
    def _history_clauses(filters, *, batch_alias="b"):
        """Build the one canonical history scope used by the UI and exports."""
        clauses, args = [], []
        mapping={"status":f"{batch_alias}.status","campaign":f"{batch_alias}.campaign","company":f"{batch_alias}.company","crop":f"{batch_alias}.crop","remittance_id":f"{batch_alias}.remesa_id"}
        for key, column in mapping.items():
            if filters.get(key) not in (None, ""):
                clauses.append(f"{column}=?"); args.append(filters[key])
        if filters.get("member_id") not in (None, ""):
            if is_excluded_member(filters["member_id"]): clauses.append("0=1")
            else:
                clauses.append(f"EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id={batch_alias}.batch_id AND l.recipient_member_id=? AND l.id_socio<>0)"); args.append(filters["member_id"])
        clauses.append(f"(NOT EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id={batch_alias}.batch_id) OR EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id={batch_alias}.batch_id AND l.recipient_member_id<>0 AND l.id_socio<>0))")
        if filters.get("date_from"): clauses.append(f"substr({batch_alias}.payment_date,1,10)>=?"); args.append(str(filters["date_from"]))
        if filters.get("date_to"): clauses.append(f"substr({batch_alias}.payment_date,1,10)<=?"); args.append(str(filters["date_to"]))
        return clauses, args

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
        """Read cascading options while excluding each option's own selection.

        This keeps the remittance selector populated after selecting a remittance,
        while the same selection still scopes the other dependent selectors.
        """
        base = {
            "campaign": campaign,
            "company": company,
            "crop": crop,
            "remittance_id": remittance_id,
            "member_id": member_id,
            "status": status,
            "date_from": date_from,
            "date_to": date_to,
        }

        def distinct(column, excluded_filter):
            scope = {key: value for key, value in base.items() if key != excluded_filter}
            clauses, args = self._history_clauses(scope)
            clauses.extend((f"b.{column} IS NOT NULL", f"TRIM(b.{column}) <> ''"))
            return (f"SELECT DISTINCT b.{column} FROM liquidation_batches b WHERE "
                    + " AND ".join(clauses), args)

        with self.database.connect() as conn:
            sql, args = distinct("campaign", "campaign")
            campaigns = tuple(row[0] for row in conn.execute(sql + " ORDER BY b.campaign DESC", args))
            sql, args = distinct("company", "company")
            companies = tuple(row[0] for row in conn.execute(sql + " ORDER BY b.company", args))
            sql, args = distinct("crop", "crop")
            crops = tuple(row[0] for row in conn.execute(sql + " ORDER BY b.crop", args))
            clauses, args = self._history_clauses(
                {key: value for key, value in base.items() if key != "remittance_id"}
            )
            sql = "SELECT DISTINCT b.remesa_id, b.remesa_name FROM liquidation_batches b"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY b.remesa_id"
            remittances = tuple({"id": row[0], "name": row[1],
                                 "display": f"{row[0]} — {row[1]}"}
                                for row in conn.execute(sql, args))
        return {"campaigns": campaigns, "companies": companies, "crops": crops,
                "remittances": remittances}

    def history_summary(self, **filters):
        clauses, args = self._history_clauses(filters)
        sql="""SELECT COUNT(*) batch_count,
          COALESCE(SUM((SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id<>0 AND l.id_socio<>0)),0) line_count,
          COALESCE(SUM((SELECT COUNT(DISTINCT recipient_member_id) FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id<>0 AND l.id_socio<>0)),0) recipient_count
          FROM liquidation_batches b""" + (" WHERE " + " AND ".join(clauses) if clauses else "")
        with self.database.connect() as conn: return conn.execute(sql,args).fetchone()

    def search_liquidation_members(
        self,
        text,
        *,
        campaign=None,
        company=None,
        crop=None,
        remittance_id=None,
        status=None,
        date_from=None,
        date_to=None,
        limit=30,
    ):
        """Find recipient members in saved lines, not in batch headers."""
        query = normalize_search_text(text)
        if not query:
            return ()
        scope = {
            "campaign": campaign,
            "company": company,
            "crop": crop,
            "remittance_id": remittance_id,
            "status": status,
            "date_from": date_from,
            "date_to": date_to,
        }
        clauses, args = self._history_clauses(scope)
        where = " AND ".join(clauses) if clauses else "1=1"
        contains = f"%{query}%"
        starts = f"{query}%"
        with self.database.connect() as conn:
            matches = conn.execute(f"""SELECT DISTINCT l.recipient_member_id AS member_id,
                l.socio AS name
                FROM liquidaciones l
                JOIN liquidation_batches b ON b.batch_id = l.batch_id
                WHERE {where}
                  AND l.recipient_member_id IS NOT NULL
                  AND l.recipient_member_id <> 0 AND l.id_socio <> 0
                  AND (CAST(l.recipient_member_id AS TEXT) LIKE ?
                       OR NORMALIZE_SEARCH_TEXT(l.socio) LIKE ?)
                ORDER BY CASE
                    WHEN CAST(l.recipient_member_id AS TEXT) = ? THEN 0
                    WHEN CAST(l.recipient_member_id AS TEXT) LIKE ? THEN 1
                    WHEN NORMALIZE_SEARCH_TEXT(l.socio) LIKE ? THEN 2
                    ELSE 3
                END, l.recipient_member_id, NORMALIZE_SEARCH_TEXT(l.socio)
                LIMIT ?""", (*args, contains, contains, query, starts, starts, int(limit))).fetchall()
        logger.info(
            "[MemberSearch] text=%r normalized=%r campaign=%r company=%r crop=%r "
            "remittance_id=%r status=%r date_from=%r date_to=%r results=%d",
            text, query, campaign, company, crop, remittance_id, status,
            date_from, date_to, len(matches),
        )
        return tuple(matches)

    def list_batch_documents(self, batch_id: str):
        with self.database.connect() as conn:
            return conn.execute("""SELECT d.*,b.status batch_status,b.remesa_name,
              (SELECT socio FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id LIMIT 1) recipient_name,
              (SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) line_count,
              (SELECT group_concat(id_liq,' · ') FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) id_liqs
              FROM generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id
              WHERE d.batch_id=? AND d.recipient_member_id<>0 ORDER BY d.id DESC""",(batch_id,)).fetchall()

    def list_latest_batch_documents(self, batch_id: str):
        """Return the newest attempt for each recipient and document type."""
        with self.database.connect() as conn:
            return conn.execute("""SELECT d.*,b.status batch_status,b.remesa_name,
              (SELECT socio FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id LIMIT 1) recipient_name,
              (SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) line_count,
              (SELECT group_concat(id_liq,' · ') FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) id_liqs
              FROM generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id
              WHERE d.batch_id=? AND d.recipient_member_id<>0 AND d.id=(
                SELECT MAX(d2.id) FROM generated_documents d2
                WHERE d2.batch_id=d.batch_id
                  AND d2.recipient_member_id=d.recipient_member_id
                  AND d2.document_type=d.document_type
              ) ORDER BY d.recipient_member_id,d.document_type""", (batch_id,)).fetchall()

    def list_active_documents(self, batch_id: str):
        return [r for r in self.list_batch_documents(batch_id) if r["status"] == "GENERATED"]

    def mark_batch_voided(self, batch_id: str, *, reason: str, user: str | None, voided_at: str) -> bool:
        with self.database.connect() as conn:
            return conn.execute("UPDATE liquidation_batches SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'",(voided_at,user,reason,batch_id)).rowcount == 1

    def mark_lines_voided(self, batch_id: str, *, reason: str, user: str | None, voided_at: str) -> int:
        with self.database.connect() as conn:
            return conn.execute("UPDATE liquidaciones SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'",(voided_at,user,reason,batch_id)).rowcount

    def record_document(self, **values) -> None:
        if is_excluded_member(values.get("recipient_member_id")):
            logger.warning("[SYSTEM_MEMBER_EXCLUDED] origin=LiquidationRepository.record_document records=1")
            return
        with self.database.connect() as conn:
            previous = conn.execute("SELECT COALESCE(MAX(generation_attempt),0) FROM generated_documents WHERE batch_id=? AND recipient_member_id=? AND document_type=?", (values["batch_id"], values["recipient_member_id"], values["document_type"])).fetchone()[0]
            conn.execute("INSERT INTO generated_documents(batch_id,remittance_id,recipient_member_id,document_type,file_path,status,generated_at,error_message,generation_attempt,file_hash,created_by) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (values["batch_id"],values["remittance_id"],values["recipient_member_id"],values["document_type"],values["file_path"],values["status"],values.get("generated_at"),values.get("error_message"),previous+1,values.get("file_hash"),values.get("created_by")))

    def audit(self, batch_id: str, action: str, details: str, user: str | None = None) -> None:
        from data.persistence.migrations import utcnow
        with self.database.connect() as conn:
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)", (batch_id,action,"DOCUMENT",batch_id,details,utcnow(),user))

    def supersede_batch_documents(self, batch_id: str) -> None:
        with self.database.connect() as conn:
            conn.execute("UPDATE generated_documents SET status='SUPERSEDED' WHERE batch_id=? AND status='GENERATED'", (batch_id,))

    def record_exported_draft(self, **values) -> None:
        if is_excluded_member(values.get("recipient_member_id")):
            logger.warning("[SYSTEM_MEMBER_EXCLUDED] origin=LiquidationRepository.record_exported_draft records=1")
            return
        with self.database.connect() as conn:
            conn.execute("""INSERT INTO exported_draft_documents
              (remittance_id,recipient_member_id,member_name,campaign,company,crop,remittance_name,file_path,status,generated_at,source,file_hash)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", (values.get("remittance_id"), values.get("recipient_member_id"),
              values.get("member_name", ""), values.get("campaign", ""), values.get("company", ""),
              values.get("crop", ""), values.get("remittance_name", ""), values["file_path"], "GENERATED", values["generated_at"],
              values.get("source", "MANUAL_DRAFT_EXPORT"), values.get("file_hash")))

    def list_mergeable_documents(self, *, document_kind: str, include_voided: bool = False, **filters):
        clauses=["recipient_member_id<>0"]; args=[]
        mapping={"campaign":"b.campaign","company":"b.company","crop":"b.crop","remittance_id":"b.remesa_id","member_id":"d.recipient_member_id","status":"d.status"}
        if document_kind == "PDF_DRAFT":
            mapping={"campaign":"campaign","company":"company","crop":"crop","remittance_id":"remittance_id","member_id":"recipient_member_id","status":"status"}
            for key,column in mapping.items():
                if filters.get(key) not in (None, ""): clauses.append(f"{column}=?"); args.append(filters[key])
            if filters.get("date_from"): clauses.append("substr(generated_at,1,10)>=?"); args.append(str(filters["date_from"]))
            if filters.get("date_to"): clauses.append("substr(generated_at,1,10)<=?"); args.append(str(filters["date_to"]))
            sql="SELECT *,NULL batch_id,'ACTIVE' batch_status,'' id_liqs FROM exported_draft_documents WHERE 1=1"
            if clauses: sql += " AND " + " AND ".join(clauses)
            with self.database.connect() as conn: return conn.execute(sql+" ORDER BY campaign,crop,remittance_id,recipient_member_id,generated_at",args).fetchall()
        for key,column in mapping.items():
            if filters.get(key) not in (None, ""): clauses.append(f"{column}=?"); args.append(filters[key])
        clauses += ["d.document_type='PDF_MEMBER'", "b.status IN (" + ("'ACTIVE','VOIDED'" if include_voided else "'ACTIVE'") + ")"]
        if filters.get("date_from"): clauses.append("substr(d.generated_at,1,10)>=?"); args.append(str(filters["date_from"]))
        if filters.get("date_to"): clauses.append("substr(d.generated_at,1,10)<=?"); args.append(str(filters["date_to"]))
        sql="""SELECT d.*,b.status batch_status,b.campaign,b.company,b.crop,b.remesa_name,
          COALESCE((SELECT socio FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id LIMIT 1),'') member_name,
          COALESCE((SELECT group_concat(id_liq,' · ') FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id),'') id_liqs
          FROM generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id WHERE """+" AND ".join(clauses)+"""
          AND d.id=(SELECT MAX(x.id) FROM generated_documents x WHERE x.batch_id=d.batch_id AND x.recipient_member_id=d.recipient_member_id AND x.document_type=d.document_type)
          ORDER BY b.campaign,b.crop,b.remesa_id,d.recipient_member_id,d.generated_at"""
        with self.database.connect() as conn: return conn.execute(sql,args).fetchall()

    def list_document_filter_options(self, *, document_kind: str, campaign=None, company=None, crop=None):
        """Return real, registered document masters; the UI never reads output folders."""
        if document_kind == "PDF_DRAFT":
            table="exported_draft_documents"; active="status='GENERATED'"
            columns=("campaign","company","crop","remittance_id","remittance_name")
        elif document_kind == "PDF_MEMBER":
            table="generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id"
            active="d.document_type='PDF_MEMBER' AND d.status='GENERATED' AND b.status='ACTIVE'"
            columns=("b.campaign","b.company","b.crop","b.remesa_id","b.remesa_name")
        else: raise ValueError("Tipo documental no admitido")
        clauses=[active]; args=[]
        for value,column in ((campaign,columns[0]),(company,columns[1]),(crop,columns[2])):
            if value not in (None, ""): clauses.append(f"{column}=?"); args.append(value)
        where=" AND ".join(clauses)
        with self.database.connect() as conn:
            campaigns=tuple(r[0] for r in conn.execute(f"SELECT DISTINCT {columns[0]} FROM {table} WHERE {active} ORDER BY 1") if r[0])
            companies=tuple(r[0] for r in conn.execute(f"SELECT DISTINCT {columns[1]} FROM {table} WHERE {where} ORDER BY 1",args) if r[0])
            crops=tuple(r[0] for r in conn.execute(f"SELECT DISTINCT {columns[2]} FROM {table} WHERE {where} ORDER BY 1",args) if r[0])
            remittances=tuple((r[0],r[1]) for r in conn.execute(f"SELECT DISTINCT {columns[3]},{columns[4]} FROM {table} WHERE {where} ORDER BY 1",args))
        return {"campaigns":campaigns,"companies":companies,"crops":crops,"remittances":remittances}
