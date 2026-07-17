from __future__ import annotations

from data.persistence.database import PersistenceDatabase


class LiquidationRepository:
    """Único punto de lectura/escritura documental de la SQLite local."""

    def __init__(self, database: PersistenceDatabase) -> None:
        self.database = database

    def get_batch(self, batch_id: str):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidation_batches WHERE batch_id=?", (batch_id,)).fetchone()

    def list_batch_liquidations(self, batch_id: str):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidaciones WHERE batch_id=? ORDER BY recipient_member_id,id", (batch_id,)).fetchall()

    def list_active_batches_for_remittance(self, remittance_id: int):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidation_batches WHERE remesa_id=? AND status='ACTIVE' ORDER BY created_at DESC", (remittance_id,)).fetchall()

    def list_recipient_lines(self, batch_id: str, recipient_member_id: int):
        with self.database.connect() as conn:
            return conn.execute("SELECT * FROM liquidaciones WHERE batch_id=? AND recipient_member_id=? ORDER BY id", (batch_id, recipient_member_id)).fetchall()

    def list_batches(self, **filters):
        clauses=[]; args=[]
        mapping={"status":"b.status","campaign":"b.campaign","company":"b.company","crop":"b.crop","remittance_id":"b.remesa_id"}
        for key,column in mapping.items():
            if filters.get(key) not in (None, ""):
                clauses.append(f"{column}=?"); args.append(filters[key])
        if filters.get("member_id") not in (None, ""):
            clauses.append("EXISTS(SELECT 1 FROM liquidaciones l WHERE l.batch_id=b.batch_id AND l.recipient_member_id=?)"); args.append(filters["member_id"])
        if filters.get("date_from"): clauses.append("substr(b.payment_date,1,10)>=?"); args.append(filters["date_from"])
        if filters.get("date_to"): clauses.append("substr(b.payment_date,1,10)<=?"); args.append(filters["date_to"])
        sql = """SELECT b.*,
          (SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=b.batch_id) line_count,
          (SELECT COUNT(DISTINCT recipient_member_id) FROM liquidaciones l WHERE l.batch_id=b.batch_id) recipient_count,
          (SELECT COUNT(*) FROM generated_documents d WHERE d.batch_id=b.batch_id AND d.status='GENERATED') document_count
          FROM liquidation_batches b"""
        if clauses: sql += " WHERE " + " AND ".join(clauses)
        with self.database.connect() as conn:
            return conn.execute(sql + " ORDER BY b.created_at DESC", tuple(args)).fetchall()

    def list_batch_documents(self, batch_id: str):
        with self.database.connect() as conn:
            return conn.execute("""SELECT d.*,b.status batch_status,b.remesa_name,
              (SELECT socio FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id LIMIT 1) recipient_name,
              (SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) line_count,
              (SELECT group_concat(id_liq,' · ') FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) id_liqs
              FROM generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id
              WHERE d.batch_id=? ORDER BY d.id DESC""",(batch_id,)).fetchall()

    def list_latest_batch_documents(self, batch_id: str):
        """Return the newest attempt for each recipient and document type."""
        with self.database.connect() as conn:
            return conn.execute("""SELECT d.*,b.status batch_status,b.remesa_name,
              (SELECT socio FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id LIMIT 1) recipient_name,
              (SELECT COUNT(*) FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) line_count,
              (SELECT group_concat(id_liq,' · ') FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id) id_liqs
              FROM generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id
              WHERE d.batch_id=? AND d.id=(
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
        with self.database.connect() as conn:
            conn.execute("""INSERT INTO exported_draft_documents
              (remittance_id,recipient_member_id,member_name,campaign,crop,remittance_name,file_path,status,generated_at,source)
              VALUES(?,?,?,?,?,?,?,?,?,?)""", (values.get("remittance_id"), values.get("recipient_member_id"),
              values.get("member_name", ""), values.get("campaign", ""), values.get("crop", ""),
              values.get("remittance_name", ""), values["file_path"], "GENERATED", values["generated_at"],
              "MANUAL_DRAFT_EXPORT"))

    def list_mergeable_documents(self, *, document_kind: str, include_voided: bool = False, **filters):
        clauses=[]; args=[]
        mapping={"campaign":"b.campaign","crop":"b.crop","remittance_id":"b.remesa_id","member_id":"d.recipient_member_id"}
        if document_kind == "PDF_DRAFT":
            mapping={"campaign":"campaign","crop":"crop","remittance_id":"remittance_id","member_id":"recipient_member_id"}
            for key,column in mapping.items():
                if filters.get(key) not in (None, ""): clauses.append(f"{column}=?"); args.append(filters[key])
            if filters.get("date_from"): clauses.append("substr(generated_at,1,10)>=?"); args.append(str(filters["date_from"]))
            if filters.get("date_to"): clauses.append("substr(generated_at,1,10)<=?"); args.append(str(filters["date_to"]))
            sql="SELECT *,NULL batch_id,'ACTIVE' batch_status,'' id_liqs FROM exported_draft_documents WHERE status='GENERATED'"
            if clauses: sql += " AND " + " AND ".join(clauses)
            with self.database.connect() as conn: return conn.execute(sql+" ORDER BY campaign,crop,remittance_id,recipient_member_id,generated_at",args).fetchall()
        for key,column in mapping.items():
            if filters.get(key) not in (None, ""): clauses.append(f"{column}=?"); args.append(filters[key])
        clauses += ["d.document_type='PDF_MEMBER'", "d.status='GENERATED'", "b.status IN (" + ("'ACTIVE','VOIDED'" if include_voided else "'ACTIVE'") + ")"]
        if filters.get("date_from"): clauses.append("substr(d.generated_at,1,10)>=?"); args.append(str(filters["date_from"]))
        if filters.get("date_to"): clauses.append("substr(d.generated_at,1,10)<=?"); args.append(str(filters["date_to"]))
        sql="""SELECT d.*,b.status batch_status,b.campaign,b.crop,b.remesa_name,
          COALESCE((SELECT socio FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id LIMIT 1),'') member_name,
          COALESCE((SELECT group_concat(id_liq,' · ') FROM liquidaciones l WHERE l.batch_id=d.batch_id AND l.recipient_member_id=d.recipient_member_id),'') id_liqs
          FROM generated_documents d JOIN liquidation_batches b ON b.batch_id=d.batch_id WHERE """+" AND ".join(clauses)+"""
          AND d.id=(SELECT MAX(x.id) FROM generated_documents x WHERE x.batch_id=d.batch_id AND x.recipient_member_id=d.recipient_member_id AND x.document_type=d.document_type)
          ORDER BY b.campaign,b.crop,b.remesa_id,d.recipient_member_id,d.generated_at"""
        with self.database.connect() as conn: return conn.execute(sql,args).fetchall()
