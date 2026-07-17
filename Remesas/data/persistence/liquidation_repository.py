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

    def list_batches(self, *, status: str | None = None):
        sql = "SELECT * FROM liquidation_batches"; args = ()
        if status: sql += " WHERE status=?"; args = (status,)
        with self.database.connect() as conn:
            return conn.execute(sql + " ORDER BY created_at DESC", args).fetchall()

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
