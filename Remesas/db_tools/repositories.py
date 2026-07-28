"""Repositorios PostgreSQL iniciales para pruebas controladas; la UI aún usa SQLite."""
from __future__ import annotations
from contextlib import contextmanager

class PostgresRepository:
    def __init__(self,factory): self.factory=factory
    @contextmanager
    def transaction(self):
        with self.factory.transaction() as connection: yield connection

class PostgresLiquidationRepository(PostgresRepository):
    def get_batch(self,batch_id):
        with self.factory.connect() as c:return c.execute("SELECT * FROM liquidaciones.liquidation_batches WHERE batch_id=%s",(batch_id,)).fetchone()
    def get_lines(self,batch_id):
        with self.factory.connect() as c:return c.execute("SELECT * FROM liquidaciones.liquidaciones WHERE batch_id=%s ORDER BY id",(batch_id,)).fetchall()

class PostgresDocumentRepository(PostgresRepository):
    def get_snapshots(self,batch_id):
        with self.factory.connect() as c:return c.execute("SELECT * FROM liquidaciones.liquidation_document_snapshots WHERE batch_id=%s ORDER BY recipient_member_id",(batch_id,)).fetchall()

class PostgresAccountingExportRepository(PostgresRepository):
    def get(self,export_id):
        with self.factory.connect() as c:return c.execute("SELECT * FROM liquidaciones.accounting_exports WHERE id=%s",(export_id,)).fetchone()

class PostgresAuditRepository(PostgresRepository):
    def record(self,batch_id,action,details,created_by=None):
        with self.factory.transaction() as c:c.execute("INSERT INTO liquidaciones.liquidation_audit(batch_id,action,details_json,created_at,created_by) VALUES(%s,%s,%s,CURRENT_TIMESTAMP,%s)",(batch_id,action,details,created_by))

class PostgresSequenceRepository(PostgresRepository):
    def reserve(self,crop,campaign,company):
        with self.factory.transaction() as c:
            row=c.execute("SELECT last_sequence FROM liquidaciones.liquidation_sequences WHERE crop=%s AND campaign=%s AND company=%s FOR UPDATE",(crop,campaign,company)).fetchone()
            if not row: raise KeyError("La secuencia IdLiq no está inicializada")
            value=row[0]+1;c.execute("UPDATE liquidaciones.liquidation_sequences SET last_sequence=%s,updated_at=CURRENT_TIMESTAMP WHERE crop=%s AND campaign=%s AND company=%s",(value,crop,campaign,company));return value
