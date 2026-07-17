from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

PREFIXES = (("CITRICOS","CI"),("MANDARINA","MA"),("DIRECTO","VE"),("DIRECTOCHF","VC"),("INDUSTRIA","IN"),("KAKIS","KA"),("FRUTA","FR"),("CIRUELA","CR"),("SANDIA","SA"))

MIGRATIONS = ((1, "initial_persistence", """
CREATE TABLE liquidation_prefixes(crop TEXT PRIMARY KEY, prefix TEXT NOT NULL UNIQUE, active INTEGER NOT NULL DEFAULT 1 CHECK(active IN(0,1)), description TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE liquidation_sequences(crop TEXT NOT NULL, campaign TEXT NOT NULL, company TEXT NOT NULL, prefix TEXT NOT NULL, last_sequence INTEGER NOT NULL, initialized_from TEXT NOT NULL, legacy_last_idliq TEXT, initialized_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(crop,campaign,company));
CREATE TABLE split_rules(id INTEGER PRIMARY KEY AUTOINCREMENT, source_member_id INTEGER NOT NULL, source_member_name TEXT, split_type TEXT NOT NULL, campaign TEXT, crop TEXT, variety TEXT, remittance_id INTEGER, effective_from TEXT, effective_to TEXT, active INTEGER NOT NULL DEFAULT 1, priority INTEGER NOT NULL DEFAULT 100, notes TEXT, source TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE split_rule_recipients(id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER NOT NULL REFERENCES split_rules(id) ON DELETE CASCADE, recipient_member_id INTEGER NOT NULL, recipient_member_name TEXT, value TEXT NOT NULL, is_residual INTEGER NOT NULL DEFAULT 0, sort_order INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE liquidation_batches(batch_id TEXT PRIMARY KEY, remesa_id INTEGER NOT NULL, remesa_name TEXT NOT NULL, campaign TEXT NOT NULL, company TEXT NOT NULL, crop TEXT NOT NULL, payment_date TEXT, calculation_fingerprint TEXT NOT NULL, original_line_count INTEGER NOT NULL, final_line_count INTEGER NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, created_by TEXT, voided_at TEXT, voided_by TEXT, void_reason TEXT);
CREATE UNIQUE INDEX uq_active_batch_fingerprint ON liquidation_batches(remesa_id,calculation_fingerprint) WHERE status='ACTIVE';
CREATE TABLE liquidaciones(id INTEGER PRIMARY KEY AUTOINCREMENT, id_liq TEXT NOT NULL UNIQUE, fecha TEXT NOT NULL, cultivo TEXT NOT NULL, campana TEXT NOT NULL, empresa TEXT NOT NULL, id_socio INTEGER NOT NULL, socio TEXT NOT NULL, cod_art INTEGER, variedad TEXT NOT NULL, neto TEXT NOT NULL, imp_bruto TEXT NOT NULL, precio_comer TEXT, recoleccion TEXT NOT NULL, cuota_ha TEXT NOT NULL, bp_calidad TEXT NOT NULL, b_transporte TEXT NOT NULL, b_global TEXT NOT NULL, base_i TEXT NOT NULL, precio_medio TEXT, iva TEXT NOT NULL, retencion TEXT NOT NULL, importe_total TEXT NOT NULL, id_concepto_liq INTEGER NOT NULL, concepto_liq TEXT NOT NULL, tipo TEXT NOT NULL, remesa_id INTEGER, source_member_id INTEGER NOT NULL, recipient_member_id INTEGER NOT NULL, source_member_name TEXT, source_variety TEXT, source_liquidation_key TEXT NOT NULL, split_rule_id INTEGER REFERENCES split_rules(id), split_type TEXT, split_factor TEXT NOT NULL DEFAULT '1', is_split INTEGER NOT NULL DEFAULT 0, batch_id TEXT REFERENCES liquidation_batches(batch_id), status TEXT NOT NULL DEFAULT 'ACTIVE', created_at TEXT NOT NULL, created_by TEXT, calculation_fingerprint TEXT, voided_at TEXT, voided_by TEXT, void_reason TEXT);
CREATE INDEX ix_liq_context ON liquidaciones(cultivo,campana,empresa); CREATE INDEX ix_liq_member ON liquidaciones(id_socio); CREATE INDEX ix_liq_remesa ON liquidaciones(remesa_id); CREATE INDEX ix_liq_source ON liquidaciones(source_member_id); CREATE INDEX ix_liq_recipient ON liquidaciones(recipient_member_id); CREATE INDEX ix_liq_status ON liquidaciones(status); CREATE INDEX ix_liq_source_key ON liquidaciones(source_liquidation_key);
CREATE TABLE liquidation_audit(id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT, action TEXT NOT NULL, entity_type TEXT, entity_id TEXT, details_json TEXT, created_at TEXT NOT NULL, created_by TEXT);
CREATE TABLE legacy_imports(name TEXT PRIMARY KEY, imported_at TEXT NOT NULL, details TEXT);
"""),(2, "generated_documents", """
CREATE TABLE generated_documents(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 batch_id TEXT NOT NULL REFERENCES liquidation_batches(batch_id),
 remittance_id INTEGER NOT NULL, recipient_member_id INTEGER NOT NULL,
 document_type TEXT NOT NULL, file_path TEXT NOT NULL, status TEXT NOT NULL,
 generated_at TEXT, error_message TEXT,
 generation_attempt INTEGER NOT NULL DEFAULT 1, file_hash TEXT, created_by TEXT
);
CREATE INDEX ix_generated_documents_batch ON generated_documents(batch_id,status);
"""))

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def migrate(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT NOT NULL)")
    done={r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
    for version,name,sql in MIGRATIONS:
        if version in done: continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executescript(sql)
            now=utcnow()
            conn.executemany("INSERT OR IGNORE INTO liquidation_prefixes(crop,prefix,created_at,updated_at) VALUES(?,?,?,?)", ((c,p,now,now) for c,p in PREFIXES))
            conn.execute("INSERT INTO schema_migrations VALUES(?,?,?)",(version,name,now)); conn.commit()
        except Exception:
            conn.rollback(); raise
