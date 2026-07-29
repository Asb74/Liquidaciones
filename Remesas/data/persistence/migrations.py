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
CREATE TABLE liquidaciones(id INTEGER PRIMARY KEY AUTOINCREMENT, id_liq TEXT NOT NULL UNIQUE, fecha TEXT NOT NULL, cultivo TEXT NOT NULL, campana TEXT NOT NULL, empresa TEXT NOT NULL, id_socio INTEGER NOT NULL, socio TEXT NOT NULL, cod_art TEXT, variedad TEXT NOT NULL, neto TEXT NOT NULL, imp_bruto TEXT NOT NULL, precio_comer TEXT, recoleccion TEXT NOT NULL, cuota_ha TEXT NOT NULL, bp_calidad TEXT NOT NULL, b_transporte TEXT NOT NULL, b_global TEXT NOT NULL, base_i TEXT NOT NULL, precio_medio TEXT, iva TEXT NOT NULL, retencion TEXT NOT NULL, importe_total TEXT NOT NULL, id_concepto_liq INTEGER NOT NULL, concepto_liq TEXT NOT NULL, tipo TEXT NOT NULL, remesa_id INTEGER, source_member_id INTEGER NOT NULL, recipient_member_id INTEGER NOT NULL, source_member_name TEXT, source_variety TEXT, source_liquidation_key TEXT NOT NULL, split_rule_id INTEGER REFERENCES split_rules(id), split_type TEXT, split_factor TEXT NOT NULL DEFAULT '1', is_split INTEGER NOT NULL DEFAULT 0, batch_id TEXT REFERENCES liquidation_batches(batch_id), status TEXT NOT NULL DEFAULT 'ACTIVE', created_at TEXT NOT NULL, created_by TEXT, calculation_fingerprint TEXT, voided_at TEXT, voided_by TEXT, void_reason TEXT);
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
"""),(3, "exported_draft_documents", """
CREATE TABLE exported_draft_documents(
 id INTEGER PRIMARY KEY AUTOINCREMENT, remittance_id INTEGER,
 recipient_member_id INTEGER, member_name TEXT NOT NULL DEFAULT '',
 campaign TEXT NOT NULL, crop TEXT NOT NULL, remittance_name TEXT NOT NULL DEFAULT '',
 file_path TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'GENERATED',
 generated_at TEXT NOT NULL, source TEXT NOT NULL DEFAULT 'MANUAL_DRAFT_EXPORT'
);
CREATE INDEX ix_exported_drafts_context ON exported_draft_documents(campaign,crop,remittance_id,recipient_member_id);
"""),(4, "exported_draft_metadata", """
ALTER TABLE exported_draft_documents ADD COLUMN company TEXT NOT NULL DEFAULT '';
ALTER TABLE exported_draft_documents ADD COLUMN file_hash TEXT;
"""),(5, "liquidation_rectification", """
ALTER TABLE liquidation_batches ADD COLUMN operation_type TEXT NOT NULL DEFAULT 'ORIGINAL';
ALTER TABLE liquidation_batches ADD COLUMN original_batch_id TEXT REFERENCES liquidation_batches(batch_id);
ALTER TABLE liquidation_batches ADD COLUMN replacement_batch_id TEXT REFERENCES liquidation_batches(batch_id);
ALTER TABLE liquidation_batches ADD COLUMN modification_group_id TEXT;
ALTER TABLE liquidaciones ADD COLUMN operation_type TEXT NOT NULL DEFAULT 'ORIGINAL';
ALTER TABLE liquidaciones ADD COLUMN original_batch_id TEXT;
ALTER TABLE liquidaciones ADD COLUMN original_id_liq TEXT;
ALTER TABLE liquidaciones ADD COLUMN replacement_batch_id TEXT;
ALTER TABLE liquidaciones ADD COLUMN replacement_id_liq TEXT;
ALTER TABLE liquidaciones ADD COLUMN modification_group_id TEXT;
CREATE INDEX ix_batch_modification_group ON liquidation_batches(modification_group_id);
CREATE INDEX ix_liq_modification_group ON liquidaciones(modification_group_id);
CREATE INDEX ix_liq_original_id ON liquidaciones(original_id_liq);
UPDATE liquidation_batches SET operation_type='ORIGINAL' WHERE operation_type IS NULL OR operation_type='';
UPDATE liquidaciones SET operation_type='ORIGINAL' WHERE operation_type IS NULL OR operation_type='';
"""),(6, "accounting_exports", """
CREATE TABLE IF NOT EXISTS accounting_exports(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 batch_id TEXT NULL, modification_group_id TEXT NULL, remittance_id INTEGER NULL,
 member_id INTEGER NULL, export_type TEXT NOT NULL, file_path TEXT NOT NULL,
 info_file_path TEXT NULL, status TEXT NOT NULL, line_count INTEGER NOT NULL DEFAULT 0,
 excluded_line_count INTEGER NOT NULL DEFAULT 0, net_total TEXT NULL, amount_total TEXT NULL,
 file_hash TEXT NULL, source_fingerprint TEXT, generated_at TEXT NULL,
 created_at TEXT NOT NULL, created_by TEXT NULL, error_message TEXT NULL,
 generation_attempt INTEGER NOT NULL DEFAULT 1, supersedes_export_id INTEGER NULL
);
CREATE INDEX IF NOT EXISTS ix_accounting_exports_scope ON accounting_exports(batch_id, modification_group_id, member_id, export_type, status);
CREATE INDEX IF NOT EXISTS ix_accounting_exports_fingerprint ON accounting_exports(source_fingerprint, status);
"""),(7, "accounting_mass_export", """
ALTER TABLE accounting_exports ADD COLUMN batch_ids_json TEXT;
CREATE INDEX IF NOT EXISTS ix_accounting_exports_batch_ids ON accounting_exports(export_type,status);
"""),(8, "liquidation_document_snapshots", """
CREATE TABLE IF NOT EXISTS liquidation_document_snapshots(
 batch_id TEXT NOT NULL REFERENCES liquidation_batches(batch_id), recipient_member_id INTEGER NOT NULL,
 payload_json TEXT NOT NULL, schema_version INTEGER NOT NULL, calculation_fingerprint TEXT NOT NULL,
 created_at TEXT NOT NULL, created_by TEXT, PRIMARY KEY(batch_id,recipient_member_id)
);
"""),(9, "article_code_as_text", ""),(10, "persisted_benchmark_metadata", """
ALTER TABLE generated_documents ADD COLUMN benchmark_source_fingerprint TEXT;
CREATE INDEX IF NOT EXISTS ix_documents_benchmark_fingerprint ON generated_documents(benchmark_source_fingerprint);
"""),(11, "persisted_variety_groups", """
ALTER TABLE liquidaciones ADD COLUMN variety_code TEXT;
ALTER TABLE liquidaciones ADD COLUMN variety_name TEXT;
ALTER TABLE liquidaciones ADD COLUMN variety_group_code TEXT;
ALTER TABLE liquidaciones ADD COLUMN variety_group_name TEXT;
CREATE INDEX IF NOT EXISTS ix_liq_benchmark_scope ON liquidaciones(campana,empresa,variety_group_code,status);
"""),(12, "duplicate_liquidation_protection", """
ALTER TABLE liquidation_batches ADD COLUMN supersedes_batch_id TEXT REFERENCES liquidation_batches(batch_id);
ALTER TABLE liquidation_batches ADD COLUMN modification_reason TEXT;
CREATE INDEX IF NOT EXISTS ix_batch_supersedes ON liquidation_batches(supersedes_batch_id);
CREATE TABLE IF NOT EXISTS accounting_export_items(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 export_id INTEGER NOT NULL REFERENCES accounting_exports(id),
 batch_id TEXT NOT NULL REFERENCES liquidation_batches(batch_id),
 liquidation_id INTEGER NULL REFERENCES liquidaciones(id),
 recipient_member_id INTEGER NULL,
 operation_type TEXT NULL,
 created_at TEXT NOT NULL,
 UNIQUE(export_id, liquidation_id)
);
CREATE INDEX IF NOT EXISTS ix_accounting_export_items_export ON accounting_export_items(export_id);
CREATE INDEX IF NOT EXISTS ix_accounting_export_items_batch ON accounting_export_items(batch_id);
CREATE INDEX IF NOT EXISTS ix_accounting_export_items_liquidation ON accounting_export_items(liquidation_id);
CREATE INDEX IF NOT EXISTS ix_accounting_export_items_recipient ON accounting_export_items(recipient_member_id);
"""),(13, "active_scope_unique_index", ""))


def _migrate_article_code_as_text(conn: sqlite3.Connection) -> None:
    """Rebuild the SQLite table so numeric affinity cannot strip leading zeroes."""
    column = next(row for row in conn.execute("PRAGMA table_info(liquidaciones)") if row[1] == "cod_art")
    if str(column[2]).upper() == "TEXT":
        return
    table_sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='liquidaciones'").fetchone()[0]
    indexes = [row[0] for row in conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='liquidaciones' AND sql IS NOT NULL")]
    columns = [row[1] for row in conn.execute("PRAGMA table_info(liquidaciones)")]
    replacement = table_sql.replace("CREATE TABLE liquidaciones", "CREATE TABLE liquidaciones_article_text", 1)
    replacement = replacement.replace("cod_art INTEGER", "cod_art TEXT", 1)
    conn.execute(replacement)
    names = ",".join(f'"{name}"' for name in columns)
    select_names = ",".join("CAST(cod_art AS TEXT)" if name == "cod_art" else f'"{name}"' for name in columns)
    conn.execute(f"INSERT INTO liquidaciones_article_text({names}) SELECT {select_names} FROM liquidaciones")
    conn.execute("DROP TABLE liquidaciones")
    conn.execute("ALTER TABLE liquidaciones_article_text RENAME TO liquidaciones")
    for index_sql in indexes:
        conn.execute(index_sql)

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def migrate(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT NOT NULL)")
    done={r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
    for version,name,sql in MIGRATIONS:
        if version in done: continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            if version == 9:
                _migrate_article_code_as_text(conn)
            elif version == 13:
                # Existing conflicts are evidence, never data to silently repair.
                # Leave the migration pending so startup can report and an
                # administrator can resolve them before the safety index lands.
                duplicate = conn.execute("""SELECT campaign,company,crop,remesa_id,COUNT(*)
                    FROM liquidation_batches
                    WHERE status IN ('ACTIVE','PARTIAL')
                      AND operation_type IN ('ORIGINAL','REPLACEMENT')
                    GROUP BY campaign,company,crop,remesa_id HAVING COUNT(*)>1 LIMIT 1""").fetchone()
                if duplicate:
                    raise RuntimeError("ACTIVE_SCOPE_CONFLICT_DETECTED: existen liquidaciones vigentes duplicadas; ejecute el diagnóstico antes de migrar")
                conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_liquidation_active_scope
                    ON liquidation_batches(campaign,company,crop,remesa_id)
                    WHERE status IN ('ACTIVE','PARTIAL')
                      AND operation_type IN ('ORIGINAL','REPLACEMENT')""")
            else:
                conn.executescript(sql)
            now=utcnow()
            conn.executemany("INSERT OR IGNORE INTO liquidation_prefixes(crop,prefix,created_at,updated_at) VALUES(?,?,?,?)", ((c,p,now,now) for c,p in PREFIXES))
            conn.execute("INSERT INTO schema_migrations VALUES(?,?,?)",(version,name,now)); conn.commit()
        except Exception:
            conn.rollback(); raise
