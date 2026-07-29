import sqlite3

import pytest

from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
from domain.liquidation_conflicts import LiquidationConflictType, LiquidationScope
from services.liquidation_conflict_service import LiquidationConflictService


def _batch(conn, batch_id, *, status="ACTIVE", operation="ORIGINAL"):
    conn.execute("""INSERT INTO liquidation_batches
        (batch_id,remesa_id,remesa_name,campaign,company,crop,calculation_fingerprint,
         original_line_count,final_line_count,status,created_at,operation_type)
        VALUES(?,2271,'R2271','2026','1','DIRECTO',?,1,1,?,'now',?)""",
        (batch_id,batch_id,status,operation))


def test_scope_conflict_and_partial_unique_index(tmp_path):
    db=PersistenceDatabase(str(tmp_path/'db.sqlite')); db.initialize()
    repo=LiquidationRepository(db); service=LiquidationConflictService(repo)
    scope=LiquidationScope('2026','1','directo',2271)
    assert service.inspect(scope).conflict_type is LiquidationConflictType.NONE
    with db.connect() as conn: _batch(conn,'first')
    assert service.inspect(scope).conflict_type is LiquidationConflictType.ACTIVE_NOT_EXPORTED
    with pytest.raises(sqlite3.IntegrityError):
        with db.connect() as conn: _batch(conn,'second',operation='REPLACEMENT')
    with db.connect() as conn: _batch(conn,'reversal',operation='REVERSAL')


def test_generated_export_items_are_source_of_truth(tmp_path):
    db=PersistenceDatabase(str(tmp_path/'db.sqlite')); db.initialize(); repo=LiquidationRepository(db)
    with db.connect() as conn:
        _batch(conn,'first')
        conn.execute("""INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,
          variedad,neto,imp_bruto,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,iva,
          retencion,importe_total,id_concepto_liq,concepto_liq,tipo,source_member_id,recipient_member_id,
          source_liquidation_key,batch_id,created_at) VALUES('x','now','DIRECTO','2026','1',1,'S','V',
          '1','1','0','0','0','0','0','1','0','0','1',2271,'R','T',1,1,'k','first','now')""")
    export_id=repo.record_csv_export(batch_id='first',export_type='FULL_BATCH',file_path='x.csv',
                                     status='GENERATED')
    assert repo.has_generated_accounting_export('first')
    with db.connect() as conn:
        item=conn.execute("SELECT batch_id,liquidation_id FROM accounting_export_items WHERE export_id=?",(export_id,)).fetchone()
        assert tuple(item)==('first',1)


def test_duplicate_diagnostic_does_not_modify_data(tmp_path):
    db=PersistenceDatabase(str(tmp_path/'db.sqlite')); db.initialize(); repo=LiquidationRepository(db)
    with db.connect() as conn:
        conn.execute('DROP INDEX ux_liquidation_active_scope'); _batch(conn,'first'); _batch(conn,'second')
    rows=repo.list_duplicate_active_scopes()
    assert rows[0]['remesa_id']==2271 and rows[0]['active_count']==2
    with db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM liquidation_batches WHERE status='ACTIVE'").fetchone()[0]==2
