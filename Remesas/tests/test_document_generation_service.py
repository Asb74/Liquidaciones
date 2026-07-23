from pathlib import Path
import pytest

from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
from services.document_generation_service import DocumentGenerationOptions, DocumentGenerationService


def _database(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"liquidaciones.sqlite")); db.initialize()
    with db.connect() as conn:
        conn.execute("INSERT INTO liquidation_batches(batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,calculation_fingerprint,original_line_count,final_line_count,status,created_at) VALUES('batch',2207,'REMESA UNO','2026','1','CITRICOS','2026-01-31','fp',2,3,'ACTIVE','now')")
        values=[]
        for id_liq,recipient,variety,total in (("CI2026010001",5893,"BARBERINA","50"),("CI2026010002",5893,"LANE LATE","60"),("CI2026010003",5970,"BARBERINA","110")):
            values.append((id_liq,recipient,str(recipient),variety,total,recipient,id_liq,"batch"))
        conn.executemany("INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,variedad,neto,imp_bruto,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,source_member_id,recipient_member_id,source_liquidation_key,batch_id,created_at) VALUES(?,'2026-01-31','CITRICOS','2026','1',?,?,?,'10','100','1','2','3','4','5','85','10','2',?,2207,'REMESA UNO','NORMAL',5970,?,?,?,'now')", values)
    return db


def test_groups_varieties_per_recipient_and_reads_persisted_ids(tmp_path):
    db=_database(tmp_path); captured=[]
    def exporter(vm,path): captured.append(vm); path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes("|".join(vm.id_liqs).encode()); return path
    result=DocumentGenerationService(LiquidationRepository(db),tmp_path/"out",exporter=exporter).generate_for_batch("batch",options=DocumentGenerationOptions())
    assert result.requested_documents==2
    recipient=next(vm for vm in captured if vm.recipient_member_id==5893)
    assert len(recipient.lines)==2 and recipient.id_liqs==("CI2026010001","CI2026010002")
    assert str(recipient.totals.importe_total)=="110"
    with db.connect() as conn: assert conn.execute("SELECT COUNT(*) FROM generated_documents WHERE status='GENERATED'").fetchone()[0]==2


def test_failure_is_registered_without_changing_persisted_batch(tmp_path):
    db=_database(tmp_path)
    def broken(vm,path): raise RuntimeError("printer unavailable")
    result=DocumentGenerationService(LiquidationRepository(db),tmp_path/"out",exporter=broken).generate_for_batch("batch",options=DocumentGenerationOptions())
    assert len(result.failed_documents)==2
    with db.connect() as conn:
        assert conn.execute("SELECT status FROM liquidation_batches WHERE batch_id='batch'").fetchone()[0]=="PARTIAL"
        assert conn.execute("SELECT COUNT(*) FROM generated_documents WHERE status='FAILED'").fetchone()[0]==2


def test_regeneration_without_snapshot_explains_the_missing_document_data(tmp_path):
    db=_database(tmp_path); seen=[]
    def exporter(vm,path): seen.append(vm.id_liqs); path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(b"pdf"); return path
    service=DocumentGenerationService(LiquidationRepository(db),tmp_path/"out",exporter=exporter)
    service.generate_for_batch("batch",options=DocumentGenerationOptions())
    with pytest.raises(ValueError, match="falta el snapshot documental"):
        service.regenerate_documents("batch")
    with db.connect() as conn: assert conn.execute("SELECT last_sequence FROM liquidation_sequences").fetchall()==[]


def test_void_supersedes_documents_without_deleting_files(tmp_path):
    db=_database(tmp_path); repo=LiquidationRepository(db)
    path=tmp_path/"document.pdf"; path.write_bytes(b"pdf")
    repo.record_document(batch_id="batch",remittance_id=2207,recipient_member_id=5893,document_type="PDF",file_path=str(path),status="GENERATED")
    repo.supersede_batch_documents("batch")
    assert path.exists()
    with db.connect() as conn: assert conn.execute("SELECT status FROM generated_documents").fetchone()[0]=="SUPERSEDED"
