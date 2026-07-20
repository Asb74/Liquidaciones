from pathlib import Path

import pytest

from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
from services.liquidation_history_service import LiquidationHistoryService


class Documents:
    def regenerate_documents(self,batch_id,recipient_member_id=None):
        return (batch_id,recipient_member_id)


@pytest.fixture
def history(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"history.sqlite")); db.initialize()
    with db.connect() as conn:
        conn.execute("INSERT INTO liquidation_batches(batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,calculation_fingerprint,original_line_count,final_line_count,status,created_at) VALUES('b1',7,'R7','2026','1','CITRICOS','2026-02-01','fp',1,1,'ACTIVE','now')")
        conn.execute("INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,variedad,neto,imp_bruto,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,source_member_id,recipient_member_id,source_liquidation_key,batch_id,created_at) VALUES('CI2026010001','2026-02-01','CITRICOS','2026','1',10,'SOCIO','NAVEL','10','20','1','1','0','0','0','18','12','2','19.8',7,'R7','NORMAL',10,10,'key','b1','now')")
    return LiquidationHistoryService(LiquidationRepository(db),Documents()),db


def test_history_survives_new_service_and_filters_persisted_batches(history):
    service,db=history
    restarted=LiquidationHistoryService(LiquidationRepository(PersistenceDatabase(str(db.path))),Documents())
    rows=restarted.list_batches({"campaign":"2026","member_id":10,"status":"ACTIVE"})
    assert len(rows)==1 and rows[0]["line_count"]==1 and rows[0]["recipient_count"]==1
    assert restarted.regenerate_documents("b1",10)==("b1",10)


def test_void_is_logical_keeps_ids_and_supersedes_documents(history,tmp_path):
    service,db=history; pdf=tmp_path/"member.pdf"; pdf.write_bytes(b"pdf")
    service.repository.record_document(batch_id="b1",remittance_id=7,recipient_member_id=10,document_type="PDF_MEMBER",file_path=str(pdf),status="GENERATED")
    service.void_batch("b1","Duplicada","tester")
    with db.connect() as conn:
        assert conn.execute("SELECT status FROM liquidation_batches").fetchone()[0]=="VOIDED"
        row=conn.execute("SELECT id_liq,status,void_reason FROM liquidaciones").fetchone()
        assert tuple(row)==("CI2026010001","VOIDED","Duplicada")
        assert conn.execute("SELECT status FROM generated_documents").fetchone()[0]=="SUPERSEDED"
    assert pdf.exists()
    with pytest.raises(ValueError): service.void_batch("b1","otra vez","tester")


def test_latest_document_query_returns_newest_attempt(history, tmp_path):
    service, db = history
    for version, status in ((1, "GENERATED"), (2, "FAILED")):
        service.repository.record_document(batch_id="b1", remittance_id=7, recipient_member_id=10,
            document_type="PDF_MEMBER", file_path=str(tmp_path / f"member_v{version}.pdf"), status=status,
            error_message="fallo" if status == "FAILED" else None)
    latest = service.list_recipient_documents("b1")
    assert len(latest) == 1
    assert latest[0]["status"] == "FAILED"
    assert latest[0]["file_path"].endswith("member_v2.pdf")
    assert len(service.list_documents("b1")) == 2


def test_history_filter_options_are_dependent_and_members_are_normalized(history):
    service, db = history
    with db.connect() as conn:
        conn.execute("INSERT INTO liquidation_batches(batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,calculation_fingerprint,original_line_count,final_line_count,status,created_at) VALUES('b2',8,'Sem 44','2027','2','KAKIS','2027-02-01','fp2',1,1,'ACTIVE','now')")
        conn.execute("INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,variedad,neto,imp_bruto,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,source_member_id,recipient_member_id,source_liquidation_key,batch_id,created_at) VALUES('KA2027010001','2027-02-01','KAKIS','2027','2',1540,'GARCÍA PÉREZ, ANTONIO','KAKI','10','20','1','1','0','0','0','18','12','2','19.8',8,'R8','NORMAL',99,1540,'key2','b2','now')")
    options=service.list_history_filter_options(campaign='2027')
    assert options['companies']==('2',) and options['crops']==('KAKIS',)
    assert options['remittances']==({'id':8,'name':'Sem 44','display':'8 — Sem 44'},)
    assert [row['member_id'] for row in service.search_liquidation_members('154')] == [1540]
    assert [row['member_id'] for row in service.search_liquidation_members('garcia')] == [1540]
    assert [row['member_id'] for row in service.search_liquidation_members('GARCÍA', campaign='2027')] == [1540]
    assert service.history_summary({'campaign':'2027'})['batch_count']==1
