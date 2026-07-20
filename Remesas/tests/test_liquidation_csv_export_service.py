from decimal import Decimal
from pathlib import Path

from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
from services.liquidation_csv_export_service import CSV_HEADERS, LiquidationCsvExportService


class Legacy:
    def __init__(self, excluded=()): self.excluded=set(excluded)
    def member_is_self_billed(self, member_id): return member_id in self.excluded


def _save(db, *, batch_id="batch-1", member=42, operation="ORIGINAL", group=None, net="31577.1200", total="406.4344"):
    db.initialize()
    with db.connect() as c:
        c.execute("INSERT OR IGNORE INTO liquidation_batches(batch_id,remesa_id,remesa_name,campaign,company,crop,calculation_fingerprint,original_line_count,final_line_count,status,created_at,operation_type,modification_group_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (batch_id,7,"BLANCA TEMPRANA", "2025", "ACME", "CITRICOS",batch_id,1,1,"ACTIVE","2025-01-01T00:00:00",operation,group))
        c.execute("INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,cod_art,variedad,neto,imp_bruto,precio_comer,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,precio_medio,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,source_member_id,recipient_member_id,source_liquidation_key,batch_id,status,created_at,operation_type,modification_group_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (f"CI{batch_id}{member}","2025-09-11","CITRICOS","2025","ACME",member,"CRUZ RUIZ,JESÚS",3983,"BLANCA",net,"12.0000","0.2679115","-2500.00","0","0","0","0","12","0.1","2","-1",total,8,"SEMANA PENULTIMA","TIPO",member,member,"key"+batch_id,batch_id,"ACTIVE","2025-01-01",operation,group))


def test_full_export_historical_format_and_duplicate(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"db.sqlite")); _save(db)
    service=LiquidationCsvExportService(LiquidationRepository(db), Legacy(), tmp_path)
    result=service.export_batch("batch-1", user="tester")
    assert result.success
    raw=result.csv_path.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    text=raw.decode("cp1252")
    lines=text.splitlines()
    assert lines[0] == ";".join(CSV_HEADERS)
    assert len(lines[0].split(";")) == 26
    assert len(lines[1].split(";")) == 26
    assert ";11/09/2025;" in lines[1]
    assert "31577,12" in lines[1] and "0,2679115" in lines[1] and "-2500" in lines[1]
    assert "CRUZ RUIZ,JESÚS" in lines[1]
    assert "\r\n" in raw.decode("cp1252")
    assert result.info_path.exists() and result.file_hash
    duplicate=service.export_batch("batch-1")
    assert duplicate.already_existed


def test_member_exclusion_and_semicolon_validation(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"db.sqlite")); _save(db)
    repo=LiquidationRepository(db)
    excluded=LiquidationCsvExportService(repo, Legacy((42,)), tmp_path).export_batch("batch-1")
    assert not excluded.success and excluded.excluded_line_count == 1
    with db.connect() as c: c.execute("UPDATE liquidaciones SET socio='INCORRECTO; SOCIO'")
    result=LiquidationCsvExportService(repo, Legacy(), tmp_path).export_batch("batch-1")
    assert not result.success and "punto y coma" in result.error_message


def test_modification_exports_reversal_before_replacement_and_regenerates(tmp_path):
    db=PersistenceDatabase(str(tmp_path/"db.sqlite")); _save(db, batch_id="negative", operation="REVERSAL", group="mod", net="-1", total="-2"); _save(db, batch_id="positive", operation="REPLACEMENT", group="mod", net="1", total="2")
    service=LiquidationCsvExportService(LiquidationRepository(db), Legacy(), tmp_path)
    result=service.export_modification("mod")
    assert result.success and result.line_count == 2
    lines=result.csv_path.read_text(encoding="cp1252").splitlines()
    assert ";-1;" in lines[1] and ";1;" in lines[2]
    regenerated=service.regenerate_export(result.export_id)
    assert regenerated.success and regenerated.export_id != result.export_id
    assert service.repository.get_csv_export(result.export_id)["status"] == "SUPERSEDED"
