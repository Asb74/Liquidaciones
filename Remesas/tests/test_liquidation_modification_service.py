from types import SimpleNamespace

from data.persistence.database import PersistenceDatabase
from services.liquidation_modification_service import LiquidationModificationService


class Persistence:
    def __init__(self, db): self.database = db
    def _next_id(self, conn, crop, campaign, company, user, batch_id):
        row = conn.execute("SELECT last_sequence,prefix FROM liquidation_sequences WHERE crop=? AND campaign=? AND company=?", (crop, campaign, str(int(company)))).fetchone()
        seq = row["last_sequence"] + 1
        conn.execute("UPDATE liquidation_sequences SET last_sequence=? WHERE crop=? AND campaign=? AND company=?", (seq,crop,campaign,str(int(company))))
        return f"{row['prefix']}{campaign}{int(company):02d}{seq:04d}"


def test_reversal_copies_persisted_amounts_but_not_prices(tmp_path):
    db=PersistenceDatabase(str(tmp_path/'db.sqlite')); db.initialize(); service=LiquidationModificationService(Persistence(db))
    with db.connect() as c:
        c.execute("INSERT INTO liquidation_sequences VALUES('CITRICOS','2026','1','CI',1,'test',NULL,'now','now')")
        c.execute("INSERT INTO liquidation_batches(batch_id,remesa_id,remesa_name,campaign,company,crop,calculation_fingerprint,original_line_count,final_line_count,status,created_at) VALUES('o',1,'R','2026','1','CITRICOS','x',1,1,'ACTIVE','now')")
        c.execute("""INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,variedad,neto,imp_bruto,precio_comer,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,precio_medio,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,source_member_id,recipient_member_id,source_liquidation_key,batch_id,status,created_at)
          VALUES('CI2026010001','2026-01-01','CITRICOS','2026','1',1,'S','N','10','20','3','1','2','3','4','5','6','7','21','2','8',1,'R','T',1,1,'k','o','ACTIVE','now')""")
    reversal=service._create_reversal('o','g','u')
    with db.connect() as c:
        row=c.execute("SELECT * FROM liquidaciones WHERE batch_id=?",(reversal,)).fetchone()
        assert row['operation_type']=='REVERSAL' and row['original_id_liq']=='CI2026010001'
        assert (row['neto'],row['imp_bruto'],row['importe_total'],row['precio_comer'],row['precio_medio'],row['iva'],row['retencion']) == ('-10','-20','-8','3','7','21','2')
