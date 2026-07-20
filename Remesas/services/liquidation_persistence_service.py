from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from data.legacy_persistence_repository import LegacyPersistenceRepository
from data.persistence.database import PersistenceDatabase
from domain.persistence_models import (BatchPersistenceSaveResult, PendingBatchPersistence,
    PendingRemittancePersistence, PersistedLiquidation, PersistenceBatch,
    PersistencePreview, RemittancePersistenceSaveResult)
from services.liquidation_split_service import LiquidationSplitService
from domain.member_rules import (SYSTEM_MEMBER_EXCLUDED_MESSAGE, is_excluded_member,
                                 log_system_member_excluded)
import logging

logger = logging.getLogger(__name__)


def _now(): return datetime.now(timezone.utc).isoformat()
def _d(value): return format(Decimal(value), "f")


class LiquidationPersistenceService:
    def __init__(self, database: PersistenceDatabase, legacy_conn, *, crop_aliases: dict[str,str] | None=None) -> None:
        self.database=database; self.database.initialize(); self.legacy=LegacyPersistenceRepository(legacy_conn); self.legacy_conn=legacy_conn; self.aliases=crop_aliases or {}

    def prepare_preview(self, result) -> PersistencePreview:
        if result is None or not result.member_results: raise ValueError("El resultado de liquidación está vacío")
        h=result.header
        try: remesa_id=int(h.remesa_id)
        except (TypeError,ValueError): raise ValueError("La remesa no tiene IdREMESA válido") from None
        with self.database.connect() as conn:
            prefix=conn.execute("SELECT prefix FROM liquidation_prefixes WHERE crop=? AND active=1",(str(h.cultivo).strip().upper(),)).fetchone()
            if not prefix: raise ValueError(f"No existe prefijo activo para {h.cultivo}")
            splitter=LiquidationSplitService(conn,self.legacy_conn); lines=[]
            excluded = [member for member in result.member_results if is_excluded_member(member.member_id)]
            members = [member for member in result.member_results if not is_excluded_member(member.member_id)]
            if excluded:
                log_system_member_excluded(logger, origin="LiquidationPersistenceService.prepare_preview",
                                           count=len(excluded), net_kg=sum((Decimal(x.net_kg) for x in excluded), Decimal("0")), remesa_id=remesa_id)
            for member in members:
                if Decimal(member.net_kg)<0: raise ValueError(f"Neto negativo para socio {member.member_id}")
                cod=self.legacy.article_code(str(h.cultivo),member.variety,self.aliases)
                if cod is None: raise ValueError(f"No se encontró MVariedad.ARTICULO para {member.variety}")
                lines.extend(splitter.split(member,h,cod_art=cod))
        if not lines:
            raise ValueError("No existen liquidaciones válidas para guardar. El socio 0 es un registro técnico excluido.")
        payload={"header":[remesa_id,h.remesa_name,h.campana,h.empresa,h.cultivo,h.fecha_pago,h.tipo_liquidacion],"lines":[[x.source_member_id,x.recipient_member_id,x.variety,*(_d(getattr(x,n)) for n in ("split_factor","net_kg","gross_amount","taxable_base","total_amount"))] for x in lines]}
        fingerprint=hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        return PersistencePreview(h,tuple(lines),fingerprint,len(members),tuple(w for x in lines for w in x.warnings))

    def prepare_batch_preview(self, result) -> PendingBatchPersistence:
        """Prepara divisiones para resultados ya calculados, sin recalcular ni guardar."""
        pending=[]; warnings=[]
        for item in result.successful_results:
            try:
                calculation=item.calculation_result
                original=calculation.result if hasattr(calculation,"result") else calculation
                preview=self.prepare_preview(original)
                pending.append(PendingRemittancePersistence(item.remittance,original,preview,preview.valid,preview.warnings,item.output_directory))
            except Exception as exc:
                warnings.append(f"Remesa {item.remittance.remittance_id}: {exc}")
        warnings.extend(f"Remesa {x.remittance.remittance_id} excluida: {x.error_message}" for x in result.failed_results)
        first=(result.successful_results[0].remittance if result.successful_results else
               (result.failed_results[0].remittance if result.failed_results else None))
        return PendingBatchPersistence(
            f"{result.started_at:%Y%m%d%H%M%S%f}",
            first.campaign if first else "", first.company if first else "", first.crop if first else "",
            tuple(pending), sum(x.persistence_preview.original_line_count for x in pending),
            sum(len(x.persistence_preview.lines) for x in pending), tuple(warnings),
            any(x.valid for x in pending), tuple(x.remittance for x in result.failed_results))

    def save_batch(self, preview: PendingBatchPersistence) -> BatchPersistenceSaveResult:
        """Guarda cada remesa en su propia transacción y continúa tras un fallo."""
        results=[]; warnings=[]
        for item in preview.remittances:
            if not item.valid:
                continue
            try:
                batch=self.save(item.persistence_preview)
                results.append(RemittancePersistenceSaveResult(item.remittance,True,batch,None,()))
            except Exception as exc:
                warnings.append(f"Remesa {item.remittance.remittance_id}: {exc}")
                results.append(RemittancePersistenceSaveResult(item.remittance,False,None,str(exc)))
        saved=sum(x.saved for x in results)
        return BatchPersistenceSaveResult(len(results),saved,len(results)-saved,tuple(results),tuple(warnings))

    def _next_id(self, conn, crop: str, campaign: str, company: str, user: str | None, batch_id: str) -> str:
        crop=crop.strip().upper(); campaign=str(campaign).strip(); company_num=int(str(company).strip()); company_key=str(company_num); company_fmt=f"{company_num:02d}"
        row=conn.execute("SELECT * FROM liquidation_sequences WHERE crop=? AND campaign=? AND company=?",(crop,campaign,company_key)).fetchone()
        if row is None:
            p=conn.execute("SELECT prefix FROM liquidation_prefixes WHERE crop=? AND active=1",(crop,)).fetchone()
            if not p: raise ValueError(f"No existe prefijo activo para {crop}")
            prefix=str(p[0]); stem=f"{prefix}{campaign}{company_fmt}"
            legacy=self.legacy.max_liquidation_id(stem)
            local=[str(r[0]) for r in conn.execute("SELECT id_liq FROM liquidaciones WHERE id_liq LIKE ?",(stem+"%",)) if re.fullmatch(re.escape(stem)+r"\d{4}",str(r[0]))]
            local_max=max((int(x[-4:]) for x in local),default=0); legacy_max=int(legacy[-4:]) if legacy else 0; last=max(local_max,legacy_max); now=_now()
            conn.execute("INSERT INTO liquidation_sequences VALUES(?,?,?,?,?,?,?,?,?)",(crop,campaign,company_key,prefix,last,"DLIQUIDACIONES",legacy,now,now))
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,details_json,created_at,created_by) VALUES(?,?,?,?,?,?)",(batch_id,"SEQUENCE_INITIALIZED","SEQUENCE",json.dumps({"stem":stem,"legacy":legacy}),now,user)); row={"prefix":prefix,"last_sequence":last}
        sequence=int(row["last_sequence"])+1
        if sequence>9999: raise ValueError("Secuencia IdLiq agotada")
        conn.execute("UPDATE liquidation_sequences SET last_sequence=?,updated_at=? WHERE crop=? AND campaign=? AND company=?",(sequence,_now(),crop,campaign,company_key))
        return f"{row['prefix']}{campaign}{company_fmt}{sequence:04d}"

    def save(self, preview: PersistencePreview, *, user: str | None=None) -> PersistenceBatch:
        if not preview.lines:
            raise ValueError("No existen liquidaciones válidas para guardar. El socio 0 es un registro técnico excluido.")
        invalid = [line for line in preview.lines if is_excluded_member(line.source_member_id) or is_excluded_member(line.recipient_member_id)]
        if invalid:
            log_system_member_excluded(logger, origin="LiquidationPersistenceService.save", count=len(invalid),
                                       net_kg=sum((Decimal(x.net_kg) for x in invalid), Decimal("0")))
            raise ValueError(SYSTEM_MEMBER_EXCLUDED_MESSAGE)
        h=preview.header; batch_id=str(uuid.uuid4()); now=_now(); persisted=[]
        conn=self.database.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            duplicate=conn.execute("SELECT batch_id FROM liquidation_batches WHERE remesa_id=? AND calculation_fingerprint=? AND status='ACTIVE'",(int(h.remesa_id),preview.fingerprint)).fetchone()
            if duplicate: raise ValueError(f"La remesa ya está guardada en el batch {duplicate[0]}")
            conn.execute("""INSERT INTO liquidation_batches
              (batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,calculation_fingerprint,
               original_line_count,final_line_count,status,created_at,created_by,operation_type)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(batch_id,int(h.remesa_id),h.remesa_name,str(h.campana),str(h.empresa),str(h.cultivo),str(h.fecha_pago),preview.fingerprint,preview.original_line_count,len(preview.lines),"ACTIVE",now,user,"ORIGINAL"))
            for line in preview.lines:
                id_liq=self._next_id(conn,str(h.cultivo),str(h.campana),str(h.empresa),user,batch_id)
                key="|".join(map(str,(h.campana,h.empresa,h.cultivo,h.remesa_id,line.source_member_id,line.variety)))
                values=(id_liq,str(h.fecha_pago),str(h.cultivo),str(h.campana),str(h.empresa),line.recipient_member_id,line.recipient_name,line.cod_art,line.variety,_d(line.net_kg),_d(line.gross_amount),_d(line.commercial_price) if line.commercial_price is not None else None,_d(line.collection_amount),_d(line.hectare_fee_amount),_d(line.quality_amount),_d(line.transport_amount),_d(line.globalgap_amount),_d(line.taxable_base),_d(line.final_average_price) if line.final_average_price is not None else None,_d(line.vat_rate),_d(line.withholding_rate),_d(line.total_amount),int(h.remesa_id),h.remesa_name,h.tipo_liquidacion,int(h.remesa_id),line.source_member_id,line.recipient_member_id,line.source_member_name,line.variety,key,line.split_rule_id,line.split_type,_d(line.split_factor),int(line.split_factor!=1),batch_id,"ACTIVE",now,user,preview.fingerprint,"ORIGINAL")
                conn.execute("INSERT INTO liquidaciones(id_liq,fecha,cultivo,campana,empresa,id_socio,socio,cod_art,variedad,neto,imp_bruto,precio_comer,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,precio_medio,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,remesa_id,source_member_id,recipient_member_id,source_member_name,source_variety,source_liquidation_key,split_rule_id,split_type,split_factor,is_split,batch_id,status,created_at,created_by,calculation_fingerprint,operation_type) VALUES("+",".join("?" for _ in values)+")",values)
                persisted.append(PersistedLiquidation(id_liq,line.recipient_member_id,line.total_amount))
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",(batch_id,"SAVE","BATCH",batch_id,json.dumps({"lines":len(persisted)}),_now(),user)); conn.commit()
        except Exception: conn.rollback(); raise
        finally: conn.close()
        return PersistenceBatch(batch_id,"ACTIVE",tuple(persisted))

    def void_batch(self,batch_id: str,reason: str,user: str | None=None) -> None:
        # Kept as the compatibility entry point used by older UI flows.  The
        # accounting cancellation itself is now an immutable reversal.
        from services.liquidation_modification_service import LiquidationModificationService
        return LiquidationModificationService(self).void(batch_id, reason, user=user)

    def _void_batch_legacy(self,batch_id: str,reason: str,user: str | None=None) -> None:
        if not reason.strip(): raise ValueError("El motivo de anulación es obligatorio")
        conn=self.database.connect(); now=_now()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("UPDATE liquidation_batches SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'",(now,user,reason.strip(),batch_id)).rowcount!=1: raise ValueError("El batch no existe o ya está anulado")
            conn.execute("UPDATE liquidaciones SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'",(now,user,reason.strip(),batch_id))
            conn.execute("UPDATE generated_documents SET status='SUPERSEDED' WHERE batch_id=? AND status='GENERATED'",(batch_id,))
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",(batch_id,"VOID","BATCH",batch_id,json.dumps({"reason":reason.strip()}),now,user)); conn.commit()
        except Exception: conn.rollback(); raise
        finally: conn.close()

    def record_pdf_generated(self,batch_id: str,paths, user: str | None=None) -> None:
        with self.database.connect() as conn:
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",(batch_id,"PDF_GENERATED","BATCH",batch_id,json.dumps({"paths":[str(p) for p in paths]},ensure_ascii=False),_now(),user))

    def import_legacy_split_rules(self) -> bool:
        """Semilla idempotente confirmada; no vuelve a consultar DDividirLiq."""
        grouped={5970:[(5893,50)],496:[(495,50)],5993:[(7157,50),(7159,50)],7157:[(7159,50)]}; conn=self.database.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("SELECT 1 FROM legacy_imports WHERE name='LEGACY_DDIVIDIRLIQ'").fetchone(): conn.rollback(); return False
            now=_now()
            for source,recipients in grouped.items():
                cur=conn.execute("INSERT INTO split_rules(source_member_id,source_member_name,split_type,priority,notes,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",(source,self.legacy.member_name(source),"PERCENTAGE_WITH_RESIDUAL",100,"Importación histórica confirmada","LEGACY_DDIVIDIRLIQ",now,now))
                for order,(recipient,value) in enumerate(recipients): conn.execute("INSERT INTO split_rule_recipients(rule_id,recipient_member_id,recipient_member_name,value,sort_order) VALUES(?,?,?,?,?)",(cur.lastrowid,recipient,self.legacy.member_name(recipient),str(value),order))
            conn.execute("INSERT INTO legacy_imports VALUES(?,?,?)",("LEGACY_DDIVIDIRLIQ",now,json.dumps({"rules":len(grouped)}))); conn.commit(); return True
        except Exception: conn.rollback(); raise
        finally: conn.close()
