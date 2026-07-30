from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from data.legacy_persistence_repository import LegacyPersistenceRepository
from data.variety_repository import VarietyRepository
from data.persistence.database import PersistenceDatabase
from domain.persistence_models import (BatchPersistenceSaveResult, PendingBatchPersistence,
    PendingRemittancePersistence, PersistedLiquidation, PersistenceBatch,
    PersistencePreview, RemittancePersistenceSaveResult, RemittanceReplacementState,
    RemittanceSaveStatus, ReplacementRequest)
from services.liquidation_split_service import LiquidationSplitService
from services.variety_selection_resolver import VarietySelectionKind, VarietySelectionResolver
from services.variety_group_service import VarietyGroupService
from services.persisted_variety_benchmark_service import variety_group_code
from presentation.liquidation_document_snapshot import dump as dump_snapshot, load as load_snapshot, SCHEMA_VERSION
from services.variety_group_migration_service import migrate_persisted_variety_groups
from domain.member_rules import (SYSTEM_MEMBER_EXCLUDED_MESSAGE, is_excluded_member,
                                 log_system_member_excluded, configure_excluded_members,
                                 excluded_member_service)
import logging
from collections.abc import Mapping
from domain.liquidation_conflicts import LiquidationConflictType, LiquidationScope
from services.liquidation_conflict_service import LiquidationConflictService
from services.document_snapshot_repair_service import repair_invalid_v4_snapshots
from services.document_snapshot_diagnostic import diagnostic_logger
from services.split_document_audit import split_document_logger

logger = logging.getLogger(__name__)


def _now(): return datetime.now(timezone.utc).isoformat()
def _d(value): return format(Decimal(value), "f")


class LiquidationPersistenceService:
    def __init__(self, database: PersistenceDatabase, legacy_conn, *, crop_aliases: dict[str,str] | None=None) -> None:
        self.database=database; self.database.initialize(); self.legacy=LegacyPersistenceRepository(legacy_conn); self.legacy_conn=legacy_conn; self.aliases=crop_aliases or {}
        self.variety_resolver = VarietySelectionResolver(VarietyRepository(legacy_conn))
        self.variety_groups = VarietyGroupService(VarietyRepository(legacy_conn))
        self.conflicts = LiquidationConflictService(self.legacy_repository_adapter())
        self.snapshot_repair_report = repair_invalid_v4_snapshots(self.database, legacy_conn)
        self.variety_group_migration_report = migrate_persisted_variety_groups(self.legacy_repository_adapter(), self.variety_groups)
        configure_excluded_members(connection=legacy_conn)

    def legacy_repository_adapter(self):
        """Expose the local repository used by the idempotent startup backfill."""
        from data.persistence.liquidation_repository import LiquidationRepository
        return LiquidationRepository(self.database)

    def _article_code(self, source_crop: str, variety: str) -> str | None:
        """Resolve ARTICULO against the master crop selected for this variety.

        Output crops such as DIRECTO can contain varieties from more than one
        master crop.  A crop-wide alias is therefore insufficient: the exact
        variety resolution determines which MVariedad.CULTIVO must be queried.
        """
        resolution = self.variety_resolver.resolve(source_crop, variety)
        if resolution.kind in {VarietySelectionKind.VARIETY, VarietySelectionKind.GROUP} and resolution.resolved_master_crop:
            return self.legacy.article_code(resolution.resolved_master_crop, variety)
        if resolution.kind == VarietySelectionKind.AMBIGUOUS:
            logger.warning(
                "[MVariedadArticulo]\ncrop=%s\nvariety=%s\narticle=\nstatus=ambiguous",
                source_crop,
                variety,
            )
            return None
        # Preserve compatibility for installations with additional aliases not
        # yet represented in the shared crop-resolution configuration.
        return self.legacy.article_code(source_crop, variety, self.aliases)

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
                cod=self._article_code(str(h.cultivo),member.variety)
                if cod is None: raise ValueError(f"No se encontró MVariedad.ARTICULO para {member.variety}")
                lines.extend(splitter.split(member,h,cod_art=cod))
        if not lines:
            raise ValueError("No existen agricultores liquidables. Los registros encontrados corresponden a socios excluidos.")
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

    @staticmethod
    def _active_batch(conn, scope):
        rows=conn.execute("""SELECT batch_id,status,operation_type FROM liquidation_batches
            WHERE campaign=? AND company=? AND crop=? AND remesa_id=?
            AND status IN ('ACTIVE','PARTIAL') AND operation_type IN ('ORIGINAL','REPLACEMENT')
            ORDER BY created_at,batch_id""",scope).fetchall()
        if len(rows)>1:
            raise ValueError("Se han encontrado varias liquidaciones vigentes para esta remesa. Debe corregirse la duplicidad antes de continuar.")
        return rows[0] if rows else None

    @staticmethod
    def _is_accounting_exported(conn, batch_id):
        return bool(conn.execute("""SELECT 1 FROM accounting_exports e
            LEFT JOIN accounting_export_items i ON i.export_id=e.id
            WHERE e.status='GENERATED' AND (i.batch_id=? OR (i.id IS NULL AND e.batch_id=?)) LIMIT 1""",
            (batch_id,batch_id)).fetchone())

    def get_replacement_state(self, *, campaign, company, crop, remittance_id):
        scope=(str(campaign).strip(),str(company).strip(),str(crop).strip().upper(),int(remittance_id))
        with self.database.connect() as conn:
            active=self._active_batch(conn,scope)
            batch_id=active["batch_id"] if active else None
            exported=self._is_accounting_exported(conn,batch_id) if batch_id else False
        reason=("ACCOUNTING_EXPORTED" if exported else ("REQUIRES_CONFIRMATION" if batch_id else None))
        state=RemittanceReplacementState(int(remittance_id),batch_id,bool(batch_id),exported,bool(batch_id and not exported),reason)
        logger.info("[LiquidationReplacementCheck]\nremittance_id=%s\nactive_batch_id=%s\nis_exported=%s\ncan_replace=%s\ndecision=%s",state.remittance_id,state.active_batch_id,state.is_accounting_exported,state.can_replace,state.reason or "CREATE")
        return state

    def save_batch(self, preview: PendingBatchPersistence, *, snapshots_by_remittance: Mapping[int, Mapping[int, str]] | None = None,
                   replacements_by_remittance: Mapping[int, ReplacementRequest] | None = None, user: str | None = None) -> BatchPersistenceSaveResult:
        """Guarda cada remesa en su propia transacción y continúa tras un fallo."""
        results=[]; warnings=[]
        for item in preview.remittances:
            if not item.valid:
                results.append(RemittancePersistenceSaveResult(item.remittance,False,error="La remesa contiene errores de validación",status=RemittanceSaveStatus.VALIDATION_ERROR,error_type="ValidationError")); continue
            remittance_id=int(item.persistence_preview.header.remesa_id)
            try:
                state=self.get_replacement_state(campaign=preview.campaign,company=preview.company,crop=preview.crop,remittance_id=remittance_id)
                if state.is_accounting_exported:
                    message="La remesa ya fue exportada a contabilidad. Debe generarse una rectificación contable antes de volver a liquidarla."
                    logger.warning("[LiquidationReplacementBlocked]\nremittance_id=%s\nbatch_id=%s\nreason=ACCOUNTING_EXPORTED",remittance_id,state.active_batch_id)
                    results.append(RemittancePersistenceSaveResult(item.remittance,False,error=message,status=RemittanceSaveStatus.BLOCKED_EXPORTED,previous_batch_id=state.active_batch_id,message=message)); continue
                request=(replacements_by_remittance or {}).get(remittance_id)
                if state.has_active_liquidation and (not request or request.replace_batch_id != state.active_batch_id):
                    message="La remesa tiene una liquidación no exportada y requiere confirmación para sustituirla."
                    results.append(RemittancePersistenceSaveResult(item.remittance,False,error=message,status=RemittanceSaveStatus.REQUIRES_CONFIRMATION,previous_batch_id=state.active_batch_id,message=message)); continue
                snapshots = (snapshots_by_remittance or {}).get(remittance_id, {})
                batch=self.save(item.persistence_preview,document_snapshots=snapshots,replace_batch_id=request.replace_batch_id if request else None,reason=request.reason if request else None,user=user)
                status=RemittanceSaveStatus.REPLACED if request else RemittanceSaveStatus.CREATED
                results.append(RemittancePersistenceSaveResult(item.remittance,True,batch,None,(),status,state.active_batch_id,status.value))
            except Exception as exc:
                warnings.append(f"Remesa {item.remittance.remittance_id}: {exc}")
                results.append(RemittancePersistenceSaveResult(item.remittance,False,None,str(exc),(),RemittanceSaveStatus.SAVE_ERROR,None,str(exc),type(exc).__name__))
        saved=sum(x.saved for x in results)
        logger.info("[MassivePersistenceSummary]\ntotal=%s\ncreated=%s\nreplaced=%s\nblocked_exported=%s\nrequires_confirmation=%s\nvalidation_errors=%s\nsave_errors=%s",len(results),sum(x.status is RemittanceSaveStatus.CREATED for x in results),sum(x.status is RemittanceSaveStatus.REPLACED for x in results),sum(x.status is RemittanceSaveStatus.BLOCKED_EXPORTED for x in results),sum(x.status is RemittanceSaveStatus.REQUIRES_CONFIRMATION for x in results),sum(x.status is RemittanceSaveStatus.VALIDATION_ERROR for x in results),sum(x.status is RemittanceSaveStatus.SAVE_ERROR for x in results))
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

    def save(self, preview: PersistencePreview, *, user: str | None=None,
             document_snapshots: Mapping[int, str] | None = None,
             replace_batch_id: str | None = None, reason: str | None = None) -> PersistenceBatch:
        if not preview.lines:
            raise ValueError("No existen liquidaciones válidas para guardar. El socio 0 es un registro técnico excluido.")
        invalid = [line for line in preview.lines if is_excluded_member(line.source_member_id) or is_excluded_member(line.recipient_member_id)]
        if invalid:
            log_system_member_excluded(logger, origin="LiquidationPersistenceService.save", count=len(invalid),
                                       net_kg=sum((Decimal(x.net_kg) for x in invalid), Decimal("0")))
            member_id = invalid[0].source_member_id if is_excluded_member(invalid[0].source_member_id) else invalid[0].recipient_member_id
            reason = excluded_member_service.reason_for_exclusion(member_id)
            raise ValueError(f"El socio {member_id} está excluido porque {reason}.")
        if replace_batch_id and not str(reason or "").strip():
            raise ValueError("El motivo de sustitución es obligatorio")
        h=preview.header; batch_id=str(uuid.uuid4()); now=_now(); persisted=[]
        snapshot_diagnostic = diagnostic_logger()
        split_document_audit = split_document_logger()
        conn=self.database.open_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            scope=(str(h.campana).strip(),str(h.empresa).strip(),str(h.cultivo).strip().upper(),int(h.remesa_id))
            active=self._active_batch(conn,scope)
            operation_type="ORIGINAL"; modification_group_id=None
            if active:
                existing=active["batch_id"]
                if replace_batch_id != existing:
                    raise ValueError(f"La remesa ya tiene una liquidación activa ({existing}). No puede guardarse como una nueva liquidación independiente.")
                exported=self._is_accounting_exported(conn,existing)
                if exported:
                    raise ValueError("La remesa ya fue exportada a contabilidad. Debe generarse una rectificación contable.")
                modification_group_id=str(uuid.uuid4()); operation_type="REPLACEMENT"
                logger.info("[LiquidationReplacementConfirmed]\nremittance_id=%s\nprevious_batch_id=%s\nreason=%s\nuser=%s",h.remesa_id,existing,str(reason).strip(),user)
                conn.execute("UPDATE liquidation_batches SET status='SUPERSEDED',replacement_batch_id=?,modification_reason=? WHERE batch_id=? AND status IN ('ACTIVE','PARTIAL')",(batch_id,str(reason).strip(),existing))
                conn.execute("UPDATE liquidaciones SET status='SUPERSEDED',replacement_batch_id=? WHERE batch_id=? AND status IN ('ACTIVE','PARTIAL')",(batch_id,existing))
                conn.execute("UPDATE generated_documents SET status='SUPERSEDED' WHERE batch_id=? AND status='GENERATED'",(existing,))
            conn.execute("""INSERT INTO liquidation_batches
              (batch_id,remesa_id,remesa_name,campaign,company,crop,payment_date,calculation_fingerprint,
               original_line_count,final_line_count,status,created_at,created_by,operation_type,
               supersedes_batch_id,original_batch_id,modification_group_id,modification_reason)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(batch_id,int(h.remesa_id),h.remesa_name,str(h.campana),str(h.empresa),str(h.cultivo).strip().upper(),str(h.fecha_pago),preview.fingerprint,preview.original_line_count,len(preview.lines),"ACTIVE",now,user,operation_type,replace_batch_id,replace_batch_id,modification_group_id,str(reason).strip() if reason else None))
            member_groups = {}
            for line in preview.lines:
                id_liq=self._next_id(conn,str(h.cultivo),str(h.campana),str(h.empresa),user,batch_id)
                group, resolution = self.variety_groups.resolve_variety_group(str(h.cultivo), line.variety)
                group_code = variety_group_code(group.group, group.subgroup)
                group_name = group.label
                variety_name = resolution.selected_varieties[0]
                variety_code = resolution.normalized_value
                member_groups.setdefault(int(line.recipient_member_id), set()).add((variety_code,variety_name,group_code,group_name))
                logger.info("[VarietyGroupResolution]\nbatch_id=%s\nrecipient_member_id=%s\nid_liq=%s\nvariety=%s\nnormalized_variety=%s\ngroup_code=%s\ngroup_name=%s\nsource=liquidation\nstatus=RESOLVED",
                    batch_id,line.recipient_member_id,id_liq,line.variety,variety_code,group_code,group_name)
                key="|".join(map(str,(h.campana,h.empresa,h.cultivo,h.remesa_id,line.source_member_id,line.variety)))
                values=(id_liq,str(h.fecha_pago),str(h.cultivo),str(h.campana),str(h.empresa),line.recipient_member_id,line.recipient_name,line.cod_art,line.variety,_d(line.net_kg),_d(line.gross_amount),_d(line.commercial_price) if line.commercial_price is not None else None,_d(line.collection_amount),_d(line.hectare_fee_amount),_d(line.quality_amount),_d(line.transport_amount),_d(line.globalgap_amount),_d(line.taxable_base),_d(line.final_average_price) if line.final_average_price is not None else None,_d(line.vat_rate),_d(line.withholding_rate),_d(line.total_amount),int(h.remesa_id),h.remesa_name,h.tipo_liquidacion,int(h.remesa_id),line.source_member_id,line.recipient_member_id,line.source_member_name,line.variety,key,line.split_rule_id,line.split_type,_d(line.split_factor),int(line.split_factor!=1),batch_id,"ACTIVE",now,user,preview.fingerprint,operation_type)
                columns="id_liq,fecha,cultivo,campana,empresa,id_socio,socio,cod_art,variedad,neto,imp_bruto,precio_comer,recoleccion,cuota_ha,bp_calidad,b_transporte,b_global,base_i,precio_medio,iva,retencion,importe_total,id_concepto_liq,concepto_liq,tipo,remesa_id,source_member_id,recipient_member_id,source_member_name,source_variety,source_liquidation_key,split_rule_id,split_type,split_factor,is_split,batch_id,status,created_at,created_by,calculation_fingerprint,operation_type,original_batch_id,modification_group_id,variety_code,variety_name,variety_group_code,variety_group_name"
                values=values+(replace_batch_id,modification_group_id,variety_code,variety_name,group_code,group_name)
                conn.execute("INSERT INTO liquidaciones("+columns+") VALUES("+",".join("?" for _ in values)+")",values)
                persisted.append(PersistedLiquidation(id_liq,line.recipient_member_id,line.total_amount))
            snapshot_diagnostic.info(
                "[DocumentSnapshotPersistenceInput]\nbatch_id=%s\n"
                "document_snapshots parameter is None?=%s\ndocument_snapshots count=%s\n"
                "recipient ids=%s\nmember_groups count=%s",
                batch_id, "yes" if document_snapshots is None else "no",
                len(document_snapshots or {}),
                ",".join(str(value) for value in sorted((document_snapshots or {}).keys())),
                len(member_groups),
            )
            for recipient_member_id, payload_json in (document_snapshots or {}).items():
                identities = member_groups.get(int(recipient_member_id), set())
                if len({item[2] for item in identities}) > 1:
                    raise ValueError("El documento contiene liquidaciones de más de un grupo varietal y no puede generar una comparativa única.")
                # Validate and canonicalize every payload at the final write
                # boundary, including callers that did not need enrichment.
                vm=load_snapshot(payload_json)
                if identities:
                    variety_code_value,variety_name_value,group_code_value,group_name_value = next(iter(identities))
                    vm=replace(vm,variety_code=variety_code_value,variety_name=variety_name_value,
                        variety_group_code=group_code_value,variety_group_name=group_name_value,
                        surface_group_code=group_code_value,surface_group_name=group_name_value)
                payload_json=dump_snapshot(vm)
                logger.info("[DocumentSnapshotFixedPrices] member_id=%s remesa=%s schema_version=%s national_market_price=%s rotten_leaves_price=%s", recipient_member_id, h.remesa_id, SCHEMA_VERSION, vm.national_market_price, vm.rotten_leaves_price)
                logger.info("[DocumentSnapshot]\nbatch_id=%s\nrecipient_member_id=%s\nschema_version=%s\nstatus=STARTED", batch_id, recipient_member_id, SCHEMA_VERSION)
                conn.execute("INSERT OR REPLACE INTO liquidation_document_snapshots(batch_id,recipient_member_id,payload_json,schema_version,calculation_fingerprint,created_at,created_by) VALUES(?,?,?,?,?,?,?)", (batch_id, int(recipient_member_id), payload_json, SCHEMA_VERSION, preview.fingerprint, now, user))
                split_document_audit.info(
                    "[SplitSnapshotCreated]\nbatch_id=%s\nrecipient_member_id=%s\n"
                    "snapshot_net_kg=%s\nsnapshot_total_amount=%s\nsource=POST_SPLIT_PREVIEW",
                    batch_id, recipient_member_id, vm.effective_net_kg, vm.total_amount,
                )
                conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)", (batch_id, "DOCUMENT_SNAPSHOT_SAVED", "DOCUMENT", str(recipient_member_id), json.dumps({"recipient_member_id": recipient_member_id, "schema_version": SCHEMA_VERSION, "calculation_fingerprint": preview.fingerprint}), now, user))
                logger.info("[DocumentSnapshot]\nbatch_id=%s\nrecipient_member_id=%s\nschema_version=%s\nstatus=SAVED", batch_id, recipient_member_id, SCHEMA_VERSION)
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",(batch_id,"SAVE","BATCH",batch_id,json.dumps({"lines":len(persisted)}),_now(),user)); conn.commit()
            if replace_batch_id:
                logger.info("[LiquidationReplacementCompleted]\nremittance_id=%s\nprevious_batch_id=%s\nnew_batch_id=%s\nold_status=SUPERSEDED\nnew_status=ACTIVE\nsnapshot_count=%s\ndocument_count=0",h.remesa_id,replace_batch_id,batch_id,len(document_snapshots or {}))
            logger.info("[PersistenceTransaction]\nbatch_id=%s\nstatus=COMMITTED", batch_id)
        except Exception as exc:
            conn.rollback()
            logger.exception("[PersistenceTransaction]\nbatch_id=%s\nstatus=ROLLED_BACK\nerror_type=%s\nerror_message=%s", batch_id, type(exc).__name__, exc)
            raise
        finally: self.database.close_connection(conn)
        return PersistenceBatch(batch_id,"ACTIVE",tuple(persisted))

    def void_batch(self,batch_id: str,reason: str,user: str | None=None) -> None:
        # Kept as the compatibility entry point used by older UI flows.  The
        # accounting cancellation itself is now an immutable reversal.
        from services.liquidation_modification_service import LiquidationModificationService
        return LiquidationModificationService(self).void(batch_id, reason, user=user)

    def _void_batch_legacy(self,batch_id: str,reason: str,user: str | None=None) -> None:
        if not reason.strip(): raise ValueError("El motivo de anulación es obligatorio")
        conn=self.database.open_connection(); now=_now()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("UPDATE liquidation_batches SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'",(now,user,reason.strip(),batch_id)).rowcount!=1: raise ValueError("El batch no existe o ya está anulado")
            conn.execute("UPDATE liquidaciones SET status='VOIDED',voided_at=?,voided_by=?,void_reason=? WHERE batch_id=? AND status='ACTIVE'",(now,user,reason.strip(),batch_id))
            conn.execute("UPDATE generated_documents SET status='SUPERSEDED' WHERE batch_id=? AND status='GENERATED'",(batch_id,))
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",(batch_id,"VOID","BATCH",batch_id,json.dumps({"reason":reason.strip()}),now,user)); conn.commit()
        except Exception: conn.rollback(); raise
        finally: self.database.close_connection(conn)

    def record_pdf_generated(self,batch_id: str,paths, user: str | None=None) -> None:
        with self.database.connect() as conn:
            conn.execute("INSERT INTO liquidation_audit(batch_id,action,entity_type,entity_id,details_json,created_at,created_by) VALUES(?,?,?,?,?,?,?)",(batch_id,"PDF_GENERATED","BATCH",batch_id,json.dumps({"paths":[str(p) for p in paths]},ensure_ascii=False),_now(),user))

    def import_legacy_split_rules(self) -> bool:
        """Semilla idempotente confirmada; no vuelve a consultar DDividirLiq."""
        grouped={5970:[(5893,50)],496:[(495,50)],5993:[(7157,50),(7159,50)],7157:[(7159,50)]}; conn=self.database.open_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("SELECT 1 FROM legacy_imports WHERE name='LEGACY_DDIVIDIRLIQ'").fetchone(): conn.rollback(); return False
            now=_now()
            for source,recipients in grouped.items():
                cur=conn.execute("INSERT INTO split_rules(source_member_id,source_member_name,split_type,priority,notes,source,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",(source,self.legacy.member_name(source),"PERCENTAGE_WITH_RESIDUAL",100,"Importación histórica confirmada","LEGACY_DDIVIDIRLIQ",now,now))
                for order,(recipient,value) in enumerate(recipients): conn.execute("INSERT INTO split_rule_recipients(rule_id,recipient_member_id,recipient_member_name,value,sort_order) VALUES(?,?,?,?,?)",(cur.lastrowid,recipient,self.legacy.member_name(recipient),str(value),order))
            conn.execute("INSERT INTO legacy_imports VALUES(?,?,?)",("LEGACY_DDIVIDIRLIQ",now,json.dumps({"rules":len(grouped)}))); conn.commit(); return True
        except Exception: conn.rollback(); raise
        finally: self.database.close_connection(conn)
