from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
from typing import Callable, Sequence
import logging

from data.persistence.liquidation_repository import LiquidationRepository
from domain.document_models import DocumentType
from presentation.liquidation_document_snapshot import load as load_document_snapshot
from domain.member_rules import is_excluded_member, log_system_member_excluded
from domain.utils import safe_path_part
from exporters.persisted_liquidation_pdf_exporter import export_persisted_liquidation_pdf
from presentation.persisted_liquidation_pdf_view_model import PersistedLiquidationPdfLine, PersistedLiquidationPdfTotals, PersistedLiquidationPdfViewModel

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class DocumentGenerationOptions:
    generate_pdfs: bool=True; generate_csv: bool=False; overwrite_existing: bool=False; open_output_folder: bool=False
@dataclass(frozen=True)
class GeneratedDocument:
    batch_id: str; remittance_id: int; recipient_member_id: int; document_type: str; path: Path; success: bool; error_message: str|None=None
@dataclass(frozen=True)
class DocumentGenerationResult:
    batch_id: str; requested_documents: int; generated_documents: tuple[GeneratedDocument,...]; failed_documents: tuple[GeneratedDocument,...]; output_directory: Path
@dataclass(frozen=True)
class BatchDocumentGenerationResult:
    requested_batches: int; completed_batches: int; failed_batches: int; results: tuple[DocumentGenerationResult,...]; cancelled: bool=False

class DocumentGenerationService:
    PHASES=("LOADING_PERSISTED_LINES","GROUPING_RECIPIENTS","BUILDING_VIEWMODEL","GENERATING_PDF","REGISTERING_DOCUMENT","FINISHED","ERROR")
    def __init__(self, repository: LiquidationRepository, output_root: Path, *, exporter=export_persisted_liquidation_pdf, user: str|None=None):
        self.repository=repository; self.output_root=Path(output_root); self.exporter=exporter; self.user=user
    def _emit(self, callback, phase, **data):
        if callback: callback({"phase":phase,**data})
    def _vm(self,batch,rows):
        d=lambda v: Decimal(str(v or 0)); optional=lambda v: None if v in (None,"") else d(v)
        lines=tuple(PersistedLiquidationPdfLine(str(r["id_liq"]),str(r["variedad"]),r["cod_art"],d(r["neto"]),d(r["imp_bruto"]),optional(r["precio_comer"]),d(r["recoleccion"]),d(r["cuota_ha"]),d(r["bp_calidad"]),d(r["b_transporte"]),d(r["b_global"]),d(r["base_i"]),d(r["iva"]),d(r["retencion"]),d(r["importe_total"]),optional(r["precio_medio"])) for r in rows)
        total=lambda field: sum((getattr(x,field) for x in lines),Decimal(0))
        raw=str(batch["payment_date"] or "")[:10]
        try: payment=date.fromisoformat(raw)
        except ValueError: payment=None
        first=rows[0]
        return PersistedLiquidationPdfViewModel(str(batch["batch_id"]),int(batch["remesa_id"]),str(batch["remesa_name"]),str(batch["campaign"]),str(batch["company"]),str(batch["crop"]),payment,int(first["recipient_member_id"]),str(first["socio"]),tuple(x.id_liq for x in lines),lines,PersistedLiquidationPdfTotals(total("neto"),total("imp_bruto"),total("base_i"),total("importe_total")),str(first["concepto_liq"]),str(first["tipo"]))
    def _available_path(self,path,overwrite):
        if overwrite or not path.exists(): return path
        i=2
        while path.with_name(f"{path.stem}_v{i}{path.suffix}").exists(): i+=1
        return path.with_name(f"{path.stem}_v{i}{path.suffix}")
    def generate_for_batch(self,batch_id:str,*,options:DocumentGenerationOptions=DocumentGenerationOptions(),progress_callback=None,recipient_member_id:int|None=None):
        self._emit(progress_callback,"LOADING_PERSISTED_LINES",batch_id=batch_id); batch=self.repository.get_batch(batch_id)
        if batch is None: raise ValueError(f"Batch inexistente: {batch_id}")
        rows=self.repository.list_batch_liquidations(batch_id)
        groups=defaultdict(list)
        for row in rows:
            recipient = int(row["recipient_member_id"])
            if is_excluded_member(recipient):
                continue
            if recipient_member_id is None or recipient == recipient_member_id:
                groups[recipient].append(row)
        if is_excluded_member(recipient_member_id):
            log_system_member_excluded(logger, origin="DocumentGenerationService.generate_for_batch", count=1, batch_id=batch_id)
        self._emit(progress_callback,"GROUPING_RECIPIENTS",batch_id=batch_id,recipients=len(groups))
        out=self.output_root/safe_path_part(batch["campaign"])/safe_path_part(batch["crop"])/safe_path_part(batch["remesa_name"])/"definitivos"
        good=[]; bad=[]; self.repository.audit(batch_id,"DOCUMENT_GENERATION_STARTED",json.dumps({"recipients":len(groups)}))
        for index,(recipient,member_rows) in enumerate(groups.items(),1):
            snapshot=self.repository.get_document_snapshot(batch_id, recipient)
            vm=load_document_snapshot(snapshot["payload_json"]) if snapshot else self._vm(batch,member_rows); self._emit(progress_callback,"BUILDING_VIEWMODEL",batch_id=batch_id,recipient_index=index,recipient_count=len(groups),recipient_member_id=recipient)
            suffix=vm.id_liqs[0] if len(vm.id_liqs)==1 else str(batch["remesa_id"])
            recipient_name=getattr(vm, "recipient_name", getattr(vm, "member_name", ""))
            path=self._available_path(out/f"Liquidacion_{recipient}_{safe_path_part(recipient_name)}_{safe_path_part(suffix)}.pdf",options.overwrite_existing)
            try:
                if options.generate_pdfs: self._emit(progress_callback,"GENERATING_PDF",path=str(path)); self.exporter(vm,path)
                digest=sha256(path.read_bytes()).hexdigest() if path.exists() else None; now=datetime.now(timezone.utc).isoformat()
                doc=GeneratedDocument(batch_id,int(batch["remesa_id"]),recipient,DocumentType.PDF_MEMBER.value,path,True); good.append(doc)
                self.repository.record_document(batch_id=batch_id,remittance_id=int(batch["remesa_id"]),recipient_member_id=recipient,document_type=DocumentType.PDF_MEMBER.value,file_path=str(path),status="GENERATED",generated_at=now,file_hash=digest,created_by=self.user)
                self.repository.audit(batch_id,"DOCUMENT_GENERATED",json.dumps({"recipient_member_id":recipient,"path":str(path)}))
            except Exception as exc:
                doc=GeneratedDocument(batch_id,int(batch["remesa_id"]),recipient,DocumentType.PDF_MEMBER.value,path,False,str(exc)); bad.append(doc)
                self.repository.record_document(batch_id=batch_id,remittance_id=int(batch["remesa_id"]),recipient_member_id=recipient,document_type=DocumentType.PDF_MEMBER.value,file_path=str(path),status="FAILED",generated_at=None,error_message=str(exc),created_by=self.user)
                self.repository.audit(batch_id,"DOCUMENT_GENERATION_FAILED",json.dumps({"recipient_member_id":recipient,"error":str(exc)})); self._emit(progress_callback,"ERROR",error=str(exc))
        self._emit(progress_callback,"FINISHED",batch_id=batch_id)
        logger.info("[DocumentGenerationFinished]\nbatch_id=%s\nrequested=%s\ngenerated=%s\nfailed=%s", batch_id, len(groups), len(good), len(bad))
        return DocumentGenerationResult(batch_id,len(groups),tuple(good),tuple(bad),out)
    def generate_for_batches(self,batch_ids:Sequence[str],*,options:DocumentGenerationOptions=DocumentGenerationOptions(),progress_callback=None,cancel_requested:Callable[[],bool]|None=None):
        results=[]; cancelled=False
        for index,batch_id in enumerate(batch_ids,1):
            if cancel_requested and cancel_requested(): cancelled=True; break
            try: results.append(self.generate_for_batch(batch_id,options=options,progress_callback=progress_callback))
            except Exception: results.append(DocumentGenerationResult(batch_id,0,(),(GeneratedDocument(batch_id,0,0,DocumentType.PDF_MEMBER.value,Path(),False,"Error de batch"),),self.output_root))
        failed=sum(bool(x.failed_documents) for x in results)
        return BatchDocumentGenerationResult(len(batch_ids),len(results)-failed,failed,tuple(results),cancelled)
    def regenerate_documents(self,batch_id:str,*,recipient_member_id:int|None=None,options:DocumentGenerationOptions=DocumentGenerationOptions()):
        self.repository.audit(batch_id,"DOCUMENT_REGENERATED",json.dumps({"recipient_member_id":recipient_member_id})); return self.generate_for_batch(batch_id,options=options,recipient_member_id=recipient_member_id)
