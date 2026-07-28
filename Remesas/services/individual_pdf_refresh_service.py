from __future__ import annotations
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import json, logging, os
from pathlib import Path
from time import monotonic
from domain.benchmark_models import BenchmarkScope
from domain.document_models import DocumentType
from exporters.persisted_liquidation_pdf_exporter import export_persisted_liquidation_pdf
from presentation.liquidation_document_snapshot import load as load_snapshot
from services.persisted_variety_benchmark_service import variety_group_code

logger=logging.getLogger(__name__)

@dataclass(frozen=True)
class IndividualPdfRefreshItem:
    document_id: int | None; batch_id: str; recipient_member_id: int; path: Path
    success: bool; source_fingerprint: str | None=None; error: str | None=None

@dataclass(frozen=True)
class IndividualPdfRefreshResult:
    items: tuple[IndividualPdfRefreshItem,...]; cancelled: bool=False
    @property
    def failed(self): return tuple(x for x in self.items if not x.success)
    @property
    def unresolved(self): return tuple(x for x in self.failed if x.error and "resolver el grupo" in x.error)
    @property
    def multiple_groups(self): return tuple(x for x in self.failed if x.error and "más de un grupo" in x.error)


def _document_group(lines):
    groups={(str(row["variety_group_code"]),str(row["variety_group_name"] or row["variety_group_code"]))
            for row in lines if row["variety_group_code"]}
    if len(groups)>1:
        raise ValueError("El documento contiene liquidaciones de más de un grupo varietal y no puede generar una comparativa única.")
    if not groups:
        variety=next((str(row["variety_name"] or row["variety"] or "").strip() for row in lines if row["variety_name"] or row["variety"]), "—")
        raise ValueError(f"No se ha podido resolver el grupo varietal de la variedad {variety}.")
    return next(iter(groups))

class IndividualPdfRefreshService:
    def __init__(self, repository, benchmark_service, *, exporter=export_persisted_liquidation_pdf, user=None):
        self.repository=repository; self.benchmarks=benchmark_service; self.exporter=exporter; self.user=user
    def scopes_for_documents(self, documents):
        scopes=[]
        for doc in documents:
            try:
                code,_name=_document_group(self.repository.list_document_variety_lines(doc.batch_id,doc.member_id))
                scopes.append(BenchmarkScope(str(doc.campaign),str(doc.company),code))
            except ValueError:
                continue
        return tuple(dict.fromkeys(scopes))
    def refresh_documents(self, documents, *, progress_callback=None, should_cancel=None, user=None):
        docs=tuple(documents); scopes=self.scopes_for_documents(docs)
        if progress_callback:
            for i,_ in enumerate(scopes,1): progress_callback("comparativas",i,len(scopes))
        benchmarks=self.benchmarks.get_group_benchmarks(scopes); items=[]
        for index,doc in enumerate(docs,1):
            if should_cancel and should_cancel(): return IndividualPdfRefreshResult(tuple(items),True)
            started=monotonic(); path=Path(doc.file_path); temp=path.with_suffix(path.suffix+".refresh.tmp")
            try:
                snapshot=self.repository.get_document_snapshot(doc.batch_id,doc.member_id)
                if not snapshot: raise ValueError("El documento no tiene snapshot económico persistido.")
                vm=load_snapshot(snapshot["payload_json"])
                code,name=_document_group(self.repository.list_document_variety_lines(doc.batch_id,doc.member_id))
                logger.info("[VarietyGroupResolution]\nbatch_id=%s\nrecipient_member_id=%s\nid_liq=%s\nvariety=%s\nnormalized_variety=%s\ngroup_code=%s\ngroup_name=%s\nsource=liquidation\nstatus=RESOLVED",
                    doc.batch_id,doc.member_id,",".join(vm.id_liqs),vm.variety_name or vm.variety_text,
                    vm.variety_code or "",code,name)
                scope=BenchmarkScope(str(doc.campaign),str(doc.company),code)
                benchmark=benchmarks[scope]
                snapshot_has_benchmark=vm.group_benchmark is not None
                group_benchmark=self.benchmarks.for_member(
                    benchmark,
                    doc.member_id,
                    template=vm.group_benchmark,
                    group_name=name,
                    campaign=str(doc.campaign),
                )
                vm=replace(vm,group_benchmark=group_benchmark)
                audit_context={
                    "benchmark_source_fingerprint":benchmark.source_fingerprint,
                    "group_code":code,
                    "group_name":name,
                    "comparable_members":len(benchmark.comparable_members),
                    "benchmark_created_from_scratch":not snapshot_has_benchmark,
                    "document_id":doc.document_id,
                    "recipient_member_id":doc.member_id,
                }
                self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_STARTED",json.dumps(audit_context),user or self.user)
                self.exporter(vm,temp); digest=sha256(temp.read_bytes()).hexdigest(); path.parent.mkdir(parents=True,exist_ok=True); os.replace(temp,path)
                self.repository.supersede_member_document(doc.batch_id,doc.member_id)
                self.repository.record_document(batch_id=doc.batch_id,remittance_id=doc.remittance_id,recipient_member_id=doc.member_id,document_type=DocumentType.PDF_MEMBER.value,file_path=str(path),status="GENERATED",generated_at=datetime.now(timezone.utc).isoformat(),file_hash=digest,created_by=user or self.user,benchmark_source_fingerprint=benchmark.source_fingerprint)
                self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_COMPLETED",json.dumps({**audit_context,"new_hash":digest}),user or self.user)
                items.append(IndividualPdfRefreshItem(doc.document_id,doc.batch_id,doc.member_id,path,True,benchmark.source_fingerprint))
                logger.info("[IndividualPdfRefresh] document_id=%s batch_id=%s member_id=%s snapshot_has_benchmark=%s benchmark_created_from_scratch=%s group_code=%s group_name=%s status=GENERATED duration_ms=%d",doc.document_id,doc.batch_id,doc.member_id,snapshot_has_benchmark,not snapshot_has_benchmark,code,name,(monotonic()-started)*1000)
            except Exception as exc:
                temp.unlink(missing_ok=True); self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_FAILED",json.dumps({"document_id":doc.document_id,"recipient_member_id":doc.member_id,"error":str(exc)}),user or self.user)
                items.append(IndividualPdfRefreshItem(doc.document_id,doc.batch_id,doc.member_id,path,False,error=str(exc))); logger.exception("[IndividualPdfRefresh] document_id=%s status=FAILED",doc.document_id)
            if progress_callback: progress_callback("documentos",index,len(docs))
        return IndividualPdfRefreshResult(tuple(items))
