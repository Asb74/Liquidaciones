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

class IndividualPdfRefreshService:
    def __init__(self, repository, benchmark_service, *, exporter=export_persisted_liquidation_pdf, user=None):
        self.repository=repository; self.benchmarks=benchmark_service; self.exporter=exporter; self.user=user
    def scopes_for_documents(self, documents):
        scopes=[]
        for doc in documents:
            snapshot=self.repository.get_document_snapshot(doc.batch_id,doc.member_id)
            if not snapshot: continue
            vm=load_snapshot(snapshot["payload_json"]); group=vm.group_benchmark
            if group: scopes.append(BenchmarkScope(str(doc.campaign),str(doc.company),variety_group_code(group.group,group.subgroup)))
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
                if not vm.group_benchmark: raise ValueError("Comparativa con el grupo varietal no disponible.")
                scope=BenchmarkScope(str(doc.campaign),str(doc.company),variety_group_code(vm.group_benchmark.group,vm.group_benchmark.subgroup))
                benchmark=benchmarks[scope]
                vm=replace(vm,group_benchmark=self.benchmarks.for_member(benchmark,doc.member_id,vm.group_benchmark))
                self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_STARTED",json.dumps({"document_id":doc.document_id,"recipient_member_id":doc.member_id,"source_fingerprint":benchmark.source_fingerprint}),user or self.user)
                self.exporter(vm,temp); digest=sha256(temp.read_bytes()).hexdigest(); path.parent.mkdir(parents=True,exist_ok=True); os.replace(temp,path)
                self.repository.supersede_member_document(doc.batch_id,doc.member_id)
                self.repository.record_document(batch_id=doc.batch_id,remittance_id=doc.remittance_id,recipient_member_id=doc.member_id,document_type=DocumentType.PDF_MEMBER.value,file_path=str(path),status="GENERATED",generated_at=datetime.now(timezone.utc).isoformat(),file_hash=digest,created_by=user or self.user,benchmark_source_fingerprint=benchmark.source_fingerprint)
                self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_COMPLETED",json.dumps({"document_id":doc.document_id,"recipient_member_id":doc.member_id,"new_hash":digest,"source_fingerprint":benchmark.source_fingerprint}),user or self.user)
                items.append(IndividualPdfRefreshItem(doc.document_id,doc.batch_id,doc.member_id,path,True,benchmark.source_fingerprint))
                logger.info("[IndividualPdfRefresh] document_id=%s batch_id=%s recipient_member_id=%s status=GENERATED duration_ms=%d",doc.document_id,doc.batch_id,doc.member_id,(monotonic()-started)*1000)
            except Exception as exc:
                temp.unlink(missing_ok=True); self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_FAILED",json.dumps({"document_id":doc.document_id,"recipient_member_id":doc.member_id,"error":str(exc)}),user or self.user)
                items.append(IndividualPdfRefreshItem(doc.document_id,doc.batch_id,doc.member_id,path,False,error=str(exc))); logger.exception("[IndividualPdfRefresh] document_id=%s status=FAILED",doc.document_id)
            if progress_callback: progress_callback("documentos",index,len(docs))
        return IndividualPdfRefreshResult(tuple(items))
