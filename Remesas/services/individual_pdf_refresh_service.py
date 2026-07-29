from __future__ import annotations
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import json, logging, os
import unicodedata
from pathlib import Path
from time import monotonic
from domain.benchmark_models import BenchmarkScope
from domain.document_models import DocumentType
from exporters.persisted_liquidation_pdf_exporter import export_persisted_liquidation_pdf
from presentation.liquidation_document_snapshot import load as load_snapshot
from services.persisted_variety_benchmark_service import variety_group_code

logger=logging.getLogger(__name__)
PDF_COMPARISON_RESOLUTION_LOG = Path(__file__).resolve().parent.parent / "logs" / "pdf_comparison_resolution.log"

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
    def __init__(self, repository, benchmark_service, *, exporter=export_persisted_liquidation_pdf, user=None,
                 comparison_log_path=PDF_COMPARISON_RESOLUTION_LOG):
        self.repository=repository; self.benchmarks=benchmark_service; self.exporter=exporter; self.user=user
        self.comparison_log_path=Path(comparison_log_path)

    @staticmethod
    def _candidate_metadata(key, value, generation_run_id):
        fields=("member_id","campaign","company","crop","group_label","subgroup","variety",
                "liquidation_id","document_id","batch_id","snapshot_id","generation_run_id",
                "liquidation_type","category")
        if len(key) >= 12:
            metadata=dict(zip(fields,key))
        else:  # Compatibility with callers using the original seven-part in-memory key.
            legacy=("member_id","group_label","campaign","company","crop","liquidation_type","category")
            metadata=dict(zip(legacy,key)); metadata["generation_run_id"]=generation_run_id
        metadata["benchmark"]=value
        return metadata

    def _write_resolution(self, context, candidates, selected, status, generation_run_id):
        self.comparison_log_path.parent.mkdir(parents=True,exist_ok=True)
        with self.comparison_log_path.open("a",encoding="utf-8") as stream:
            stream.write("[PdfComparisonResolution]\n")
            values={**context,"candidate_count":len(candidates),
                    "selected_candidate":selected if selected is not None else "",
                    "resolution_status":status,"generation_run_id":generation_run_id or ""}
            for field in ("document_id","liquidation_id","member_id","campaign","company","crop",
                          "group_label","subgroup","variety","batch_id","snapshot_id",
                          "generation_run_id","candidate_count","selected_candidate","resolution_status"):
                stream.write(f"{field}={values.get(field,'')}\n")
            stream.write("\n")
            if len(candidates)>1:
                for index,candidate in enumerate(candidates,1):
                    metric=candidate["benchmark"].kilograms_per_hectare
                    stream.write("[PdfComparisonCandidate]\n")
                    details={"document_id":context["document_id"],"candidate_index":index,
                             "trace_id":f"{generation_run_id or 'unknown'}:{context['document_id']}:{index}",
                             "run_id":candidate.get("generation_run_id",generation_run_id),
                             **{field:candidate.get(field,"") for field in ("member_id","campaign","group_label","subgroup","variety")},
                             "user_value":metric.own_value,"max_value":metric.maximum_value,
                             "average_value":metric.average_value,"min_value":metric.minimum_value,
                             "created_at":datetime.now(timezone.utc).isoformat()}
                    for field,value in details.items(): stream.write(f"{field}={'' if value is None else value}\n")
                    stream.write("\n")

    def _resolve_comparison(self, calculated_benchmarks, doc, vm, code, name, generation_run_id):
        def normalized(value):
            text=unicodedata.normalize("NFKD",str(value or "")).encode("ascii","ignore").decode()
            return " ".join(text.upper().split())
        liquidation_id="|".join(str(value) for value in (getattr(vm,"id_liqs",()) or ()))
        context={"document_id":getattr(doc,"document_id",None),"liquidation_id":liquidation_id,
                 "member_id":getattr(doc,"member_id",None),"campaign":str(doc.campaign),
                 "company":str(doc.company),"crop":str(getattr(vm,"crop","") or ""),
                 "group_label":name,"subgroup":str(getattr(getattr(vm,"group_benchmark",None),"subgroup","") or ""),
                 "variety":str(getattr(vm,"variety_name","") or ""),"batch_id":str(doc.batch_id),
                 "snapshot_id":getattr(doc,"snapshot_id",None)}
        candidates=[self._candidate_metadata(key,value,generation_run_id) for key,value in calculated_benchmarks.items()]
        # A generation identifier is the hard boundary.  Candidates explicitly tied
        # to another execution are never considered, even if every business field matches.
        candidates=[c for c in candidates if not generation_run_id or str(c.get("generation_run_id"))==str(generation_run_id)]
        criteria=("document_id","liquidation_id","snapshot_id","member_id","campaign","company","crop",
                  "group_label","subgroup","variety","batch_id")
        for field in criteria:
            expected=context.get(field)
            if expected in (None,""): continue
            explicit=[c for c in candidates if c.get(field) not in (None,"")]
            if explicit: candidates=[c for c in explicit if normalized(c.get(field))==normalized(expected)]
        status="UNIQUE" if len(candidates)==1 else ("NOT_FOUND" if not candidates else "AMBIGUOUS")
        selected=1 if status=="UNIQUE" else None
        self._write_resolution(context,candidates,selected,status,generation_run_id)
        if status!="UNIQUE":
            logger.warning("[PdfComparisonResolution] document_id=%s member_id=%s generation_run_id=%s candidate_count=%s resolution_status=%s; la auditoría no bloquea el PDF",
                           context["document_id"],context["member_id"],generation_run_id,len(candidates),status)
        return candidates[0]["benchmark"] if status=="UNIQUE" else None, status
    def scopes_for_documents(self, documents):
        scopes=[]
        for doc in documents:
            try:
                code,_name=_document_group(self.repository.list_document_variety_lines(doc.batch_id,doc.member_id))
                scopes.append(BenchmarkScope(str(doc.campaign),str(doc.company),code))
            except ValueError:
                continue
        return tuple(dict.fromkeys(scopes))
    def refresh_documents(self, documents, *, progress_callback=None, should_cancel=None, user=None,
                          calculated_benchmarks=None, benchmark_run_id=None, generation_run_id=None):
        generation_run_id=generation_run_id or benchmark_run_id
        docs=tuple(documents); scopes=self.scopes_for_documents(docs)
        if progress_callback:
            for i,_ in enumerate(scopes,1): progress_callback("comparativas",i,len(scopes))
        benchmarks={} if calculated_benchmarks else self.benchmarks.get_group_benchmarks(scopes); items=[]
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
                snapshot_has_benchmark=vm.group_benchmark is not None
                if calculated_benchmarks is not None:
                    audited_benchmark,resolution_status=self._resolve_comparison(
                        calculated_benchmarks,doc,vm,code,name,generation_run_id)
                    # Audit data is diagnostic.  On NOT_FOUND/AMBIGUOUS retain the
                    # persisted document value rather than cancelling rendering/merge.
                    group_benchmark=audited_benchmark if audited_benchmark is not None else vm.group_benchmark
                    fingerprint=f"AUDITED:{generation_run_id or 'CURRENT'}"
                else:
                    scope=BenchmarkScope(str(doc.campaign),str(doc.company),code)
                    benchmark=benchmarks[scope]
                    if benchmark.comparable_members and not (benchmark.production_metric.comparable_count or benchmark.final_amount_metric.comparable_count):
                        raise ValueError("No se recuperó ninguna superficie válida; la actualización de comparativas no puede considerarse completada.")
                    group_benchmark=self.benchmarks.for_member(benchmark,doc.member_id,template=vm.group_benchmark,group_name=name,campaign=str(doc.campaign))
                    fingerprint=benchmark.source_fingerprint
                vm=replace(vm,group_benchmark=group_benchmark)
                audit_context={
                    "benchmark_source_fingerprint":fingerprint,
                    "group_code":code,
                    "group_name":name,
                    "comparable_members":(group_benchmark.kilograms_per_hectare.valid_member_count
                                          if group_benchmark is not None else 0),
                    "benchmark_created_from_scratch":not snapshot_has_benchmark,
                    "document_id":doc.document_id,
                    "recipient_member_id":doc.member_id,
                }
                self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_STARTED",json.dumps(audit_context),user or self.user)
                self.exporter(vm,temp); digest=sha256(temp.read_bytes()).hexdigest(); path.parent.mkdir(parents=True,exist_ok=True); os.replace(temp,path)
                self.repository.supersede_member_document(doc.batch_id,doc.member_id)
                self.repository.record_document(batch_id=doc.batch_id,remittance_id=doc.remittance_id,recipient_member_id=doc.member_id,document_type=DocumentType.PDF_MEMBER.value,file_path=str(path),status="GENERATED",generated_at=datetime.now(timezone.utc).isoformat(),file_hash=digest,created_by=user or self.user,benchmark_source_fingerprint=fingerprint)
                self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_COMPLETED",json.dumps({**audit_context,"new_hash":digest}),user or self.user)
                items.append(IndividualPdfRefreshItem(doc.document_id,doc.batch_id,doc.member_id,path,True,fingerprint))
                logger.info("[IndividualPdfRefresh] document_id=%s batch_id=%s member_id=%s snapshot_has_benchmark=%s benchmark_created_from_scratch=%s group_code=%s group_name=%s status=GENERATED duration_ms=%d",doc.document_id,doc.batch_id,doc.member_id,snapshot_has_benchmark,not snapshot_has_benchmark,code,name,(monotonic()-started)*1000)
            except Exception as exc:
                temp.unlink(missing_ok=True); self.repository.audit(doc.batch_id,"INDIVIDUAL_PDF_REFRESH_FAILED",json.dumps({"document_id":doc.document_id,"recipient_member_id":doc.member_id,"error":str(exc)}),user or self.user)
                items.append(IndividualPdfRefreshItem(doc.document_id,doc.batch_id,doc.member_id,path,False,error=str(exc))); logger.exception("[IndividualPdfRefresh] document_id=%s status=FAILED",doc.document_id)
            if progress_callback: progress_callback("documentos",index,len(docs))
        return IndividualPdfRefreshResult(tuple(items))
