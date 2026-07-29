"""Global productive-benchmark validation performed before a massive PDF rebuild.

The service deliberately has no Tk dependency and does not generate documents.  It
reconstructs persisted business snapshots, consolidates every selected remittance,
and only then delegates benchmark/surface calculation to GroupBenchmarkService.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Sequence

from domain.calculation_models import LiquidationHeader
from presentation.liquidation_document_snapshot import load as load_snapshot
from services.group_benchmark_applicability import CROP_NOT_INCLUDED_REASON, is_group_benchmark_applicable

logger = logging.getLogger(__name__)


class MassivePdfMode(str, Enum):
    MERGE_EXISTING_PDFS = "MERGE_EXISTING_PDFS"
    REBUILD_AND_VALIDATE = "REBUILD_AND_VALIDATE"


@dataclass(frozen=True)
class MassiveBenchmarkIncident:
    code: str
    message: str
    severe: bool = False
    document_id: int | None = None
    member_id: int | None = None


@dataclass(frozen=True)
class MassiveMemberProduction:
    context: tuple[str, ...]
    member_id: int
    member_name: str
    group_label: str
    varieties: tuple[str, ...]
    remittances: tuple[str, ...]
    line_count: int
    total_net_kg: Decimal
    total_commercial_kg: Decimal
    total_amount: Decimal
    surface_hectares: Decimal | None
    production_kg_ha: Decimal | None
    status: str


@dataclass(frozen=True)
class MassiveBenchmarkAuditResult:
    mass_run_id: str
    mode: MassivePdfMode
    productions: tuple[MassiveMemberProduction, ...]
    incidents: tuple[MassiveBenchmarkIncident, ...]
    context_count: int
    document_count: int
    benchmarks: dict[tuple, object] | None = None

    @property
    def generation_run_id(self) -> str:
        """Identifier shared by calculation, DTO resolution, rendering and merge."""
        return self.mass_run_id

    @property
    def has_severe_incidents(self) -> bool:
        return any(item.severe for item in self.incidents)


class MassiveBenchmarkAuditService:
    """Rebuild and consolidate the business inputs selected in the PDF tool."""

    def __init__(self, repository, benchmark_service, *, audit_log_path: str | Path = "logs/mass_pdf_benchmark_audit.log"):
        self.repository = repository
        self.benchmark_service = benchmark_service
        self.audit_log_path = Path(audit_log_path)

    @staticmethod
    def new_run_id() -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    def _write(self, section: str, **values) -> None:
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_log_path.open("a", encoding="utf-8") as stream:
            stream.write(f"[{section}]\n")
            for key, value in values.items():
                if isinstance(value, (tuple, list)): value = "|".join(str(x) for x in value)
                stream.write(f"{key}={'' if value is None else value}\n")
            stream.write("\n")

    @staticmethod
    def _decimal(value):
        if value is None: return None
        try: return value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError): return None

    def audit_selection(self, liquidations: Sequence, filters=None, *, mode=MassivePdfMode.REBUILD_AND_VALIDATE,
                        progress_callback: Callable[[str, int, int], None] | None = None) -> MassiveBenchmarkAuditResult:
        mode = MassivePdfMode(mode)
        if mode is not MassivePdfMode.REBUILD_AND_VALIDATE:
            raise ValueError("La auditoría masiva requiere REBUILD_AND_VALIDATE")
        documents = tuple(liquidations); started = datetime.now(); run_id = self.new_run_id()
        filters = filters or {}
        remittances = {str(getattr(d, "remittance_id", "")) for d in documents}
        members = {getattr(d, "member_id", None) for d in documents if getattr(d, "member_id", None) is not None}
        start = dict(mass_run_id=run_id, started_at=started.isoformat(), campaign_filter=filters.get("campaign", ""),
                     company_filter=filters.get("company", ""), crop_filter=filters.get("crop", ""),
                     selected_document_count=len(documents), selected_remittance_count=len(remittances),
                     selected_member_count=len(members), mode=mode.value)
        self._write("MassPdfBenchmarkAuditRun", **start)
        logger.info("[MassPdfBenchmarkAuditRun] %s", " ".join(f"{k}={v}" for k,v in start.items()))
        logger.info("[MassPdfFlowTrace] phase=RECONSTRUCTING selected_documents=%s selected_remittances=%s selected_members=%s source=SNAPSHOT", len(documents),len(remittances),len(members))
        incidents=[]; grouped={}; incomplete_count=0
        for index, doc in enumerate(documents, 1):
            if progress_callback: progress_callback("2. Reconstruyendo datos", index, len(documents))
            path=Path(getattr(doc,"file_path", "")); exists=path.is_file()
            self._write("MassPdfExistingDocument",mass_run_id=run_id,document_id=getattr(doc,"document_id",None),path=path,
                        exists="yes" if exists else "no",will_reuse="no",reason="MASSIVE_VALIDATION_REQUIRES_REBUILD")
            snapshot=self.repository.get_document_snapshot(doc.batch_id,doc.member_id) if getattr(doc,"batch_id",None) else None
            raw={}; vm=None
            try:
                raw=json.loads(snapshot["payload_json"] if snapshot else "{}")
                vm=load_snapshot(snapshot["payload_json"]) if snapshot else None
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                incidents.append(MassiveBenchmarkIncident("INCOMPLETE_SNAPSHOT",str(exc),True,getattr(doc,"document_id",None),getattr(doc,"member_id",None)))
            model=raw.get("model",{}) if isinstance(raw,dict) else {}
            checks={"has_net_kg": model.get("effective_net_kg") is not None,
                    "has_commercial_kg": model.get("commercial_kg") is not None,
                    "has_amount": model.get("total_amount") is not None,
                    "has_variety": bool(model.get("variety_name") or model.get("varieties"))}
            complete=vm is not None and all(checks.values()) and bool(getattr(vm,"member_id",None)) and bool(getattr(vm,"campaign",None)) and bool(getattr(vm,"company",None)) and bool(getattr(vm,"crop",None))
            self._write("MassPdfSnapshotValidation",mass_run_id=run_id,document_id=getattr(doc,"document_id",None),remittance_id=getattr(doc,"remittance_id",None),member_id=getattr(doc,"member_id",None),schema_version=raw.get("schema_version",""),**{k:"yes" if v else "no" for k,v in checks.items()},status="OK" if complete else "INCOMPLETE")
            if not complete:
                incomplete_count+=1
                if not any(x.document_id==getattr(doc,"document_id",None) and x.code=="INCOMPLETE_SNAPSHOT" for x in incidents): incidents.append(MassiveBenchmarkIncident("INCOMPLETE_SNAPSHOT","Faltan campos de negocio obligatorios",True,getattr(doc,"document_id",None),getattr(doc,"member_id",None)))
                continue
            if not is_group_benchmark_applicable(vm.crop,getattr(doc,"document_type",None),None):
                self._write("MassPdfBenchmarkNotApplicable",mass_run_id=run_id,
                            document_id=getattr(doc,"document_id",None),member_id=vm.member_id,
                            campaign=vm.campaign,crop=vm.crop,reason=CROP_NOT_INCLUDED_REASON)
                continue
            benchmark=getattr(vm,"group_benchmark",None)
            group_label=(getattr(vm,"variety_group_name",None) or (benchmark.group_label if benchmark else "")).strip()
            liquidation_type=benchmark.liquidation_type if benchmark else ""
            category=benchmark.category if benchmark else ""
            if not group_label:
                incidents.append(MassiveBenchmarkIncident("MISSING_GROUP","Liquidación sin grupo varietal",True,getattr(doc,"document_id",None),vm.member_id)); continue
            context=(str(vm.campaign),str(vm.company),str(vm.crop),group_label,str(liquidation_type),str(category))
            key=context+(str(vm.member_id),)
            item=grouped.setdefault(key,{"vm":vm,"documents":[],"varieties":set(),"remittances":set(),"lines":0,"net":Decimal(0),"commercial":Decimal(0),"amount":Decimal(0)})
            item["documents"].append((doc, vm))
            item["varieties"].update(v for v in (getattr(vm,"variety_name",None),*getattr(vm,"varieties",())) if v)
            item["remittances"].add(str(getattr(doc,"remittance_id",None) or vm.remittance_name)); item["lines"]+=1
            item["net"]+=self._decimal(vm.effective_net_kg) or Decimal(0); item["commercial"]+=self._decimal(vm.commercial_kg) or Decimal(0); item["amount"]+=self._decimal(vm.total_amount) or Decimal(0)
        if progress_callback: progress_callback("3. Agrupando socios y grupos",len(grouped),len(grouped))
        by_context={}
        for key,item in grouped.items(): by_context.setdefault(key[:6],[]).append((key,item))
        productions=[]; calculated_benchmarks={}
        for context_index,(context,entries) in enumerate(by_context.items(),1):
            campaign,company,crop,group_label,liq_type,category=context
            members_for_benchmark=[]
            for key,item in entries:
                vm=item["vm"]; varieties=tuple(sorted(item["varieties"])); rems=tuple(sorted(item["remittances"]))
                self._write("MassBenchmarkMemberAggregation",mass_run_id=run_id,member_id=vm.member_id,member_name=vm.member_name,campaign=campaign,company=company,crop=crop,group_label=group_label,varieties=varieties,remittance_count=len(rems),remittances=rems,line_count=item["lines"],total_net_kg=item["net"],total_commercial_kg=item["commercial"],total_amount=item["amount"])
                for variety in (varieties[:1] or (getattr(vm,"variety_name",None),)):
                    members_for_benchmark.append(SimpleNamespace(member_id=vm.member_id,member_name=vm.member_name,variety=variety,net_kg=item["net"],commercial_kg=item["commercial"],total_amount=item["amount"],statuses={}))
            header=LiquidationHeader(None,"AUDITORIA MASIVA",campaign,company,crop,"","","",liq_type,category,"",[],{},{})
            if progress_callback: progress_callback("4. Validando superficies",context_index,len(by_context))
            benchmarks=self.benchmark_service.build_benchmarks(header,tuple(members_for_benchmark),parent_run_id=run_id,run_source="MASS_PDF_REBUILD")
            # The calculation service keys a result by member and business context.  Do
            # not expose that coarse key to PDF resolution: one member/context may have
            # several real documents, and later runs may contain the same tuple.
            for benchmark_key, benchmark in benchmarks.items():
                member_id = int(benchmark_key[0])
                matching_entry = next((item for key, item in entries if int(key[6]) == member_id), None)
                if matching_entry is None:
                    continue
                for document, document_vm in matching_entry["documents"]:
                    liquidation_ids = tuple(str(value) for value in (getattr(document_vm, "id_liqs", ()) or ()))
                    correlation_key = (
                        member_id, campaign, company, crop, group_label,
                        str(getattr(benchmark, "subgroup", "") or ""),
                        str(getattr(document_vm, "variety_name", "") or ""),
                        "|".join(liquidation_ids), getattr(document, "document_id", None),
                        str(getattr(document, "batch_id", "") or ""),
                        getattr(document, "snapshot_id", None), run_id,
                        liq_type, category,
                    )
                    calculated_benchmarks[correlation_key] = benchmark
            details=getattr(self.benchmark_service,"last_surface_details",{})
            for key,item in entries:
                vm=item["vm"]; detail=details.get((vm.member_id,group_label),{}); ha=detail.get("hectares")
                prod=item["net"]/ha if ha is not None and ha>0 and item["net"]>0 else None
                warnings=tuple(detail.get("warnings",())); status="OK" if prod is not None else ("INVALID_KG" if item["net"]<=0 else "INVALID_HECTARES")
                if warnings and prod is not None: status="PARTIAL_SURFACE"
                if any("distintas" in w for w in warnings): incidents.append(MassiveBenchmarkIncident("CONFLICTING_SURFACE","; ".join(warnings),True,member_id=vm.member_id))
                rems=tuple(sorted(item["remittances"])); varieties=tuple(sorted(item["varieties"]))
                production=MassiveMemberProduction(context,vm.member_id,vm.member_name,group_label,varieties,rems,item["lines"],item["net"],item["commercial"],item["amount"],ha,prod,status); productions.append(production)
                self._write("MassBenchmarkMemberProduction",mass_run_id=run_id,member_id=vm.member_id,group_label=group_label,total_net_kg=item["net"],surface_hectares=ha,production_kg_ha=prod,candidate_boletas=detail.get("candidate_boletas",()),matched_boletas=detail.get("matched_boletas",()),included_boletas=detail.get("included_boletas",()),parcel_count=detail.get("parcel_count",0),excluded_count=detail.get("excluded_count",0),warnings=warnings,status=status)
        valid=[p for p in productions if p.production_kg_ha is not None]; values=[p.production_kg_ha for p in valid]
        minimum=min(valid,key=lambda p:p.production_kg_ha) if valid else None; maximum=max(valid,key=lambda p:p.production_kg_ha) if valid else None
        summary=dict(mass_run_id=run_id,documents_selected=len(documents),documents_rebuilt=len(documents),contexts_processed=len(by_context),members_processed=len(productions),valid_production_count=len(valid),invalid_surface_count=sum(p.status=="INVALID_HECTARES" for p in productions),partial_surface_count=sum(p.status=="PARTIAL_SURFACE" for p in productions),conflicting_surface_count=sum(x.code=="CONFLICTING_SURFACE" for x in incidents),missing_group_count=sum(x.code=="MISSING_GROUP" for x in incidents),minimum_production=min(values) if values else None,average_production=sum(values,Decimal(0))/len(values) if values else None,maximum_production=max(values) if values else None,minimum_member_id=minimum.member_id if minimum else None,minimum_member_name=minimum.member_name if minimum else None,minimum_remittances=minimum.remittances if minimum else (),maximum_member_id=maximum.member_id if maximum else None,maximum_member_name=maximum.member_name if maximum else None,maximum_remittances=maximum.remittances if maximum else ())
        self._write("MassPdfBenchmarkSummary",**summary)
        status="ERROR" if any(x.severe for x in incidents) else ("WARNING" if incidents else "OK")
        completed=dict(mass_run_id=run_id,context_count=len(by_context),member_group_count=len(productions),document_count=len(documents),incident_count=len(incidents),status=status,finished_at=datetime.now().isoformat())
        self._write("MassPdfBenchmarkAuditRunCompleted",**completed); logger.info("[MassPdfBenchmarkAuditRunCompleted] %s", " ".join(f"{k}={v}" for k,v in completed.items()))
        return MassiveBenchmarkAuditResult(run_id,mode,tuple(productions),tuple(incidents),len(by_context),len(documents),calculated_benchmarks)
