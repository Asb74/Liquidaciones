from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import services.persisted_variety_benchmark_service as module
from domain.benchmark_models import BenchmarkScope, PersistedBenchmarkMetric, PersistedMemberBenchmark, VarietyGroupBenchmark
from presentation.liquidation_document_snapshot import dump, load
from services.group_benchmark_service import PremiumGroupBenchmark
from services.individual_pdf_refresh_service import IndividualPdfRefreshService
from services.persisted_variety_benchmark_service import PersistedVarietyBenchmarkService
from tests.test_premium_pdf import _header, _member
from presentation.premium_liquidation_view_model import from_member_liquidation

class Repository:
    def __init__(self, rows): self.rows=rows; self.calls=0
    def list_persisted_benchmark_rows(self,campaign,company): self.calls+=1; return self.rows

def row(identifier,member,batch,kg,amount,price="2",payload="snapshot",crop="CITRICOS",date="2026-01-01"):
    return {"id":identifier,"recipient_member_id":member,"socio":f"S{member}","neto":kg,"precio_medio":price,"importe_total":amount,"batch_id":batch,"status":"ACTIVE","batch_status":"ACTIVE","payload_json":payload,"variety_group_code":"NAVEL_TARDIA","payment_date":date,"snapshot_created_at":date,"cultivo":crop}

def frozen(surface, *, benchmark=True, fingerprint="surface-v1"):
    group=SimpleNamespace(group="NAVEL",subgroup="TARDÍA") if benchmark else None
    return SimpleNamespace(group_benchmark=group,applicable_hectares=Decimal(surface) if surface is not None else None,surface_source="HECTARE_FEE",surface_fingerprint=fingerprint)

def test_global_benchmark_aggregates_remittances_weighted_and_caches(monkeypatch):
    values=iter((frozen("1"),frozen("1"),frozen("2")))
    monkeypatch.setattr(module,"load_snapshot",lambda _:next(values))
    repo=Repository((row(1,818,"A","100","200"),row(2,818,"B","300","900","3"),row(3,1395,"C","100","100","1")))
    service=PersistedVarietyBenchmarkService(repo)
    benchmark=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    again=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    assert again is benchmark and repo.calls==1
    member=benchmark.comparable_members[0]
    assert member.commercial_kg==Decimal("400")
    assert member.final_average_price==Decimal("2.75")
    assert member.production_kg_ha==Decimal("400")
    assert member.final_amount_eur_ha==Decimal("1100")
    assert benchmark.price_metric.comparable_count==2
    assert benchmark.production_metric.comparable_count==2

def test_fingerprint_changes_when_persisted_population_changes(monkeypatch):
    monkeypatch.setattr(module,"load_snapshot",lambda _:frozen("1"))
    repo=Repository([row(1,1,"A","100","200")]); service=PersistedVarietyBenchmarkService(repo)
    first=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA").source_fingerprint
    repo.rows.append(row(2,2,"B","100","300")); service.clear_cache()
    second=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA").source_fingerprint
    assert first!=second


def test_snapshot_without_group_benchmark_uses_explicit_surface(monkeypatch):
    monkeypatch.setattr(module,"load_snapshot",lambda _:frozen("2.5",benchmark=False))
    benchmark=PersistedVarietyBenchmarkService(Repository([row(1,1,"A","1000","500")])).get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    member=benchmark.comparable_members[0]
    assert member.surface_hectares==Decimal("2.5")
    assert member.production_kg_ha==Decimal("400")
    assert member.final_amount_eur_ha==Decimal("200")


def test_old_snapshot_recovers_only_surface_from_cuota_ha_service(monkeypatch):
    monkeypatch.setattr(module,"load_snapshot",lambda _:frozen(None,benchmark=False))
    class Surface:
        def __init__(self): self.calls=[]
        def get_member_variety_group_surface(self,**scope):
            self.calls.append(scope); return Decimal("4"),"fallback-fingerprint",()
    surface=Surface()
    benchmark=PersistedVarietyBenchmarkService(Repository([row(1,7,"A","800","400")]),surface_service=surface).get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    assert benchmark.comparable_members[0].production_kg_ha==Decimal("200")
    assert len(surface.calls)==1


def test_live_surface_replaces_stale_snapshot_for_pdf_benchmark(monkeypatch):
    monkeypatch.setattr(module,"load_snapshot",lambda _:frozen("1.0552"))
    class Surface:
        def get_member_variety_group_surface(self,**_scope):
            return Decimal("7.4259"),"current-surface",()
    benchmark=PersistedVarietyBenchmarkService(
        Repository([row(1,1623,"A","112745","1000")]),surface_service=Surface()
    ).get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    member=benchmark.comparable_members[0]
    assert member.surface_hectares==Decimal("7.4259")
    assert member.production_kg_ha.quantize(Decimal("0.01"))==Decimal("15182.67")


def test_latest_surface_is_selected_once_across_remittances_and_crops(monkeypatch,caplog):
    values=iter((frozen("2",fingerprint="old"),frozen("3",fingerprint="new"),frozen("3",fingerprint="new")))
    monkeypatch.setattr(module,"load_snapshot",lambda _:next(values))
    rows=[row(1,1,"A","300","300",crop="CITRICOS",date="2026-01-01"),row(2,1,"B","300","300",crop="INDUSTRIA",date="2026-02-01"),row(3,1,"C","300","300",crop="DIRECTO",date="2026-03-01")]
    benchmark=PersistedVarietyBenchmarkService(Repository(rows)).get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    member=benchmark.comparable_members[0]
    assert member.surface_hectares==Decimal("3") and member.production_kg_ha==Decimal("300")
    assert "SURFACE_VALUE_CHANGED" in caplog.text


def test_member_without_surface_remains_in_price_but_not_hectare_metrics(monkeypatch):
    values=iter((frozen("2"),frozen(None)))
    monkeypatch.setattr(module,"load_snapshot",lambda _:next(values))
    benchmark=PersistedVarietyBenchmarkService(Repository([row(1,1,"A","100","200"),row(2,2,"B","100","300")])).get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA")
    assert benchmark.price_metric.comparable_count==2
    assert benchmark.production_metric.comparable_count==benchmark.final_amount_metric.comparable_count==1


def test_surface_changes_benchmark_fingerprint(monkeypatch):
    current=["2"]
    monkeypatch.setattr(module,"load_snapshot",lambda _:frozen(current[0],fingerprint="fp-"+current[0]))
    service=PersistedVarietyBenchmarkService(Repository([row(1,1,"A","100","200")]))
    first=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA").source_fingerprint
    current[0]="4"; service.clear_cache()
    second=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA").source_fingerprint
    assert first!=second


def complete_benchmark(members=True):
    comparable=(PersistedMemberBenchmark(818,"S818",Decimal("100"),Decimal("250"),Decimal("2"),Decimal("2.5"),Decimal("50"),Decimal("125")),) if members else ()
    metric=lambda maximum,average,minimum: PersistedBenchmarkMetric(None,maximum,average,minimum,None,len(comparable))
    return VarietyGroupBenchmark(
        BenchmarkScope("2026","1","NAVEL_TEMPRANA"), comparable,
        metric(Decimal("3"),Decimal("2.5"),Decimal("2")) if members else metric(None,None,None),
        metric(Decimal("60"),Decimal("50"),Decimal("40")) if members else metric(None,None,None),
        metric(Decimal("150"),Decimal("125"),Decimal("100")) if members else metric(None,None,None),
        datetime.now(timezone.utc), "fingerprint",
    )


def test_for_member_builds_complete_group_benchmark_without_template():
    result=PersistedVarietyBenchmarkService.for_member(
        complete_benchmark(),818,template=None,group_name="NAVEL TEMPRANA",campaign="2026"
    )
    assert isinstance(result,PremiumGroupBenchmark)
    assert (result.group_label,result.group,result.subgroup,result.campaign)==("NAVEL TEMPRANA","NAVEL","TEMPRANA","2026")
    assert result.price_per_kg.own_value==Decimal("2.5")
    assert (result.price_per_kg.maximum_value,result.price_per_kg.average_value,result.price_per_kg.minimum_value)==(Decimal("3"),Decimal("2.5"),Decimal("2"))
    assert result.price_per_kg.valid_member_count==1
    assert result.kilograms_per_hectare.own_value==Decimal("50")
    assert result.euros_per_hectare.own_value==Decimal("125")


def test_for_member_without_comparable_data_builds_unavailable_metrics():
    result=PersistedVarietyBenchmarkService.for_member(complete_benchmark(False),999,group_name="BLANCA TEMPRANA")
    for metric in (result.price_per_kg,result.kilograms_per_hectare,result.euros_per_hectare):
        assert metric.own_value is None and metric.valid_member_count==0
        assert metric.status=="unavailable" and metric.warning=="Sin datos comparables suficientes"


def test_for_member_keeps_optional_metadata_from_old_template():
    original=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA")
    old=module.replace(original,crop="CITRICOS",varieties=("NAVELINA",),liquidation_type="Normal",category="Primera")
    result=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,template=old,group_name="NAVEL TEMPRANA",campaign="2026")
    assert (result.crop,result.varieties,result.liquidation_type,result.category)==("CITRICOS",("NAVELINA",),"Normal","Primera")


class RefreshRepository:
    def __init__(self,payload): self.payload=payload; self.audits=[]; self.recorded=[]
    def list_document_variety_lines(self,*_): return ({"variety_group_code":"NAVEL_TEMPRANA","variety_group_name":"NAVEL TEMPRANA","variety":"NAVELINA","variety_name":"NAVELINA"},)
    def get_document_snapshot(self,*_): return {"payload_json":self.payload}
    def audit(self,*args): self.audits.append(args)
    def supersede_member_document(self,*_): pass
    def record_document(self,**values): self.recorded.append(values)


class RefreshBenchmarks:
    def get_group_benchmarks(self,scopes): return {scope:complete_benchmark() for scope in scopes}
    for_member=staticmethod(PersistedVarietyBenchmarkService.for_member)


def test_refresh_snapshot_without_group_benchmark_creates_and_renders_pdf(tmp_path: Path):
    vm=from_member_liquidation(_header(),_member(member_id=818,variety="NAVELINA"))
    repository=RefreshRepository(dump(vm)); rendered=[]
    def exporter(updated,path):
        rendered.append(updated); Path(path).write_bytes(b"generated pdf")
    document=SimpleNamespace(document_id=7,batch_id="B1",member_id=818,campaign="2026",company="1",file_path=str(tmp_path/"member.pdf"),remittance_id=3)
    result=IndividualPdfRefreshService(repository,RefreshBenchmarks(),exporter=exporter).refresh_documents((document,))
    assert not result.failed and (tmp_path/"member.pdf").exists()
    assert rendered[0].group_benchmark.group_label=="NAVEL TEMPRANA"
    assert rendered[0].group_benchmark.price_per_kg.own_value==Decimal("2.5")
    assert any('"benchmark_created_from_scratch": true' in args[2] for args in repository.audits)


def test_mass_refresh_renders_exact_benchmark_returned_by_audit(tmp_path: Path):
    vm=from_member_liquidation(_header(),_member(member_id=818,variety="NAVELINA"))
    repository=RefreshRepository(dump(vm)); rendered=[]
    audited=module.replace(
        PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA"),
        kilograms_per_hectare=module.BenchmarkMetric(Decimal("15182.67"),Decimal("52120.55"),Decimal("261"),Decimal("17523"),55,0,"ok"),
    )
    def exporter(updated,path):
        rendered.append(updated); Path(path).write_bytes(b"generated pdf")
    document=SimpleNamespace(document_id=7,batch_id="B1",member_id=818,campaign="2026",company="1",file_path=str(tmp_path/"member.pdf"),remittance_id=3)
    key=(818,"NAVEL TEMPRANA","2026","1","CITRICOS","","")
    result=IndividualPdfRefreshService(repository,RefreshBenchmarks(),exporter=exporter).refresh_documents(
        (document,),calculated_benchmarks={key:audited},benchmark_run_id="audit-1")
    assert not result.failed
    metric=rendered[0].group_benchmark.kilograms_per_hectare
    assert (metric.own_value,metric.maximum_value,metric.valid_member_count)==(Decimal("15182.67"),Decimal("52120.55"),55)
    assert result.items[0].source_fingerprint=="AUDITED:audit-1"


def _audited_key(document, *, group="NAVEL TEMPRANA", variety="NAVELINA", run_id="audit-1",
                 liquidation_id="", snapshot_id=None, crop="CITRICOS"):
    return (document.member_id,str(document.campaign),str(document.company),crop,group,"TEMPRANA",
            variety,liquidation_id,document.document_id,document.batch_id,snapshot_id,run_id,"","")


def test_audited_comparison_is_resolved_per_document_and_group(tmp_path: Path):
    vm=from_member_liquidation(_header(),_member(member_id=818,variety="NAVELINA"))
    repository=RefreshRepository(dump(vm)); rendered=[]
    document=SimpleNamespace(document_id=7,batch_id="B1",member_id=818,campaign="2026",company="1",
                             file_path=str(tmp_path/"member.pdf"),remittance_id=3)
    selected=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA")
    other=module.replace(selected,group_label="NAVEL TARDÍA",subgroup="TARDÍA")
    service=IndividualPdfRefreshService(repository,RefreshBenchmarks(),
        exporter=lambda updated,path:(rendered.append(updated),Path(path).write_bytes(b"pdf")),
        comparison_log_path=tmp_path/"resolution.log")
    result=service.refresh_documents((document,),calculated_benchmarks={
        _audited_key(document):selected,
        _audited_key(document,group="NAVEL TARDÍA",variety="NAVELATE"):other,
    },benchmark_run_id="audit-1")
    assert not result.failed and rendered[0].group_benchmark is selected
    assert "candidate_count=1" in (tmp_path/"resolution.log").read_text()
    assert "resolution_status=UNIQUE" in (tmp_path/"resolution.log").read_text()


def test_mandarina_with_comparison_remains_unique(tmp_path: Path):
    vm=from_member_liquidation(module.replace(_header(),cultivo="MANDARINA"),_member(member_id=818,variety="NAVELINA"))
    repository=RefreshRepository(dump(vm)); rendered=[]
    document=SimpleNamespace(document_id=8,batch_id="B2",member_id=818,campaign="2026",company="1",
                             crop="MANDARINA",file_path=str(tmp_path/"mandarina.pdf"),remittance_id=4)
    selected=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA")
    log=tmp_path/"mandarina.log"
    service=IndividualPdfRefreshService(repository,RefreshBenchmarks(),
        exporter=lambda updated,path:(rendered.append(updated),Path(path).write_bytes(b"pdf")),comparison_log_path=log)
    result=service.refresh_documents((document,),calculated_benchmarks={
        _audited_key(document,crop="MANDARINA"):selected,
    },benchmark_run_id="audit-1")
    assert not result.failed and rendered[0].group_benchmark is selected
    assert "resolution_status=UNIQUE" in log.read_text()


def test_audited_comparison_ignores_previous_generation(tmp_path: Path):
    vm=from_member_liquidation(_header(),_member(member_id=818,variety="NAVELINA"))
    repository=RefreshRepository(dump(vm)); rendered=[]
    document=SimpleNamespace(document_id=7,batch_id="B1",member_id=818,campaign="2026",company="1",
                             file_path=str(tmp_path/"member.pdf"),remittance_id=3)
    current=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA")
    old=module.replace(current,kilograms_per_hectare=module.replace(current.kilograms_per_hectare,own_value=Decimal("1")))
    service=IndividualPdfRefreshService(repository,RefreshBenchmarks(),
        exporter=lambda updated,path:(rendered.append(updated),Path(path).write_bytes(b"pdf")),
        comparison_log_path=tmp_path/"resolution.log")
    result=service.refresh_documents((document,),calculated_benchmarks={
        _audited_key(document,run_id="old-run"):old,
        _audited_key(document,run_id="current-run"):current,
    },benchmark_run_id="current-run")
    assert not result.failed and rendered[0].group_benchmark is current


def test_ambiguous_or_missing_audit_does_not_cancel_pdf(tmp_path: Path):
    vm=from_member_liquidation(_header(),_member(member_id=818,variety="NAVELINA"))
    persisted=vm.group_benchmark
    benchmark=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA")
    for label,candidates,expected_status in (
        ("ambiguous",{(818,"NAVEL TEMPRANA","2026","1","CITRICOS","A",""):benchmark,
                      (818,"NAVEL TEMPRANA","2026","1","CITRICOS","B",""):benchmark},"AMBIGUOUS"),
        ("missing",{},"NOT_FOUND"),
    ):
        repository=RefreshRepository(dump(vm)); rendered=[]
        document=SimpleNamespace(document_id=7,batch_id="B1",member_id=818,campaign="2026",company="1",
                                 file_path=str(tmp_path/f"{label}.pdf"),remittance_id=3)
        log=tmp_path/f"{label}.log"
        service=IndividualPdfRefreshService(repository,RefreshBenchmarks(),
            exporter=lambda updated,path:(rendered.append(updated),Path(path).write_bytes(b"pdf")),comparison_log_path=log)
        result=service.refresh_documents((document,),calculated_benchmarks=candidates,benchmark_run_id="audit-1")
        assert not result.failed and rendered[0].group_benchmark==persisted
        text=log.read_text(); assert f"resolution_status={expected_status}" in text
        if expected_status=="AMBIGUOUS": assert text.count("[PdfComparisonCandidate]")==2


def test_directo_and_industria_resolve_their_group_comparison(tmp_path: Path):
    benchmark=PersistedVarietyBenchmarkService.for_member(complete_benchmark(),818,group_name="NAVEL TEMPRANA")
    for crop in ("DIRECTO","INDUSTRIA"):
        vm=from_member_liquidation(module.replace(_header(),cultivo=crop),_member(member_id=818,variety="NAVELINA"),group_benchmark=benchmark)
        repository=RefreshRepository(dump(vm)); rendered=[]
        document=SimpleNamespace(document_id=7,batch_id="B1",member_id=818,campaign="2026",company="1",
                                 crop=crop,file_path=str(tmp_path/f"{crop}.pdf"),remittance_id=3)
        log=tmp_path/f"{crop}.log"
        service=IndividualPdfRefreshService(repository,RefreshBenchmarks(),
            exporter=lambda updated,path:(rendered.append(updated),Path(path).write_bytes(b"pdf")),comparison_log_path=log)
        result=service.refresh_documents((document,),calculated_benchmarks={_audited_key(document):benchmark},benchmark_run_id="audit-1")
        assert not result.failed and rendered[0].group_benchmark is benchmark
        text=log.read_text()
        assert "candidate_count=1" in text and "resolution_status=UNIQUE" in text


def test_new_snapshot_persists_explicit_surface_with_schema_four():
    vm=from_member_liquidation(_header(),_member(member_id=818,variety="NAVELINA",applicable_hectares=Decimal("2.64")))
    payload=dump(vm)
    assert '"schema_version":4' in payload
    restored=load(payload)
    assert restored.applicable_hectares==Decimal("2.64")
    assert restored.surface_source=="HECTARE_FEE" and restored.surface_fingerprint
