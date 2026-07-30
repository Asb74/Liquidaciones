from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import json

import services.massive_benchmark_audit_service as module
from services.massive_benchmark_audit_service import MassiveBenchmarkAuditService


class Repository:
    def get_document_snapshot(self, batch_id, member_id):
        return {"payload_json": json.dumps({"schema_version":4,"model":{
            "effective_net_kg":"1","commercial_kg":"1","total_amount":"1","variety_name":"NAVELINA"
        }})}


class Benchmarks:
    def __init__(self): self.calls=[]; self.last_surface_details={}
    def build_benchmarks(self, header, members, **kwargs):
        self.calls.append((header, members, kwargs))
        ids={m.member_id for m in members}
        self.last_surface_details={(mid,"NAVEL TEMPRANA"):{"hectares":Decimal("7.2337"),"parcel_count":7,"excluded_count":0,"warnings":(),"candidate_boletas":("659","660","698","699","701","702","703"),"matched_boletas":(),"included_boletas":()} for mid in ids}
        return {}


def vm(kilos, campaign="2026", company="1", group="NAVEL TEMPRANA", crop="CITRICOS",
       commercial_kilos=None, amount="100"):
    return SimpleNamespace(member_id=1623,member_name="SOCIO 1623",campaign=campaign,company=company,crop=crop,effective_net_kg=Decimal(kilos),commercial_kg=Decimal(commercial_kilos if commercial_kilos is not None else kilos),total_amount=Decimal(amount),variety_name="NAVELINA",varieties=("NAVELINA",),variety_group_name=group,remittance_name="SEMANA",group_benchmark=SimpleNamespace(group_label=group,liquidation_type="NORMAL",category="A"))


def doc(number):
    return SimpleNamespace(document_id=number,batch_id=f"b{number}",member_id=1623,remittance_id=number,file_path=Path(f"missing-{number}.pdf"))


def test_each_liquidation_keeps_own_value_and_uses_unique_group_surface(monkeypatch,tmp_path):
    values=iter((vm("42255"),vm("34012"),vm("36478")))
    monkeypatch.setattr(module,"load_snapshot",lambda _:next(values))
    benchmark=Benchmarks(); log=tmp_path/"mass.log"
    result=MassiveBenchmarkAuditService(Repository(),benchmark,audit_log_path=log).audit_selection([doc(1),doc(2),doc(3)])
    assert len(result.productions)==3
    assert [row.total_net_kg for row in result.productions]==[Decimal("42255"),Decimal("34012"),Decimal("36478")]
    assert all(row.surface_hectares==Decimal("7.2337") for row in result.productions)
    assert len(benchmark.calls)==1
    assert benchmark.calls[0][2]["parent_run_id"]==result.mass_run_id
    assert benchmark.calls[0][2]["run_source"]=="MASS_PDF_REBUILD"
    text=log.read_text(encoding="utf-8")
    for section in ("MassPdfBenchmarkAuditRun","MassBenchmarkMemberAggregation","MassBenchmarkMemberProduction","MassPdfBenchmarkSummary"):
        assert f"[{section}]" in text
    assert "will_reuse=no" in text


def test_contexts_do_not_mix_and_run_ids_are_unique(monkeypatch,tmp_path):
    snapshots=[vm("10",campaign="2025"),vm("20",campaign="2026")]
    monkeypatch.setattr(module,"load_snapshot",lambda _:snapshots.pop(0))
    service=MassiveBenchmarkAuditService(Repository(),Benchmarks(),audit_log_path=tmp_path/"mass.log")
    first=service.audit_selection([doc(1),doc(2)])
    monkeypatch.setattr(module,"load_snapshot",lambda _:vm("10"))
    second=service.audit_selection([doc(3)])
    assert first.context_count==2
    assert len(first.productions)==2
    assert first.mass_run_id != second.mass_run_id


def test_incomplete_snapshot_is_reported(monkeypatch,tmp_path):
    monkeypatch.setattr(module,"load_snapshot",lambda _:None)
    result=MassiveBenchmarkAuditService(Repository(),Benchmarks(),audit_log_path=tmp_path/"mass.log").audit_selection([doc(1)])
    assert result.has_severe_incidents
    assert result.incidents[0].code=="INCOMPLETE_SNAPSHOT"
    assert "status=INCOMPLETE" in (tmp_path/"mass.log").read_text()


def test_industria_participates_in_varietal_group(monkeypatch,tmp_path):
    monkeypatch.setattr(module,"load_snapshot",lambda _:vm("10",crop="INDUSTRIA"))
    benchmark=Benchmarks()
    log=tmp_path/"mass.log"
    result=MassiveBenchmarkAuditService(Repository(),benchmark,audit_log_path=log).audit_selection([doc(1)])
    assert result.context_count==1 and len(result.benchmarks)==1
    assert len(benchmark.calls)==1
    assert "crop=INDUSTRIA" in log.read_text()


def test_benchmark_member_uses_final_amount_divided_by_effective_net_kg(monkeypatch,tmp_path):
    monkeypatch.setattr(module,"load_snapshot",lambda _:vm("40",commercial_kilos="25",amount="100"))
    benchmark=Benchmarks(); log=tmp_path/"mass.log"

    MassiveBenchmarkAuditService(Repository(),benchmark,audit_log_path=log).audit_selection([doc(1)])

    member=benchmark.calls[0][1][0]
    assert member.final_average_price==Decimal("2.5")
    assert member.final_average_price != member.total_amount/member.commercial_kg
    text=log.read_text(encoding="utf-8")
    assert "[MassBenchmarkPriceInput]" in text
    assert "source=TOTAL_AMOUNT_DIV_EFFECTIVE_NET_KG" in text


def test_consolidated_price_is_weighted_from_totals_and_population_uses_it(monkeypatch,tmp_path):
    snapshots=iter((vm("20",commercial_kilos="10",amount="40"),vm("80",commercial_kilos="20",amount="360")))
    monkeypatch.setattr(module,"load_snapshot",lambda _:next(snapshots))
    population=[]
    original=module.benchmark_for_entry
    def capture(value,*args,**kwargs):
        population.append(value)
        return original(value,*args,**kwargs)
    monkeypatch.setattr(module,"benchmark_for_entry",capture)
    benchmark=Benchmarks()

    MassiveBenchmarkAuditService(Repository(),benchmark,audit_log_path=tmp_path/"mass.log").audit_selection([doc(1),doc(1)])

    member=benchmark.calls[0][1][0]
    assert member.total_amount==Decimal("400")
    assert member.net_kg==Decimal("100")
    assert member.final_average_price==Decimal("4")
    assert member.final_average_price != (Decimal("2")+Decimal("4.5"))/2
    assert member.final_average_price != member.total_amount/member.commercial_kg
    assert population[0].price==member.final_average_price


def test_zero_effective_net_kg_has_no_final_average_price(monkeypatch,tmp_path):
    monkeypatch.setattr(module,"load_snapshot",lambda _:vm("0",commercial_kilos="50",amount="100"))
    benchmark=Benchmarks()

    MassiveBenchmarkAuditService(Repository(),benchmark,audit_log_path=tmp_path/"mass.log").audit_selection([doc(1)])

    assert benchmark.calls[0][1][0].final_average_price is None
