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


def vm(kilos, campaign="2026", company="1", group="NAVEL TEMPRANA"):
    return SimpleNamespace(member_id=1623,member_name="SOCIO 1623",campaign=campaign,company=company,crop="NARANJA",effective_net_kg=Decimal(kilos),commercial_kg=Decimal(kilos),total_amount=Decimal("100"),variety_name="NAVELINA",varieties=("NAVELINA",),variety_group_name=group,remittance_name="SEMANA",group_benchmark=SimpleNamespace(group_label=group,liquidation_type="NORMAL",category="A"))


def doc(number):
    return SimpleNamespace(document_id=number,batch_id=f"b{number}",member_id=1623,remittance_id=number,file_path=Path(f"missing-{number}.pdf"))


def test_global_massive_aggregation_uses_unique_surface(monkeypatch,tmp_path):
    values=iter((vm("42255"),vm("34012"),vm("36478")))
    monkeypatch.setattr(module,"load_snapshot",lambda _:next(values))
    benchmark=Benchmarks(); log=tmp_path/"mass.log"
    result=MassiveBenchmarkAuditService(Repository(),benchmark,audit_log_path=log).audit_selection([doc(1),doc(2),doc(3)])
    assert len(result.productions)==1
    row=result.productions[0]
    assert row.total_net_kg==Decimal("112745")
    assert row.surface_hectares==Decimal("7.2337")
    assert abs(row.production_kg_ha-Decimal("15586.07628")) < Decimal("0.00001")
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
