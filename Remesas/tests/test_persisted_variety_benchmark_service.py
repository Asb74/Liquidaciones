from decimal import Decimal
from types import SimpleNamespace
import services.persisted_variety_benchmark_service as module
from services.persisted_variety_benchmark_service import PersistedVarietyBenchmarkService

class Repository:
    def __init__(self, rows): self.rows=rows; self.calls=0
    def list_persisted_benchmark_rows(self,campaign,company): self.calls+=1; return self.rows

def row(identifier,member,batch,kg,amount,price="2"):
    return {"id":identifier,"recipient_member_id":member,"socio":f"S{member}","neto":kg,"precio_medio":price,"importe_total":amount,"batch_id":batch,"status":"ACTIVE","batch_status":"ACTIVE","payload_json":"snapshot"}

def frozen(production):
    metric=SimpleNamespace(own_value=Decimal(production))
    return SimpleNamespace(group_benchmark=SimpleNamespace(group="NAVEL",subgroup="TARDÍA",kilograms_per_hectare=metric))

def test_global_benchmark_aggregates_remittances_weighted_and_caches(monkeypatch):
    values=iter((frozen("100"),frozen("300"),frozen("50")))
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
    assert benchmark.price_metric.comparable_count==2

def test_fingerprint_changes_when_persisted_population_changes(monkeypatch):
    monkeypatch.setattr(module,"load_snapshot",lambda _:frozen("100"))
    repo=Repository([row(1,1,"A","100","200")]); service=PersistedVarietyBenchmarkService(repo)
    first=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA").source_fingerprint
    repo.rows.append(row(2,2,"B","100","300")); service.clear_cache()
    second=service.get_group_benchmark(campaign="2026",company="1",variety_group_code="NAVEL_TARDIA").source_fingerprint
    assert first!=second
