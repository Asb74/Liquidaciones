from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from data.group_benchmark_repository import GroupBenchmarkRepository, VarietalGroup
from domain.calculation_models import CalculationStatus, LiquidationHeader, MemberLiquidation

@dataclass(frozen=True)
class BenchmarkMetric:
    own_value: Decimal | None; maximum_value: Decimal | None; minimum_value: Decimal | None; average_value: Decimal | None
    valid_member_count: int; excluded_member_count: int; status: str; warning: str = ""

@dataclass(frozen=True)
class PremiumGroupBenchmark:
    group_label: str; crop: str; group: str; subgroup: str; varieties: tuple[str, ...]; campaign: str; company: str; liquidation_type: str; category: str
    price_per_kg: BenchmarkMetric; kilograms_per_hectare: BenchmarkMetric; euros_per_hectare: BenchmarkMetric; warnings: tuple[str, ...] = ()

def _d(v) -> Decimal: return v if isinstance(v, Decimal) else Decimal(str(v or "0"))
def _q(v): return None if v is None else v.quantize(Decimal("0.00001"), ROUND_HALF_UP)

class GroupBenchmarkService:
    """Benchmarks current-remittance MemberLiquidation rows; no external final amounts are invented."""
    def __init__(self, repository: GroupBenchmarkRepository, log_path: str | Path = "logs/group_benchmark.log") -> None:
        self.repository=repository; self.log_path=Path(log_path)
    def resolve_varietal_group(self, crop: str, variety: str) -> VarietalGroup | None:
        return self.repository.get_varietal_group(crop, variety)
    def build_benchmarks(self, header: LiquidationHeader, members: tuple[MemberLiquidation, ...]) -> dict[tuple, PremiumGroupBenchmark]:
        grouped={}; missing=[]
        for m in members:
            g=self.resolve_varietal_group(header.cultivo, m.variety or "")
            if not g: missing.append(f"Grupo varietal no encontrado para {header.cultivo}/{m.variety}"); continue
            grouped.setdefault(g.label, (g, []))[1].append(m)
        out={}
        for label,(g, lines) in grouped.items():
            per={}
            for m in lines:
                x=per.setdefault(m.member_id,{"member":m,"kg":Decimal("0"),"amount":Decimal("0")})
                x["kg"]+=_d(m.net_kg); x["amount"]+=_d(m.total_amount)
            surfaces={mid:self.repository.get_productive_hectares(mid, header.campana, header.empresa, header.cultivo, g.varieties) for mid in per}
            for mid,x in per.items():
                ha=surfaces[mid].hectares; x["ha"]=ha; x["kg_ha"]=x["kg"]/ha if ha>0 else None; x["eur_ha"]=x["amount"]/ha if ha>0 else None
            price_valid=[m for m in lines if m.total_amount is not None and _d(m.net_kg)>0 and m.final_average_price is not None and not any(getattr(s,"value",s) in {CalculationStatus.ERROR.value,CalculationStatus.PENDING.value} for s in m.statuses.values())]
            kg_sum=sum((x["kg"] for x in per.values()), Decimal("0")); amt_sum=sum((x["amount"] for x in per.values()), Decimal("0"))
            kg_valid=[x for x in per.values() if x["kg_ha"] is not None]; eur_valid=[x for x in per.values() if x["eur_ha"] is not None]
            warnings=tuple(w for s in surfaces.values() for w in s.warnings)+tuple(missing)
            for mid,x in per.items():
                b=PremiumGroupBenchmark(label,g.crop,g.group,g.subgroup,g.varieties,str(header.campana),header.empresa,header.tipo_liquidacion,header.categoria,
                    self._metric(x["member"].final_average_price,[m.final_average_price for m in price_valid], amt_sum/kg_sum if kg_sum>0 else None, len(price_valid), len(lines)-len(price_valid)),
                    self._metric(x["kg_ha"],[v["kg_ha"] for v in kg_valid], sum((v["kg"] for v in kg_valid),Decimal("0"))/sum((v["ha"] for v in kg_valid),Decimal("0")) if kg_valid else None, len(kg_valid), len(per)-len(kg_valid), "No se ha podido determinar una superficie productiva válida para este grupo varietal." if x["kg_ha"] is None else ""),
                    self._metric(x["eur_ha"],[v["eur_ha"] for v in eur_valid], sum((v["amount"] for v in eur_valid),Decimal("0"))/sum((v["ha"] for v in eur_valid),Decimal("0")) if eur_valid else None, len(eur_valid), len(per)-len(eur_valid), "No se ha podido determinar una superficie productiva válida para este grupo varietal." if x["eur_ha"] is None else ""), warnings)
                out[(mid,label,str(header.campana),header.empresa,header.cultivo,header.tipo_liquidacion,header.categoria)]=b; self._log(b)
        return out
    def _metric(self, own, vals, avg, valid, excluded, warning=""):
        vals=[v for v in vals if v is not None]
        return BenchmarkMetric(_q(own), _q(max(vals)) if vals else None, _q(min(vals)) if vals else None, _q(avg), valid, excluded, "ok" if vals else "unavailable", warning)
    def _log(self,b):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a",encoding="utf-8") as f:
            f.write(f"[Benchmark]\ncampaign={b.campaign}\ncompany={b.company}\ncrop={b.crop}\ngroup={b.group}\nsubgroup={b.subgroup}\nlabel={b.group_label}\nvarieties={','.join(b.varieties)}\nvalid_members={b.price_per_kg.valid_member_count}\nexcluded_members={b.price_per_kg.excluded_member_count}\nprice_own={b.price_per_kg.own_value}\nprice_max={b.price_per_kg.maximum_value}\nprice_avg={b.price_per_kg.average_value}\nprice_min={b.price_per_kg.minimum_value}\nkg_ha_own={b.kilograms_per_hectare.own_value}\nkg_ha_max={b.kilograms_per_hectare.maximum_value}\nkg_ha_avg={b.kilograms_per_hectare.average_value}\nkg_ha_min={b.kilograms_per_hectare.minimum_value}\neur_ha_own={b.euros_per_hectare.own_value}\neur_ha_max={b.euros_per_hectare.maximum_value}\neur_ha_avg={b.euros_per_hectare.average_value}\neur_ha_min={b.euros_per_hectare.minimum_value}\n\n")
