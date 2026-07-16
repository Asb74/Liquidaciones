from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from data.group_benchmark_repository import GroupBenchmarkRepository, VarietalGroup
from domain.calculation_models import CalculationStatus, LiquidationHeader, MemberLiquidation

@dataclass(frozen=True)
class BenchmarkMetric:
    own_value: Decimal | None; maximum_value: Decimal | None; minimum_value: Decimal | None; average_value: Decimal | None
    valid_member_count: int; excluded_member_count: int; status: str; warning: str = ""; metric: str = ""; excluded_null: int = 0; excluded_zero: int = 0; excluded_negative: int = 0

def validate_benchmark_metric(metric: BenchmarkMetric) -> tuple[str, ...]:
    warnings=[]
    vals=(metric.minimum_value, metric.average_value, metric.maximum_value)
    if metric.valid_member_count < 1: warnings.append("comparable_count < 1")
    if any(v is None for v in vals): warnings.append("valores estadísticos incompletos")
    for v in vals + (metric.own_value,):
        if v is not None and (not v.is_finite()): warnings.append("valor no finito")
    if all(v is not None for v in vals) and not (metric.minimum_value <= metric.average_value <= metric.maximum_value): warnings.append("minimum <= average <= maximum incumplido")
    if metric.metric in {"PRODUCTION_KG_HA", "FINAL_AMOUNT_EUR_HA"} and metric.minimum_value is not None and metric.minimum_value <= 0: warnings.append("minimum debe ser > 0")
    return tuple(dict.fromkeys(warnings))

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
                x=per.setdefault(m.member_id,{"member":m,"kg":Decimal("0"),"amount":Decimal("0"),"statuses":[]})
                x["kg"]+=_d(m.net_kg); x["amount"]+=_d(m.total_amount); x["statuses"].extend(m.statuses.values())
            surfaces={mid:self.repository.get_productive_hectares(mid, header.campana, header.empresa, header.cultivo, g.varieties) for mid in per}
            for mid,x in per.items():
                ha=surfaces[mid].hectares; x["ha"]=ha; x["kg_ha"]=x["kg"]/ha if ha>0 else None; x["eur_ha"]=x["amount"]/ha if ha>0 else None; x["price"]=x["amount"]/x["kg"] if x["kg"]>0 else None
            warnings=tuple(w for srf in surfaces.values() for w in srf.warnings)+tuple(missing)
            price=self._metric("FINAL_PRICE", [dict(v, value=v["price"], weight=v["kg"], amount=v["amount"], member_id=mid) for mid,v in per.items()], allow_zero=True, weighted_price=True)
            prod=self._metric("PRODUCTION_KG_HA", [dict(v, value=v["kg_ha"], member_id=mid) for mid,v in per.items()], allow_zero=False)
            amount=self._metric("FINAL_AMOUNT_EUR_HA", [dict(v, value=v["eur_ha"], member_id=mid) for mid,v in per.items()], allow_zero=False)
            for mid,x in per.items():
                p=price[0](x["price"]); k=prod[0](x["kg_ha"], "No se ha podido determinar una superficie productiva válida para este grupo varietal." if x["kg_ha"] is None else ""); e=amount[0](x["eur_ha"], "No se ha podido determinar una superficie productiva válida para este grupo varietal." if x["eur_ha"] is None else "")
                b=PremiumGroupBenchmark(label,g.crop,g.group,g.subgroup,g.varieties,str(header.campana),header.empresa,header.tipo_liquidacion,header.categoria,p,k,e,warnings+validate_benchmark_metric(p)+validate_benchmark_metric(k)+validate_benchmark_metric(e))
                out[(mid,label,str(header.campana),header.empresa,header.cultivo,header.tipo_liquidacion,header.categoria)]=b; self._log(b, member_id=mid)
        return out
    def _metric(self, name, candidates, *, allow_zero: bool, weighted_price: bool=False):
        excluded={"null":0,"zero":0,"negative":0}; valid=[]
        for c in candidates:
            v=c.get("value")
            if v is None: excluded["null"]+=1; self._log_excluded(name,c.get("member_id"),"NULL_VALUE"); continue
            try: v = v if isinstance(v, Decimal) else Decimal(str(v))
            except (InvalidOperation, ValueError): excluded["null"]+=1; self._log_excluded(name,c.get("member_id"),"NULL_VALUE"); continue
            if not v.is_finite(): excluded["null"]+=1; self._log_excluded(name,c.get("member_id"),"NULL_VALUE"); continue
            if v < 0: excluded["negative"]+=1; self._log_excluded(name,c.get("member_id"),"NEGATIVE_VALUE"); continue
            if v == 0 and not allow_zero: excluded["zero"]+=1; self._log_excluded(name,c.get("member_id"),"ZERO_VALUE"); continue
            valid.append(v)
        avg=(sum((c.get("amount", Decimal("0")) for c in candidates if c.get("value") in valid), Decimal("0")) / sum((c.get("weight", Decimal("0")) for c in candidates if c.get("value") in valid), Decimal("0")) if weighted_price and valid and sum((c.get("weight", Decimal("0")) for c in candidates if c.get("value") in valid), Decimal("0")) > 0 else ((sum(valid,Decimal("0"))/len(valid)) if valid else None))
        def build(own, warning=""):
            m=BenchmarkMetric(_q(own), _q(max(valid)) if valid else None, _q(min(valid)) if valid else None, _q(avg), len(valid), len(candidates)-len(valid), "ok" if valid else "unavailable", warning, name, excluded["null"], excluded["zero"], excluded["negative"])
            ws=validate_benchmark_metric(m)
            return m if not ws else BenchmarkMetric(m.own_value,None,None,None,0,m.excluded_member_count,"unavailable", warning or "; ".join(ws), name, excluded["null"], excluded["zero"], excluded["negative"])
        return build, excluded
    def _log_excluded(self, metric, member_id, reason):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a",encoding="utf-8") as f: f.write(f"[GroupBenchmarkExcluded]\nmetric={metric}\nmember_id={member_id}\nreason={reason}\n\n")
    def _log(self,b, member_id=None):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a",encoding="utf-8") as f:
            f.write(f"[GroupBenchmarkContext]\ncampaign={b.campaign}\ncompany={b.company}\ncrop={b.crop}\nvarietal_group={b.group_label}\nmember_id={member_id}\n\n")
            for metric in (b.price_per_kg,b.kilograms_per_hectare,b.euros_per_hectare):
                f.write(f"[GroupBenchmarkMetric]\nmetric={metric.metric}\ncandidate_count={metric.valid_member_count+metric.excluded_member_count}\nvalid_count={metric.valid_member_count}\nexcluded_null={metric.excluded_null}\nexcluded_zero={metric.excluded_zero}\nexcluded_negative={metric.excluded_negative}\nminimum={metric.minimum_value}\naverage={metric.average_value}\nmaximum={metric.maximum_value}\nown_value={metric.own_value}\nvalid={metric.status=='ok'}\nwarning={metric.warning}\n\n")
