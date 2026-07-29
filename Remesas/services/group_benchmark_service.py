from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from datetime import datetime
import logging
from data.group_benchmark_repository import GroupBenchmarkRepository, VarietalGroup
from domain.calculation_models import LiquidationHeader, MemberLiquidation
from group_benchmark_surface_audit import AUDIT_LOG_PATH, append_surface_audit, record_surface_audit_config

LOGGER = logging.getLogger(__name__)

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
    if metric.minimum_value is not None and metric.minimum_value <= 0: warnings.append("minimum debe ser > 0")
    return tuple(dict.fromkeys(warnings))

@dataclass(frozen=True)
class PremiumGroupBenchmark:
    group_label: str; crop: str; group: str; subgroup: str; varieties: tuple[str, ...]; campaign: str; company: str; liquidation_type: str; category: str
    price_per_kg: BenchmarkMetric; kilograms_per_hectare: BenchmarkMetric; euros_per_hectare: BenchmarkMetric; warnings: tuple[str, ...] = ()

def _d(v) -> Decimal: return v if isinstance(v, Decimal) else Decimal(str(v or "0"))
def _q(v): return None if v is None else v.quantize(Decimal("0.00001"), ROUND_HALF_UP)

def _positive_decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return value if value.is_finite() and value > 0 else None

class GroupBenchmarkService:
    """Benchmarks current-remittance MemberLiquidation rows; no external final amounts are invented."""
    def __init__(self, repository: GroupBenchmarkRepository, log_path: str | Path = "logs/group_benchmark.log", audit_log_path: str | Path | None = None) -> None:
        self.repository=repository; self.log_path=Path(log_path)
        self.audit_log_path = Path(audit_log_path) if audit_log_path is not None else Path(getattr(repository, "audit_log_path", AUDIT_LOG_PATH))
        self.audit_log_path = record_surface_audit_config(self.audit_log_path)
    def resolve_varietal_group(self, crop: str, variety: str) -> VarietalGroup | None:
        return self.repository.get_varietal_group(crop, variety)
    def build_benchmarks(self, header: LiquidationHeader, members: tuple[MemberLiquidation, ...], *,
                         parent_run_id: str | None = None,
                         run_source: str = "REMESA_CALCULATION") -> dict[tuple, PremiumGroupBenchmark]:
        started = datetime.now()
        run_id = started.strftime("%Y%m%d_%H%M%S_%f")
        if hasattr(self.repository, "set_audit_run_id"):
            self.repository.set_audit_run_id(run_id)
        if hasattr(self.repository, "set_audit_context"):
            self.repository.set_audit_context(parent_run_id, run_source)
        self.last_surface_details = {}
        self._surface_audit("GroupBenchmarkAuditRun", run_id=run_id, parent_run_id=parent_run_id,
                            run_source=run_source, started_at=started.isoformat(),
                            campaign=header.campana, company=header.empresa, crop=header.cultivo,
                            member_line_count=len(members))
        LOGGER.info("[GroupBenchmarkSurfaceAudit]\nrun_id=%s\npath=%s\nmembers=%s\nstatus=ENABLED",
                    run_id, self.audit_log_path, len(members))
        grouped={}; missing=[]
        for m in members:
            g=self.resolve_varietal_group(header.cultivo, m.variety or "")
            if not g: missing.append(f"Grupo varietal no encontrado para {header.cultivo}/{m.variety}"); continue
            grouped.setdefault(g.label, (g, []))[1].append(m)
        out={}
        for label,(g, lines) in grouped.items():
            per={}
            for m in lines:
                x=per.setdefault(m.member_id,{"member":m,"kg":Decimal("0"),"commercial_kg":Decimal("0"),"amount":Decimal("0"),"statuses":[]})
                x["kg"]+=_d(m.net_kg); x["commercial_kg"]+=_d(m.commercial_kg); x["amount"]+=_d(m.total_amount); x["statuses"].extend(m.statuses.values())
            for mid,x in per.items():
                member_lines = [m for m in lines if m.member_id == mid]
                self._surface_audit("GroupBenchmarkMemberAggregation", run_id=run_id,
                                    parent_run_id=parent_run_id, run_source=run_source, member_id=mid,
                                    group_label=g.label, member_variety=tuple(m.variety for m in member_lines),
                                    variety_lines=len(member_lines), total_net_kg=x["kg"],
                                    total_commercial_kg=x["commercial_kg"], total_amount=x["amount"],
                                    statuses=tuple(x["statuses"]))
            surfaces={mid:self.repository.get_productive_hectares(mid, header.campana, header.empresa, header.cultivo, g.varieties) for mid in per}
            for mid,x in per.items():
                ha=surfaces[mid].hectares; x["ha"]=ha
                x["kg_ha"]=x["kg"]/ha if _positive_decimal(x["kg"]) and _positive_decimal(ha) else None
                x["eur_ha"]=x["amount"]/ha if _positive_decimal(x["amount"]) and _positive_decimal(ha) else None
                x["price"]=x["amount"]/x["commercial_kg"] if _positive_decimal(x["amount"]) and _positive_decimal(x["commercial_kg"]) else None
                surface=surfaces[mid]
                self.last_surface_details[(mid, g.label)] = {
                    "hectares": ha, "parcel_count": surface.parcel_count,
                    "excluded_count": surface.excluded_count, "warnings": surface.warnings,
                    "candidate_boletas": surface.candidate_boletas,
                    "matched_boletas": surface.matched_boletas,
                    "included_boletas": surface.included_boletas,
                }
                reason = None
                if x["kg_ha"] is None:
                    valid_kg, valid_ha = _positive_decimal(x["kg"]), _positive_decimal(ha)
                    reason = "BOTH" if not valid_kg and not valid_ha else ("INVALID_KG" if not valid_kg else "INVALID_HECTARES")
                self._surface_audit("GroupBenchmarkMemberProduction", run_id=run_id,
                                    parent_run_id=parent_run_id, run_source=run_source, member_id=mid,
                                    group_label=g.label, group_varieties=g.varieties, net_kg=x["kg"],
                                    surface_hectares=ha, production_kg_ha=x["kg_ha"], reason=reason,
                                    parcel_count=surface.parcel_count, excluded_count=surface.excluded_count,
                                    warnings=surface.warnings)
            warnings=tuple(w for srf in surfaces.values() for w in srf.warnings)+tuple(missing)
            price=self._metric("COMPARATIVA_PRECIO", "FINAL_PRICE", [dict(v, value=v["price"], price=v["price"], kilos=v["commercial_kg"], importe=v["amount"]) for v in per.values()])
            prod=self._metric("COMPARATIVA_PRODUCCION", "PRODUCTION_KG_HA", [dict(v, value=v["kg_ha"], produccion=v["kg"], hectareas=v["ha"]) for v in per.values()])
            amount=self._metric("COMPARATIVA_IMPORTE", "FINAL_AMOUNT", [dict(v, value=v["amount"], importe=v["amount"]) for v in per.values()])
            eurha=self._metric("COMPARATIVA_EUROS_HECTAREA", "FINAL_AMOUNT_EUR_HA", [dict(v, value=v["eur_ha"], importe=v["amount"], hectareas=v["ha"]) for v in per.values()])
            for mid,x in per.items():
                p=price[0](x["price"]); k=prod[0](x["kg_ha"], "No se ha podido determinar una superficie productiva válida para este grupo varietal." if x["kg_ha"] is None else ""); e=eurha[0](x["eur_ha"], "No se ha podido determinar una superficie productiva válida para este grupo varietal." if x["eur_ha"] is None else "")
                b=PremiumGroupBenchmark(label,g.crop,g.group,g.subgroup,g.varieties,str(header.campana),header.empresa,header.tipo_liquidacion,header.categoria,p,k,e,warnings+validate_benchmark_metric(p)+validate_benchmark_metric(k)+validate_benchmark_metric(e))
                out[(mid,label,str(header.campana),header.empresa,header.cultivo,header.tipo_liquidacion,header.categoria)]=b; self._log(b, amount[1])
        self._surface_audit("GroupBenchmarkAuditRunCompleted", run_id=run_id, parent_run_id=parent_run_id,
                            run_source=run_source, group_count=len(grouped),
                            benchmark_count=len(out), finished_at=datetime.now().isoformat())
        return out
    def _surface_audit(self, section, **values):
        append_surface_audit(section, values, self.audit_log_path)
    def _metric(self, log_name, name, candidates):
        excluded={"null":0,"zero":0,"negative":0}; valid=[]
        for c in candidates:
            v=c.get("value")
            try: v = v if isinstance(v, Decimal) else Decimal(str(v))
            except (InvalidOperation, ValueError, TypeError): excluded["null"]+=1; continue
            if not v.is_finite(): excluded["null"]+=1; continue
            if v < 0: excluded["negative"]+=1; continue
            if v == 0: excluded["zero"]+=1; continue
            valid.append(v)
        avg=(sum(valid,Decimal("0"))/len(valid)) if valid else None
        def build(own, warning=""):
            m=BenchmarkMetric(_q(own), _q(max(valid)) if valid else None, _q(min(valid)) if valid else None, _q(avg), len(valid), len(candidates)-len(valid), "ok" if valid else "unavailable", warning or ("Sin datos comparables suficientes" if not valid else ""), name, excluded["null"], excluded["zero"], excluded["negative"])
            ws=validate_benchmark_metric(m)
            return m if not ws else BenchmarkMetric(m.own_value,None,None,None,0,m.excluded_member_count,"unavailable", warning or "Sin datos comparables suficientes", name, excluded["null"], excluded["zero"], excluded["negative"])
        self._log_metric_summary(log_name, len(candidates), len(valid), excluded, min(valid) if valid else None, avg, max(valid) if valid else None)
        return build, excluded
    def _log_metric_summary(self, name, total, valid, excluded, minimum, average, maximum):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a",encoding="utf-8") as f:
            f.write(f"{name}: registros_totales={total}\n{name}: registros_validos={valid}\n{name}: excluidos_nulos={excluded['null']}\n{name}: excluidos_cero={excluded['zero']}\n{name}: excluidos_negativos={excluded['negative']}\n{name}: minimo={minimum} media={average} maximo={maximum}\n")
    def _log(self,b, amount_excluded=None):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a",encoding="utf-8") as f:
            f.write(f"[GroupBenchmarkContext]\ncampaign={b.campaign}\ncompany={b.company}\ncrop={b.crop}\nvarietal_group={b.group_label}\n\n")
            for metric in (b.price_per_kg,b.kilograms_per_hectare,b.euros_per_hectare):
                f.write(f"[GroupBenchmarkMetric]\nmetric={metric.metric}\ncandidate_count={metric.valid_member_count+metric.excluded_member_count}\nvalid_count={metric.valid_member_count}\nexcluded_null={metric.excluded_null}\nexcluded_zero={metric.excluded_zero}\nexcluded_negative={metric.excluded_negative}\nminimum={metric.minimum_value}\naverage={metric.average_value}\nmaximum={metric.maximum_value}\nown_value={metric.own_value}\nvalid={metric.status=='ok'}\nwarning={metric.warning}\n\n")
