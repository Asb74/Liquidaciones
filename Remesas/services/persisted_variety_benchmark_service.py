from __future__ import annotations
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json, logging, re, unicodedata
from domain.benchmark_models import BenchmarkScope, PersistedBenchmarkMetric, PersistedMemberBenchmark, VarietyGroupBenchmark
from presentation.liquidation_document_snapshot import load as load_snapshot
from services.group_benchmark_service import BenchmarkMetric, PremiumGroupBenchmark

logger=logging.getLogger(__name__)

def variety_group_code(group, subgroup=""):
    text=unicodedata.normalize("NFKD","_".join(filter(None,(group,subgroup))).upper()).encode("ascii","ignore").decode()
    return re.sub(r"[^A-Z0-9]+","_",text).strip("_")

class PersistedVarietyBenchmarkService:
    """Aggregates immutable local liquidation values, never consulting Access."""
    def __init__(self, repository): self.repository=repository; self._cache={}
    def clear_cache(self): self._cache.clear()
    def get_group_benchmarks(self, scopes):
        return {s:self.get_group_benchmark(campaign=s.campaign,company=s.company,variety_group_code=s.variety_group_code) for s in dict.fromkeys(scopes)}
    def get_group_benchmark(self, *, campaign, company, variety_group_code):
        scope=BenchmarkScope(str(campaign),str(company),str(variety_group_code))
        if scope in self._cache: return self._cache[scope]
        members={}; sources=[]
        for row in self.repository.list_persisted_benchmark_rows(scope.campaign,scope.company):
            try:
                frozen=load_snapshot(row["payload_json"]).group_benchmark if row["payload_json"] else None
                persisted_code = row["variety_group_code"] if "variety_group_code" in row.keys() else None
                # Compatibility is deliberately limited to pre-migration row
                # shapes. Real migrated rows are always classified by their
                # persisted liquidation variety, never by remittance metadata.
                effective_code = str(persisted_code or (globals()["variety_group_code"](frozen.group,frozen.subgroup) if frozen else ""))
                if effective_code != scope.variety_group_code: continue
                kg=Decimal(str(row["neto"])); amount=Decimal(str(row["importe_total"]))
            except (ValueError,TypeError,KeyError,InvalidOperation): continue
            if not kg.is_finite() or not amount.is_finite() or kg<0: continue
            mid=int(row["recipient_member_id"]); x=members.setdefault(mid,{"name":str(row["socio"]),"kg":Decimal(0),"commercial":Decimal(0),"amount":Decimal(0),"batch_kg":{},"batch_own":{}})
            x["kg"]+=kg; x["amount"]+=amount; x["batch_kg"][row["batch_id"]]=x["batch_kg"].get(row["batch_id"],Decimal(0))+kg
            own=frozen.kilograms_per_hectare.own_value if frozen else None; x["batch_own"][row["batch_id"]]=own
            price=Decimal(str(row["precio_medio"])) if row["precio_medio"] not in (None,"") else None
            x["commercial"] += amount/price if price and price>0 else kg
            sources.append((int(row["id"]),row["status"],row["batch_status"],str(kg),str(amount),scope.variety_group_code))
        result=[]
        for mid,x in sorted(members.items()):
            candidates=[batch_kg/x["batch_own"][batch] for batch,batch_kg in x["batch_kg"].items() if x["batch_own"].get(batch) and x["batch_own"][batch]>0]
            ha=max(candidates) if candidates else None; kg=x["kg"]; commercial=x["commercial"]; amount=x["amount"]
            result.append(PersistedMemberBenchmark(mid,x["name"],commercial,amount,ha,amount/commercial if commercial>0 else None,kg/ha if ha and ha>0 else None,amount/ha if ha and ha>0 else None))
        fingerprint=sha256(json.dumps([scope.__dict__,sources],sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
        benchmark=VarietyGroupBenchmark(scope,tuple(result),self._metric(result,"final_average_price"),self._metric(result,"production_kg_ha"),self._metric(result,"final_amount_eur_ha"),datetime.now(timezone.utc),fingerprint)
        self._cache[scope]=benchmark
        logger.info("[PersistedBenchmark] campaign=%s company=%s group=%s members=%s liquidations=%s fingerprint=%s",scope.campaign,scope.company,scope.variety_group_code,len(result),len(sources),fingerprint)
        return benchmark
    @staticmethod
    def _metric(members, field):
        values=[getattr(m,field) for m in members if getattr(m,field) is not None and getattr(m,field)>0]
        avg=sum(values,Decimal(0))/len(values) if values else None
        return PersistedBenchmarkMetric(None,max(values) if values else None,avg,min(values) if values else None,None,len(values))
    @staticmethod
    def for_member(
        benchmark,
        member_id,
        template=None,
        *,
        group_name=None,
        campaign=None,
        subgroup=None,
    ) -> PremiumGroupBenchmark:
        """Build the document benchmark, optionally preserving old metadata.

        Metrics always come from the current persisted benchmark.  A benchmark
        stored in the economic snapshot is metadata only and is deliberately
        not required.
        """
        member=next((m for m in benchmark.comparable_members if m.recipient_member_id==int(member_id)),None)
        def convert(summary, own, name):
            return BenchmarkMetric(own,summary.maximum,summary.minimum,summary.average,summary.comparable_count,len(benchmark.comparable_members)-summary.comparable_count,"ok" if summary.comparable_count else "unavailable","" if summary.comparable_count else "Sin datos comparables suficientes",name)
        metrics = {
            "price_per_kg": convert(benchmark.price_metric,member.final_average_price if member else None,"FINAL_PRICE"),
            "kilograms_per_hectare": convert(benchmark.production_metric,member.production_kg_ha if member else None,"PRODUCTION_KG_HA"),
            "euros_per_hectare": convert(benchmark.final_amount_metric,member.final_amount_eur_ha if member else None,"FINAL_AMOUNT_EUR_HA"),
        }
        if template is not None:
            return replace(
                template,
                group_label=group_name or template.group_label,
                campaign=str(campaign or template.campaign),
                subgroup=subgroup if subgroup is not None else template.subgroup,
                **metrics,
            )

        logger.info("[BenchmarkTemplate] snapshot_template_missing=True action=CREATE_FROM_SCRATCH")
        label=str(group_name or benchmark.scope.variety_group_code).strip()
        if not label:
            raise ValueError("El benchmark no contiene un nombre de grupo varietal válido.")
        resolved_campaign=str(campaign or benchmark.scope.campaign).strip()
        if not resolved_campaign:
            raise ValueError("El benchmark no contiene una campaña válida.")
        label_parts=label.split(maxsplit=1)
        resolved_group=label_parts[0]
        resolved_subgroup=subgroup if subgroup is not None else (label_parts[1] if len(label_parts)>1 else "")
        return PremiumGroupBenchmark(
            group_label=label,
            crop="",
            group=resolved_group,
            subgroup=resolved_subgroup,
            varieties=(),
            campaign=resolved_campaign,
            company=str(benchmark.scope.company),
            liquidation_type="",
            category="",
            warnings=(),
            **metrics,
        )
