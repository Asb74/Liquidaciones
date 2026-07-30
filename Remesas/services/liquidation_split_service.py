from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from data.fiscal_regime_repository import FiscalRegimeRepository
from domain.liquidacion_calculator import calculate_fiscal_result
from domain.persistence_models import SplitPreviewLine, SplitRecipient, SplitRule
from domain.member_rules import is_excluded_member, log_system_member_excluded
import logging

MONEY=Decimal("0.01"); KILOS=Decimal("0.001")


def _audit_logger() -> logging.Logger:
    logger = logging.getLogger("liquidation_split_audit")
    if not logger.handlers:
        path = Path(__file__).resolve().parents[2] / "logs" / "liquidation_split_audit.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class LiquidationSplitService:
    def __init__(self, persistence_conn, legacy_conn) -> None:
        self.persistence_conn=persistence_conn
        self.fiscal=FiscalRegimeRepository(legacy_conn)
        self.logger=logging.getLogger(__name__)
        self.audit=_audit_logger()

    def rules_for(self, member_id: int) -> tuple[SplitRule,...]:
        if is_excluded_member(member_id):
            return ()
        rows=self.persistence_conn.execute("SELECT * FROM split_rules WHERE source_member_id=? AND active=1",(member_id,)).fetchall()
        result=[]
        for row in rows:
            recipients=self.persistence_conn.execute("SELECT * FROM split_rule_recipients WHERE rule_id=? AND active=1 ORDER BY sort_order,id",(row["id"],)).fetchall()
            valid = [r for r in recipients if not is_excluded_member(r["recipient_member_id"])]
            if len(valid) != len(recipients):
                log_system_member_excluded(self.logger, origin="LiquidationSplitService.rules_for", count=len(recipients)-len(valid))
                continue
            result.append(SplitRule(row["id"],member_id,row["split_type"],tuple(SplitRecipient(r["recipient_member_id"],r["recipient_member_name"] or "",Decimal(str(r["value"])),bool(r["is_residual"]),r["sort_order"]) for r in valid),row["source_member_name"] or "",row["campaign"],row["crop"],row["variety"],row["remittance_id"],row["priority"]))
        return tuple(result)

    def resolve_rule(self, member, header) -> SplitRule | None:
        candidates=[]
        for r in self.rules_for(member.member_id):
            values=((r.campaign,str(header.campana)),(r.crop,str(header.cultivo)),(r.variety,str(member.variety)),(r.remittance_id,str(header.remesa_id)))
            if all(expected is None or str(expected).strip().upper()==actual.strip().upper() for expected,actual in values):
                specificity=sum(v is not None for v,_ in values); candidates.append((specificity,-r.priority,r))
        if not candidates: return None
        candidates.sort(key=lambda x:(x[0],x[1]),reverse=True)
        if len(candidates)>1 and candidates[0][:2]==candidates[1][:2]: raise ValueError(f"Regla de división ambigua para socio {member.member_id}")
        return candidates[0][2]

    @staticmethod
    def factors(rule: SplitRule | None, source_id: int, source_name: str) -> tuple[tuple[SplitRecipient,Decimal],...]:
        if rule is None: return ((SplitRecipient(source_id,source_name,Decimal("1"),True),Decimal("1")),)
        rs=rule.recipients
        if not rs: raise ValueError(f"La regla {rule.id} no tiene destinatarios")
        kind=rule.split_type.upper()
        if kind=="EQUAL_PARTS": factors=[Decimal(1)/len(rs)]*len(rs)
        elif kind=="WEIGHTS":
            total=sum((r.value for r in rs),Decimal(0))
            if total<=0: raise ValueError("La suma de pesos debe ser positiva")
            factors=[r.value/total for r in rs]
        elif kind in ("PERCENTAGE","PERCENTAGE_WITH_RESIDUAL"):
            factors=[r.value/Decimal(100) for r in rs]
        else: raise ValueError(f"Tipo de división no soportado: {kind}")
        total=sum(factors,Decimal(0))
        if total>1 and kind in ("PERCENTAGE","PERCENTAGE_WITH_RESIDUAL"):
            raise ValueError(f"Los porcentajes de la regla {rule.id} no pueden sumar más de 100")
        if total<1 and kind in ("PERCENTAGE","PERCENTAGE_WITH_RESIDUAL"):
            source_index=next((i for i,r in enumerate(rs) if r.recipient_member_id==source_id),None)
            if source_index is None:
                rs=rs+(SplitRecipient(source_id,source_name,Decimal(0),False,9999),)
                factors.append(1-total)
            else:
                factors[source_index]+=1-total
        if sum(factors,Decimal(0))!=1: raise ValueError(f"Los factores de la regla {rule.id} no suman 1")
        return tuple(zip(rs,factors))

    @staticmethod
    def _allocate(total: Decimal, factors, quantum: Decimal, residual_index: int) -> list[Decimal]:
        parts=[(total*f).quantize(quantum,rounding=ROUND_HALF_UP) for f in factors]
        parts[residual_index]+=total-sum(parts,Decimal(0))
        return parts

    def _audit_field(self, rule_id, field, source, before, quantum, residual_member_id,
                     adjustment, after, *, status="OK") -> None:
        self.audit.info(
            "[SplitFieldConservation]\nrule_id=%s\nfield=%s\nsource_value=%s\n"
            "allocated_value_before_adjustment=%s\ndifference_before_adjustment=%s\n"
            "quantum=%s\nresidual_member_id=%s\nresidual_adjustment=%s\n"
            "allocated_value_after_adjustment=%s\ndifference_after_adjustment=%s\nstatus=%s",
            rule_id,field,source,before,source-before,quantum,residual_member_id,
            adjustment,after,source-after,status)

    def split(self, member, header, *, cod_art: str | None=None) -> tuple[SplitPreviewLine,...]:
        if is_excluded_member(member.member_id):
            log_system_member_excluded(self.logger, origin="LiquidationSplitService.split", count=1,
                                       net_kg=getattr(member, "net_kg", 0), remesa_id=header.remesa_id)
            return ()
        rule=self.resolve_rule(member,header); pairs=self.factors(rule,member.member_id,member.member_name)
        configured_residual=next((i for i,(r,_) in enumerate(pairs) if r.is_residual),None)
        source_residual=next((i for i,(r,_) in enumerate(pairs) if r.recipient_member_id==member.member_id),None)
        residual_source="CONFIGURED"
        if configured_residual is not None:
            residual=configured_residual
        elif source_residual is not None:
            residual=source_residual; residual_source="FALLBACK_SOURCE_MEMBER"
        else:
            residual=len(pairs)-1; residual_source="FALLBACK_LAST_PARTICIPANT"
        factors=[f for _,f in pairs]; residual_member_id=pairs[residual][0].recipient_member_id
        self.audit.info("[SplitResidual]\nrule_id=%s\nresidual_member_id=%s\nresidual_source=%s",
                        rule.id if rule else None,residual_member_id,residual_source)
        self.audit.info("[SplitRuleResolved]\nrule_id=%s\nsource_member_id=%s\nsplit_type=%s\nremittance_id=%s\ncampaign=%s\ncrop=%s\nvariety=%s",
            rule.id if rule else None,member.member_id,rule.split_type if rule else None,header.remesa_id,header.campana,header.cultivo,member.variety)
        for recipient,factor in pairs:
            self.audit.info("[SplitFactor]\nrule_id=%s\nmember_id=%s\nrole=%s\nconfigured_percentage=%s\nfinal_factor=%s\nis_residual=%s",
                rule.id if rule else None,recipient.recipient_member_id,"SOURCE" if recipient.recipient_member_id==member.member_id else "RECIPIENT",
                recipient.value,factor,recipient.is_residual)
        fields={"net": (Decimal(member.net_kg),KILOS), "gross":(Decimal(member.gross_amount),MONEY), "collection":(Decimal(member.collection_amount or 0),MONEY), "hectare":(Decimal(member.hectare_fee_amount or 0),MONEY), "quality":(Decimal(member.quality_amount or 0),MONEY), "transport":(Decimal(member.transport_amount or 0),MONEY), "globalgap":(Decimal(member.globalgap_amount or 0),MONEY), "base":(Decimal(member.taxable_base or 0),MONEY)}
        self.audit.info("[SplitSource]\nsource_member_id=%s\nvariety=%s\nnet_kg=%s\ngross_amount=%s\ntaxable_base=%s\ntotal_amount=%s",
            member.member_id,member.variety,fields["net"][0],fields["gross"][0],fields["base"][0],Decimal(member.total_amount or 0))
        allocated={name:self._allocate(total,factors,q,residual) for name,(total,q) in fields.items()}
        audit_names={"net":"net_kg","gross":"gross_amount","collection":"collection_amount",
            "hectare":"hectare_fee_amount","quality":"quality_amount","transport":"transport_amount",
            "globalgap":"globalgap_amount","base":"taxable_base"}
        for name,(source,quantum) in fields.items():
            after=sum(allocated[name],Decimal(0))
            # _allocate performs the adjustment as part of the residual part; report
            # what independent rounding would have produced as the pre-adjustment sum.
            before=sum(((source*f).quantize(quantum,rounding=ROUND_HALF_UP) for f in factors),Decimal(0))
            self._audit_field(rule.id if rule else None,audit_names[name],source,before,quantum,
                              residual_member_id,source-before,after)
        lines=[]
        for i,(recipient,factor) in enumerate(pairs):
            lookup=self.fiscal.get_for_member(recipient.recipient_member_id); base=allocated["base"][i]
            fiscal=calculate_fiscal_result(base,allocated["net"][i],lookup.regime.vat_rate,lookup.regime.withholding_rate)
            net=allocated["net"][i]; gross=allocated["gross"][i]
            lines.append(SplitPreviewLine(member.member_id,member.member_name,recipient.recipient_member_id,recipient.recipient_member_name or str(recipient.recipient_member_id),member.variety,factor,net,gross,allocated["collection"][i],allocated["hectare"][i],allocated["quality"][i],allocated["transport"][i],allocated["globalgap"][i],base,fiscal.vat_rate,fiscal.withholding_rate,fiscal.vat_amount,fiscal.withholding_amount,fiscal.total_amount,(gross/net).quantize(Decimal("0.0000001")) if net else None,fiscal.final_average_price,cod_art,rule.id if rule else None,rule.split_type if rule else None,lookup.warnings,member.destruction_price,member.table_destruction_price,member.rotten_price,member.national_market_price,member.rotten_leaves_price))
            self.audit.info("[SplitAllocation]\nsource_member_id=%s\nrecipient_member_id=%s\nfactor=%s\nnet_kg=%s\ngross_amount=%s\ntaxable_base=%s\ntotal_amount=%s",
                member.member_id,recipient.recipient_member_id,factor,net,gross,base,fiscal.total_amount)
        fiscal_sources={"vat_amount":Decimal(getattr(member,"vat_amount",0) or 0),
                        "withholding_amount":Decimal(getattr(member,"withholding_amount",0) or 0),
                        "total_amount":Decimal(member.total_amount or 0)}
        failed=[]
        # Fiscality is calculated per recipient first. Only a rounding-sized
        # reconciliation is then posted explicitly to the residual participant.
        max_fiscal_adjustment=MONEY*Decimal(len(lines))/Decimal(2)
        for field,source in fiscal_sources.items():
            before=sum((getattr(x,field) for x in lines),Decimal(0)); adjustment=source-before
            status="OK"
            if abs(adjustment)>max_fiscal_adjustment:
                status="ERROR"; failed.append((field,source,before,adjustment,MONEY))
                after=before
            else:
                lines[residual]=replace(lines[residual],**{field:getattr(lines[residual],field)+adjustment})
                after=sum((getattr(x,field) for x in lines),Decimal(0))
                if source-after!=0:
                    status="ERROR"; failed.append((field,source,after,source-after,MONEY))
            self._audit_field(rule.id if rule else None,field,source,before,MONEY,
                              residual_member_id,adjustment if status=="OK" else Decimal(0),after,status=status)

        summary_fields=(("net_kg",fields["net"][0],KILOS),("gross_amount",fields["gross"][0],MONEY),
            ("taxable_base",fields["base"][0],MONEY),("vat_amount",fiscal_sources["vat_amount"],MONEY),
            ("withholding_amount",fiscal_sources["withholding_amount"],MONEY),("total_amount",fiscal_sources["total_amount"],MONEY))
        summary={name:(source,sum((getattr(x,name) for x in lines),Decimal(0))) for name,source,_ in summary_fields}
        for name,source,quantum in summary_fields:
            allocated_total=summary[name][1]
            if source-allocated_total!=0 and not any(item[0]==name for item in failed):
                failed.append((name,source,allocated_total,source-allocated_total,quantum))
        failed_fields=",".join(item[0] for item in failed)
        self.audit.info("[SplitConservation]\nsource_net_kg=%s\nallocated_net_kg=%s\nnet_difference=%s\nsource_gross_amount=%s\nallocated_gross_amount=%s\ngross_difference=%s\nsource_taxable_base=%s\nallocated_taxable_base=%s\nbase_difference=%s\nsource_vat_amount=%s\nallocated_vat_amount=%s\nvat_difference=%s\nsource_withholding_amount=%s\nallocated_withholding_amount=%s\nwithholding_difference=%s\nsource_total_amount=%s\nallocated_total_amount=%s\ntotal_difference=%s\nstatus=%s\nfailed_fields=%s",
            *(value for name,_,_ in summary_fields for value in (summary[name][0],summary[name][1],summary[name][0]-summary[name][1])),
            "ERROR" if failed else "OK",failed_fields)
        if failed:
            field,source,allocated_total,difference,quantum=failed[0]
            raise ValueError(f"La regla {rule.id if rule else None} no conserva la liquidación: "
                f"field={field} source={source} allocated={allocated_total} difference={difference} "
                f"quantum={quantum} residual_member={residual_member_id}")
        return tuple(lines)
