from __future__ import annotations

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

    def split(self, member, header, *, cod_art: str | None=None) -> tuple[SplitPreviewLine,...]:
        if is_excluded_member(member.member_id):
            log_system_member_excluded(self.logger, origin="LiquidationSplitService.split", count=1,
                                       net_kg=getattr(member, "net_kg", 0), remesa_id=header.remesa_id)
            return ()
        rule=self.resolve_rule(member,header); pairs=self.factors(rule,member.member_id,member.member_name)
        residual=next((i for i,(r,_) in enumerate(pairs) if r.is_residual),len(pairs)-1); factors=[f for _,f in pairs]
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
        lines=[]
        for i,(recipient,factor) in enumerate(pairs):
            lookup=self.fiscal.get_for_member(recipient.recipient_member_id); base=allocated["base"][i]
            fiscal=calculate_fiscal_result(base,allocated["net"][i],lookup.regime.vat_rate,lookup.regime.withholding_rate)
            net=allocated["net"][i]; gross=allocated["gross"][i]
            lines.append(SplitPreviewLine(member.member_id,member.member_name,recipient.recipient_member_id,recipient.recipient_member_name or str(recipient.recipient_member_id),member.variety,factor,net,gross,allocated["collection"][i],allocated["hectare"][i],allocated["quality"][i],allocated["transport"][i],allocated["globalgap"][i],base,fiscal.vat_rate,fiscal.withholding_rate,fiscal.vat_amount,fiscal.withholding_amount,fiscal.total_amount,(gross/net).quantize(Decimal("0.0000001")) if net else None,fiscal.final_average_price,cod_art,rule.id if rule else None,rule.split_type if rule else None,lookup.warnings,member.destruction_price,member.table_destruction_price,member.rotten_price,member.national_market_price,member.rotten_leaves_price))
            self.audit.info("[SplitAllocation]\nsource_member_id=%s\nrecipient_member_id=%s\nfactor=%s\nnet_kg=%s\ngross_amount=%s\ntaxable_base=%s\ntotal_amount=%s",
                member.member_id,recipient.recipient_member_id,factor,net,gross,base,fiscal.total_amount)
        source_total=Decimal(member.total_amount or 0)
        totals=(sum((x.net_kg for x in lines),Decimal(0)),sum((x.gross_amount for x in lines),Decimal(0)),
            sum((x.taxable_base for x in lines),Decimal(0)),sum((x.total_amount for x in lines),Decimal(0)))
        differences=(fields["net"][0]-totals[0],fields["gross"][0]-totals[1],fields["base"][0]-totals[2],source_total-totals[3])
        ok=abs(differences[0])<KILOS and all(abs(value)<MONEY for value in differences[1:])
        self.audit.info("[SplitConservation]\nsource_net_kg=%s\nallocated_net_kg=%s\nnet_difference=%s\nsource_gross_amount=%s\nallocated_gross_amount=%s\ngross_difference=%s\nsource_taxable_base=%s\nallocated_taxable_base=%s\nbase_difference=%s\nsource_total_amount=%s\nallocated_total_amount=%s\ntotal_difference=%s\nstatus=%s",
            fields["net"][0],totals[0],differences[0],fields["gross"][0],totals[1],differences[1],fields["base"][0],totals[2],differences[2],source_total,totals[3],differences[3],"OK" if ok else "ERROR")
        if not ok:
            raise ValueError(f"La regla {rule.id if rule else None} no conserva los kilos o importes de la liquidación")
        return tuple(lines)
