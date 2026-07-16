from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from data.fiscal_regime_repository import FiscalRegimeRepository
from domain.liquidacion_calculator import calculate_fiscal_result
from domain.persistence_models import SplitPreviewLine, SplitRecipient, SplitRule

MONEY=Decimal("0.01"); KILOS=Decimal("0.001")


class LiquidationSplitService:
    def __init__(self, persistence_conn, legacy_conn) -> None:
        self.persistence_conn=persistence_conn
        self.fiscal=FiscalRegimeRepository(legacy_conn)

    def rules_for(self, member_id: int) -> tuple[SplitRule,...]:
        rows=self.persistence_conn.execute("SELECT * FROM split_rules WHERE source_member_id=? AND active=1",(member_id,)).fetchall()
        result=[]
        for row in rows:
            recipients=self.persistence_conn.execute("SELECT * FROM split_rule_recipients WHERE rule_id=? AND active=1 ORDER BY sort_order,id",(row["id"],)).fetchall()
            result.append(SplitRule(row["id"],member_id,row["split_type"],tuple(SplitRecipient(r["recipient_member_id"],r["recipient_member_name"] or "",Decimal(str(r["value"])),bool(r["is_residual"]),r["sort_order"]) for r in recipients),row["source_member_name"] or "",row["campaign"],row["crop"],row["variety"],row["remittance_id"],row["priority"]))
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
        total=sum(factors,Decimal(0)); residual=[i for i,r in enumerate(rs) if r.is_residual]
        if total<1 and kind in ("PERCENTAGE","PERCENTAGE_WITH_RESIDUAL"):
            if residual: factors[residual[0]]+=1-total
            else: rs=rs+(SplitRecipient(source_id,source_name,Decimal(0),True,9999),); factors.append(1-total)
        if sum(factors,Decimal(0))!=1: raise ValueError(f"Los factores de la regla {rule.id} no suman 1")
        return tuple(zip(rs,factors))

    @staticmethod
    def _allocate(total: Decimal, factors, quantum: Decimal, residual_index: int) -> list[Decimal]:
        parts=[(total*f).quantize(quantum,rounding=ROUND_HALF_UP) for f in factors]
        parts[residual_index]+=total-sum(parts,Decimal(0))
        return parts

    def split(self, member, header, *, cod_art: int | None=None) -> tuple[SplitPreviewLine,...]:
        rule=self.resolve_rule(member,header); pairs=self.factors(rule,member.member_id,member.member_name)
        residual=next((i for i,(r,_) in enumerate(pairs) if r.is_residual),len(pairs)-1); factors=[f for _,f in pairs]
        fields={"net": (Decimal(member.net_kg),KILOS), "gross":(Decimal(member.gross_amount),MONEY), "collection":(Decimal(member.collection_amount or 0),MONEY), "hectare":(Decimal(member.hectare_fee_amount or 0),MONEY), "quality":(Decimal(member.quality_amount or 0),MONEY), "transport":(Decimal(member.transport_amount or 0),MONEY), "globalgap":(Decimal(member.globalgap_amount or 0),MONEY), "base":(Decimal(member.taxable_base or 0),MONEY)}
        allocated={name:self._allocate(total,factors,q,residual) for name,(total,q) in fields.items()}
        lines=[]
        for i,(recipient,factor) in enumerate(pairs):
            lookup=self.fiscal.get_for_member(recipient.recipient_member_id); base=allocated["base"][i]
            fiscal=calculate_fiscal_result(base,allocated["net"][i],lookup.regime.vat_rate,lookup.regime.withholding_rate)
            net=allocated["net"][i]; gross=allocated["gross"][i]
            lines.append(SplitPreviewLine(member.member_id,member.member_name,recipient.recipient_member_id,recipient.recipient_member_name or str(recipient.recipient_member_id),member.variety,factor,net,gross,allocated["collection"][i],allocated["hectare"][i],allocated["quality"][i],allocated["transport"][i],allocated["globalgap"][i],base,fiscal.vat_rate,fiscal.withholding_rate,fiscal.vat_amount,fiscal.withholding_amount,fiscal.total_amount,(gross/net).quantize(Decimal("0.0000001")) if net else None,fiscal.final_average_price,cod_art,rule.id if rule else None,rule.split_type if rule else None,lookup.warnings))
        return tuple(lines)
