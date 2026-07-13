from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Sequence
import logging

from domain.calculation_models import CalculationStatus, GradeBreakdown, LiquidationCalculationResult, LiquidationHeader, LiquidationLine, LiquidationResult, LiquidationTotals, MemberLiquidation
from domain.financial_rules import calculate_quality_adjustment
from domain.hectare_fee import calculate_line_hectare_fee
from domain.models import Delivery, Remesa
from domain.utils import get_price_labels, is_liquidated, parse_yes_no, round_money, round_price, to_decimal

PRICE_FIELDS = [f"P{i}" for i in range(12)] + ["PDESTRIO", "PDMESA", "PPODRIDO"]


def calculate_delivery_collection(delivery: Delivery) -> Decimal:
    return delivery.collection_cost + delivery.social_security_collection + delivery.foreman_cost


def calculate_delivery_transport(delivery: Delivery) -> Decimal:
    return delivery.transport_cost


def calculate_member_collection(deliveries: Sequence[Delivery], apply_collection: bool) -> tuple[Decimal, Decimal]:
    detected_amount = sum((calculate_delivery_collection(delivery) for delivery in deliveries), start=Decimal("0"))
    return detected_amount, detected_amount if apply_collection else Decimal("0")


def calculate_member_transport(deliveries: Sequence[Delivery], apply_transport: bool) -> tuple[Decimal, Decimal]:
    detected_amount = sum((calculate_delivery_transport(delivery) for delivery in deliveries), start=Decimal("0"))
    return detected_amount, detected_amount if apply_transport else Decimal("0")


def calculate_taxable_base(commercial_amount: Decimal, collection: Decimal, hectare_fee: Decimal, quality: Decimal, transport: Decimal, globalgap: Decimal) -> Decimal:
    return round_money(commercial_amount - collection - hectare_fee + quality + transport + globalgap)


def calculate_vat(taxable_base: Decimal, rate: Decimal) -> Decimal:
    return round_money(taxable_base * rate / Decimal("100"))


def calculate_withholding(taxable_base: Decimal, rate: Decimal) -> Decimal:
    return round_money(taxable_base * rate / Decimal("100"))


def calculate_total(taxable_base: Decimal, vat_amount: Decimal, withholding_amount: Decimal) -> Decimal:
    return round_money(taxable_base + vat_amount - withholding_amount)


class LiquidacionCalculator:
    """Simulación en memoria; no escribe en Access ni en DLiquidaciones."""

    def __init__(self, quality_repository: Any | None = None, hectare_repository: Any | None = None, hectare_config: Any | None = None) -> None:
        self.quality_repository = quality_repository
        self.hectare_repository = hectare_repository
        self.hectare_config = hectare_config
        self.logger = logging.getLogger(__name__)

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None) -> LiquidationCalculationResult:
        prices = {field: to_decimal((remesa.prices if remesa else {}).get(field)) for field in PRICE_FIELDS}
        values = remesa.values if remesa else {}
        options = {
            "Recolección": parse_yes_no(values.get("AplRec")),
            "Transporte": parse_yes_no(values.get("AplTte")),
            "Calidad": parse_yes_no(values.get("AplCal")),
            "GlobalGAP": parse_yes_no(values.get("AplGlobal")),
            "Cuota por hectárea": parse_yes_no(values.get("AplCHa")),
            "Precalibrado": parse_yes_no(values.get("Precalibrado")),
        }
        header = LiquidationHeader(values.get("IdREMESA", ""), str(values.get("REMESA") or "Simulación"), str(values.get("CAMPAÑA") or ""), str(values.get("EMPRESA") or ""), str(values.get("CULTIVO") or ""), str(values.get("FECHARE") or ""), str(values.get("PERIODO1") or ""), str(values.get("PERIODO2") or ""), str(values.get("TipoLiq") or ""), str(values.get("CATEGORIA") or ""), str(values.get("IdSocio") or "0"), [str(values.get("VARIEDAD") or "")], options, prices)
        grouped: dict[tuple[int, str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "net": Decimal("0"), "deliveries": [], "grades": [Decimal("0") for _ in range(12)], "des": Decimal("0"), "mesa": Decimal("0"), "pod": Decimal("0")})
        warnings: list[str] = []
        liquidated = sum(1 for d in deliveries if is_liquidated(d.liquidado))
        if liquidated:
            warnings.append(f"{liquidated} entregas ya figuran como liquidadas.")
        for d in deliveries:
            key = (int(d.socio or 0), str(d.nombre_socio or ""), str(d.variedad or ""))
            data = grouped[key]
            data["count"] += 1
            data["net"] += d.effective_net_kg
            data["deliveries"].append(d)
            extra = d.extra or {}
            for i in range(12):
                data["grades"][i] += to_decimal(extra.get(f"Cal{i}"))
            data["des"] += to_decimal(extra.get("DesLinea")); data["mesa"] += to_decimal(extra.get("DesMesa")); data["pod"] += to_decimal(extra.get("Podrido"))
        labels = get_price_labels(header.cultivo)
        members: list[MemberLiquidation] = []
        member_indexes: dict[int, list[int]] = defaultdict(list)
        for (socio, name, variety), data in sorted(grouped.items()):
            grades=[]; commercial_amount=Decimal("0"); commercial_kg=Decimal("0")
            for i, kg in enumerate(data["grades"]):
                amount=round_money(kg*prices[f"P{i}"]); commercial_amount += amount; commercial_kg += kg
                grades.append(GradeBreakdown(f"P{i}", labels[i] if i < len(labels) else f"P{i}", kg, round_price(prices[f"P{i}"]), amount))
            des_amount=round_money(data["des"]*prices["PDESTRIO"]); mesa_amount=round_money(data["mesa"]*prices["PDMESA"]); pod_amount=round_money(data["pod"]*prices["PPODRIDO"])
            gross=round_money(commercial_amount+des_amount+mesa_amount+pod_amount)
            detected_collection, collection = calculate_member_collection(data["deliveries"], options["Recolección"])
            detected_transport, transport = calculate_member_transport(data["deliveries"], options["Transporte"])
            qrate=Decimal("0"); qsource="disabled" if not options["Calidad"] else "not_found"; qamount=Decimal("0"); line_warnings=list(warnings)
            if options["Calidad"] and self.quality_repository:
                qr=self.quality_repository.get_quality_rate(socio, header.campana, header.empresa, header.cultivo, int(header.remesa_id or 0))
                qrate=qr.rate; qsource=qr.source; line_warnings.extend(qr.warnings)
                qamount=calculate_quality_adjustment(data["net"], qrate, True)
                self.logger.debug("Calidad socio=%s IdConcepto=%s fuente=%s tarifa=%s kilos=%s importe=%s", socio, qr.concept_id, qsource, qrate, data["net"], qamount)
            elif options["Calidad"]:
                line_warnings.append("Repositorio BonCalidad no disponible; calidad no calculada.")
            statuses={"commercial": CalculationStatus.CALCULATED, "collection": CalculationStatus.CALCULATED if options["Recolección"] else CalculationStatus.NOT_APPLICABLE, "transport": CalculationStatus.CALCULATED if options["Transporte"] else CalculationStatus.NOT_APPLICABLE, "quality": CalculationStatus.CALCULATED if options["Calidad"] else CalculationStatus.DISABLED, "globalgap": CalculationStatus.PENDING if options["GlobalGAP"] else CalculationStatus.NOT_APPLICABLE, "hectare_fee": CalculationStatus.DISABLED if not options["Cuota por hectárea"] else CalculationStatus.PENDING, "taxable_base": CalculationStatus.CALCULATED, "vat": CalculationStatus.PENDING, "withholding": CalculationStatus.PENDING, "total": CalculationStatus.PENDING}
            avg=round_price(commercial_amount/commercial_kg) if commercial_kg else None
            member=MemberLiquidation(member_id=socio, member_name=name, variety=variety, delivery_count=data["count"], net_deliveries=data["net"], net_commercial=commercial_kg, net_waste=data["des"]+data["mesa"], net_rotten=data["pod"], grades=tuple(grades), commercial_amount=round_money(commercial_amount), destruction_amount=des_amount, table_destruction_amount=mesa_amount, rotten_amount=pod_amount, gross_amount=gross, detected_collection_amount=detected_collection, collection_amount=round_money(collection), detected_transport_amount=detected_transport, transport_amount=round_money(transport), quality_amount=qamount, globalgap_amount=Decimal("0"), hectare_fee_amount=Decimal("0"), effective_net_kg=data["net"], quality_rate=qrate, quality_source=qsource, taxable_base=None, commercial_average_price=avg, final_average_price=None, warnings=tuple(line_warnings), statuses=statuses, source_deliveries=tuple(data["deliveries"]))
            member_indexes[socio].append(len(members)); members.append(member)
        members = self._apply_hectare_fee(members, header, options["Cuota por hectárea"])
        final_members=[]
        for m in members:
            tb=calculate_taxable_base(m.commercial_amount, m.collection_amount or Decimal("0"), m.hectare_fee_amount or Decimal("0"), m.quality_amount or Decimal("0"), m.transport_amount or Decimal("0"), m.globalgap_amount or Decimal("0"))
            final_avg=round_price(tb/m.net_kg) if m.net_kg else None
            final_members.append(self._replace(m, taxable_base=tb, final_average_price=final_avg))
        members=final_members
        def sum_opt(attr: str) -> Decimal | None:
            vals=[getattr(m, attr) for m in members]
            return None if any(v is None for v in vals) else sum(vals, Decimal("0"))
        totals=LiquidationTotals(sum((m.net_kg for m in members), Decimal("0")), sum((m.commercial_amount for m in members), Decimal("0")), sum((m.gross_amount for m in members), Decimal("0")), sum((m.detected_collection_amount for m in members), Decimal("0")), sum_opt("collection_amount"), sum((m.detected_transport_amount for m in members), Decimal("0")), sum_opt("transport_amount"), sum_opt("quality_amount"), sum_opt("globalgap_amount"), sum_opt("hectare_fee_amount"), sum_opt("taxable_base"), sum_opt("vat_amount"), sum_opt("withholding_amount"), sum_opt("total_amount"))
        result=LiquidationResult(header, tuple(members), totals, tuple(dict.fromkeys(w for m in members for w in m.warnings)))
        lines=[LiquidationLine(m.member_id,m.member_name,m.variety,m.delivery_count,m.net_kg,m.commercial_amount,m.collection_amount,m.transport_amount,m.quality_amount,m.globalgap_amount,m.hectare_fee_amount,m.taxable_base,m.vat_amount,m.withholding_amount,m.total_amount) for m in members]
        return LiquidationCalculationResult(lines, len(deliveries), len({d.socio for d in deliveries}), len({d.variedad for d in deliveries if d.variedad}), totals.net_kg, totals.commercial_amount, list(result.warnings), result)

    def _replace(self, member: MemberLiquidation, **changes: Any) -> MemberLiquidation:
        from dataclasses import replace
        return replace(member, **changes)

    def _apply_hectare_fee(self, members: list[MemberLiquidation], header: LiquidationHeader, apply_fee: bool) -> list[MemberLiquidation]:
        delivery_crops = tuple(getattr(self.hectare_config, "hectare_fee_delivery_crops", ("CITRICOS", "MANDARINA", "DIRECTO", "DIRECTOCHF", "INDUSTRIA")))
        remittance_crops = tuple(getattr(self.hectare_config, "hectare_fee_applicable_remittance_crops", getattr(self.hectare_config, "hectare_fee_applicable_crops", delivery_crops)))
        price = getattr(self.hectare_config, "hectare_fee_price_per_hectare", Decimal("195"))
        crop = header.cultivo.strip().upper()

        if crop not in remittance_crops:
            return [self._replace(m, hectare_fee_price=price, hectare_fee_status=CalculationStatus.NOT_APPLICABLE, hectare_fee_amount=Decimal("0"), statuses={**m.statuses, "hectare_fee": CalculationStatus.NOT_APPLICABLE}) for m in members]

        result = list(members)
        by_member: dict[int, list[int]] = defaultdict(list)
        for idx, m in enumerate(result):
            by_member[m.member_id].append(idx)

        if not self.hectare_repository:
            return [self._replace(m, warnings=(*m.warnings, "Repositorio DEEPP/PesosFres no disponible; cuota Ha no calculada."), hectare_fee_status=CalculationStatus.ERROR, statuses={**m.statuses, "hectare_fee": CalculationStatus.ERROR}) for m in members]

        for socio, indexes in by_member.items():
            hectares, hwarn = self.hectare_repository.calculate_applicable_hectares(socio, header.campana, header.empresa)
            total_fee = round_money(hectares * price)
            total_kg = self.hectare_repository.total_effective_kg(socio, header.campana, header.empresa, delivery_crops)
            rate = None if total_kg <= 0 else total_fee / total_kg
            for idx in indexes:
                m = result[idx]
                warnings = (*m.warnings, *hwarn)
                if total_kg <= 0:
                    result[idx] = self._replace(m, applicable_hectares=hectares, hectare_fee_price=price, hectare_fee_total_member=total_fee, hectare_fee_total_effective_kg=total_kg, hectare_fee_rate_per_kg=None, hectare_fee_amount=Decimal("0"), hectare_fee_status=CalculationStatus.ERROR, warnings=(*warnings, "Cuota Ha no calculable: kilos efectivos totales <= 0."), statuses={**m.statuses, "hectare_fee": CalculationStatus.ERROR})
                    continue
                detected_fee = calculate_line_hectare_fee(m.net_kg, rate)
                status = CalculationStatus.CALCULATED if apply_fee else CalculationStatus.DISABLED
                applied_fee = detected_fee if apply_fee else Decimal("0")
                result[idx] = self._replace(m, applicable_hectares=hectares, hectare_fee_price=price, hectare_fee_total_member=total_fee, hectare_fee_total_effective_kg=total_kg, hectare_fee_rate_per_kg=rate, hectare_fee_amount=applied_fee, hectare_fee_status=status, hectare_fee_rounding_adjustment=Decimal("0"), warnings=warnings, statuses={**m.statuses, "hectare_fee": status})
                self.logger.debug("Cuota socio=%s hectareas=%s precio_ha=%s cuota_anual=%s kilos_totales=%s proporcion=%s neto_linea=%s cuota_linea=%s filtros=delivery_crops:%s remittance_crops:%s", socio, hectares, price, total_fee, total_kg, rate, m.net_kg, applied_fee, delivery_crops, remittance_crops)
        return result
