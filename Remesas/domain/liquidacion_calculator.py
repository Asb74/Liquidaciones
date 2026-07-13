from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Sequence

from domain.calculation_models import CalculationStatus, GradeBreakdown, LiquidationCalculationResult, LiquidationHeader, LiquidationLine, LiquidationResult, LiquidationTotals, MemberLiquidation
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


def calculate_quality_adjustment(member_id: int, net_kg: Decimal, context: Any) -> Decimal | None:
    if not getattr(context, "apply_quality", False):
        return None
    return None


def calculate_globalgap_adjustment(member_id: int, commercial_kg: Decimal, context: Any) -> Decimal | None:
    if not getattr(context, "apply_globalgap", False):
        return None
    return None


def calculate_hectare_fee(member_id: int, variety: str, context: Any) -> Decimal | None:
    if not getattr(context, "apply_hectare_fee", False):
        return None
    return None


def calculate_taxable_base(commercial_amount: Decimal, *concepts: Decimal | None) -> Decimal | None:
    return None if any(v is None for v in concepts) else round_money(commercial_amount + sum(concepts, Decimal("0")))


def calculate_vat(taxable_base: Decimal, rate: Decimal) -> Decimal:
    return round_money(taxable_base * rate / Decimal("100"))


def calculate_withholding(taxable_base: Decimal, rate: Decimal) -> Decimal:
    return round_money(taxable_base * rate / Decimal("100"))


def calculate_total(taxable_base: Decimal, vat_amount: Decimal, withholding_amount: Decimal) -> Decimal:
    return round_money(taxable_base + vat_amount - withholding_amount)


class LiquidacionCalculator:
    """Simulación en memoria; no escribe en Access ni en DLiquidaciones."""

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None) -> LiquidationCalculationResult:
        prices = {field: to_decimal((remesa.prices if remesa else {}).get(field)) for field in PRICE_FIELDS}
        values = remesa.values if remesa else {}
        options = {
            "Recolección": parse_yes_no(values.get("AplRec")),
            "Transporte": parse_yes_no(values.get("AplTte")),
            "Calidad": str(values.get("AplCal") or "").upper() == "S",
            "GlobalGAP": str(values.get("AplGlobal") or "").upper() == "S",
            "Cuota por hectárea": str(values.get("AplCHa") or "").upper() == "S",
            "Precalibrado": str(values.get("Precalibrado") or "").upper() == "S",
        }
        header = LiquidationHeader(values.get("IdREMESA", ""), str(values.get("REMESA") or "Simulación"), str(values.get("CAMPAÑA") or ""), str(values.get("EMPRESA") or ""), str(values.get("CULTIVO") or ""), str(values.get("FECHARE") or ""), str(values.get("PERIODO1") or ""), str(values.get("PERIODO2") or ""), str(values.get("TipoLiq") or ""), str(values.get("CATEGORIA") or ""), str(values.get("IdSocio") or "0"), [str(values.get("VARIEDAD") or "")], options, prices)
        grouped: dict[tuple[int, str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "net": Decimal("0"), "deliveries": [], "grades": [Decimal("0") for _ in range(12)], "des": Decimal("0"), "mesa": Decimal("0"), "pod": Decimal("0")})
        warnings: list[str] = []
        liquidated = sum(1 for d in deliveries if is_liquidated(d.liquidado))
        if liquidated:
            warnings.append(f"{liquidated} entregas ya figuran como liquidadas.")
        if any(options[k] for k in ["Calidad", "GlobalGAP", "Cuota por hectárea"]):
            warnings.append("frmPagosCIT.frm no está disponible en el repositorio: calidad, GlobalGAP y cuota Ha quedan Pendiente.")
        for d in deliveries:
            key = (int(d.socio or 0), str(d.nombre_socio or ""), str(d.variedad or ""))
            data = grouped[key]; data["count"] += 1; data["net"] += to_decimal(d.neto); data["deliveries"].append(d)
            extra = d.extra or {}
            for i in range(12): data["grades"][i] += to_decimal(extra.get(f"Cal{i}"))
            data["des"] += to_decimal(extra.get("DesLinea")); data["mesa"] += to_decimal(extra.get("DesMesa")); data["pod"] += to_decimal(extra.get("Podrido"))
        labels = get_price_labels(header.cultivo)
        members: list[MemberLiquidation] = []
        for (socio, name, variety), data in sorted(grouped.items()):
            grades=[]; commercial_amount=Decimal("0"); commercial_kg=Decimal("0")
            for i, kg in enumerate(data["grades"]):
                amount=round_money(kg*prices[f"P{i}"]); commercial_amount += amount; commercial_kg += kg
                grades.append(GradeBreakdown(f"P{i}", labels[i] if i < len(labels) else f"P{i}", kg, round_price(prices[f"P{i}"]), amount))
            des_amount=round_money(data["des"]*prices["PDESTRIO"]); mesa_amount=round_money(data["mesa"]*prices["PDMESA"]); pod_amount=round_money(data["pod"]*prices["PPODRIDO"])
            gross=round_money(commercial_amount+des_amount+mesa_amount+pod_amount)
            detected_collection, collection = calculate_member_collection(data["deliveries"], options["Recolección"])
            detected_transport, transport = calculate_member_transport(data["deliveries"], options["Transporte"])
            taxable_base = round_money(commercial_amount - collection + transport)
            statuses={"commercial": CalculationStatus.CALCULATED, "collection": CalculationStatus.CALCULATED if options["Recolección"] else CalculationStatus.NOT_APPLICABLE, "transport": CalculationStatus.CALCULATED if options["Transporte"] else CalculationStatus.NOT_APPLICABLE, "quality": CalculationStatus.PENDING if options["Calidad"] else CalculationStatus.NOT_APPLICABLE, "globalgap": CalculationStatus.PENDING if options["GlobalGAP"] else CalculationStatus.NOT_APPLICABLE, "hectare_fee": CalculationStatus.PENDING if options["Cuota por hectárea"] else CalculationStatus.NOT_APPLICABLE, "taxable_base": CalculationStatus.CALCULATED, "vat": CalculationStatus.PENDING, "withholding": CalculationStatus.PENDING, "total": CalculationStatus.PENDING}
            avg=round_price(commercial_amount/commercial_kg) if commercial_kg else None
            members.append(MemberLiquidation(socio, name, variety, data["count"], data["net"], commercial_kg, data["des"]+data["mesa"], data["pod"], tuple(grades), round_money(commercial_amount), des_amount, mesa_amount, pod_amount, gross, detected_collection, round_money(collection), detected_transport, round_money(transport), None, None, None, taxable_base, None, None, None, None, None, avg, None, tuple(warnings), statuses, tuple(data["deliveries"])))
        def sum_opt(attr: str) -> Decimal | None:
            vals=[getattr(m, attr) for m in members]
            return None if any(v is None for v in vals) else sum(vals, Decimal("0"))
        totals=LiquidationTotals(sum((m.net_kg for m in members), Decimal("0")), sum((m.commercial_amount for m in members), Decimal("0")), sum((m.gross_amount for m in members), Decimal("0")), sum((m.detected_collection_amount for m in members), Decimal("0")), sum_opt("collection_amount"), sum((m.detected_transport_amount for m in members), Decimal("0")), sum_opt("transport_amount"), sum_opt("quality_amount"), sum_opt("globalgap_amount"), sum_opt("hectare_fee_amount"), sum_opt("taxable_base"), sum_opt("vat_amount"), sum_opt("withholding_amount"), sum_opt("total_amount"))
        import logging
        logging.getLogger(__name__).debug("Remesa %s: deliveries=%s collection_cost=%s social_security=%s foreman_cost=%s collection_detected=%s collection_applied=%s transport_detected=%s transport_applied=%s apply_collection=%s apply_transport=%s", header.remesa_id, len(deliveries), sum((d.collection_cost for d in deliveries), Decimal("0")), sum((d.social_security_collection for d in deliveries), Decimal("0")), sum((d.foreman_cost for d in deliveries), Decimal("0")), totals.detected_collection_amount, totals.collection_amount, totals.detected_transport_amount, totals.transport_amount, options["Recolección"], options["Transporte"])
        result=LiquidationResult(header, tuple(members), totals, tuple(warnings))
        lines=[LiquidationLine(m.member_id,m.member_name,m.variety,m.delivery_count,m.net_kg,m.commercial_amount,m.collection_amount,m.transport_amount,m.quality_amount,m.globalgap_amount,m.hectare_fee_amount,m.taxable_base,m.vat_amount,m.withholding_amount,m.total_amount) for m in members]
        return LiquidationCalculationResult(lines, len(deliveries), len({d.socio for d in deliveries}), len({d.variedad for d in deliveries if d.variedad}), totals.net_kg, totals.commercial_amount, warnings, result)
