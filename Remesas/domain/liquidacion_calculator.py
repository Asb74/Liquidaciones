from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Sequence

from domain.calculation_models import CalculationStatus, GradeBreakdown, LiquidationCalculationResult, LiquidationHeader, LiquidationLine, LiquidationResult, LiquidationTotals, MemberLiquidation
from domain.models import Delivery, Remesa
from domain.utils import get_price_labels, is_liquidated, round_money, round_price, to_decimal

PRICE_FIELDS = [f"P{i}" for i in range(12)] + ["PDESTRIO", "PDMESA", "PPODRIDO"]


def calculate_collection(deliveries: Sequence[Delivery], context: Any) -> Decimal | None:
    if not getattr(context, "apply_collection", False):
        return None
    return None


def calculate_transport(deliveries: Sequence[Delivery], context: Any) -> Decimal | None:
    if not getattr(context, "apply_transport", False):
        return None
    return None


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
            "Recolección": str(values.get("AplRec") or "").upper() == "S",
            "Transporte": str(values.get("AplTte") or "").upper() == "S",
            "Calidad": str(values.get("AplCal") or "").upper() == "S",
            "GlobalGAP": str(values.get("AplGlobal") or "").upper() == "S",
            "Cuota por hectárea": str(values.get("AplCHa") or "").upper() == "S",
            "Precalibrado": str(values.get("Precalibrado") or "").upper() == "S",
        }
        header = LiquidationHeader(values.get("IdREMESA", ""), str(values.get("REMESA") or "Simulación"), str(values.get("CAMPAÑA") or ""), str(values.get("EMPRESA") or ""), str(values.get("CULTIVO") or ""), str(values.get("FECHARE") or ""), str(values.get("PERIODO1") or ""), str(values.get("PERIODO2") or ""), str(values.get("TipoLiq") or ""), str(values.get("CATEGORIA") or ""), str(values.get("IdSocio") or "0"), [str(values.get("VARIEDAD") or "")], options, prices)
        grouped: dict[tuple[int, str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "net": Decimal("0"), "grades": [Decimal("0") for _ in range(12)], "des": Decimal("0"), "mesa": Decimal("0"), "pod": Decimal("0")})
        warnings: list[str] = []
        liquidated = sum(1 for d in deliveries if is_liquidated(d.liquidado))
        if liquidated:
            warnings.append(f"{liquidated} entregas ya figuran como liquidadas.")
        if any(options[k] for k in ["Recolección", "Transporte", "Calidad", "GlobalGAP", "Cuota por hectárea"]):
            warnings.append("frmPagosCIT.frm no está disponible en el repositorio: los conceptos económicos dependientes de VB6 quedan Pendiente y no se sustituyen por 0,00 €.")
        for d in deliveries:
            key = (int(d.socio or 0), str(d.nombre_socio or ""), str(d.variedad or ""))
            data = grouped[key]; data["count"] += 1; data["net"] += to_decimal(d.neto)
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
            statuses={"commercial": CalculationStatus.CALCULATED, "collection": CalculationStatus.PENDING if options["Recolección"] else CalculationStatus.NOT_APPLICABLE, "transport": CalculationStatus.PENDING if options["Transporte"] else CalculationStatus.NOT_APPLICABLE, "quality": CalculationStatus.PENDING if options["Calidad"] else CalculationStatus.NOT_APPLICABLE, "globalgap": CalculationStatus.PENDING if options["GlobalGAP"] else CalculationStatus.NOT_APPLICABLE, "hectare_fee": CalculationStatus.PENDING if options["Cuota por hectárea"] else CalculationStatus.NOT_APPLICABLE, "taxable_base": CalculationStatus.PENDING, "vat": CalculationStatus.PENDING, "withholding": CalculationStatus.PENDING, "total": CalculationStatus.PENDING}
            avg=round_price(commercial_amount/commercial_kg) if commercial_kg else None
            members.append(MemberLiquidation(socio, name, variety, data["count"], data["net"], commercial_kg, data["des"]+data["mesa"], data["pod"], tuple(grades), round_money(commercial_amount), des_amount, mesa_amount, pod_amount, gross, None, None, None, None, None, None, None, None, None, None, None, avg, None, tuple(warnings), statuses))
        def sum_opt(attr: str) -> Decimal | None:
            vals=[getattr(m, attr) for m in members]
            return None if any(v is None for v in vals) else sum(vals, Decimal("0"))
        totals=LiquidationTotals(sum((m.net_kg for m in members), Decimal("0")), sum((m.commercial_amount for m in members), Decimal("0")), sum((m.gross_amount for m in members), Decimal("0")), sum_opt("collection_amount"), sum_opt("transport_amount"), sum_opt("quality_amount"), sum_opt("globalgap_amount"), sum_opt("hectare_fee_amount"), sum_opt("taxable_base"), sum_opt("vat_amount"), sum_opt("withholding_amount"), sum_opt("total_amount"))
        result=LiquidationResult(header, tuple(members), totals, tuple(warnings))
        lines=[LiquidationLine(m.member_id,m.member_name,m.variety,m.delivery_count,m.net_kg,m.commercial_amount,m.collection_amount,m.transport_amount,m.quality_amount,m.globalgap_amount,m.hectare_fee_amount,m.taxable_base,m.vat_amount,m.withholding_amount,m.total_amount) for m in members]
        return LiquidationCalculationResult(lines, len(deliveries), len({d.socio for d in deliveries}), len({d.variedad for d in deliveries if d.variedad}), totals.net_kg, totals.commercial_amount, warnings, result)
