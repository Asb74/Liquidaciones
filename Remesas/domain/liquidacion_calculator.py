from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from domain.calculation_models import GradeLiquidation, LiquidationCalculationResult, LiquidationHeader, LiquidationLine, LiquidationResult, LiquidationTotals, MemberLiquidation
from domain.models import Delivery, Remesa
from domain.utils import get_grade_labels, is_liquidated, to_decimal

PRICE_FIELDS = [f"P{i}" for i in range(12)] + ["PDESTRIO", "PDMESA", "PPODRIDO"]
CALIBER_FIELDS = [f"Cal{i}" for i in range(12)]
CENT = Decimal("0.01")


class LiquidacionCalculator:
    """Simulación en memoria; no escribe en Access ni en DLiquidaciones."""

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None) -> LiquidationCalculationResult:
        prices = {field: to_decimal((remesa.prices if remesa else {}).get(field)) for field in PRICE_FIELDS}
        values = remesa.values if remesa else {}
        header = LiquidationHeader(
            remesa_id=values.get("IdREMESA", ""), remesa_name=str(values.get("REMESA") or "Simulación"),
            campana=str(values.get("CAMPAÑA") or ""), empresa=str(values.get("EMPRESA") or ""), cultivo=str(values.get("CULTIVO") or ""),
            fecha_pago=str(values.get("FECHARE") or ""), periodo_desde=str(values.get("PERIODO1") or ""), periodo_hasta=str(values.get("PERIODO2") or ""),
            tipo_liquidacion=str(values.get("TipoLiq") or ""), categoria=str(values.get("CATEGORIA") or ""), socio=str(values.get("IdSocio") or "0"),
            variedades=[str(values.get("VARIEDAD") or "")], options={}, prices=prices)
        grouped: dict[tuple[int, str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "net": Decimal("0"), "grades": [Decimal("0") for _ in range(12)], "des": Decimal("0"), "mesa": Decimal("0"), "pod": Decimal("0")})
        warnings: list[str] = []
        liquidated = sum(1 for d in deliveries if is_liquidated(d.liquidado))
        if liquidated:
            warnings.append(f"Advertencia: {liquidated} entregas ya figuran como liquidadas.")
        for d in deliveries:
            key = (int(d.socio or 0), str(d.nombre_socio or ""), str(d.variedad or ""))
            data = grouped[key]
            data["count"] += 1; data["net"] += to_decimal(d.neto)
            extra = d.extra or {}
            for i in range(12): data["grades"][i] += to_decimal(extra.get(f"Cal{i}"))
            data["des"] += to_decimal(extra.get("DesLinea")); data["mesa"] += to_decimal(extra.get("DesMesa")); data["pod"] += to_decimal(extra.get("Podrido"))
        labels = get_grade_labels(header.cultivo)
        members: list[MemberLiquidation] = []
        for (socio, name, variety), data in sorted(grouped.items()):
            grades: list[GradeLiquidation] = []
            commercial_amount = Decimal("0")
            commercial_kg = Decimal("0")
            for i, kg in enumerate(data["grades"]):
                amount = (kg * prices[f"P{i}"]).quantize(CENT, ROUND_HALF_UP)
                commercial_amount += amount; commercial_kg += kg
                grades.append(GradeLiquidation(f"P{i}", labels[i] if i < len(labels) else f"P{i}", kg, prices[f"P{i}"], amount))
            des_amount = (data["des"] * prices["PDESTRIO"]).quantize(CENT, ROUND_HALF_UP)
            mesa_amount = (data["mesa"] * prices["PDMESA"]).quantize(CENT, ROUND_HALF_UP)
            pod_amount = (data["pod"] * prices["PPODRIDO"]).quantize(CENT, ROUND_HALF_UP)
            gross = (commercial_amount + des_amount + mesa_amount + pod_amount).quantize(CENT, ROUND_HALF_UP)
            # Bonificaciones/deducciones quedan a cero hasta localizar las reglas fiscales exactas de VB6.
            taxable = gross
            vat = Decimal("0.00"); withholding = Decimal("0.00")
            members.append(MemberLiquidation(socio, name, variety, data["count"], data["net"], commercial_kg, data["des"], data["mesa"], data["pod"], grades, commercial_amount.quantize(CENT), des_amount, mesa_amount, pod_amount, gross, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"), taxable, Decimal("0"), vat, Decimal("0"), withholding, (taxable + vat - withholding).quantize(CENT)))
        totals = LiquidationTotals(sum((m.net_kg for m in members), Decimal("0")), sum((m.commercial_amount for m in members), Decimal("0")), sum((m.gross_amount for m in members), Decimal("0")), sum((m.taxable_base for m in members), Decimal("0")), sum((m.vat_amount for m in members), Decimal("0")), sum((m.withholding_amount for m in members), Decimal("0")), sum((m.total_amount for m in members), Decimal("0")))
        result = LiquidationResult(header, members, totals, warnings)
        lines = [LiquidationLine(m.member_id, m.member_name, m.variety, m.delivery_count, m.net_kg, m.commercial_amount, m.collection_amount, m.transport_amount, m.quality_amount, m.globalgap_amount, m.hectare_fee_amount, m.taxable_base, m.vat_amount, m.withholding_amount, m.total_amount) for m in members]
        return LiquidationCalculationResult(lines, len(deliveries), len({d.socio for d in deliveries}), len({d.variedad for d in deliveries if d.variedad}), totals.net_kg, totals.commercial_amount, warnings, result)
