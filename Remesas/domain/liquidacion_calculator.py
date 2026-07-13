from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from domain.calculation_models import LiquidationCalculationResult, LiquidationLine
from domain.models import Delivery, Remesa
from domain.utils import is_liquidated, to_decimal

PRICE_FIELDS = [f"P{i}" for i in range(12)] + ["PDESTRIO", "PDMESA", "PPODRIDO"]
CALIBER_FIELDS = [f"Cal{i}" for i in range(12)]


class LiquidacionCalculator:
    """Cálculo comercial en memoria.

    No se encontró frmPagosCIT.frm en el repositorio, por lo que esta primera
    versión aplica la fórmula lineal pedida: Cal0..Cal11 * P0..P11 más destrío,
    mesa y podrido. Null se trata como 0 y los importes se redondean a 0,01.
    """

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None) -> LiquidationCalculationResult:
        prices = {field: to_decimal((remesa.prices if remesa else {}).get(field)) for field in PRICE_FIELDS}
        grouped: dict[tuple[int, str, str], dict[str, Any]] = defaultdict(lambda: {"count": 0, "net": Decimal("0"), "commercial": Decimal("0")})
        warnings: list[str] = []
        liquidated = sum(1 for d in deliveries if is_liquidated(d.liquidado))
        if liquidated:
            warnings.append(f"Advertencia: {liquidated} entregas ya figuran como liquidadas.")
        for d in deliveries:
            amount = self._commercial_amount(d, prices)
            key = (int(d.socio or 0), str(d.nombre_socio or ""), str(d.variedad or ""))
            grouped[key]["count"] += 1
            grouped[key]["net"] += to_decimal(d.neto)
            grouped[key]["commercial"] += amount
        lines = [
            LiquidationLine(member_id=socio, member_name=name, variety=variety, delivery_count=data["count"], net_kg=data["net"], commercial_amount=data["commercial"].quantize(Decimal("0.01"), ROUND_HALF_UP), collection_amount=None, transport_amount=None, quality_amount=None, globalgap_amount=None, hectare_fee_amount=None, taxable_base=None, vat_amount=None, withholding_amount=None, total_amount=None)
            for (socio, name, variety), data in sorted(grouped.items())
        ]
        return LiquidationCalculationResult(lines=lines, delivery_count=len(deliveries), member_count=len({d.socio for d in deliveries}), variety_count=len({d.variedad for d in deliveries if d.variedad}), net_kg=sum((to_decimal(d.neto) for d in deliveries), Decimal("0")), commercial_amount=sum((l.commercial_amount for l in lines), Decimal("0")), warnings=warnings)

    def _commercial_amount(self, delivery: Delivery, prices: dict[str, Decimal]) -> Decimal:
        values = delivery.extra or {}
        total = Decimal("0")
        for i in range(12):
            total += to_decimal(values.get(f"Cal{i}")) * prices[f"P{i}"]
        total += to_decimal(values.get("DesLinea")) * prices["PDESTRIO"]
        total += to_decimal(values.get("DesMesa")) * prices["PDMESA"]
        total += to_decimal(values.get("Podrido")) * prices["PPODRIDO"]
        return total.quantize(Decimal("0.01"), ROUND_HALF_UP)
