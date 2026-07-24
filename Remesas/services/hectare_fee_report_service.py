"""Auditable per-boleta view of the existing hectare-fee rules."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import logging

from data.hectare_repository import HectareRepository
from domain.financial_rules import calculate_total_hectare_fee
from domain.hectare_fee_master import HectareFeeMasterRepository
from domain.utils import decimal_or_zero, round_money


@dataclass(frozen=True)
class HectareFeeCropBreakdown:
    crop: str; delivery_count: int; kilograms: Decimal; percentage: Decimal; rate_per_kg: Decimal | None; applied_fee: Decimal

@dataclass(frozen=True)
class HectareFeeSurfaceDetail:
    boleta: str; crop: str; variety: str; polygon: str; parcel: str; enclosure: str; surface: Decimal; cha_active: bool; included: bool; exclusion_reason: str

@dataclass(frozen=True)
class HectareFeeBoletaSummary:
    member_id: int; member_name: str; campaign: str; company: str; boleta: str; surface_hectares: Decimal; price_per_hectare: Decimal; annual_fee: Decimal; total_delivery_kg: Decimal; delivery_crops: tuple[str, ...]; rate_per_kg: Decimal | None; applied_fee: Decimal; pending_fee: Decimal; status: str; warnings: tuple[str, ...] = ()


class HectareFeeReportService:
    """Keeps reporting calculation outside Tk and reuses HectareRepository rules."""
    def __init__(self, repository: HectareRepository, master_repository=None) -> None:
        self.repository = repository; self.master_repository = master_repository or HectareFeeMasterRepository(); self.logger = logging.getLogger(__name__)

    def build_report(self, campaign, company, **filters):
        master = self.master_repository.load(); summaries = []; crop_details = {}; surface_details = {}; incidents = []
        self.logger.info("HECTARE_FEE_REPORT_STARTED campaign=%s company=%s", campaign, company)
        try:
            keys = self.repository.list_fee_report_boletas(campaign, company, filters.get("member_id"), filters.get("boleta"), filters.get("crop"), filters.get("date_from"), filters.get("date_to"))
            # Add surface-only boletas: DEEPP is the authoritative source for that side.
            for row in keys:
                summary, crops, surfaces = self._one(row[0], row[1], row[4], campaign, company, master.eligible_crops, master.price_per_hectare, filters)
                summaries.append(summary); crop_details[self._key(summary)] = crops; surface_details[self._key(summary)] = surfaces
                if summary.status != "CORRECTO": incidents.append((summary.status, summary.member_id, summary.boleta, "; ".join(summary.warnings)))
            for r in self.repository.list_deliveries_without_valid_boleta(campaign, company):
                incidents.append(("SIN BOLETA", r[0], "", f"registro={r[2]}; cultivo={r[5]}; kilos={decimal_or_zero(r[6])}; boleta={r[7]!r}")); self.logger.warning("HECTARE_FEE_DELIVERY_WITHOUT_BOLETA member=%s record=%s", r[0], r[2])
            self.logger.info("HECTARE_FEE_REPORT_COMPLETED rows=%s", len(summaries))
            return tuple(summaries), crop_details, surface_details, tuple(incidents)
        except Exception:
            self.logger.exception("HECTARE_FEE_REPORT_FAILED campaign=%s company=%s", campaign, company); raise

    def _one(self, member_id, name, boleta, campaign, company, eligible, price, filters):
        raw_surfaces = self.repository.get_boleta_surface_details(member_id, boleta, campaign, company, eligible)
        surfaces = tuple(HectareFeeSurfaceDetail(str(boleta), str(a.get("Cultivo", "")), str(a.get("Variedad", "")), str(a.get("Polígono", "")), str(a.get("Parcela", "")), str(a.get("Recinto", "")), decimal_or_zero(dp[9]), bool(a.get("CHA activo") == "Sí"), included, reason or "") for a, included, reason, dp in raw_surfaces)
        area = sum((x.surface for x in surfaces if x.included), Decimal("0")); annual = calculate_total_hectare_fee(area, price)
        deliveries = self.repository.get_boleta_deliveries(member_id, boleta, campaign, company, filters.get("crop"), filters.get("date_from"), filters.get("date_to"))
        grouped: dict[str, list[Decimal]] = {}
        for r in deliveries: grouped.setdefault(str(r[1] or "").strip().upper() or "SIN CULTIVO", []).append(decimal_or_zero(r[4]))
        total = sum((sum(v, Decimal("0")) for v in grouped.values()), Decimal("0")); rate = annual / total if annual and total > 0 else None
        details = self._allocate(grouped, total, annual, rate)
        applied = sum((x.applied_fee for x in details), Decimal("0")); warnings = []
        if not deliveries: status="SIN ENTREGAS"; warnings.append("Boleta con superficie válida sin entregas."); self.logger.warning("HECTARE_FEE_BOLETA_WITHOUT_DELIVERIES member=%s boleta=%s", member_id, boleta)
        elif not area: status="SIN SUPERFICIE VÁLIDA"; warnings.append("Boleta con entregas sin superficie válida."); self.logger.warning("HECTARE_FEE_BOLETA_WITHOUT_SURFACE member=%s boleta=%s", member_id, boleta)
        elif applied != annual: status="CON DIFERENCIAS"; warnings.append("La distribución no cuadra con la cuota anual.")
        else: status="CORRECTO"
        return HectareFeeBoletaSummary(member_id, str(name or ""), str(campaign), str(company), str(boleta), area, price, annual, total, tuple(grouped), rate, applied, annual-applied, status, tuple(warnings)), details, surfaces

    def _allocate(self, grouped, total, annual, rate):
        if not total or not annual or rate is None: return tuple(HectareFeeCropBreakdown(c, len(v), sum(v, Decimal("0")), Decimal("0"), None, Decimal("0")) for c,v in grouped.items())
        result = [HectareFeeCropBreakdown(c, len(v), sum(v, Decimal("0")), sum(v, Decimal("0"))/total, rate, round_money(sum(v, Decimal("0"))*rate)) for c,v in grouped.items()]
        difference = annual - sum((x.applied_fee for x in result), Decimal("0"))
        if difference:
            i = max(range(len(result)), key=lambda n: (result[n].kilograms, result[n].crop)); x=result[i]; result[i]=HectareFeeCropBreakdown(x.crop,x.delivery_count,x.kilograms,x.percentage,x.rate_per_kg,x.applied_fee+difference); self.logger.info("HECTARE_FEE_ROUNDING_ADJUSTMENT crop=%s difference=%s", x.crop,difference)
        return tuple(result)

    @staticmethod
    def _key(s): return (s.member_id, s.boleta, s.campaign, s.company)
