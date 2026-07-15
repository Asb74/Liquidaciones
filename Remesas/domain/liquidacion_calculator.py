from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Sequence
import logging

from domain.calculation_models import CalculationStatus, FiscalCalculation, GradeBreakdown, GlobalGapAuditData, HectareFeeAuditData, MemberHectareFeeContext, LiquidationCalculationResult, LiquidationHeader, LiquidationLine, LiquidationResult, LiquidationTotals, MemberLiquidation
from domain.financial_rules import applied_amount_or_zero, calculate_quality_adjustment
from domain.hectare_fee import calculate_line_hectare_fee
from domain.models import Delivery, Remesa
from domain.utils import is_liquidated, parse_yes_no, round_money, round_price, to_decimal
from services.calibre_master_service import CalibreMasterService
from domain.audit import current_audit

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


def calculate_taxable_base(
    gross_amount: Decimal,
    collection_amount: Decimal,
    hectare_fee_amount: Decimal,
    quality_amount: Decimal,
    transport_amount: Decimal,
    globalgap_amount: Decimal,
) -> Decimal:
    return round_money(
        gross_amount
        - collection_amount
        - hectare_fee_amount
        + quality_amount
        + transport_amount
        + globalgap_amount
    )


def amount_for_taxable_base(amount: Decimal | None, status: CalculationStatus) -> Decimal | None:
    return applied_amount_or_zero(amount, status)


def calculate_fiscal_result(
    taxable_base: Decimal,
    effective_net_kg: Decimal,
    vat_rate: Decimal,
    withholding_rate: Decimal,
) -> FiscalCalculation:
    """Calculate fiscal totals in the same order as Perceco.

    IVA is applied first. Retention is applied afterwards over the amount after IVA,
    not over the initial taxable base. The total is rounded once after the
    multiplicative sequence, and the final average price uses that final total.
    """
    one = Decimal("1")
    hundred = Decimal("100")
    vat_factor = one + vat_rate / hundred
    withholding_factor = one - withholding_rate / hundred
    raw_amount_after_vat = taxable_base * vat_factor
    raw_total_amount = raw_amount_after_vat * withholding_factor
    amount_after_vat = round_money(raw_amount_after_vat)
    total_amount = round_money(raw_total_amount)
    vat_amount = round_money(raw_amount_after_vat - taxable_base)
    withholding_amount = round_money(raw_amount_after_vat - raw_total_amount)
    final_average_price = round_price(total_amount / effective_net_kg) if effective_net_kg > 0 else None
    return FiscalCalculation(vat_rate, withholding_rate, vat_factor, withholding_factor, raw_amount_after_vat, amount_after_vat, vat_amount, raw_total_amount, withholding_amount, total_amount, final_average_price)


def calculate_vat(taxable_base: Decimal, rate: Decimal) -> Decimal:
    return calculate_fiscal_result(taxable_base, Decimal("0"), rate, Decimal("0")).vat_amount


def calculate_withholding(amount_after_vat: Decimal, rate: Decimal) -> Decimal:
    return round_money(amount_after_vat * rate / Decimal("100"))


def calculate_total(taxable_base: Decimal, vat_amount: Decimal, withholding_amount: Decimal) -> Decimal:
    return round_money(taxable_base + vat_amount - withholding_amount)


class LiquidacionCalculator:
    """Simulación en memoria; no escribe en Access ni en DLiquidaciones."""

    def __init__(self, quality_repository: Any | None = None, hectare_repository: Any | None = None, hectare_config: Any | None = None, globalgap_repository: Any | None = None, fiscal_regime_repository: Any | None = None) -> None:
        self.quality_repository = quality_repository
        self.hectare_repository = hectare_repository
        self.globalgap_repository = globalgap_repository
        self.fiscal_regime_repository = fiscal_regime_repository
        self.hectare_config = hectare_config
        self.hectare_master = None
        self.calibre_master_service = CalibreMasterService()
        self.logger = logging.getLogger(__name__)

    def calculate(self, deliveries: list[Delivery], remesa: Remesa | None) -> LiquidationCalculationResult:
        audit = current_audit()
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
        members: list[MemberLiquidation] = []
        member_indexes: dict[int, list[int]] = defaultdict(list)
        for (socio, name, variety), data in sorted(grouped.items()):
            grades=[]; commercial_amount=Decimal("0"); commercial_kg=Decimal("0")
            for i, kg in enumerate(data["grades"]):
                amount=round_money(kg*prices[f"P{i}"]); commercial_amount += amount; commercial_kg += kg
                price = round_price(prices[f"P{i}"])
                label = self.calibre_master_service.resolve_label(header.cultivo, i)
                self.calibre_master_service.audit_resolution(campaign=header.campana, company=header.empresa, crop=header.cultivo, calibre_index=i, label=label, kilograms=kg, price=price, amount=amount)
                grades.append(GradeBreakdown(f"c{i}", label, kg, price, amount))
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
        members = self._apply_globalgap(members, header, options["GlobalGAP"])
        members = self._apply_hectare_fee(members, header, options["Cuota por hectárea"])
        final_members=[]
        for m in members:
            collection_amount = amount_for_taxable_base(m.collection_amount, m.statuses.get("collection", CalculationStatus.PENDING))
            hectare_fee_amount = amount_for_taxable_base(m.hectare_fee_amount, m.statuses.get("hectare_fee", CalculationStatus.PENDING))
            quality_amount = amount_for_taxable_base(m.quality_amount, m.statuses.get("quality", CalculationStatus.PENDING))
            transport_amount = amount_for_taxable_base(m.transport_amount, m.statuses.get("transport", CalculationStatus.PENDING))
            globalgap_amount = amount_for_taxable_base(m.globalgap_amount, m.statuses.get("globalgap", CalculationStatus.PENDING))
            taxable_inputs = (collection_amount, hectare_fee_amount, quality_amount, transport_amount, globalgap_amount)
            if any(amount is None for amount in taxable_inputs):
                pending_or_error = next((status for status in (m.statuses.get("collection"), m.statuses.get("hectare_fee"), m.statuses.get("quality"), m.statuses.get("transport"), m.statuses.get("globalgap")) if status in (CalculationStatus.ERROR, CalculationStatus.PENDING)), CalculationStatus.PENDING)
                final_members.append(self._replace(m, taxable_base=None, final_average_price=None, statuses={**m.statuses, "taxable_base": pending_or_error}))
                continue
            tb=calculate_taxable_base(m.gross_amount, collection_amount, hectare_fee_amount, quality_amount, transport_amount, globalgap_amount)
            self.logger.info("[BaseImponible] socio=%s gross_amount=%s collection_amount=%s hectare_fee_amount=%s quality_amount=%s transport_amount=%s globalgap_amount=%s expected_taxable_base=%s stored_taxable_base=%s aligned=%s", m.member_id, m.gross_amount, collection_amount, hectare_fee_amount, quality_amount, transport_amount, globalgap_amount, tb, tb, True)
            if not self.fiscal_regime_repository:
                final_members.append(self._replace(m, taxable_base=tb, final_average_price=None, statuses={**m.statuses, "taxable_base": CalculationStatus.CALCULATED, "vat": CalculationStatus.PENDING, "withholding": CalculationStatus.PENDING, "total": CalculationStatus.PENDING}))
                continue
            lookup = self.fiscal_regime_repository.get_for_member(m.member_id)
            fiscal = calculate_fiscal_result(tb, m.net_kg, lookup.regime.vat_rate, lookup.regime.withholding_rate)
            warnings_with_fiscal = (*m.warnings, *lookup.warnings)
            self.logger.info("[Fiscal] socio=%s taxable_base=%s vat_rate=%s vat_factor=%s raw_amount_after_vat=%s amount_after_vat=%s withholding_rate=%s withholding_factor=%s raw_total_amount=%s total_amount=%s effective_net_kg=%s final_average_price=%s preview_total=%s model_total=%s excel_total=%s aligned=%s", m.member_id, tb, fiscal.vat_rate, fiscal.vat_factor, fiscal.raw_amount_after_vat, fiscal.amount_after_vat, fiscal.withholding_rate, fiscal.withholding_factor, fiscal.raw_total_amount, fiscal.total_amount, m.net_kg, fiscal.final_average_price, fiscal.total_amount, fiscal.total_amount, fiscal.total_amount, True)
            self.logger.info("[RégimenFiscal] Socio=%s Regimen=%s IVA=%s Ret=%s Base=%s IVAImporte=%s ImporteTrasIVA=%s RetImporte=%s Total=%s PMedioFinal=%s", m.member_id, lookup.regime.name, fiscal.vat_rate, fiscal.withholding_rate, tb, fiscal.vat_amount, fiscal.amount_after_vat, fiscal.withholding_amount, fiscal.total_amount, fiscal.final_average_price)
            final_members.append(self._replace(m, taxable_base=tb, final_average_price=fiscal.final_average_price, fiscal_regime_name=lookup.regime.name, vat_rate=fiscal.vat_rate, vat_factor=fiscal.vat_factor, vat_amount=fiscal.vat_amount, amount_after_vat=fiscal.amount_after_vat, raw_amount_after_vat=fiscal.raw_amount_after_vat, withholding_rate=fiscal.withholding_rate, withholding_factor=fiscal.withholding_factor, withholding_amount=fiscal.withholding_amount, raw_total_amount=fiscal.raw_total_amount, total_amount=fiscal.total_amount, warnings=warnings_with_fiscal, statuses={**m.statuses, "taxable_base": CalculationStatus.CALCULATED, "vat": CalculationStatus.CALCULATED, "withholding": CalculationStatus.CALCULATED, "total": CalculationStatus.CALCULATED}))
        members=final_members
        def sum_opt(attr: str) -> Decimal | None:
            vals=[getattr(m, attr) for m in members]
            return None if any(v is None for v in vals) else sum(vals, Decimal("0"))
        totals=LiquidationTotals(sum((m.net_kg for m in members), Decimal("0")), sum((m.commercial_amount for m in members), Decimal("0")), sum((m.gross_amount for m in members), Decimal("0")), sum((m.detected_collection_amount for m in members), Decimal("0")), sum_opt("collection_amount"), sum((m.detected_transport_amount for m in members), Decimal("0")), sum_opt("transport_amount"), sum_opt("quality_amount"), sum_opt("globalgap_amount"), sum_opt("hectare_fee_amount"), sum_opt("taxable_base"), sum_opt("vat_amount"), sum_opt("withholding_amount"), sum_opt("total_amount"))
        if totals.taxable_base is not None:
            expected_total_base = calculate_taxable_base(totals.gross_amount, totals.collection_amount or Decimal("0"), totals.hectare_fee_amount or Decimal("0"), totals.quality_amount or Decimal("0"), totals.transport_amount or Decimal("0"), totals.globalgap_amount or Decimal("0"))
            if expected_total_base != totals.taxable_base:
                self.logger.error("[BaseImponible] Alineación global incorrecta expected=%s stored=%s", expected_total_base, totals.taxable_base)
        result=LiquidationResult(header, tuple(members), totals, tuple(dict.fromkeys(w for m in members for w in m.warnings)), self.hectare_master, getattr(self.hectare_master, "fingerprint", ""))
        if audit:
            for _member in result.member_results:
                audit.audit_model(_member)
            audit.audit_result(result)
            audit.audit_final_summary(result.member_results)
        lines=[LiquidationLine(m.member_id,m.member_name,m.variety,m.delivery_count,m.net_kg,m.commercial_amount,m.collection_amount,m.transport_amount,m.quality_amount,m.globalgap_amount,m.hectare_fee_amount,m.taxable_base,m.vat_amount,m.withholding_amount,m.total_amount) for m in members]
        return LiquidationCalculationResult(lines, len(deliveries), len({d.socio for d in deliveries}), len({d.variedad for d in deliveries if d.variedad}), totals.net_kg, totals.commercial_amount, list(result.warnings), result)

    def _replace(self, member: MemberLiquidation, **changes: Any) -> MemberLiquidation:
        from dataclasses import replace
        return replace(member, **changes)

    def _apply_hectare_fee(self, members: list[MemberLiquidation], header: LiquidationHeader, apply_fee: bool) -> list[MemberLiquidation]:
        master = self.hectare_master
        surface_crops = tuple(getattr(master, "surface_crops", getattr(self.hectare_config, "hectare_fee_surface_crops", ("CITRICOS", "MANDARINA"))))
        delivery_crops = tuple(getattr(master, "delivery_crops", getattr(self.hectare_config, "hectare_fee_delivery_crops", ("CITRICOS", "MANDARINA", "DIRECTO", "DIRECTOCHF", "INDUSTRIA"))))
        remittance_crops = delivery_crops
        price = getattr(master, "price_per_hectare", getattr(self.hectare_config, "hectare_fee_price_per_hectare", Decimal("195")))
        crop = header.cultivo.strip().upper()

        if crop not in remittance_crops:
            return [self._replace(m, hectare_fee_price=price, hectare_fee_status=CalculationStatus.NOT_APPLICABLE, hectare_fee_amount=Decimal("0"), hectare_fee_audit=self._hectare_audit_data(surface_crops, delivery_crops, price, Decimal("0"), Decimal("0"), Decimal("0"), None, m.net_kg, Decimal("0"), CalculationStatus.NOT_APPLICABLE, m.warnings), statuses={**m.statuses, "hectare_fee": CalculationStatus.NOT_APPLICABLE}) for m in members]

        result = list(members)
        by_member: dict[int, list[int]] = defaultdict(list)
        for idx, m in enumerate(result):
            by_member[m.member_id].append(idx)

        if not self.hectare_repository:
            return [self._replace(m, warnings=(*m.warnings, "Repositorio DEEPP/PesosFres no disponible; cuota Ha no calculada."), hectare_fee_status=CalculationStatus.ERROR, statuses={**m.statuses, "hectare_fee": CalculationStatus.ERROR}) for m in members]

        for socio, indexes in by_member.items():
            audit = current_audit()
            if audit and indexes:
                audit.audit_member_start(result[indexes[0]])
            hectares, hwarn = self.hectare_repository.calculate_applicable_hectares(socio, header.campana, header.empresa, surface_crops)
            parcel_audit_rows = tuple(getattr(self.hectare_repository, "last_surface_audit_rows", ()))
            total_fee = round_money(hectares * price)
            total_kg = self.hectare_repository.total_effective_kg(socio, header.campana, header.empresa, delivery_crops)
            delivery_audit_rows = tuple(getattr(self.hectare_repository, "last_delivery_audit_rows", ()))
            rate = None if total_kg <= 0 else total_fee / total_kg
            member_context = MemberHectareFeeContext(socio, hectares, total_fee, total_kg, rate, CalculationStatus.PENDING, tuple(hwarn), parcel_audit_rows, delivery_audit_rows)
            for idx in indexes:
                m = result[idx]
                warnings = (*m.warnings, *hwarn)
                diagnostic_state = "CALCULATED"
                if not apply_fee:
                    detected_fee = calculate_line_hectare_fee(m.net_kg, member_context.rate_per_kg) if rate is not None else Decimal("0")
                    result[idx] = self._replace(m, applicable_hectares=hectares, hectare_fee_price=price, hectare_fee_total_member=total_fee, hectare_fee_total_effective_kg=total_kg, hectare_fee_rate_per_kg=rate, hectare_fee_amount=Decimal("0"), hectare_fee_status=CalculationStatus.DISABLED, hectare_fee_rounding_adjustment=Decimal("0"), line_effective_kg=m.net_kg, hectare_fee_parcels=member_context.parcel_audit, hectare_fee_delivery_audit=member_context.delivery_audit, hectare_fee_audit=self._hectare_audit_data(surface_crops, delivery_crops, price, member_context.applicable_hectares, member_context.total_member_fee, member_context.total_effective_kg, member_context.rate_per_kg, m.net_kg, Decimal("0"), CalculationStatus.DISABLED, warnings), warnings=warnings, statuses={**m.statuses, "hectare_fee": CalculationStatus.DISABLED})
                    if audit:
                        self._audit_hectare_member(audit, result[idx], total_fee, total_kg, rate)
                    diagnostic_state = "DISABLED"
                    self.logger.info("Cuota Ha socio=%s hectares=%s annual_fee=%s total_effective_kg=%s rate_per_kg=%s line_effective_kg=%s line_fee=%s status=%s warnings=%s", socio, hectares, total_fee, total_kg, rate, m.net_kg, Decimal("0"), diagnostic_state, "; ".join(warnings))
                    continue
                if hectares <= 0:
                    msg = "Cuota Ha no aplicable: sin superficie sujeta a cuota."
                    result[idx] = self._replace(m, applicable_hectares=Decimal("0"), hectare_fee_price=price, hectare_fee_total_member=Decimal("0"), hectare_fee_total_effective_kg=total_kg, hectare_fee_rate_per_kg=None, hectare_fee_amount=Decimal("0"), hectare_fee_status=CalculationStatus.NOT_APPLICABLE, line_effective_kg=m.net_kg, hectare_fee_parcels=member_context.parcel_audit, hectare_fee_delivery_audit=member_context.delivery_audit, hectare_fee_audit=self._hectare_audit_data(surface_crops, delivery_crops, price, Decimal("0"), Decimal("0"), member_context.total_effective_kg, None, m.net_kg, Decimal("0"), CalculationStatus.NOT_APPLICABLE, (*warnings, msg)), warnings=(*warnings, msg), statuses={**m.statuses, "hectare_fee": CalculationStatus.NOT_APPLICABLE})
                    if audit:
                        self._audit_hectare_member(audit, result[idx], total_fee, total_kg, rate)
                    self.logger.warning("Cuota Ha socio=%s hectares=%s annual_fee=%s total_effective_kg=%s rate_per_kg=%s line_effective_kg=%s line_fee=%s status=NOT_APPLICABLE warnings=%s", socio, hectares, total_fee, total_kg, None, m.net_kg, Decimal("0"), "; ".join((*warnings, msg)))
                    continue
                if total_kg <= 0:
                    msg = "Cuota Ha no calculable: kilos efectivos totales <= 0."
                    result[idx] = self._replace(m, applicable_hectares=hectares, hectare_fee_price=price, hectare_fee_total_member=total_fee, hectare_fee_total_effective_kg=total_kg, hectare_fee_rate_per_kg=None, hectare_fee_amount=None, hectare_fee_status=CalculationStatus.ERROR, line_effective_kg=m.net_kg, hectare_fee_parcels=member_context.parcel_audit, hectare_fee_delivery_audit=member_context.delivery_audit, hectare_fee_audit=self._hectare_audit_data(surface_crops, delivery_crops, price, member_context.applicable_hectares, member_context.total_member_fee, member_context.total_effective_kg, member_context.rate_per_kg, m.net_kg, None, CalculationStatus.ERROR, (*warnings, msg)), warnings=(*warnings, msg), statuses={**m.statuses, "hectare_fee": CalculationStatus.ERROR})
                    if audit:
                        self._audit_hectare_member(audit, result[idx], total_fee, total_kg, None)
                    self.logger.warning("Cuota Ha socio=%s hectares=%s annual_fee=%s total_effective_kg=%s rate_per_kg=%s line_effective_kg=%s line_fee=%s status=ERROR warnings=%s", socio, hectares, total_fee, total_kg, None, m.net_kg, None, "; ".join((*warnings, msg)))
                    continue
                detected_fee = calculate_line_hectare_fee(m.net_kg, member_context.rate_per_kg)
                result[idx] = self._replace(m, applicable_hectares=hectares, hectare_fee_price=price, hectare_fee_total_member=total_fee, hectare_fee_total_effective_kg=total_kg, hectare_fee_rate_per_kg=member_context.rate_per_kg, hectare_fee_amount=detected_fee, hectare_fee_status=CalculationStatus.CALCULATED, hectare_fee_rounding_adjustment=Decimal("0"), line_effective_kg=m.net_kg, hectare_fee_parcels=member_context.parcel_audit, hectare_fee_delivery_audit=member_context.delivery_audit, hectare_fee_audit=self._hectare_audit_data(surface_crops, delivery_crops, price, member_context.applicable_hectares, member_context.total_member_fee, member_context.total_effective_kg, member_context.rate_per_kg, m.net_kg, detected_fee, CalculationStatus.CALCULATED, warnings), warnings=warnings, statuses={**m.statuses, "hectare_fee": CalculationStatus.CALCULATED})
                if audit:
                    self._audit_hectare_member(audit, result[idx], total_fee, total_kg, rate)
                self.logger.info("Cuota Ha socio=%s hectares=%s annual_fee=%s total_effective_kg=%s rate_per_kg=%s line_effective_kg=%s line_fee=%s status=CALCULATED warnings=%s", socio, hectares, total_fee, total_kg, rate, m.net_kg, detected_fee, "; ".join(warnings))
        return result


    def _empty_globalgap_audit(self, member: MemberLiquidation, status: CalculationStatus, warnings=()):
        return GlobalGapAuditData(False, False, (), (), None, None, None, None, None, member.net_kg, member.commercial_kg, None, None, Decimal("0") if status in (CalculationStatus.DISABLED, CalculationStatus.NOT_APPLICABLE) else None, status, tuple(warnings))

    def _apply_globalgap(self, members: list[MemberLiquidation], header: LiquidationHeader, apply_globalgap: bool) -> list[MemberLiquidation]:
        repo = self.globalgap_repository
        if not repo:
            msg = "Repositorio DEEPP/MNivelGlobal/BonGlobal no disponible; GlobalGAP no calculado."
            status = CalculationStatus.ERROR if apply_globalgap else CalculationStatus.DISABLED
            return [self._replace(m, globalgap_amount=Decimal("0") if not apply_globalgap else None, globalgap_audit=self._empty_globalgap_audit(m, status, (*m.warnings, msg)), warnings=(*m.warnings, msg), statuses={**m.statuses, "globalgap": status}) for m in members]

        result = list(members)
        cert_cache = {}
        levels_cache = {}
        index_cache = {}
        rate = repo.get_bonus_rate(header)
        rate_warnings = tuple(rate.warnings)
        by_member: dict[int, list[int]] = defaultdict(list)
        for idx, m in enumerate(result):
            by_member[m.member_id].append(idx)

        for socio, indexes in by_member.items():
            cert = cert_cache.setdefault(socio, repo.get_member_certification(socio, header.campana, header.empresa))
            levels = levels_cache.setdefault(socio, repo.get_member_levels(socio, header.campana, header.empresa)) if cert.certified else ()
            level_result = None
            member_warnings = list(cert.warnings) + list(rate_warnings)
            if not apply_globalgap:
                member_status = CalculationStatus.DISABLED
            elif not cert.certified:
                member_status = CalculationStatus.NOT_APPLICABLE
            elif len(levels) == 0:
                member_status = CalculationStatus.ERROR
                member_warnings.append(f"El socio {socio} está certificado GlobalGAP, pero no tiene NivelGlobal definido.")
            elif len(levels) > 1:
                member_status = CalculationStatus.ERROR
                member_warnings.append(f"El socio {socio} tiene varios niveles GlobalGAP: {', '.join(levels)}. No se ha calculado la bonificación.")
            else:
                level = levels[0]
                level_result = index_cache.setdefault(level.strip().upper(), repo.get_level_index(level))
                member_warnings.extend(level_result.warnings)
                member_status = level_result.status
            if member_status == CalculationStatus.CALCULATED and (rate.bonus_rate is None or rate.category is None):
                member_status = CalculationStatus.ERROR
            if member_status == CalculationStatus.CALCULATED and rate.category not in (0, 1):
                member_status = CalculationStatus.ERROR
                member_warnings.append(f"CATEGORIA GlobalGAP no válida: {rate.category}.")

            for idx in indexes:
                m = result[idx]
                base_type = None
                base_kg = None
                detected = None
                applied = Decimal("0") if member_status in (CalculationStatus.DISABLED, CalculationStatus.NOT_APPLICABLE) else None
                if member_status == CalculationStatus.CALCULATED:
                    # BonGlobal.CATEGORIA: 0 = base NetoComercial; 1 = base NetoEfectivo.
                    base_type = "neto_comercial" if rate.category == 0 else "neto_efectivo"
                    base_kg = m.commercial_kg if rate.category == 0 else m.net_kg
                    raw_amount = (level_result.index or Decimal("0")) * (rate.bonus_rate or Decimal("0")) * base_kg
                    detected = round_money(raw_amount)
                    applied = detected if apply_globalgap else Decimal("0")
                audit_data = GlobalGapAuditData(cert.certified, cert.inconsistent, cert.certified_crops, cert.non_certified_crops, (levels[0] if len(levels)==1 else None), (level_result.index if level_result else None), rate.bonus_rate, rate.category, base_type, m.net_kg, m.commercial_kg, base_kg, detected, applied, member_status, tuple(member_warnings))
                result[idx] = self._replace(m, globalgap_amount=applied, globalgap_audit=audit_data, warnings=(*m.warnings, *member_warnings), statuses={**m.statuses, "globalgap": member_status})
                self.logger.info("[GlobalGAP] socio=%s certified=%s inconsistent=%s certified_crops=%s non_certified_crops=%s level=%s index=%s bonus_rate=%s category=%s base=%s base_kg=%s detected_amount=%s applied_amount=%s status=%s", socio, cert.certified, cert.inconsistent, ','.join(cert.certified_crops), ','.join(cert.non_certified_crops), audit_data.level, audit_data.index, audit_data.bonus_rate, audit_data.category, audit_data.base_type, audit_data.base_kg, audit_data.detected_amount, audit_data.applied_amount, member_status.value)
        return result

    def _hectare_audit_data(self, surface_crops, delivery_crops, price, hectares, total_fee, total_kg, rate, line_kg, line_fee, status, warnings):
        
        rows = tuple(getattr(self.hectare_repository, "last_surface_audit_rows", ()) or ())
        candidate_boletas = len({r.get("Boleta DEEPP") for r in rows if r.get("Boleta DEEPP")})
        active_cha_boletas = len({r.get("Boleta DEEPP") for r in rows if r.get("CHA activo") == "Sí" and r.get("Boleta DEEPP")})
        candidate_parcels = sum(1 for r in rows if r.get("Boleta DParcela"))
        included_parcels = sum(1 for r in rows if r.get("Incluida") == "Sí")
        excluded_parcels = sum(1 for r in rows if r.get("Incluida") == "No")
        young_parcels = sum(1 for r in rows if "PLANTACION_MENOR_CINCO_ANOS" in str(r.get("Motivo exclusión", "")))
        inactive_parcels = sum(1 for r in rows if "PARCELA_DADA_DE_BAJA" in str(r.get("Motivo exclusión", "")))
        delivery_rows = tuple(getattr(self.hectare_repository, "last_delivery_audit_rows", ()) or ())
        kg_by_crop = tuple((crop, sum((row.get("NetoEfectivo") or Decimal("0") for row in delivery_rows if str(row.get("Cultivo", "")).strip().upper() == crop), Decimal("0"))) for crop in delivery_crops)
        reason = next((str(r.get("Motivo exclusión")) for r in rows if r.get("Motivo exclusión")), "")
        return HectareFeeAuditData(tuple(surface_crops), tuple(delivery_crops), price, hectares, total_fee, total_kg, rate, line_kg, line_fee, status, tuple(warnings), candidate_boletas, active_cha_boletas, candidate_parcels, included_parcels, excluded_parcels, young_parcels, inactive_parcels, kg_by_crop, reason)

    def _audit_hectare_member(self, audit: Any, member: MemberLiquidation, total_fee: Decimal, total_kg: Decimal, rate: Decimal | None) -> None:
        audit.subsection("CuotaHa.Superficie")
        parcels = tuple(getattr(member, "hectare_fee_parcels", ()) or ())
        valid_boletas = sorted({str(r.get("Boleta DParcela") or r.get("Boleta DEEPP")) for r in parcels if r.get("Incluida") == "Sí" and (r.get("Boleta DParcela") or r.get("Boleta DEEPP"))})
        audit.line("[CuotaHaSurface]")
        audit.line(f"member_id={member.member_id}")
        audit.line(f"campaign={next((r.get('Campaña DParcela') or r.get('Campaña DEEPP') for r in parcels if r.get('Campaña DParcela') or r.get('Campaña DEEPP')), '')}")
        audit.line(f"company={next((r.get('Empresa DParcela') or r.get('Empresa DEEPP') for r in parcels if r.get('Empresa DParcela') or r.get('Empresa DEEPP')), '')}")
        audit.line(f"eligible_crops={','.join(getattr(member.hectare_fee_audit, 'surface_crops', ())) if member.hectare_fee_audit else ''}")
        audit.line(f"valid_boletas={','.join(valid_boletas)}")
        audit.line(f"physical_rows={sum(1 for r in parcels if r.get('RowId parcela'))}")
        audit.line(f"included_rows={getattr(member.hectare_fee_audit, 'included_parcels', 0)}")
        audit.line(f"excluded_rows={getattr(member.hectare_fee_audit, 'excluded_parcels', 0)}")
        audit.line(f"surface_total={member.applicable_hectares}")
        for row in parcels:
            if not row.get("RowId parcela"):
                continue
            audit.line("[CuotaHaParcelRow]")
            audit.line(f"row_id={row.get('RowId parcela')}")
            audit.line(f"boleta={row.get('Boleta DParcela')}")
            audit.line(f"pol={row.get('Pol')}")
            audit.line(f"par={row.get('Par')}")
            audit.line(f"rec={row.get('Rec')}")
            audit.line(f"year={row.get('Año')}")
            audit.line(f"surface={row.get('SupCul DParcela')}")
            audit.line(f"cha_active={str(row.get('CHA activo') == 'Sí').lower()}")
            audit.line(f"inactive={str(bool(row.get('Baja DParcela'))).lower()}")
            audit.line(f"old_enough={str(row.get('Antigüedad suficiente') == 'Sí').lower()}")
            audit.line(f"included={str(row.get('Incluida') == 'Sí').lower()}")
            audit.line(f"reason={row.get('Motivo') or row.get('Motivo exclusión') or 'VALIDA'}")
        audit.line(f"Precio hectárea: {member.hectare_fee_price}")
        audit.line(f"Cuota anual: {total_fee}")
        audit.subsection("CuotaHa.Prorrateo")
        audit.line(f"{total_fee} / {total_kg} = {rate} €/kg")
        audit.subsection("CuotaHa.Linea")
        audit.line("[CuotaHaSummary]")
        audit.line(f"member_id={member.member_id}")
        audit.line(f"surface_total={member.applicable_hectares}")
        audit.line(f"rate_per_hectare={member.hectare_fee_price}")
        audit.line(f"annual_cost={total_fee}")
        audit.line(f"total_eligible_kg={total_kg}")
        audit.line(f"proportion={rate}")
        audit.line(f"remittance_kg={member.net_kg}")
        audit.line(f"remittance_fee={member.hectare_fee_amount}")
        audit.line("boletas_used_for_decision=False")
        audit.line(f"Número de registros: {member.delivery_count}")
        audit.line(f"Kilos efectivos remesa: {member.net_kg}")
        audit.subsection("CUOTA PARCIAL")
        audit.line(f"{member.net_kg} × {rate} = {member.hectare_fee_amount}")
        audit.console(f"[CuotaHa] Socio {member.member_id} | Superficie: {member.applicable_hectares} | Kg campaña: {member.hectare_fee_total_effective_kg} | Proporción: {member.hectare_fee_rate_per_kg} | Kg remesa: {member.net_kg} | Cuota: {member.hectare_fee_amount}")
