from __future__ import annotations

from dataclasses import dataclass, is_dataclass, replace
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from hashlib import sha256
import json
import logging
import re
from typing import Any

from domain.calculation_models import LiquidationHeader, MemberLiquidation
from services.group_benchmark_service import PremiumGroupBenchmark
from services.calibre_master_service import CalibreMasterService
from services.production_destination_master_service import ProductionDestinationMasterService
from domain.utils import format_decimal_es
from domain.utils import round_price

logger = logging.getLogger(__name__)

PESETA_RATE = Decimal("166.386")

DEFAULT_PREMIUM_PDF_CONFIG = {
    "title": "Liquidación de entrega",
    "show_points_per_kg": True,
    "show_price_references": True,
    "show_distribution_bar": True,
    "show_qr": False,
    "show_commercial_breakdown": True,
    "total_label": "Total a percibir",
    "footer_message": "Gracias por confiar su producción a su Cooperativa.",
    "logo_path": "assets/logo_sansebas.png",
    "generate_combined_premium_pdf": False,
}


@dataclass(frozen=True)
class CommercialBreakdownRow:
    category: str
    kilograms: Decimal
    price: Decimal | None
    amount: Decimal | None


@dataclass(frozen=True)
class PremiumLiquidationViewModel:
    member_id: int
    member_name: str
    tax_id_masked: str | None
    remittance_name: str
    campaign: str
    company: str
    crop: str
    varieties: tuple[str, ...]
    period_from: str
    period_to: str
    payment_date: str | None
    effective_net_kg: Decimal
    commercial_net_kg: Decimal
    waste_net_kg: Decimal
    rotten_net_kg: Decimal
    gross_amount: Decimal
    commercial_amount: Decimal | None
    commercial_average_price: Decimal | None
    destruction_amount: Decimal | None
    destruction_price: Decimal | None
    rotten_amount: Decimal | None
    rotten_price: Decimal | None
    national_market_price: Decimal | None
    rotten_leaves_price: Decimal | None
    gross_average_price: Decimal | None
    commercial_breakdown_title: str
    primary_label: str
    secondary_label: str | None
    waste_label: str
    secondary_enabled: bool
    secondary_counts_as_commercial: bool
    primary_kg: Decimal
    primary_price: Decimal | None
    primary_amount: Decimal
    secondary_kg: Decimal
    secondary_price: Decimal | None
    secondary_amount: Decimal
    waste_kg: Decimal
    waste_price: Decimal | None
    waste_amount: Decimal
    commercial_kg: Decimal
    collection_amount: Decimal | None
    hectare_fee_amount: Decimal | None
    quality_amount: Decimal | None
    transport_amount: Decimal | None
    globalgap_amount: Decimal | None
    taxable_base: Decimal | None
    vat_rate: Decimal | None
    vat_amount: Decimal | None
    withholding_rate: Decimal | None
    withholding_amount: Decimal | None
    total_amount: Decimal | None
    final_average_price: Decimal | None
    final_average_price_pts: Decimal | None
    commercial_breakdown: tuple[CommercialBreakdownRow, ...]
    price_average_reference: Decimal | None = None
    price_max_reference: Decimal | None = None
    price_min_reference: Decimal | None = None
    group_benchmark: PremiumGroupBenchmark | None = None
    id_liqs: tuple[str, ...] = ()
    variety_code: str | None = None
    variety_name: str | None = None
    variety_group_code: str | None = None
    variety_group_name: str | None = None
    applicable_hectares: Decimal | None = None
    surface_group_code: str | None = None
    surface_group_name: str | None = None
    surface_source: str | None = None
    surface_fingerprint: str | None = None

    @property
    def variety_text(self) -> str:
        return ", ".join(v for v in self.varieties if v) or "—"


def mask_tax_id(value: object) -> str | None:
    text = str(value or "").strip().upper().replace(" ", "")
    if not text:
        return None
    if len(text) <= 4:
        return "*" * len(text)
    return text[:2] + "*" * max(2, len(text) - 4) + text[-2:]


def load_premium_pdf_config(path: str | Path = "config/premium_pdf_config.json") -> dict[str, Any]:
    config = dict(DEFAULT_PREMIUM_PDF_CONFIG)
    p = Path(path)
    if p.exists():
        with p.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        for key in DEFAULT_PREMIUM_PDF_CONFIG:
            if key in raw:
                config[key] = raw[key]
    if not str(config["total_label"]).strip():
        config["total_label"] = DEFAULT_PREMIUM_PDF_CONFIG["total_label"]
    return config


def from_member_liquidation(header: LiquidationHeader, member: MemberLiquidation, *, tax_id: object = None, group_benchmark: PremiumGroupBenchmark | None = None) -> PremiumLiquidationViewModel:
    """Adapt a calculated MemberLiquidation to presentation data without economic recalculation.

    MemberLiquidation currently groups the liquidated result by member and variety:
    each item carries one ``member_id`` and one ``variety`` plus its own grade rows.
    The Premium PDF therefore emits one page/file per existing member-variety item.
    """
    logger.info("LiquidationHeader=%s", vars(header) if is_dataclass(header) else header)
    pts = getattr(member, "final_average_price_pts", None)
    if pts is None and member.final_average_price is not None:
        pts = (member.final_average_price * PESETA_RATE).quantize(Decimal("0.01"), ROUND_HALF_UP)
    rows = tuple(
        CommercialBreakdownRow(g.label or g.code, g.kilograms, g.price, g.amount)
        for g in member.grades
        if (g.kilograms or g.amount)
    )


    dest = ProductionDestinationMasterService().get_for_crop(header.cultivo)
    secondary_kg = member.destruction_kg + member.table_destruction_kg
    secondary_amount = member.destruction_amount + member.table_destruction_amount
    try:
        pdestrio = round_price(header.prices["PDESTRIO"])
        pdmesa = round_price(header.prices["PDMESA"])
        ppodrido = round_price(header.prices["PPODRIDO"])
    except (KeyError, TypeError):
        raise ValueError(
            "Falta el precio fijo PPODRIDO/PDESTRIO/PDMESA de la remesa para generar el documento."
        ) from None
    if pdestrio != pdmesa:
        logger.error(
            "[FixedRemittancePriceMismatch] remesa_id=%s PDESTRIO=%s PDMESA=%s",
            header.remesa_id, pdestrio, pdmesa,
        )
        raise ValueError("PDESTRIO y PDMESA no coinciden; no se puede mostrar un precio único de Mercado Nacional.")
    secondary_price = pdestrio
    waste_price = ppodrido
    commercial_kg = member.commercial_kg + (secondary_kg if dest.secondary_enabled and dest.secondary_counts_as_commercial else Decimal("0"))
    logger.info("[ProductionDestination] crop=%s primary_label=%s secondary_enabled=%s secondary_label=%s secondary_counts_as_commercial=%s waste_label=%s", dest.crop, dest.primary_label, dest.secondary_enabled, dest.secondary_label, dest.secondary_counts_as_commercial, dest.waste_label)
    logger.info("[ProductionFixedPrices] remesa_id=%s member_id=%s source=remittance PDESTRIO=%s PDMESA=%s PPODRIDO=%s national_display_price=%s rotten_display_price=%s", header.remesa_id, member.member_id, pdestrio, pdmesa, ppodrido, secondary_price, waste_price)
    logger.info("[ProductionSummary] primary_kg=%s primary_price=%s primary_amount=%s secondary_kg=%s secondary_price=%s secondary_amount=%s waste_kg=%s waste_price=%s waste_amount=%s commercial_kg=%s total_delivered_kg=%s", member.commercial_kg, member.commercial_average_price, member.commercial_amount, secondary_kg, secondary_price, secondary_amount, member.rotten_kg, waste_price, member.rotten_amount, commercial_kg, member.net_kg)
    surface = member.applicable_hectares
    surface_audit = getattr(member, "hectare_fee_audit", None)
    surface_fingerprint = None
    if surface is not None:
        surface_fingerprint = sha256("|".join((str(header.campana), str(header.empresa), str(member.member_id),
            str(surface), ",".join(getattr(surface_audit, "eligible_crops", ())))).encode()).hexdigest()
    return PremiumLiquidationViewModel(
        member_id=member.member_id, member_name=member.member_name, tax_id_masked=mask_tax_id(tax_id),
        remittance_name=header.remesa_name, campaign=str(header.campana), company=header.empresa, crop=header.cultivo,
        varieties=(member.variety,) if member.variety else tuple(header.variedades or ()),
        period_from=header.periodo_desde, period_to=header.periodo_hasta, payment_date=header.fecha_pago or None,
        effective_net_kg=member.net_kg, commercial_net_kg=commercial_kg,
        waste_net_kg=secondary_kg, rotten_net_kg=member.rotten_kg,
        gross_amount=member.gross_amount, commercial_amount=member.commercial_amount, commercial_average_price=member.commercial_average_price,
        destruction_amount=secondary_amount, destruction_price=secondary_price, rotten_amount=member.rotten_amount, rotten_price=waste_price, national_market_price=secondary_price, rotten_leaves_price=waste_price, gross_average_price=(member.gross_amount / member.net_kg if member.net_kg else None), commercial_breakdown_title=CalibreMasterService().commercial_breakdown_title(header.cultivo),
        primary_label=dest.primary_label, secondary_label=dest.secondary_label if dest.secondary_enabled else None, waste_label=dest.waste_label, secondary_enabled=dest.secondary_enabled, secondary_counts_as_commercial=dest.secondary_counts_as_commercial,
        primary_kg=member.commercial_kg, primary_price=member.commercial_average_price, primary_amount=member.commercial_amount, secondary_kg=secondary_kg, secondary_price=secondary_price, secondary_amount=secondary_amount, waste_kg=member.rotten_kg, waste_price=waste_price, waste_amount=member.rotten_amount, commercial_kg=commercial_kg,
        collection_amount=member.collection_amount, hectare_fee_amount=member.hectare_fee_amount,
        quality_amount=member.quality_amount, transport_amount=member.transport_amount, globalgap_amount=member.globalgap_amount,
        taxable_base=member.taxable_base, vat_rate=member.vat_rate, vat_amount=member.vat_amount,
        withholding_rate=member.withholding_rate, withholding_amount=member.withholding_amount,
        total_amount=member.total_amount, final_average_price=member.final_average_price, final_average_price_pts=pts,
        commercial_breakdown=rows, group_benchmark=group_benchmark,
        applicable_hectares=surface,
        surface_source="HECTARE_FEE",
        surface_fingerprint=surface_fingerprint,
    )

def from_persistence_preview(result, preview, *, benchmark_for_member=None) -> dict[int, PremiumLiquidationViewModel]:
    """Build recipient documents from the already allocated persistence lines.

    ``preview.lines`` is deliberately the economic source of truth here.  The
    calculated members are consulted only for the production breakdown which
    is not represented in :class:`SplitPreviewLine`; that breakdown is scaled
    once by the line's allocation factor and never drives fiscal totals.
    """
    from services.split_document_audit import split_document_logger

    audit = split_document_logger()
    sources = {(int(m.member_id), str(m.variety or "")): m for m in result.member_results}
    grouped: dict[int, list[tuple[object, PremiumLiquidationViewModel]]] = {}
    for line in preview.lines:
        audit.info(
            "[SplitDocumentInput]\nremittance_id=%s\nsource_member_id=%s\nrecipient_member_id=%s\n"
            "split_rule_id=%s\nsplit_factor=%s\nnet_kg=%s\ngross_amount=%s\ntaxable_base=%s\ntotal_amount=%s",
            preview.header.remesa_id, line.source_member_id, line.recipient_member_id,
            line.split_rule_id, line.split_factor, line.net_kg, line.gross_amount,
            line.taxable_base, line.total_amount,
        )
        source = sources.get((int(line.source_member_id), str(line.variety or "")))
        if source is None:
            raise ValueError(
                f"No existe el resultado origen {line.source_member_id}/{line.variety} para construir el documento post-split."
            )
        benchmark = benchmark_for_member(source) if benchmark_for_member else None
        grouped.setdefault(int(line.recipient_member_id), []).append(
            (line, from_member_liquidation(result.header, source, group_benchmark=benchmark))
        )

    money_fields = (
        "gross_amount", "collection_amount", "hectare_fee_amount", "quality_amount",
        "transport_amount", "globalgap_amount", "taxable_base", "vat_amount",
        "withholding_amount", "total_amount",
    )
    scaled_fields = (
        "commercial_net_kg", "waste_net_kg", "rotten_net_kg", "commercial_amount",
        "destruction_amount", "rotten_amount", "primary_kg", "primary_amount",
        "secondary_kg", "secondary_amount", "waste_kg", "waste_amount", "commercial_kg",
    )
    documents: dict[int, PremiumLiquidationViewModel] = {}
    for recipient, entries in grouped.items():
        first_line, first_vm = entries[0]
        total = lambda attr: sum((Decimal(getattr(line, attr)) for line, _ in entries), Decimal("0"))
        scaled = lambda attr: sum(
            (Decimal(getattr(vm, attr) or 0) * Decimal(line.split_factor) for line, vm in entries),
            Decimal("0"),
        )
        breakdown: dict[tuple[str, Decimal | None], list[Decimal]] = {}
        for line, vm in entries:
            for row in vm.commercial_breakdown:
                key = (row.category, row.price)
                values = breakdown.setdefault(key, [Decimal("0"), Decimal("0")])
                values[0] += row.kilograms * Decimal(line.split_factor)
                values[1] += Decimal(row.amount or 0) * Decimal(line.split_factor)
        changes = {
            "member_id": recipient,
            "member_name": first_line.recipient_name,
            "varieties": tuple(dict.fromkeys(str(line.variety) for line, _ in entries if line.variety)),
            "effective_net_kg": total("net_kg"),
            "commercial_average_price": (total("gross_amount") / total("net_kg") if total("net_kg") else None),
            "gross_average_price": (total("gross_amount") / total("net_kg") if total("net_kg") else None),
            "final_average_price": total("total_amount") / total("net_kg") if total("net_kg") else None,
            "final_average_price_pts": None,
            "commercial_breakdown": tuple(
                CommercialBreakdownRow(category, values[0], price, values[1])
                for (category, price), values in breakdown.items()
            ),
        }
        changes.update({field: total(field) for field in money_fields})
        changes.update({field: scaled(field) for field in scaled_fields})
        # Fiscal identity always belongs to the final recipient.
        changes.update(vat_rate=first_line.vat_rate, withholding_rate=first_line.withholding_rate)
        documents[recipient] = replace(first_vm, **changes)
    return documents

def format_kg(value: Decimal | None) -> str:
    return "—" if value is None else f"{format_decimal_es(value, 0)} kg"

def format_money(value: Decimal | None) -> str:
    return "—" if value is None else f"{format_decimal_es(value, 2)} €"

def format_unit_price(value: Decimal | None, decimals: int = 5) -> str:
    return "—" if value is None else f"{format_decimal_es(value, decimals)} €/kg"

def format_percent(value: Decimal | None) -> str:
    return "—" if value is None else f"{format_decimal_es(value, 0)} %"

def format_hectares(value: Decimal | None) -> str:
    return "—" if value is None else f"{format_decimal_es(value, 2)} ha"

def format_signed_money(value: Decimal | None, *, force_negative: bool = False, force_positive: bool = False) -> str:
    if value is None or value == 0:
        return "—"
    sign = "−" if (force_negative or value < 0) and not force_positive else "+"
    return f"{sign}{format_money(abs(value))}"


def sanitize_filename(value: object, max_length: int = 120) -> str:
    text = re.sub(r"[<>:\"/\\|?*]+", "_", str(value or "sin_nombre").strip())
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text).strip("._") or "sin_nombre"
    return text[:max_length].rstrip("._")
