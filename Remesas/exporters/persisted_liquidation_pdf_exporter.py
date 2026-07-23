from __future__ import annotations

from decimal import Decimal
import logging
from pathlib import Path

from domain.document_models import LiquidationDocumentMode
from exporters.premium_pdf_exporter import PremiumLiquidationPdfRenderer
from presentation.persisted_liquidation_pdf_view_model import PersistedLiquidationPdfViewModel
from presentation.premium_liquidation_view_model import CommercialBreakdownRow, PremiumLiquidationViewModel, PESETA_RATE

logger = logging.getLogger(__name__)


def build_premium_view_model_from_persisted(
    persisted_vm: PersistedLiquidationPdfViewModel,
) -> PremiumLiquidationViewModel:
    """Adapt legacy persisted rows before they reach the Premium renderer.

    Snapshots already deserialize to ``PremiumLiquidationViewModel`` and must not
    pass through this adapter: they intentionally do not expose persisted lines.
    """
    z = Decimal("0")
    total = lambda name: sum((getattr(line, name) for line in persisted_vm.lines), z)
    net = total("neto"); gross = total("imp_bruto")
    collection = total("recoleccion"); hectare = total("cuota_ha"); quality = total("bp_calidad")
    transport = total("b_transporte"); globalgap = total("b_global"); base = total("base_i"); final = total("importe_total")
    vat_rate = persisted_vm.lines[0].iva if persisted_vm.lines else z
    withholding_rate = persisted_vm.lines[0].retencion if persisted_vm.lines else z
    fiscal_delta = final - base
    denom = vat_rate - withholding_rate
    vat_amount = fiscal_delta * vat_rate / denom if denom else z
    withholding_amount = vat_amount - fiscal_delta
    rows = tuple(CommercialBreakdownRow(line.variedad, line.neto, line.precio_comer, line.imp_bruto) for line in persisted_vm.lines)
    average = (final / net) if net else None
    return PremiumLiquidationViewModel(
        member_id=persisted_vm.recipient_member_id, member_name=persisted_vm.recipient_name, tax_id_masked=None,
        remittance_name=persisted_vm.remittance_name, campaign=persisted_vm.campaign, company=persisted_vm.company, crop=persisted_vm.crop,
        varieties=tuple(dict.fromkeys(line.variedad for line in persisted_vm.lines)), period_from="—", period_to="—", payment_date=str(persisted_vm.payment_date or ""),
        effective_net_kg=net, commercial_net_kg=net, waste_net_kg=z, rotten_net_kg=z, gross_amount=gross,
        commercial_amount=gross, commercial_average_price=(gross / net if net else None), destruction_amount=z, destruction_price=None,
        rotten_amount=z, rotten_price=None, gross_average_price=(gross / net if net else None), commercial_breakdown_title="DESGLOSE COMERCIAL",
        primary_label="Producción liquidada", secondary_label=None, waste_label="Mermas", secondary_enabled=False, secondary_counts_as_commercial=False,
        primary_kg=net, primary_price=(gross / net if net else None), primary_amount=gross, secondary_kg=z, secondary_price=None, secondary_amount=z,
        waste_kg=z, waste_price=None, waste_amount=z, commercial_kg=net, collection_amount=collection, hectare_fee_amount=hectare,
        quality_amount=quality, transport_amount=transport, globalgap_amount=globalgap, taxable_base=base, vat_rate=vat_rate,
        vat_amount=vat_amount, withholding_rate=withholding_rate, withholding_amount=withholding_amount, total_amount=final,
        final_average_price=average, final_average_price_pts=(average * PESETA_RATE if average is not None else None), commercial_breakdown=rows,
        id_liqs=persisted_vm.id_liqs,
    )


def export_persisted_liquidation_pdf(vm: PremiumLiquidationViewModel, path: Path) -> Path:
    """Render a final PDF from the already prepared Premium view model."""
    logger.debug("[PersistedPdfExporter] input_type=%s", type(vm).__name__)
    if not isinstance(vm, PremiumLiquidationViewModel):
        raise TypeError("export_persisted_liquidation_pdf requires PremiumLiquidationViewModel")
    return PremiumLiquidationPdfRenderer().render(vm, path, document_mode=LiquidationDocumentMode.FINAL)
