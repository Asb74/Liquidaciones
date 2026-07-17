from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from exporters.premium_pdf_exporter import PremiumLiquidationPdfRenderer
from presentation.premium_liquidation_view_model import CommercialBreakdownRow, PremiumLiquidationViewModel, PESETA_RATE


def _premium_view_model(vm):
    """Adapta exclusivamente filas ya persistidas, sin consultar entregas ni Perceco."""
    z = Decimal("0")
    total = lambda name: sum((getattr(line, name) for line in vm.lines), z)
    net = total("neto"); gross = total("imp_bruto")
    collection=total("recoleccion"); hectare=total("cuota_ha"); quality=total("bp_calidad")
    transport=total("b_transporte"); globalgap=total("b_global"); base=total("base_i"); final=total("importe_total")
    vat_rate = vm.lines[0].iva if vm.lines else z
    withholding_rate = vm.lines[0].retencion if vm.lines else z
    # Los importes fiscales se presentan como la diferencia persistida, distribuida
    # por sus tasas; nunca se reconstruye la liquidación ni se modifican sus datos.
    fiscal_delta = final-base
    denom = vat_rate-withholding_rate
    vat_amount = fiscal_delta * vat_rate / denom if denom else z
    withholding_amount = vat_amount-fiscal_delta
    rows=tuple(CommercialBreakdownRow(line.variedad,line.neto,line.precio_comer,line.imp_bruto) for line in vm.lines)
    average = (final/net) if net else None
    return PremiumLiquidationViewModel(
        member_id=vm.recipient_member_id,member_name=vm.recipient_name,tax_id_masked=None,
        remittance_name=vm.remittance_name,campaign=vm.campaign,company=vm.company,crop=vm.crop,
        varieties=tuple(dict.fromkeys(line.variedad for line in vm.lines)),period_from="—",period_to="—",payment_date=str(vm.payment_date or ""),
        effective_net_kg=net,commercial_net_kg=net,waste_net_kg=z,rotten_net_kg=z,gross_amount=gross,
        commercial_amount=gross,commercial_average_price=(gross/net if net else None),destruction_amount=z,destruction_price=None,
        rotten_amount=z,rotten_price=None,gross_average_price=(gross/net if net else None),commercial_breakdown_title="DESGLOSE COMERCIAL",
        primary_label="Producción liquidada",secondary_label=None,waste_label="Mermas",secondary_enabled=False,secondary_counts_as_commercial=False,
        primary_kg=net,primary_price=(gross/net if net else None),primary_amount=gross,secondary_kg=z,secondary_price=None,secondary_amount=z,
        waste_kg=z,waste_price=None,waste_amount=z,commercial_kg=net,collection_amount=collection,hectare_fee_amount=hectare,
        quality_amount=quality,transport_amount=transport,globalgap_amount=globalgap,taxable_base=base,vat_rate=vat_rate,
        vat_amount=vat_amount,withholding_rate=withholding_rate,withholding_amount=withholding_amount,total_amount=final,
        final_average_price=average,final_average_price_pts=(average*PESETA_RATE if average is not None else None),commercial_breakdown=rows,
        id_liqs=vm.id_liqs)


def export_persisted_liquidation_pdf(vm, path: Path) -> Path:
    return PremiumLiquidationPdfRenderer().render(_premium_view_model(vm), path, is_draft=False)
