from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

@dataclass(frozen=True)
class PersistedLiquidationPdfLine:
    id_liq: str; variedad: str; cod_art: int | None; neto: Decimal; imp_bruto: Decimal
    precio_comer: Decimal | None; recoleccion: Decimal; cuota_ha: Decimal; bp_calidad: Decimal
    b_transporte: Decimal; b_global: Decimal; base_i: Decimal; iva: Decimal; retencion: Decimal
    importe_total: Decimal; precio_medio: Decimal | None

@dataclass(frozen=True)
class PersistedLiquidationPdfTotals:
    neto: Decimal; imp_bruto: Decimal; base_i: Decimal; importe_total: Decimal

@dataclass(frozen=True)
class PersistedLiquidationPdfViewModel:
    batch_id: str; remittance_id: int; remittance_name: str; campaign: str; company: str
    crop: str; payment_date: date | None; recipient_member_id: int; recipient_name: str
    id_liqs: tuple[str,...]; lines: tuple[PersistedLiquidationPdfLine,...]
    totals: PersistedLiquidationPdfTotals; liquidation_concept: str = ""; liquidation_type: str = ""
    is_draft: bool = False
