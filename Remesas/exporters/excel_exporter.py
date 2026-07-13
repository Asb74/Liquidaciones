from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils import get_column_letter

from domain.calculation_models import LiquidationResult

logger = logging.getLogger(__name__)

SUMMARY_HEADERS = [
    "Nº Socio",
    "Socio",
    "Variedad",
    "Neto",
    "I. Bruto",
    "P. Comer.",
    "Recolec.",
    "C. Has.",
    "B/P Cal.",
    "B. Trans.",
    "B. Glob.",
    "Base Imponible",
    "P.Medio",
    "I.V.A",
    "Ret.",
    "Importe Total.",
    "Concepto Liquidación",
    "Cultivo",
    "Com. Recol. (Pts/kg)(<11,65)",
    "Com. Global (Pts/kg)(<2)",
    "Com. Trans. (Pts/kg) (<1,7)",
]

INTEGER_FORMAT = '#,##0;-#,##0;-'
MONEY_FORMAT = '#,##0.00;-#,##0.00;-'
PRICE_FORMAT = '0.00000;-0.00000;-'
PTS_KG_FORMAT = '0.00;-0.00;-'
PERCENT_FORMAT = '0"%"'

COLUMN_WIDTHS = {
    "A": 12, "B": 35, "C": 18, "D": 14, "E": 15, "F": 12, "G": 14,
    "H": 14, "I": 14, "J": 14, "K": 14, "L": 17, "M": 12, "N": 10,
    "O": 10, "P": 17, "Q": 32, "R": 16, "S": 24, "T": 23, "U": 24,
}


def _number(value, field_name: str, *, required: bool = False):
    if value is None:
        if required:
            raise ValueError(f"Falta el dato obligatorio: {field_name}")
        logger.warning("Dato no informado para %s; se deja la celda vacía", field_name)
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise ValueError(f"Valor numérico no válido para {field_name}: {value!r}")
    if isinstance(value, (int, float)):
        return value
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Valor numérico no válido para {field_name}: {value!r}") from exc


def _validate_result(result: LiquidationResult) -> None:
    if result is None:
        raise ValueError("No existe un cálculo actual para exportar")
    if not getattr(result, "member_results", None):
        raise ValueError("El cálculo no contiene líneas para exportar")
    for idx, member in enumerate(result.member_results, start=1):
        if member.member_id in (None, ""):
            raise ValueError(f"La línea {idx} no tiene IdSocio")
        if not member.variety:
            raise ValueError(f"La línea {idx} no tiene Variedad")
        _number(member.net_kg, f"Neto línea {idx}", required=True)
        for field_name in (
            "gross_amount", "commercial_average_price", "collection_amount", "hectare_fee_amount",
            "quality_amount", "transport_amount", "globalgap_amount", "taxable_base",
            "final_average_price", "vat_rate", "withholding_rate", "total_amount",
        ):
            _number(getattr(member, field_name), f"{field_name} línea {idx}")


def export_liquidation_summary(result: LiquidationResult, path: Path) -> Path:
    _validate_result(result)

    wb = Workbook()
    ws = wb.active
    ws.title = "Resumen"
    ws.append(SUMMARY_HEADERS)

    for row_number, member in enumerate(result.member_results, start=2):
        ws.append([
            member.member_id,
            member.member_name,
            member.variety,
            _number(member.net_kg, "Neto", required=True),
            _number(member.gross_amount, "I. Bruto"),
            _number(member.commercial_average_price, "P. Comer."),
            _number(member.collection_amount, "Recolec."),
            _number(member.hectare_fee_amount, "C. Has."),
            _number(member.quality_amount, "B/P Cal."),
            _number(member.transport_amount, "B. Trans."),
            _number(member.globalgap_amount, "B. Glob."),
            _number(member.taxable_base, "Base Imponible"),
            _number(member.final_average_price, "P.Medio"),
            _number(member.vat_rate, "I.V.A"),
            _number(member.withholding_rate, "Ret."),
            _number(member.total_amount, "Importe Total."),
            result.header.remesa_name,
            result.header.cultivo,
            f'=IFERROR(166.386*G{row_number}/D{row_number},0)',
            f'=IFERROR(166.386*K{row_number}/D{row_number},0)',
            f'=IFERROR(166.386*J{row_number}/D{row_number},0)',
        ])

    total_row = ws.max_row + 1
    ws.cell(total_row, 2, "TOTAL")
    for column in (4, 5, 7, 8, 9, 10, 11, 12, 16):
        letter = get_column_letter(column)
        ws.cell(total_row, column, f"=SUM({letter}2:{letter}{total_row - 1})")

    _style_summary(ws, total_row)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


def _style_summary(ws, total_row: int) -> None:
    thin = Side(style="thin", color="000000")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border

    for row in ws.iter_rows(min_row=2, max_row=total_row):
        for cell in row:
            cell.alignment = Alignment(vertical="center")

    for cell in ws[total_row]:
        cell.font = Font(bold=True)
        cell.border = border

    number_formats = {
        "A": INTEGER_FORMAT,
        "D": INTEGER_FORMAT,
        "E": MONEY_FORMAT, "G": MONEY_FORMAT, "H": MONEY_FORMAT, "I": MONEY_FORMAT,
        "J": MONEY_FORMAT, "K": MONEY_FORMAT, "L": MONEY_FORMAT, "P": MONEY_FORMAT,
        "F": PRICE_FORMAT, "M": PRICE_FORMAT,
        "N": PERCENT_FORMAT, "O": PERCENT_FORMAT,
        "S": PTS_KG_FORMAT, "T": PTS_KG_FORMAT, "U": PTS_KG_FORMAT,
    }
    for column_letter, number_format in number_formats.items():
        for cell in ws[column_letter][1:]:
            cell.number_format = number_format

    for column_letter, width in COLUMN_WIDTHS.items():
        ws.column_dimensions[column_letter].width = width

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
