from __future__ import annotations

import logging
import os
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils import get_column_letter

from domain.calculation_models import CalculationStatus, LiquidationResult
from domain.audit import audit_latest_excel_row, current_audit
from exporters.file_lock import FileLockedError, ensure_target_is_writable

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



def _hectare_fee_excel_value(member):
    status = getattr(member, "hectare_fee_status", None)
    if status == CalculationStatus.ERROR:
        logger.warning(
            "Cuota Ha no exportable socio=%s status=%s warnings=%s",
            member.member_id,
            getattr(status, "value", status),
            "; ".join(member.warnings),
        )
        return None
    if status in (CalculationStatus.DISABLED, CalculationStatus.NOT_APPLICABLE):
        return Decimal("0")
    return _number(member.hectare_fee_amount, "C. Has.")

def export_liquidation_summary(result: LiquidationResult, path: Path) -> Path:
    _validate_result(result)

    wb = Workbook()
    ws = wb.active
    ws.title = "Resumen"
    ws.append(SUMMARY_HEADERS)

    for row_number, member in enumerate(result.member_results, start=2):
        hectare_excel_value = _hectare_fee_excel_value(member)
        audit = current_audit()
        if audit:
            audit.audit_excel_row(member, hectare_excel_value)
        else:
            audit_latest_excel_row(member, hectare_excel_value)
        ws.append([
            member.member_id,
            member.member_name,
            member.variety,
            _number(member.net_kg, "Neto", required=True),
            _number(member.gross_amount, "I. Bruto"),
            _number(member.commercial_average_price, "P. Comer."),
            _number(member.collection_amount, "Recolec."),
            hectare_excel_value,
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

    detail = wb.create_sheet("Detalle ajustes")
    detail.append(["Nº Socio", "Socio", "Variedad", "Bon/Pen tarifa", "Fuente tarifa", "Hectáreas", "Precio/ha", "Cuota total socio", "Kilos efectivos totales", "Proporción €/kg", "Cuota parcial", "Ajuste redondeo"])
    for member in result.member_results:
        detail.append([member.member_id, member.member_name, member.variety, _number(member.quality_rate, "Bon/Pen tarifa"), member.quality_source, _number(member.applicable_hectares, "Hectáreas"), _number(member.hectare_fee_price, "Precio/ha"), _number(member.hectare_fee_total_member, "Cuota total socio"), _number(member.hectare_fee_total_effective_kg, "Kilos efectivos totales"), _number(member.hectare_fee_rate_per_kg, "Proporción €/kg"), _number(member.hectare_fee_amount, "Cuota parcial"), _number(member.hectare_fee_rounding_adjustment, "Ajuste redondeo")])


    parcels = wb.create_sheet("02_Parcelas")
    parcel_headers = ["IdSocio", "Nombre socio", "Boleta DEEPP", "Boleta DParcela", "Campaña DEEPP", "Campaña DParcela", "Empresa DEEPP", "Empresa DParcela", "Cultivo DEEPP", "Cultivo DParcela", "CHA", "BAJA DEEPP", "BAJA DParcela", "IdPM", "Pol", "Par", "Recinto DEEPP", "Rec DParcela", "SupCul DEEPP", "SupCul DParcela", "SupApor", "Incluida", "Motivo exclusión", "Clave deduplicación"]
    parcels.append(parcel_headers)
    for member in result.member_results:
        for row in getattr(member, "hectare_fee_parcels", ()):
            parcels.append([
                row.get("IdSocio", member.member_id), member.member_name, row.get("Boleta DEEPP"), row.get("Boleta DParcela"),
                row.get("Campaña DEEPP"), row.get("Campaña DParcela"), row.get("Empresa DEEPP"), row.get("Empresa DParcela"),
                row.get("Cultivo DEEPP"), row.get("Cultivo DParcela"), row.get("CHA"), row.get("BAJA DEEPP"), row.get("BAJA DParcela"),
                row.get("IdPM"), row.get("Pol"), row.get("Par"), row.get("Recinto DEEPP"), row.get("Rec DParcela"),
                _number(row.get("SupCul DEEPP"), "SupCul DEEPP"), _number(row.get("SupCul DParcela"), "SupCul DParcela"), _number(row.get("SupApor"), "SupApor"),
                row.get("Incluida"), row.get("Motivo exclusión"), row.get("Clave deduplicación"),
            ])

    total_row = ws.max_row + 1
    ws.cell(total_row, 2, "TOTAL")
    for column in (4, 5, 7, 8, 9, 10, 11, 12, 16):
        letter = get_column_letter(column)
        ws.cell(total_row, column, f"=SUM({letter}2:{letter}{total_row - 1})")

    _style_summary(ws, total_row)
    diagnostics = wb.create_sheet("04_CuotaHa")
    diagnostics.append(["Nº Socio", "Socio", "Variedad", "Estado", "Hectáreas", "Cuota anual", "Kilos efectivos totales", "Proporción €/kg", "Neto efectivo línea", "Cuota calculada", "Advertencias"])
    for member in result.member_results:
        diagnostics.append([
            member.member_id,
            member.member_name,
            member.variety,
            getattr(member.hectare_fee_status, "value", str(member.hectare_fee_status)),
            _number(member.applicable_hectares, "Hectáreas"),
            _number(member.hectare_fee_total_member, "Cuota anual"),
            _number(member.hectare_fee_total_effective_kg, "Kilos efectivos totales"),
            _number(member.hectare_fee_rate_per_kg, "Proporción €/kg"),
            _number(member.net_kg, "Neto efectivo línea"),
            _number(member.hectare_fee_amount, "Cuota calculada"),
            "; ".join(member.warnings),
        ])

    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_target_is_writable(path)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(prefix=f".{path.stem}_", suffix=path.suffix, dir=path.parent, delete=False) as tmp:
            temp_name = tmp.name
        temp_path = Path(temp_name)
        wb.save(temp_path)
        try:
            os.replace(temp_path, path)
        except PermissionError as exc:
            raise FileLockedError(path) from exc
    finally:
        if temp_name:
            temp_path = Path(temp_name)
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    logger.warning("No se pudo eliminar el temporal de Excel: %s", temp_path)
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
