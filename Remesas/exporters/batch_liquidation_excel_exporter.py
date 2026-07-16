from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from exporters.excel_exporter import (
    MONEY_FORMAT as SUMMARY_MONEY_FORMAT,
    INTEGER_FORMAT as SUMMARY_INTEGER_FORMAT,
    PERCENT_FORMAT as SUMMARY_PERCENT_FORMAT,
    PRICE_FORMAT,
    PTS_KG_FORMAT,
    SUMMARY_HEADERS,
    build_liquidation_summary_rows,
    get_liquidation_summary_columns,
)

MONEY_FORMAT = '#,##0.00;-#,##0.00;-'
INTEGER_FORMAT = '#,##0;-#,##0;-'
PERCENT_FORMAT = '0"%"'
DATE_FORMAT = 'DD/MM/YYYY'


def _n(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return value


def _sum_members(result, attr):
    total = Decimal("0")
    seen = False
    for member in getattr(result, "member_results", ()):
        value = getattr(member, attr, None)
        if value is not None:
            total += value
            seen = True
    return total if seen else None


def _style_sheet(ws, *, money_cols=(), integer_cols=(), percent_cols=(), date_cols=()):
    header_fill = PatternFill("solid", fgColor="D9EAF7")
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(wrap_text=True, vertical="center")
        cell.border = border
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for row in ws.iter_rows():
        for cell in row:
            cell.border = border
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            if cell.column in money_cols:
                cell.number_format = MONEY_FORMAT
            elif cell.column in integer_cols:
                cell.number_format = INTEGER_FORMAT
            elif cell.column in percent_cols:
                cell.number_format = PERCENT_FORMAT
            elif cell.column in date_cols:
                cell.number_format = DATE_FORMAT
    for column_cells in ws.columns:
        width = min(max(len(str(cell.value or "")) for cell in column_cells) + 2, 45)
        ws.column_dimensions[get_column_letter(column_cells[0].column)].width = max(width, 12)


def _mark_total_row(ws, row: int, color: str):
    for cell in ws[row]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill("solid", fgColor=color)


def _decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _shift_excel_formula(formula: str, row_number: int) -> str:
    replacements = {
        f"G{row_number}": f"I{row_number}",
        f"D{row_number}": f"F{row_number}",
        f"K{row_number}": f"M{row_number}",
        f"J{row_number}": f"L{row_number}",
    }
    shifted = formula
    for old, new in replacements.items():
        shifted = shifted.replace(old, new)
    return shifted


def _append_batch_detail_table(detail, results: Sequence) -> int | None:
    headers = ["Id Remesa", "Remesa", *SUMMARY_HEADERS]
    detail.append(headers)
    columns = get_liquidation_summary_columns()
    totals = {column.key: Decimal("0") for column in columns if column.accumulable}
    has_rows = False

    for item in results:
        rem = item.remittance
        calc = item.calculation_result.result if hasattr(item.calculation_result, "result") else item.calculation_result
        start_row = detail.max_row + 1
        member_results = tuple(getattr(calc, "member_results", ()) or ())
        for offset, row_values in enumerate(build_liquidation_summary_rows(calc, start_row=start_row), start=0):
            excel_row = start_row + offset
            shifted_values = [
                _shift_excel_formula(value, excel_row) if isinstance(value, str) and value.startswith("=") else value
                for value in row_values
            ]
            detail.append([rem.remittance_id, rem.name, *shifted_values])
            has_rows = True
        for column in columns:
            if column.accumulable:
                totals[column.key] += sum((_decimal(getattr(member, column.key, None)) for member in member_results), Decimal("0"))

    if not has_rows:
        return None

    total_row = detail.max_row + 1
    detail.cell(total_row, 4, "TOTAL GENERAL")
    for col_idx, column in enumerate(columns, start=3):
        if column.key in totals:
            detail.cell(total_row, col_idx, totals[column.key])
    total_amount = totals.get("total_amount", Decimal("0"))
    total_net = totals.get("net_kg", Decimal("0"))
    detail.cell(total_row, 15, (total_amount / total_net) if total_net else Decimal("0"))
    _mark_total_row(detail, total_row, "C6E0B4")
    return total_row


def _style_batch_detail_table(ws, total_row: int | None) -> None:
    thin = Side(style="thin", color="000000")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border
    for row in ws.iter_rows(min_row=2, max_row=total_row or ws.max_row):
        for cell in row:
            cell.alignment = Alignment(vertical="center")
            if total_row and cell.row == total_row:
                cell.font = Font(bold=True)
                cell.border = border
    formats = {
        "C": SUMMARY_INTEGER_FORMAT, "F": SUMMARY_INTEGER_FORMAT,
        "G": SUMMARY_MONEY_FORMAT, "I": SUMMARY_MONEY_FORMAT, "J": SUMMARY_MONEY_FORMAT, "K": SUMMARY_MONEY_FORMAT,
        "L": SUMMARY_MONEY_FORMAT, "M": SUMMARY_MONEY_FORMAT, "N": SUMMARY_MONEY_FORMAT, "R": SUMMARY_MONEY_FORMAT,
        "H": PRICE_FORMAT, "O": PRICE_FORMAT,
        "P": SUMMARY_PERCENT_FORMAT, "Q": SUMMARY_PERCENT_FORMAT,
        "U": PTS_KG_FORMAT, "V": PTS_KG_FORMAT, "W": PTS_KG_FORMAT,
    }
    for column_letter, number_format in formats.items():
        for cell in ws[column_letter][1:]:
            cell.number_format = number_format
    widths = {"A": 12, "B": 35, "C": 12, "D": 35, "E": 18, "F": 14, "G": 15, "H": 12, "I": 14, "J": 14, "K": 14, "L": 14, "M": 14, "N": 17, "O": 12, "P": 10, "Q": 10, "R": 17, "S": 32, "T": 16, "U": 24, "V": 23, "W": 24}
    for column_letter, width in widths.items():
        ws.column_dimensions[column_letter].width = width
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

def export_batch_liquidation_summary(results: Sequence, failed_results: Sequence, output_path: Path, *, campaign: str, company: str, crop: str, execution_started_at: datetime, execution_finished_at: datetime) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = "Resumen por remesa"
    headers = ["Id Remesa", "Remesa", "Fecha de pago", "Periodo desde", "Periodo hasta", "Categoría", "Tipo liquidación", "Nº entregas", "Nº socios", "Nº variedades", "Kilos netos", "Importe comercial", "Recolección", "Calidad", "Transporte", "GlobalGAP", "Cuota Ha", "Base imponible", "Importe IVA", "Importe retención", "Importe total", "Estado", "Carpeta de salida"]
    ws.append(headers)
    totals = {h: Decimal("0") for h in headers[10:21]}
    for item in results:
        rem = item.remittance
        calc = item.calculation_result.result if hasattr(item.calculation_result, "result") else item.calculation_result
        t = calc.totals
        row = [rem.remittance_id, rem.name, rem.payment_date, rem.period_from, rem.period_to, rem.category, rem.liquidation_type, item.delivery_count, item.member_count, getattr(calc, "variety_count", ""), _n(t.net_kg), _n(t.commercial_amount), _n(t.collection_amount), _n(t.quality_amount), _n(t.transport_amount), _n(t.globalgap_amount), _n(t.hectare_fee_amount), _n(t.taxable_base), _n(t.vat_amount), _n(t.withholding_amount), _n(t.total_amount), "SUCCESS", str(item.output_directory)]
        ws.append(row)
        for idx, h in enumerate(headers[10:21], start=11):
            if row[idx - 1] is not None:
                totals[h] += row[idx - 1]
    if results:
        ws.append(["TOTAL GENERAL", "", "", "", "", "", "", sum(r.delivery_count for r in results), sum(r.member_count for r in results), "", *[totals[h] for h in headers[10:21]], "", ""])
        _mark_total_row(ws, ws.max_row, "C6E0B4")
    _style_sheet(ws, money_cols=range(12, 22), integer_cols=(8, 9, 10, 11), date_cols=(3, 4, 5))

    detail = wb.create_sheet("Detalle acumulado")
    detail_total_row = _append_batch_detail_table(detail, results)
    _style_batch_detail_table(detail, detail_total_row)
    detail.page_setup.orientation = "landscape"
    detail.page_setup.fitToWidth = 1
    detail.page_setup.fitToHeight = 0
    detail.sheet_properties.pageSetUpPr.fitToPage = True
    detail.page_margins.left = 0.25
    detail.page_margins.right = 0.25
    detail.page_margins.top = 0.5
    detail.page_margins.bottom = 0.5

    inc = wb.create_sheet("Incidencias")
    inc.append(["Id Remesa", "Remesa", "Fase", "Tipo de error", "Mensaje", "Fecha y hora", "Estado"])
    for fail in failed_results:
        inc.append([fail.remittance.remittance_id, fail.remittance.name, fail.phase, fail.error_type, fail.error_message, execution_finished_at, "ERROR"])
    for item in results:
        calc = item.calculation_result.result if hasattr(item.calculation_result, "result") else item.calculation_result
        for warning in getattr(calc, "warnings", ()):
            inc.append([item.remittance.remittance_id, item.remittance.name, "WARNING", "Warning", warning, execution_finished_at, "WARNING"])
    _style_sheet(inc, date_cols=(6,))

    params = wb.create_sheet("Parámetros")
    first_calc = None
    if results:
        first_calc = results[0].calculation_result.result if hasattr(results[0].calculation_result, "result") else results[0].calculation_result
    master = getattr(first_calc, "hectare_fee_master", None)
    params_rows = [("Fecha y hora de inicio", execution_started_at), ("Fecha y hora de fin", execution_finished_at), ("Campaña", campaign), ("Empresa", company), ("Cultivo", crop), ("Usuario", ""), ("Número de remesas seleccionadas", len(results) + len(failed_results)), ("Número correctas", len(results)), ("Número con error", len(failed_results)), ("IDs seleccionados", ", ".join(str(r.remittance.remittance_id) for r in results) + (", " if results and failed_results else "") + ", ".join(str(f.remittance.remittance_id) for f in failed_results)), ("Nombres de las remesas", ", ".join(r.remittance.name for r in results) + (", " if results and failed_results else "") + ", ".join(f.remittance.name for f in failed_results)), ("Tarifa Cuota Ha", getattr(master, "price_per_hectare", "")), ("Cultivos sujetos a Cuota Ha", ", ".join(getattr(master, "eligible_crops", ()))), ("Versión de la aplicación", "Remesas"), ("Ruta base de salida", str(output_path.parent))]
    for row in params_rows:
        params.append(row)
    _style_sheet(params, date_cols=(2,))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        wb.save(output_path)
    except PermissionError:
        raise PermissionError("No se ha podido guardar el resumen acumulado porque el archivo está abierto en Excel. Cierre el archivo y pulse Reintentar.")
    return output_path
