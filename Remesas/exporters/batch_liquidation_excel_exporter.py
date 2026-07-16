from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

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
    dheaders = ["Id Remesa", "Remesa", "Fecha de pago", "Periodo desde", "Periodo hasta", "Campaña", "Empresa", "Cultivo", "Categoría", "Tipo liquidación", "Nº Socio", "Socio", "Variedad o grupo varietal", "Neto", "Importe bruto", "Precio comercial", "Recolección", "Cuota Ha", "Bonificación/Penalización calidad", "Transporte", "GlobalGAP", "Base imponible", "Precio medio", "IVA %", "Importe IVA", "Retención %", "Importe retención", "Importe total", "Concepto liquidación"]
    detail.append(dheaders)
    general = {h: Decimal("0") for h in dheaders[13:28] if "%" not in h and "Precio" not in h}
    for item in results:
        rem = item.remittance
        calc = item.calculation_result.result if hasattr(item.calculation_result, "result") else item.calculation_result
        start_row = detail.max_row + 1
        for m in calc.member_results:
            detail.append([rem.remittance_id, rem.name, rem.payment_date, rem.period_from, rem.period_to, rem.campaign, rem.company, rem.crop, rem.category, rem.liquidation_type, m.member_id, m.member_name, m.variety, _n(m.net_kg), _n(m.gross_amount), _n(m.commercial_average_price), _n(m.collection_amount), _n(m.hectare_fee_amount), _n(m.quality_amount), _n(m.transport_amount), _n(m.globalgap_amount), _n(m.taxable_base), _n(m.final_average_price), _n(m.vat_rate), _n(m.vat_amount), _n(m.withholding_rate), _n(m.withholding_amount), _n(m.total_amount), calc.header.remesa_name])
        subtotal = ["SUBTOTAL REMESA", rem.name, "", "", "", rem.campaign, rem.company, rem.crop, "", "", "", "", "", _sum_members(calc, "net_kg"), _sum_members(calc, "gross_amount"), "", _sum_members(calc, "collection_amount"), _sum_members(calc, "hectare_fee_amount"), _sum_members(calc, "quality_amount"), _sum_members(calc, "transport_amount"), _sum_members(calc, "globalgap_amount"), _sum_members(calc, "taxable_base"), "", "", _sum_members(calc, "vat_amount"), "", _sum_members(calc, "withholding_amount"), _sum_members(calc, "total_amount"), ""]
        detail.append(subtotal)
        _mark_total_row(detail, detail.max_row, "E2F0D9")
        for row in detail.iter_rows(min_row=start_row, max_row=detail.max_row - 1, values_only=True):
            for idx, h in enumerate(dheaders[13:28], start=14):
                if h in general and row[idx - 1] is not None:
                    general[h] += row[idx - 1]
    if results:
        detail.append(["TOTAL GENERAL", "", "", "", "", campaign, company, crop, "", "", "", "", ""] + [general.get(h, "") for h in dheaders[13:28]] + [""])
        _mark_total_row(detail, detail.max_row, "C6E0B4")
    _style_sheet(detail, money_cols=(15,17,18,19,20,21,22,25,27,28), integer_cols=(14,), percent_cols=(24,26), date_cols=(3,4,5))

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
