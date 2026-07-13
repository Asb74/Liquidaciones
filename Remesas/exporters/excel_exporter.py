from __future__ import annotations
from pathlib import Path
from domain.calculation_models import LiquidationResult
from domain.utils import format_currency_es, format_decimal_es, format_integer_es, format_price_es, format_percentage_es

def _money(value):
    return format_currency_es(value) if value is not None else "Pendiente"

def _price(value):
    return format_price_es(value) if value is not None else "Pendiente"

def _pct(value):
    return format_percentage_es(value) if value is not None else "Pendiente"


SUMMARY_HEADERS = ["IdSocio","Socio","Variedad","Neto partidas","Neto comercial","Neto destrío","Neto podrido","Importe comercial","Importe destrío","Importe bruto","Recolección","Transporte","Calidad","GlobalGAP","Cuota Ha","Base imponible","% IVA","IVA","% Retención","Retención","Importe total","Precio medio comercial","Precio medio final"]


def export_liquidation_summary(result: LiquidationResult, path: Path) -> Path:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError as exc:
        raise RuntimeError("openpyxl no está instalado. Instale requirements.txt para exportar Excel.") from exc
    wb = Workbook(); ws = wb.active; ws.title = "Resumen"
    ws.append([result.header.remesa_name]); ws.append(SUMMARY_HEADERS)
    for m in result.member_results:
        ws.append([m.member_id,m.member_name,m.variety,format_decimal_es(m.net_kg, 2),format_decimal_es(m.commercial_kg, 2),format_decimal_es(m.destruction_kg + m.table_destruction_kg, 2),format_decimal_es(m.rotten_kg, 2),_money(m.commercial_amount),_money(m.destruction_amount + m.table_destruction_amount),_money(m.gross_amount),_money(m.collection_amount),_money(m.transport_amount),_money(m.quality_amount),_money(m.globalgap_amount),_money(m.hectare_fee_amount),_money(m.taxable_base),_pct(m.vat_rate),_money(m.vat_amount),_pct(m.withholding_rate),_money(m.withholding_amount),_money(m.total_amount),_price(m.commercial_average_price),_price(m.final_average_price)])
    ws.append(["TOTALES", "", "", format_decimal_es(result.totals.net_kg, 2), "", "", "", _money(result.totals.commercial_amount), "", _money(result.totals.gross_amount), _money(result.totals.collection_amount), _money(result.totals.transport_amount), _money(result.totals.quality_amount), _money(result.totals.globalgap_amount), _money(result.totals.hectare_fee_amount), _money(result.totals.taxable_base), "", _money(result.totals.vat_amount), "", _money(result.totals.withholding_amount), _money(result.totals.total_amount), "", ""])
    _style(ws)
    cal = wb.create_sheet("Calibres"); headers=["IdSocio","Socio","Variedad"]
    for grade in (result.member_results[0].grades if result.member_results else []): headers += [f"Kilos {grade.label}", f"Precio {grade.label}", f"Importe {grade.label}"]
    headers += ["Destrío línea","Destrío mesa","Podrido"]; cal.append(headers)
    for m in result.member_results:
        row=[m.member_id,m.member_name,m.variety]
        for g in m.grades: row += [format_decimal_es(g.kg, 2), format_price_es(g.price), format_currency_es(g.amount)]
        row += [format_decimal_es(m.destruction_kg, 2), format_decimal_es(m.table_destruction_kg, 2), format_decimal_es(m.rotten_kg, 2)]; cal.append(row)
    _style(cal)
    costs = wb.create_sheet("Entregas")
    costs.append(["IdSocio","Socio","Variedad","Fecha","Registro","Neto","Coste_Recoleccion","SSocialRecoleccion","Manijeria","Recolección total calculada","Coste_Trans"]);
    for m in result.member_results:
        for d in m.source_deliveries:
            collection = d.collection_cost + d.social_security_collection + d.foreman_cost
            costs.append([m.member_id, m.member_name, m.variety, d.fecha, d.registro, format_decimal_es(d.neto, 2), format_currency_es(d.collection_cost), format_currency_es(d.social_security_collection), format_currency_es(d.foreman_cost), format_currency_es(collection), format_currency_es(d.transport_cost)])
    _style(costs)
    cfg=wb.create_sheet("Configuración")
    for k,v in [("IdREMESA",result.header.remesa_id),("Nombre remesa",result.header.remesa_name),("Campaña",result.header.campana),("Empresa",result.header.empresa),("Cultivo",result.header.cultivo),("Fecha de pago",result.header.fecha_pago),("Periodo",f"{result.header.periodo_desde} - {result.header.periodo_hasta}"),("Tipo de liquidación",result.header.tipo_liquidacion),("Categoría",result.header.categoria),("Socio o todos",result.header.socio),("Variedades",", ".join(result.header.variedades)),("Fecha de generación",result.header.generated_at.strftime("%d/%m/%Y %H:%M"))]: cfg.append([k,v])
    for k,v in result.header.options.items(): cfg.append([k, "Sí" if v else "No"])
    for k,v in result.header.prices.items(): cfg.append([k, format_price_es(v)])
    _style(cfg)
    warn=wb.create_sheet("Advertencias"); warn.append(["Tipo","Socio","Variedad","Mensaje"])
    for msg in result.warnings: warn.append(["Advertencia", "", "", msg])
    _style(warn)
    path.parent.mkdir(parents=True, exist_ok=True); wb.save(path); return path


def _style(ws):
    for cell in ws[1]: cell.font = Font(bold=True)
    if ws.max_row >= 2:
        for cell in ws[2]: cell.font = Font(bold=True)
    ws.freeze_panes = "A3" if ws.title == "Resumen" else "A2"; ws.auto_filter.ref = ws.dimensions
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = min(max(len(str(c.value or "")) for c in col)+2, 35)
