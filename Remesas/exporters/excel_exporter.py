from __future__ import annotations
from pathlib import Path
from domain.calculation_models import LiquidationResult

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
        ws.append([m.member_id,m.member_name,m.variety,float(m.net_kg),float(m.commercial_kg),float(m.destruction_kg + m.table_destruction_kg),float(m.rotten_kg),float(m.commercial_amount),float(m.destruction_amount + m.table_destruction_amount),float(m.gross_amount),float(m.collection_amount),float(m.transport_amount),float(m.quality_amount),float(m.globalgap_amount),float(m.hectare_fee_amount),float(m.taxable_base),float(m.vat_percent),float(m.vat_amount),float(m.withholding_percent),float(m.withholding_amount),float(m.total_amount),float(m.commercial_average_price),float(m.final_average_price)])
    ws.append(["TOTALES", "", "", float(result.totals.net_kg), "", "", "", float(result.totals.commercial_amount), "", float(result.totals.gross_amount), "", "", "", "", "", float(result.totals.taxable_base), "", float(result.totals.vat_amount), "", float(result.totals.withholding_amount), float(result.totals.total_amount), "", ""])
    _style(ws)
    cal = wb.create_sheet("Calibres"); headers=["IdSocio","Socio","Variedad"]
    for grade in (result.member_results[0].grades if result.member_results else []): headers += [f"Kilos {grade.label}", f"Precio {grade.label}", f"Importe {grade.label}"]
    headers += ["Destrío línea","Destrío mesa","Podrido"]; cal.append(headers)
    for m in result.member_results:
        row=[m.member_id,m.member_name,m.variety]
        for g in m.grades: row += [float(g.kg), float(g.price), float(g.amount)]
        row += [float(m.destruction_kg), float(m.table_destruction_kg), float(m.rotten_kg)]; cal.append(row)
    _style(cal)
    cfg=wb.create_sheet("Configuración")
    for k,v in [("IdREMESA",result.header.remesa_id),("Nombre remesa",result.header.remesa_name),("Campaña",result.header.campana),("Empresa",result.header.empresa),("Cultivo",result.header.cultivo),("Fecha de pago",result.header.fecha_pago),("Periodo",f"{result.header.periodo_desde} - {result.header.periodo_hasta}"),("Tipo de liquidación",result.header.tipo_liquidacion),("Categoría",result.header.categoria),("Socio o todos",result.header.socio),("Variedades",", ".join(result.header.variedades)),("Fecha de generación",result.header.generated_at.strftime("%d/%m/%Y %H:%M"))]: cfg.append([k,v])
    for k,v in result.header.options.items(): cfg.append([k, "Sí" if v else "No"])
    for k,v in result.header.prices.items(): cfg.append([k, float(v)])
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
