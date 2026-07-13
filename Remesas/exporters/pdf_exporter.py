from __future__ import annotations
from pathlib import Path
from domain.calculation_models import LiquidationResult
from domain.utils import format_currency_es, format_decimal_es, format_integer_es, format_price_es


def export_member_pdf(result: LiquidationResult, path: Path) -> Path:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    except ImportError as exc:
        raise RuntimeError("reportlab no está instalado. Instale requirements.txt para generar PDF.") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    styles = getSampleStyleSheet(); story=[]
    for idx, m in enumerate(result.member_results):
        if idx: story.append(PageBreak())
        story.append(Paragraph(f"Liquidación socio {m.member_id} - {m.member_name}", styles["Title"]))
        story.append(Paragraph(f"{result.header.remesa_name} | Campaña {result.header.campana} | Empresa {result.header.empresa} | Cultivo {result.header.cultivo} | Categoría {result.header.categoria} | Tipo {result.header.tipo_liquidacion}", styles["Normal"]))
        story.append(Paragraph(f"Periodo {result.header.periodo_desde} - {result.header.periodo_hasta} | Generado {result.header.generated_at:%d/%m/%Y %H:%M} | Variedad {m.variety}", styles["Normal"])); story.append(Spacer(1, 10))
        story.append(Paragraph("Resumen económico", styles["Heading2"]))
        rows=[["Concepto","Valor"],["Neto partidas",format_integer_es(m.net_kg)],["Neto comercial",format_integer_es(m.commercial_kg)],["Neto destrío",format_integer_es(m.destruction_kg + m.table_destruction_kg)],["Neto podrido y otros",format_integer_es(m.rotten_kg)],["Importe bruto",format_currency_es(m.gross_amount)],["Recolección",format_currency_es(m.collection_amount)],["Transporte",format_currency_es(m.transport_amount)],["Calidad",format_currency_es(m.quality_amount)],["GlobalGAP",format_currency_es(m.globalgap_amount)],["Cuota por hectárea",format_currency_es(m.hectare_fee_amount)],["Base imponible",format_currency_es(m.taxable_base)],["IVA",format_currency_es(m.vat_amount)],["Retención",format_currency_es(m.withholding_amount)],["Importe total",format_currency_es(m.total_amount)],["Precio medio final",format_price_es(m.final_average_price)]]
        story.append(_table(Table, TableStyle, colors, rows)); story.append(Spacer(1, 10))
        story.append(Paragraph("Liquidación comercial", styles["Heading2"]))
        story.append(_table(Table, TableStyle, colors, [["Calibre","Kilos","Precio","Importe"], *[[g.label, format_integer_es(g.kg), format_price_es(g.price), format_currency_es(g.amount)] for g in m.grades]])); story.append(Spacer(1, 10))
        story.append(Paragraph("Destrío", styles["Heading2"]))
        story.append(_table(Table, TableStyle, colors, [["Concepto","Kilos","Importe"],["Destrío línea",format_integer_es(m.destruction_kg),format_currency_es(m.destruction_amount)],["Destrío mesa",format_integer_es(m.table_destruction_kg),format_currency_es(m.table_destruction_amount)],["Podrido",format_integer_es(m.rotten_kg),format_currency_es(m.rotten_amount)]])); story.append(Spacer(1, 10))
        story.append(Paragraph("Liquidación Administración", styles["Heading2"]))
        story.append(_table(Table, TableStyle, colors, [["Base imponible",format_currency_es(m.taxable_base)],["IVA",format_currency_es(m.vat_amount)],["Retención",format_currency_es(m.withholding_amount)],["Importe total",format_currency_es(m.total_amount)]]))
    doc=SimpleDocTemplate(str(path), pagesize=A4, rightMargin=28,leftMargin=28,topMargin=28,bottomMargin=28)
    doc.build(story); return path


def _table(Table, TableStyle, colors, rows):
    t=Table(rows, hAlign="LEFT")
    t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.lightgrey),("GRID",(0,0),(-1,-1),0.25,colors.grey),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    return t
