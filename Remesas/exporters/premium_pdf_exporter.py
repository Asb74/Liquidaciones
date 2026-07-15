from __future__ import annotations

from dataclasses import is_dataclass
import logging
import os
import tempfile
from pathlib import Path

from decimal import Decimal
from domain.calculation_models import LiquidationResult
from exporters.file_lock import FileLockedError, ensure_target_is_writable
from presentation.premium_liquidation_view_model import (
    PremiumLiquidationViewModel, format_kg, format_money, format_percent,
    format_signed_money, format_unit_price, from_member_liquidation,
    load_premium_pdf_config, sanitize_filename, format_decimal_es,
)

logger = logging.getLogger(__name__)

PRIMARY_COLOR = "#1F4E79"
ACCENT_COLOR = "#F4A261"
LIGHT_BACKGROUND = "#F4F6F8"
TEXT_COLOR = "#1F2933"
POSITIVE_COLOR = "#2E7D32"
NEGATIVE_COLOR = "#B23B3B"
BENCHMARK_OWN_COLOR = PRIMARY_COLOR
BENCHMARK_MAX_COLOR = "#81C784"
BENCHMARK_AVERAGE_COLOR = "#F6B26B"
BENCHMARK_MIN_COLOR = "#8F9AA3"
DISTRIBUTION_BASE_COLOR = PRIMARY_COLOR
DISTRIBUTION_COLLECTION_COLOR = "#C97A35"
DISTRIBUTION_HECTARE_COLOR = "#8F9AA3"
DISTRIBUTION_POSITIVE_ADJUSTMENT_COLOR = "#81C784"
DISTRIBUTION_NEGATIVE_ADJUSTMENT_COLOR = "#D98C8C"
ENABLE_QR = False
MM = 72 / 25.4
GENERATE_COMBINED_PREMIUM_PDF = False
LOCKED_PDF_MESSAGE = "No se puede generar el PDF porque el archivo está abierto. Ciérrelo y vuelva a intentarlo."
OVERFLOW_MESSAGE = "No se puede generar la liquidación Premium en una sola hoja porque el desglose contiene demasiadas líneas."



def _visible_commercial_rows(vm: PremiumLiquidationViewModel, max_rows: int = 9):
    rows = [r for r in vm.commercial_breakdown if (r.kilograms and r.kilograms != 0) or (r.amount and r.amount != 0)]
    if len(rows) <= max_rows:
        return rows
    return rows[:max_rows]

def premium_member_filename(vm: PremiumLiquidationViewModel) -> str:
    return sanitize_filename(f"Liquidacion_Premium_{vm.member_id}_{vm.member_name}_{vm.remittance_name}_{vm.variety_text}") + ".pdf"


def export_premium_member_pdfs(result: LiquidationResult, output_dir: Path, config_path: str | Path = "config/premium_pdf_config.json") -> tuple[Path, ...]:
    logger.info("LiquidationHeader=%s", vars(result.header) if is_dataclass(result.header) else result.header)
    target_dir = output_dir / "socios"
    target_dir.mkdir(parents=True, exist_ok=True)
    return tuple(export_premium_member_pdf(from_member_liquidation(result.header, m), target_dir / premium_member_filename(from_member_liquidation(result.header, m)), config_path) for m in result.member_results)


def export_premium_member_pdf(vm: PremiumLiquidationViewModel, path: Path, config_path: str | Path = "config/premium_pdf_config.json") -> Path:
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.platypus import SimpleDocTemplate
    except ImportError as exc:
        raise RuntimeError("reportlab no está instalado. Instale requirements.txt para generar PDF.") from exc
    config = load_premium_pdf_config(config_path)
    if len(vm.commercial_breakdown) > 26:
        logger.error("Premium PDF overflow member=%s rows=%s", vm.member_id, len(vm.commercial_breakdown))
        raise ValueError(OVERFLOW_MESSAGE)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_target_is_writable(path)
    page_size = landscape(A4)
    margin = 7 * MM
    available_width = page_size[0] - 2 * margin
    logger.info("Premium layout member=%s uses natural stacked blocks", vm.member_id)
    fd, tmp = tempfile.mkstemp(suffix=".pdf", dir=str(path.parent)); os.close(fd)
    try:
        doc = SimpleDocTemplate(tmp, pagesize=page_size, leftMargin=margin, rightMargin=margin, topMargin=margin, bottomMargin=margin, pageCompression=0)
        story = build_premium_story(vm, config, available_width)
        doc.build(story)
        data = Path(tmp).read_bytes()
        if data.count(b"/Type /Page\n") > 1:
            raise ValueError(OVERFLOW_MESSAGE)
        os.replace(tmp, path)
    except PermissionError as exc:
        raise FileLockedError(path) from exc
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    return path


def _premium_styles():
    from reportlab.lib.styles import ParagraphStyle
    return {
        "title": ParagraphStyle("premium_title", fontName="Helvetica-Bold", fontSize=18, leading=19, textColor=PRIMARY_COLOR),
        "h": ParagraphStyle("premium_h", fontName="Helvetica-Bold", fontSize=10, leading=11, textColor=PRIMARY_COLOR, spaceAfter=2),
        "small": ParagraphStyle("premium_small", fontName="Helvetica", fontSize=7.4, leading=8.4, textColor=TEXT_COLOR),
        "small_b": ParagraphStyle("premium_small_b", fontName="Helvetica-Bold", fontSize=8, leading=9, textColor=TEXT_COLOR),
        "right": ParagraphStyle("premium_right", fontName="Helvetica", fontSize=8, leading=9, alignment=2, textColor=TEXT_COLOR),
    }


def _table_style(*commands):
    from reportlab.lib import colors
    from reportlab.platypus import TableStyle
    return TableStyle(list(commands), parent=None)


def _section(title, flowables, width):
    from reportlab.platypus import Paragraph, Table, TableStyle
    from reportlab.lib import colors
    st = _premium_styles()
    inner = [Paragraph(title, st["h"])] + flowables
    t = Table([[inner]], colWidths=[width])
    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOX", (0, 0), (-1, -1), .35, colors.HexColor("#D6DEE6")),
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    return t


def build_premium_story(vm, config, width):
    from reportlab.platypus import Spacer, Table, TableStyle
    story = [build_header_flowable(vm, config, width), Spacer(1, 2.5 * MM), build_summary_cards_flowable(vm, config, width), Spacer(1, 2.5 * MM)]
    gap = 5 * MM
    left_w = (width - gap) * .51
    right_w = width - gap - left_w
    left_column = [build_production_section(vm, left_w), Spacer(1, 2 * MM), build_commercial_section(vm, config, left_w)]
    right_column = [build_economic_section(vm, right_w), Spacer(1, 2 * MM), build_tax_section(vm, config, right_w)]
    main = Table([[left_column, right_column]], colWidths=[left_w, right_w])
    main.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0,0), (-1,-1), 0), ("RIGHTPADDING", (0,0), (-1,-1), 0), ("TOPPADDING", (0,0), (-1,-1), 0), ("BOTTOMPADDING", (0,0), (-1,-1), 0)]))
    story += [main, Spacer(1, 2.5 * MM), build_benchmark_flowable(vm, width), Spacer(1, 2 * MM)]
    if config.get("show_distribution_bar", True):
        story.append(build_distribution_flowable(vm, width))
        story.append(Spacer(1, 3 * MM))
    story.append(build_footer_flowable(vm, config, width))
    return story


def build_header_flowable(vm, config, width):
    from reportlab.platypus import Paragraph, Table, TableStyle
    from reportlab.lib import colors
    st = _premium_styles()
    left = [Paragraph("S.C.A. San Sebastián", st["small_b"]), Paragraph(str(config.get("title", "Liquidación de entrega")).upper(), st["title"])]
    center = [Paragraph(f"<b>{vm.remittance_name[:58]}</b>", st["small"]), Paragraph(f"Campaña {vm.campaign} · {vm.crop} · {vm.variety_text[:42]}", st["small"]), Paragraph(f"Periodo: {vm.period_from} – {vm.period_to} · Pago: {vm.payment_date or '—'}", st["small"])]
    right = [Paragraph(f"<b>Socio {vm.member_id:,}</b>".replace(',', '.'), st["right"]), Paragraph(vm.member_name[:42], st["right"]), Paragraph(f"NIF {vm.tax_id_masked}" if vm.tax_id_masked else vm.company[:38], st["right"])]
    t = Table([[left, center, right]], colWidths=[width*.32, width*.43, width*.25])
    t.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP"), ("BOTTOMPADDING", (0,0), (-1,-1), 0), ("TOPPADDING", (0,0), (-1,-1), 0), ("TEXTCOLOR", (0,0), (-1,-1), colors.HexColor(TEXT_COLOR))]))
    return t


def build_summary_cards_flowable(vm, config, width):
    from reportlab.platypus import Paragraph, Table, TableStyle
    from reportlab.lib import colors
    st = _premium_styles()
    vals = [("KILOS ENTREGADOS", format_kg(vm.effective_net_kg)), ("KILOS COMERCIALES", format_kg(vm.commercial_net_kg)), ("PRECIO MEDIO FINAL", format_unit_price(vm.final_average_price)), (str(config.get("total_label", "Total a percibir")).upper(), format_money(vm.total_amount))]
    cells = [[Paragraph(f"<b>{lab}</b><br/><font size='15'><b>{val}</b></font>", st["small"])] for lab, val in vals]
    card_w = (width - 9 * MM) / 4
    t = Table([cells], colWidths=[card_w] * 4)
    t.setStyle(TableStyle([("ALIGN", (0,0), (-1,-1), "CENTER"), ("VALIGN", (0,0), (-1,-1), "MIDDLE"), ("BACKGROUND", (0,0), (2,0), colors.HexColor(LIGHT_BACKGROUND)), ("BACKGROUND", (3,0), (3,0), colors.HexColor(ACCENT_COLOR)), ("BOX", (0,0), (-1,-1), .5, colors.HexColor(PRIMARY_COLOR)), ("INNERGRID", (0,0), (-1,-1), 3 * MM, colors.white), ("ROUNDEDCORNERS", [6,6,6,6]), ("TOPPADDING", (0,0), (-1,-1), 5), ("BOTTOMPADDING", (0,0), (-1,-1), 5)]))
    return t


def _rl_table(rows, widths, font=8, header=True, accent_row=None):
    from reportlab.platypus import Table, TableStyle
    from reportlab.lib import colors
    t = Table(rows, colWidths=widths, repeatRows=1 if header else 0)
    cmds = [("GRID", (0,0), (-1,-1), .25, colors.lightgrey), ("FONT", (0,0), (-1,-1), "Helvetica", font), ("VALIGN", (0,0), (-1,-1), "MIDDLE"), ("ALIGN", (1,1), (-1,-1), "RIGHT"), ("TOPPADDING", (0,0), (-1,-1), 1.4), ("BOTTOMPADDING", (0,0), (-1,-1), 1.4)]
    if header: cmds += [("BACKGROUND", (0,0), (-1,0), colors.HexColor(LIGHT_BACKGROUND)), ("TEXTCOLOR", (0,0), (-1,0), colors.HexColor(PRIMARY_COLOR)), ("FONT", (0,0), (-1,0), "Helvetica-Bold", font)]
    if accent_row is not None: cmds += [("BACKGROUND", (0,accent_row), (-1,accent_row), colors.HexColor(ACCENT_COLOR)), ("FONT", (0,accent_row), (-1,accent_row), "Helvetica-Bold", max(font, 9))]
    t.setStyle(TableStyle(cmds)); return t


def build_production_section(vm, width):
    rows=[["Producción","Kilos","Precio","Importe"],["Comercial",format_kg(vm.commercial_net_kg),format_unit_price(vm.commercial_average_price),"—"],["Destrío",format_kg(vm.waste_net_kg),"—",format_money(vm.destruction_amount)],["Podrido/Hojas",format_kg(vm.rotten_net_kg),"—",format_money(vm.rotten_amount)],["Total entregado",format_kg(vm.effective_net_kg),"—",format_money(vm.gross_amount)]]
    return _section("RESUMEN DE PRODUCCIÓN", [_rl_table(rows, [width*.34,width*.21,width*.22,width*.23])], width)

def build_economic_section(vm, width):
    rows=[["Concepto","Explicación","Importe"],["Importe bruto","Valor liquidado de producción.",format_signed_money(vm.gross_amount, force_positive=True)],["Recolección","Coste aplicado a entregas.",format_signed_money(vm.collection_amount, force_negative=True)],["Cuota Ha","Parte proporcional anual.",format_signed_money(vm.hectare_fee_amount, force_negative=True)],["Calidad","Bonificación/penalización.",format_signed_money(vm.quality_amount)],["Transporte","Bonificación o ajuste.",format_signed_money(vm.transport_amount)],["GlobalGAP","Bonificación certificación.",format_signed_money(vm.globalgap_amount)],["Base imponible","Antes de IVA y retención.",format_money(vm.taxable_base)]]
    return _section("CÓMO SE FORMA SU LIQUIDACIÓN", [_rl_table(rows, [width*.32,width*.43,width*.25], font=7.4)], width)

def build_commercial_section(vm, config, width):
    rows=[["Categoría/calibre","Kilos","Precio","Importe"]]+[[r.category[:18],format_kg(r.kilograms),format_unit_price(r.price),format_money(r.amount)] for r in _visible_commercial_rows(vm)]
    return _section("DESGLOSE COMERCIAL POR CATEGORÍAS", [_rl_table(rows, [width*.34,width*.21,width*.22,width*.23], font=7.6)], width)

def build_tax_section(vm, config, width):
    rows=[["Base imponible",format_money(vm.taxable_base)],[f"IVA {format_percent(vm.vat_rate)}",format_signed_money(vm.vat_amount, force_positive=True)],[f"Retención {format_percent(vm.withholding_rate)}",format_signed_money(vm.withholding_amount, force_negative=True)],[str(config.get("total_label", "Total a percibir")).upper(),format_money(vm.total_amount)],["Precio medio final",format_unit_price(vm.final_average_price)]]
    if config.get("show_points_per_kg", True) and vm.final_average_price_pts is not None: rows.append(["Equivalencia", f"{format_decimal_es(vm.final_average_price_pts,2)} pts/kg"])
    return _section("FISCALIDAD Y RESULTADO FINAL", [_rl_table(rows, [width*.55,width*.45], font=8.1, header=False, accent_row=3)], width)


def build_benchmark_flowable(vm, width):
    from reportlab.platypus import Paragraph, Table, TableStyle
    st = _premium_styles()
    if not vm.group_benchmark:
        return _section("COMPARATIVA CON SU GRUPO VARIETAL", [Paragraph("Comparativa con el grupo varietal no disponible para esta liquidación.", st["small"])], width)
    b = vm.group_benchmark
    comparable = b.price_per_kg.valid_member_count if b.price_per_kg else 0
    subtitle = Paragraph(f"{b.group_label} · Campaña {b.campaign}" + (f" · {comparable} socios comparables" if comparable else ""), st["small"])
    gap = 2 * MM
    card_w = (width - 2 * gap) / 3
    chart_h = 23 * MM
    charts = [
        build_compact_benchmark_chart("PRECIO MEDIO FINAL", "€/kg", b.price_per_kg, card_w, chart_h),
        build_compact_benchmark_chart("PRODUCCIÓN", "kg/ha", b.kilograms_per_hectare, card_w, chart_h),
        build_compact_benchmark_chart("IMPORTE FINAL", "€/ha", b.euros_per_hectare, card_w, chart_h),
    ]
    table = Table([charts], colWidths=[card_w] * 3)
    table.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return _section("COMPARATIVA CON SU GRUPO VARIETAL", [subtitle, table], width)

def build_distribution_flowable(vm, width):
    from reportlab.graphics.shapes import Drawing, Rect, String
    from reportlab.lib import colors
    from reportlab.platypus import Paragraph, Table, TableStyle
    st=_premium_styles(); base=vm.taxable_base or Decimal("0"); collection=vm.collection_amount or Decimal("0"); hectare=vm.hectare_fee_amount or Decimal("0"); adj=(vm.quality_amount or Decimal("0"))+(vm.transport_amount or Decimal("0"))+(vm.globalgap_amount or Decimal("0"))
    parts=[("Base antes de fiscalidad", base, DISTRIBUTION_BASE_COLOR, format_money(base)), ("Recolección", collection, DISTRIBUTION_COLLECTION_COLOR, format_signed_money(collection, force_negative=True)), ("Cuota Ha", hectare, DISTRIBUTION_HECTARE_COLOR, format_signed_money(hectare, force_negative=True)), ("Ajustes", adj, DISTRIBUTION_POSITIVE_ADJUSTMENT_COLOR if adj>=0 else DISTRIBUTION_NEGATIVE_ADJUSTMENT_COLOR, _signed_label(adj))]
    total=sum(abs(p[1]) for p in parts) or Decimal("1")
    d=Drawing(width-8, 9); x=0
    for _, val, color, _ in parts:
        ww=(width-8)*float(abs(val)/total); d.add(Rect(x, 1, ww, 6, fillColor=colors.HexColor(color), strokeColor=None)); x+=ww
    d.add(Rect(0,1,width-8,6,fillColor=None,strokeColor=colors.HexColor("#D6DEE6"),strokeWidth=.5))
    legend=[]
    for label,val,_,amount in parts:
        pct=format_decimal_es((abs(val)/total*Decimal("100")),1); legend.append(f"{label}: {amount} ({pct} %)")
    return _section("DISTRIBUCIÓN DEL IMPORTE BRUTO ANTES DE FISCALIDAD", [d, Paragraph(" · ".join(legend), st["small"])], width)


def build_footer_flowable(vm, config, width):
    from datetime import datetime
    from reportlab.platypus import Paragraph, Table, TableStyle
    from reportlab.lib import colors
    st=_premium_styles(); left=Paragraph("S.C.A. San Sebastián · " + str(config.get("footer_message")), st["small"]); right=Paragraph(f"Generado {datetime.now():%d/%m/%Y} · Página 1 de 1 · {vm.remittance_name[:30]}", st["right"])
    t=Table([[left,right]], colWidths=[width*.55,width*.45])
    t.setStyle(TableStyle([("LINEABOVE", (0,0), (-1,0), .5, colors.HexColor(PRIMARY_COLOR)), ("VALIGN", (0,0), (-1,-1), "MIDDLE"), ("TOPPADDING", (0,0), (-1,-1), 1)])); return t

def _signed_label(value):
    return format_signed_money(value) if value else "—"

def _metric_values(metric):
    vals = [metric.own_value, metric.maximum_value, metric.average_value, metric.minimum_value]
    return vals if any(v is not None for v in vals) else None

def _fmt_metric(value, unit):
    if value is None: return "No disponible"
    if unit == "€/kg": return format_decimal_es(value, 5)
    return format_decimal_es(value, 0)

def _benchmark_difference_text(metric):
    if metric.own_value is None or metric.average_value in (None, 0): return ""
    diff = (metric.own_value - metric.average_value) / metric.average_value * Decimal("100")
    if abs(diff) < Decimal("0.05"): return "Igual que la media"
    where = "sobre la media" if diff > 0 else "por debajo de la media"
    return f"{format_decimal_es(abs(diff), 1)} % {where}"

def build_compact_benchmark_chart(title: str, unit: str, metric, width: float, height: float):
    from reportlab.graphics.shapes import Drawing, String, Rect, Line
    from reportlab.lib import colors
    d = Drawing(width, height)
    d.add(Rect(0, 0, width, height, rx=4, ry=4, fillColor=colors.HexColor("#FFFFFF"), strokeColor=colors.HexColor("#D6DEE6"), strokeWidth=.6))
    d.add(String(width/2, height-9, title.upper(), textAnchor="middle", fontName="Helvetica-Bold", fontSize=7.6, fillColor=colors.HexColor(PRIMARY_COLOR)))
    d.add(String(width/2, height-18, unit, textAnchor="middle", fontName="Helvetica", fontSize=7.0, fillColor=colors.HexColor(TEXT_COLOR)))
    vals = _metric_values(metric)
    if vals is None:
        d.add(String(width/2, height/2-2, "No disponible", textAnchor="middle", fontName="Helvetica-Bold", fontSize=8, fillColor=colors.HexColor(TEXT_COLOR)))
        if metric.warning:
            d.add(String(width/2, height/2-12, metric.warning[:62], textAnchor="middle", fontName="Helvetica", fontSize=5.8, fillColor=colors.HexColor(TEXT_COLOR)))
        return d
    labels = ["Usted", "Máximo", "Media", "Mínimo"]
    palette = [BENCHMARK_OWN_COLOR, BENCHMARK_MAX_COLOR, BENCHMARK_AVERAGE_COLOR, BENCHMARK_MIN_COLOR]
    numeric = [float(v) for v in vals if v is not None]
    maxv = max(numeric) if numeric else 0
    chart_x = 12; chart_y = 22; chart_w = width - 24; max_h = height - 47; bar_w = min(12, chart_w/7)
    step = chart_w / 4
    for i, v in enumerate(vals):
        cx = chart_x + step*i + step/2
        if v is None:
            d.add(String(cx, chart_y+max_h/2, "No disponible", textAnchor="middle", fontName="Helvetica", fontSize=5.5, fillColor=colors.HexColor(TEXT_COLOR)))
        else:
            fv = float(v)
            bh = 0 if maxv <= 0 or fv <= 0 else max(2.5*MM, (fv/maxv)*max_h)
            if bh <= 0:
                d.add(Line(cx-bar_w/2, chart_y, cx+bar_w/2, chart_y, strokeColor=colors.HexColor(palette[i]), strokeWidth=1))
            else:
                d.add(Rect(cx-bar_w/2, chart_y, bar_w, bh, fillColor=colors.HexColor(palette[i]), strokeColor=None))
            d.add(String(cx, min(height-21, chart_y+bh+3), _fmt_metric(v, unit), textAnchor="middle", fontName="Helvetica-Bold" if i == 0 else "Helvetica", fontSize=7.2 if i == 0 else 6.8, fillColor=colors.HexColor(TEXT_COLOR)))
        d.add(String(cx, 10, labels[i], textAnchor="middle", fontName="Helvetica", fontSize=7.5, fillColor=colors.HexColor(TEXT_COLOR)))
    diff = _benchmark_difference_text(metric)
    if diff:
        d.add(String(width/2, 2.5, diff, textAnchor="middle", fontName="Helvetica", fontSize=6.8, fillColor=colors.HexColor(TEXT_COLOR)))
    return d
