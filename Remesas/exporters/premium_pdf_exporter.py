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


def premium_member_filename(vm: PremiumLiquidationViewModel) -> str:
    return sanitize_filename(f"Liquidacion_Premium_{vm.member_id}_{vm.member_name}_{vm.remittance_name}_{vm.variety_text}") + ".pdf"


def export_premium_member_pdfs(result: LiquidationResult, output_dir: Path, config_path: str | Path = "config/premium_pdf_config.json") -> tuple[Path, ...]:
    logger.info("LiquidationHeader=%s", vars(result.header) if is_dataclass(result.header) else result.header)
    target_dir = output_dir / "socios"
    target_dir.mkdir(parents=True, exist_ok=True)
    return tuple(export_premium_member_pdf(from_member_liquidation(result.header, m), target_dir / premium_member_filename(from_member_liquidation(result.header, m)), config_path) for m in result.member_results)


def export_premium_member_pdf(vm: PremiumLiquidationViewModel, path: Path, config_path: str | Path = "config/premium_pdf_config.json") -> Path:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.pdfgen import canvas
        from reportlab.platypus import Table, TableStyle
    except ImportError as exc:
        raise RuntimeError("reportlab no está instalado. Instale requirements.txt para generar PDF.") from exc
    config = load_premium_pdf_config(config_path)
    if len(vm.commercial_breakdown) > 26:
        logger.error("Premium PDF overflow member=%s rows=%s", vm.member_id, len(vm.commercial_breakdown))
        raise ValueError(OVERFLOW_MESSAGE)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_target_is_writable(path)
    fd, tmp = tempfile.mkstemp(suffix=".pdf", dir=str(path.parent)); os.close(fd)
    try:
        c = canvas.Canvas(tmp, pagesize=landscape(A4), pageCompression=0)
        w, h = landscape(A4); m = 12 * MM; y = h - 10 * MM
        build_header(c, vm, config, m, y, w - 2*m); y -= 36*MM
        build_summary_cards(c, vm, config, m, y, w - 2*m); y -= 27*MM
        left_w = (w - 2*m) * 0.51; right_x = m + left_w + 7*MM; right_w = w - m - right_x
        build_production_table(c, Table, TableStyle, colors, vm, m, y, left_w, config)
        build_economic_breakdown(c, Table, TableStyle, colors, vm, right_x, y, right_w)
        y2 = y - 59*MM
        build_commercial_breakdown(c, Table, TableStyle, colors, vm, m, y2, left_w, config)
        build_tax_summary(c, Table, TableStyle, colors, vm, config, right_x, y2, right_w)
        if vm.group_benchmark:
            build_benchmark_section(c, vm, m, 73*MM, w - 2*m)
        else:
            build_benchmark_unavailable(c, m, 104*MM, w - 2*m)
        if config.get("show_distribution_bar", True):
            build_distribution_bar(c, vm, m, 44*MM, w - 2*m)
        build_footer(c, vm, config, m, 13*MM, w - 2*m)
        c.showPage(); c.save(); os.replace(tmp, path)
    except PermissionError as exc:
        raise FileLockedError(path) from exc
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    return path


def _font(c, size=9, bold=False, color=TEXT_COLOR):
    c.setFont("Helvetica-Bold" if bold else "Helvetica", size); c.setFillColor(color)

def build_header(c, vm, config, x, y, width):
    _font(c, 9, True, PRIMARY_COLOR); c.drawString(x, y, "S.C.A. San Sebastián")
    logo = Path(config.get("logo_path") or "")
    if logo.exists(): c.drawImage(str(logo), x, y-19, width=28, height=18, preserveAspectRatio=True, mask='auto')
    _font(c, 18, True, PRIMARY_COLOR); c.drawString(x, y-30, str(config.get("title", "Liquidación de entrega")).upper())
    cx = x + width * .34; _font(c, 12, True); c.drawString(cx, y, vm.remittance_name[:48])
    _font(c, 9); c.drawString(cx, y-13, f"Campaña {vm.campaign} · {vm.crop} · {vm.variety_text[:36]}")
    c.drawString(cx, y-26, f"Periodo: {vm.period_from} – {vm.period_to}"); c.drawString(cx, y-39, f"Fecha de pago: {vm.payment_date or '—'}")
    _font(c, 13, True); c.drawRightString(x+width, y, f"Socio {vm.member_id:,}".replace(',', '.'))
    _font(c, 10, True); c.drawRightString(x+width, y-15, vm.member_name[:38])
    _font(c, 9); c.drawRightString(x+width, y-28, f"NIF {vm.tax_id_masked}" if vm.tax_id_masked else vm.company[:38])

def build_summary_cards(c, vm, config, x, y, width):
    vals = [("KILOS ENTREGADOS", format_kg(vm.effective_net_kg)), ("KILOS COMERCIALES", format_kg(vm.commercial_net_kg)), ("PRECIO MEDIO FINAL", format_unit_price(vm.final_average_price)), (str(config.get("total_label", "Total a percibir")).upper(), format_money(vm.total_amount))]
    cw = width/4 - 3
    for i,(label,val) in enumerate(vals):
        xx=x+i*(cw+4); accent=i==3; c.setFillColor(ACCENT_COLOR if accent else LIGHT_BACKGROUND); c.roundRect(xx,y-20*MM,cw,20*MM,4,fill=1,stroke=0); c.setStrokeColor(ACCENT_COLOR if accent else PRIMARY_COLOR); c.roundRect(xx,y-20*MM,cw,20*MM,4,fill=0,stroke=1)
        _font(c,8,True); c.drawCentredString(xx+cw/2,y-7,label); _font(c,16 if not accent else 18,True); c.drawCentredString(xx+cw/2,y-25,val)

def _draw_table(c, Table, rows, x, y, widths, style):
    t=Table(rows, colWidths=widths, repeatRows=1); t.setStyle(style); _, th=t.wrapOn(c, sum(widths), 220); t.drawOn(c,x,y-th); return th

def build_production_table(c, Table, TS, colors, vm, x, y, width, config):
    _font(c,11,True,PRIMARY_COLOR); c.drawString(x,y,"RESUMEN DE PRODUCCIÓN")
    rows=[["Producción","Kilos","Precio","Importe"],["Comercial",format_kg(vm.commercial_net_kg),format_unit_price(vm.commercial_average_price),"—"],["Destrío",format_kg(vm.waste_net_kg),"—",format_money(vm.destruction_amount)],["Podrido/Hojas",format_kg(vm.rotten_net_kg),"—",format_money(vm.rotten_amount)],["Total entregado",format_kg(vm.effective_net_kg),"—",format_money(vm.gross_amount)]]
    _draw_table(c, Table, rows, x, y-5, [width*.34,width*.21,width*.22,width*.23], TS([('BACKGROUND',(0,0),(-1,0),colors.HexColor(LIGHT_BACKGROUND)),('TEXTCOLOR',(0,0),(-1,0),colors.HexColor(PRIMARY_COLOR)),('GRID',(0,0),(-1,-1),.25,colors.lightgrey),('FONT',(0,0),(-1,0),'Helvetica-Bold',8.5),('FONT',(0,1),(-1,-1),'Helvetica',8.5),('ALIGN',(1,1),(-1,-1),'RIGHT')]))

def build_economic_breakdown(c, Table, TS, colors, vm, x, y, width):
    _font(c,11,True,PRIMARY_COLOR); c.drawString(x,y,"CÓMO SE FORMA SU LIQUIDACIÓN")
    rows=[["Concepto","Explicación","Importe"],["Importe bruto de la fruta","Valor liquidado de la producción.",format_signed_money(vm.gross_amount, force_positive=True)],["Recolección","Coste de recolección aplicado a sus entregas.",format_signed_money(vm.collection_amount, force_negative=True)],["Cuota por hectárea","Parte proporcional de la cuota anual.",format_signed_money(vm.hectare_fee_amount, force_negative=True)],["Calidad","Bonificación o penalización según calidad.",format_signed_money(vm.quality_amount)],["Transporte","Bonificación o ajuste de transporte.",format_signed_money(vm.transport_amount)],["GlobalGAP","Bonificación asociada a la certificación.",format_signed_money(vm.globalgap_amount)],["Base imponible","Resultado fiscal antes de IVA y retención.",format_money(vm.taxable_base)]]
    _draw_table(c, Table, rows, x, y-5, [width*.34,width*.43,width*.23], TS([('BACKGROUND',(0,0),(-1,0),colors.HexColor(LIGHT_BACKGROUND)),('TEXTCOLOR',(0,0),(-1,0),colors.HexColor(PRIMARY_COLOR)),('GRID',(0,0),(-1,-1),.25,colors.lightgrey),('FONT',(0,0),(-1,0),'Helvetica-Bold',8),('FONT',(0,1),(-1,-1),'Helvetica',7.8),('FONT',(0,-1),(-1,-1),'Helvetica-Bold',8),('ALIGN',(2,1),(2,-1),'RIGHT')]))

def build_commercial_breakdown(c, Table, TS, colors, vm, x, y, width, config):
    if not config.get("show_commercial_breakdown", True) or not vm.commercial_breakdown: return
    _font(c,10,True,PRIMARY_COLOR); c.drawString(x,y,"DESGLOSE COMERCIAL POR CATEGORÍAS")
    rows=[["Categoría/calibre","Kilos","Precio","Importe"]]+[[r.category[:18],format_kg(r.kilograms),format_unit_price(r.price),format_money(r.amount)] for r in vm.commercial_breakdown[:13]]
    _draw_table(c, Table, rows, x, y-5, [width*.34,width*.21,width*.22,width*.23], TS([('BACKGROUND',(0,0),(-1,0),colors.HexColor(LIGHT_BACKGROUND)),('GRID',(0,0),(-1,-1),.25,colors.lightgrey),('FONT',(0,0),(-1,0),'Helvetica-Bold',7.5),('FONT',(0,1),(-1,-1),'Helvetica',7.2),('ALIGN',(1,1),(-1,-1),'RIGHT')]))

def build_tax_summary(c, Table, TS, colors, vm, config, x, y, width):
    _font(c,11,True,PRIMARY_COLOR); c.drawString(x,y,"FISCALIDAD Y RESULTADO FINAL")
    rows=[["Base imponible",format_money(vm.taxable_base)],[f"IVA {format_percent(vm.vat_rate)}",format_signed_money(vm.vat_amount, force_positive=True)],[f"Retención {format_percent(vm.withholding_rate)}",format_signed_money(vm.withholding_amount, force_negative=True)],[str(config.get("total_label", "Total a percibir")).upper(),format_money(vm.total_amount)],["Precio medio final",format_unit_price(vm.final_average_price)]]
    if config.get("show_points_per_kg", True) and vm.final_average_price_pts is not None: rows.append(["Equivalencia", f"{format_decimal_es(vm.final_average_price_pts,2)} pts/kg"])
    _draw_table(c, Table, rows, x, y-5, [width*.55,width*.45], TS([('BOX',(0,0),(-1,-1),.8,colors.HexColor(PRIMARY_COLOR)),('INNERGRID',(0,0),(-1,-1),.25,colors.lightgrey),('BACKGROUND',(0,3),(-1,3),colors.HexColor(ACCENT_COLOR)),('FONT',(0,3),(-1,3),'Helvetica-Bold',12),('FONT',(0,0),(-1,-1),'Helvetica',9),('ALIGN',(1,0),(1,-1),'RIGHT')]))

def _signed_label(value):
    return format_signed_money(value) if value else "—"

def build_distribution_bar(c, vm, x, y, width):
    base_before_tax = vm.taxable_base or Decimal("0")
    collection = vm.collection_amount or Decimal("0")
    hectare_fee = vm.hectare_fee_amount or Decimal("0")
    net_adjustments = (vm.quality_amount or Decimal("0")) + (vm.transport_amount or Decimal("0")) + (vm.globalgap_amount or Decimal("0"))
    parts = [
        ("Base antes de fiscalidad", base_before_tax, DISTRIBUTION_BASE_COLOR, format_money(base_before_tax)),
        ("Recolección", collection, DISTRIBUTION_COLLECTION_COLOR, format_signed_money(collection, force_negative=True)),
        ("Cuota Ha", hectare_fee, DISTRIBUTION_HECTARE_COLOR, format_signed_money(hectare_fee, force_negative=True)),
        ("Ajustes netos", net_adjustments, DISTRIBUTION_POSITIVE_ADJUSTMENT_COLOR if net_adjustments >= 0 else DISTRIBUTION_NEGATIVE_ADJUSTMENT_COLOR, _signed_label(net_adjustments)),
    ]
    total = sum(abs(p[1]) for p in parts)
    if total <= 0: return
    _font(c, 8, True, PRIMARY_COLOR); c.drawString(x, y+25, "DISTRIBUCIÓN DEL IMPORTE BRUTO ANTES DE FISCALIDAD")
    xx = x; bar_y = y + 14; bar_h = 6
    for _, val, color, _ in parts:
        ww = width * float(abs(val) / total)
        c.setFillColor(color); c.rect(xx, bar_y, ww, bar_h, fill=1, stroke=0); xx += ww
    c.setStrokeColor("#D6DEE6"); c.rect(x, bar_y, width, bar_h, fill=0, stroke=1)
    legend = []
    for label, val, _, amount in parts:
        pct = format_decimal_es((abs(val) / total * Decimal("100")), 1) if total else "0,0"
        legend.append(f"{label}: {amount} ({pct} %)")
    _font(c, 6.7, False, TEXT_COLOR); c.drawString(x, y+4, " · ".join(legend)[:170])
    c.drawString(x, y-5, "Los ajustes pueden ser positivos o negativos. No incluye IVA ni retención.")

def build_footer(c, vm, config, x, y, width):
    from datetime import datetime
    c.setStrokeColor(PRIMARY_COLOR); c.line(x,y+8,width+x,y+8); _font(c,8); c.drawString(x,y,"S.C.A. San Sebastián · " + str(config.get("footer_message"))); c.drawRightString(x+width,y,f"Generado {datetime.now():%d/%m/%Y} · Página 1 de 1 · {vm.remittance_name[:30]}")


def _metric_values(metric):
    vals = [metric.own_value, metric.maximum_value, metric.average_value, metric.minimum_value]
    return vals if any(v is not None for v in vals) else None

def _fmt_metric(value, unit):
    if value is None: return "—"
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

def build_benchmark_unavailable(c, x, y, width):
    c.setFillColor(LIGHT_BACKGROUND); c.roundRect(x, y-11*MM, width, 11*MM, 4, fill=1, stroke=0)
    c.setStrokeColor("#D6DEE6"); c.roundRect(x, y-11*MM, width, 11*MM, 4, fill=0, stroke=1)
    _font(c, 8, True, PRIMARY_COLOR); c.drawString(x+4*MM, y-7*MM, "Comparativa con el grupo varietal no disponible para esta liquidación.")

def build_benchmark_section(c, vm, x, y, width):
    from reportlab.graphics import renderPDF
    b = vm.group_benchmark
    _font(c, 9, True, PRIMARY_COLOR); c.drawString(x, y+35*MM, "COMPARATIVA CON SU GRUPO VARIETAL")
    comparable = b.price_per_kg.valid_member_count if b.price_per_kg else 0
    suffix = f" · {comparable} socios comparables" if comparable else ""
    _font(c, 7.4, False, TEXT_COLOR); c.drawString(x, y+31*MM, f"{b.group_label} · Campaña {b.campaign}{suffix}")
    gap = 4*MM; cw = (width-2*gap)/3; ch = 28*MM
    charts = [("PRECIO MEDIO FINAL", "€/kg", b.price_per_kg), ("PRODUCCIÓN", "kg/ha", b.kilograms_per_hectare), ("IMPORTE FINAL", "€/ha", b.euros_per_hectare)]
    for i, (title, unit, metric) in enumerate(charts):
        renderPDF.draw(build_compact_benchmark_chart(title, unit, metric, cw, ch), c, x+i*(cw+gap), y)
