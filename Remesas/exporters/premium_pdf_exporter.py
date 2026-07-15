from __future__ import annotations

from dataclasses import is_dataclass
import logging
import os
import tempfile
from pathlib import Path

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
        if config.get("show_distribution_bar", True):
            build_distribution_bar(c, vm, right_x, 29*MM, right_w)
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

def build_distribution_bar(c, vm, x, y, width):
    gross = vm.gross_amount or 0
    if gross <= 0: return
    parts=[(vm.taxable_base or 0, PRIMARY_COLOR),(abs(vm.collection_amount or 0), NEGATIVE_COLOR),(abs(vm.hectare_fee_amount or 0), "#777777"),(abs((vm.quality_amount or 0)+(vm.transport_amount or 0)+(vm.globalgap_amount or 0)), POSITIVE_COLOR)]
    total=sum(p[0] for p in parts) or gross; xx=x; _font(c,8,True,PRIMARY_COLOR); c.drawString(x,y+12,"Distribución visual del importe bruto")
    for val, color in parts:
        ww=width*float(val/total); c.setFillColor(color); c.rect(xx,y,ww,6,fill=1,stroke=0); xx+=ww
    _font(c,7); c.drawString(x,y-9,"Valor para el socio · Recolección · Cuota Ha · Otros ajustes")

def build_footer(c, vm, config, x, y, width):
    from datetime import datetime
    c.setStrokeColor(PRIMARY_COLOR); c.line(x,y+8,width+x,y+8); _font(c,8); c.drawString(x,y,"S.C.A. San Sebastián · " + str(config.get("footer_message"))); c.drawRightString(x+width,y,f"Generado {datetime.now():%d/%m/%Y} · Página 1 de 1 · {vm.remittance_name[:30]}")
