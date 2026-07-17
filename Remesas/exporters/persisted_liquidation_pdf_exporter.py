from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen.canvas import Canvas
from presentation.persisted_liquidation_pdf_view_model import PersistedLiquidationPdfViewModel

def export_persisted_liquidation_pdf(vm: PersistedLiquidationPdfViewModel, path: Path) -> Path:
    """Renderiza exclusivamente valores del ViewModel construido desde SQLite."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf=Canvas(str(path), pagesize=A4); y=805
    pdf.setTitle(f"Liquidación definitiva {vm.recipient_member_id}")
    pdf.setFont("Helvetica-Bold",16); pdf.drawString(38,y,"Liquidación definitiva"); y-=24
    pdf.setFont("Helvetica",9)
    for text in (f"IdLiq: {' · '.join(vm.id_liqs)}",f"Fecha: {vm.payment_date or '—'}  Campaña: {vm.campaign}  Empresa: {vm.company}",f"Cultivo: {vm.crop}  Remesa: {vm.remittance_id} - {vm.remittance_name}",f"Socio destinatario: {vm.recipient_member_id} - {vm.recipient_name}",f"Concepto: {vm.liquidation_concept}  Tipo: {vm.liquidation_type}"):
        pdf.drawString(38,y,text); y-=15
    y-=10; pdf.setFont("Helvetica-Bold",8)
    pdf.drawString(38,y,"Variedad / Artículo / kg / Bruto / Rec. / Cuota Ha / Calidad / Transporte / GlobalGAP / Base / IVA / Ret. / Total"); y-=15
    pdf.setFont("Helvetica",7)
    for line in vm.lines:
        text=f"{line.variedad} / {line.cod_art or '—'} / {line.neto} / {line.imp_bruto} / {line.recoleccion} / {line.cuota_ha} / {line.bp_calidad} / {line.b_transporte} / {line.b_global} / {line.base_i} / {line.iva} / {line.retencion} / {line.importe_total}"
        pdf.drawString(38,y,text[:155]); y-=14
        if y<55: pdf.showPage(); y=805; pdf.setFont("Helvetica",7)
    pdf.setFont("Helvetica-Bold",10); pdf.drawString(38,y-8,f"Total destinatario: {vm.totals.importe_total} EUR  | Base: {vm.totals.base_i} EUR  | Kilos: {vm.totals.neto}")
    pdf.save(); return path
