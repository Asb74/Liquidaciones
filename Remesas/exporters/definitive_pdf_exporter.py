from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen.canvas import Canvas

from domain.utils import safe_path_part


def export_definitive_pdfs(preview, batch, output_dir: Path) -> tuple[Path,...]:
    """Un documento aislado por destinatario, generado únicamente tras el commit."""
    target=Path(output_dir)/"definitivos"; target.mkdir(parents=True,exist_ok=True); grouped=defaultdict(list)
    for line in preview.lines:
        if line.net_kg: grouped[(line.recipient_member_id,line.recipient_name)].append(line)
    paths=[]
    for (member_id,name),lines in grouped.items():
        path=target/f"Liquidacion_{member_id}_{safe_path_part(name)}_{safe_path_part(preview.header.remesa_name)}_{batch.batch_id[:8]}.pdf"
        pdf=Canvas(str(path),pagesize=A4); y=800
        pdf.setTitle(f"Liquidación definitiva {member_id}"); pdf.setFont("Helvetica-Bold",14); pdf.drawString(40,y,"Liquidación definitiva"); y-=28
        pdf.setFont("Helvetica",10); pdf.drawString(40,y,f"Socio: {member_id} - {name}"); y-=18; pdf.drawString(40,y,f"Remesa: {preview.header.remesa_name} | Batch: {batch.batch_id}"); y-=28
        for line in lines:
            pdf.drawString(40,y,f"{line.variety}: {line.net_kg} kg | Base {line.taxable_base} | IVA {line.vat_rate}% | Ret. {line.withholding_rate}% | Total {line.total_amount}"); y-=18
        pdf.setFont("Helvetica-Bold",11); pdf.drawString(40,y,f"Total destinatario: {sum((x.total_amount for x in lines))}"); pdf.save(); paths.append(path)
    return tuple(paths)
