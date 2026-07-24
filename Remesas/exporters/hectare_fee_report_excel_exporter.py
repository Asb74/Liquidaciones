from __future__ import annotations
from decimal import Decimal
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

def export_hectare_fee_report(path, summaries, crop_details, surface_details, incidents):
    wb=Workbook(); wb.remove(wb.active)
    sheets=[("Resumen por boleta", ["Socio","Agricultor","Boleta","Campaña","Empresa","Superficie","Precio/ha","Cuota Ha","Entregas","Cultivos","Índice €/kg","Cuota aplicada","Cuota pendiente","Estado"]), ("Detalle por cultivo", ["Socio","Agricultor","Boleta","Cultivo","Número de entregas","Kilos","Porcentaje","Índice €/kg","Cuota aplicada"]), ("Detalle de superficie", ["Socio","Agricultor","Boleta","Cultivo superficie","Variedad","Polígono","Parcela","Recinto","Superficie","CHA","Incluida","Motivo exclusión"]), ("Incidencias", ["Tipo","Socio","Boleta","Detalle"]), ("Boletas revisadas", ["Socio","Agricultor","Boleta","CHA","Nº parcelas","Superficie total","Superficie válida","Superficie excluida","Años detectados","Estado","Motivos","Incidencias"]), ("Detalle parcelas", ["Socio","Boleta","Polígono","Parcela","Recinto","SupCul","Año","Antigüedad","Incluida","Motivo","Incidencia"]), ("Incidencias Cuota Ha", ["Tipo","Socio","Boleta","Parcela","Valor encontrado","Descripción","Impide cálculo (Sí/No)"])]
    for name,headers in sheets:
        ws=wb.create_sheet(name); ws.append(headers); ws.freeze_panes="A2"; ws.auto_filter.ref=f"A1:{chr(64+len(headers))}1"
        for cell in ws[1]: cell.font=Font(bold=True,color="FFFFFF"); cell.fill=PatternFill("solid",fgColor="1F4E78")
    for s in summaries:
        wb.worksheets[0].append([s.member_id,s.member_name,s.boleta,s.campaign,s.company,s.surface_hectares,s.price_per_hectare,s.annual_fee,s.total_delivery_kg," / ".join(s.delivery_crops),s.rate_per_kg,s.applied_fee,s.pending_fee,s.status])
        key=(s.member_id,s.boleta,s.campaign,s.company)
        for c in crop_details.get(key,()): wb.worksheets[1].append([s.member_id,s.member_name,s.boleta,c.crop,c.delivery_count,c.kilograms,c.percentage,c.rate_per_kg,c.applied_fee])
        for d in surface_details.get(key,()): wb.worksheets[2].append([s.member_id,s.member_name,s.boleta,d.crop,d.variety,d.polygon,d.parcel,d.enclosure,d.surface,"Sí" if d.cha_active else "No","Sí" if d.included else "No",d.exclusion_reason])
        audit = getattr(s, "audit", None)
        if audit:
            wb.worksheets[4].append([s.member_id,s.member_name,s.boleta,"Sí" if audit["cha"] else "No",audit["numero_parcelas"],audit["superficie_total"],audit["superficie_valida"],audit["superficie_excluida"],", ".join(audit["anos_detectados"]),audit["estado_boleta"],"; ".join(audit["motivos_exclusion"]),"; ".join(audit["incidencias"])])
            for parcel in audit["parcelas"]:
                reason = parcel.get("Motivo exclusión", "")
                incident = "PARCELA_SUPERFICIE_CERO" if decimal_or_zero(parcel.get("SupCul DParcela")) == 0 else ""
                wb.worksheets[5].append([s.member_id,s.boleta,parcel.get("Pol", ""),parcel.get("Par", ""),parcel.get("Rec", ""),parcel.get("SupCul DParcela", ""),parcel.get("Año", ""),parcel.get("Antigüedad", ""),parcel.get("Incluida", ""),reason,incident])
                if reason:
                    wb.worksheets[6].append([reason,s.member_id,s.boleta,parcel.get("Par", ""),parcel.get("SupCul DParcela", ""),reason,"Sí" if parcel.get("Incluida") != "Sí" else "No"])
            for incident in audit["incidencias"]:
                wb.worksheets[6].append([incident,s.member_id,s.boleta,"", "", incident, "No"])
    for row in incidents: wb.worksheets[3].append(row)
    for ws in wb.worksheets:
        for col in ws.columns: ws.column_dimensions[col[0].column_letter].width=min(45,max(12,max(len(str(c.value or "")) for c in col)+2))
        for row in ws.iter_rows(min_row=2):
            for c in row:
                if isinstance(c.value, Decimal): c.number_format='#,##0.00########'
    wb.save(path); return path
