from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class BatchPersistencePreviewDialog(tk.Toplevel):
    """Vista consolidada del lote conservado. No ejecuta persistencia."""
    def __init__(self,parent: tk.Misc,preview,*,allow_confirm=False):
        super().__init__(parent); self.preview=preview; self.confirmed=False
        self.title("Vista previa del lote de liquidaciones"); self.geometry("1250x700"); self.transient(parent); self.grab_set()
        nb=ttk.Notebook(self); nb.pack(fill="both",expand=True,padx=8,pady=8)
        tabs={name:ttk.Frame(nb) for name in ("Resumen del lote","Remesas","Divisiones","Líneas finales","Advertencias")}
        for name,frame in tabs.items(): nb.add(frame,text=name)
        valid=[x for x in preview.remittances if x.valid]; lines=[line for item in valid for line in item.persistence_preview.lines]
        originals={line.source_member_id for line in lines}; recipients={line.recipient_member_id for line in lines}; split_rules={line.split_rule_id for line in lines if line.split_rule_id}
        total_net=sum((x.net_kg for x in lines),0); total_base=sum((x.taxable_base for x in lines),0); total=sum((x.total_amount for x in lines),0)
        text=(f"Campaña: {preview.campaign}\nEmpresa: {preview.company}\nCultivo: {preview.crop}\n"
              f"Remesas calculadas/válidas/con error: {len(preview.remittances)+len(preview.excluded_remittances)}/{len(valid)}/{len(preview.excluded_remittances)}\n"
              f"Líneas originales/finales: {preview.total_original_lines}/{preview.total_final_lines}\nSocios originales: {len(originals)} | Destinatarios finales: {len(recipients)} | Reglas: {len(split_rules)}\n"
              f"Total neto: {total_net} | Total base: {total_base} | Total final estimado: {total}")
        ttk.Label(tabs["Resumen del lote"],text=text,justify="left").pack(anchor="nw",padx=12,pady=12)
        rem_tree=self._tree(tabs["Remesas"],("remesa","nombre","originales","finales","divisiones","estado","advertencias","guardable"))
        for item in preview.remittances:
            p=item.persistence_preview; rem_tree.insert("","end",values=(item.remittance.remittance_id,item.remittance.name,p.original_line_count,len(p.lines),sum(x.split_factor!=1 for x in p.lines),"VÁLIDA" if item.valid else "ERROR","; ".join(item.warnings),"Sí" if item.valid else "No"))
        for rem in preview.excluded_remittances: rem_tree.insert("","end",values=(rem.remittance_id,rem.name,0,0,0,"ERROR","Excluida durante el cálculo","No"))
        split_tree=self._tree(tabs["Divisiones"],("remesa","origen","variedad","tipo","destino","factor","resultado"))
        final_cols=("remesa","origen","destino","variedad","factor","neto","bruto","recolección","cuota_ha","calidad","transporte","globalgap","base","iva","retención","total")
        final_tree=self._tree(tabs["Líneas finales"],final_cols)
        for item in valid:
            for x in item.persistence_preview.lines:
                split_tree.insert("","end",values=(item.remittance.remittance_id,x.source_member_id,x.variety,x.split_type or "SIN DIVISIÓN",x.recipient_member_id,x.split_factor,"Guardable"))
                final_tree.insert("","end",values=(item.remittance.remittance_id,x.source_member_id,x.recipient_member_id,x.variety,x.split_factor,x.net_kg,x.gross_amount,x.collection_amount,x.hectare_fee_amount,x.quality_amount,x.transport_amount,x.globalgap_amount,x.taxable_base,x.vat_rate,x.withholding_rate,x.total_amount))
        ttk.Label(tabs["Advertencias"],text="\n".join(preview.warnings) or "Sin advertencias",justify="left").pack(anchor="nw",padx=12,pady=12)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=8,pady=8)
        ttk.Button(bar,text="Cerrar" if not allow_confirm else "Cancelar",command=self.destroy).pack(side="right")
        if allow_confirm: ttk.Button(bar,text="Confirmar y guardar",command=self._confirm).pack(side="right",padx=6)
    def _tree(self,parent,columns):
        tree=ttk.Treeview(parent,columns=columns,show="headings")
        for c in columns: tree.heading(c,text=c.title()); tree.column(c,width=95)
        sx=ttk.Scrollbar(parent,orient="horizontal",command=tree.xview); sy=ttk.Scrollbar(parent,orient="vertical",command=tree.yview); tree.configure(xscrollcommand=sx.set,yscrollcommand=sy.set)
        tree.grid(row=0,column=0,sticky="nsew"); sy.grid(row=0,column=1,sticky="ns"); sx.grid(row=1,column=0,sticky="ew"); parent.rowconfigure(0,weight=1); parent.columnconfigure(0,weight=1); return tree
    def _confirm(self): self.confirmed=True; self.destroy()

    def show(self):
        self.wait_window(); return self.confirmed
