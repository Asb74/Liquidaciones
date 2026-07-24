from __future__ import annotations
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from exporters.hectare_fee_report_excel_exporter import export_hectare_fee_report

class HectareFeeReportDialog(tk.Toplevel):
    def __init__(self, parent, service, campaigns, companies):
        super().__init__(parent); self.service=service; self.title("Informe de cuota por hectárea"); self.geometry("1350x600"); self.data=None
        box=ttk.Frame(self,padding=8); box.pack(fill="x"); self.campaign=tk.StringVar(value=campaigns[0] if campaigns else ""); self.company=tk.StringVar(value=companies[0] if companies else "")
        for label,var,values in (("Campaña",self.campaign,campaigns),("Empresa",self.company,companies)):
            ttk.Label(box,text=label).pack(side="left",padx=(0,4)); ttk.Combobox(box,textvariable=var,values=values,state="readonly",width=12).pack(side="left",padx=(0,12))
        ttk.Button(box,text="Consultar",command=self.refresh).pack(side="left"); ttk.Button(box,text="Exportar Excel",command=self.export).pack(side="left",padx=5)
        cols=("Socio","Agricultor","Boleta","Superficie","Cuota Ha","Entregas","Cultivos","Índice €/kg","Precio/ha","Cuota aplicada","Cuota pendiente","Estado")
        self.tree=ttk.Treeview(self,columns=cols,show="headings"); self.tree.pack(fill="both",expand=True,padx=8,pady=8)
        for c in cols: self.tree.heading(c,text=c); self.tree.column(c,width=110,anchor="w")
    def refresh(self):
        try:
            self.data=self.service.build_report(self.campaign.get(),self.company.get())
            self.tree.delete(*self.tree.get_children())
            for s in self.data[0]: self.tree.insert("","end",values=(s.member_id,s.member_name,s.boleta,s.surface_hectares,s.annual_fee,s.total_delivery_kg," / ".join(s.delivery_crops) or "Sin entregas",s.rate_per_kg or "No calculable",s.price_per_hectare,s.applied_fee,s.pending_fee,s.status))
        except Exception as exc: messagebox.showerror(self.title(),str(exc),parent=self)
    def export(self):
        if not self.data: return
        path=filedialog.asksaveasfilename(parent=self,defaultextension=".xlsx",filetypes=[("Excel","*.xlsx")])
        if path: export_hectare_fee_report(path,*self.data); messagebox.showinfo(self.title(),"Excel exportado correctamente.",parent=self)
