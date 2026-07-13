from __future__ import annotations

import csv
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk

from data.db_connection import ReadOnlyDatabase, load_config, setup_logging
from data.deliveries_repository import DeliveriesRepository
from data.metadata_repository import MetadataRepository
from data.remesas_repository import RemesasRepository
from domain.models import DeliveryFilter, Period
from domain.validators import parse_user_date, validate_context, validate_period
from services.context_service import ContextService
from services.deliveries_service import DeliveriesService
from services.remesas_service import RemesasService
from ui.context_panel import ContextPanel
from ui.deliveries_panel import COLUMNS, DeliveriesPanel
from ui.remesa_panel import RemesaPanel
from ui.summary_panel import SummaryPanel

class RemesasFrame(ttk.Frame):
    def __init__(self, master, config_path: str | None = None):
        super().__init__(master, padding=8)
        self.config=load_config(config_path); setup_logging(self.config)
        self.db=ReadOnlyDatabase(self.config); self.conn=None; self.summary=None
        self._build(); self._connect()

    def _build(self):
        self.context_panel=ContextPanel(self,self.config,self._context_changed); self.context_panel.grid(row=0,column=0,columnspan=3,sticky="ew")
        self.remesa_panel=RemesaPanel(self); self.remesa_panel.grid(row=1,column=0,sticky="nsew",padx=4,pady=4)
        self._build_varieties(); self.var_frame.grid(row=1,column=1,sticky="nsew",padx=4,pady=4)
        self.summary_panel=SummaryPanel(self); self.summary_panel.grid(row=1,column=2,rowspan=2,sticky="nsew",padx=4,pady=4)
        self.deliveries_panel=DeliveriesPanel(self); self.deliveries_panel.grid(row=2,column=0,columnspan=2,sticky="nsew",padx=4,pady=4)
        self._build_buttons(); self.buttons.grid(row=3,column=0,columnspan=3,sticky="ew")
        self.status=tk.StringVar(value="Listo"); ttk.Label(self,textvariable=self.status).grid(row=4,column=0,columnspan=3,sticky="ew")
        self.rowconfigure(2,weight=1); self.columnconfigure(1,weight=1)

    def _build_varieties(self):
        self.var_frame=ttk.LabelFrame(self,text="Variedades y configuración")
        self.available=tk.Listbox(self.var_frame,height=7,exportselection=False); self.selected=tk.Listbox(self.var_frame,height=7,exportselection=False)
        ttk.Label(self.var_frame,text="Disponibles").grid(row=0,column=0); ttk.Label(self.var_frame,text="Seleccionadas").grid(row=0,column=2)
        self.available.grid(row=1,column=0,rowspan=4,sticky="nsew"); self.selected.grid(row=1,column=2,rowspan=4,sticky="nsew")
        ttk.Button(self.var_frame,text="Añadir",command=self._add_var).grid(row=1,column=1); ttk.Button(self.var_frame,text="Quitar",command=self._remove_var).grid(row=2,column=1); ttk.Button(self.var_frame,text="Añadir todas",command=self._add_all_var).grid(row=3,column=1); ttk.Button(self.var_frame,text="Limpiar",command=lambda:self.selected.delete(0,"end")).grid(row=4,column=1)
        self.option_vars={name:tk.BooleanVar() for name in ["Aplica recolección","Aplica transporte","Aplica calidad","Aplica GlobalGAP","Aplica cuota por hectárea","Liquida precalibrado"]}
        for i,(name,var) in enumerate(self.option_vars.items()): ttk.Checkbutton(self.var_frame,text=name,variable=var).grid(row=5+i//2,column=i%2,sticky="w",columnspan=1)
        self.prices=ttk.LabelFrame(self.var_frame,text="Precios") ; self.prices.grid(row=8,column=0,columnspan=3,sticky="ew")
        self.price_vars={f"P{i}":tk.StringVar(value="Pendiente") for i in range(12)} | {"PDESTRIO":tk.StringVar(value="Pendiente"),"PDMESA":tk.StringVar(value="Pendiente"),"PPODRIDO":tk.StringVar(value="Pendiente")}
        for i,(k,v) in enumerate(self.price_vars.items()): ttk.Label(self.prices,text=(f"Calibre {k[1:]}" if k.startswith('P') and k[1:].isdigit() else k)).grid(row=i//4,column=(i%4)*2); ttk.Label(self.prices,textvariable=v).grid(row=i//4,column=(i%4)*2+1)

    def _build_buttons(self):
        self.buttons=ttk.Frame(self)
        actions=[("Nueva remesa",self._clear),("Cargar remesa existente",self._load_remesa),("Buscar entregas",self._search),("Limpiar filtros",self._clear),("Vista previa",lambda:messagebox.showinfo("Vista previa","Vista previa inicial.")),("Exportar entregas a CSV",self._export_csv),("Exportar entregas a Excel",self._export_excel),("Cerrar",self.winfo_toplevel().destroy)]
        for i,(text,cmd) in enumerate(actions): ttk.Button(self.buttons,text=text,command=cmd).grid(row=0,column=i,padx=2)
        for i,text in enumerate(["Calcular liquidación","Guardar liquidaciones","Anular liquidación","Generar PDF final"]): ttk.Button(self.buttons,text=text,state="disabled").grid(row=1,column=i,padx=2,pady=2)
        ttk.Label(self.buttons,text="Disponible en una fase posterior.").grid(row=1,column=4,columnspan=4,sticky="w")

    def _connect(self):
        try:
            self.conn=self.db.connect_fruta_with_eepp(); self.meta=ContextService(MetadataRepository(self.conn)); self.deliveries=DeliveriesService(DeliveriesRepository(self.conn)); self.remesas=RemesasService(RemesasRepository(self.conn))
            self.context_panel.campaña_cb["values"]=self.meta.campaigns(); self.context_panel.set_status(self.db.status())
        except Exception as exc: messagebox.showerror("Error",f"No se ha podido acceder a las bases SQLite: {exc}")

    def _context_changed(self):
        self.deliveries_panel.clear(); self.summary_panel.clear(); self.selected.delete(0,"end")
        ctx=self.context_panel.context()
        try:
            if ctx.campana and not ctx.empresa: self.context_panel.empresa_cb["values"]=self.meta.empresas(ctx.campana)
            if ctx.campana and ctx.empresa and not ctx.cultivo: self.context_panel.cultivo_cb["values"]=self.meta.cultivos(ctx.campana,ctx.empresa)
            if ctx.campana and ctx.empresa and ctx.cultivo: self._load_varieties()
        except Exception as exc: messagebox.showerror("Error",str(exc))

    def _load_varieties(self):
        self.available.delete(0,"end"); ctx=self.context_panel.context()
        for v in self.meta.variedades(ctx.campana,ctx.empresa,ctx.cultivo): self.available.insert("end",v)
    def _add_var(self):
        for i in self.available.curselection():
            v=self.available.get(i)
            if v not in self.selected.get(0,"end"): self.selected.insert("end",v)
    def _add_all_var(self):
        self.selected.delete(0,"end")
        for v in self.available.get(0,"end"): self.selected.insert("end",v)
    def _remove_var(self):
        for i in reversed(self.selected.curselection()): self.selected.delete(i)
    def _filters(self):
        ctx=self.context_panel.context(); validate_context(ctx); d=self.remesa_panel.data(); period=Period(parse_user_date(d["desde"]),parse_user_date(d["hasta"])); validate_period(period)
        return DeliveryFilter(ctx, period, list(self.selected.get(0,"end")), d.get("socio"), d.get("categoria") or None)
    def _search(self):
        try:
            rows,summary,elapsed,total=self.deliveries.search(self._filters()); self.deliveries_panel.set_rows(rows); self.summary=summary; self.summary_panel.set_summary(summary)
            extra="" if total<=len(rows) else f" Se muestran las primeras {len(rows)}."
            self.status.set(f"{total} entregas cargadas en {elapsed:.2f} s.{extra}")
            if not total: messagebox.showinfo("Sin datos","No se han encontrado entregas para el periodo seleccionado.")
        except Exception as exc: messagebox.showerror("Error",str(exc))
    def _load_remesa(self):
        try:
            items=self.remesas.list_remesas(); choice=simpledialog.askstring("Remesa", "IdREMESA a cargar:\n"+"\n".join(f"{i} - {n}" for i,n in items[:20]))
            if not choice: return
            rem=self.remesas.get_remesa(choice.split()[0]); self.remesa_panel.load(rem.values)
            for k,v in rem.prices.items(): self.price_vars[k].set(str(v if v is not None else ""))
        except Exception as exc: messagebox.showerror("Error",str(exc))
    def _clear(self): self.deliveries_panel.clear(); self.summary_panel.clear(); self.status.set("Filtros/resultados limpiados")
    def _output_dir(self):
        p=Path("C:/Liquidaciones/salidas/remesas") if Path("C:/").exists() else Path.cwd().parents[0]/"salidas"/"remesas"; p.mkdir(parents=True,exist_ok=True); return p
    def _export_csv(self):
        path=self._output_dir()/"entregas_remesas.csv"
        with path.open("w",newline="",encoding="utf-8-sig") as f: w=csv.writer(f); w.writerow(COLUMNS); w.writerows(self.deliveries_panel.visible_rows())
        messagebox.showinfo("Exportación",f"CSV creado: {path}")
    def _export_excel(self):
        try:
            from openpyxl import Workbook
        except ImportError as exc:
            messagebox.showerror("Exportación", "openpyxl no está instalado. Instale requirements.txt para exportar a Excel.")
            return
        wb=Workbook(); ws=wb.active; ws.title="Contexto"; ctx=self.context_panel.context(); data=self.remesa_panel.data()
        for row in [("campaña",ctx.campana),("empresa",ctx.empresa),("cultivo",ctx.cultivo),("periodo",f"{data['desde']} - {data['hasta']}"),("remesa",data['remesa'])]: ws.append(row)
        ws=wb.create_sheet("Entregas"); ws.append(COLUMNS); [ws.append(r) for r in self.deliveries_panel.visible_rows()]
        ws=wb.create_sheet("Resumen");
        if self.summary:
            for k,v in self.summary.__dict__.items(): ws.append((k,str(v)))
        path=self._output_dir()/"entregas_remesas.xlsx"; wb.save(path); messagebox.showinfo("Exportación",f"Excel creado: {path}")
