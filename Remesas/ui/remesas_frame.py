from __future__ import annotations

import csv
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, ttk

from data.db_connection import ReadOnlyDatabase, load_config, setup_logging
from data.deliveries_repository import DeliveriesRepository
from data.metadata_repository import MetadataRepository
from data.remesas_repository import RemesasRepository
from domain.models import DeliveryFilter, Period, Remesa
from domain.validators import parse_user_date, validate_context, validate_period
from domain.utils import format_display_date, parse_yes_no, safe_path_part
from services.context_service import ContextService
from services.deliveries_service import DeliveriesService
from services.remesas_service import RemesasService
from services.calculation_service import CalculationService
from ui.context_panel import ContextPanel
from ui.deliveries_panel import COLUMNS, DeliveriesPanel
from ui.remesa_panel import RemesaPanel
from ui.summary_panel import SummaryPanel
from exporters.excel_exporter import export_liquidation_summary
from exporters.pdf_exporter import export_member_pdf

class RemesasFrame(ttk.Frame):
    def __init__(self, master, config_path: str | None = None):
        super().__init__(master, padding=8)
        self.config=load_config(config_path); setup_logging(self.config)
        self.db=ReadOnlyDatabase(self.config); self.conn=None; self.summary=None; self.current_remesa=None; self.current_calculation=None; self.current_deliveries=[]; self.calculation_valid=False
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
        ttk.Button(self.var_frame,text="Añadir",command=self._add_var).grid(row=1,column=1); ttk.Button(self.var_frame,text="Quitar",command=self._remove_var).grid(row=2,column=1); ttk.Button(self.var_frame,text="Añadir todas",command=self._add_all_var).grid(row=3,column=1); ttk.Button(self.var_frame,text="Limpiar",command=self._clear_selected_varieties).grid(row=4,column=1)
        self.apply_collection_var=tk.BooleanVar(value=False); self.apply_transport_var=tk.BooleanVar(value=False); self.apply_quality_var=tk.BooleanVar(value=False); self.apply_globalgap_var=tk.BooleanVar(value=False); self.apply_hectare_fee_var=tk.BooleanVar(value=False); self.apply_precalibrated_var=tk.BooleanVar(value=False)
        self.option_vars={"Aplica recolección":self.apply_collection_var,"Aplica transporte":self.apply_transport_var,"Aplica calidad":self.apply_quality_var,"Aplica GlobalGAP":self.apply_globalgap_var,"Aplica cuota por hectárea":self.apply_hectare_fee_var,"Liquida precalibrado":self.apply_precalibrated_var}
        for i,(name,var) in enumerate(self.option_vars.items()):
            ttk.Checkbutton(self.var_frame,text=name,variable=var,command=self._invalidate_calculation).grid(row=5+i//2,column=i%2,sticky="w",columnspan=1)
        self.prices=ttk.LabelFrame(self.var_frame,text="Precios") ; self.prices.grid(row=8,column=0,columnspan=3,sticky="ew")
        self.price_vars={f"P{i}":tk.StringVar(value="Pendiente") for i in range(12)} | {"PDESTRIO":tk.StringVar(value="Pendiente"),"PDMESA":tk.StringVar(value="Pendiente"),"PPODRIDO":tk.StringVar(value="Pendiente")}
        for i,(k,v) in enumerate(self.price_vars.items()): ttk.Label(self.prices,text=(f"Calibre {k[1:]}" if k.startswith('P') and k[1:].isdigit() else k)).grid(row=i//4,column=(i%4)*2); ttk.Label(self.prices,textvariable=v).grid(row=i//4,column=(i%4)*2+1)

    def _build_buttons(self):
        self.buttons=ttk.Frame(self)
        actions=[("Nueva remesa",self._clear),("Cargar remesa existente",self._load_remesa),("Buscar entregas",self._search),("Exportar entregas a CSV",self._export_csv),("Exportar entregas a Excel",self._export_excel),("Vista previa",self._preview),("Cerrar",self.winfo_toplevel().destroy)]
        self.action_buttons={}
        for i,(text,cmd) in enumerate(actions):
            b=ttk.Button(self.buttons,text=text,command=cmd); b.grid(row=0,column=i,padx=2); self.action_buttons[text]=b
        for i,(text,cmd) in enumerate([("Calcular liquidación",self._calculate),("Exportar resumen de liquidación",self._export_liquidation_excel),("Generar PDF de liquidación",self._export_liquidation_pdf),("Guardar liquidaciones",lambda:None),("Anular liquidación",lambda:None)]):
            b=ttk.Button(self.buttons,text=text,command=cmd,state="disabled"); b.grid(row=1,column=i,padx=2,pady=2); self.action_buttons[text]=b
        ttk.Label(self.buttons,text="Persistencia deshabilitada en esta fase.").grid(row=1,column=5,columnspan=3,sticky="w")

    def _connect(self):
        try:
            self.conn=self.db.connect_fruta_with_eepp(); self.meta=ContextService(MetadataRepository(self.conn)); self.deliveries=DeliveriesService(DeliveriesRepository(self.conn)); self.remesas=RemesasService(RemesasRepository(self.conn))
            self.context_panel.campaña_cb["values"]=self.meta.campaigns(); self.context_panel.set_status(self.db.status()); self.calculations=CalculationService(); self._refresh_action_states()
        except Exception as exc: messagebox.showerror("Error",f"No se ha podido acceder a las bases SQLite: {exc}")

    def _context_changed(self):
        self.current_remesa=None; self.remesa_panel.load({}); self._clear_selected_varieties(); self.deliveries_panel.clear(); self.summary_panel.clear(); self.current_calculation=None; self.calculation_valid=False; self.current_deliveries=[]
        ctx=self.context_panel.context()
        try:
            if ctx.campana and not ctx.empresa: self.context_panel.empresa_cb["values"]=self.meta.empresas(ctx.campana)
            if ctx.campana and ctx.empresa and not ctx.cultivo: self.context_panel.cultivo_cb["values"]=self.meta.cultivos(ctx.campana,ctx.empresa)
            if ctx.campana and ctx.empresa and ctx.cultivo: self._load_varieties()
        except Exception as exc: messagebox.showerror("Error",str(exc))
        self._refresh_action_states()

    def _load_varieties(self):
        self.available.delete(0,"end"); ctx=self.context_panel.context()
        d=self.remesa_panel.data(); desde=hasta=None
        try:
            if d.get("desde") and d.get("hasta"):
                desde=parse_user_date(d["desde"]); hasta=parse_user_date(d["hasta"])
        except ValueError:
            pass
        for v in self.meta.variedades(ctx.campana,ctx.empresa,ctx.cultivo,desde,hasta): self.available.insert("end",v)
    def _clear_selected_varieties(self):
        self.selected.delete(0,"end"); self._invalidate_calculation()
    def _add_var(self):
        for i in self.available.curselection():
            v=self.available.get(i)
            if v not in self.selected.get(0,"end"): self.selected.insert("end",v)
        self._invalidate_calculation()
    def _add_all_var(self):
        self.selected.delete(0,"end")
        for v in self.available.get(0,"end"): self.selected.insert("end",v)
        self._invalidate_calculation()
    def _remove_var(self):
        for i in reversed(self.selected.curselection()): self.selected.delete(i)
        self._invalidate_calculation()
    def _filters(self):
        ctx=self.context_panel.context(); validate_context(ctx); d=self.remesa_panel.data(); period=Period(parse_user_date(d["desde"]),parse_user_date(d["hasta"])); validate_period(period)
        return DeliveryFilter(ctx, period, list(self.selected.get(0,"end")), d.get("socio"), d.get("categoria") or None)
    def _search(self):
        try:
            rows,summary,elapsed,total=self.deliveries.search(self._filters()); self.current_deliveries=rows; self.deliveries_panel.set_rows(rows); self.summary=summary; self.summary_panel.set_summary(summary)
            extra="" if total<=len(rows) else f" Se muestran las primeras {len(rows)}."
            self.status.set(f"{total} entregas cargadas en {elapsed:.2f} s.{extra}")
            if not total: messagebox.showinfo("Sin datos","No se han encontrado entregas para el periodo seleccionado.")
            self._refresh_action_states()
        except Exception as exc: messagebox.showerror("Error",str(exc))
    def _load_remesa(self):
        ctx=self.context_panel.context()
        if not (ctx.campana and ctx.empresa and ctx.cultivo):
            messagebox.showwarning("Contexto obligatorio", "Seleccione campaña, empresa y cultivo antes de cargar una remesa."); return
        try:
            items=self.remesas.list_remesas(ctx.campana, ctx.empresa, ctx.cultivo)
            remesa_id=self._select_remesa_dialog(items, ctx)
            if not remesa_id: return
            rem=self.remesas.get_remesa(remesa_id); self.current_remesa=rem; self.remesa_panel.load(rem.values)
            for k,v in rem.prices.items(): self.price_vars[k].set(str(v if v is not None else ""))
            self.apply_collection_var.set(parse_yes_no(rem.values.get("AplRec")))
            self.apply_transport_var.set(parse_yes_no(rem.values.get("AplTte")))
            self.apply_quality_var.set(parse_yes_no(rem.values.get("AplCal")))
            self.apply_globalgap_var.set(parse_yes_no(rem.values.get("AplGlobal")))
            self.apply_hectare_fee_var.set(parse_yes_no(rem.values.get("AplCHa")))
            self.apply_precalibrated_var.set(parse_yes_no(rem.values.get("AplPrecalibrado")))
            self._load_varieties(); self._restore_remesa_varieties(rem)
            self.deliveries_panel.clear(); self.summary_panel.clear(); self.current_deliveries=[]; self.current_calculation=None; self.calculation_valid=False
            self._refresh_action_states()
            if messagebox.askyesno("Buscar entregas", "¿Desea buscar las entregas correspondientes a esta remesa?"):
                self._search()
        except Exception as exc: messagebox.showerror("Error",str(exc))

    def _select_remesa_dialog(self, items, ctx):
        win=tk.Toplevel(self); win.title("Seleccionar remesa"); win.transient(self.winfo_toplevel()); win.grab_set(); win.geometry("900x430")
        ttk.Label(win,text=f"Campaña: {ctx.campana} | Empresa: {ctx.empresa} | Cultivo: {ctx.cultivo}").pack(anchor="w",padx=8,pady=4)
        query=tk.StringVar(); ttk.Entry(win,textvariable=query).pack(fill="x",padx=8,pady=4)
        cols=("IdREMESA","REMESA","FECHARE","PERIODO1","PERIODO2","CATEGORIA","TipoLiq")
        tree=ttk.Treeview(win,columns=cols,show="headings"); [tree.heading(c,text=c) for c in cols]; [tree.column(c,width=120,anchor="w") for c in cols]; tree.pack(fill="both",expand=True,padx=8,pady=4)
        result={"id":None}
        def fill():
            tree.delete(*tree.get_children()); q=query.get().strip().upper()
            for row in items:
                hay=" ".join(str(row.get(k) or "") for k in ("IdREMESA","REMESA","CATEGORIA","TipoLiq")).upper()
                if not q or q in hay: tree.insert("","end",values=[row.get(c) or "" for c in cols])
        def load(_=None):
            sel=tree.selection();
            if sel: result["id"]=tree.item(sel[0],"values")[0]; win.destroy()
        query.trace_add("write", lambda *_: fill()); tree.bind("<Double-1>", load); win.bind("<Return>", load); win.bind("<Escape>", lambda e: win.destroy())
        bf=ttk.Frame(win); bf.pack(fill="x",padx=8,pady=6); ttk.Button(bf,text="Cargar",command=load).pack(side="right",padx=4); ttk.Button(bf,text="Cancelar",command=win.destroy).pack(side="right")
        fill(); win.wait_window(); return result["id"]

    def _restore_remesa_varieties(self, rem: Remesa) -> None:
        target=str(rem.values.get("VARIEDAD") or "").strip()
        self.selected.delete(0,"end")
        available=list(self.available.get(0,"end"))
        if target and target in available: self.selected.insert("end", target)
        elif target:
            messagebox.showwarning("Variedades", f"No se pudo reconstruir exactamente la selección de variedades para '{target}'. Revise la selección antes de calcular.")
    def _clear(self):
        self.current_remesa=None; self.remesa_panel.load({}); self.selected.delete(0,"end"); self.deliveries_panel.clear(); self.summary_panel.clear(); self.summary=None; self.current_calculation=None; self.calculation_valid=False; self.current_deliveries=[]; self.status.set("Filtros/resultados limpiados"); self._refresh_action_states()
    def _context_ready(self) -> bool:
        try:
            self._filters()
            return True
        except Exception:
            return False

    def _refresh_action_states(self) -> None:
        has_context = self._context_ready()
        has_varieties = bool(self.selected.get(0,"end"))
        has_deliveries = bool(self.deliveries_panel.visible_rows())
        if hasattr(self, "action_buttons"):
            self.action_buttons["Buscar entregas"].configure(state="normal" if has_context else "disabled")
            self.action_buttons["Calcular liquidación"].configure(state="normal" if has_context and has_varieties and has_deliveries else "disabled")
            self.action_buttons["Vista previa"].configure(state="normal" if has_deliveries else "disabled")
            self.action_buttons["Guardar liquidaciones"].configure(state="disabled")
            self.action_buttons["Anular liquidación"].configure(state="disabled")
            ok=bool(self.current_calculation and self.current_calculation.result and self.calculation_valid and self.current_calculation.result.member_results)
            if "Exportar resumen de liquidación" in self.action_buttons: self.action_buttons["Exportar resumen de liquidación"].configure(state="normal" if ok else "disabled")
            if "Generar PDF de liquidación" in self.action_buttons: self.action_buttons["Generar PDF de liquidación"].configure(state="normal" if ok else "disabled")

    def _deliveries(self):
        return list(self.current_deliveries)

    def _calculate(self):
        try:
            deliveries = self._deliveries()
            if not deliveries:
                self._search(); deliveries = self._deliveries()
            if not deliveries:
                return
            self.current_calculation = self.calculations.calculate(deliveries, self._calculation_remesa())
            self.calculation_valid=True
            self.summary_panel.set_calculation(self.current_calculation)
            self.status.set(f"Liquidación calculada: {self.current_calculation.member_count} socios, {self.current_calculation.net_kg:,.3f} kg, importe comercial {self.current_calculation.commercial_amount:,.2f} €")
            self._refresh_action_states()
        except Exception as exc:
            messagebox.showerror("Error", str(exc))

    def _calculation_remesa(self):
        base = dict(self.current_remesa.values) if self.current_remesa else {}
        base.update({k:v.get() for k,v in self.price_vars.items()})
        ctx=self.context_panel.context(); data=self.remesa_panel.data()
        base.update({"CAMPAÑA":ctx.campana,"EMPRESA":ctx.empresa,"CULTIVO":ctx.cultivo,"REMESA":data.get("remesa"),"FECHARE":data.get("fecha_pago"),"PERIODO1":data.get("desde"),"PERIODO2":data.get("hasta"),"TipoLiq":data.get("tipo"),"CATEGORIA":data.get("categoria"),"IdSocio":data.get("socio"),"VARIEDAD":", ".join(self.selected.get(0,"end"))})
        return Remesa(base)

    def _invalidate_calculation(self):
        if getattr(self, "calculation_valid", False):
            self.calculation_valid=False; self.status.set("Los parámetros han cambiado. Vuelva a calcular la liquidación.")
        self._refresh_action_states()

    def _pending(self, value):
        return "Pendiente de implementar" if value is None else f"{value:,.2f} €"

    def _preview(self):
        if not self.deliveries_panel.visible_rows():
            return
        win=tk.Toplevel(self); win.title("Vista previa de liquidación"); win.transient(self.winfo_toplevel()); win.grab_set(); win.geometry("1200x700"); win.resizable(True, True)
        nb=ttk.Notebook(win); nb.pack(fill="both", expand=True, padx=8, pady=8)
        head=ttk.Frame(nb); detail=ttk.Frame(nb); nb.add(head,text="Resumen"); nb.add(detail,text="Detalle")
        ctx=self.context_panel.context(); data=self.remesa_panel.data(); selected=', '.join(self.selected.get(0,'end')) or "Todas"
        options=', '.join(name for name,var in self.option_vars.items() if var.get()) or "Ninguna"
        rows=[("Nombre de remesa",data.get('remesa')), ("Campaña",ctx.campana), ("Empresa",ctx.empresa), ("Cultivo",ctx.cultivo), ("Periodo",f"{format_display_date(data.get('desde'))} - {format_display_date(data.get('hasta'))}"), ("Fecha de pago",format_display_date(data.get('fecha_pago'))), ("Tipo de liquidación",data.get('tipo')), ("Categoría",data.get('categoria')), ("Socio",data.get('socio') or "Todos"), ("Variedades",selected), ("Opciones activadas",options)]
        for i,(k,v) in enumerate(rows): ttk.Label(head,text=k,font=("TkDefaultFont",9,"bold")).grid(row=i,column=0,sticky="nw",padx=4,pady=2); ttk.Label(head,text=v,wraplength=800).grid(row=i,column=1,sticky="w",padx=4,pady=2)
        s=self.summary
        base=12
        if s:
            for j,(k,v) in enumerate([("Número de entregas",s.total_entregas),("Número de socios",s.socios),("Número de variedades",s.variedades),("Kilos netos",f"{s.kilos_netos:,.2f}"),("Primera fecha",s.primera_fecha),("Última fecha",s.ultima_fecha),("Entregas ya marcadas como liquidadas",s.liquidadas),("Registros sin variedad",s.sin_variedad),("Registros sin socio válido",s.sin_socio_valido),("Registros sin categoría",s.sin_categoria)]): ttk.Label(head,text=k,font=("TkDefaultFont",9,"bold")).grid(row=base+j,column=0,sticky="w",padx=4,pady=2); ttk.Label(head,text=v).grid(row=base+j,column=1,sticky="w",padx=4,pady=2)
        calc=self.current_calculation
        econ=[("Importe comercial calculado", f"{calc.commercial_amount:,.2f} €" if calc else "Pendiente de implementar"),("Recolección","Pendiente de implementar"),("Transporte","Pendiente de implementar"),("Calidad","Pendiente de implementar"),("GlobalGAP","Pendiente de implementar"),("Cuota por hectárea","Pendiente de implementar"),("Base imponible","Pendiente de implementar"),("IVA","Pendiente de implementar"),("Retención","Pendiente de implementar"),("Total","Pendiente de implementar")]
        for j,(k,v) in enumerate(econ): ttk.Label(head,text=k,font=("TkDefaultFont",9,"bold")).grid(row=base+11+j,column=0,sticky="w",padx=4,pady=2); ttk.Label(head,text=v).grid(row=base+11+j,column=1,sticky="w",padx=4,pady=2)
        cols=("Socio","Nombre","Variedad","Nº entregas","Kilos netos","Importe comercial","Recolección","Transporte","Calidad","GlobalGAP","Cuota Ha","Base imponible","IVA","Retención","Total")
        tree=ttk.Treeview(detail,columns=cols,show="headings"); [tree.heading(c,text=c) for c in cols]; [tree.column(c,width=120,anchor="w") for c in cols]
        y=ttk.Scrollbar(detail,orient="vertical",command=tree.yview); x=ttk.Scrollbar(detail,orient="horizontal",command=tree.xview); tree.configure(yscrollcommand=y.set,xscrollcommand=x.set)
        tree.grid(row=0,column=0,sticky="nsew"); y.grid(row=0,column=1,sticky="ns"); x.grid(row=1,column=0,sticky="ew"); detail.rowconfigure(0,weight=1); detail.columnconfigure(0,weight=1)
        if calc:
            for l in calc.lines: tree.insert("","end",values=(l.member_id,l.member_name,l.variety,l.delivery_count,f"{l.net_kg:,.3f}",f"{l.commercial_amount:,.2f}","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar","Pendiente de implementar"))
        else:
            for vals in self.deliveries_panel.visible_rows(): tree.insert("","end",values=(vals[2],vals[3],vals[4],1,vals[6],"Pendiente de implementar",*(["Pendiente de implementar"]*9)))
    def _output_dir(self):
        base=Path("C:/Liquidaciones/salidas/remesas") if Path("C:/").exists() else Path.cwd().parents[0]/"salidas"/"remesas"
        ctx=self.context_panel.context(); data=self.remesa_panel.data()
        p=base/safe_path_part(ctx.campana)/safe_path_part(ctx.cultivo)/(safe_path_part(data.get("remesa") or "remesa")); p.mkdir(parents=True,exist_ok=True); return p
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

    def _export_liquidation_excel(self):
        if not (self.current_calculation and self.current_calculation.result and self.calculation_valid): return
        path=export_liquidation_summary(self.current_calculation.result, self._output_dir()/"resumen_liquidaciones.xlsx")
        messagebox.showinfo("Exportación", f"Excel de liquidación creado: {path}")

    def _export_liquidation_pdf(self):
        if not (self.current_calculation and self.current_calculation.result and self.calculation_valid): return
        path=export_member_pdf(self.current_calculation.result, self._output_dir()/"liquidacion_socios.pdf")
        messagebox.showinfo("Exportación", f"PDF de liquidación creado: {path}")
