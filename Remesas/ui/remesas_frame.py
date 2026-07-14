from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, ttk

from data.db_connection import ReadOnlyDatabase, load_config, setup_logging
from data.deliveries_repository import DeliveriesRepository
from data.metadata_repository import MetadataRepository
from data.hectare_fee_master_repository import HectareFeeCropRepository
from data.remesas_repository import RemesasRepository
from domain.models import DeliveryFilter, Period, Remesa
from domain.hectare_fee_master import HectareFeeMasterRepository
from domain.validators import parse_user_date, validate_context, validate_period
from domain.utils import format_currency_es, format_decimal_es, format_display_date, format_integer_es, format_price_es, parse_yes_no, safe_path_part
from services.context_service import ContextService
from services.deliveries_service import DeliveriesService
from services.remesas_service import RemesasService
from services.calculation_service import CalculationService
from services.hectare_fee_master_service import HectareFeeMasterService
from services.local_database_sync_service import LocalDatabaseSyncService
from ui.context_panel import ContextPanel
from ui.deliveries_panel import COLUMNS, DeliveriesPanel
from ui.remesa_panel import RemesaPanel
from ui.summary_panel import SummaryPanel
from ui.hectare_fee_master_dialog import HectareFeeMasterDialog
from exporters.excel_exporter import export_liquidation_summary
from exporters.file_lock import FileLockedError

logger = logging.getLogger(__name__)
from exporters.pdf_exporter import export_member_pdf

class RemesasFrame(ttk.Frame):
    def __init__(self, master, config_path: str | None = None):
        super().__init__(master, padding=8)
        self.config=load_config(config_path); setup_logging(self.config)
        self.db=ReadOnlyDatabase(self.config); self.conn=None; self.summary=None; self.current_remesa=None; self.current_calculation=None; self.current_deliveries=[]; self.calculation_valid=False; self.sync_results=[]; self.master_repository=HectareFeeMasterRepository(); self.hectare_master_service=HectareFeeMasterService(self.master_repository); self.active_master=self.hectare_master_service.load_master()
        self._build(); self._connect()

    def _build(self):
        self.db_status_text=tk.StringVar(value="Preparando bases locales...")
        ttk.Label(self,textvariable=self.db_status_text).grid(row=0,column=0,columnspan=3,sticky="ew")
        self.context_panel=ContextPanel(self,self.config,self._context_changed); self.context_panel.grid(row=1,column=0,columnspan=3,sticky="ew")
        self.remesa_panel=RemesaPanel(self); self.remesa_panel.grid(row=2,column=0,sticky="nsew",padx=4,pady=4)
        self._build_varieties(); self.var_frame.grid(row=2,column=1,sticky="nsew",padx=4,pady=4)
        self.summary_panel=SummaryPanel(self); self.summary_panel.grid(row=2,column=2,rowspan=2,sticky="nsew",padx=4,pady=4)
        self.deliveries_panel=DeliveriesPanel(self); self.deliveries_panel.grid(row=3,column=0,columnspan=2,sticky="nsew",padx=4,pady=4)
        self._build_hectare_config_info(); self.hectare_info.grid(row=4,column=0,columnspan=3,sticky="ew",pady=3)
        self._build_buttons(); self.buttons.grid(row=5,column=0,columnspan=3,sticky="ew")
        self.status=tk.StringVar(value="Listo"); ttk.Label(self,textvariable=self.status).grid(row=6,column=0,columnspan=3,sticky="ew")
        self.rowconfigure(3,weight=1); self.columnconfigure(1,weight=1)

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

    def _build_hectare_config_info(self):
        self.hectare_info=ttk.LabelFrame(self,text="Configuración cuota Ha")
        self.hectare_config_text=tk.StringVar(value="")
        ttk.Label(self.hectare_info,textvariable=self.hectare_config_text,wraplength=1100).pack(side="left",fill="x",expand=True,padx=6)
        self._refresh_hectare_config_label()

    def _refresh_hectare_config_label(self):
        m=self.active_master
        self.hectare_config_text.set(f"{str(m.price_per_hectare).replace('.', ',')} €/ha | Superficie: {', '.join(m.surface_crops)} | Kilos: {', '.join(m.delivery_crops)}")

    def open_hectare_fee_master(self):
        self._open_hectare_fee_master()

    def show_about(self):
        self._show_about()

    def _show_about(self):
        messagebox.showinfo("Acerca de", "Liquidaciones - Remesas\nModo prueba SQLite\nVersión actual del módulo")

    def _open_hectare_fee_master(self):
        try:
            HectareFeeMasterDialog(self.winfo_toplevel(), self.hectare_master_service, on_saved=self._hectare_master_saved)
        except Exception:
            logger.exception("No se ha podido abrir el maestro de cuota por hectárea")
            messagebox.showerror("Maestro cuota Ha", "No se ha podido abrir el maestro de cuota por hectárea.")

    def _hectare_master_saved(self):
        self.active_master = self.hectare_master_service.load_master()
        self._refresh_hectare_config_label()
        self._invalidate_master_changed()

    def _connect(self):
        try:
            self.conn=self.db.connect_fruta_with_eepp(); self.meta=ContextService(MetadataRepository(self.conn)); self.deliveries=DeliveriesService(DeliveriesRepository(self.conn)); self.remesas=RemesasService(RemesasRepository(self.conn))
            self.context_panel.campaña_cb["values"]=self.meta.campaigns(); self.context_panel.set_status(self.db.status()); self.hectare_master_service=HectareFeeMasterService(self.master_repository, HectareFeeCropRepository(self.conn)); self.calculations=CalculationService(self.conn, self.config); self._refresh_database_status(); self._refresh_action_states()
        except Exception as exc:
            logger.exception("No se ha podido abrir la copia local de las bases SQLite")
            messagebox.showerror("Error", "No se han podido preparar las bases de datos.\n\nDetalle:\nNo existe una copia local válida para abrir en modo lectura.\n\nRevise la conexión de red o utilice la última copia local disponible.")

    def synchronize_local_databases(self, manual: bool = False) -> bool:
        if manual and self.calculation_valid and not messagebox.askyesno("Actualizar bases locales", "Existe un cálculo activo. La actualización limpiará los resultados actuales. ¿Continuar?"):
            return False
        try:
            service=LocalDatabaseSyncService(self.config, progress_callback=self.status.set if hasattr(self, "status") else None)
            self.sync_results=service.synchronize_all()
            errors=[r for r in self.sync_results if not (r.synchronized or r.used_local_fallback)]
            if errors:
                detail="\n".join(f"{r.database_name}: {r.error_message}" for r in errors)
                messagebox.showerror("Bases de datos", f"No se han podido preparar las bases de datos.\n\nDetalle:\n{detail}\n\nRevise la conexión de red o utilice la última copia local disponible.")
                return False
            if manual:
                self._reconnect_after_sync()
                messagebox.showinfo("Bases locales", self._fallback_warning_text() or "Bases locales actualizadas desde red.")
            self._refresh_database_status()
            return True
        except Exception as exc:
            logger.exception("Error de sincronización manual")
            messagebox.showerror("Bases de datos", f"No se han podido preparar las bases de datos.\n\nDetalle:\n{exc}\n\nRevise la conexión de red o utilice la última copia local disponible.")
            return False

    def _reconnect_after_sync(self):
        if self.conn is not None:
            self.conn.close()
        self._clear()
        self._connect()

    def open_data_folder(self):
        Path(self.config.local_database_dir).mkdir(parents=True, exist_ok=True)
        if hasattr(os, "startfile"):
            os.startfile(self.config.local_database_dir)
        else:
            messagebox.showinfo("Carpeta de datos", self.config.local_database_dir)

    def _fallback_warning_text(self) -> str:
        if not any(r.used_local_fallback for r in self.sync_results):
            return ""
        lines=["No se ha podido acceder a las bases de red.", "", "La aplicación utilizará la última copia local válida:"]
        for r in self.sync_results:
            if r.used_local_fallback:
                stamp=r.local_modified_at.strftime("%d/%m/%Y %H:%M") if r.local_modified_at else "fecha desconocida"
                lines.append(f"{r.database_name}: {stamp}")
        lines += ["", "Los datos pueden no estar actualizados."]
        return "\n".join(lines)

    def _refresh_database_status(self):
        if not hasattr(self, "db_status_text"):
            return
        status=self.db.status()
        parts=[]
        for key in ("DBfruta", "DBEEPPL"):
            parts.append(f"{key} local: {status.get(key, 'No accesible')}")
        if self.sync_results:
            parts.append(" | ".join(sorted({r.status for r in self.sync_results})))
        self.db_status_text.set("   ".join(parts))

    def _context_changed(self):
        self.current_remesa=None; self.remesa_panel.load({}); self._clear_selected_varieties(invalidate=False); self.deliveries_panel.clear(); self.summary_panel.clear(); self.current_calculation=None; self.calculation_valid=False; self.current_deliveries=[]
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
    def _clear_selected_varieties(self, invalidate: bool = True):
        self.selected.delete(0,"end")
        if invalidate:
            self._invalidate_calculation()
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
        has_valid_context = self._context_ready()
        has_varieties = bool(self.selected.get(0,"end"))
        has_deliveries = bool(self.current_deliveries) or bool(self.deliveries_panel.visible_rows())
        has_valid_calculation = bool(self.current_calculation and self.current_calculation.result and self.calculation_valid and self.current_calculation.result.member_results)
        can_search = has_valid_context
        can_calculate = has_valid_context and has_varieties and has_deliveries
        can_preview = has_deliveries
        can_export_calculation = has_valid_calculation
        if hasattr(self, "action_buttons"):
            self.action_buttons["Buscar entregas"].configure(state="normal" if can_search else "disabled")
            self.action_buttons["Calcular liquidación"].configure(state="normal" if can_calculate else "disabled")
            self.action_buttons["Vista previa"].configure(state="normal" if can_preview else "disabled")
            self.action_buttons["Guardar liquidaciones"].configure(state="disabled")
            self.action_buttons["Anular liquidación"].configure(state="disabled")
            if "Exportar resumen de liquidación" in self.action_buttons: self.action_buttons["Exportar resumen de liquidación"].configure(state="normal" if can_export_calculation else "disabled")
            if "Generar PDF de liquidación" in self.action_buttons: self.action_buttons["Generar PDF de liquidación"].configure(state="normal" if can_export_calculation else "disabled")

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
            self.status.set(f"Liquidación calculada: {self.current_calculation.member_count} socios, {format_decimal_es(self.current_calculation.net_kg, 3)} kg, importe comercial {format_currency_es(self.current_calculation.commercial_amount)}")
            self._refresh_action_states()
        except Exception as exc:
            messagebox.showerror("Error", str(exc))

    def _calculation_remesa(self):
        base = dict(self.current_remesa.values) if self.current_remesa else {}
        base.update({k:v.get() for k,v in self.price_vars.items()})
        ctx=self.context_panel.context(); data=self.remesa_panel.data()
        base.update({"CAMPAÑA":ctx.campana,"EMPRESA":ctx.empresa,"CULTIVO":ctx.cultivo,"REMESA":data.get("remesa"),"FECHARE":data.get("fecha_pago"),"PERIODO1":data.get("desde"),"PERIODO2":data.get("hasta"),"TipoLiq":data.get("tipo"),"CATEGORIA":data.get("categoria"),"IdSocio":data.get("socio"),"VARIEDAD":", ".join(self.selected.get(0,"end")),"AplRec":"S" if self.apply_collection_var.get() else "N","AplTte":"S" if self.apply_transport_var.get() else "N","AplCal":"S" if self.apply_quality_var.get() else "N","AplGlobal":"S" if self.apply_globalgap_var.get() else "N","AplCHa":"S" if self.apply_hectare_fee_var.get() else "N","AplPrecalibrado":"S" if self.apply_precalibrated_var.get() else "N"})
        return Remesa(base)

    def _invalidate_master_changed(self):
        if getattr(self, "current_calculation", None):
            self.current_calculation = None
            logger.info("Invalidación de cálculo por cambio de maestro cuota Ha")
        self.calculation_valid = False
        self.status.set("La configuración de cuota Ha ha cambiado. Vuelva a calcular la liquidación.")
        self._refresh_action_states()

    def _invalidate_calculation(self):
        if getattr(self, "calculation_valid", False):
            self.calculation_valid=False; self.status.set("Los parámetros han cambiado. Vuelva a calcular la liquidación.")
        self._refresh_action_states()

    def _concept_text(self, value, status=None):
        if value is not None:
            return format_currency_es(value)
        if status and getattr(status, "value", "") == "not_applicable":
            return "No aplica"
        if status and getattr(status, "value", "") == "error":
            return "Error"
        return "Pendiente"

    def _sort_tree(self, tree, col, reverse=False):
        data = [(tree.set(k, col), k) for k in tree.get_children("")]
        def key(item):
            text = str(item[0]).replace(".", "").replace(",", ".").replace(" €", "").replace(" %", "")
            try: return float(text)
            except ValueError: return str(item[0]).lower()
        data.sort(key=key, reverse=reverse)
        for index, (_, k) in enumerate(data): tree.move(k, "", index)
        tree.heading(col, command=lambda: self._sort_tree(tree, col, not reverse))

    def _preview(self):
        if not self.deliveries_panel.visible_rows():
            return
        win=tk.Toplevel(self); win.title("Vista previa de liquidación"); win.transient(self.winfo_toplevel()); win.grab_set(); win.geometry("1280x760"); win.resizable(True, True)
        nb=ttk.Notebook(win); nb.pack(fill="both", expand=True, padx=8, pady=8)
        summary_tab=ttk.Frame(nb); members_tab=ttk.Frame(nb); detail_tab=ttk.Frame(nb)
        nb.add(summary_tab,text="Resumen"); nb.add(members_tab,text="Socios"); nb.add(detail_tab,text="Detalle")
        ctx=self.context_panel.context(); data=self.remesa_panel.data(); selected=', '.join(self.selected.get(0,'end')) or "Todas"; calc=self.current_calculation

        ident=ttk.LabelFrame(summary_tab,text="Identificación"); ident.grid(row=0,column=0,sticky="nsew",padx=6,pady=6)
        rows=[("Remesa",data.get('remesa')), ("Campaña",ctx.campana), ("Empresa",ctx.empresa), ("Cultivo",ctx.cultivo), ("Periodo",f"{format_display_date(data.get('desde'))} - {format_display_date(data.get('hasta'))}"), ("Fecha de pago",format_display_date(data.get('fecha_pago'))), ("Tipo de liquidación",data.get('tipo')), ("Categoría",data.get('categoria')), ("Socio",data.get('socio') or "Todos"), ("Variedades",selected), ("Precio €/ha activo", str(calc.result.hectare_fee_master.price_per_hectare).replace(".", ",") if calc and calc.result and calc.result.hectare_fee_master else ""), ("Cultivos superficie activos", ", ".join(calc.result.hectare_fee_master.surface_crops) if calc and calc.result and calc.result.hectare_fee_master else ""), ("Cultivos entrega activos", ", ".join(calc.result.hectare_fee_master.delivery_crops) if calc and calc.result and calc.result.hectare_fee_master else "")]
        for i,(k,v) in enumerate(rows):
            r=i//2; c=(i%2)*2; ttk.Label(ident,text=k,font=("TkDefaultFont",9,"bold")).grid(row=r,column=c,sticky="w",padx=4,pady=2); ttk.Label(ident,text=v,wraplength=360).grid(row=r,column=c+1,sticky="w",padx=4,pady=2)
        opts=ttk.LabelFrame(summary_tab,text="Opciones aplicadas"); opts.grid(row=1,column=0,sticky="ew",padx=6,pady=6)
        labels=[("Recolección",self.apply_collection_var), ("Transporte",self.apply_transport_var), ("Calidad",self.apply_quality_var), ("GlobalGAP",self.apply_globalgap_var), ("Cuota por hectárea",self.apply_hectare_fee_var), ("Precalibrado",self.apply_precalibrated_var)]
        for i,(name,var) in enumerate(labels): ttk.Label(opts,text=("✓ " if var.get() else "✗ ")+name).grid(row=i//3,column=i%3,sticky="w",padx=12,pady=3)
        origin=ttk.LabelFrame(summary_tab,text="Datos de origen"); origin.grid(row=0,column=1,rowspan=2,sticky="nsew",padx=6,pady=6)
        if self.summary:
            origin_rows=[("Entregas",self.summary.total_entregas),("Socios",self.summary.socios),("Variedades",self.summary.variedades),("Kilos netos efectivos",format_decimal_es(self.summary.kilos_netos,2)),("Primera fecha",self.summary.primera_fecha),("Última fecha",self.summary.ultima_fecha),("Entregas ya liquidadas",self.summary.liquidadas),("Registros con incidencias",self.summary.sin_variedad+self.summary.sin_socio_valido+self.summary.sin_categoria)]
            for i,(k,v) in enumerate(origin_rows): ttk.Label(origin,text=k,font=("TkDefaultFont",9,"bold")).grid(row=i,column=0,sticky="w",padx=4,pady=3); ttk.Label(origin,text=v).grid(row=i,column=1,sticky="w",padx=4,pady=3)
        econ=ttk.LabelFrame(summary_tab,text="Totales económicos"); econ.grid(row=2,column=0,columnspan=2,sticky="nsew",padx=6,pady=6)
        ecols=("Concepto","Importe"); etree=ttk.Treeview(econ,columns=ecols,show="headings",height=10); [etree.heading(c,text=c) for c in ecols]; etree.column("Concepto",width=220); etree.column("Importe",width=180,anchor="e"); etree.pack(fill="both",expand=True)
        totals=calc.result.totals if calc and calc.result else None
        concepts=[("Kilos netos efectivos", totals.net_kg if totals else None, None),("Importe comercial", calc.commercial_amount if calc else None, None),("Recolección", getattr(totals,'collection_amount',None), None),("Transporte", getattr(totals,'transport_amount',None), None),("Calidad", getattr(totals,'quality_amount',None), None),("GlobalGAP", getattr(totals,'globalgap_amount',None), None),("Cuota Ha", getattr(totals,'hectare_fee_amount',None), None),("Base imponible", getattr(totals,'taxable_base',None), None),("IVA", getattr(totals,'vat_amount',None), None),("Retención", getattr(totals,'withholding_amount',None), None),("Total", getattr(totals,'total_amount',None), None)]
        for name,val,st in concepts: etree.insert("","end",values=(name,self._concept_text(val,st)),tags=("strong",) if name in {"Base imponible","Total"} else ())
        etree.tag_configure("strong",font=("TkDefaultFont",9,"bold"))
        warn=ttk.LabelFrame(summary_tab,text="Advertencias"); warn.grid(row=3,column=0,columnspan=2,sticky="nsew",padx=6,pady=6)
        wlist=tk.Listbox(warn,height=4); wlist.pack(fill="both",expand=True)
        for msg in ((calc.warnings if calc else []) or ["Cálculo pendiente de ejecutar."]): wlist.insert("end",msg)
        summary_tab.columnconfigure(0,weight=1); summary_tab.columnconfigure(1,weight=1); summary_tab.rowconfigure(2,weight=1)

        mcols=("Nº socio","Socio","Variedad","Entregas","Neto efectivo","Neto comercial","Neto destrío","Neto podrido","Importe comercial","Recolección","Transporte","Calidad","GlobalGAP","Cuota Ha","Base imponible","IVA","Retención","Total","Precio medio","Ha","Cuota anual","Kg totales Ha","€/kg Ha","Kg línea Ha","Cuota parcial Ha","Estado Ha")
        mtree=ttk.Treeview(members_tab,columns=mcols,show="headings");
        for c in mcols: mtree.heading(c,text=c,command=lambda c=c: self._sort_tree(mtree,c)); mtree.column(c,width=120,anchor="e" if c not in {"Socio","Variedad"} else "w")
        my=ttk.Scrollbar(members_tab,orient="vertical",command=mtree.yview); mx=ttk.Scrollbar(members_tab,orient="horizontal",command=mtree.xview); mtree.configure(yscrollcommand=my.set,xscrollcommand=mx.set); mtree.grid(row=0,column=0,sticky="nsew"); my.grid(row=0,column=1,sticky="ns"); mx.grid(row=1,column=0,sticky="ew"); members_tab.rowconfigure(0,weight=1); members_tab.columnconfigure(0,weight=1)
        dcols=("Nº socio","Socio","Variedad","Registro","Concepto","Coste_Recoleccion","SSocialRecoleccion","Manijeria","Recolección entrega","Coste_Trans","Kilos","Precio","Importe")
        dtree=ttk.Treeview(detail_tab,columns=dcols,show="headings");
        for c in dcols: dtree.heading(c,text=c,command=lambda c=c: self._sort_tree(dtree,c)); dtree.column(c,width=140,anchor="e" if c not in {"Socio","Variedad","Concepto"} else "w")
        dy=ttk.Scrollbar(detail_tab,orient="vertical",command=dtree.yview); dx=ttk.Scrollbar(detail_tab,orient="horizontal",command=dtree.xview); dtree.configure(yscrollcommand=dy.set,xscrollcommand=dx.set); dtree.grid(row=0,column=0,sticky="nsew"); dy.grid(row=0,column=1,sticky="ns"); dx.grid(row=1,column=0,sticky="ew"); detail_tab.rowconfigure(0,weight=1); detail_tab.columnconfigure(0,weight=1)
        if calc and calc.result:
            for m in calc.result.member_results:
                mtree.insert("","end",values=(m.member_id,m.member_name,m.variety,m.delivery_count,format_decimal_es(m.net_kg,2),format_decimal_es(m.commercial_kg,2),format_decimal_es(m.destruction_kg,2),format_decimal_es(m.rotten_kg,2),format_currency_es(m.commercial_amount),self._concept_text(m.collection_amount),self._concept_text(m.transport_amount),self._concept_text(m.quality_amount),self._concept_text(m.globalgap_amount),self._concept_text(m.hectare_fee_amount),self._concept_text(m.taxable_base),self._concept_text(m.vat_amount),self._concept_text(m.withholding_amount),self._concept_text(m.total_amount),format_price_es(m.commercial_average_price or 0),format_decimal_es(m.applicable_hectares,4),format_currency_es(m.hectare_fee_total_member),format_decimal_es(m.hectare_fee_total_effective_kg,2),format_price_es(m.hectare_fee_rate_per_kg or 0),format_decimal_es(m.net_kg,2),self._concept_text(m.hectare_fee_amount,m.hectare_fee_status),getattr(m.hectare_fee_status,"value",m.hectare_fee_status)))
                for g in m.grades:
                    if g.kilograms or g.price: dtree.insert("","end",values=(m.member_id,m.member_name,m.variety,"",g.label,"","","","","",format_decimal_es(g.kilograms,2),format_price_es(g.price),format_currency_es(g.amount)))
                for d in m.source_deliveries:
                    collection = d.collection_cost + d.social_security_collection + d.foreman_cost
                    dtree.insert("","end",values=(m.member_id,m.member_name,m.variety,d.registro,"Costes de entrada",format_currency_es(d.collection_cost),format_currency_es(d.social_security_collection),format_currency_es(d.foreman_cost),format_currency_es(collection),format_currency_es(d.transport_cost),format_decimal_es(d.effective_net_kg,2),"",""))
        buttons=ttk.Frame(win); buttons.pack(fill="x",padx=8,pady=(0,8))
        ttk.Button(buttons,text="Cerrar",command=win.destroy).pack(side="right",padx=3)
        ttk.Button(buttons,text="Copiar resumen",command=lambda: (win.clipboard_clear(), win.clipboard_append(f"{data.get('remesa')} - {format_currency_es(calc.commercial_amount) if calc else 'Pendiente'}"))).pack(side="right",padx=3)
        state="normal" if calc and calc.result else "disabled"
        ttk.Button(buttons,text="Generar PDF",command=self._export_liquidation_pdf,state=state).pack(side="right",padx=3)
        ttk.Button(buttons,text="Exportar resumen a Excel",command=self._export_liquidation_excel,state=state).pack(side="right",padx=3)
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
        try:
            path=export_liquidation_summary(self.current_calculation.result, self._output_dir()/"resumen_liquidaciones.xlsx")
        except FileLockedError as exc:
            logger.warning("Excel bloqueado: %s", exc.path)
            messagebox.showwarning(
                "Archivo Excel abierto",
                "No se puede actualizar el resumen porque el archivo está abierto o bloqueado.\n\n"
                f"Cierre el archivo:\n{exc.path}\n\n"
                "y vuelva a pulsar \"Exportar resumen de liquidación\"."
            )
            return
        except Exception as exc:
            logger.exception("Error exportando el resumen de liquidación")
            messagebox.showerror("Exportar resumen", f"No se ha podido generar el Excel:\n{exc}")
            return
        messagebox.showinfo("Exportación completada", f"El resumen se ha guardado en:\n{path}")

    def _export_liquidation_pdf(self):
        if not (self.current_calculation and self.current_calculation.result and self.calculation_valid): return
        path=export_member_pdf(self.current_calculation.result, self._output_dir()/"liquidacion_socios.pdf")
        messagebox.showinfo("Exportación", f"PDF de liquidación creado: {path}")
