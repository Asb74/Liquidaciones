from __future__ import annotations

import logging
import threading
from datetime import date, datetime
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

from ui.widgets.member_search_entry import MemberSearchEntry
from ui.widgets.nullable_date_entry import NullableDateEntry

from services.path_opener import open_path

logger = logging.getLogger(__name__)


FILTER_LABELS = {
    "campaign": "Campaña",
    "company": "Empresa",
    "crop": "Cultivo",
    "remittance_id": "N.º remesa",
    "member_id": "N.º socio",
    "date_from": "Fecha desde",
    "date_to": "Fecha hasta",
    "status": "Estado",
}

COLUMN_LABELS = {
    "batch_id": "Id. de lote",
    "remesa": "Remesa",
    "fecha": "Fecha",
    "cultivo": "Cultivo",
    "campaña": "Campaña",
    "empresa": "Empresa",
    "líneas": "Líneas",
    "destinatarios": "Destinatarios",
    "pdfs": "PDF",
    "estado": "Estado",
    "creado": "Creado",
}

DOCUMENT_COLUMN_LABELS = {
    "batch": "Id. de lote",
    "remesa": "Remesa",
    "socio": "N.º socio",
    "nombre": "Socio",
    "lineas": "Líneas",
    "idliq": "IdLiq",
    "ruta": "Ruta",
    "estado": "Estado",
}

STATUS_FILTER_VALUES = {
    "Todos": "",
    "Activa": "ACTIVE",
    "Anulada": "VOIDED",
    "Sustituida": "SUPERSEDED",
    "Parcial": "PARTIAL",
}


def _optional_filter(value):
    """Convert UI's unfiltered sentinel into the repository's ``None`` value."""
    return None if value in (None, "", "Todos") else value


def _status_label(value: str) -> str:
    labels = {
        "ACTIVE": "Activa",
        "VOIDED": "Anulada",
        "PARTIAL": "Parcial",
        "FAILED": "Error",
        "GENERATED": "Generado",
        "SUPERSEDED": "Sustituido",
    }
    return labels.get(str(value or "").upper(), str(value or ""))


def _date_label(value, *, include_time=False) -> str:
    """Format common persisted ISO dates without changing their stored value."""
    if value in (None, ""):
        return ""
    original = str(value)
    try:
        parsed = value if isinstance(value, (date, datetime)) else datetime.fromisoformat(original.replace("Z", "+00:00"))
        if include_time:
            if isinstance(parsed, datetime):
                return parsed.strftime("%d/%m/%Y %H:%M")
            return parsed.strftime("%d/%m/%Y 00:00")
        return parsed.strftime("%d/%m/%Y")
    except (TypeError, ValueError):
        return original


class DocumentSelectorDialog(tk.Toplevel):
    def __init__(self, parent, history, batch_ids):
        super().__init__(parent)
        self.history = history
        self.batch_ids = tuple(batch_ids)
        self.title("Documentos definitivos")
        self.geometry("1200x520")
        self.transient(parent)
        columns = ("batch", "remesa", "socio", "nombre", "lineas", "idliq", "ruta", "estado")
        self.tree = ttk.Treeview(self, columns=columns, show="headings")
        for column in columns:
            self.tree.heading(column, text=DOCUMENT_COLUMN_LABELS[column])
            self.tree.column(column, width=125)
        self.tree.column("ruta", width=310)
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)
        self.tree.bind("<Double-1>", lambda _event: self.open_pdf())
        self._documents = {}
        bar = ttk.Frame(self); bar.pack(fill="x", padx=8, pady=8)
        for text, command in (("Abrir PDF", self.open_pdf), ("Abrir carpeta", self.open_folder), ("Regenerar PDF", self.regenerate), ("Cerrar", self.destroy)):
            ttk.Button(bar, text=text, command=command).pack(side="left", padx=3)
        self.refresh()

    def refresh(self):
        self.tree.delete(*self.tree.get_children())
        self._documents.clear()
        for batch_id in self.batch_ids:
            for document in self.history.list_recipient_documents(batch_id):
                values = (batch_id, document["remittance_id"], document["recipient_member_id"], document["recipient_name"], document["line_count"], document["id_liqs"], document["file_path"], document["status"])
                item = self.tree.insert("", "end", values=(*values[:-1], _status_label(values[-1])))
                self._documents[item] = values
        children = self.tree.get_children()
        if children:
            self.tree.selection_set(children[0]); self.tree.focus(children[0])

    def selected(self):
        selected = self.tree.selection()
        return self._documents.get(selected[0]) if selected else None

    def _open(self, path, *, batch_id, recipient_member_id):
        try:
            open_path(path)
            logger.info("[DocumentOpened]\nbatch_id=%s\nrecipient_member_id=%s\npath=%s", batch_id, recipient_member_id, path)
        except Exception as exc:
            logger.exception("[DocumentOpenFailed]\npath=%s\nerror=%s", path, exc)
            messagebox.showerror("Abrir documento", f"No se pudo abrir el documento:\n\n{path}\n\nDetalle:\n{exc}\n\nPuede regenerarlo desde esta ventana.", parent=self)

    def open_pdf(self):
        row = self.selected()
        if not row: return
        if row[7] == "FAILED":
            messagebox.showerror("Documento fallido", "Este documento no se generó correctamente. Puede regenerarlo.", parent=self); return
        batch = self.history.get_batch_detail(row[0])["batch"]
        if batch and batch["status"] == "VOIDED":
            messagebox.showwarning("Liquidación anulada", "Este documento pertenece a una liquidación anulada.", parent=self)
        self._open(Path(row[6]), batch_id=row[0], recipient_member_id=row[2])

    def open_folder(self):
        row = self.selected()
        if row: self._open(Path(row[6]).parent, batch_id=row[0], recipient_member_id=row[2])

    def regenerate(self):
        row = self.selected()
        if not row: return
        result = self.history.regenerate_documents(row[0], int(row[2]))
        self.refresh()
        if result.generated_documents:
            logger.info("[DocumentRegenerated]\nbatch_id=%s\nrecipient_member_id=%s\npath=%s", row[0], row[2], result.generated_documents[0].path)
        messagebox.showinfo("Regenerar", f"Generados: {len(result.generated_documents)}\nErrores: {len(result.failed_documents)}", parent=self)


class LiquidationHistoryDialog(tk.Toplevel):
    def __init__(self, parent, history):
        super().__init__(parent); self.history=history; self.title("Liquidaciones guardadas — Historial"); self.geometry("1300x720"); self.transient(parent)
        self.vars={name:tk.StringVar(value="Todos") for name in ("campaign","company","crop","remittance_id","status")}
        self._updating_filter_options=False
        self.export_all_filtered=tk.BooleanVar(value=False); self.remittance_display_to_id={}; self._last_export_batch_ids=()
        filters=ttk.LabelFrame(self,text="Filtros"); filters.pack(fill="x",padx=8,pady=8)
        self.combos={}
        for col,(key,label) in enumerate((("campaign","Campaña"),("company","Empresa"),("crop","Cultivo"),("remittance_id","N.º remesa"),("status","Estado"))):
            ttk.Label(filters,text=label).grid(row=0,column=col,sticky="w",padx=2)
            combo=ttk.Combobox(filters,textvariable=self.vars[key],state="readonly",width=20); combo.grid(row=1,column=col,sticky="ew",padx=2); self.combos[key]=combo
        self.combos['campaign'].bind('<<ComboboxSelected>>', lambda _e:self._campaign_changed()); self.combos['company'].bind('<<ComboboxSelected>>', lambda _e:self._company_changed()); self.combos['crop'].bind('<<ComboboxSelected>>', lambda _e:self._crop_changed())
        ttk.Label(filters,text="Socio").grid(row=2,column=0,sticky="w",padx=2); self.member_search=MemberSearchEntry(filters,self._search_members,width=52); self.member_search.grid(row=3,column=0,columnspan=2,sticky="ew",padx=2)
        self.date_from_picker=NullableDateEntry(filters, "Desde"); self.date_from_picker.grid(row=3,column=2,sticky="w",padx=2)
        self.date_to_picker=NullableDateEntry(filters, "Hasta"); self.date_to_picker.grid(row=3,column=3,sticky="w",padx=2)
        ttk.Checkbutton(filters,text="Exportar todas las liquidaciones filtradas",variable=self.export_all_filtered,command=self._update_scope_label).grid(row=3,column=4,sticky="w",padx=4)
        ttk.Button(filters,text="Buscar",command=self.refresh).grid(row=4,column=3,pady=5); ttk.Button(filters,text="Limpiar filtros",command=self.clear_filters).grid(row=4,column=4,pady=5)
        for i in range(5): filters.columnconfigure(i,weight=1)
        self.summary_var=tk.StringVar(); ttk.Label(self,textvariable=self.summary_var).pack(anchor="w",padx=12)
        cols=("batch_id","remesa","fecha","cultivo","campaña","empresa","líneas","destinatarios","pdfs","estado","creado")
        self.tree=ttk.Treeview(self,columns=cols,show="headings",selectmode="extended")
        widths={"batch_id":240,"remesa":100,"fecha":95,"cultivo":110,"campaña":80,"empresa":70,"líneas":70,"destinatarios":95,"pdfs":60,"estado":80,"creado":135}
        for c in cols:self.tree.heading(c,text=COLUMN_LABELS[c]); self.tree.column(c,width=widths[c])
        self.tree.pack(fill="both",expand=True,padx=8,pady=4)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=8,pady=8); self.void_button=None; self.export_button=None; self.regenerate_csv_button=None
        for text,cmd in (("Seleccionar todo lo visible",self.select_visible),("Limpiar selección",self.clear_selection),("Ver detalle",self.detail),("Visualizar PDF",self.documents),("Regenerar PDF",self.regenerate),("Exportar CSV",self.export_csv),("Abrir último CSV",self.open_last_csv),("Regenerar CSV",self.regenerate_csv),("Anular liquidación",self.void),("Abrir carpeta",self.folder),("Cerrar",self.destroy)):
            button=ttk.Button(bar,text=text,command=cmd); button.pack(side="left",padx=3)
            if text == "Anular liquidación": self.void_button=button
            if text == "Exportar CSV": self.export_button=button
            if text == "Regenerar CSV": self.regenerate_csv_button=button
        self.tree.bind("<<TreeviewSelect>>", lambda _event:self._update_actions()); self._load_options(); self.refresh()
    def _filters(self):
        date_from = self.date_from_picker.iso_value()
        date_to = self.date_to_picker.iso_value()
        if date_from and date_to and date_from > date_to:
            raise ValueError("La fecha desde no puede ser posterior a la fecha hasta.")
        return {"campaign": _optional_filter(self.vars['campaign'].get()), "company": _optional_filter(self.vars['company'].get()), "crop": _optional_filter(self.vars['crop'].get()), "remittance_id": _optional_filter(self.remittance_display_to_id.get(self.vars['remittance_id'].get())), "status": _optional_filter(STATUS_FILTER_VALUES.get(self.vars['status'].get())), "member_id": _optional_filter(self.member_search.selected_member_id), "date_from": _optional_filter(date_from), "date_to": _optional_filter(date_to)}

    def _load_options(self):
        try:
            filters = self._filters()
            options = self.history.list_history_filter_options(**filters)
        except Exception as exc:
            logger.exception("[HistoryFilterOptionsFailed]")
            messagebox.showerror("Filtros del historial", f"No se pudieron cargar los filtros del historial.\n\nDetalle:\n{exc}", parent=self)
            return
        logger.info("[HistoryFilterOptions]\ncampaigns=%s\ncompanies=%s\ncrops=%s\nremittances=%s", options['campaigns'], options['companies'], options['crops'], options['remittances'])
        self._updating_filter_options = True
        try:
            self.combos['campaign']['values'] = ("Todos", *options['campaigns'])
            self.combos['company']['values'] = ("Todos", *options['companies'])
            self.combos['crop']['values'] = ("Todos", *options['crops'])
            self.combos['status']['values'] = tuple(STATUS_FILTER_VALUES)
            self.remittance_display_to_id = {row['display']: row['id'] for row in options['remittances']}
            self.combos['remittance_id']['values'] = ("Todos", *self.remittance_display_to_id)
        finally:
            self._updating_filter_options = False

    def _campaign_changed(self):
        if self._updating_filter_options: return
        self.vars['company'].set('Todos'); self.vars['crop'].set('Todos'); self.vars['remittance_id'].set('Todos'); self._load_options()
    def _company_changed(self):
        if self._updating_filter_options: return
        self.vars['crop'].set('Todos'); self.vars['remittance_id'].set('Todos'); self._load_options()
    def _crop_changed(self):
        if self._updating_filter_options: return
        self.vars['remittance_id'].set('Todos'); self._load_options()
    def _member_search_filters(self):
        """Return only context filters; a member cannot scope its own lookup."""
        filters = self._filters()
        return {
            "campaign": filters["campaign"],
            "company": filters["company"],
            "crop": filters["crop"],
            "remittance_id": filters["remittance_id"],
            "status": filters["status"],
            "date_from": filters["date_from"],
            "date_to": filters["date_to"],
        }

    def _search_members(self, text):
        return self.history.search_liquidation_members(
            text, **self._member_search_filters()
        )
    def selected_batch_ids(self): return tuple(self.tree.item(item,"values")[0] for item in self.tree.selection())
    def batch_id(self):
        selected=self.selected_batch_ids(); return selected[0] if selected else None
    def refresh(self):
        member_text = self.member_search.member_search_text.get().strip()
        if member_text and self.member_search.selected_member_id is None:
            messagebox.showwarning(
                "Socio", "Seleccione un socio de la lista de resultados.", parent=self
            )
            return
        try: filters=self._filters()
        except ValueError as exc: messagebox.showerror("Filtros",str(exc),parent=self); return
        logger.info("[HistoryFilterReload]\nselected_campaign=%s\nselected_company=%s\nselected_crop=%s\nselected_status=%s", filters['campaign'], filters['company'], filters['crop'], filters['status'])
        try:
            self.tree.delete(*self.tree.get_children())
            for b in self.history.list_batches(filters): self.tree.insert("","end",values=(b['batch_id'],b['remesa_id'],_date_label(b['payment_date']),b['crop'],b['campaign'],b['company'],b['line_count'],b['recipient_count'],b['document_count'],_status_label(b['status']),_date_label(b['created_at'],include_time=True)))
            summary=self.history.history_summary(filters)
        except Exception as exc:
            logger.exception("[HistoryFilterReloadFailed]")
            self.summary_var.set(f"Error al consultar el historial: {exc}")
            messagebox.showerror("Historial", f"No se pudo consultar el historial.\n\nDetalle:\n{exc}", parent=self)
            return
        self.summary_var.set(f"Resultados: {summary['batch_count']} remesas · {summary['line_count']} líneas · {summary['recipient_count']} destinatarios" + (" — alcance de la exportación filtrada" if self.export_all_filtered.get() else "")); self._update_actions()
    def clear_filters(self):
        for key in self.combos: self.vars[key].set("Todos")
        self.member_search.clear(); self.date_from_picker.clear(); self.date_to_picker.clear(); self.export_all_filtered.set(False); self._load_options(); self.refresh()
    def _update_scope_label(self): self.refresh()
    def select_visible(self): self.tree.selection_set(self.tree.get_children())
    def clear_selection(self): self.tree.selection_remove(self.tree.selection())
    def _update_actions(self):
        bid=self.batch_id(); batch=self.history.get_batch_detail(bid)['batch'] if bid else None; self.void_button.configure(state="normal" if batch and batch['status']=='ACTIVE' else 'disabled')
    def detail(self):
        bid=self.batch_id()
        if bid:
            d=self.history.get_batch_detail(bid); chain="\n".join(f"{x['operation_type']}: {x['batch_id']} ({_status_label(x['status'])})" for x in d.get('chain',()))
            messagebox.showinfo("Detalle",f"Id. de lote: {bid}\nRemesa: {d['batch']['remesa_name']}\nEstado: {_status_label(d['batch']['status'])}\nLíneas: {len(d['lines'])}\n\nTrazabilidad:\n{chain or 'Sin rectificaciones'}",parent=self)
    def documents(self):
        if self.batch_id(): DocumentSelectorDialog(self,self.history,(self.batch_id(),))
    def regenerate(self):
        if self.batch_id(): self.history.regenerate_documents(self.batch_id()); self.refresh()
    def export_csv(self):
        try: filters=self._filters()
        except ValueError as exc: messagebox.showerror("Exportar CSV",str(exc),parent=self); return
        batch_ids=self.history.filtered_batch_ids(filters) if self.export_all_filtered.get() else self.selected_batch_ids()
        if not batch_ids: messagebox.showinfo("Exportar CSV","No existen liquidaciones que coincidan con los filtros seleccionados." if self.export_all_filtered.get() else "Seleccione una o varias remesas.",parent=self); return
        if self.export_all_filtered.get() and len(batch_ids)>1:
            s=self.history.history_summary(filters); text=f"Se van a exportar:\n\nRemesas: {s['batch_count']}\nLiquidaciones: {s['line_count']}\nDestinatarios: {s['recipient_count']}\nCampaña: {filters['campaign'] or 'Todas'}\nEmpresa: {filters['company'] or 'Todas'}\nCultivo: {filters['crop'] or 'Todos'}\nFechas: {filters['date_from'] or 'Sin límite'} — {filters['date_to'] or 'Sin límite'}\n\nSe generará un único archivo CSV.\n\n¿Desea continuar?"
            if not messagebox.askyesno("Confirmar exportación",text,parent=self): return
        self._last_export_batch_ids=tuple(batch_ids); self.export_button.configure(state='disabled'); self.regenerate_csv_button.configure(state='disabled')
        def work():
            try: result=self.history.export_csv(batch_ids[0]) if len(batch_ids)==1 else self.history.export_csv_batches(batch_ids)
            except Exception as exc: self.after(0,lambda: done(None,exc)); return
            self.after(0,lambda: done(result,None))
        def done(result,error):
            self.export_button.configure(state='normal'); self.regenerate_csv_button.configure(state='normal')
            if error: messagebox.showerror("Exportar CSV",str(error),parent=self); return
            if result.already_existed: messagebox.showinfo("Exportar CSV","Esta liquidación ya fue exportada a contabilidad.",parent=self); return
            if not result.success: messagebox.showerror("Exportar CSV",result.error_message,parent=self); return
            messagebox.showinfo("Exportar CSV",f"CSV generado correctamente.\n\nLíneas: {result.line_count}\nLíneas excluidas: {result.excluded_line_count}\nNeto: {result.net_total}\nImporte total: {result.amount_total}\nRuta: {result.csv_path}",parent=self)
        threading.Thread(target=work,daemon=True).start()
    def open_last_csv(self):
        batch_ids=self._last_export_batch_ids or self.selected_batch_ids(); generated=self.history.last_csv_export(batch_ids) if batch_ids else None
        if generated:
            try: open_path(generated['file_path'])
            except Exception as exc: messagebox.showerror("Abrir CSV",str(exc),parent=self)
    def regenerate_csv(self):
        batch_ids=self.selected_batch_ids(); generated=self.history.last_csv_export(batch_ids) if batch_ids else None
        if not generated: messagebox.showinfo("Regenerar CSV","No existe una exportación CSV generada para este lote.",parent=self); return
        try:
            result=self.history.regenerate_csv_export(generated['id'])
            if not result.success: raise ValueError(result.error_message)
            messagebox.showinfo("Regenerar CSV",f"CSV regenerado correctamente:\n{result.csv_path}",parent=self)
        except Exception as exc: messagebox.showerror("Regenerar CSV",str(exc),parent=self)
    def void(self):
        bid=self.batch_id(); batch=self.history.get_batch_detail(bid)['batch'] if bid else None
        if not batch or batch['status']!='ACTIVE': self._update_actions(); return
        reason=simpledialog.askstring("Anular liquidación","Motivo obligatorio:",parent=self)
        if reason and messagebox.askyesno("Confirmar",f"¿Anular la remesa {batch['remesa_name']}?",parent=self): self.history.void_batch(bid,reason); logger.info("[BatchVoided]\\nbatch_id=%s\\nreason=%s",bid,reason); self.refresh()
    def folder(self):
        bid=self.batch_id(); docs=self.history.list_recipient_documents(bid) if bid else ()
        if docs:
            try: open_path(Path(docs[0]['file_path']).parent)
            except Exception as exc: messagebox.showerror("Abrir carpeta",str(exc),parent=self)
