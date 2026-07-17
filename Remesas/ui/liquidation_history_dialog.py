from __future__ import annotations

import logging
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

from services.path_opener import open_path

logger = logging.getLogger(__name__)


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
            self.tree.heading(column, text=column.title())
            self.tree.column(column, width=125)
        self.tree.column("ruta", width=310)
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)
        self.tree.bind("<Double-1>", lambda _event: self.open_pdf())
        bar = ttk.Frame(self); bar.pack(fill="x", padx=8, pady=8)
        for text, command in (("Abrir PDF", self.open_pdf), ("Abrir carpeta", self.open_folder), ("Regenerar", self.regenerate), ("Cerrar", self.destroy)):
            ttk.Button(bar, text=text, command=command).pack(side="left", padx=3)
        self.refresh()

    def refresh(self):
        self.tree.delete(*self.tree.get_children())
        for batch_id in self.batch_ids:
            for document in self.history.list_recipient_documents(batch_id):
                self.tree.insert("", "end", values=(batch_id, document["remittance_id"], document["recipient_member_id"], document["recipient_name"], document["line_count"], document["id_liqs"], document["file_path"], document["status"]))
        children = self.tree.get_children()
        if children:
            self.tree.selection_set(children[0]); self.tree.focus(children[0])

    def selected(self):
        selected = self.tree.selection()
        return self.tree.item(selected[0], "values") if selected else None

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
        super().__init__(parent); self.history=history; self.title("Liquidaciones guardadas — Historial"); self.geometry("1300x650"); self.transient(parent)
        filters=ttk.LabelFrame(self,text="Filtros"); filters.pack(fill="x",padx=8,pady=8); self.vars={}
        for i,name in enumerate(("campaign","company","crop","remittance_id","member_id","date_from","date_to","status")):
            ttk.Label(filters,text=name.replace("_"," ").title()).grid(row=0,column=i,sticky="w"); self.vars[name]=tk.StringVar(); ttk.Entry(filters,textvariable=self.vars[name],width=14).grid(row=1,column=i,padx=2)
        ttk.Button(filters,text="Buscar",command=self.refresh).grid(row=1,column=8,padx=6)
        cols=("batch_id","remesa","fecha","cultivo","campaña","empresa","líneas","destinatarios","pdfs","estado","creado")
        self.tree=ttk.Treeview(self,columns=cols,show="headings")
        for c in cols:self.tree.heading(c,text=c.title()); self.tree.column(c,width=105)
        self.tree.column("batch_id",width=240); self.tree.pack(fill="both",expand=True,padx=8,pady=4)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=8,pady=8)
        self.void_button = None
        for text,cmd in (("Ver detalle",self.detail),("Visualizar PDFs",self.documents),("Regenerar PDFs",self.regenerate),("Anular",self.void),("Abrir carpeta",self.folder),("Cerrar",self.destroy)):
            button=ttk.Button(bar,text=text,command=cmd); button.pack(side="left",padx=3)
            if text == "Anular": self.void_button = button
        self.tree.bind("<<TreeviewSelect>>", lambda _event: self._update_actions()); self.refresh()
    def batch_id(self):
        s=self.tree.selection(); return self.tree.item(s[0],"values")[0] if s else None
    def refresh(self):
        self.tree.delete(*self.tree.get_children()); filters={k:v.get().strip() for k,v in self.vars.items() if v.get().strip()}
        for b in self.history.list_batches(filters): self.tree.insert("","end",values=(b["batch_id"],b["remesa_id"],b["payment_date"],b["crop"],b["campaign"],b["company"],b["line_count"],b["recipient_count"],b["document_count"],b["status"],b["created_at"]))
        self._update_actions()
    def _update_actions(self):
        bid=self.batch_id(); batch=self.history.get_batch_detail(bid)["batch"] if bid else None
        self.void_button.configure(state="normal" if batch and batch["status"] == "ACTIVE" else "disabled")
    def detail(self):
        bid=self.batch_id()
        if bid:
            d=self.history.get_batch_detail(bid); messagebox.showinfo("Detalle",f"Batch: {bid}\nRemesa: {d['batch']['remesa_name']}\nEstado: {d['batch']['status']}\nLíneas: {len(d['lines'])}",parent=self)
    def documents(self):
        if self.batch_id(): DocumentSelectorDialog(self,self.history,(self.batch_id(),))
    def regenerate(self):
        if self.batch_id(): self.history.regenerate_documents(self.batch_id()); self.refresh()
    def void(self):
        bid=self.batch_id(); batch=self.history.get_batch_detail(bid)["batch"] if bid else None
        if not batch or batch["status"] != "ACTIVE": self._update_actions(); return
        reason=simpledialog.askstring("Anular liquidación","Motivo obligatorio:",parent=self)
        if reason and messagebox.askyesno("Confirmar",f"¿Anular la remesa {batch['remesa_name']}?",parent=self): self.history.void_batch(bid,reason); logger.info("[BatchVoided]\nbatch_id=%s\nreason=%s",bid,reason); self.refresh()
    def folder(self):
        bid=self.batch_id(); docs=self.history.list_recipient_documents(bid) if bid else ()
        if docs:
            try: open_path(Path(docs[0]["file_path"]).parent)
            except Exception as exc: messagebox.showerror("Abrir carpeta", str(exc), parent=self)
