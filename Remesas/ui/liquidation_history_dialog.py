from __future__ import annotations

import logging
import os
from pathlib import Path
import subprocess
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

logger=logging.getLogger(__name__)


def open_path(path):
    path=Path(path)
    if os.name == "nt": os.startfile(str(path))
    else: subprocess.Popen(["xdg-open",str(path)])


class DocumentSelectorDialog(tk.Toplevel):
    def __init__(self,parent,history,batch_ids):
        super().__init__(parent); self.history=history; self.title("Documentos definitivos"); self.geometry("1200x520"); self.transient(parent)
        self.tree=ttk.Treeview(self,columns=("batch","remesa","socio","nombre","lineas","idliq","ruta","estado"),show="headings")
        for col in self.tree["columns"]: self.tree.heading(col,text=col.title()); self.tree.column(col,width=125)
        self.tree.column("ruta",width=310); self.tree.pack(fill="both",expand=True,padx=8,pady=8)
        for batch_id in batch_ids:
            for d in history.list_recipient_documents(batch_id):
                self.tree.insert("","end",values=(batch_id,d["remittance_id"],d["recipient_member_id"],d["recipient_name"],d["line_count"],d["id_liqs"],d["file_path"],d["status"]))
        bar=ttk.Frame(self); bar.pack(fill="x",padx=8,pady=8)
        for text,cmd in (("Abrir PDF",self.open_pdf),("Abrir carpeta",self.open_folder),("Regenerar",self.regenerate),("Cerrar",self.destroy)): ttk.Button(bar,text=text,command=cmd).pack(side="left",padx=3)

    def selected(self):
        item=self.tree.selection()
        return self.tree.item(item[0],"values") if item else None

    def open_pdf(self):
        row=self.selected()
        if not row:return
        batch=self.history.get_batch_detail(row[0])["batch"]
        if batch["status"] == "VOIDED": messagebox.showwarning("Liquidación anulada","Este documento pertenece a una liquidación anulada.",parent=self)
        path=Path(row[6])
        if not path.exists(): messagebox.showerror("Documento","El archivo no existe. Puede regenerarlo.",parent=self); return
        open_path(path); logger.info("[DocumentOpened]\nbatch_id=%s\nrecipient_member_id=%s\npath=%s",row[0],row[2],path)

    def open_folder(self):
        row=self.selected()
        if row: open_path(Path(row[6]).parent)

    def regenerate(self):
        row=self.selected()
        if row:
            result=self.history.regenerate_documents(row[0],int(row[2])); messagebox.showinfo("Regenerar",f"Generados: {len(result.generated_documents)}\nErrores: {len(result.failed_documents)}",parent=self)


class LiquidationHistoryDialog(tk.Toplevel):
    def __init__(self,parent,history):
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
        for text,cmd in (("Ver detalle",self.detail),("Visualizar PDFs",self.documents),("Regenerar PDFs",self.regenerate),("Anular",self.void),("Abrir carpeta",self.folder),("Cerrar",self.destroy)):ttk.Button(bar,text=text,command=cmd).pack(side="left",padx=3)
        self.refresh()
    def batch_id(self):
        s=self.tree.selection(); return self.tree.item(s[0],"values")[0] if s else None
    def refresh(self):
        self.tree.delete(*self.tree.get_children()); filters={k:v.get().strip() for k,v in self.vars.items() if v.get().strip()}
        for b in self.history.list_batches(filters): self.tree.insert("","end",values=(b["batch_id"],b["remesa_id"],b["payment_date"],b["crop"],b["campaign"],b["company"],b["line_count"],b["recipient_count"],b["document_count"],b["status"],b["created_at"]))
    def detail(self):
        bid=self.batch_id()
        if bid:
            d=self.history.get_batch_detail(bid); messagebox.showinfo("Detalle",f"Batch: {bid}\nEstado: {d['batch']['status']}\nLíneas: {len(d['lines'])}",parent=self)
    def documents(self):
        if self.batch_id(): DocumentSelectorDialog(self,self.history,(self.batch_id(),))
    def regenerate(self):
        if self.batch_id(): self.history.regenerate_documents(self.batch_id()); self.refresh()
    def void(self):
        bid=self.batch_id()
        if not bid:return
        reason=simpledialog.askstring("Anular liquidación","Motivo obligatorio:",parent=self)
        if reason and messagebox.askyesno("Confirmar",f"¿Anular el batch {bid}?",parent=self): self.history.void_batch(bid,reason); logger.info("[BatchVoided]\nbatch_id=%s\nreason=%s",bid,reason); self.refresh()
    def folder(self):
        bid=self.batch_id(); docs=self.history.list_recipient_documents(bid) if bid else ()
        if docs: open_path(Path(docs[0]["file_path"]).parent)
