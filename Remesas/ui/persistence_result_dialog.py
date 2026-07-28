from __future__ import annotations
from pathlib import Path
import logging
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk
from ui.liquidation_history_dialog import DocumentSelectorDialog, LiquidationHistoryDialog
from services.path_opener import open_path

logger = logging.getLogger(__name__)


class PersistenceResultDialog(tk.Toplevel):
    """Post-commit actions backed exclusively by the SQLite document registry."""
    def __init__(self,parent,history,batch_ids,documents,lines):
        super().__init__(parent); self.history=history; self.batch_ids=tuple(batch_ids); self.lines=lines
        self.title("Liquidaciones guardadas"); self.geometry("720x360"); self.transient(parent)
        self.summary=tk.StringVar(); ttk.Label(self,textvariable=self.summary,justify="left",font=("Segoe UI",11)).pack(anchor="w",padx=20,pady=18)
        self.message=tk.StringVar(); ttk.Label(self,textvariable=self.message,wraplength=670).pack(anchor="w",padx=20)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=16,pady=20)
        self.void_button=None
        for text,cmd in (("Visualizar PDF definitivo",self.view),("Abrir carpeta de documentos",self.folder),("Regenerar PDFs",self.regenerate),("Anular liquidación",self.void),("Ver liquidaciones guardadas",lambda:LiquidationHistoryDialog(self,history)),("Cerrar",self.destroy)):
            button=ttk.Button(bar,text=text,command=cmd); button.pack(side="left",padx=2,pady=2)
            if text == "Anular liquidación": self.void_button=button
        self._refresh_documents()

    def _refresh_documents(self):
        self.current_documents=tuple(d for bid in self.batch_ids for d in self.history.list_recipient_documents(bid))
        generated=sum(d["status"] == "GENERATED" for d in self.current_documents); failed=sum(d["status"] == "FAILED" for d in self.current_documents)
        self.summary.set(f"Remesas guardadas: {len(self.batch_ids)}\nLíneas persistidas: {self.lines}\nPDFs esperados: {generated+failed}\nPDFs generados: {generated}\nPDFs con error: {failed}\nEstado general: {'CORRECTO' if not failed else 'PARCIAL'}")
        self.message.set("Las liquidaciones se han guardado correctamente y se han generado los PDFs definitivos." if not failed else f"Las liquidaciones se han guardado correctamente, pero {failed} PDFs no pudieron generarse. Puede regenerarlos desde esta ventana.")
        batches=(self.history.get_batch_detail(bid)["batch"] for bid in self.batch_ids)
        active=any(batch is not None and batch["status"] == "ACTIVE" for batch in batches)
        self.void_button.configure(state="normal" if active else "disabled")
        logger.info("[DocumentRegistryRefreshed]\nbatch_ids=%s\ndocuments=%s",self.batch_ids,len(self.current_documents))

    def _generated(self): return [d for d in self.current_documents if d["status"] == "GENERATED"]
    def view(self):
        docs=self._generated()
        if len(docs)==1:
            path=Path(docs[0]["file_path"])
            try: open_path(path); logger.info("[DocumentOpened]\nbatch_id=%s\nrecipient_member_id=%s\npath=%s",docs[0]["batch_id"],docs[0]["recipient_member_id"],path)
            except Exception as exc: logger.exception("[DocumentOpenFailed]\npath=%s\nerror=%s",path,exc); messagebox.showerror("Abrir documento",f"No se pudo abrir el documento:\n\n{path}\n\nDetalle:\n{exc}\n\nPuede regenerarlo desde esta ventana.",parent=self)
        elif docs: DocumentSelectorDialog(self,self.history,self.batch_ids)
        else: messagebox.showwarning("Documentos","No hay PDFs definitivos disponibles. Puede regenerarlos.",parent=self)
    def folder(self):
        docs=self._generated()
        if not docs: messagebox.showwarning("Documentos","No hay PDFs definitivos disponibles. Puede regenerarlos.",parent=self); return
        path=Path(docs[0]["file_path"]).parent
        try: open_path(path)
        except Exception as exc: messagebox.showerror("Abrir carpeta",f"No se pudo abrir:\n\n{path}\n\nDetalle:\n{exc}",parent=self)
    def regenerate(self):
        for bid in self.batch_ids:self.history.regenerate_documents(bid)
        self._refresh_documents(); messagebox.showinfo("Regenerar","Regeneración finalizada.",parent=self)
    def void(self):
        active=[(bid,self.history.get_batch_detail(bid)["batch"]) for bid in self.batch_ids]
        active=[(bid,b) for bid,b in active if b and b["status"] == "ACTIVE"]
        if not active: self._refresh_documents(); return
        if len(active)==1: bid,batch=active[0]
        else:
            names="\n".join(f"{bid}: {b['remesa_name']}" for bid,b in active)
            bid=simpledialog.askstring("Anular",f"Indique el batch a anular:\n{names}",parent=self); batch=next((b for candidate,b in active if candidate==bid),None)
        reason=simpledialog.askstring("Anular","Motivo obligatorio:",parent=self) if batch else None
        if reason and messagebox.askyesno("Confirmar",f"¿Anular la remesa {batch['remesa_name']}?",parent=self): self.history.void_batch(bid,reason); self._refresh_documents(); messagebox.showinfo("Anular","Liquidación anulada.",parent=self)
