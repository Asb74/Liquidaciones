from __future__ import annotations
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk
from ui.liquidation_history_dialog import DocumentSelectorDialog, LiquidationHistoryDialog, open_path


class PersistenceResultDialog(tk.Toplevel):
    """Resultado post-commit. Punto de extensión: CSV_ADMINISTRATION."""
    def __init__(self,parent,history,batch_ids,documents,lines):
        super().__init__(parent); self.history=history; self.batch_ids=tuple(batch_ids); self.documents=tuple(documents)
        self.title("Liquidaciones guardadas"); self.geometry("680x340"); self.transient(parent)
        good=sum(d.success for d in self.documents); bad=len(self.documents)-good
        text=(f"Remesas guardadas: {len(self.batch_ids)}\nLíneas persistidas: {lines}\nPDFs esperados: {len(self.documents)}\nPDFs generados: {good}\nPDFs con error: {bad}\nEstado general: {'CORRECTO' if not bad else 'PARCIAL'}")
        ttk.Label(self,text=text,justify="left",font=("Segoe UI",11)).pack(anchor="w",padx=20,pady=18)
        msg="Las liquidaciones se han guardado correctamente y se han generado los PDFs definitivos." if not bad else f"Las liquidaciones se han guardado correctamente, pero {bad} PDFs no pudieron generarse. Puede regenerarlos desde esta ventana."
        ttk.Label(self,text=msg,wraplength=630).pack(anchor="w",padx=20)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=16,pady=20)
        for text,cmd in (("Visualizar PDF definitivo",self.view),("Abrir carpeta de documentos",self.folder),("Regenerar PDFs",self.regenerate),("Anular liquidación",self.void),("Ver liquidaciones guardadas",lambda:LiquidationHistoryDialog(self,history)),("Cerrar",self.destroy)):ttk.Button(bar,text=text,command=cmd).pack(side="left",padx=2,pady=2)
    def view(self):
        docs=[d for bid in self.batch_ids for d in self.history.list_recipient_documents(bid) if d["status"]=="GENERATED"]
        if len(docs)==1:
            selector=DocumentSelectorDialog(self,self.history,self.batch_ids); first=selector.tree.get_children()[0]; selector.tree.selection_set(first); selector.open_pdf(); selector.destroy()
        elif docs: DocumentSelectorDialog(self,self.history,self.batch_ids)
        else: messagebox.showwarning("Documentos","No hay PDF generado; use Regenerar PDFs.",parent=self)
    def folder(self):
        paths=[Path(d.path).parent for d in self.documents if d.success]
        if paths:open_path(paths[0])
    def regenerate(self):
        for bid in self.batch_ids:self.history.regenerate_documents(bid)
        messagebox.showinfo("Regenerar","Regeneración finalizada.",parent=self)
    def void(self):
        bid=self.batch_ids[0] if len(self.batch_ids)==1 else simpledialog.askstring("Anular","Batch ID a anular:",parent=self)
        reason=simpledialog.askstring("Anular","Motivo obligatorio:",parent=self) if bid else None
        if reason and messagebox.askyesno("Confirmar",f"¿Anular el batch {bid}?",parent=self):self.history.void_batch(bid,reason); messagebox.showinfo("Anular","Liquidación anulada.",parent=self)
