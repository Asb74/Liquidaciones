from __future__ import annotations
from dataclasses import replace
from datetime import datetime
import logging
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from domain.document_models import DocumentType
from services.path_opener import open_path
from services.pdf_merge_service import PdfMergeCancelled

logger=logging.getLogger(__name__)


class PdfMergeToolDialog(tk.Toplevel):
    COLUMNS=("sel","type","campaign","crop","remittance","member","name","idliq","date","status","path","pages","size")
    HEADERS=("Seleccionar","Tipo","Campaña","Cultivo","Remesa","N.º socio","Socio","IdLiq","Fecha","Estado","Ruta","Páginas","Tamaño")
    def __init__(self,parent,service,*,output_root=None,regenerate_callback=None):
        super().__init__(parent); self.service=service; self.regenerate_callback=regenerate_callback
        self.output_root=Path(output_root or (r"C:\Liquidaciones\salidas\impresion_masiva" if Path("C:/").exists() else Path.cwd().parent/"salidas"/"impresion_masiva"))
        self.title("Unificar PDFs para impresión"); self.geometry("1400x720"); self.documents=[]; self.selected=set(); self.validation_status={}; self.cancelled=False
        self._build()

    def _build(self):
        filters=ttk.LabelFrame(self,text="Filtros"); filters.pack(fill="x",padx=8,pady=8)
        self.kind=tk.StringVar(value="Definitivos"); self.campaign=tk.StringVar(); self.crop=tk.StringVar(); self.remittance=tk.StringVar(); self.member=tk.StringVar(); self.date_from=tk.StringVar(); self.date_to=tk.StringVar(); self.state=tk.StringVar(value="Todos"); self.voided=tk.BooleanVar()
        fields=(("Tipo de documento",self.kind,("Definitivos","Borradores")),("Campaña",self.campaign,None),("Cultivo",self.crop,None),("Remesa",self.remittance,None),("N.º socio",self.member,None),("Fecha desde",self.date_from,None),("Fecha hasta",self.date_to,None),("Estado",self.state,("Todos","Generado","No disponible","Anulada")))
        for i,(label,var,values) in enumerate(fields):
            ttk.Label(filters,text=label).grid(row=0,column=i,sticky="w",padx=3)
            (ttk.Combobox(filters,textvariable=var,values=values,state="readonly",width=14) if values else ttk.Entry(filters,textvariable=var,width=14)).grid(row=1,column=i,padx=3,pady=3)
        ttk.Checkbutton(filters,text="Incluir liquidaciones anuladas",variable=self.voided).grid(row=2,column=0,columnspan=2,sticky="w")
        ttk.Button(filters,text="Buscar documentos",command=self.search).grid(row=1,column=8,padx=8)
        actions=ttk.Frame(self); actions.pack(fill="x",padx=8)
        for text,cmd in (("Seleccionar todos",self.select_all),("Quitar selección",self.clear_selection),("Invertir selección",self.invert_selection),("Seleccionar visibles",self.select_all),("Quitar no disponibles",self.remove_unavailable)):
            ttk.Button(actions,text=text,command=cmd).pack(side="left",padx=2)
        ttk.Label(actions,text="Ordenar por:").pack(side="left",padx=(15,2)); self.order=tk.StringVar(value="Orden actual")
        order=ttk.Combobox(actions,textvariable=self.order,state="readonly",values=("Orden actual","Remesa","N.º socio","Nombre del socio","Fecha","IdLiq"),width=18); order.pack(side="left"); order.bind("<<ComboboxSelected>>",lambda _e:self.sort())
        for text,delta in (("Primero",-999999),("Subir",-1),("Bajar",1),("Último",999999)): ttk.Button(actions,text=text,command=lambda d=delta:self.move(d)).pack(side="left",padx=2)
        self.tree=ttk.Treeview(self,columns=self.COLUMNS,show="headings",selectmode="browse")
        for col,head in zip(self.COLUMNS,self.HEADERS): self.tree.heading(col,text=head); self.tree.column(col,width=80 if col not in ("name","path") else 220,anchor="w")
        self.tree.pack(fill="both",expand=True,padx=8,pady=8); self.tree.bind("<Button-1>",self.toggle); self.tree.bind("<Double-1>",self.open_individual)
        bottom=ttk.Frame(self); bottom.pack(fill="x",padx=8,pady=(0,8)); self.counters=tk.StringVar(); ttk.Label(bottom,textvariable=self.counters).pack(side="left")
        ttk.Button(bottom,text="Regenerar seleccionado",command=self.regenerate).pack(side="right",padx=3)
        ttk.Button(bottom,text="Cancelar proceso",command=lambda:setattr(self,"cancelled",True)).pack(side="right",padx=3)
        ttk.Button(bottom,text="Generar PDF combinado",command=self.merge).pack(side="right",padx=3); ttk.Button(bottom,text="Cerrar",command=self.destroy).pack(side="right",padx=3)
        self.progress=tk.StringVar(); ttk.Label(bottom,textvariable=self.progress).pack(side="right",padx=10)
        self.search()

    def search(self):
        try:
            kind=DocumentType.PDF_MEMBER.value if self.kind.get()=="Definitivos" else DocumentType.PDF_DRAFT.value
            self.documents=list(self.service.list_available_documents(document_kind=kind,campaign=self.campaign.get() or None,crop=self.crop.get() or None,remittance_id=int(self.remittance.get()) if self.remittance.get() else None,member_id=int(self.member.get()) if self.member.get() else None,date_from=self.date_from.get() or None,date_to=self.date_to.get() or None,include_voided=self.voided.get()))
            validation=self.service.validate_documents(self.documents)
            self.validation_status={str(item.document.file_path):item.status.value for item in validation.items}
            self.documents=[replace(item.document,page_count=item.page_count or None) for item in validation.items]
            requested=self.state.get()
            if requested=="No disponible": self.documents=[d for d in self.documents if self.validation_status.get(str(d.file_path),"MISSING")!="VALID"]
            elif requested=="Anulada": self.documents=[d for d in self.documents if d.batch_status=="VOIDED"]
            elif requested=="Generado": self.documents=[d for d in self.documents if self.validation_status.get(str(d.file_path))=="VALID" and d.batch_status!="VOIDED"]
            self.selected=set(); self.refresh()
        except Exception as exc: logger.exception("Búsqueda PDF"); messagebox.showerror("Buscar documentos",str(exc),parent=self)

    def refresh(self):
        self.tree.delete(*self.tree.get_children())
        for i,d in enumerate(self.documents):
            validation=self.validation_status.get(str(d.file_path),"VALID"); available=validation=="VALID"; status="Anulada" if d.batch_status=="VOIDED" else ({"VALID":"Generado","MISSING":"No disponible","CORRUPT":"Corrupto","ENCRYPTED":"Cifrado","EMPTY":"Vacío","DUPLICATE":"Duplicado"}.get(validation,validation))
            vals=("☑" if i in self.selected else "☐","Definitivo" if d.document_kind=="PDF_MEMBER" else "Borrador",d.campaign,d.crop,d.remittance_name,d.member_id or "",d.member_name," · ".join(d.id_liqs),d.generated_at.strftime("%d/%m/%Y %H:%M") if d.generated_at else "",status,str(d.file_path),d.page_count or "",self._size(d.file_size))
            self.tree.insert("","end",iid=str(i),values=vals)
        pages=sum(self.documents[i].page_count or 0 for i in self.selected); self.counters.set(f"Documentos visibles: {len(self.documents)}    Documentos seleccionados: {len(self.selected)}    Páginas estimadas: {pages}")
    @staticmethod
    def _size(size): return "" if size is None else f"{size/1024:.1f} KB"
    def toggle(self,event):
        if self.tree.identify_region(event.x,event.y)=="cell" and self.tree.identify_column(event.x)=="#1":
            row=self.tree.identify_row(event.y)
            if row: i=int(row); self.selected.symmetric_difference_update({i}); self.refresh()
    def select_all(self): self.selected={i for i,d in enumerate(self.documents) if d.file_path.exists()}; self.refresh()
    def clear_selection(self): self.selected.clear(); self.refresh()
    def invert_selection(self): self.selected={i for i,d in enumerate(self.documents) if d.file_path.exists()}-self.selected; self.refresh()
    def remove_unavailable(self): self.selected={i for i in self.selected if self.documents[i].file_path.exists()}; self.refresh()
    def sort(self):
        key={"Remesa":lambda d:(d.remittance_name,d.remittance_id or 0),"N.º socio":lambda d:d.member_id or 0,"Nombre del socio":lambda d:d.member_name.casefold(),"Fecha":lambda d:d.generated_at or datetime.min,"IdLiq":lambda d:d.id_liqs}.get(self.order.get())
        if not key:return
        chosen={self.documents[i] for i in self.selected}; self.documents.sort(key=key); self.selected={i for i,d in enumerate(self.documents) if d in chosen}; self.refresh()
    def move(self,delta):
        rows=self.tree.selection()
        if not rows:return
        old=int(rows[0]); chosen={self.documents[i] for i in self.selected}; new=0 if delta < -1 else len(self.documents)-1 if delta>1 else max(0,min(len(self.documents)-1,old+delta)); doc=self.documents.pop(old); self.documents.insert(new,doc)
        self.selected={i for i,d in enumerate(self.documents) if d in chosen}; self.refresh(); self.tree.selection_set(str(new))
    def open_individual(self,_event=None):
        row=self.tree.focus()
        if row:
            try: open_path(self.documents[int(row)].file_path)
            except Exception as exc: messagebox.showerror("Abrir PDF",str(exc),parent=self)
    def _proposed(self,docs):
        campaigns={d.campaign for d in docs}; crops={d.crop for d in docs}; label=self.kind.get(); stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        detail=f"{next(iter(crops))}_{next(iter(campaigns))}" if len(campaigns)==len(crops)==1 else "Seleccion"
        folder=self.output_root/(next(iter(campaigns)) if len(campaigns)==1 else "seleccion")/label.casefold()
        return folder/f"{label}_{detail}_{stamp}.pdf"
    def merge(self):
        docs=[d for i,d in enumerate(self.documents) if i in self.selected]
        if not docs: messagebox.showwarning("Unificar","Seleccione al menos un documento.",parent=self); return
        if any(d.batch_status=="VOIDED" for d in docs) and not messagebox.askyesno("Liquidaciones anuladas","La selección contiene liquidaciones anuladas. ¿Desea continuar?",parent=self): return
        proposed=self._proposed(docs); proposed.parent.mkdir(parents=True,exist_ok=True)
        path=filedialog.asksaveasfilename(parent=self,title="Archivo de salida",initialdir=proposed.parent,initialfile=proposed.name,defaultextension=".pdf",filetypes=(("Documento PDF","*.pdf"),))
        if not path:return
        self.cancelled=False
        try:
            result=self.service.merge_documents(docs,path,progress_callback=self._progress,should_cancel=lambda:self.cancelled)
            messagebox.showinfo("PDF combinado",f"PDF combinado generado correctamente.\n\nTipo: {self.kind.get()}\nDocumentos seleccionados: {len(docs)}\nDocumentos incluidos: {result.documents_included}\nDocumentos excluidos: {result.documents_excluded}\nPáginas totales: {result.page_count}\nRuta:\n{result.output_path}",parent=self)
            if messagebox.askyesno("Abrir PDF","¿Desea abrir el PDF combinado?",parent=self): open_path(result.output_path)
        except PdfMergeCancelled: messagebox.showinfo("Unificar","Operación cancelada.",parent=self)
        except Exception as exc: logger.exception("Unificación PDF"); messagebox.showerror("Unificar",str(exc),parent=self)
    def _progress(self,phase,current,total,pages): self.progress.set(f"{phase.capitalize()} documento {current} de {total}. Páginas procesadas: {pages}"); self.update()
    def regenerate(self):
        if self.kind.get()=="Borradores": messagebox.showinfo("Regenerar","Los borradores no pueden regenerarse automáticamente.",parent=self); return
        if not self.regenerate_callback: messagebox.showinfo("Regenerar","Abra Liquidaciones guardadas para regenerar este definitivo sin recalcular.",parent=self); return
        self.regenerate_callback(); self.search()
