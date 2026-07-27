from __future__ import annotations
from dataclasses import replace
from datetime import datetime
import logging
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
try:
    from tkcalendar import Calendar
except ImportError:
    Calendar = None

from domain.document_models import DocumentType
from services.path_opener import open_path
from services.pdf_merge_service import PdfMergeCancelled
from services.liquidation_csv_export_service import CsvExportCancelled

logger=logging.getLogger(__name__)


def collect_unique_remittances(selected_documents, resolver):
    """Resolve real remittance ids, de-duplicating in document display order."""
    remittances=[]; missing=[]; seen=set()
    for document in selected_documents:
        remittance_id=getattr(document, "remittance_id", None)
        if remittance_id is None:
            missing.append(document); continue
        try:
            remittance=resolver(document)
        except (KeyError, ValueError, LookupError):
            missing.append(document); continue
        real_id=getattr(remittance, "remittance_id", None)
        if real_id is None:
            missing.append(document); continue
        if real_id not in seen:
            seen.add(real_id); remittances.append(remittance)
    return remittances, missing


def collect_unique_batch_ids(selected_documents):
    """Return persisted batch ids once, preserving the visible selection order."""
    batch_ids=[]; missing=[]; seen=set()
    for document in selected_documents:
        batch_id=getattr(document, "batch_id", None)
        if not batch_id:
            missing.append(document); continue
        batch_id=str(batch_id)
        if batch_id not in seen:
            seen.add(batch_id); batch_ids.append(batch_id)
    return batch_ids, missing

class NullableDatePicker(ttk.Frame):
    """Calendar-backed date field which can genuinely remain empty."""
    def __init__(self, parent, variable):
        super().__init__(parent); self.variable=variable
        ttk.Entry(self,textvariable=variable,state="readonly",width=11).pack(side="left")
        ttk.Button(self,text="…",width=3,command=self._open).pack(side="left")
        ttk.Button(self,text="×",width=3,command=lambda:variable.set("")).pack(side="left")
    def _open(self):
        if Calendar is None:
            messagebox.showerror("Calendario","Falta la dependencia tkcalendar. Instálela con: pip install tkcalendar",parent=self); return
        win=tk.Toplevel(self); win.title("Seleccionar fecha"); win.transient(self.winfo_toplevel()); win.grab_set()
        cal=Calendar(win,date_pattern="dd/mm/yyyy",locale="es_ES"); cal.pack(padx=8,pady=8)
        def accept(): self.variable.set(cal.get_date()); win.destroy()
        ttk.Button(win,text="Aceptar",command=accept).pack(pady=(0,8))


class PdfMergeToolDialog(tk.Toplevel):
    COLUMNS=("sel","type","campaign","company","crop","remittance","member","name","idliq","date","status","path","pages","size")
    HEADERS=("Seleccionar","Tipo","Campaña","Empresa","Cultivo","Remesa","N.º socio","Socio","IdLiq","Fecha","Estado","Ruta","Páginas","Tamaño")
    def __init__(self,parent,service,*,output_root=None,regenerate_callback=None,
                 remittance_resolver=None,excel_callback=None,cancel_excel_callback=None,
                 csv_export_service=None):
        super().__init__(parent); self.service=service; self.regenerate_callback=regenerate_callback
        self.remittance_resolver=remittance_resolver; self.excel_callback=excel_callback; self.cancel_excel_callback=cancel_excel_callback
        self.csv_export_service=csv_export_service
        self.output_root=Path(output_root or (r"C:\Liquidaciones\salidas\impresion_masiva" if Path("C:/").exists() else Path.cwd().parent/"salidas"/"impresion_masiva"))
        self.title("Generación masiva de documentos"); self.geometry("1400x720"); self.documents=[]; self.selected=set(); self.validation_status={}; self.cancelled=False
        self._build()

    def _build(self):
        filters=ttk.LabelFrame(self,text="Filtros"); filters.pack(fill="x",padx=8,pady=8)
        self.kind=tk.StringVar(value="Definitivos"); self.campaign=tk.StringVar(value="Todas"); self.company=tk.StringVar(value="Todas"); self.crop=tk.StringVar(value="Todos"); self.remittance=tk.StringVar(value="Todas"); self.member=tk.StringVar(); self.date_from=tk.StringVar(); self.date_to=tk.StringVar(); self.state=tk.StringVar(value="Todos"); self.voided=tk.BooleanVar(); self.remittance_ids={}
        labels=("Tipo de documento","Campaña","Empresa","Cultivo","Remesa","N.º socio","Fecha desde","Fecha hasta","Estado")
        for i,label in enumerate(labels): ttk.Label(filters,text=label).grid(row=0,column=i,sticky="w",padx=3)
        self.kind_combo=ttk.Combobox(filters,textvariable=self.kind,values=("Definitivos","Borradores"),state="readonly",width=14); self.kind_combo.grid(row=1,column=0,padx=3)
        self.campaign_combo=ttk.Combobox(filters,textvariable=self.campaign,state="readonly",width=12); self.campaign_combo.grid(row=1,column=1,padx=3)
        self.company_combo=ttk.Combobox(filters,textvariable=self.company,state="readonly",width=12); self.company_combo.grid(row=1,column=2,padx=3)
        self.crop_combo=ttk.Combobox(filters,textvariable=self.crop,state="readonly",width=14); self.crop_combo.grid(row=1,column=3,padx=3)
        self.remittance_combo=ttk.Combobox(filters,textvariable=self.remittance,state="readonly",width=25); self.remittance_combo.grid(row=1,column=4,padx=3)
        ttk.Entry(filters,textvariable=self.member,width=11).grid(row=1,column=5,padx=3)
        NullableDatePicker(filters,self.date_from).grid(row=1,column=6,padx=3); NullableDatePicker(filters,self.date_to).grid(row=1,column=7,padx=3)
        ttk.Combobox(filters,textvariable=self.state,values=("Todos","Generado","Error","Sustituido"),state="readonly",width=12).grid(row=1,column=8,padx=3)
        self.kind_combo.bind("<<ComboboxSelected>>",self._kind_changed); self.campaign_combo.bind("<<ComboboxSelected>>",self._campaign_changed); self.company_combo.bind("<<ComboboxSelected>>",self._company_changed); self.crop_combo.bind("<<ComboboxSelected>>",self._crop_changed)
        self.voided_check=ttk.Checkbutton(filters,text="Incluir liquidaciones anuladas",variable=self.voided); self.voided_check.grid(row=2,column=0,columnspan=2,sticky="w")
        self.search_button=ttk.Button(filters,text="Buscar documentos",command=self.search); self.search_button.grid(row=1,column=9,padx=8)
        actions=ttk.Frame(self); actions.pack(fill="x",padx=8)
        for text,cmd in (("Seleccionar todos",self.select_all),("Quitar selección",self.clear_selection),("Invertir selección",self.invert_selection),("Seleccionar visibles",self.select_all),("Quitar no disponibles",self.remove_unavailable)):
            ttk.Button(actions,text=text,command=cmd).pack(side="left",padx=2)
        ttk.Label(actions,text="Ordenar por:").pack(side="left",padx=(15,2)); self.order=tk.StringVar(value="Orden actual")
        order=ttk.Combobox(actions,textvariable=self.order,state="readonly",values=("Orden actual","Remesa","N.º socio","Nombre del socio","Fecha","IdLiq"),width=18); order.pack(side="left"); order.bind("<<ComboboxSelected>>",lambda _e:self.sort())
        for text,delta in (("Primero",-999999),("Subir",-1),("Bajar",1),("Último",999999)): ttk.Button(actions,text=text,command=lambda d=delta:self.move(d)).pack(side="left",padx=2)
        self.tree=ttk.Treeview(self,columns=self.COLUMNS,show="headings",selectmode="browse")
        widths={"sel":80,"type":90,"campaign":75,"company":70,"crop":100,"remittance":160,"member":80,"name":200,"idliq":150,"date":90,"status":90,"path":300,"pages":65,"size":80}
        for col,head in zip(self.COLUMNS,self.HEADERS): self.tree.heading(col,text=head); self.tree.column(col,width=widths[col],anchor="w")
        self.tree.pack(fill="both",expand=True,padx=8,pady=8); self.tree.bind("<Button-1>",self.toggle); self.tree.bind("<Double-1>",self.open_individual)
        bottom=ttk.Frame(self); bottom.pack(fill="x",padx=8,pady=(0,8)); self.counters=tk.StringVar(); ttk.Label(bottom,textvariable=self.counters).pack(side="left")
        ttk.Button(bottom,text="Regenerar seleccionado",command=self.regenerate).pack(side="right",padx=3)
        ttk.Button(bottom,text="Cancelar proceso",command=self.cancel_process).pack(side="right",padx=3)
        self.excel_button=ttk.Button(bottom,text="Generar resumen Excel",command=self.generate_excel); self.excel_button.pack(side="right",padx=3)
        self.csv_button=ttk.Button(bottom,text="Generar CSV contable",command=self.generate_csv); self.csv_button.pack(side="right",padx=3)
        self.pdf_button=ttk.Button(bottom,text="Generar PDF combinado",command=self.merge); self.pdf_button.pack(side="right",padx=3); ttk.Button(bottom,text="Cerrar",command=self.destroy).pack(side="right",padx=3)
        ttk.Label(bottom,text="El Excel y el CSV se generan por remesas completas.").pack(side="right",padx=6)
        self.progress=tk.StringVar(); ttk.Label(bottom,textvariable=self.progress).pack(side="right",padx=10)
        self._reload_filters(); self.search()

    def _technical(self,value,all_label): return None if value==all_label else value
    def _reload_filters(self):
        kind=DocumentType.PDF_MEMBER.value if self.kind.get()=="Definitivos" else DocumentType.PDF_DRAFT.value
        campaign=self._technical(self.campaign.get(),"Todas"); company=self._technical(self.company.get(),"Todas"); crop=self._technical(self.crop.get(),"Todos")
        options=self.service.list_filter_options(document_kind=kind,campaign=campaign,company=company,crop=crop)
        self.campaign_combo["values"]=("Todas",*options["campaigns"]); self.company_combo["values"]=("Todas",*options["companies"]); self.crop_combo["values"]=("Todos",*options["crops"])
        self.remittance_ids={f"{rid} - {name}":rid for rid,name in options["remittances"]}; self.remittance_combo["values"]=("Todas",*self.remittance_ids)
    def _kind_changed(self,_=None):
        self.campaign.set("Todas"); self.company.set("Todas"); self.crop.set("Todos"); self.remittance.set("Todas"); self.documents=[]; self.selected=set(); self.refresh()
        if self.kind.get()=="Borradores": self.voided.set(False)
        self.voided_check.configure(state="disabled" if self.kind.get()=="Borradores" else "normal")
        self._reload_filters()
    def _campaign_changed(self,_=None): self.company.set("Todas"); self.crop.set("Todos"); self.remittance.set("Todas"); self._reload_filters()
    def _company_changed(self,_=None): self.crop.set("Todos"); self.remittance.set("Todas"); self._reload_filters()
    def _crop_changed(self,_=None): self.remittance.set("Todas"); self._reload_filters()

    @staticmethod
    def parse_member(value):
        if not value.strip(): return None
        if not value.strip().isdigit() or int(value)<=0: raise ValueError("El N.º socio debe ser un número entero.")
        return int(value)
    @staticmethod
    def parse_date(value):
        if not value.strip(): return None
        try: return datetime.strptime(value.strip(),"%d/%m/%Y").date().isoformat()
        except ValueError as exc: raise ValueError("La fecha debe tener formato dd/mm/aaaa.") from exc

    def search(self):
        try:
            kind=DocumentType.PDF_MEMBER.value if self.kind.get()=="Definitivos" else DocumentType.PDF_DRAFT.value
            date_from=self.parse_date(self.date_from.get()); date_to=self.parse_date(self.date_to.get())
            if date_from and date_to and date_from>date_to: raise ValueError("La fecha desde no puede ser posterior a la fecha hasta.")
            status={"Todos":None,"Generado":"GENERATED","Error":"FAILED","Sustituido":"SUPERSEDED"}[self.state.get()]
            self.documents=list(self.service.list_available_documents(document_kind=kind,campaign=self._technical(self.campaign.get(),"Todas"),company=self._technical(self.company.get(),"Todas"),crop=self._technical(self.crop.get(),"Todos"),remittance_id=self.remittance_ids.get(self.remittance.get()),member_id=self.parse_member(self.member.get()),date_from=date_from,date_to=date_to,status=status,include_voided=self.voided.get() if kind==DocumentType.PDF_MEMBER.value else False))
            validation=self.service.validate_documents(self.documents)
            self.validation_status={str(item.document.file_path):item.status.value for item in validation.items}
            self.documents=[replace(item.document,page_count=item.page_count or None) for item in validation.items]
            self.selected=set(); self.refresh()
            if not self.documents: self.progress.set("No se encontraron documentos con los filtros seleccionados.")
        except Exception as exc: logger.exception("Búsqueda PDF"); messagebox.showerror("Buscar documentos",str(exc),parent=self)

    def refresh(self):
        self.tree.delete(*self.tree.get_children())
        for i,d in enumerate(self.documents):
            validation=self.validation_status.get(str(d.file_path),"VALID"); available=validation=="VALID"; status="Anulada" if d.batch_status=="VOIDED" else ({"VALID":"Generado","MISSING":"No disponible","CORRUPT":"Corrupto","ENCRYPTED":"Cifrado","EMPTY":"Vacío","DUPLICATE":"Duplicado"}.get(validation,validation))
            vals=("☑" if i in self.selected else "☐","Definitivo" if d.document_kind=="PDF_MEMBER" else "Borrador",d.campaign,d.company,d.crop,d.remittance_name,d.member_id or "",d.member_name," · ".join(d.id_liqs),d.generated_at.strftime("%d/%m/%Y %H:%M") if d.generated_at else "",status,str(d.file_path),d.page_count or "",self._size(d.file_size))
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
        if not docs: messagebox.showwarning("Unificar","Debe seleccionar al menos un documento.",parent=self); return
        if any(d.batch_status=="VOIDED" for d in docs) and not messagebox.askyesno("Liquidaciones anuladas","La selección contiene liquidaciones anuladas. ¿Desea continuar?",parent=self): return
        proposed=self._proposed(docs); proposed.parent.mkdir(parents=True,exist_ok=True)
        path=filedialog.asksaveasfilename(parent=self,title="Archivo de salida",initialdir=proposed.parent,initialfile=proposed.name,defaultextension=".pdf",filetypes=(("Documento PDF","*.pdf"),))
        if not path:return
        self.cancelled=False
        self._set_generating(True)
        try:
            result=self.service.merge_documents(docs,path,progress_callback=self._progress,should_cancel=lambda:self.cancelled)
            messagebox.showinfo("PDF combinado",f"PDF combinado generado correctamente.\n\nTipo: {self.kind.get()}\nDocumentos seleccionados: {len(docs)}\nDocumentos incluidos: {result.documents_included}\nDocumentos excluidos: {result.documents_excluded}\nPáginas totales: {result.page_count}\nRuta:\n{result.output_path}",parent=self)
            if messagebox.askyesno("Abrir PDF","¿Desea abrir el PDF combinado?",parent=self): open_path(result.output_path)
        except PdfMergeCancelled: messagebox.showinfo("Unificar","Operación cancelada.",parent=self)
        except Exception as exc: logger.exception("Unificación PDF"); messagebox.showerror("Unificar",str(exc),parent=self)
        finally: self._set_generating(False)
    def _set_generating(self, running):
        state="disabled" if running else "normal"
        self.pdf_button.configure(state=state); self.excel_button.configure(state=state); self.csv_button.configure(state=state); self.search_button.configure(state=state)
    def cancel_process(self):
        self.cancelled=True
        if self.cancel_excel_callback: self.cancel_excel_callback()
    def generate_excel(self):
        docs=[d for i,d in enumerate(self.documents) if i in self.selected]
        if not docs:
            messagebox.showwarning("Resumen Excel","Debe seleccionar al menos un documento.",parent=self); return
        if not self.remittance_resolver or not self.excel_callback:
            messagebox.showerror("Resumen Excel","La generación Excel no está disponible.",parent=self); return
        remittances,missing=collect_unique_remittances(docs,self.remittance_resolver)
        if missing:
            labels="\n".join(f"- documento {d.document_id or d.file_path.name}" for d in missing)
            messagebox.showwarning("Documentos sin remesa",f"Los siguientes documentos no tienen una remesa asociada válida:\n{labels}",parent=self)
        if not remittances:
            messagebox.showwarning("Resumen Excel","Los documentos seleccionados no contienen ninguna remesa válida\npara generar el resumen Excel.",parent=self); return
        if not messagebox.askyesno("Generar resumen Excel",f"Se han seleccionado {len(docs)} documentos correspondientes a {len(remittances)} remesas.\n\nEl Excel incluirá las liquidaciones completas de esas {len(remittances)} remesas,\nno solamente los socios seleccionados.\n\n¿Desea continuar?",parent=self): return
        self.cancelled=False; self._set_generating(True)
        try:
            self.progress.set(f"Procesando remesa 0 de {len(remittances)}")
            self.excel_callback(remittances)
        except Exception:
            logger.exception("Error al generar resumen Excel desde la ventana de unificación",extra={"selected_document_count":len(docs),"unique_remittance_count":len(remittances),"remittance_ids":[r.remittance_id for r in remittances],"campaign":self.campaign.get(),"company":self.company.get(),"crop":self.crop.get(),"destination_path":None})
            messagebox.showerror("Resumen Excel","No se pudo generar el resumen Excel.",parent=self)
        finally:
            self._set_generating(False); self.progress.set("")
    def generate_csv(self):
        docs=[d for i,d in enumerate(self.documents) if i in self.selected]
        if not docs:
            messagebox.showwarning("CSV contable","Debe seleccionar al menos un documento.",parent=self); return
        if not self.csv_export_service:
            messagebox.showerror("CSV contable","La exportación CSV no está disponible.",parent=self); return
        batch_ids,missing=collect_unique_batch_ids(docs)
        if missing:
            labels="\n".join(f"- documento {d.document_id or d.file_path.name}" for d in missing)
            messagebox.showwarning("Documentos sin liquidación",f"Los siguientes documentos no tienen una liquidación guardada asociada:\n{labels}",parent=self)
        if not batch_ids:
            messagebox.showwarning("CSV contable","Los documentos seleccionados no contienen liquidaciones exportables.",parent=self); return
        if any(d.batch_status=="VOIDED" for d in docs):
            messagebox.showerror("CSV contable","Las liquidaciones anuladas no se pueden exportar a contabilidad.",parent=self); return
        if not messagebox.askyesno("Generar CSV contable",f"Se han seleccionado {len(docs)} documentos correspondientes a {len(batch_ids)} remesas.\n\nEl CSV incluirá las liquidaciones completas de esas remesas y aplicará las mismas validaciones contables que la exportación individual.\n\n¿Desea continuar?",parent=self): return
        self.cancelled=False; self._set_generating(True)
        try:
            result=self.csv_export_service.export_batches(batch_ids, progress_callback=self._csv_progress, should_cancel=lambda:self.cancelled)
            if result.already_existed:
                messagebox.showinfo("CSV contable","Esta selección ya fue exportada a contabilidad.",parent=self); return
            if not result.success:
                messagebox.showerror("CSV contable",result.error_message or "No se pudo generar el CSV contable.",parent=self); return
            messagebox.showinfo("CSV contable",f"CSV generado correctamente.\n\nRemesas: {len(batch_ids)}\nLíneas: {result.line_count}\nLíneas excluidas: {result.excluded_line_count}\nNeto: {result.net_total}\nImporte total: {result.amount_total}\nRuta:\n{result.csv_path}",parent=self)
            if messagebox.askyesno("Abrir CSV","¿Desea abrir el CSV contable?",parent=self): open_path(result.csv_path)
        except CsvExportCancelled:
            messagebox.showinfo("CSV contable","Operación cancelada.",parent=self)
        except Exception as exc:
            logger.exception("Exportación CSV contable masiva")
            messagebox.showerror("CSV contable",str(exc),parent=self)
        finally:
            self._set_generating(False); self.progress.set("")
    def _csv_progress(self,phase,current,total):
        self.progress.set(f"{phase.capitalize()} CSV: {current} de {total}"); self.update()
    def _progress(self,phase,current,total,pages): self.progress.set(f"{phase.capitalize()} documento {current} de {total}. Páginas procesadas: {pages}"); self.update()
    def regenerate(self):
        if self.kind.get()=="Borradores": messagebox.showinfo("Regenerar","Los borradores no pueden regenerarse automáticamente.",parent=self); return
        if not self.regenerate_callback: messagebox.showinfo("Regenerar","Abra Liquidaciones guardadas para regenerar este definitivo sin recalcular.",parent=self); return
        self.regenerate_callback(); self.search()
