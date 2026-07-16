from __future__ import annotations

from datetime import datetime, timezone
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk


class LiquidationSplitMasterDialog(tk.Toplevel):
    """Editor de reglas y destinatarios de división almacenados en SQLite."""
    MODES=("PERCENTAGE","PERCENTAGE_WITH_RESIDUAL","EQUAL_PARTS","WEIGHTS")
    def __init__(self,parent: tk.Misc,repository,on_saved=None):
        super().__init__(parent); self.repository=repository; self.on_saved=on_saved
        self.title("Maestro de división de liquidaciones"); self.geometry("1120x650"); self.transient(parent); self.grab_set()
        self.vars={name:tk.StringVar() for name in ("source_member_id","source_member_name","campaign","crop","variety","remittance_id","effective_from","effective_to","priority","notes")}
        self.vars["priority"].set("100"); self.kind=tk.StringVar(value=self.MODES[0]); self.active=tk.BooleanVar(value=True); self.recipients=[]
        form=ttk.LabelFrame(self,text="Regla"); form.pack(fill="x",padx=8,pady=6)
        fields=(("Socio origen","source_member_id"),("Nombre","source_member_name"),("Campaña","campaign"),("Cultivo","crop"),("Variedad","variety"),("Remesa","remittance_id"),("Vigente desde","effective_from"),("Vigente hasta","effective_to"),("Prioridad","priority"),("Observaciones","notes"))
        for i,(label,key) in enumerate(fields):
            ttk.Label(form,text=label).grid(row=i//5*2,column=i%5,sticky="w",padx=3); ttk.Entry(form,textvariable=self.vars[key],width=20).grid(row=i//5*2+1,column=i%5,padx=3,pady=(0,5))
        ttk.Label(form,text="Tipo").grid(row=4,column=0,sticky="w"); ttk.Combobox(form,textvariable=self.kind,values=self.MODES,state="readonly",width=28).grid(row=5,column=0,sticky="w")
        ttk.Checkbutton(form,text="Activa",variable=self.active).grid(row=5,column=1,sticky="w")
        panes=ttk.Panedwindow(self,orient="horizontal"); panes.pack(fill="both",expand=True,padx=8)
        self.rules=ttk.Treeview(panes,columns=("id","source","name","type","filters","priority","active"),show="headings")
        self.targets=ttk.Treeview(panes,columns=("member","name","value","residual","order","active"),show="headings")
        for tree,columns in ((self.rules,("id","source","name","type","filters","priority","active")),(self.targets,("member","name","value","residual","order","active"))):
            for c in columns: tree.heading(c,text=c.title()); tree.column(c,width=100)
            panes.add(tree,weight=1)
        self.rules.bind("<<TreeviewSelect>>",self._select_rule)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=8,pady=8)
        for text,cmd in (("Nuevo",self._new),("Editar",self._select_rule),("Eliminar",self._delete),("Activar/desactivar",self._toggle),("Añadir destinatario",self._add_recipient),("Quitar destinatario",self._remove_recipient),("Guardar",self._save),("Cancelar",self._new),("Cerrar",self.destroy)):
            ttk.Button(bar,text=text,command=cmd).pack(side="left",padx=2)
        self._reload()
    def _reload(self):
        self.rules.delete(*self.rules.get_children())
        for r in self.repository.list_rules():
            filters=" / ".join(str(r[x]) for x in ("campaign","crop","variety","remittance_id") if r[x])
            self.rules.insert("","end",iid=str(r["id"]),values=(r["id"],r["source_member_id"],r["source_member_name"] or "",r["split_type"],filters,r["priority"],"Sí" if r["active"] else "No"))
    def _new(self):
        for v in self.vars.values(): v.set("")
        self.vars["priority"].set("100"); self.kind.set(self.MODES[0]); self.active.set(True); self.recipients=[]; self._fill_targets()
    def _select_rule(self,_event=None):
        if not self.rules.selection(): return
        rule=self.repository.get_rule(int(self.rules.selection()[0])); self._editing_id=rule["id"]
        for key in self.vars: self.vars[key].set(rule.get(key) or "")
        self.kind.set(rule["split_type"]); self.active.set(bool(rule["active"])); self.recipients=list(rule["recipients"]); self._fill_targets()
    def _fill_targets(self):
        self.targets.delete(*self.targets.get_children())
        for i,r in enumerate(self.recipients): self.targets.insert("","end",iid=str(i),values=(r[0],r[1],r[2],"Sí" if r[3] else "No",i,"Sí"))
    def _add_recipient(self):
        member=simpledialog.askinteger("Destinatario","Socio destino:",parent=self)
        if member is None:return
        name=simpledialog.askstring("Destinatario","Nombre:",parent=self) or ""; value=simpledialog.askstring("Destinatario","Valor:",initialvalue="0",parent=self)
        if value is None:return
        residual=messagebox.askyesno("Destinatario","¿Es destinatario residual?",parent=self); self.recipients.append((member,name,value,residual)); self._fill_targets()
    def _remove_recipient(self):
        if self.targets.selection(): self.recipients.pop(int(self.targets.selection()[0])); self._fill_targets()
    def _save(self):
        try:
            filters={k:(v.get().strip() or None) for k,v in self.vars.items() if k not in {"source_member_id","source_member_name","priority","notes"}}
            filters.update(source_member_name=self.vars["source_member_name"].get(),priority=int(self.vars["priority"].get() or 100),notes=self.vars["notes"].get(),active=self.active.get())
            self.repository.save_rule(int(self.vars["source_member_id"].get()),self.kind.get(),self.recipients,rule_id=getattr(self,"_editing_id",None),**filters)
            self._new(); self._reload(); self.on_saved and self.on_saved(); messagebox.showinfo("Divisiones","Regla guardada.",parent=self)
        except Exception as exc: messagebox.showerror("Divisiones",str(exc),parent=self)
    def _delete(self):
        if self.rules.selection() and messagebox.askyesno("Eliminar","¿Eliminar la regla?",parent=self): self.repository.delete_rule(int(self.rules.selection()[0])); self._new(); self._reload(); self.on_saved and self.on_saved()
    def _toggle(self): self.active.set(not self.active.get())
