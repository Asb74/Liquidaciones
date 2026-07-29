from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk


class LiquidationPrefixMasterDialog(tk.Toplevel):
    """CRUD del maestro SQLite de prefijos de liquidación."""

    def __init__(self, parent: tk.Misc, repository, on_saved=None):
        super().__init__(parent)
        self.repository=repository; self.on_saved=on_saved
        self.title("Maestro de prefijos de liquidación"); self.geometry("720x430")
        self.transient(parent); self.grab_set()
        self.crop=tk.StringVar(); self.prefix=tk.StringVar(); self.active=tk.BooleanVar(value=True); self.description=tk.StringVar()
        form=ttk.Frame(self); form.pack(fill="x",padx=10,pady=8)
        for row,(label,var,width) in enumerate((("Cultivo",self.crop,24),("Prefijo",self.prefix,8),("Descripción",self.description,50))):
            ttk.Label(form,text=label).grid(row=row,column=0,sticky="w",pady=3); ttk.Entry(form,textvariable=var,width=width).grid(row=row,column=1,sticky="w",pady=3)
        ttk.Checkbutton(form,text="Activo",variable=self.active).grid(row=1,column=2,padx=10)
        self.tree=ttk.Treeview(self,columns=("crop","prefix","active","description"),show="headings")
        for col,title,width in (("crop","Cultivo",150),("prefix","Prefijo",80),("active","Activo",70),("description","Descripción",330)):
            self.tree.heading(col,text=title); self.tree.column(col,width=width)
        self.tree.pack(fill="both",expand=True,padx=10); self.tree.bind("<<TreeviewSelect>>",self._select); self.tree.bind("<Double-1>",self._select)
        bar=ttk.Frame(self); bar.pack(fill="x",padx=10,pady=8)
        for text,cmd in (("Nuevo",self._new),("Editar",self._select),("Eliminar",self._delete),("Activar/desactivar",self._toggle),("Guardar",self._save),("Cancelar",self._new),("Cerrar",self.destroy)):
            ttk.Button(bar,text=text,command=cmd).pack(side="left",padx=2)
        self._reload()

    def _reload(self):
        self.tree.delete(*self.tree.get_children())
        for row in self.repository.list_prefixes():
            self.tree.insert("","end",iid=row["crop"],values=(row["crop"],row["prefix"],"Sí" if row["active"] else "No",row["description"] or ""))
    def _new(self):
        self.crop.set(""); self.prefix.set(""); self.active.set(True); self.description.set("")
    def _select(self,_event=None):
        if not self.tree.selection(): return
        values=self.tree.item(self.tree.selection()[0],"values")
        self.crop.set(values[0]); self.prefix.set(values[1]); self.active.set(values[2]=="Sí"); self.description.set(values[3])
    def _save(self):
        try:
            self.repository.save_prefix(self.crop.get(),self.prefix.get(),active=self.active.get(),description=self.description.get().strip() or None)
            self._reload(); self.on_saved and self.on_saved(); messagebox.showinfo("Prefijos","Prefijo guardado.",parent=self)
        except Exception as exc: messagebox.showerror("Prefijos",str(exc),parent=self)
    def _delete(self):
        crop=self.crop.get().strip()
        if crop and messagebox.askyesno("Eliminar",f"¿Eliminar el prefijo de {crop}?",parent=self):
            self.repository.delete_prefix(crop); self._new(); self._reload(); self.on_saved and self.on_saved()
    def _toggle(self):
        if self.crop.get(): self.active.set(not self.active.get()); self._save()
