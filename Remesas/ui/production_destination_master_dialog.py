from __future__ import annotations
import tkinter as tk
from tkinter import messagebox, ttk
from domain.production_destination_master import DEFAULT_PRODUCTION_DESTINATION_MASTER, ProductionDestinationConfig
from services.production_destination_master_service import ProductionDestinationMasterService

class ProductionDestinationMasterDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, service: ProductionDestinationMasterService|None=None):
        super().__init__(master); self.title("Maestro de destinos de producción"); self.geometry("820x360"); self.service=service or ProductionDestinationMasterService(); self.items=list(self.service.list_all()); self._build(); self._reload()
    def _build(self):
        cols=("crop","primary","secondary_enabled","secondary","counts","waste","active")
        self.tree=ttk.Treeview(self, columns=cols, show="headings", height=9)
        for c,t in zip(cols,("Cultivo","Etiqueta principal","Existe secundario","Etiqueta secundaria","Cuenta comercial","Residuos/podrido","Activo")):
            self.tree.heading(c,text=t); self.tree.column(c,width=115)
        self.tree.pack(fill="both",expand=True,padx=8,pady=8)
        form=ttk.Frame(self); form.pack(fill="x",padx=8)
        self.vars={k:tk.StringVar() for k in ("crop","primary","secondary","waste")}; self.bools={k:tk.BooleanVar() for k in ("secondary_enabled","counts","active")}
        for i,(lab,k) in enumerate((("Cultivo","crop"),("Principal","primary"),("Secundaria","secondary"),("Residuo","waste"))): ttk.Label(form,text=lab).grid(row=0,column=i*2); ttk.Entry(form,textvariable=self.vars[k],width=16).grid(row=0,column=i*2+1)
        for i,(lab,k) in enumerate((("Existe secundario","secondary_enabled"),("Cuenta comercial","counts"),("Activo","active"))): ttk.Checkbutton(form,text=lab,variable=self.bools[k]).grid(row=1,column=i,sticky="w")
        self.tree.bind("<<TreeviewSelect>>", self._select)
        btn=ttk.Frame(self); btn.pack(fill="x",padx=8,pady=8)
        ttk.Button(btn,text="Guardar",command=self._save).pack(side="left",padx=3); ttk.Button(btn,text="Restaurar valores iniciales",command=self._defaults).pack(side="left",padx=3); ttk.Button(btn,text="Copiar configuración desde otro cultivo",command=self._copy).pack(side="left",padx=3); ttk.Button(btn,text="Cerrar",command=self.destroy).pack(side="right",padx=3)
    def _reload(self):
        self.tree.delete(*self.tree.get_children())
        for i,x in enumerate(self.items): self.tree.insert("", "end", iid=str(i), values=(x.crop,x.primary_label,x.secondary_enabled,x.secondary_label,x.secondary_counts_as_commercial,x.waste_label,x.active))
    def _select(self,*_):
        if not self.tree.selection(): return
        x=self.items[int(self.tree.selection()[0])]; self.vars["crop"].set(x.crop); self.vars["primary"].set(x.primary_label); self.vars["secondary"].set(x.secondary_label); self.vars["waste"].set(x.waste_label); self.bools["secondary_enabled"].set(x.secondary_enabled); self.bools["counts"].set(x.secondary_counts_as_commercial); self.bools["active"].set(x.active)
    def _current(self): return ProductionDestinationConfig(self.vars["crop"].get(), self.vars["primary"].get(), self.bools["secondary_enabled"].get(), self.vars["secondary"].get(), self.bools["counts"].get(), self.vars["waste"].get(), self.bools["active"].get())
    def _save(self):
        item=self._current(); self.items=[x for x in self.items if x.crop!=item.crop]+[item]; self.service.save_all(self.items); self._reload(); messagebox.showinfo("Destinos de producción","Configuración guardada.")
    def _defaults(self): self.items=[ProductionDestinationConfig(**i) for i in DEFAULT_PRODUCTION_DESTINATION_MASTER["items"]]; self.service.save_all(self.items); self._reload()
    def _copy(self):
        if self.tree.selection(): self._select()
