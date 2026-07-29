from __future__ import annotations
import json, tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from data.calibre_master_repository import CalibreMasterRepository
from domain.calibre_master import CalibreMasterItem, DEFAULT_CALIBRE_MASTER

class CalibreMasterDialog(tk.Toplevel):
    def __init__(self, master: tk.Misc, repository: CalibreMasterRepository|None=None):
        super().__init__(master); self.title("Maestro de calibres y categorías"); self.geometry("760x430"); self.repository=repository or CalibreMasterRepository(); self.items=list(self.repository.load_items()); self.crop_var=tk.StringVar(); self._build(); self._reload_crops()
    def _build(self):
        top=ttk.Frame(self); top.pack(fill="x",padx=10,pady=8); ttk.Label(top,text="Cultivo").pack(side="left"); self.combo=ttk.Combobox(top,textvariable=self.crop_var,state="readonly",width=24); self.combo.pack(side="left",padx=6); self.combo.bind("<<ComboboxSelected>>",lambda e:self._fill())
        cols=("base","field","label","order","active"); self.tree=ttk.Treeview(self,columns=cols,show="headings",height=12)
        for col,txt,w in (("base","Base",70),("field","Campo PesosFres",120),("label","Descripción",300),("order","Orden",70),("active","Activo",70)): self.tree.heading(col,text=txt); self.tree.column(col,width=w,anchor="w")
        self.tree.pack(fill="both",expand=True,padx=10); self.tree.bind("<Double-1>",lambda e:self.edit_description())
        btn=ttk.Frame(self); btn.pack(fill="x",padx=10,pady=8)
        for text,cmd in (("Nuevo cultivo",self.new_crop),("Editar descripción",self.edit_description),("Copiar configuración desde otro cultivo",self.copy_from_crop),("Guardar",self.save),("Restaurar valores iniciales",self.restore_defaults),("Exportar a JSON",self.export_json),("Importar desde JSON",self.import_json),("Cerrar",self.destroy)): ttk.Button(btn,text=text,command=cmd).pack(side="left",padx=2)
    def _reload_crops(self):
        crops=sorted({i.crop for i in self.items}); self.combo["values"]=crops; self.crop_var.set(self.crop_var.get() or (crops[0] if crops else "")); self._fill()
    def _fill(self):
        self.tree.delete(*self.tree.get_children()); crop=self.crop_var.get(); rows={i.base:i for i in self.items if i.crop==crop}
        for n in range(12):
            item=rows.get(f"c{n}"); self.tree.insert("","end",iid=f"c{n}",values=(f"c{n}",f"Cal{n}",item.label if item else "", item.order if item else n, "Sí" if (not item or item.active) else "No"))
    def new_crop(self):
        crop=simpledialog.askstring("Nuevo cultivo","Cultivo:",parent=self)
        if not crop: return
        crop=crop.strip().upper(); self.items.extend(CalibreMasterItem(f"c{i}",crop,f"CAL {i}",i,True) for i in range(12)); self.crop_var.set(crop); self._reload_crops()
    def edit_description(self):
        sel=self.tree.selection();
        if not sel: return
        base=sel[0]; crop=self.crop_var.get(); old=self.tree.set(base,"label"); label=simpledialog.askstring("Editar descripción",f"Descripción para {base} / {crop}:",initialvalue=old,parent=self)
        if label is None: return
        self.items=[i for i in self.items if not (i.base==base and i.crop==crop)]+[CalibreMasterItem(base,crop,label,int(base[1:]),True)]; self._fill()
    def copy_from_crop(self):
        src=simpledialog.askstring("Copiar configuración","Cultivo origen:",parent=self); dst=simpledialog.askstring("Copiar configuración","Cultivo destino:",initialvalue=self.crop_var.get(),parent=self)
        if not src or not dst: return
        src=src.strip().upper(); dst=dst.strip().upper(); source=sorted([i for i in self.items if i.crop==src], key=lambda x:x.order)
        if not source: messagebox.showwarning("Copiar", "No existe el cultivo origen."); return
        self.items=[i for i in self.items if i.crop!=dst]+[CalibreMasterItem(i.base,dst,i.label,i.order,i.active) for i in source]; self.crop_var.set(dst); self._reload_crops()
    def save(self): self.repository.save_items(self.items); messagebox.showinfo("Maestro", "Maestro guardado.")
    def restore_defaults(self): self.items=[CalibreMasterItem(**i) for i in DEFAULT_CALIBRE_MASTER["items"]]; self._reload_crops()
    def export_json(self):
        path=filedialog.asksaveasfilename(defaultextension=".json");
        if path: Path(path).write_text(json.dumps({"version":1,"items":[i.__dict__ for i in self.items]},ensure_ascii=False,indent=2),encoding="utf-8")
    def import_json(self):
        path=filedialog.askopenfilename(filetypes=[("JSON","*.json")]);
        if path: self.items=[CalibreMasterItem(**i) for i in json.loads(Path(path).read_text(encoding="utf-8")).get("items",[])]; self._reload_crops()
