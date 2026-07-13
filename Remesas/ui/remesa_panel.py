from __future__ import annotations
import tkinter as tk
from tkinter import ttk

class RemesaPanel(ttk.LabelFrame):
    def __init__(self, master):
        super().__init__(master, text="Datos de remesa")
        self.vars={k: tk.StringVar() for k in ["remesa","fecha_pago","desde","hasta","tipo","categoria","socio"]}
        labels=[("Nombre", "remesa"),("Fecha pago","fecha_pago"),("Periodo desde","desde"),("Periodo hasta","hasta"),("Tipo liquidación","tipo"),("Condición/Categoría","categoria"),("Socio (0=todos)","socio")]
        for i,(txt,key) in enumerate(labels):
            ttk.Label(self,text=txt).grid(row=i//2,column=(i%2)*2,sticky="w",padx=4,pady=3)
            ttk.Entry(self,textvariable=self.vars[key],width=24).grid(row=i//2,column=(i%2)*2+1,sticky="ew",padx=4,pady=3)
        ttk.Label(self,text="Observaciones").grid(row=4,column=0,sticky="nw",padx=4)
        self.observaciones=tk.Text(self,height=3,width=48); self.observaciones.grid(row=4,column=1,columnspan=3,sticky="ew",padx=4)
    def data(self): return {k:v.get() for k,v in self.vars.items()} | {"observaciones": self.observaciones.get("1.0","end").strip()}
    def load(self, values: dict):
        mapping={"REMESA":"remesa","FECHARE":"fecha_pago","PERIODO1":"desde","PERIODO2":"hasta","TipoLiq":"tipo","CATEGORIA":"categoria","IdSocio":"socio"}
        for src,dst in mapping.items(): self.vars[dst].set(str(values.get(src) or ""))
        self.observaciones.delete("1.0","end"); self.observaciones.insert("1.0", str(values.get("Observaciones") or ""))
