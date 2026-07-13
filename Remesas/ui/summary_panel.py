from __future__ import annotations
import tkinter as tk
from tkinter import ttk

class SummaryPanel(ttk.LabelFrame):
    def __init__(self, master):
        super().__init__(master, text="Resumen y advertencias")
        self.vars={k:tk.StringVar(value="0") for k in ["entregas","socios","variedades","kilos","primera","ultima","liquidadas","sin_variedad","sin_socio","sin_categoria"]}
        for i,(label,key) in enumerate([("Nº entregas","entregas"),("Nº socios","socios"),("Nº variedades","variedades"),("Kilos netos","kilos"),("Primera fecha","primera"),("Última fecha","ultima"),("Liquidadas","liquidadas"),("Sin variedad","sin_variedad"),("Sin socio válido","sin_socio"),("Sin categoría","sin_categoria")]):
            ttk.Label(self,text=label).grid(row=i,column=0,sticky="w"); ttk.Label(self,textvariable=self.vars[key]).grid(row=i,column=1,sticky="e")
        self.warn=tk.Text(self,height=5,width=38); self.warn.grid(row=10,column=0,columnspan=2,sticky="ew",pady=4)
        ttk.Label(self,text="Conceptos económicos: Pendiente").grid(row=11,column=0,columnspan=2,sticky="w")
    def set_summary(self,s):
        vals={"entregas":s.total_entregas,"socios":s.socios,"variedades":s.variedades,"kilos":f"{s.kilos_netos:,.2f}","primera":s.primera_fecha,"ultima":s.ultima_fecha,"liquidadas":s.liquidadas,"sin_variedad":s.sin_variedad,"sin_socio":s.sin_socio_valido,"sin_categoria":s.sin_categoria}
        for k,v in vals.items(): self.vars[k].set(str(v))
        self.warn.delete("1.0","end"); self.warn.insert("1.0", "\n".join(s.warnings))
    def clear(self):
        for v in self.vars.values(): v.set("0")
        self.warn.delete("1.0","end")
