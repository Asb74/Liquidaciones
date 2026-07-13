from __future__ import annotations
import tkinter as tk
from tkinter import ttk

COLUMNS=("Fecha","Registro","Socio","Nombre socio","Variedad","Categoría","Neto","Albarán","Boleta","Plataforma","Liquidado","Precalibrado")
class DeliveriesPanel(ttk.LabelFrame):
    def __init__(self, master):
        super().__init__(master, text="Entregas del periodo")
        self.tree=ttk.Treeview(self, columns=COLUMNS, show="headings", height=14)
        for c in COLUMNS: self.tree.heading(c,text=c); self.tree.column(c,width=105,anchor="w")
        y=ttk.Scrollbar(self, orient="vertical", command=self.tree.yview); self.tree.configure(yscrollcommand=y.set)
        self.tree.grid(row=0,column=0,sticky="nsew"); y.grid(row=0,column=1,sticky="ns")
        self.columnconfigure(0,weight=1); self.rowconfigure(0,weight=1)
    def set_rows(self, rows):
        self.clear()
        for d in rows: self.tree.insert("", "end", values=(d.fecha,d.registro,d.socio,d.nombre_socio,d.variedad,d.categoria,d.neto,d.albaran,d.boleta,d.plataforma,d.liquidado,d.precalibrado))
    def clear(self):
        for item in self.tree.get_children(): self.tree.delete(item)
    def visible_rows(self): return [self.tree.item(i)["values"] for i in self.tree.get_children()]
