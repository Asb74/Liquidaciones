from __future__ import annotations
import os, tkinter as tk
from tkinter import ttk
from domain.models import WorkContext

class ContextPanel(ttk.LabelFrame):
    def __init__(self, master, config, on_change):
        super().__init__(master, text="Contexto obligatorio")
        self.config=config; self.on_change=on_change
        self.campana=tk.StringVar(); self.empresa=tk.StringVar(); self.cultivo=tk.StringVar()
        ttk.Label(self, text="Remesas de liquidaciones", style="Title.TLabel").grid(row=0,column=0,columnspan=2,sticky="w")
        ttk.Label(self, text="MODO PRUEBA - SQLITE", style="Mode.TLabel").grid(row=0,column=2,sticky="w")
        self.status=tk.StringVar(value="Bases pendientes de comprobar")
        ttk.Label(self, textvariable=self.status).grid(row=1,column=0,columnspan=6,sticky="w")
        for i,(label,var) in enumerate((("Campaña",self.campana),("Empresa",self.empresa),("Cultivo",self.cultivo))):
            ttk.Label(self,text=label).grid(row=2,column=i*2,sticky="w",padx=4,pady=4)
            cb=ttk.Combobox(self,textvariable=var,state="readonly",width=18); cb.grid(row=2,column=i*2+1,sticky="ew",padx=4,pady=4)
            setattr(self,label.lower()+"_cb",cb); cb.bind("<<ComboboxSelected>>", lambda e: self.on_change())
        self.columnconfigure(5, weight=1)
    def set_status(self, status: dict[str,str]) -> None:
        self.status.set(" | ".join(f"{k}: {v}" for k,v in status.items()))
    def context(self) -> WorkContext: return WorkContext(self.campana.get(), self.empresa.get(), self.cultivo.get())
    def clear_downstream(self, level: str) -> None:
        if level in ("campana",): self.empresa.set(""); self.cultivo.set("")
        if level in ("empresa",): self.cultivo.set("")
