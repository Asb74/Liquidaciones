from __future__ import annotations
from decimal import Decimal
import tkinter as tk
from tkinter import ttk

from domain.utils import format_currency_es, format_decimal_es

class SummaryPanel(ttk.LabelFrame):
    def __init__(self, master):
        super().__init__(master, text="Resumen y advertencias")
        self.vars={k:tk.StringVar(value="0") for k in ["entregas","socios","variedades","kilos","primera","ultima","liquidadas","sin_variedad","sin_socio","sin_categoria"]}
        for i,(label,key) in enumerate([("Nº entregas","entregas"),("Nº socios","socios"),("Nº variedades","variedades"),("Kilos netos","kilos"),("Primera fecha","primera"),("Última fecha","ultima"),("Liquidadas","liquidadas"),("Sin variedad","sin_variedad"),("Sin socio válido","sin_socio"),("Sin categoría","sin_categoria")]):
            ttk.Label(self,text=label).grid(row=i,column=0,sticky="w"); ttk.Label(self,textvariable=self.vars[key]).grid(row=i,column=1,sticky="e")
        self.warn=tk.Text(self,height=5,width=38); self.warn.grid(row=10,column=0,columnspan=2,sticky="ew",pady=4)
        self.economic_vars={k:tk.StringVar(value="Pendiente") for k in ["kilos","comercial","recoleccion","transporte","calidad","globalgap","cuota","base","iva_rate","iva","retencion_rate","retencion","total"]}
        self.econ=ttk.LabelFrame(self,text="Totales económicos"); self.econ.grid(row=11,column=0,columnspan=2,sticky="ew")
        labels=[("Kilos netos","kilos"),("Importe comercial","comercial"),("Recolección","recoleccion"),("Transporte","transporte"),("Calidad","calidad"),("GlobalGAP","globalgap"),("Cuota Ha","cuota"),("Base imponible","base"),("IVA (%)","iva_rate"),("Importe IVA","iva"),("Retención (%)","retencion_rate"),("Importe Retención","retencion"),("Importe Total","total")]
        for i,(label,key) in enumerate(labels):
            ttk.Label(self.econ,text=label).grid(row=i,column=0,sticky="w"); ttk.Label(self.econ,textvariable=self.economic_vars[key]).grid(row=i,column=1,sticky="e")
    def set_summary(self,s):
        vals={"entregas":s.total_entregas,"socios":s.socios,"variedades":s.variedades,"kilos":f"{s.kilos_netos:,.2f}","primera":s.primera_fecha,"ultima":s.ultima_fecha,"liquidadas":s.liquidadas,"sin_variedad":s.sin_variedad,"sin_socio":s.sin_socio_valido,"sin_categoria":s.sin_categoria}
        for k,v in vals.items(): self.vars[k].set(str(v))
        self.warn.delete("1.0","end"); self.warn.insert("1.0", "\n".join(s.warnings))
    def set_calculation(self, result):
        totals = result.result.totals if result.result else None
        self.economic_vars["kilos"].set(format_decimal_es(result.net_kg, 3))
        self.economic_vars["comercial"].set(format_currency_es(result.commercial_amount))
        mapping = {"recoleccion": getattr(totals, "collection_amount", None), "transporte": getattr(totals, "transport_amount", None), "calidad": getattr(totals, "quality_amount", None), "globalgap": getattr(totals, "globalgap_amount", None), "cuota": getattr(totals, "hectare_fee_amount", None), "base": getattr(totals, "taxable_base", None), "iva": getattr(totals, "vat_amount", None), "retencion": getattr(totals, "withholding_amount", None), "total": getattr(totals, "total_amount", None)}
        members = tuple(getattr(result.result, "member_results", ()) if result.result else ())
        vat_rates = {m.vat_rate for m in members if m.vat_rate is not None}
        withholding_rates = {m.withholding_rate for m in members if m.withholding_rate is not None}
        self.economic_vars["iva_rate"].set((f"{next(iter(vat_rates)):g} %" if len(vat_rates) == 1 else "Varios") if vat_rates else "Pendiente")
        self.economic_vars["retencion_rate"].set((f"{next(iter(withholding_rates)):g} %" if len(withholding_rates) == 1 else "Varios") if withholding_rates else "Pendiente")
        for key, value in mapping.items():
            self.economic_vars[key].set(format_currency_es(value) if value is not None else "Pendiente")
    def clear(self):
        for v in self.vars.values(): v.set("0")
        for v in self.economic_vars.values(): v.set("Pendiente")
        self.warn.delete("1.0","end")
