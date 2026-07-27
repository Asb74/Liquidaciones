from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk


EMPTY_SELECTION_MESSAGE = "Debe seleccionar al menos una remesa."
COLUMNS = (
    "IdREMESA", "REMESA", "CAMPAÑA", "EMPRESA", "CULTIVO", "FECHARE",
    "PERIODO1", "PERIODO2", "CATEGORIA", "TipoLiq", "ESTADO",
    "NUM_ENTREGAS", "NUM_SOCIOS",
)
HEADINGS = {
    "IdREMESA": "Id remesa", "REMESA": "Nombre", "CAMPAÑA": "Campaña",
    "EMPRESA": "Empresa", "CULTIVO": "Cultivo", "FECHARE": "Fecha de pago",
    "PERIODO1": "Periodo desde", "PERIODO2": "Periodo hasta",
    "CATEGORIA": "Categoría", "TipoLiq": "Tipo liquidación", "ESTADO": "Estado",
    "NUM_ENTREGAS": "Entregas", "NUM_SOCIOS": "Socios",
}


class MultiRemittanceSelectionDialog(tk.Toplevel):
    """Shared campaign selector used by both massive PDF and Excel actions."""

    def __init__(self, parent, items, context, *, selection_factory, purpose="pdf", on_generate=None, modal=False):
        super().__init__(parent)
        self.title("Seleccionar remesas")
        self.geometry("1250x560")
        self.transient(parent)
        self._items = list(items)
        self._context = context
        self._selection_factory = selection_factory
        self._on_generate = on_generate
        self._purpose = purpose
        self.result = None
        self._sort_column = "IdREMESA"
        self._descending = False
        if modal:
            self.grab_set()

        ttk.Label(self, text=f"Campaña: {context.campana} | Empresa: {context.empresa} | Cultivo: {context.cultivo}").pack(anchor="w", padx=8, pady=6)
        self.query = tk.StringVar()
        ttk.Entry(self, textvariable=self.query).pack(fill="x", padx=8, pady=4)
        self.counter = tk.StringVar(value="Remesas seleccionadas: 0")
        ttk.Label(self, textvariable=self.counter).pack(anchor="w", padx=8)
        self.tree = ttk.Treeview(self, columns=COLUMNS, show="headings", selectmode="extended")
        for column in COLUMNS:
            self.tree.heading(column, text=HEADINGS[column], command=lambda c=column: self._sort(c))
            self.tree.column(column, width=105, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=8, pady=4)
        self.tree.bind("<<TreeviewSelect>>", self._update_counter)
        self.query.trace_add("write", lambda *_: self._fill())

        buttons = ttk.Frame(self); buttons.pack(fill="x", padx=8, pady=8)
        ttk.Button(buttons, text="Seleccionar todas", command=self._select_all).pack(side="left", padx=3)
        ttk.Button(buttons, text="Quitar selección", command=self._clear_selection).pack(side="left", padx=3)
        ttk.Button(buttons, text="Limpiar", command=self._clear_selection).pack(side="left", padx=3)
        ttk.Button(buttons, text="Cancelar", command=self.destroy).pack(side="right", padx=3)
        action = "Generar Excel" if purpose == "excel" else "Generar PDF"
        ttk.Button(buttons, text=action, command=self._generate).pack(side="right", padx=3)
        self._fill()

    def _visible_items(self):
        query = self.query.get().strip().casefold()
        rows = [row for row in self._items if not query or query in f"{row.get('IdREMESA', '')} {row.get('REMESA', '')}".casefold()]
        rows.sort(key=lambda row: str(row.get(self._sort_column) or "").casefold(), reverse=self._descending)
        return rows

    def _fill(self):
        self.tree.delete(*self.tree.get_children())
        for index, row in enumerate(self._visible_items()):
            self.tree.insert("", "end", iid=str(index), values=[row.get(column) or "" for column in COLUMNS], tags=(str(row.get("IdREMESA")),))
        self._update_counter()

    def _sort(self, column):
        self._descending = not self._descending if self._sort_column == column else False
        self._sort_column = column
        self._fill()

    def _select_all(self):
        self.tree.selection_set(self.tree.get_children()); self._update_counter()

    def _clear_selection(self):
        self.tree.selection_remove(self.tree.selection()); self._update_counter()

    def _update_counter(self, _event=None):
        self.counter.set(f"Remesas seleccionadas: {len(self.tree.selection())}")

    def _generate(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Selección obligatoria", EMPTY_SELECTION_MESSAGE, parent=self)
            return
        rows = {str(row.get("IdREMESA")): row for row in self._items}
        remittances = [self._selection_factory(rows[self.tree.item(item, "tags")[0]], self._context) for item in selected]
        self.result = remittances
        if self._on_generate:
            self.destroy()
            self._on_generate(remittances)
        else:
            self.destroy()
