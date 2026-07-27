from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk


EMPTY_SELECTION_MESSAGE = "Debe seleccionar al menos una liquidación, remesa o archivo."
COLUMNS = ("IdREMESA", "REMESA", "FECHARE", "PERIODO1", "PERIODO2", "CATEGORIA", "TipoLiq")


class ExcelConsolidationDialog(tk.Toplevel):
    """Non-modal selector dedicated to the optional consolidated workbook."""

    def __init__(self, parent, items, context, *, selection_factory, on_generate):
        super().__init__(parent)
        self.title("Montar resúmenes Excel")
        self.geometry("980x520")
        self.transient(parent)
        self._items = list(items)
        self._context = context
        self._selection_factory = selection_factory
        self._on_generate = on_generate
        self._sort_column = "IdREMESA"
        self._descending = False

        ttk.Label(self, text=f"Campaña: {context.campana} | Empresa: {context.empresa} | Cultivo: {context.cultivo}").pack(anchor="w", padx=8, pady=6)
        self.query = tk.StringVar()
        ttk.Entry(self, textvariable=self.query).pack(fill="x", padx=8, pady=4)
        self.counter = tk.StringVar(value="Elementos seleccionados: 0")
        ttk.Label(self, textvariable=self.counter).pack(anchor="w", padx=8)
        self.tree = ttk.Treeview(self, columns=COLUMNS, show="headings", selectmode="extended")
        for column in COLUMNS:
            self.tree.heading(column, text=column, command=lambda c=column: self._sort(c))
            self.tree.column(column, width=130, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=8, pady=4)
        self.tree.bind("<<TreeviewSelect>>", self._update_counter)
        self.query.trace_add("write", lambda *_: self._fill())

        buttons = ttk.Frame(self); buttons.pack(fill="x", padx=8, pady=8)
        ttk.Button(buttons, text="Seleccionar todas", command=self._select_all).pack(side="left", padx=3)
        ttk.Button(buttons, text="Quitar selección", command=self._clear_selection).pack(side="left", padx=3)
        ttk.Button(buttons, text="Limpiar", command=self._clear_selection).pack(side="left", padx=3)
        ttk.Button(buttons, text="Cerrar", command=self.destroy).pack(side="right", padx=3)
        ttk.Button(buttons, text="Generar Excel", command=self._generate).pack(side="right", padx=3)
        self._fill()

    def _fill(self):
        query = self.query.get().strip().casefold()
        rows = [row for row in self._items if not query or query in " ".join(str(row.get(c) or "") for c in COLUMNS).casefold()]
        rows.sort(key=lambda row: str(row.get(self._sort_column) or "").casefold(), reverse=self._descending)
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=[row.get(column) or "" for column in COLUMNS])
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
        self.counter.set(f"Elementos seleccionados: {len(self.tree.selection())}")

    def _generate(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Selección obligatoria", EMPTY_SELECTION_MESSAGE, parent=self)
            return
        remittances = [self._selection_factory(self.tree.item(item, "values"), self._context) for item in selected]
        self._on_generate(remittances)

