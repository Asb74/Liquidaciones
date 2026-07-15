from __future__ import annotations

from decimal import Decimal
import tkinter as tk
from tkinter import messagebox, ttk

from domain.hectare_fee_master import HectareFeeMaster, normalize_crops, parse_decimal
from services.hectare_fee_master_service import HectareFeeMasterService

CHECKED = "☑"
UNCHECKED = "☐"


def format_decimal_es_value(value: Decimal) -> str:
    return f"{value:.2f}".replace(".", ",")


class HectareFeeMasterDialog(tk.Toplevel):
    def __init__(self, parent: tk.Misc, service: HectareFeeMasterService, on_saved=None) -> None:
        super().__init__(parent)
        self.service = service
        self.on_saved = on_saved
        self.saved = False
        self.title("Maestro de cuota por hectárea")
        self.transient(parent)
        self.grab_set()
        self.geometry("900x650")
        self.resizable(True, True)
        self.master_data = self.service.load_master()
        self.price_var = tk.StringVar(value=format_decimal_es_value(self.master_data.price_per_hectare))
        self._eligible_selected = set(self.master_data.eligible_crops)
        self._build()
        self._load_lists()
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def _build(self) -> None:
        general = ttk.LabelFrame(self, text="Parámetros generales")
        general.pack(fill="x", padx=8, pady=8)
        ttk.Label(general, text="Precio por hectárea").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Label(general, text=f"Valor actual: {format_decimal_es_value(self.master_data.price_per_hectare)} €/ha").grid(row=0, column=1, sticky="w", padx=6)
        ttk.Entry(general, textvariable=self.price_var, width=18).grid(row=0, column=2, sticky="w", padx=6)

        lists = ttk.Frame(self)
        lists.pack(fill="both", expand=True, padx=8, pady=4)
        ttk.Label(lists, text="Estos cultivos se utilizarán tanto para calcular la superficie como para sumar los kilos anuales del socio.").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.eligible_tree = self._build_tree(lists, "CULTIVOS SUJETOS A CUOTA HA")
        self.eligible_tree.master.grid(row=1, column=0, sticky="nsew")
        lists.columnconfigure(0, weight=1); lists.rowconfigure(1, weight=1)

        buttons = ttk.Frame(self)
        buttons.pack(fill="x", padx=8, pady=8)
        ttk.Button(buttons, text="Restaurar valores iniciales", command=self._restore_defaults).pack(side="left", padx=3)
        ttk.Button(buttons, text="Seleccionar todos", command=lambda: self._set_all(self.eligible_tree, self._eligible_selected, True)).pack(side="left", padx=3)
        ttk.Button(buttons, text="Quitar todos", command=lambda: self._set_all(self.eligible_tree, self._eligible_selected, False)).pack(side="left", padx=3)
        ttk.Button(buttons, text="Cancelar", command=self.destroy).pack(side="right", padx=3)
        ttk.Button(buttons, text="Guardar", command=self._save).pack(side="right", padx=3)

    def _build_tree(self, parent: tk.Misc, title: str) -> ttk.Treeview:
        frame = ttk.LabelFrame(parent, text=title)
        tree = ttk.Treeview(frame, columns=("active", "crop", "status"), show="headings", selectmode="browse")
        tree.heading("active", text="Activo"); tree.heading("crop", text="Cultivo"); tree.heading("status", text="Origen")
        tree.column("active", width=70, anchor="center"); tree.column("crop", width=220); tree.column("status", width=120)
        y = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=y.set)
        tree.grid(row=0, column=0, sticky="nsew"); y.grid(row=0, column=1, sticky="ns")
        frame.rowconfigure(0, weight=1); frame.columnconfigure(0, weight=1)
        tree.bind("<Double-1>", lambda _e, t=tree: self._toggle_tree(t))
        tree.bind("<space>", lambda _e, t=tree: self._toggle_tree(t))
        return tree

    def _load_lists(self) -> None:
        self._populate(self.eligible_tree, self.service.list_eligible_crop_options(), self._eligible_selected)

    def _populate(self, tree: ttk.Treeview, db_options: list[str], selected: set[str]) -> None:
        tree.delete(*tree.get_children())
        db = set(normalize_crops(db_options))
        missing = [c for c in selected if c not in db]
        for crop in normalize_crops([*db_options, *missing]):
            status = "No encontrado" if crop in missing else "Base de datos"
            tree.insert("", "end", iid=crop, values=(CHECKED if crop in selected else UNCHECKED, crop, status))
        if missing:
            messagebox.showwarning("Maestro cuota Ha", "Hay cultivos del JSON que no aparecen en la base de datos. Se mantienen como No encontrado.")

    def _toggle_tree(self, tree: ttk.Treeview) -> None:
        item = tree.focus()
        if not item: return
        target = self._eligible_selected
        if item in target: target.remove(item)
        else: target.add(item)
        crop = tree.set(item, "crop"); status = tree.set(item, "status")
        tree.item(item, values=(CHECKED if item in target else UNCHECKED, crop, status))

    def _set_all(self, tree: ttk.Treeview, selected: set[str], value: bool) -> None:
        selected.clear()
        if value:
            selected.update(tree.get_children())
        for item in tree.get_children():
            tree.item(item, values=(CHECKED if value else UNCHECKED, tree.set(item, "crop"), tree.set(item, "status")))

    def _restore_defaults(self) -> None:
        self.master_data = self.service.restore_defaults()
        self.price_var.set(format_decimal_es_value(self.master_data.price_per_hectare))
        self._eligible_selected = set(self.master_data.eligible_crops)
        self._load_lists()

    def _validated_master(self) -> HectareFeeMaster:
        try:
            price = parse_decimal(self.price_var.get(), "El precio por hectárea")
        except ValueError as exc:
            raise ValueError("El precio por hectárea debe ser un número mayor que cero.") from exc
        if not self._eligible_selected:
            raise ValueError("Debe seleccionar al menos un cultivo sujeto a Cuota Ha.")
        return HectareFeeMaster(price, normalize_crops(self._eligible_selected))

    def _save(self) -> None:
        try:
            master = self._validated_master()
        except ValueError as exc:
            messagebox.showwarning("Datos inválidos", str(exc)); return
        self.service.save_master(master)
        self.saved = True
        if self.on_saved:
            self.on_saved()
        messagebox.showinfo("Maestro cuota Ha", "La configuración de cuota por hectárea se ha actualizado.\nDebe volver a calcular la liquidación.")
        self.destroy()
