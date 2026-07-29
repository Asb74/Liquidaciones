from __future__ import annotations

from decimal import Decimal, InvalidOperation
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk


class LiquidationSplitMasterDialog(tk.Toplevel):
    """Editor orientado al usuario para las reglas de división."""

    TYPE_LABELS = {
        "PERCENTAGE": "Por porcentaje",
        "PERCENTAGE_WITH_RESIDUAL": "Por porcentaje (ajuste automático del resto)",
        "EQUAL_PARTS": "A partes iguales",
        "WEIGHTS": "Por ponderaciones",
    }

    def __init__(self, parent: tk.Misc, repository, member_name_lookup=None, on_saved=None):
        super().__init__(parent)
        self.repository = repository
        self.member_name_lookup = member_name_lookup or (lambda _member_id: None)
        self.on_saved = on_saved
        self._editing_id = None
        self._previous_selection = None
        self._lookup_job = None
        self._mode = "view"
        self.recipients = []

        self.title("Maestro de división de liquidaciones")
        self.geometry("1180x690")
        self.transient(parent)
        self.grab_set()
        self.vars = {name: tk.StringVar() for name in (
            "id", "source_member_id", "source_member_name", "campaign", "crop",
            "variety", "remittance_id", "effective_from", "effective_to", "priority", "notes",
        )}
        self.kind_label = tk.StringVar(value=self.TYPE_LABELS["PERCENTAGE"])
        self.active = tk.BooleanVar(value=True)
        self.member_status = tk.StringVar()
        self._build()
        self._reload()
        self._set_mode("view")

    def _build(self):
        ttk.Style(self).configure("Invalid.TEntry", fieldbackground="#ffd9d9", bordercolor="#b00020")
        form = ttk.LabelFrame(self, text="Regla")
        form.pack(fill="x", padx=8, pady=6)
        fields = (("Id", "id"), ("Socio origen", "source_member_id"), ("Nombre", "source_member_name"),
                  ("Campaña", "campaign"), ("Cultivo", "crop"), ("Variedad", "variety"),
                  ("Remesa", "remittance_id"), ("Vigente desde", "effective_from"),
                  ("Vigente hasta", "effective_to"), ("Prioridad", "priority"), ("Observaciones", "notes"))
        self.entries = {}
        for i, (label, key) in enumerate(fields):
            row, column = divmod(i, 6)
            ttk.Label(form, text=label).grid(row=row * 2, column=column, sticky="w", padx=3)
            entry = ttk.Entry(form, textvariable=self.vars[key], width=19)
            entry.grid(row=row * 2 + 1, column=column, padx=3, pady=(0, 5), sticky="ew")
            self.entries[key] = entry
        self.entries["id"].configure(state="readonly")
        self.entries["source_member_name"].configure(state="readonly")
        source = self.entries["source_member_id"]
        source.bind("<Return>", self._lookup_source)
        source.bind("<FocusOut>", self._lookup_source)
        source.bind("<KeyRelease>", self._schedule_source_lookup)
        ttk.Label(form, textvariable=self.member_status, foreground="#b00020").grid(row=4, column=0, columnspan=4, sticky="w", padx=3)
        ttk.Label(form, text="Tipo de reparto").grid(row=4, column=4, sticky="w")
        self.kind_combo = ttk.Combobox(form, textvariable=self.kind_label,
                                       values=tuple(self.TYPE_LABELS.values()), state="readonly", width=43)
        self.kind_combo.grid(row=5, column=4, sticky="w")
        self.active_check = ttk.Checkbutton(form, text="Activa", variable=self.active)
        self.active_check.grid(row=5, column=5, sticky="w")

        panes = ttk.Panedwindow(self, orient="horizontal")
        panes.pack(fill="both", expand=True, padx=8)
        self.rules = ttk.Treeview(panes, columns=("id", "source", "name", "type", "filters", "priority", "active"), show="headings")
        self.targets = ttk.Treeview(panes, columns=("member", "name", "value", "residual", "order"), show="headings")
        rule_headers = {"id": "Id", "source": "Socio origen", "name": "Nombre", "type": "Tipo de reparto",
                        "filters": "Filtros", "priority": "Prioridad", "active": "Activa"}
        target_headers = {"member": "Socio", "name": "Nombre", "value": "Valor", "residual": "Residual", "order": "Orden"}
        for tree, headers in ((self.rules, rule_headers), (self.targets, target_headers)):
            for column, label in headers.items():
                tree.heading(column, text=label)
                tree.column(column, width=125)
            panes.add(tree, weight=1)
        self.rules.bind("<<TreeviewSelect>>", self._select_rule)

        bar = ttk.Frame(self)
        bar.pack(fill="x", padx=8, pady=8)
        actions = (("new", "Nuevo", self._new), ("edit", "Editar", self._edit),
                   ("delete", "Eliminar", self._delete), ("toggle", "Activar/desactivar", self._toggle),
                   ("add", "Añadir destinatario", self._add_recipient),
                   ("remove", "Quitar destinatario", self._remove_recipient),
                   ("save", "Guardar", self._save), ("cancel", "Cancelar", self._cancel))
        self.buttons = {}
        for key, text, command in actions:
            self.buttons[key] = ttk.Button(bar, text=text, command=command)
            self.buttons[key].pack(side="left", padx=2)
        ttk.Button(bar, text="Cerrar", command=self.destroy).pack(side="right", padx=2)

    def _split_type(self):
        return next((code for code, label in self.TYPE_LABELS.items() if label == self.kind_label.get()), "PERCENTAGE")

    def _reload(self, select_id=None):
        self.rules.delete(*self.rules.get_children())
        for rule in self.repository.list_rules():
            filters = " / ".join(str(rule[x]) for x in ("campaign", "crop", "variety", "remittance_id") if rule[x])
            self.rules.insert("", "end", iid=str(rule["id"]), values=(rule["id"], rule["source_member_id"],
                rule["source_member_name"] or "", self.TYPE_LABELS.get(rule["split_type"], rule["split_type"]),
                filters, rule["priority"], "Sí" if rule["active"] else "No"))
        if select_id is not None and self.rules.exists(str(select_id)):
            self.rules.selection_set(str(select_id)); self.rules.focus(str(select_id)); self.rules.see(str(select_id))
            self._load_rule(int(select_id))

    def _set_mode(self, mode):
        self._mode = mode
        editable = mode in {"new", "edit"}
        for key, entry in self.entries.items():
            entry.configure(state="normal" if editable and key not in {"id", "source_member_name"} else "readonly")
        self.kind_combo.configure(state="readonly" if editable else "disabled")
        self.active_check.configure(state="normal" if editable else "disabled")
        for key in ("add", "remove", "save", "cancel"):
            self.buttons[key].configure(state="normal" if editable else "disabled")
        self.buttons["edit"].configure(state="disabled" if mode == "new" or not self.rules.selection() else "normal")
        for key in ("delete", "toggle"):
            self.buttons[key].configure(state="disabled" if mode == "new" or not self.rules.selection() else "normal")

    def _clear(self):
        for variable in self.vars.values(): variable.set("")
        self.vars["priority"].set("100")
        self.kind_label.set(self.TYPE_LABELS["PERCENTAGE"])
        self.active.set(True); self.member_status.set(""); self.recipients = []; self._fill_targets()

    def _new(self):
        selected = self.rules.selection()
        self._previous_selection = selected[0] if selected else None
        self.rules.selection_remove(*selected)
        self._editing_id = None
        self._clear()
        self._set_mode("new")
        self.entries["source_member_id"].focus_set()

    def _edit(self):
        if self.rules.selection():
            self._editing_id = int(self.rules.selection()[0]); self._set_mode("edit")

    def _select_rule(self, _event=None):
        if self._mode in {"new", "edit"} or not self.rules.selection(): return
        self._load_rule(int(self.rules.selection()[0]))

    def _load_rule(self, rule_id):
        rule = self.repository.get_rule(rule_id); self._editing_id = rule["id"]
        for key in self.vars: self.vars[key].set(rule.get(key) or "")
        self.kind_label.set(self.TYPE_LABELS.get(rule["split_type"], rule["split_type"]))
        self.active.set(bool(rule["active"])); self.recipients = list(rule["recipients"]); self._fill_targets(); self._set_mode("view")

    def _cancel(self):
        restore = self._previous_selection if self._mode == "new" else (str(self._editing_id) if self._editing_id else None)
        self._clear(); self._editing_id = None; self._set_mode("view")
        if restore and self.rules.exists(restore):
            self.rules.selection_set(restore); self._load_rule(int(restore))

    def _schedule_source_lookup(self, _event=None):
        if self._lookup_job: self.after_cancel(self._lookup_job)
        self._lookup_job = self.after(450, self._lookup_source)

    def _lookup_member(self, raw_value):
        try: member_id = int(str(raw_value).strip())
        except ValueError: return None
        return self.member_name_lookup(member_id)

    def _lookup_source(self, _event=None):
        self._lookup_job = None
        raw = self.vars["source_member_id"].get().strip()
        if not raw:
            self.vars["source_member_name"].set(""); self.member_status.set(""); return False
        name = self._lookup_member(raw)
        self.vars["source_member_name"].set(name or "")
        self.member_status.set("" if name else "El socio origen no existe en DSocio.")
        self.entries["source_member_id"].configure(style="TEntry" if name else "Invalid.TEntry")
        return bool(name)

    def _fill_targets(self):
        self.targets.delete(*self.targets.get_children())
        for i, recipient in enumerate(self.recipients):
            self.targets.insert("", "end", iid=str(i), values=(recipient[0], recipient[1], recipient[2], "Sí" if recipient[3] else "No", i))

    def _add_recipient(self):
        member = simpledialog.askinteger("Destinatario", "Socio:", parent=self)
        if member is None: return
        name = self._lookup_member(member)
        if not name:
            messagebox.showerror("Destinatario", "El socio indicado no existe en DSocio.", parent=self); return
        value = simpledialog.askstring("Destinatario", f"Nombre: {name}\n\nValor:", initialvalue="0", parent=self)
        if value is None: return
        residual = messagebox.askyesno("Destinatario", "¿Es destinatario residual?", parent=self)
        self.recipients.append((member, name, value, residual)); self._fill_targets()

    def _remove_recipient(self):
        if self.targets.selection(): self.recipients.pop(int(self.targets.selection()[0])); self._fill_targets()

    def _validation_errors(self):
        errors = []
        if not self._lookup_source(): errors.append("El socio origen no existe en DSocio.")
        try:
            priority = int(self.vars["priority"].get())
            if priority < 0: raise ValueError
        except ValueError: errors.append("La prioridad debe ser un número entero igual o mayor que cero.")
        if not self.recipients: errors.append("Debe indicar al menos un destinatario.")
        ids = [recipient[0] for recipient in self.recipients]
        if len(ids) != len(set(ids)): errors.append("No puede haber socios destinatarios duplicados.")
        missing = [str(member_id) for member_id in ids if not self._lookup_member(member_id)]
        if missing: errors.append("No existen en DSocio los destinatarios: " + ", ".join(missing) + ".")
        values = []
        for member_id, _name, value, _residual in self.recipients:
            try:
                number = Decimal(str(value).replace(",", "."))
                if number < 0: raise InvalidOperation
                values.append(number)
            except (InvalidOperation, ValueError): errors.append(f"El valor del socio {member_id} no es un número válido no negativo.")
        residuals = sum(bool(recipient[3]) for recipient in self.recipients)
        if residuals > 1: errors.append("Solo puede existir un destinatario residual.")
        kind = self._split_type()
        if kind == "PERCENTAGE" and len(values) == len(self.recipients) and sum(values) != Decimal("100"):
            errors.append("En el reparto por porcentaje, la suma debe ser 100.")
        if kind == "PERCENTAGE_WITH_RESIDUAL" and len(values) == len(self.recipients):
            if sum(values) > Decimal("100"): errors.append("Los porcentajes no pueden sumar más de 100.")
            if residuals != 1: errors.append("El reparto con ajuste automático debe tener un único destinatario residual.")
        if kind == "WEIGHTS" and len(values) == len(self.recipients) and sum(values) <= 0:
            errors.append("La suma de las ponderaciones debe ser mayor que cero.")
        return errors

    def _save(self):
        errors = self._validation_errors()
        if errors:
            messagebox.showerror("Revise la regla", "Se han encontrado los siguientes errores:\n\n• " + "\n• ".join(errors), parent=self); return
        try:
            filters = {key: (value.get().strip() or None) for key, value in self.vars.items()
                       if key not in {"id", "source_member_id", "source_member_name", "priority", "notes"}}
            filters.update(source_member_name=self.vars["source_member_name"].get(),
                           priority=int(self.vars["priority"].get()), notes=self.vars["notes"].get(), active=self.active.get())
            # En modo alta se fuerza rule_id=None: nunca se reutiliza la selección anterior.
            rule_id = self._editing_id if self._mode == "edit" else None
            saved_id = self.repository.save_rule(int(self.vars["source_member_id"].get()), self._split_type(),
                                                 self.recipients, rule_id=rule_id, **filters)
            was_new = self._mode == "new"
            self._editing_id = None; self._reload(saved_id); self.on_saved and self.on_saved()
            messagebox.showinfo("Divisiones", "Regla creada correctamente." if was_new else "Regla actualizada correctamente.", parent=self)
        except Exception as exc: messagebox.showerror("Divisiones", str(exc), parent=self)

    def _delete(self):
        if self.rules.selection() and messagebox.askyesno("Eliminar", "¿Eliminar la regla?", parent=self):
            self.repository.delete_rule(int(self.rules.selection()[0])); self._clear(); self._reload(); self._set_mode("view"); self.on_saved and self.on_saved()

    def _toggle(self):
        if self._mode == "view" and self.rules.selection(): self.active.set(not self.active.get())
