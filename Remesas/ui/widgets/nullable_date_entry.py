from __future__ import annotations

from datetime import date
import tkinter as tk
from tkinter import ttk
from tkcalendar import DateEntry


class NullableDateEntry(ttk.Frame):
    """A DateEntry enabled only when its checkbox is active; inactive means no date."""
    def __init__(self, parent, label, *, initial_date=None):
        super().__init__(parent)
        self.enabled = tk.BooleanVar(value=False)
        self.date_entry = DateEntry(self, date_pattern="dd/mm/yyyy", state="disabled",
                                    width=11, year=(initial_date or date.today()).year)
        ttk.Checkbutton(self, text=label, variable=self.enabled, command=self._toggle).pack(side="left")
        self.date_entry.pack(side="left", padx=(3, 0))

    def _toggle(self):
        self.date_entry.configure(state="normal" if self.enabled.get() else "disabled")

    def iso_value(self):
        return self.date_entry.get_date().isoformat() if self.enabled.get() else None

    def clear(self):
        self.enabled.set(False)
        self._toggle()
