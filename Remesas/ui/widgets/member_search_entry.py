"""Reusable widgets used by history filters."""
from __future__ import annotations

import tkinter as tk
import logging
from tkinter import font as tkfont
from tkinter import ttk


logger = logging.getLogger(__name__)

class MemberSearchEntry(ttk.Frame):
    """Debounced member autocomplete whose selected id is separate from its text."""

    _MAX_POPUP_WIDTH = 800
    _VISIBLE_RESULTS = 10
    _LISTBOX_PADDING = 4

    def __init__(self, parent, search, *, width=46):
        super().__init__(parent)
        self.search = search
        self.member_search_text = tk.StringVar()
        self.selected_member_id = None
        self._after_id = None
        self._results = ()
        self._selecting = False
        self.popup = None
        self.entry = ttk.Entry(self, textvariable=self.member_search_text, width=width)
        self.entry.pack(side="left", fill="x", expand=True)
        ttk.Button(self, text="Limpiar", command=self.clear).pack(side="left", padx=(4, 0))
        self.member_search_text.trace_add("write", self._changed)
        self.entry.bind("<Down>", self._down)
        self.entry.bind("<Up>", self._up)
        self.entry.bind("<Return>", self._accept)
        self.entry.bind("<Escape>", lambda _event: self._close())
        self.entry.bind("<FocusOut>", lambda _event: self.after(120, self._close))

    def _changed(self, *_):
        if self._selecting:
            return
        self.selected_member_id = None
        if self._after_id:
            self.after_cancel(self._after_id)
            self._after_id = None
        text = self.member_search_text.get().strip()
        if len(text) < 2:
            self._close()
            return
        self._after_id = self.after(250, self._search)

    def _search(self):
        self._after_id = None
        text = self.member_search_text.get().strip()
        try:
            self._results = tuple(self.search(text))
        except Exception:
            logger.exception("[MemberSearchFailed]")
            self._results = ()
            self._close()
            return
        if not self._results:
            self._close()
            return
        if not self.popup:
            self.popup = tk.Toplevel(self)
            self.popup.wm_overrideredirect(True)
            self.popup.transient(self.winfo_toplevel())
            self.listbox = tk.Listbox(
                self.popup,
                exportselection=False,
            )
            self.scrollbar = ttk.Scrollbar(
                self.popup,
                orient="vertical",
                command=self.listbox.yview,
            )
            self.listbox.configure(yscrollcommand=self.scrollbar.set)
            self.listbox.pack(side="left", fill="both", expand=True)
            self.listbox.bind("<ButtonRelease-1>", self._accept)
            self.listbox.bind("<Return>", self._accept)
            self.listbox.bind("<Escape>", lambda _event: self._close())
        self.listbox.delete(0, "end")
        labels = tuple(
            f"{row['member_id']} — {row['name']}" for row in self._results
        )
        for label in labels:
            self.listbox.insert("end", label)
        has_scrollbar = len(self._results) > self._VISIBLE_RESULTS
        if has_scrollbar:
            self.scrollbar.pack(side="right", fill="y")
        else:
            self.scrollbar.pack_forget()
        self.listbox.configure(height=min(self._VISIBLE_RESULTS, len(self._results)))
        self.listbox.selection_set(0)
        self._position_popup(labels, has_scrollbar)
        self.popup.deiconify()

    def _position_popup(self, labels, has_scrollbar):
        """Size the popup from the rendered labels and align it with the entry."""
        self.update_idletasks()
        self.popup.update_idletasks()

        font = tkfont.Font(font=self.listbox.cget("font"))
        longest_label_width = max((font.measure(label) for label in labels), default=0)
        listbox_width = longest_label_width + (2 * self._LISTBOX_PADDING)
        listbox_width += 2 * (
            int(self.listbox.cget("borderwidth"))
            + int(self.listbox.cget("highlightthickness"))
        )
        popup_width = max(self.entry.winfo_width(), listbox_width)
        if has_scrollbar:
            popup_width += self.scrollbar.winfo_reqwidth()
        popup_width = min(popup_width, self._MAX_POPUP_WIDTH)

        self.popup.geometry(
            f"{popup_width}x{self.popup.winfo_reqheight()}"
            f"+{self.entry.winfo_rootx()}"
            f"+{self.entry.winfo_rooty() + self.entry.winfo_height()}"
        )

    def _accept(self, _event=None):
        if self.popup:
            selected = self.listbox.curselection()
            if selected:
                row = self._results[selected[0]]
                self._selecting = True
                self.member_search_text.set(f"{row['member_id']} — {row['name']}")
                self._selecting = False
                self.selected_member_id = row["member_id"]
        self._close()
        return "break"

    def _down(self, _event):
        if self.popup:
            self.listbox.focus_set()
            self.listbox.selection_clear(0, "end")
            self.listbox.selection_set(0)
        return "break"

    def _up(self, event):
        return self._down(event)

    def _close(self):
        if self.popup:
            self.popup.destroy()
            self.popup = None

    def clear(self):
        self.selected_member_id = None
        self.member_search_text.set("")
        self._close()
        self.entry.focus_set()
