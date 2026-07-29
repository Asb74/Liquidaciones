from __future__ import annotations
from tkinter import ttk

def apply_styles(root) -> None:
    style = ttk.Style(root)
    try: style.theme_use("clam")
    except Exception: pass
    style.configure("Title.TLabel", font=("Segoe UI", 18, "bold"))
    style.configure("Mode.TLabel", foreground="#b45309", font=("Segoe UI", 10, "bold"))
    style.configure("Header.TFrame", background="#eef2ff")
    style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))
