from __future__ import annotations
import tkinter as tk
from ui.remesas_frame import RemesasFrame
from ui.main_menu import MainMenuHandlers, build_main_menu
from ui.styles import apply_styles
from data.db_connection import load_config

def main() -> None:
    config=load_config()
    root=tk.Tk(); root.title(config.app_name); root.geometry(f"{config.window_width}x{config.window_height}")
    apply_styles(root); frame=RemesasFrame(root); frame.pack(fill="both", expand=True); root.config(menu=build_main_menu(root, MainMenuHandlers(close=root.destroy, open_hectare_fee_master=frame.open_hectare_fee_master, show_about=frame.show_about))); root.mainloop()
if __name__ == "__main__": main()
