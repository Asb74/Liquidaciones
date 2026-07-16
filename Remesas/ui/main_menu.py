from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import tkinter as tk


@dataclass(frozen=True)
class MainMenuHandlers:
    close: Callable[[], None]
    open_hectare_fee_master: Callable[[], None]
    show_about: Callable[[], None]
    open_calibre_master: Callable[[], None] = lambda: None
    open_production_destination_master: Callable[[], None] = lambda: None
    open_liquidation_prefix_master: Callable[[], None] = lambda: None
    open_liquidation_split_master: Callable[[], None] = lambda: None
    refresh_local_databases: Callable[[], None] = lambda: None
    open_data_folder: Callable[[], None] = lambda: None


def build_main_menu(root: tk.Misc, handlers: MainMenuHandlers) -> tk.Menu:
    menu_bar = tk.Menu(root)

    file_menu = tk.Menu(menu_bar, tearoff=False)
    file_menu.add_command(label="Actualizar bases locales", command=handlers.refresh_local_databases)
    file_menu.add_command(label="Abrir carpeta de datos", command=handlers.open_data_folder)
    file_menu.add_separator()
    file_menu.add_command(label="Cerrar", command=handlers.close)
    menu_bar.add_cascade(label="Archivo", menu=file_menu)

    masters_menu = tk.Menu(menu_bar, tearoff=False)
    masters_menu.add_command(label="Cuota por hectárea", command=handlers.open_hectare_fee_master)
    masters_menu.add_command(label="Calibres y categorías", command=handlers.open_calibre_master)
    masters_menu.add_command(label="Destinos de producción", command=handlers.open_production_destination_master)
    masters_menu.add_command(label="Prefijos de liquidación", command=handlers.open_liquidation_prefix_master)
    masters_menu.add_command(label="División de liquidaciones", command=handlers.open_liquidation_split_master)
    menu_bar.add_cascade(label="Maestros", menu=masters_menu)

    help_menu = tk.Menu(menu_bar, tearoff=False)
    help_menu.add_command(label="Acerca de", command=handlers.show_about)
    menu_bar.add_cascade(label="Ayuda", menu=help_menu)

    return menu_bar
