from __future__ import annotations
import logging
import tkinter as tk
from tkinter import messagebox, ttk
from ui.remesas_frame import RemesasFrame
from ui.main_menu import MainMenuHandlers, build_main_menu
from ui.styles import apply_styles
from ui.calibre_master_dialog import CalibreMasterDialog
from ui.production_destination_master_dialog import ProductionDestinationMasterDialog
from data.db_connection import load_config, setup_logging
from services.local_database_sync_service import LocalDatabaseSyncService
from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
from services.pdf_merge_service import PdfMergeService
from ui.pdf_merge_tool_dialog import PdfMergeToolDialog

logger = logging.getLogger(__name__)


def _prepare_databases(root: tk.Tk, config) -> bool:
    if not config.sync_on_start:
        return True
    win = tk.Toplevel(root)
    win.title("Preparando bases de datos")
    win.resizable(False, False)
    ttk.Label(win, text="Preparando bases de datos...").pack(padx=18, pady=(14, 4), anchor="w")
    status = tk.StringVar(value="Comprobando DBfruta.")
    ttk.Label(win, textvariable=status, width=58).pack(padx=18, pady=(0, 14), anchor="w")
    win.update_idletasks()

    def progress(message: str) -> None:
        status.set(message)
        win.update_idletasks()

    try:
        results = LocalDatabaseSyncService(config, progress_callback=progress).synchronize_all()
        errors = [r for r in results if not (r.synchronized or r.used_local_fallback)]
        if errors:
            detail = "\n".join(f"{r.database_name}: {r.error_message}" for r in errors)
            messagebox.showerror("Bases de datos", f"No se han podido preparar las bases de datos.\n\nDetalle:\n{detail}\n\nRevise la conexión de red o utilice la última copia local disponible.")
            return False
        fallback = [r for r in results if r.used_local_fallback]
        if fallback:
            lines = ["No se ha podido acceder a las bases de red.", "", "La aplicación utilizará la última copia local válida:"]
            for r in fallback:
                stamp = r.local_modified_at.strftime("%d/%m/%Y %H:%M") if r.local_modified_at else "fecha desconocida"
                lines.append(f"{r.database_name}: {stamp}")
            lines += ["", "Los datos pueden no estar actualizados."]
            messagebox.showwarning("Bases locales", "\n".join(lines))
        progress("Iniciando aplicación.")
        return True
    except Exception as exc:
        logger.exception("No se han podido preparar las bases de datos")
        messagebox.showerror("Bases de datos", f"No se han podido preparar las bases de datos.\n\nDetalle:\n{exc}\n\nRevise la conexión de red o utilice la última copia local disponible.")
        return False
    finally:
        win.destroy()


def main() -> None:
    config=load_config(); setup_logging(config)
    if config.persistence_enabled:
        try:
            PersistenceDatabase(config.persistence_database_path).initialize()
        except Exception:
            logger.exception("Persistencia local deshabilitada: falló su inicialización")
            object.__setattr__(config, "persistence_enabled", False)
    root=tk.Tk(); root.withdraw(); root.title(config.app_name); root.geometry(f"{config.window_width}x{config.window_height}")
    apply_styles(root)
    if not _prepare_databases(root, config):
        root.destroy(); return
    root.deiconify()
    frame=RemesasFrame(root); frame.pack(fill="both", expand=True)
    root.protocol("WM_DELETE_WINDOW", frame.close_application)
    repository=LiquidationRepository(PersistenceDatabase(config.persistence_database_path))
    root.config(menu=build_main_menu(root, MainMenuHandlers(close=frame.close_application, open_hectare_fee_master=frame.open_hectare_fee_master, open_calibre_master=lambda: CalibreMasterDialog(root), open_production_destination_master=lambda: ProductionDestinationMasterDialog(root), open_liquidation_prefix_master=frame.open_liquidation_prefix_master, open_liquidation_split_master=frame.open_liquidation_split_master, show_about=frame.show_about, refresh_local_databases=lambda: frame.synchronize_local_databases(manual=True), open_data_folder=frame.open_data_folder, open_liquidation_history=frame.open_liquidation_history, open_pdf_merge_tool=lambda: PdfMergeToolDialog(root,PdfMergeService(repository)))))
    root.mainloop()
if __name__ == "__main__": main()
