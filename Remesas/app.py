from __future__ import annotations
import logging
import sys
import tkinter as tk
from tkinter import messagebox, ttk
from ui.remesas_frame import RemesasFrame
from ui.main_menu import MainMenuHandlers, build_main_menu
from ui.styles import apply_styles
from ui.calibre_master_dialog import CalibreMasterDialog
from ui.production_destination_master_dialog import ProductionDestinationMasterDialog
from data.db_connection import load_config, setup_logging
from data.persistence.database import PersistenceDatabase
from data.persistence.liquidation_repository import LiquidationRepository
from services.pdf_merge_service import PdfMergeService
from ui.pdf_merge_tool_dialog import PdfMergeToolDialog
from ui.hectare_fee_report_dialog import HectareFeeReportDialog
from data.hectare_repository import HectareRepository
from services.hectare_fee_report_service import HectareFeeReportService
from services.persisted_variety_benchmark_service import PersistedVarietyBenchmarkService
from services.individual_pdf_refresh_service import IndividualPdfRefreshService
from data.postgres_repository import PostgresRepository

logger = logging.getLogger(__name__)


def _prepare_databases(root: tk.Tk, config) -> bool:
    try:
        PersistenceDatabase(settings=config.postgresql_settings).initialize()
        return True
    except Exception as exc:
        logger.exception("PostgreSQL no está disponible")
        messagebox.showerror("PostgreSQL no disponible", f"No se puede iniciar la aplicación porque PostgreSQL no está disponible.\n\nDetalle:\n{exc}\n\nNo se ha guardado ningún dato parcial.")
        return False


def main() -> None:
    config=load_config(); setup_logging(config)
    if config.persistence_enabled:
        try:
            PersistenceDatabase(settings=config.postgresql_settings).initialize()
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
    repository=LiquidationRepository(PersistenceDatabase(settings=config.postgresql_settings))
    from services.member_surface_service import MemberSurfaceService
    surface_service = MemberSurfaceService(HectareRepository(frame.conn)) if frame.conn else None
    refresh_service=IndividualPdfRefreshService(repository,PersistedVarietyBenchmarkService(repository, surface_service=surface_service))
    def open_fee_report():
        if not frame.conn: return messagebox.showwarning("Informe de cuota por hectárea", "Conecte primero las bases de datos.")
        meta=frame.meta; HectareFeeReportDialog(root, HectareFeeReportService(HectareRepository(frame.conn)), meta.campaigns(), meta.empresas, frame.context_panel.campana.get(), frame.context_panel.empresa.get())
    def open_mass_documents():
        PdfMergeToolDialog(
            root, PdfMergeService(repository),
            remittance_resolver=frame.resolve_document_remittance,
            excel_callback=lambda selected: frame._process_selected_remittances(selected, excel_only=True),
            cancel_excel_callback=lambda: setattr(frame, "batch_cancel_requested", True),
            csv_export_service=getattr(frame, "csv_export_service", None),
            individual_refresh_service=refresh_service,
        )
    root.config(menu=build_main_menu(root, MainMenuHandlers(close=frame.close_application, open_hectare_fee_master=frame.open_hectare_fee_master, open_calibre_master=lambda: CalibreMasterDialog(root), open_production_destination_master=lambda: ProductionDestinationMasterDialog(root), open_liquidation_prefix_master=frame.open_liquidation_prefix_master, open_liquidation_split_master=frame.open_liquidation_split_master, show_about=frame.show_about, refresh_local_databases=lambda: frame.synchronize_local_databases(manual=True), open_data_folder=frame.open_data_folder, open_liquidation_history=frame.open_liquidation_history, open_pdf_merge_tool=open_mass_documents, open_hectare_fee_report=open_fee_report)))
    try:
        root.mainloop()
    finally:
        PostgresRepository.shutdown_pool()
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "db":
        from db_tools.__main__ import main as db_main
        db_main()
    else:
        main()
