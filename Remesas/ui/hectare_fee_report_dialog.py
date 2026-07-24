from __future__ import annotations

import getpass
import logging
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from exporters.hectare_fee_report_excel_exporter import export_hectare_fee_report
from services.path_opener import open_path


logger = logging.getLogger(__name__)
REPORT_TITLE = "Informe de cuota por hectárea"


class HectareFeeReportDialog(tk.Toplevel):
    def __init__(self, parent, service, campaigns, companies):
        super().__init__(parent)
        self.service = service
        self.title(REPORT_TITLE)
        self.geometry("1350x600")
        self.data = None
        self._export_in_progress = False

        box = ttk.Frame(self, padding=8)
        box.pack(fill="x")
        self.campaign = tk.StringVar(value=campaigns[0] if campaigns else "")
        self.company = tk.StringVar(value=companies[0] if companies else "")
        for label, var, values in (("Campaña", self.campaign, campaigns), ("Empresa", self.company, companies)):
            ttk.Label(box, text=label).pack(side="left", padx=(0, 4))
            ttk.Combobox(box, textvariable=var, values=values, state="readonly", width=12).pack(side="left", padx=(0, 12))
        ttk.Button(box, text="Consultar", command=self.refresh).pack(side="left")
        self.export_button = ttk.Button(box, text="Exportar Excel", command=self.export)
        self.export_button.pack(side="left", padx=5)

        cols = ("Socio", "Agricultor", "Boleta", "Superficie", "Cuota Ha", "Entregas", "Cultivos", "Índice €/kg", "Precio/ha", "Cuota aplicada", "Cuota pendiente", "Estado")
        self.tree = ttk.Treeview(self, columns=cols, show="headings")
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)
        for column in cols:
            self.tree.heading(column, text=column)
            self.tree.column(column, width=110, anchor="w")

    def refresh(self):
        try:
            self.data = self.service.build_report(self.campaign.get(), self.company.get())
            self.tree.delete(*self.tree.get_children())
            for summary in self.data[0]:
                self.tree.insert("", "end", values=(summary.member_id, summary.member_name, summary.boleta, summary.surface_hectares, summary.annual_fee, summary.total_delivery_kg, " / ".join(summary.delivery_crops) or "Sin entregas", summary.rate_per_kg or "No calculable", summary.price_per_hectare, summary.applied_fee, summary.pending_fee, summary.status))
        except Exception as exc:
            messagebox.showerror(REPORT_TITLE, str(exc), parent=self)

    def export(self):
        """Export the report and optionally open the verified generated workbook."""
        if not self.data or getattr(self, "_export_in_progress", False):
            return

        selected_path = filedialog.asksaveasfilename(
            parent=self,
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
        )
        if not selected_path:
            return

        self._export_in_progress = True
        self.export_button.configure(state="disabled")
        campaign = self.campaign.get()
        company = self.company.get()
        row_count = len(self.data[0])
        user = getpass.getuser()
        try:
            exported_path = export_hectare_fee_report(selected_path, *self.data)
            path = self._validated_excel_path(exported_path)
            if path is None:
                raise RuntimeError("El exportador terminó sin crear un archivo Excel válido.")

            logger.info(
                "HECTARE_FEE_EXCEL_EXPORTED campaign=%s company=%s path=%s rows=%s user=%s",
                campaign, company, path, row_count, user,
            )
            should_open = messagebox.askyesno(
                REPORT_TITLE,
                "El informe se ha generado correctamente.\n\n"
                f"Ruta:\n{path}\n\n"
                "¿Desea abrirlo ahora?",
                parent=self,
            )
            if not should_open:
                return

            logger.info(
                "HECTARE_FEE_EXCEL_OPEN_REQUESTED campaign=%s company=%s path=%s rows=%s user=%s",
                campaign, company, path, row_count, user,
            )
            try:
                open_path(path)
                logger.info(
                    "HECTARE_FEE_EXCEL_OPENED campaign=%s company=%s path=%s rows=%s user=%s",
                    campaign, company, path, row_count, user,
                )
            except Exception as exc:
                logger.exception(
                    "HECTARE_FEE_EXCEL_OPEN_FAILED campaign=%s company=%s path=%s rows=%s user=%s error=%s",
                    campaign, company, path, row_count, user, exc,
                )
                messagebox.showwarning(
                    REPORT_TITLE,
                    "El informe se generó correctamente, pero no se pudo abrir.\n\n"
                    f"Ruta:\n{path}\n\n"
                    f"Detalle:\n{exc}",
                    parent=self,
                )
        except Exception as exc:
            logger.exception(
                "HECTARE_FEE_EXCEL_EXPORT_FAILED campaign=%s company=%s path=%s rows=%s user=%s error=%s",
                campaign, company, selected_path, row_count, user, exc,
            )
            messagebox.showerror(
                REPORT_TITLE,
                "No se pudo generar el informe Excel.\n\n"
                f"Detalle:\n{exc}",
                parent=self,
            )
        finally:
            self._export_in_progress = False
            self.export_button.configure(state="normal")

    @staticmethod
    def _validated_excel_path(exported_path) -> Path | None:
        if exported_path is None:
            return None
        path = Path(exported_path).expanduser().resolve()
        if path.suffix.lower() != ".xlsx" or not path.exists() or not path.is_file():
            return None
        return path
