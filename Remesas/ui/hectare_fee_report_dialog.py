from __future__ import annotations

from dataclasses import dataclass
import getpass
import logging
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from exporters.hectare_fee_report_excel_exporter import export_hectare_fee_report
from services.path_opener import open_path


logger = logging.getLogger(__name__)
REPORT_TITLE = "Informe de cuota por hectárea"
SELECT_COMPANY = "Seleccione empresa"


@dataclass(frozen=True)
class CompanyOption:
    """Visible company label plus the technical value used by repositories."""
    company_id: str
    code: str
    name: str = ""

    @property
    def display_text(self) -> str:
        return f"{self.code} - {self.name}" if self.name else self.code


class HectareFeeReportDialog(tk.Toplevel):
    def __init__(self, parent, service, campaigns, company_provider, default_campaign="", default_company=""):
        super().__init__(parent)
        self.service = service
        self.company_provider = company_provider
        self.title(REPORT_TITLE)
        self.geometry("1350x600")
        self.data = None
        self._last_query_context = None
        self._export_in_progress = False
        self.company_options: dict[str, CompanyOption] = {}

        box = ttk.Frame(self, padding=8)
        box.pack(fill="x")
        self.campaign = tk.StringVar(value=default_campaign if default_campaign in campaigns else (campaigns[0] if campaigns else ""))
        self.company = tk.StringVar()
        ttk.Label(box, text="Campaña").pack(side="left", padx=(0, 4))
        self.campaign_combo = ttk.Combobox(box, textvariable=self.campaign, values=campaigns, state="readonly", width=12)
        self.campaign_combo.pack(side="left", padx=(0, 12))
        ttk.Label(box, text="Empresa").pack(side="left", padx=(0, 4))
        self.company_combo = ttk.Combobox(box, textvariable=self.company, state="readonly", width=32)
        self.company_combo.pack(side="left", padx=(0, 12))
        self.campaign_combo.bind("<<ComboboxSelected>>", self._campaign_changed)
        self.company_combo.bind("<<ComboboxSelected>>", self._selection_changed)
        ttk.Button(box, text="Consultar", command=self.refresh).pack(side="left")
        self.export_button = ttk.Button(box, text="Exportar Excel", command=self.export, state="disabled")
        self.export_button.pack(side="left", padx=5)
        self.active_crops_text = tk.StringVar(value="Cultivos activos: (pendiente de consulta)")
        ttk.Label(box, textvariable=self.active_crops_text).pack(side="left", padx=(12, 0))

        cols = ("Socio", "Agricultor", "Boleta", "Superficie", "Cuota Ha", "Entregas", "Cultivos", "Índice €/kg", "Precio/ha", "Cuota aplicada", "Cuota pendiente", "Estado")
        self.tree = ttk.Treeview(self, columns=cols, show="headings")
        self.tree.pack(fill="both", expand=True, padx=8, pady=8)
        for column in cols:
            self.tree.heading(column, text=column)
            self.tree.column(column, width=110, anchor="w")
        self._load_companies(default_company)

    def _load_companies(self, preferred_code=""):
        self.company_options = {}
        self.company.set(SELECT_COMPANY)
        try:
            raw_options = self.company_provider(self.campaign.get())
            options = tuple(self._as_company_option(item) for item in raw_options)
        except Exception:
            logger.exception("[CuotaHaInforme] operation=load_companies campaign=%s", self.campaign.get())
            self.company_combo["values"] = (SELECT_COMPANY,)
            messagebox.showerror(REPORT_TITLE, "No se pudieron cargar las empresas.", parent=self)
            return
        self.company_options = {option.display_text: option for option in options}
        self.company_combo["values"] = (SELECT_COMPANY, *self.company_options)
        preferred = next((text for text, option in self.company_options.items() if option.code == str(preferred_code)), None)
        if preferred:
            self.company.set(preferred)
        elif len(options) == 1:
            self.company.set(options[0].display_text)

    @staticmethod
    def _as_company_option(item) -> CompanyOption:
        if isinstance(item, CompanyOption):
            return item
        code = str(item).strip()
        return CompanyOption(company_id=code, code=code)

    def _campaign_changed(self, _event=None):
        self._clear_report()
        self._load_companies()

    def _selection_changed(self, _event=None):
        self._clear_report()

    def _clear_report(self):
        self.data = None
        self._last_query_context = None
        self.tree.delete(*self.tree.get_children())
        self.active_crops_text.set("Cultivos activos: (pendiente de consulta)")
        self.export_button.configure(state="disabled")

    def _selected_company(self) -> CompanyOption | None:
        selected = self.company.get()
        option = getattr(self, "company_options", {}).get(selected)
        # The fallback is also useful for dialogs restored by older callers
        # that supplied technical company codes directly.
        return option or (CompanyOption(selected, selected) if selected and selected != SELECT_COMPANY else None)

    def refresh(self):
        option = self._selected_company()
        if option is None:
            self._clear_report()
            messagebox.showwarning(REPORT_TITLE, "Debe seleccionar una empresa antes de consultar.", parent=self)
            return
        try:
            campaign = self.campaign.get()
            logger.info("[CuotaHaInforme] campaign=%s company_id=%s company_code=%s operation=query", campaign, option.company_id, option.code)
            self.data = self.service.build_report(campaign, option.code)
            self._last_query_context = (campaign, option)
            crops = getattr(self.service, "last_active_fee_crops", ())
            self.active_crops_text.set("Cultivos activos: " + (", ".join(crops) if crops else "sin entregas en el período"))
            self.tree.delete(*self.tree.get_children())
            for summary in self.data[0]:
                self.tree.insert("", "end", values=(summary.member_id, summary.member_name, summary.boleta, summary.surface_hectares, summary.annual_fee, summary.total_delivery_kg, " / ".join(summary.delivery_crops) or "Sin entregas", summary.rate_per_kg or "No calculable", summary.price_per_hectare, summary.applied_fee, summary.pending_fee, summary.status))
            self.export_button.configure(state="normal")
        except Exception as exc:
            self._clear_report()
            messagebox.showerror(REPORT_TITLE, str(exc), parent=self)

    def export(self):
        """Export only the exact campaign/company data currently displayed."""
        if not self.data or getattr(self, "_export_in_progress", False):
            return
        context = getattr(self, "_last_query_context", None)
        if context is not None and (self.campaign.get(), self._selected_company()) != context:
            self._clear_report()
            messagebox.showwarning(REPORT_TITLE, "La campaña o empresa ha cambiado. Vuelva a consultar antes de exportar.", parent=self)
            return
        campaign, option = context if context is not None else (self.campaign.get(), self._selected_company())
        if option is None:
            return
        suggested_name = f"Informe_Cuota_Ha_{campaign}_{option.code}.xlsx"
        selected_path = filedialog.asksaveasfilename(parent=self, defaultextension=".xlsx", initialfile=suggested_name, filetypes=[("Excel", "*.xlsx")])
        if not selected_path:
            return
        self._export_in_progress = True
        self.export_button.configure(state="disabled")
        row_count = len(self.data[0]); user = getpass.getuser()
        try:
            logger.info("[CuotaHaInforme] campaign=%s company_id=%s company_code=%s operation=export", campaign, option.company_id, option.code)
            exported_path = export_hectare_fee_report(selected_path, *self.data, campaign, option.display_text)
            path = self._validated_excel_path(exported_path)
            if path is None:
                raise RuntimeError("El exportador terminó sin crear un archivo Excel válido.")
            logger.info("HECTARE_FEE_EXCEL_EXPORTED campaign=%s company=%s path=%s rows=%s user=%s", campaign, option.code, path, row_count, user)
            if not messagebox.askyesno(REPORT_TITLE, f"El informe se ha generado correctamente.\n\nRuta:\n{path}\n\n¿Desea abrirlo ahora?", parent=self):
                return
            try:
                open_path(path)
            except Exception as exc:
                logger.exception("HECTARE_FEE_EXCEL_OPEN_FAILED campaign=%s company=%s path=%s", campaign, option.code, path)
                messagebox.showwarning(REPORT_TITLE, f"El informe se generó correctamente, pero no se pudo abrir.\n\nRuta:\n{path}\n\nDetalle:\n{exc}", parent=self)
        except Exception as exc:
            logger.exception("HECTARE_FEE_EXCEL_EXPORT_FAILED campaign=%s company=%s path=%s rows=%s user=%s error=%s", campaign, option.code, selected_path, row_count, user, exc)
            messagebox.showerror(REPORT_TITLE, f"No se pudo generar el informe Excel.\n\nDetalle:\n{exc}", parent=self)
        finally:
            self._export_in_progress = False
            if self.data and (getattr(self, "_last_query_context", None) is None or getattr(self, "_last_query_context", None) == (self.campaign.get(), self._selected_company())):
                self.export_button.configure(state="normal")

    @staticmethod
    def _validated_excel_path(exported_path) -> Path | None:
        if exported_path is None:
            return None
        path = Path(exported_path).expanduser().resolve()
        return path if path.suffix.lower() == ".xlsx" and path.exists() and path.is_file() else None
