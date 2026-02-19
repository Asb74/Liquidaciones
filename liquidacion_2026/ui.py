"""Interfaz gráfica Tkinter para la liquidación 2026."""

from __future__ import annotations

import logging
import threading
from decimal import Decimal
from pathlib import Path
from queue import Empty, Queue
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from .app_service import build_config, configurar_logging, mostrar_resumen, run
from .config import LiquidacionConfig

LOGGER = logging.getLogger(__name__)


class LiquidacionApp(tk.Tk):
    """Aplicación de escritorio para ejecutar y exportar liquidaciones."""

    def __init__(self) -> None:
        super().__init__()
        self.title("Liquidación 2026")
        self.geometry("1120x700")
        self.minsize(960, 560)

        self._result_csv: Path | None = None
        self._worker_queue: Queue[tuple[str, str | Path]] = Queue()

        self._create_variables()
        self._build_ui()

    def _create_variables(self) -> None:
        self.campana_var = tk.StringVar(value="2026")
        self.empresa_var = tk.StringVar()
        self.cultivo_var = tk.StringVar(value="CAQUIS")

        self.precio_deslinea_var = tk.StringVar(value="-0.01")
        self.precio_desmesa_var = tk.StringVar(value="-0.02")
        self.precio_podrido_var = tk.StringVar(value="-0.03")

        self.precios_anecop_path_var = tk.StringVar()

        self.db_fruta_var = tk.StringVar()
        self.db_calidad_var = tk.StringVar()
        self.db_eeppl_var = tk.StringVar()

    def _build_ui(self) -> None:
        style = ttk.Style(self)
        if "clam" in style.theme_names():
            style.theme_use("clam")

        root = ttk.Frame(self, padding=12)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=1)

        form = ttk.LabelFrame(root, text="Parámetros", padding=10)
        form.grid(row=0, column=0, sticky="nsew")
        for col in range(4):
            form.columnconfigure(col, weight=1)

        self._add_entry(form, "Campaña", self.campana_var, row=0, column=0)
        self._add_entry(form, "Empresa", self.empresa_var, row=0, column=1)
        self._add_entry(form, "Cultivo", self.cultivo_var, row=0, column=2)

        self._add_entry(form, "Precio DesLinea", self.precio_deslinea_var, row=2, column=0)
        self._add_entry(form, "Precio DesMesa", self.precio_desmesa_var, row=2, column=1)
        self._add_entry(form, "Precio Podrido", self.precio_podrido_var, row=2, column=2)

        self._add_file_selector(form, "Precios ANECOP (JSON)", self.precios_anecop_path_var, row=2)
        self._add_file_selector(form, "DB fruta (.sqlite)", self.db_fruta_var, row=3)
        self._add_file_selector(form, "DB calidad (.sqlite)", self.db_calidad_var, row=4)
        self._add_file_selector(form, "DB EEPPL (.sqlite)", self.db_eeppl_var, row=5)

        actions = ttk.Frame(root, padding=(0, 10, 0, 10))
        actions.grid(row=1, column=0, sticky="ew")
        actions.columnconfigure(3, weight=1)

        self.run_button = ttk.Button(actions, text="Ejecutar Liquidación", command=self._on_run)
        self.run_button.grid(row=0, column=0, padx=(0, 8))

        self.export_button = ttk.Button(actions, text="Exportar CSV", command=self._on_export_csv, state=tk.DISABLED)
        self.export_button.grid(row=0, column=1, padx=(0, 8))

        self.progress = ttk.Progressbar(actions, mode="indeterminate", length=220)
        self.progress.grid(row=0, column=2, padx=(0, 8), sticky="w")

        self.status_var = tk.StringVar(value="Listo")
        ttk.Label(actions, textvariable=self.status_var).grid(row=0, column=3, sticky="e")

        table_frame = ttk.LabelFrame(root, text="Resumen semanal", padding=10)
        table_frame.grid(row=2, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        columns = ("semana", "ingreso_teorico", "fondo_gg", "ingreso_real", "factor")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=18)
        self.tree.grid(row=0, column=0, sticky="nsew")

        headings = {
            "semana": "Semana",
            "ingreso_teorico": "Ingreso Teórico",
            "fondo_gg": "Fondo GG",
            "ingreso_real": "Ingreso Real",
            "factor": "Factor",
        }
        widths = {"semana": 120, "ingreso_teorico": 180, "fondo_gg": 140, "ingreso_real": 170, "factor": 100}

        for column in columns:
            self.tree.heading(column, text=headings[column])
            self.tree.column(column, width=widths[column], anchor=tk.CENTER)

        yscroll = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=yscroll.set)

    def _add_entry(self, parent: ttk.Frame, label: str, variable: tk.StringVar, *, row: int, column: int) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=column, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(parent, textvariable=variable).grid(row=row + 1, column=column, sticky="ew", padx=(0, 12), pady=(0, 8))

    def _add_file_selector(self, parent: ttk.Frame, label: str, variable: tk.StringVar, *, row: int) -> None:
        ttk.Label(parent, text=label).grid(row=row * 2, column=0, columnspan=4, sticky="w", pady=(4, 0))
        entry = ttk.Entry(parent, textvariable=variable)
        entry.grid(row=row * 2 + 1, column=0, columnspan=3, sticky="ew", padx=(0, 8), pady=(0, 8))
        ttk.Button(
            parent,
            text="Seleccionar",
            command=lambda v=variable: self._select_file(v),
        ).grid(row=row * 2 + 1, column=3, sticky="ew", pady=(0, 8))

    def _select_file(self, variable: tk.StringVar) -> None:
        path = filedialog.askopenfilename(
            title="Seleccionar archivo",
            filetypes=[("Archivos", "*.json *.sqlite"), ("Todos", "*.*")],
        )
        if path:
            variable.set(path)

    def _on_run(self) -> None:
        try:
            config = self._build_config_from_form()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Error validando parámetros")
            messagebox.showerror("Parámetros inválidos", str(exc))
            return

        self._set_running_state(True)

        def worker() -> None:
            try:
                configurar_logging(config.log_file)
                output_csv = run(config)
                self._worker_queue.put(("ok", output_csv))
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Error ejecutando liquidación")
                self._worker_queue.put(("error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()
        self.after(150, self._poll_worker_queue)

    def _poll_worker_queue(self) -> None:
        try:
            status, payload = self._worker_queue.get_nowait()
        except Empty:
            self.after(150, self._poll_worker_queue)
            return

        self._set_running_state(False)

        if status == "error":
            messagebox.showerror("Error en liquidación", str(payload))
            self.status_var.set("Error")
            return

        output_csv = Path(str(payload))
        self._result_csv = output_csv
        self._refresh_summary(output_csv)
        self.export_button.configure(state=tk.NORMAL)
        self.status_var.set(f"Completado: {output_csv.name}")
        messagebox.showinfo("Liquidación completada", f"CSV generado en:\n{output_csv}")

    def _refresh_summary(self, csv_path: Path) -> None:
        summary = mostrar_resumen(csv_path)
        self.tree.delete(*self.tree.get_children())
        for _, row in summary.iterrows():
            self.tree.insert(
                "",
                tk.END,
                values=(
                    int(row["semana"]),
                    f"{float(row['ingreso_teorico']):,.2f}",
                    f"{float(row['fondo_gg']):,.2f}",
                    f"{float(row['ingreso_real']):,.2f}",
                    f"{float(row['factor']):,.4f}",
                ),
            )

    def _build_config_from_form(self) -> LiquidacionConfig:
        if not self.precios_anecop_path_var.get():
            raise ValueError("Debe seleccionar el archivo de precios ANECOP.")

        precios_file = Path(self.precios_anecop_path_var.get())
        if not precios_file.exists():
            raise ValueError(f"No existe el archivo de precios ANECOP: {precios_file}")

        for label, path in {
            "DB fruta": self.db_fruta_var.get(),
            "DB calidad": self.db_calidad_var.get(),
            "DB EEPPL": self.db_eeppl_var.get(),
        }.items():
            if not path:
                raise ValueError(f"Debe seleccionar {label}.")
            if not Path(path).exists():
                raise ValueError(f"No existe {label}: {path}")

        with precios_file.open("r", encoding="utf-8") as file:
            precios_anecop_raw = file.read()

        return build_config(
            campana=int(self.campana_var.get().strip()),
            empresa=self.empresa_var.get().strip(),
            cultivo=self.cultivo_var.get().strip(),
            db_fruta=Path(self.db_fruta_var.get().strip()),
            db_calidad=Path(self.db_calidad_var.get().strip()),
            db_eeppl=Path(self.db_eeppl_var.get().strip()),
            precios_anecop_raw=precios_anecop_raw,
            precios_destrio={
                "DesLinea": Decimal(self.precio_deslinea_var.get().strip()),
                "DesMesa": Decimal(self.precio_desmesa_var.get().strip()),
                "Podrido": Decimal(self.precio_podrido_var.get().strip()),
            },
        )

    def _set_running_state(self, running: bool) -> None:
        if running:
            self.run_button.configure(state=tk.DISABLED)
            self.progress.start(12)
            self.status_var.set("Calculando...")
            return

        self.run_button.configure(state=tk.NORMAL)
        self.progress.stop()

    def _on_export_csv(self) -> None:
        if self._result_csv is None:
            messagebox.showwarning("Sin resultado", "Ejecute la liquidación antes de exportar.")
            return

        export_path = filedialog.asksaveasfilename(
            title="Exportar CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=self._result_csv.name,
        )
        if not export_path:
            return

        destination = Path(export_path)
        try:
            destination.write_bytes(self._result_csv.read_bytes())
        except OSError as exc:
            LOGGER.exception("Error exportando CSV")
            messagebox.showerror("Error al exportar", str(exc))
            return

        messagebox.showinfo("Exportación completada", f"Archivo exportado en:\n{destination}")


def run_app() -> None:
    """Lanza la interfaz Tkinter de liquidación."""
    app = LiquidacionApp()
    app.mainloop()
