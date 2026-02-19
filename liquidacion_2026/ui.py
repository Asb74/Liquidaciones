"""UI Tkinter para liquidación final KAKIS."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from queue import Empty, Queue
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from .app_service import RunOutput, build_config, configurar_logging, run
from .config import DEFAULT_BDCALIDAD, DEFAULT_DBEEPPL, DEFAULT_DBFRUTA
from .utils import parse_decimal, resolve_path

LOGGER = logging.getLogger(__name__)


class LiquidacionApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Liquidación KAKIS 2025")
        self.geometry("1200x760")

        self._worker_queue: Queue[tuple[str, object]] = Queue()
        self._run_output: RunOutput | None = None

        self._create_vars()
        self._build_ui()

    def _create_vars(self) -> None:
        self.campana_var = tk.StringVar(value="2025")
        self.empresa_var = tk.StringVar(value="1")
        self.cultivo_var = tk.StringVar(value="KAKIS")
        self.bruto_var = tk.StringVar()
        self.otros_fondos_var = tk.StringVar(value="0")
        self.ratio_ii_var = tk.StringVar(value="0.5")

        self.precio_deslinea_var = tk.StringVar(value="0")
        self.precio_desmesa_var = tk.StringVar(value="0")
        self.precio_podrido_var = tk.StringVar(value="0")

        self.anecop_path_var = tk.StringVar()
        self.db_fruta_var = tk.StringVar(value=DEFAULT_DBFRUTA)
        self.db_calidad_var = tk.StringVar(value=DEFAULT_BDCALIDAD)
        self.db_eeppl_var = tk.StringVar(value=DEFAULT_DBEEPPL)

        self.total_kg_var = tk.StringVar(value="-")
        self.destrios_var = tk.StringVar(value="-")
        self.fondo_var = tk.StringVar(value="-")
        self.neto_var = tk.StringVar(value="-")
        self.total_rel_var = tk.StringVar(value="-")
        self.coef_var = tk.StringVar(value="-")
        self.num_semanas_var = tk.StringVar(value="-")
        self.descuadre_var = tk.StringVar(value="-")

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=10)
        root.pack(fill=tk.BOTH, expand=True)

        form = ttk.LabelFrame(root, text="Parámetros")
        form.pack(fill=tk.X)
        for i in range(4):
            form.columnconfigure(i, weight=1)

        self._entry(form, "Campaña", self.campana_var, 0, 0)
        self._entry(form, "Empresa", self.empresa_var, 0, 1)
        self._entry(form, "Cultivo", self.cultivo_var, 0, 2)
        self._entry(form, "Bruto campaña", self.bruto_var, 0, 3)

        self._entry(form, "Otros fondos", self.otros_fondos_var, 2, 0)
        self._entry(form, "Ratio categoría II", self.ratio_ii_var, 2, 1)
        self._entry(form, "Precio DesLinea", self.precio_deslinea_var, 2, 2)
        self._entry(form, "Precio DesMesa", self.precio_desmesa_var, 2, 3)
        self._entry(form, "Precio Podrido", self.precio_podrido_var, 4, 0)

        self._file(form, "ANECOP Excel/CSV", self.anecop_path_var, 6)
        self._file(form, "DBfruta.sqlite", self.db_fruta_var, 7)
        self._file(form, "BdCalidad.sqlite", self.db_calidad_var, 8)
        self._file(form, "DBEEPPL.sqlite", self.db_eeppl_var, 9)

        actions = ttk.Frame(root)
        actions.pack(fill=tk.X, pady=8)
        self.run_button = ttk.Button(actions, text="Ejecutar", command=self._on_run)
        self.run_button.pack(side=tk.LEFT, padx=4)
        self.export_button = ttk.Button(actions, text="Exportar", command=self._on_export, state=tk.DISABLED)
        self.export_button.pack(side=tk.LEFT, padx=4)
        self.progress = ttk.Progressbar(actions, mode="indeterminate", length=220)
        self.progress.pack(side=tk.LEFT, padx=12)
        self.status_var = tk.StringVar(value="Listo")
        ttk.Label(actions, textvariable=self.status_var).pack(side=tk.RIGHT)

        resumen = ttk.LabelFrame(root, text="Resumen auditable")
        resumen.pack(fill=tk.X, pady=6)
        items = [
            ("Total kg comerciales", self.total_kg_var),
            ("Ingreso destríos", self.destrios_var),
            ("Fondo GG total", self.fondo_var),
            ("Neto objetivo", self.neto_var),
            ("Total relativo", self.total_rel_var),
            ("Coeficiente global", self.coef_var),
            ("Nº semanas con kilos", self.num_semanas_var),
            ("Descuadre abs", self.descuadre_var),
        ]
        for idx, (label, var) in enumerate(items):
            ttk.Label(resumen, text=label).grid(row=idx // 4, column=(idx % 4) * 2, sticky="w", padx=4, pady=3)
            ttk.Label(resumen, textvariable=var).grid(row=idx // 4, column=(idx % 4) * 2 + 1, sticky="w", padx=4)

        table = ttk.LabelFrame(root, text="Tabla semanal")
        table.pack(fill=tk.BOTH, expand=True)
        cols = ("semana", "coef_global", "ref_semana", "total_kg_comercial_sem", "precio_aaa_i", "precio_aa_i", "precio_a_i")
        self.tree = ttk.Treeview(table, columns=cols, show="headings")
        self.tree.pack(fill=tk.BOTH, expand=True)
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor=tk.CENTER, width=150)

    def _entry(self, parent: ttk.Frame, text: str, var: tk.StringVar, row: int, col: int) -> None:
        ttk.Label(parent, text=text).grid(row=row, column=col, sticky="w", padx=4)
        ttk.Entry(parent, textvariable=var).grid(row=row + 1, column=col, sticky="ew", padx=4, pady=(0, 6))

    def _file(self, parent: ttk.Frame, text: str, var: tk.StringVar, row: int) -> None:
        ttk.Label(parent, text=text).grid(row=row * 2, column=0, sticky="w", padx=4)
        ttk.Entry(parent, textvariable=var).grid(row=row * 2 + 1, column=0, columnspan=3, sticky="ew", padx=4, pady=(0, 6))
        ttk.Button(parent, text="Seleccionar", command=lambda: self._pick(var)).grid(row=row * 2 + 1, column=3, sticky="ew", padx=4)

    def _pick(self, var: tk.StringVar) -> None:
        path = filedialog.askopenfilename(filetypes=[("Excel/CSV/SQLite", "*.xlsx *.xls *.csv *.sqlite"), ("Todos", "*.*")])
        if path:
            var.set(path)

    def _build_config(self):
        return build_config(
            campana=int(self.campana_var.get()),
            empresa=int(self.empresa_var.get()),
            cultivo=self.cultivo_var.get(),
            bruto_campana=parse_decimal(self.bruto_var.get()),
            otros_fondos=parse_decimal(self.otros_fondos_var.get()),
            ratio_categoria_ii=parse_decimal(self.ratio_ii_var.get()),
            anecop_path=Path(self.anecop_path_var.get()),
            db_fruta=resolve_path(self.db_fruta_var.get(), DEFAULT_DBFRUTA),
            db_calidad=resolve_path(self.db_calidad_var.get(), DEFAULT_BDCALIDAD),
            db_eeppl=resolve_path(self.db_eeppl_var.get(), DEFAULT_DBEEPPL),
            precios_destrio={
                "deslinea": parse_decimal(self.precio_deslinea_var.get()),
                "desmesa": parse_decimal(self.precio_desmesa_var.get()),
                "podrido": parse_decimal(self.precio_podrido_var.get()),
            },
        )

    def _on_run(self) -> None:
        try:
            config = self._build_config()
            if not config.anecop_path.exists():
                raise ValueError(f"No existe archivo: {config.anecop_path}")
            for p in [config.db_paths.fruta, config.db_paths.calidad, config.db_paths.eeppl]:
                if not p.exists():
                    raise ValueError(f"No existe archivo SQLite: {p}")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Parámetros inválidos", str(exc))
            return

        self.run_button.configure(state=tk.DISABLED)
        self.progress.start(10)
        self.status_var.set("Ejecutando...")

        def worker() -> None:
            try:
                configurar_logging(config.output_dir)
                result = run(config)
                self._worker_queue.put(("ok", result))
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Error en ejecución")
                self._worker_queue.put(("error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()
        self.after(150, self._poll)

    def _poll(self) -> None:
        try:
            status, payload = self._worker_queue.get_nowait()
        except Empty:
            self.after(150, self._poll)
            return

        self.progress.stop()
        self.run_button.configure(state=tk.NORMAL)

        if status == "error":
            self.status_var.set("Error")
            messagebox.showerror("Error", str(payload))
            return

        self._run_output = payload  # type: ignore[assignment]
        self.export_button.configure(state=tk.NORMAL)
        self.status_var.set("Completado")
        self._render_result(self._run_output)

    def _render_result(self, output: RunOutput) -> None:
        m = output.resultado.resumen_metricas
        self.total_kg_var.set(str(m["total_kg_comerciales"]))
        self.destrios_var.set(str(m["ingreso_destrios_total"]))
        self.fondo_var.set(str(m["fondo_gg_total"]))
        self.neto_var.set(str(m["neto_obj"]))
        self.total_rel_var.set(str(m["total_rel"]))
        self.coef_var.set(str(m["coef"]))
        self.num_semanas_var.set(str(m["num_semanas_con_kilos"]))
        self.descuadre_var.set(str(m["descuadre"]))

        self.tree.delete(*self.tree.get_children())
        for _, row in output.resultado.resumen_df.iterrows():
            self.tree.insert("", tk.END, values=tuple(row.get(c, "") for c in self.tree["columns"]))

    def _on_export(self) -> None:
        if self._run_output is None:
            return
        src = self._run_output.files["perceco"]
        dst = filedialog.asksaveasfilename(defaultextension=".csv", initialfile=src.name)
        if not dst:
            return
        Path(dst).write_bytes(src.read_bytes())
        messagebox.showinfo("Exportación", f"Archivo exportado: {dst}")


def run_app() -> None:
    app = LiquidacionApp()
    app.mainloop()
