from __future__ import annotations

import logging
from contextvars import ContextVar
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from domain.calculation_models import CalculationStatus, LiquidationResult, MemberLiquidation
from domain.models import Delivery, Remesa, AppConfig
from domain.hectare_fee_master import HectareFeeMaster

_current_audit: ContextVar["AuditLogger | None"] = ContextVar("current_liquidation_audit", default=None)
_last_audit_path: Path | None = None


def current_audit() -> "AuditLogger | None":
    audit = _current_audit.get()
    return audit if audit and audit.enabled else None


class AuditSession:
    def __init__(self, audit: "AuditLogger | None") -> None:
        self.audit = audit
        self._token = None

    def __enter__(self):
        if self.audit and self.audit.enabled:
            self._token = _current_audit.set(self.audit)
            self.audit.start()
        return self.audit

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.audit and self.audit.enabled:
            if exc:
                self.audit.section("ERROR")
                self.audit.line(f"{type(exc).__name__}: {exc}")
            self.audit.close()
        if self._token is not None:
            _current_audit.reset(self._token)


class AuditLogger:
    def __init__(self, config: AppConfig, remesa: Remesa | None, deliveries: list[Delivery], hectare_master: HectareFeeMaster | None = None) -> None:
        self.enabled = bool(getattr(config, "audit_enabled", False))
        self.config = config
        self.remesa = remesa
        self.deliveries = deliveries
        self.hectare_master = hectare_master
        self.path: Path | None = None
        self._fh = None
        self._started = False
        self.summary = {
            "processed": set(), "with_surface": set(), "without_surface": set(),
            "with_kg": set(), "without_kg": set(), "fee_calculated": set(),
            "fee_zero": set(), "errors": set(),
        }

    @classmethod
    def for_calculation(cls, config: AppConfig, remesa: Remesa | None, deliveries: list[Delivery], hectare_master: HectareFeeMaster | None = None) -> "AuditSession":
        global _last_audit_path
        if not getattr(config, "audit_enabled", False):
            _last_audit_path = None
            return AuditSession(None)
        return AuditSession(cls(config, remesa, deliveries, hectare_master))

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        logs_dir = Path(getattr(self.config, "audit_dir", "logs"))
        logs_dir.mkdir(parents=True, exist_ok=True)
        self.path = logs_dir / f"auditoria_liquidacion_{datetime.now():%Y%m%d_%H%M%S}.log"
        self._fh = self.path.open("w", encoding="utf-8")
        self.header()

    def close(self) -> None:
        global _last_audit_path
        _last_audit_path = self.path
        if self._fh:
            self._fh.flush(); self._fh.close(); self._fh = None

    def line(self, text: str = "") -> None:
        if not self.enabled or not self._fh:
            return
        self._fh.write(str(text) + "\n")

    def console(self, text: str) -> None:
        if self.enabled:
            print(text)
            logging.getLogger(__name__).info(text)

    def section(self, title: str, char: str = "=") -> None:
        self.line(char * 50); self.line(title); self.line(char * 50)

    def subsection(self, title: str) -> None:
        self.line("-" * 50); self.line(title); self.line("-" * 50)

    def header(self) -> None:
        values = self.remesa.values if self.remesa else {}
        self.section("AUDITORÍA DE LIQUIDACIÓN")
        self.line(f"Fecha y hora: {datetime.now().isoformat(sep=' ', timespec='seconds')}")
        self.line(f"Versión de la aplicación: {getattr(self.config, 'app_name', '')} / mode={getattr(self.config, 'mode', '')}")
        self.line(f"Campaña: {values.get('CAMPAÑA', '')}")
        self.line(f"Empresa: {values.get('EMPRESA', '')}")
        self.line(f"Cultivo: {values.get('CULTIVO', '')}")
        self.line(f"Remesa: {values.get('REMESA', '')} ({values.get('IdREMESA', '')})")
        self.line(f"Periodo desde: {values.get('PERIODO1', '')}")
        self.line(f"Periodo hasta: {values.get('PERIODO2', '')}")
        self.line(f"Tipo liquidación: {values.get('TipoLiq', '')}")
        self.line(f"Condición: {values.get('CATEGORIA', '')}")
        self.line(f"Variedades seleccionadas: {values.get('VARIEDAD', '')}")
        self.line("Opciones activas:")
        for label, key in (("Recolección", "AplRec"), ("Transporte", "AplTte"), ("Calidad", "AplCal"), ("GlobalGAP", "AplGlobal"), ("Cuota Ha", "AplCHa"), ("Precalibrado", "AplPrecalibrado")):
            self.line(f"- {label}: {values.get(key, '')}")
        if self.hectare_master:
            self.section("MAESTRO CUOTA HA", "-")
            self.line(f"Ruta: {self.hectare_master.path}")
            self.line(f"Versión: {self.hectare_master.version}")
            self.line(f"Huella: {self.hectare_master.fingerprint}")
            self.line(f"Cargado: {self.hectare_master.loaded_at}")
            self.line(f"Precio por ha: {self.hectare_master.price_per_hectare}")
            self.line(f"Cultivos superficie: {', '.join(self.hectare_master.surface_crops)}")
            self.line(f"Cultivos entrega: {', '.join(self.hectare_master.delivery_crops)}")

    def audit_deliveries(self) -> None:
        self.section("AUDITORÍA DE ENTREGAS")
        self.line(f"Número total de registros leídos: {len(self.deliveries)}")
        self.line(f"Número total de socios: {len({d.socio for d in self.deliveries})}")
        self.line(f"Número total de variedades: {len({d.variedad for d in self.deliveries if d.variedad})}")
        self.line(f"Kilos brutos: {sum((d.neto for d in self.deliveries), Decimal('0'))}")
        self.line(f"Kilos efectivos: {sum((d.effective_net_kg for d in self.deliveries), Decimal('0'))}")
        for d in self.deliveries:
            self.line(f"Registro={d.registro} Socio={d.socio} Variedad={d.variedad} Cultivo={(self.remesa.values.get('CULTIVO','') if self.remesa else '')} Neto={d.neto} NetoPartida={d.batch_net_kg} NetoEfectivo={d.effective_net_kg} Liquidado={d.liquidado}")

    def audit_sql(self, title: str, sql: str, params: Iterable[Any], rows: int, elapsed_ms: float) -> None:
        self.subsection(f"SQL - {title}")
        self.line("SQL:"); self.line(sql.strip())
        self.line("Parámetros:"); self.line(", ".join(str(p) for p in params))
        self.line(f"Filas={rows}"); self.line(f"Tiempo={elapsed_ms:.0f} ms")

    def audit_filters(self, title: str, counts: dict[str, Any]) -> None:
        self.subsection(f"FILTROS - {title}")
        for k, v in counts.items(): self.line(f"{k}: {v}")

    def audit_member_start(self, member: MemberLiquidation) -> None:
        self.section(f"SOCIO {member.member_id}")
        self.line(f"Nombre: {member.member_name}"); self.line(f"Variedad: {member.variety}")
        self.summary["processed"].add(member.member_id)

    def audit_model(self, member: MemberLiquidation) -> None:
        self.subsection("MODELO - MemberLiquidation")
        for name in ("member_id","gross_amount","collection_amount","transport_amount","quality_amount","globalgap_amount","hectare_fee_amount","hectare_fee_status","warnings"):
            self.line(f"{name}: {getattr(member, name)}")
        audit = getattr(member, "hectare_fee_audit", None)
        if audit:
            self.line(f"Hectáreas: {audit.applicable_hectares}")
            self.line(f"Cuota teórica: {audit.total_theoretical_fee}")
            self.line(f"Kilos totales: {audit.total_effective_kg}")
            self.line(f"Índice: {audit.rate_per_kg}")
            self.line(f"Neto línea: {audit.line_effective_kg}")
            self.line(f"Cuota parcial: {audit.line_fee}")
            self.line(f"Valor almacenado: {member.hectare_fee_amount}")
            self.line(f"Alineación audit.line_fee == member.hectare_fee_amount: {audit.line_fee == member.hectare_fee_amount}")
            if audit.line_fee != member.hectare_fee_amount:
                self.line("ERROR DE ALINEACIÓN")
        self.line(f"¿hectare_fee_amount llega con valor?: {member.hectare_fee_amount is not None}")

    def audit_result(self, result: LiquidationResult) -> None:
        self.section("AUDITORÍA DEL RESULTADO GLOBAL")
        t = result.totals
        for label, attr in (("gross","gross_amount"),("collection","collection_amount"),("transport","transport_amount"),("quality","quality_amount"),("globalgap","globalgap_amount"),("hectare_fee","hectare_fee_amount"),("base","taxable_base"),("iva","vat_amount"),("retention","withholding_amount"),("total","total_amount")):
            self.line(f"{label}: {getattr(t, attr)}")

    def audit_final_summary(self, members: Iterable[MemberLiquidation]) -> None:
        for m in members:
            s = m.member_id
            (self.summary["with_surface"] if m.applicable_hectares > 0 else self.summary["without_surface"]).add(s)
            (self.summary["with_kg"] if m.hectare_fee_total_effective_kg > 0 else self.summary["without_kg"]).add(s)
            if m.hectare_fee_amount and m.hectare_fee_amount != 0: self.summary["fee_calculated"].add(s)
            if m.hectare_fee_amount == 0: self.summary["fee_zero"].add(s)
            if m.hectare_fee_status == CalculationStatus.ERROR: self.summary["errors"].add(s)
        self.section("RESUMEN FINAL")
        labels = (("SOCIOS PROCESADOS","processed"),("Socios con superficie","with_surface"),("Socios sin superficie","without_surface"),("Socios con kilos","with_kg"),("Socios sin kilos","without_kg"),("Socios con cuota calculada","fee_calculated"),("Socios con cuota cero","fee_zero"),("Socios con errores","errors"))
        for label, key in labels: self.line(f"{label}: {len(self.summary[key])}")

    def audit_excel_row(self, member: MemberLiquidation, value: Any) -> None:
        self.subsection("EXPORTADOR EXCEL - fila")
        self.line(f"Socio: {member.member_id}")
        self.line(f"Valor recibido para C.Has.: {member.hectare_fee_amount}")
        if value is None:
            reason = "hectare_fee_amount=None" if member.hectare_fee_amount is None else f"status={getattr(member.hectare_fee_status, 'value', member.hectare_fee_status)}"
            self.line(f"Escribe '-'. Motivo: {reason}")
        elif value == 0:
            self.line(f"Escribe 0. Motivo: status={getattr(member.hectare_fee_status, 'value', member.hectare_fee_status)} o valor=0")
        else:
            self.line(f"Escribe: {value}")


def audit_latest_excel_row(member: MemberLiquidation, value: Any) -> None:
    """Append Excel export diagnostics to the latest enabled calculation audit."""
    if not _last_audit_path or not _last_audit_path.exists():
        return
    reason = ""
    if value is None:
        reason = "hectare_fee_amount=None" if member.hectare_fee_amount is None else f"status={getattr(member.hectare_fee_status, 'value', member.hectare_fee_status)}"
    elif value == 0:
        reason = f"status={getattr(member.hectare_fee_status, 'value', member.hectare_fee_status)} o valor=0"
    with _last_audit_path.open("a", encoding="utf-8") as fh:
        fh.write("-" * 50 + "\n")
        fh.write("EXPORTADOR EXCEL - fila\n")
        fh.write("-" * 50 + "\n")
        fh.write(f"Socio: {member.member_id}\n")
        fh.write(f"Valor recibido para C.Has.: {member.hectare_fee_amount}\n")
        if value is None:
            fh.write(f"Escribe '-'. Motivo: {reason}\n")
        elif value == 0:
            fh.write(f"Escribe 0. Motivo: {reason}\n")
        else:
            fh.write(f"Escribe: {value}\n")
