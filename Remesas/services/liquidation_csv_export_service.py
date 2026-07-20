"""Accounting CSV export from immutable, persisted liquidation rows only."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import time
from typing import Any
from domain.member_rules import is_excluded_member

logger = logging.getLogger(__name__)

CSV_HEADERS = ("Id", "IdLiq", "Fecha", "Cultivo", "Campaña", "Empresa", "IdSocio", "Socio", "CodArt", "Variedad", "Neto", "ImpBruto", "PrecioComer", "Recoleccion", "CuotaHa", "BPCalidad", "BTransporte", "BGlobal", "BaseI", "PrecioMedio", "IVA", "Retencion", "ImporteTotal", "IdConceptoLiq", "ConceptoLiq", "Tipo")
CSV_FIELDS = ("id", "id_liq", "fecha", "cultivo", "campana", "empresa", "id_socio", "socio", "cod_art", "variedad", "neto", "imp_bruto", "precio_comer", "recoleccion", "cuota_ha", "bp_calidad", "b_transporte", "b_global", "base_i", "precio_medio", "iva", "retencion", "importe_total", "id_concepto_liq", "concepto_liq", "tipo")
DECIMAL_FIELDS = frozenset(("neto", "imp_bruto", "precio_comer", "recoleccion", "cuota_ha", "bp_calidad", "b_transporte", "b_global", "base_i", "precio_medio", "iva", "retencion", "importe_total"))
TEXT_FIELDS = frozenset(set(CSV_FIELDS) - DECIMAL_FIELDS - {"id", "id_socio", "cod_art", "id_concepto_liq", "fecha"})


@dataclass(frozen=True)
class CsvExportResult:
    success: bool
    export_id: int | None
    csv_path: Path | None
    info_path: Path | None
    export_type: str
    line_count: int = 0
    excluded_line_count: int = 0
    net_total: Decimal = Decimal("0")
    amount_total: Decimal = Decimal("0")
    file_hash: str | None = None
    warnings: tuple[str, ...] = ()
    error_message: str | None = None
    already_existed: bool = False


class LiquidationCsvExportService:
    def __init__(self, repository, legacy_repository=None, output_directory: Path | str | None = None) -> None:
        self.repository, self.legacy_repository = repository, legacy_repository
        # CSVs live with their liquidation documents; never on a shared drive.
        self.output_root = Path(output_directory) if output_directory else Path.cwd() / "salidas" / "remesas"

    @staticmethod
    def format_decimal(value: Any) -> str:
        if value is None or value == "": return ""
        try: value = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError) as exc: raise ValueError(f"Importe no convertible a Decimal: {value!r}") from exc
        if not value.is_finite(): raise ValueError("Los importes deben ser finitos")
        value = value.normalize()
        result = format(value, "f")
        if "." in result: result = result.rstrip("0").rstrip(".")
        return ("0" if result in ("", "-0") else result).replace(".", ",")

    @staticmethod
    def format_date(value: Any) -> str:
        if value is None or value == "": return ""
        if isinstance(value, datetime): return value.strftime("%d/%m/%Y")
        if isinstance(value, date): return value.strftime("%d/%m/%Y")
        text = str(value).strip()
        for parser in (datetime.fromisoformat, lambda v: datetime.strptime(v, "%d/%m/%Y")):
            try: return parser(text.replace("Z", "+00:00")).strftime("%d/%m/%Y")
            except ValueError: pass
        raise ValueError(f"Fecha no compatible: {value!r}")

    @staticmethod
    def _value(row, field): return row[field] if hasattr(row, "keys") else getattr(row, field)

    def validate_rows(self, rows) -> tuple[str, ...]:
        errors=[]
        for row in rows:
            record = self._value(row, "id")
            for field in CSV_FIELDS:
                try: value=self._value(row, field)
                except (KeyError, AttributeError): errors.append(f"Falta la columna {field} del registro {record}."); continue
                if field in TEXT_FIELDS and value is not None:
                    text=str(value)
                    if ";" in text: errors.append(f"El campo {field} del registro {record} contiene un punto y coma no compatible con el formato contable.")
                    try: text.encode("cp1252")
                    except UnicodeEncodeError: errors.append(f"El campo {field} del registro {record} contiene caracteres no representables en CP1252.")
                if field in DECIMAL_FIELDS:
                    try: self.format_decimal(value)
                    except ValueError as exc: errors.append(f"Registro {record}, campo {field}: {exc}")
            try: self.format_date(self._value(row, "fecha"))
            except ValueError as exc: errors.append(f"Registro {record}: {exc}")
            if not self._value(row, "id_liq"): errors.append(f"El registro {record} no tiene IdLiq.")
        return tuple(errors)

    @staticmethod
    def _safe_filename(text: Any) -> str:
        return re.sub(r'[<>:"/\\|?*]', "-", str(text or "")).strip().rstrip(".")

    def build_csv_filename(self, batch, *, member_id=None, modification=False, attempt=None) -> str:
        base=f"Liquidación {self._safe_filename(batch['crop'])} {self._safe_filename(batch['campaign'])}({self._safe_filename(batch['remesa_name'])}"
        if member_id is not None: base += f" ({member_id})"
        base += ")"
        if modification: base += f" - MODIFICACIÓN {attempt or datetime.now().strftime('%Y%m%d-%H%M%S')}"
        return base + ".csv"

    def build_info_filename(self, batch, *, member_id=None, modification=False, attempt=None) -> str:
        return self.build_csv_filename(batch, member_id=member_id, modification=modification, attempt=attempt).replace("Liquidación ", "Información ", 1).removesuffix(".csv") + ".txt"

    def export_batch(self, batch_id: str, output_directory: Path | None = None, member_id: int | None = None, user: str | None = None, force: bool = False) -> CsvExportResult:
        batch=self.repository.get_batch(batch_id)
        if not batch: return self._failure("FULL_BATCH", "El lote no existe.", batch_id=batch_id, user=user)
        if batch["status"] not in ("ACTIVE", "PARTIAL"): return self._failure("FULL_BATCH", "El estado del lote no permite exportarlo.", batch_id=batch_id, user=user)
        rows=self.repository.list_csv_rows_for_batch(batch_id, member_id)
        return self._export(rows, batch, "MEMBER" if member_id is not None else "FULL_BATCH", batch_id=batch_id, member_id=member_id, output_directory=output_directory, user=user, force=force)

    def export_batches(self, batch_ids, output_directory: Path | None = None, user: str | None = None, force: bool = False) -> CsvExportResult:
        """Build one CSV directly from SQLite rows for all selected batches."""
        batch_ids = tuple(dict.fromkeys(str(batch_id) for batch_id in batch_ids))
        if len(batch_ids) == 1:
            return self.export_batch(batch_ids[0], output_directory=output_directory, user=user, force=force)
        batches = tuple(self.repository.get_batch(batch_id) for batch_id in batch_ids)
        if not batches or any(batch is None for batch in batches):
            return self._failure("MASS", "Uno de los lotes seleccionados no existe.", user=user)
        if any(batch["status"] not in ("ACTIVE", "PARTIAL") for batch in batches):
            return self._failure("MASS", "El estado de uno de los lotes no permite exportarlo.", user=user)
        rows = self.repository.export_batches(batch_ids)
        return self._export(rows, batches[0], "MASS", batch_ids=batch_ids,
                            output_directory=output_directory, user=user, force=force)

    def export_modification(self, modification_group_id: str, output_directory: Path | None = None, user: str | None = None, force: bool = False) -> CsvExportResult:
        rows=self.repository.list_csv_rows_for_modification(modification_group_id)
        reversal=[r for r in rows if r["operation_type"] == "REVERSAL"]; replacement=[r for r in rows if r["operation_type"] == "REPLACEMENT"]
        if not reversal or not replacement: return self._failure("MODIFICATION", "La rectificación no está completa. Deben existir un movimiento negativo y una nueva liquidación positiva.", modification_group_id=modification_group_id, user=user)
        batch=self.repository.get_batch(reversal[0]["batch_id"])
        return self._export(rows, batch, "MODIFICATION", batch_id=None, modification_group_id=modification_group_id, output_directory=output_directory, user=user, force=force)

    def regenerate_export(self, export_id: int, user: str | None = None) -> CsvExportResult:
        old=self.repository.get_csv_export(export_id)
        if not old: return self._failure("", "No existe la exportación solicitada.", user=user)
        self.repository.mark_csv_export_superseded(export_id)
        if old["export_type"] == "MASS":
            return self.export_batches(json.loads(old["batch_ids_json"] or "[]"), Path(old["file_path"]).parent, user, force=True)
        if old["modification_group_id"]: return self.export_modification(old["modification_group_id"], Path(old["file_path"]).parent, user, force=True)
        return self.export_batch(old["batch_id"], Path(old["file_path"]).parent, old["member_id"], user, force=True)

    def _export(self, rows, batch, export_type, *, batch_id=None, batch_ids=(), modification_group_id=None, member_id=None, output_directory=None, user=None, force=False):
        if not rows: return self._failure(export_type, "El lote no contiene liquidaciones exportables.", batch_id=batch_id, modification_group_id=modification_group_id, user=user)
        if self.legacy_repository is None: return self._failure(export_type, "No se puede consultar FacSoc en la base legacy.", batch_id=batch_id, modification_group_id=modification_group_id, user=user)
        system_rows=[row for row in rows if is_excluded_member(row["id_socio"]) or ("recipient_member_id" in row.keys() and is_excluded_member(row["recipient_member_id"]))]
        rows=[row for row in rows if row not in system_rows]
        included=[]; excluded=len(system_rows)
        try:
            for row in rows:
                if self.legacy_repository.member_is_self_billed(int(row["id_socio"])): excluded += 1
                else: included.append(row)
        except Exception as exc: return self._failure(export_type, f"No se puede consultar FacSoc: {exc}", batch_id=batch_id, modification_group_id=modification_group_id, user=user)
        if not included:
            message = "No existen liquidaciones exportables. El socio 0 es un registro técnico excluido." if system_rows else "No existen liquidaciones exportables. Todos los socios seleccionados están excluidos por FacSoc = SI."
            return self._failure(export_type, message, batch_id=batch_id, modification_group_id=modification_group_id, user=user, excluded=excluded)
        errors=self.validate_rows(included)
        if errors: return self._failure(export_type, "\n".join(errors), batch_id=batch_id, modification_group_id=modification_group_id, user=user, excluded=excluded)
        fingerprint=hashlib.sha256(json.dumps([[self._value(r, f) for f in CSV_FIELDS] for r in included], default=str, ensure_ascii=False, separators=(",",":" )).encode()).hexdigest()
        duplicate=self.repository.find_generated_csv_export(batch_id=batch_id, modification_group_id=modification_group_id, member_id=member_id, export_type=export_type, source_fingerprint=fingerprint)
        if duplicate and not force:
            return CsvExportResult(False, duplicate["id"], Path(duplicate["file_path"]), Path(duplicate["info_file_path"]) if duplicate["info_file_path"] else None, export_type, already_existed=True, error_message="Esta liquidación ya fue exportada a contabilidad.")
        directory = Path(output_directory) if output_directory else self._output_directory(batch, export_type)
        try:
            directory.mkdir(parents=True, exist_ok=True)
            if not os.access(directory, os.W_OK): raise OSError("La carpeta no permite escritura")
            # Massive filenames use the required second precision.
            stamp=datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            if export_type == "MASS":
                csv_path = directory / f"Exportación masiva {stamp}.csv"
                info_path = directory / f"Información Exportación masiva {stamp}.txt"
                # Names intentionally have second precision; regenerate in the next second.
                while csv_path.exists() or info_path.exists():
                    time.sleep(0.05)
                    stamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
                    csv_path = directory / f"Exportación masiva {stamp}.csv"
                    info_path = directory / f"Información Exportación masiva {stamp}.txt"
            else:
                attempt=datetime.now().strftime("%Y%m%d-%H%M%S-%f")
                csv_path=directory / self.build_csv_filename(batch, member_id=member_id, modification=export_type == "MODIFICATION", attempt=attempt)
                info_path=directory / self.build_info_filename(batch, member_id=member_id, modification=export_type == "MODIFICATION", attempt=attempt)
            if csv_path.exists() or info_path.exists(): raise FileExistsError("Ya existe un archivo con el nombre previsto")
            content=self._csv_content(included); self._atomic_write(csv_path, content)
            file_hash=hashlib.sha256(csv_path.read_bytes()).hexdigest()
            net=sum((Decimal(str(r["neto"])) for r in included), Decimal("0")); amount=sum((Decimal(str(r["importe_total"])) for r in included), Decimal("0"))
            self._atomic_write(info_path, self._info_content(batch, included, export_type, batch_id, batch_ids, modification_group_id, excluded, csv_path, file_hash, net, amount, user))
            export_id=self.repository.record_csv_export(batch_id=batch_id, modification_group_id=modification_group_id, remittance_id=batch["remesa_id"], member_id=member_id, export_type=export_type, batch_ids_json=json.dumps(batch_ids) if export_type == "MASS" else None, file_path=str(csv_path), info_file_path=str(info_path), status="GENERATED", line_count=len(included), excluded_line_count=excluded, net_total=str(net), amount_total=str(amount), file_hash=file_hash, source_fingerprint=fingerprint, generated_at=datetime.now().isoformat(), created_by=user)
            self.repository.audit(batch_id or "", "CSV_MASS_EXPORT" if export_type == "MASS" else "CSV_BATCH_EXPORT", json.dumps({"batch_ids":batch_ids,"modification_group_id":modification_group_id,"export_type":export_type,"batch_count":len(batch_ids) or 1,"rows_exported":len(included),"rows_excluded":excluded,"file_path":str(csv_path),"hash":file_hash}), user)
            logger.info("[AccountingCsvExport] batch_id=%s modification_group_id=%s export_type=%s selected_rows=%d exported_rows=%d excluded_rows=%d net_total=%s amount_total=%s path=%s hash=%s status=GENERATED", batch_id, modification_group_id, export_type, len(rows), len(included), excluded, net, amount, csv_path, file_hash)
            return CsvExportResult(True, export_id, csv_path, info_path, export_type, len(included), excluded, net, amount, file_hash)
        except Exception as exc:
            return self._failure(export_type, str(exc), batch_id=batch_id, modification_group_id=modification_group_id, user=user, excluded=excluded)

    def _output_directory(self, batch, export_type):
        if export_type == "MASS":
            return self.output_root / "Impresiones masivas" / "Exportaciones"
        return (self.output_root / self._safe_filename(batch["campaign"]) /
                self._safe_filename(batch["crop"]) / self._safe_filename(batch["remesa_name"]) /
                "Exportaciones")

    def _csv_content(self, rows):
        lines=[";".join(CSV_HEADERS)]
        for row in rows:
            values=[]
            for field in CSV_FIELDS:
                value=self._value(row,field)
                values.append(self.format_decimal(value) if field in DECIMAL_FIELDS else self.format_date(value) if field == "fecha" else "" if value is None else str(value))
            lines.append(";".join(values))
        return ("\r\n".join(lines)+"\r\n").encode("cp1252")

    @staticmethod
    def _atomic_write(path, content):
        tmp=Path(str(path)+".tmp")
        try:
            with open(tmp,"wb") as file: file.write(content); file.flush(); os.fsync(file.fileno())
            os.replace(tmp,path)
        except Exception:
            tmp.unlink(missing_ok=True); raise

    def _info_content(self,batch,rows,export_type,batch_id,batch_ids,group,excluded,csv_path,file_hash,net,amount,user):
        now=datetime.now(); labels={"FULL_BATCH":"COMPLETA","MEMBER":"SOCIO","MODIFICATION":"MODIFICACIÓN","REVERSAL_ONLY":"ANULACIÓN","MASS":"MASIVA"}
        if export_type == "MASS":
            remittances = tuple(dict.fromkeys(str(row["remittance_id"]) for row in rows))
            lines=(f"Número de remesas: {len(batch_ids)}", "Listado de remesas:", *remittances,
                   f"Número de líneas: {len(rows)}", f"Neto total: {self.format_decimal(net)}",
                   f"Importe total: {self.format_decimal(amount)}", f"Usuario: {user or ''}",
                   f"Fecha: {now:%d/%m/%Y}", f"Hora: {now:%H:%M:%S}", f"Ruta: {csv_path}",
                   f"Líneas excluidas por FacSoc: {excluded}", f"Hash SHA-256: {file_hash}")
        else:
            lines=(f"Nº Liquidación: {rows[0]['id_concepto_liq']}",f"Concepto Liquidación: {rows[0]['concepto_liq']}",f"Total Liquidaciones: {len(rows)}",f"Neto Liquidado: {self.format_decimal(net)}",f"Importe Total Liquidado: {self.format_decimal(amount)}",f"Campaña: {batch['campaign']}",f"Cultivo: {batch['crop']}",f"Empresa: {batch['company']}",f"Usuario: {user or ''}",f"Día: {now:%d/%m/%Y}",f"Hora: {now:%H:%M:%S}",f"Tipo de exportación: {labels[export_type]}",f"Id. de lote: {batch_id or ''}",f"Grupo de modificación: {group or ''}",f"Líneas excluidas por FacSoc: {excluded}",f"Archivo CSV: {csv_path}",f"Hash SHA-256: {file_hash}")
        return ("\r\n".join(lines)+"\r\n").encode("cp1252")

    def _failure(self, export_type, error, *, batch_id=None, modification_group_id=None, user=None, excluded=0):
        export_id=self.repository.record_csv_export(batch_id=batch_id, modification_group_id=modification_group_id, export_type=export_type or "FULL_BATCH", file_path="", status="FAILED", excluded_line_count=excluded, error_message=error, created_by=user)
        if batch_id: self.repository.audit(batch_id,"CSV_EXPORT_FAILED",json.dumps({"error":error}),user)
        logger.error("[AccountingCsvExport] batch_id=%s modification_group_id=%s status=FAILED error=%s", batch_id, modification_group_id, error)
        return CsvExportResult(False, export_id, None, None, export_type, excluded_line_count=excluded, error_message=error)
