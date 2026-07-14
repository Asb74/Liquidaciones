from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import time
from typing import Callable, Sequence

from domain.models import AppConfig

logger = logging.getLogger(__name__)

REQUIRED_TABLES: dict[str, tuple[str, ...]] = {
    "DBfruta": ("PesosFres", "PagosCIT", "DLiquidaciones"),
    "DBEEPPL": ("DEEPP", "DParcela", "DSocio"),
}


class DatabaseValidationError(RuntimeError):
    pass


class DatabaseSyncError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseSyncResult:
    database_name: str
    source_path: Path
    local_path: Path
    source_available: bool
    synchronized: bool
    used_local_fallback: bool
    source_modified_at: datetime | None
    local_modified_at: datetime | None
    file_size: int | None
    error_message: str | None

    @property
    def status(self) -> str:
        if self.synchronized:
            return "Actualizadas desde red."
        if self.used_local_fallback:
            return "Usando copia local."
        return "Error de sincronización."


class LocalDatabaseSyncService:
    def __init__(self, config: AppConfig, progress_callback: Callable[[str], None] | None = None) -> None:
        self.config = config
        self.progress_callback = progress_callback
        self.local_dir = Path(config.local_database_dir)
        self.temp_dir = Path(config.local_temp_dir)
        self.backup_dir = Path(config.local_backup_dir)
        self.metadata_path = Path(config.sync_metadata_path)

    def synchronize_all(self) -> list[DatabaseSyncResult]:
        self._ensure_directories()
        pairs = [
            ("DBfruta", Path(self.config.source_db_fruta), Path(self.config.db_fruta)),
            ("DBEEPPL", Path(self.config.source_db_eepp), Path(self.config.db_eepp)),
        ]
        results = [self.synchronize_database(source, local, name) for name, source, local in pairs]
        self._write_metadata(results)
        return results

    def synchronize_database(self, source_path: Path, local_path: Path, database_name: str | None = None) -> DatabaseSyncResult:
        name = database_name or local_path.stem
        required_tables = REQUIRED_TABLES.get(name, ())
        self._ensure_directories()
        self._progress(f"Comprobando {name}.")
        source_available = source_path.exists()
        source_modified_at = self._mtime(source_path) if source_available else None
        file_size = source_path.stat().st_size if source_available else None
        logger.info("Sincronizando %s: origen=%s local=%s existe=%s tamaño=%s", name, source_path, local_path, source_available, file_size)

        if not source_available:
            return self._fallback_or_error(name, source_path, local_path, f"No se ha podido acceder a {source_path}")

        self._warn_if_wal_present(source_path, name)
        temp_path = self.temp_dir / f"{local_path.name}.tmp"
        try:
            start = time.monotonic()
            self._progress(f"Copiando {name}.")
            if temp_path.exists():
                temp_path.unlink()
            shutil.copy2(source_path, temp_path)
            logger.info("Copia temporal %s completada en %.2fs", temp_path, time.monotonic() - start)
            self._progress(f"Validando {name}.")
            self.validate_sqlite_database(temp_path, required_tables)
            if self.config.keep_backup and local_path.exists():
                self.validate_sqlite_database(local_path, required_tables)
                backup_path = self.backup_dir / f"{local_path.stem}_previous{local_path.suffix}"
                shutil.copy2(local_path, backup_path)
                logger.info("Backup local actualizado: %s", backup_path)
            os.replace(temp_path, local_path)
            local_modified_at = self._mtime(local_path)
            logger.info("Base local abierta posteriormente en lectura: %s", local_path)
            return DatabaseSyncResult(name, source_path, local_path, True, True, False, source_modified_at, local_modified_at, file_size, None)
        except Exception as exc:
            logger.exception("Error sincronizando %s", name)
            return self._fallback_or_error(name, source_path, local_path, str(exc), source_modified_at, file_size)
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                logger.warning("No se pudo eliminar temporal %s", temp_path, exc_info=True)

    def validate_sqlite_database(self, path: Path, required_tables: Sequence[str]) -> None:
        check_result: str | None = None
        connection = None
        try:
            uri = f"file:{path.as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=10)
            row = connection.execute("PRAGMA quick_check").fetchone()
            if row is None:
                raise DatabaseValidationError(f"PRAGMA quick_check no devolvió resultado: {path}")
            check_result = str(row[0]).strip().lower()
            if check_result != "ok":
                raise DatabaseValidationError(f"SQLite no válida: {path}. Resultado: {check_result}")
            existing_tables = {str(item[0]) for item in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
            logger.info("Tablas encontradas en %s: %s", path, sorted(existing_tables))
            missing_tables = set(required_tables) - existing_tables
            if missing_tables:
                raise DatabaseValidationError(f"Faltan tablas obligatorias en {path}: {sorted(missing_tables)}")
        finally:
            if connection is not None:
                connection.close()

    def _fallback_or_error(self, name: str, source: Path, local: Path, message: str, source_mtime: datetime | None = None, file_size: int | None = None) -> DatabaseSyncResult:
        if self.config.allow_local_fallback and local.exists():
            try:
                self.validate_sqlite_database(local, REQUIRED_TABLES.get(name, ()))
                logger.warning("Usando fallback local válido para %s: %s", name, local)
                return DatabaseSyncResult(name, source, local, source.exists(), False, True, source_mtime, self._mtime(local), file_size, message)
            except Exception as exc:
                logger.exception("Fallback local inválido para %s", name)
                message = f"{message}. Copia local inválida: {exc}"
        return DatabaseSyncResult(name, source, local, source.exists(), False, False, source_mtime, self._mtime(local), file_size, message)

    def _ensure_directories(self) -> None:
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def _warn_if_wal_present(self, source_path: Path, name: str) -> None:
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(source_path) + suffix)
            if sidecar.exists() and sidecar.stat().st_size > 0:
                logger.warning("%s tiene archivo auxiliar activo %s (%s bytes); se validará la copia antes de aceptarla", name, sidecar, sidecar.stat().st_size)

    def _write_metadata(self, results: Sequence[DatabaseSyncResult]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload = {}
        for result in results:
            payload[result.database_name] = {
                "source_path": str(result.source_path),
                "source_modified_at": result.source_modified_at.isoformat() if result.source_modified_at else None,
                "local_modified_at": result.local_modified_at.isoformat() if result.local_modified_at else None,
                "synced_at": now,
                "size_bytes": result.file_size,
                "status": "ok" if result.synchronized else ("fallback" if result.used_local_fallback else "error"),
                "error_message": result.error_message,
            }
        tmp = self.metadata_path.with_suffix(self.metadata_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.metadata_path)

    def _mtime(self, path: Path) -> datetime | None:
        try:
            return datetime.fromtimestamp(path.stat().st_mtime)
        except OSError:
            return None

    def _progress(self, message: str) -> None:
        logger.info(message)
        if self.progress_callback:
            self.progress_callback(message)
