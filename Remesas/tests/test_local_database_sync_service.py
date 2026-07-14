from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from domain.models import AppConfig
from services.local_database_sync_service import DatabaseValidationError, LocalDatabaseSyncService


def make_db(path: Path, tables: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        for table in tables:
            conn.execute(f"CREATE TABLE {table} (id INTEGER)")


class LocalDatabaseSyncServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.source = self.base / "source"
        self.local = self.base / "datos"
        self.source.mkdir()
        self.config = AppConfig(
            db_fruta=str(self.local / "DBfruta.sqlite"),
            db_eepp=str(self.local / "DBEEPPL.sqlite"),
            app_name="test", mode="TEST", window_width=1, window_height=1,
            log_file=str(self.base / "test.log"), log_level="INFO",
            source_db_fruta=str(self.source / "DBfruta.sqlite"),
            source_db_eepp=str(self.source / "DBEEPPL.sqlite"),
            local_database_dir=str(self.local),
            local_temp_dir=str(self.local / "temp"),
            local_backup_dir=str(self.local / "backup"),
            sync_metadata_path=str(self.local / "sync_metadata.json"),
        )
        self.service = LocalDatabaseSyncService(self.config)

    def tearDown(self):
        self.tmp.cleanup()

    def test_synchronization_success_copies_valid_databases(self):
        make_db(Path(self.config.source_db_fruta), ("PesosFres", "PagosCIT", "DLiquidaciones"))
        make_db(Path(self.config.source_db_eepp), ("DEEPP", "DParcela", "DSocio"))
        results = self.service.synchronize_all()
        self.assertTrue(all(r.synchronized for r in results))
        self.assertTrue(Path(self.config.db_fruta).exists())
        self.assertTrue(Path(self.config.db_eepp).exists())
        self.assertTrue(Path(self.config.sync_metadata_path).exists())
        self.service.validate_sqlite_database(Path(self.config.db_fruta), ("PesosFres", "PagosCIT", "DLiquidaciones"))

    def test_source_unavailable_uses_valid_local_fallback(self):
        make_db(Path(self.config.db_fruta), ("PesosFres", "PagosCIT", "DLiquidaciones"))
        result = self.service.synchronize_database(Path(self.config.source_db_fruta), Path(self.config.db_fruta), "DBfruta")
        self.assertTrue(result.used_local_fallback)
        self.assertFalse(result.synchronized)

    def test_source_unavailable_without_local_copy_returns_controlled_error(self):
        result = self.service.synchronize_database(Path(self.config.source_db_fruta), Path(self.config.db_fruta), "DBfruta")
        self.assertFalse(result.used_local_fallback)
        self.assertFalse(result.synchronized)
        self.assertIn("No se ha podido acceder", result.error_message)

    def test_corrupt_new_copy_does_not_replace_valid_previous_copy(self):
        make_db(Path(self.config.db_fruta), ("PesosFres", "PagosCIT", "DLiquidaciones"))
        Path(self.config.source_db_fruta).write_bytes(b"not sqlite")
        result = self.service.synchronize_database(Path(self.config.source_db_fruta), Path(self.config.db_fruta), "DBfruta")
        self.assertTrue(result.used_local_fallback)
        self.service.validate_sqlite_database(Path(self.config.db_fruta), ("PesosFres", "PagosCIT", "DLiquidaciones"))

    def test_missing_required_tables_rejects_new_copy(self):
        make_db(Path(self.config.source_db_fruta), ("PesosFres",))
        result = self.service.synchronize_database(Path(self.config.source_db_fruta), Path(self.config.db_fruta), "DBfruta")
        self.assertFalse(result.synchronized)
        self.assertIn("Faltan tablas", result.error_message)

    def test_quick_check_without_row_does_not_raise_unboundlocalerror(self):
        make_db(Path(self.config.source_db_fruta), ("PesosFres", "PagosCIT", "DLiquidaciones"))
        class Cursor:
            def fetchone(self): return None
        class Connection:
            def execute(self, _sql): return Cursor()
            def close(self): pass
        with patch("services.local_database_sync_service.sqlite3.connect", return_value=Connection()):
            with self.assertRaises(DatabaseValidationError) as ctx:
                self.service.validate_sqlite_database(Path(self.config.source_db_fruta), ())
        self.assertNotIn("UnboundLocalError", str(ctx.exception))
