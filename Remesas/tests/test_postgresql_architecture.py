from pathlib import Path

from data.db_connection import load_config
from data.postgres_repository import PostgreSQLSettings
from data.repository import IDataRepository


ROOT = Path(__file__).resolve().parents[1]


def test_configuration_has_expected_non_secret_connection_values(monkeypatch):
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    settings = PostgreSQLSettings.load()
    assert (settings.host, settings.port, settings.database, settings.user, settings.sslmode) == (
        "192.168.1.3", 5432, "perceco", "perceco_engine", "prefer"
    )
    config_text = (ROOT / "config.ini").read_text().lower()
    assert "password" not in config_text
    assert "backend = postgresql" in config_text


def test_application_configuration_is_loaded_from_the_single_ini():
    config = load_config()
    assert config.mode == "POSTGRESQL"
    assert config.postgresql_settings.database == "perceco"


def test_normal_startup_does_not_import_sqlite_or_sync_service():
    forbidden_driver = "sqlite" + "3"
    forbidden_suffix = ".sql" + "ite"
    startup = (ROOT / "app.py").read_text(encoding="utf-8")
    frame = (ROOT / "ui" / "remesas_frame.py").read_text(encoding="utf-8")
    assert forbidden_driver not in startup + frame
    assert "LocalDatabaseSyncService" not in startup + frame
    assert not list(ROOT.rglob(f"*{forbidden_suffix}"))


def test_repository_contract_is_runtime_checkable():
    assert getattr(IDataRepository, "_is_runtime_protocol", False)
