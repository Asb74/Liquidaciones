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
    assert "password" not in (ROOT / "config" / "postgresql.toml").read_text().lower()


def test_application_configuration_is_loaded_from_the_single_toml():
    config = load_config()
    assert config.mode == "POSTGRESQL"
    assert config.postgresql_settings.database == "perceco"


def test_no_removed_database_driver_or_local_database_artifacts_remain():
    forbidden_driver = "sqlite" + "3"
    forbidden_suffix = ".sql" + "ite"
    production = [path for path in ROOT.rglob("*.py") if "tests" not in path.parts]
    assert not [path for path in production if forbidden_driver in path.read_text(encoding="utf-8")]
    assert not list(ROOT.rglob(f"*{forbidden_suffix}"))


def test_repository_contract_is_runtime_checkable():
    assert getattr(IDataRepository, "_is_runtime_protocol", False)

