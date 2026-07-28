from __future__ import annotations

import logging
from pathlib import Path
import configparser
import os

from data.postgres_repository import PostgresRepository, PostgreSQLSettings
from domain.models import AppConfig
from domain.utils import decimal_or_zero


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """Carga la fuente única no secreta; el entorno sólo puede sobrescribir backend."""
    base = Path(__file__).resolve().parents[1]
    ini_path = Path(config_path) if config_path else base / "config.ini"
    parser = configparser.ConfigParser(); parser.read(ini_path, encoding="utf-8")
    backend = os.environ.get("DATABASE_BACKEND", parser.get("database_backend", "backend", fallback="")).strip().lower()
    if backend != "postgresql":
        raise ValueError(f"Backend no permitido para producción: {backend or '<vacío>'}")
    settings = PostgreSQLSettings.load(ini_path)
    return AppConfig(
        db_fruta="legacy_dbfruta", db_eepp="legacy_eepp",
        app_name=parser.get("application", "name"), mode=parser.get("application", "mode"),
        window_width=parser.getint("application", "window_width"), window_height=parser.getint("application", "window_height"),
        log_file=parser.get("logging", "file"), log_level=parser.get("logging", "level"), audit_enabled=parser.getboolean("AUDIT", "enabled"),
        hectare_fee_price_per_hectare=decimal_or_zero(parser.get("hectare_fee", "price_per_hectare")),
        hectare_fee_surface_crops=tuple(x.strip() for x in parser.get("hectare_fee", "surface_crops").split(",")),
        hectare_fee_delivery_crops=tuple(x.strip() for x in parser.get("hectare_fee", "delivery_crops").split(",")),
        hectare_fee_applicable_remittance_crops=tuple(x.strip() for x in parser.get("hectare_fee", "applicable_remittance_crops").split(",")),
        postgresql_settings=settings, backend=backend,
    )


def setup_logging(config: AppConfig) -> None:
    log_path = Path(config.log_file)
    if ":" in config.log_file:
        log_path = Path(__file__).resolve().parents[1] / "logs" / "remesas.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=log_path, level=getattr(logging, config.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")


class ReadOnlyDatabase(PostgresRepository):
    """Nombre conservado para la UI; la implementación es PostgreSQL."""
    def __init__(self, config: AppConfig) -> None:
        super().__init__(config.postgresql_settings)

    def connect_fruta_with_eepp(self):
        context = self.connect()
        repository = context.__enter__()
        repository._owner_context = context
        return repository

    def connect_eepp(self):
        return self.connect_fruta_with_eepp()

    def status(self) -> dict[str, str]:
        try:
            with self.connect() as repository:
                repository.execute("SELECT 1").fetchone()
            return {"PostgreSQL": "OK - datos compartidos en tiempo real"}
        except Exception as exc:
            logging.getLogger(__name__).error("PostgreSQL no disponible: %s", exc)
            return {"PostgreSQL": "No accesible"}
