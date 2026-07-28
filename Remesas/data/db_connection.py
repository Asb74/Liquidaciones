from __future__ import annotations

import logging
from pathlib import Path
import tomllib

from data.postgres_repository import PostgresRepository, PostgreSQLSettings
from domain.models import AppConfig
from domain.utils import decimal_or_zero


def load_config(config_path: str | Path | None = None) -> AppConfig:
    """Carga ajustes no secretos; PostgreSQL se configura en un único TOML."""
    base = Path(__file__).resolve().parents[1]
    pg_path = Path(config_path) if config_path else base / "config" / "postgresql.toml"
    settings = PostgreSQLSettings.load(pg_path)
    with pg_path.open("rb") as stream:
        values = tomllib.load(stream)
    application, log = values["application"], values["logging"]
    hectare, audit = values["hectare_fee"], values["audit"]
    return AppConfig(
        db_fruta="legacy_dbfruta", db_eepp="legacy_eepp",
        app_name=application["name"], mode=application["mode"],
        window_width=application["window_width"], window_height=application["window_height"],
        log_file=log["file"], log_level=log["level"], audit_enabled=audit["enabled"],
        hectare_fee_price_per_hectare=decimal_or_zero(hectare["price_per_hectare"]),
        hectare_fee_surface_crops=tuple(hectare["surface_crops"]),
        hectare_fee_delivery_crops=tuple(hectare["delivery_crops"]),
        hectare_fee_applicable_remittance_crops=tuple(hectare["applicable_remittance_crops"]),
        postgresql_settings=settings,
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
