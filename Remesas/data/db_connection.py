from __future__ import annotations

import configparser
import logging
import os
import sqlite3
from pathlib import Path

from domain.models import AppConfig
from domain.utils import decimal_or_zero, format_file_timestamp


def load_config(config_path: str | Path | None = None) -> AppConfig:
    base = Path(__file__).resolve().parents[1]
    parser = configparser.ConfigParser()
    parser.read(config_path or base / "config.ini", encoding="utf-8")
    return AppConfig(
        db_fruta=parser.get("database", "db_fruta"),
        db_eepp=parser.get("database", "db_eepp"),
        app_name=parser.get("application", "name"),
        mode=parser.get("application", "mode"),
        window_width=parser.getint("application", "window_width"),
        window_height=parser.getint("application", "window_height"),
        log_file=parser.get("logging", "file"),
        log_level=parser.get("logging", "level"),
        audit_enabled=parser.getboolean("AUDIT", "enabled", fallback=False),
        audit_dir=str(base / "logs"),
        hectare_fee_price_per_hectare=decimal_or_zero(parser.get("hectare_fee", "price_per_hectare", fallback="195")),
        hectare_fee_surface_crops=tuple(c.strip().upper() for c in parser.get("hectare_fee", "surface_crops", fallback="CITRICOS,MANDARINA").split(",") if c.strip()),
        hectare_fee_delivery_crops=tuple(c.strip().upper() for c in parser.get("hectare_fee", "delivery_crops", fallback="CITRICOS,MANDARINA,DIRECTO,DIRECTOCHF,INDUSTRIA").split(",") if c.strip()),
        hectare_fee_applicable_remittance_crops=tuple(c.strip().upper() for c in parser.get("hectare_fee", "applicable_remittance_crops", fallback="CITRICOS,MANDARINA,DIRECTO,DIRECTOCHF,INDUSTRIA").split(",") if c.strip()),
    )


def setup_logging(config: AppConfig) -> None:
    log_path = Path(config.log_file)
    if ":" in config.log_file:
        log_path = Path(__file__).resolve().parents[1] / "logs" / "remesas.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=log_path, level=getattr(logging, config.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")


class ReadOnlyDatabase:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def readonly_uri(path: str) -> str:
        return f"file:{path}?mode=ro"

    def connect_fruta_with_eepp(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.readonly_uri(self.config.db_fruta), uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        conn.execute("ATTACH DATABASE ? AS eepp", (self.config.db_eepp,))
        self.logger.info("Conexión SQLite de lectura creada con ATTACH eepp")
        return conn

    def connect_eepp(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.readonly_uri(self.config.db_eepp), uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn

    def status(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for key, path in (("DBfruta", self.config.db_fruta), ("DBEEPPL", self.config.db_eepp)):
            try:
                mtime = os.path.getmtime(path)
                result[key] = f"OK - modificado {format_file_timestamp(mtime)}"
            except OSError:
                result[key] = "No accesible"
        return result
