"""Implementación PostgreSQL compartida de :class:`IDataRepository`."""
from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import configparser
from typing import Any, Iterable
import os


LOGGER = logging.getLogger(__name__)


class HybridRow(tuple):
    """Fila compatible con acceso posicional y por nombre de columna."""
    def __new__(cls, values, columns):
        instance = super().__new__(cls, values)
        instance._columns = columns
        return instance

    def __getitem__(self, key):
        if isinstance(key, str):
            return super().__getitem__(self._columns[key.lower()])
        return super().__getitem__(key)

    def keys(self):
        return self._columns.keys()


def hybrid_row(cursor):
    columns = {column.name.lower(): index for index, column in enumerate(cursor.description or ())}
    return lambda values: HybridRow(values, columns)


class PostgreSQLUnavailableError(RuntimeError):
    """Error de conexión presentado de forma segura al usuario."""


@dataclass(frozen=True)
class PostgreSQLSettings:
    host: str
    port: int
    database: str
    user: str
    sslmode: str = "prefer"
    min_pool_size: int = 1
    max_pool_size: int = 10
    connect_timeout: int = 5

    @classmethod
    def load(cls, path: str | Path | None = None) -> "PostgreSQLSettings":
        config_path = Path(path) if path else Path(__file__).resolve().parents[1] / "config.ini"
        parser = configparser.ConfigParser()
        if not parser.read(config_path, encoding="utf-8"):
            raise PostgreSQLUnavailableError(f"No se encontró la configuración: {config_path}")
        section = parser["postgresql"]
        return cls(
            host=section.get("host", "192.168.1.3"),
            port=section.getint("port", 5432),
            database=section.get("database", "perceco"),
            user=section.get("user", "perceco_engine"),
            sslmode=section.get("sslmode", "prefer"),
            min_pool_size=section.getint("min_pool_size", 1),
            max_pool_size=section.getint("max_pool_size", 10),
            connect_timeout=section.getint("connect_timeout", 5),
        )

    def conninfo(self) -> str:
        password = os.environ.get("POSTGRES_PASSWORD")
        if not password:
            raise PostgreSQLUnavailableError("Falta la variable de entorno POSTGRES_PASSWORD.")
        # psycopg accepts keyword dictionaries through ConnectionPool kwargs;
        # keeping the secret out of this string also keeps it out of logs.
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} sslmode={self.sslmode} connect_timeout={self.connect_timeout}"


class PostgresRepository:
    """Repositorio psycopg3; todos los ejemplares comparten un único pool."""

    _pool: Any = None
    _settings: PostgreSQLSettings | None = None

    def __init__(self, settings: PostgreSQLSettings | None = None, *, connection=None) -> None:
        self.settings = settings or PostgreSQLSettings.load()
        self._connection = connection

    @classmethod
    def pool(cls, settings: PostgreSQLSettings | None = None):
        from psycopg_pool import ConnectionPool
        settings = settings or PostgreSQLSettings.load()
        if cls._pool is None:
            password = os.environ.get("POSTGRES_PASSWORD")
            if not password:
                raise PostgreSQLUnavailableError("No se puede conectar a PostgreSQL: defina POSTGRES_PASSWORD.")
            try:
                cls._pool = ConnectionPool(
                    conninfo=settings.conninfo(),
                    kwargs={"password": password, "row_factory": hybrid_row, "autocommit": False},
                    min_size=settings.min_pool_size,
                    max_size=settings.max_pool_size,
                    open=True,
                    timeout=settings.connect_timeout,
                    max_lifetime=section_value(settings, "max_lifetime", 1800),
                    max_idle=section_value(settings, "max_idle", 300),
                )
                cls._pool.wait(timeout=settings.connect_timeout)
                cls._settings = settings
            except Exception as exc:
                cls._pool = None
                LOGGER.exception("PostgreSQL no está disponible")
                raise PostgreSQLUnavailableError(
                    f"No se puede conectar a PostgreSQL en {settings.host}:{settings.port}/{settings.database}."
                ) from exc
        return cls._pool

    @classmethod
    def shutdown_pool(cls) -> None:
        if cls._pool is not None:
            cls._pool.close()
            cls._pool = None

    @staticmethod
    def _sql(query: str) -> str:
        """Adapta placeholders y nombres históricos sin alterar las consultas funcionales."""
        query = re.sub(r"\beepp\.", "legacy_eepp.", query, flags=re.IGNORECASE)
        query = re.sub(r"\bmain\.", "liquidaciones.", query, flags=re.IGNORECASE)
        query = query.replace("?", "%s")
        query = re.sub(r"^\s*BEGIN\s+IMMEDIATE\s*$", "BEGIN", query, flags=re.IGNORECASE)
        query = query.replace("NORMALIZE_SEARCH_TEXT(", "LOWER(")
        query = re.sub(r'\bdate\(substr\(([^,]+),\s*1,\s*10\)\)', r"CAST(SUBSTRING(\1::text, 1, 10) AS date)", query, flags=re.IGNORECASE)
        query = re.sub(r"\bdate\((%s|[^()]+)\)", r"CAST(\1 AS date)", query, flags=re.IGNORECASE)
        query = query.replace('TRIM(Variedad) <> ""', "TRIM(Variedad) <> ''")
        return query

    def _conn(self):
        if self._connection is None:
            raise RuntimeError("La operación requiere una sesión obtenida mediante connect() o transaction().")
        return self._connection

    def execute(self, query: str, params: Iterable[Any] | None = None):
        pragma = re.match(r"\s*PRAGMA\s+(?:(\w+)\.)?table_info\(['\"]?([^)'\"]+)", query, re.IGNORECASE)
        if pragma:
            schema, table = pragma.groups()
            schema = "legacy_eepp" if (schema or "").lower() == "eepp" else (schema or "legacy_dbfruta")
            return self._conn().execute(
                "SELECT ordinal_position-1,column_name,data_type,NULL,NULL,NULL FROM information_schema.columns WHERE table_schema=%s AND lower(table_name)=lower(%s) ORDER BY ordinal_position",
                (schema, table),
            )
        return self._conn().execute(self._sql(query), tuple(params or ()))

    def executemany(self, query: str, params: Iterable[Iterable[Any]]):
        return self._conn().cursor().executemany(self._sql(query), params)

    @contextmanager
    def connect(self):
        if self._connection is not None:
            yield self
            return
        try:
            with self.pool(self.settings).connection() as conn:
                conn.execute("SET search_path TO liquidaciones, legacy_dbfruta, legacy_eepp, integracion, informes, public")
                yield type(self)(self.settings, connection=conn)
        except PostgreSQLUnavailableError:
            raise
        except Exception as exc:
            LOGGER.exception("Error de sesión PostgreSQL")
            raise PostgreSQLUnavailableError("Se perdió la conexión con PostgreSQL; no se guardó ningún dato parcial.") from exc

    @contextmanager
    def transaction(self):
        with self.connect() as repository:
            try:
                with repository._conn().transaction():
                    yield repository
            except Exception:
                LOGGER.exception("Transacción PostgreSQL revertida")
                raise

    def commit(self) -> None:
        self._conn().commit()

    def rollback(self) -> None:
        self._conn().rollback()

    def close(self) -> None:
        # La conexión pertenece al context manager del pool.
        return None


def section_value(settings: PostgreSQLSettings, name: str, default: int) -> int:
    """Pool defaults kept outside credentials and stable across clients."""
    return int(getattr(settings, name, default))
