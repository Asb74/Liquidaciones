"""Configuracion, conexion y diagnostico seguro de PostgreSQL."""
from __future__ import annotations

import logging
import os
import re
import socket
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class PostgresError(RuntimeError):
    """Base para errores que pueden presentarse al usuario sin secretos."""

    title = "Error de PostgreSQL"
    retryable = False


class PostgresConfigurationError(PostgresError): title = "Configuración de PostgreSQL incorrecta"


class MissingPostgresPasswordError(PostgresConfigurationError):
    title = "Configuración de PostgreSQL incompleta"


class PostgresAuthenticationError(PostgresError): title = "Autenticación PostgreSQL fallida"
class PostgresServerUnavailableError(PostgresError):
    title = "Servidor PostgreSQL no accesible"
    retryable = True
class PostgresPortRejectedError(PostgresServerUnavailableError): pass
class PostgresConnectionTimeoutError(PostgresError):
    title = "Tiempo de conexión agotado"
    retryable = True
class PostgresDatabaseNotFoundError(PostgresError): title = "Base de datos no disponible"
class PostgresPermissionDeniedError(PostgresError): title = "Permisos insuficientes"
class PostgresSchemaMissingError(PostgresError): title = "Esquema de liquidaciones no preparado"
class PostgresMigrationPendingError(PostgresError): title = "Base de datos pendiente de actualizar"
class PostgresConnectionLostError(PostgresError):
    title = "Conexión con PostgreSQL perdida"
    retryable = True
class PostgresUnknownConnectionError(PostgresError): title = "Error de PostgreSQL no clasificado"


MISSING_PASSWORD_MESSAGE = (
    "No se ha encontrado la variable de entorno POSTGRES_PASSWORD.\n\n"
    "La aplicación necesita esta variable para conectarse al servidor PostgreSQL.\n\n"
    "Configure la contraseña en Windows y vuelva a iniciar la aplicación.\n\n"
    "La contraseña no debe escribirse en el código ni en archivos versionados."
)


def sanitize_postgres_error(value: object, secrets: tuple[str, ...] = ()) -> str:
    """Elimina credenciales y DSN de texto procedente del controlador."""
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(?i)\b(password|passfile)\s*=\s*(?:'[^']*'|\"[^\"]*\"|\S+)", r"\1=[REDACTED]", text)
    text = re.sub(r"(?i)\bdbname\s*=\s*(?:'[^']*'|\"[^\"]*\"|\S+)", "dbname=[REDACTED]", text)
    text = re.sub(r"(?i)postgres(?:ql)?://[^\s]+", "postgresql://[REDACTED]", text)
    return text


@dataclass(frozen=True, repr=False)
class PostgresSettings:
    host: str = "192.168.1.3"
    port: int = 5432
    database: str = "perceco"
    user: str = "perceco_engine"
    password: str = field(default="", repr=False)
    sslmode: str = "prefer"
    connect_timeout: int = 5
    application_name: str = "liquidaciones_remesas"

    @classmethod
    def from_env(cls, *, validate: bool = True) -> "PostgresSettings":
        try:
            port = int(os.environ.get("POSTGRES_PORT", "5432"))
            timeout = int(os.environ.get("POSTGRES_CONNECT_TIMEOUT", "5"))
        except ValueError as exc:
            raise PostgresConfigurationError("POSTGRES_PORT y POSTGRES_CONNECT_TIMEOUT deben ser números enteros.") from exc
        settings = cls(
            host=os.environ.get("POSTGRES_HOST", "192.168.1.3").strip(), port=port,
            database=os.environ.get("POSTGRES_DB", "perceco").strip(),
            user=os.environ.get("POSTGRES_USER", "perceco_engine").strip(),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
            sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer").strip(), connect_timeout=timeout,
            application_name=os.environ.get("POSTGRES_APPLICATION_NAME", "liquidaciones_remesas").strip(),
        )
        if validate:
            settings.validate()
        logger.info("[PostgresConfig] host=%s port=%s database=%s user=%s sslmode=%s password_configured=%s",
                    settings.host, settings.port, settings.database, settings.user, settings.sslmode, bool(settings.password.strip()))
        return settings

    def validate(self) -> None:
        if not self.password.strip():
            raise MissingPostgresPasswordError(MISSING_PASSWORD_MESSAGE)
        if not self.host or not self.database or not self.user or not self.application_name:
            raise PostgresConfigurationError("La configuración PostgreSQL contiene campos obligatorios vacíos.")
        if not 1 <= self.port <= 65535 or self.connect_timeout < 1:
            raise PostgresConfigurationError("El puerto o el tiempo de conexión no son válidos.")

    def kwargs(self) -> dict[str, object]:
        self.validate()
        return {"host": self.host, "port": self.port, "dbname": self.database, "user": self.user,
                "password": self.password, "sslmode": self.sslmode, "connect_timeout": self.connect_timeout,
                "application_name": self.application_name}

    def safe_target(self) -> str:
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

    def __repr__(self) -> str:
        return (f"PostgresSettings(host={self.host!r}, port={self.port!r}, database={self.database!r}, "
                f"user={self.user!r}, password='[REDACTED]', sslmode={self.sslmode!r}, "
                f"connect_timeout={self.connect_timeout!r}, application_name={self.application_name!r})")


@dataclass(frozen=True)
class PostgresConnectionDiagnostic:
    success: bool
    error_type: str | None
    user_message: str
    technical_message: str
    host: str
    port: int
    database: str
    user: str
    schema_exists: bool | None = None
    migrations_ok: bool | None = None
    sqlstate: str | None = None
    retryable: bool = False
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


SQLSTATE_ERRORS: dict[str, type[PostgresError]] = {
    "28P01": PostgresAuthenticationError, "28000": PostgresAuthenticationError,
    "3D000": PostgresDatabaseNotFoundError, "42501": PostgresPermissionDeniedError,
    "3F000": PostgresSchemaMissingError, "57P01": PostgresConnectionLostError,
    "57P03": PostgresServerUnavailableError, "08006": PostgresConnectionLostError,
    "08001": PostgresServerUnavailableError, "08004": PostgresPortRejectedError,
}


def classify_postgres_exception(exc: BaseException) -> PostgresError:
    sqlstate = getattr(exc, "sqlstate", None)
    error_class = SQLSTATE_ERRORS.get(sqlstate)
    if error_class:
        return error_class(sanitize_postgres_error(exc))
    if isinstance(exc, (TimeoutError, socket.timeout)) or "timed out" in str(exc).lower() or "timeout expired" in str(exc).lower():
        return PostgresConnectionTimeoutError(sanitize_postgres_error(exc))
    text = str(exc).lower()
    if "connection refused" in text or "actively refused" in text:
        return PostgresPortRejectedError(sanitize_postgres_error(exc))
    if "could not connect" in text or "no route to host" in text or "name or service not known" in text:
        return PostgresServerUnavailableError(sanitize_postgres_error(exc))
    try:
        import psycopg
        if isinstance(exc, psycopg.OperationalError):
            return PostgresServerUnavailableError(sanitize_postgres_error(exc))
    except ImportError:
        pass
    return PostgresUnknownConnectionError(sanitize_postgres_error(exc))


def _message(error: PostgresError, settings: PostgresSettings) -> str:
    hp = f"Servidor: {settings.host}\nPuerto: {settings.port}"
    if isinstance(error, MissingPostgresPasswordError): return MISSING_PASSWORD_MESSAGE
    if isinstance(error, PostgresAuthenticationError): return f"No se ha podido iniciar sesión en PostgreSQL.\n\nRevise el usuario {settings.user} y la variable de entorno POSTGRES_PASSWORD.\n\nLa contraseña configurada no se mostrará por seguridad."
    if isinstance(error, PostgresConnectionTimeoutError): return f"El servidor PostgreSQL no respondió dentro del tiempo esperado.\n\n{hp}\n\nCompruebe la conexión de red y el estado del servidor."
    if isinstance(error, PostgresServerUnavailableError): return f"No se ha podido contactar con el servidor PostgreSQL.\n\n{hp}\n\nCompruebe que el servidor está encendido, PostgreSQL está iniciado y el equipo está conectado a la red local."
    if isinstance(error, PostgresDatabaseNotFoundError): return f"La base de datos {settings.database} no existe o no es accesible para el usuario configurado."
    if isinstance(error, PostgresPermissionDeniedError): return f"El usuario {settings.user} no dispone de los permisos necesarios para trabajar con la aplicación.\n\nRevise los permisos del esquema liquidaciones y los permisos de lectura sobre integracion, informes y legacy_*."
    if isinstance(error, PostgresSchemaMissingError): return "El servidor PostgreSQL está disponible, pero no existe el esquema liquidaciones.\n\nDebe ejecutar las migraciones de base de datos antes de iniciar la aplicación."
    if isinstance(error, PostgresMigrationPendingError): return "El esquema liquidaciones existe, pero hay migraciones pendientes.\n\nActualice la base de datos antes de continuar."
    if isinstance(error, PostgresConnectionLostError): return "Se perdió la conexión con PostgreSQL durante la operación.\n\nLa aplicación ha ejecutado rollback cuando ha sido posible.\n\nCompruebe el estado de la conexión antes de repetir la operación."
    return "PostgreSQL devolvió un error no clasificado. Consulte el registro técnico o contacte con soporte."


class PostgresConnectionDiagnostics:
    def __init__(self, settings: PostgresSettings, connector: Callable[..., Any] | None = None,
                 migrations_dir: Path | None = None):
        self.settings = settings
        self.connector = connector
        self.migrations_dir = migrations_dir or Path(__file__).resolve().parents[1] / "migrations" / "postgresql"

    def check_connection(self) -> PostgresConnectionDiagnostic:
        schema_exists = migrations_ok = None
        try:
            self.settings.validate()
            if self.connector is None:
                import psycopg
                connector = psycopg.connect
            else:
                connector = self.connector
            with connector(**self.settings.kwargs()) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    if cursor.fetchone()[0] != 1: raise PostgresUnknownConnectionError("SELECT 1 no devolvió el resultado esperado.")
                    cursor.execute("SELECT current_database(), current_user")
                    database, user = cursor.fetchone()
                    if database != self.settings.database or user != self.settings.user:
                        raise PostgresConfigurationError("La base o el usuario actuales no coinciden con la configuración.")
                    cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s)", ("liquidaciones",))
                    schema_exists = bool(cursor.fetchone()[0])
                    if not schema_exists: raise PostgresSchemaMissingError("No existe el esquema liquidaciones.")
                    cursor.execute("SELECT has_schema_privilege(current_user, %s, 'USAGE')", ("liquidaciones",))
                    if not cursor.fetchone()[0]: raise PostgresPermissionDeniedError("Falta USAGE sobre el esquema liquidaciones.")
                    cursor.execute("SELECT to_regclass(%s)", ("liquidaciones.schema_migrations",))
                    table_exists = cursor.fetchone()[0] is not None
                    expected = {int(p.name[:3]) for p in self.migrations_dir.glob("[0-9][0-9][0-9]_*.sql")}
                    applied: set[int] = set()
                    if table_exists:
                        cursor.execute("SELECT version FROM liquidaciones.schema_migrations")
                        applied = {int(row[0]) for row in cursor.fetchall()}
                    migrations_ok = table_exists and expected.issubset(applied)
                    if not migrations_ok: raise PostgresMigrationPendingError("Hay migraciones pendientes.")
            result = PostgresConnectionDiagnostic(True, None, "Conexión PostgreSQL correcta.", "OK",
                                                   self.settings.host, self.settings.port, database, user, True, True)
        except PostgresError as exc:
            result = self._failure(exc, schema_exists, migrations_ok, getattr(exc, "sqlstate", None))
        except Exception as exc:
            mapped = classify_postgres_exception(exc)
            result = self._failure(mapped, schema_exists, migrations_ok, getattr(exc, "sqlstate", None), exc)
        logger.info("[PostgresConnection] status=%s error_type=%s sqlstate=%s retryable=%s",
                    "success" if result.success else "failed", result.error_type, result.sqlstate, result.retryable)
        return result

    def _failure(self, error: PostgresError, schema: bool | None, migrations: bool | None,
                 sqlstate: str | None, original: BaseException | None = None) -> PostgresConnectionDiagnostic:
        technical = sanitize_postgres_error(original or error, (self.settings.password,))
        logger.error("PostgreSQL operation=connection_check error_type=%s sqlstate=%s host=%s port=%s database=%s user=%s detail=%s",
                     type(error).__name__, sqlstate, self.settings.host, self.settings.port,
                     self.settings.database, self.settings.user, technical)
        return PostgresConnectionDiagnostic(False, type(error).__name__, _message(error, self.settings), technical,
                                             self.settings.host, self.settings.port, self.settings.database,
                                             self.settings.user, schema, migrations, sqlstate, error.retryable)


class PostgresConnectionFactory:
    def __init__(self, settings: PostgresSettings | None = None): self.settings = settings or PostgresSettings.from_env()
    def connect(self):
        import psycopg
        return psycopg.connect(**self.settings.kwargs())
    @contextmanager
    def transaction(self):
        with self.connect() as connection:
            try:
                with connection.transaction(): yield connection
            except Exception as exc:
                mapped = classify_postgres_exception(exc)
                if isinstance(mapped, (PostgresConnectionLostError, PostgresUnknownConnectionError)):
                    raise mapped from exc
                raise
