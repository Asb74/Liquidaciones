from __future__ import annotations

import logging
from pathlib import Path

import pytest

from db_tools.postgres import (
    MissingPostgresPasswordError, PostgresAuthenticationError,
    PostgresConnectionDiagnostics, PostgresConnectionTimeoutError,
    PostgresDatabaseNotFoundError, PostgresMigrationPendingError,
    PostgresPermissionDeniedError, PostgresPortRejectedError,
    PostgresSchemaMissingError, PostgresSettings,
    classify_postgres_exception, sanitize_postgres_error,
)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_password_missing_empty_or_whitespace(monkeypatch, value):
    if value is None: monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    else: monkeypatch.setenv("POSTGRES_PASSWORD", value)
    with pytest.raises(MissingPostgresPasswordError): PostgresSettings.from_env()


def test_password_present_is_never_in_repr_or_logs(monkeypatch, caplog):
    secret = "secret-with-unique-marker"
    monkeypatch.setenv("POSTGRES_PASSWORD", secret)
    with caplog.at_level(logging.INFO): settings = PostgresSettings.from_env()
    assert secret not in repr(settings)
    assert secret not in caplog.text


def test_sanitizer_removes_password_dsn_and_dbname():
    raw = "password=hunter2 dbname=perceco postgresql://user:hunter2@host/perceco"
    clean = sanitize_postgres_error(raw)
    assert "hunter2" not in clean and "dbname=perceco" not in clean and "user:" not in clean


@pytest.mark.parametrize(("state", "expected"), [
    ("28P01", PostgresAuthenticationError), ("28000", PostgresAuthenticationError),
    ("3D000", PostgresDatabaseNotFoundError), ("42501", PostgresPermissionDeniedError),
    ("3F000", PostgresSchemaMissingError),
])
def test_sqlstate_classification(state, expected):
    exc = RuntimeError("driver detail"); exc.sqlstate = state
    assert isinstance(classify_postgres_exception(exc), expected)


def test_timeout_and_rejected_port_classification():
    assert isinstance(classify_postgres_exception(TimeoutError()), PostgresConnectionTimeoutError)
    assert isinstance(classify_postgres_exception(OSError("connection refused")), PostgresPortRejectedError)


class Cursor:
    def __init__(self, replies): self.replies = iter(replies); self.executed = []
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def execute(self, sql, params=None): self.executed.append((sql, params))
    def fetchone(self): return next(self.replies)
    def fetchall(self): return next(self.replies)


class Connection:
    def __init__(self, replies): self.cursor_value = Cursor(replies); self.autocommit = False
    def __enter__(self): return self
    def __exit__(self, *_): pass
    def cursor(self): return self.cursor_value


def connector_for(replies, captured=None):
    def connect(**kwargs):
        if captured is not None: captured.update(kwargs)
        return Connection(replies)
    return connect


def settings(): return PostgresSettings(password="not-logged")


def test_success_and_read_only_select_order(tmp_path):
    (tmp_path / "001_first.sql").write_text("SELECT 1")
    captured = {}
    connection = Connection([(1,), ("perceco", "perceco_engine"), (True,), (True,), ("liquidaciones.schema_migrations",), [(1,)]])
    result = PostgresConnectionDiagnostics(settings(), lambda **kw: (captured.update(kw) or connection), tmp_path).check_connection()
    assert result.success and result.schema_exists and result.migrations_ok
    assert connection.autocommit is True and connection.cursor_value.executed[0][0] == "SELECT 1"
    assert captured["connect_timeout"] == 5
    assert all(not sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE")) for sql, _ in connection.cursor_value.executed)


def test_schema_missing(tmp_path):
    result = PostgresConnectionDiagnostics(settings(), connector_for([(1,), ("perceco", "perceco_engine"), (False,)]), tmp_path).check_connection()
    assert result.error_type == "PostgresSchemaMissingError" and result.schema_exists is False


def test_permission_missing(tmp_path):
    result = PostgresConnectionDiagnostics(settings(), connector_for([(1,), ("perceco", "perceco_engine"), (True,), (False,)]), tmp_path).check_connection()
    assert result.error_type == "PostgresPermissionDeniedError"


def test_migrations_pending(tmp_path):
    (tmp_path / "001_first.sql").write_text("SELECT 1")
    result = PostgresConnectionDiagnostics(settings(), connector_for([(1,), ("perceco", "perceco_engine"), (True,), (True,), (None,)]), tmp_path).check_connection()
    assert result.error_type == "PostgresMigrationPendingError" and result.migrations_ok is False


def test_unknown_error_is_sanitized(caplog):
    def fail(**kwargs): raise RuntimeError("postgresql://u:topsecret@host/db password=topsecret")
    with caplog.at_level(logging.ERROR): result = PostgresConnectionDiagnostics(settings(), fail).check_connection()
    assert not result.success and "topsecret" not in result.technical_message and "topsecret" not in caplog.text


def test_diagnostic_does_not_connect_without_password():
    called = False
    def connector(**kwargs):
        nonlocal called; called = True
    result = PostgresConnectionDiagnostics(PostgresSettings(), connector).check_connection()
    assert result.error_type == "MissingPostgresPasswordError" and not called
