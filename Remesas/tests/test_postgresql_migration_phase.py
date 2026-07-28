import json,sqlite3
from decimal import Decimal
from pathlib import Path
import pytest
from db_tools.postgres import PostgresSettings,PostgresConfigurationError
from db_tools.migrations import discover
from db_tools.sqlite_migrator import fingerprint,inspect_sqlite

ROOT=Path(__file__).resolve().parents[1]
def test_password_is_required_and_not_in_error(monkeypatch):
 monkeypatch.delenv("POSTGRES_PASSWORD",raising=False)
 with pytest.raises(PostgresConfigurationError) as error: PostgresSettings().kwargs()
 assert "password=" not in str(error.value).lower()
def test_non_secret_defaults(monkeypatch):
 monkeypatch.setenv("POSTGRES_PASSWORD","super-secret")
 s=PostgresSettings(); assert (s.host,s.port,s.database,s.user,s.sslmode)==("192.168.1.3",5432,"perceco","perceco_engine","prefer")
 assert "super-secret" not in s.safe_target()
def test_versioned_migrations_and_qualified_tables():
 migrations=discover(ROOT/"migrations"/"postgresql"); assert [m.version for m in migrations]==list(range(1,8))
 sql="\n".join(m.sql for m in migrations); assert "liquidaciones.liquidation_batches" in sql; assert "legacy_dbfruta." not in sql; assert "double precision" not in sql.lower()
def test_inventory_and_fingerprint_are_deterministic(tmp_path):
 db=tmp_path/"liquidaciones.sqlite"
 with sqlite3.connect(db) as c:c.executescript("CREATE TABLE parent(id INTEGER PRIMARY KEY, amount TEXT); CREATE TABLE child(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES parent(id)); CREATE INDEX ix_child_parent ON child(parent_id); INSERT INTO parent VALUES(1,'1.20');")
 report=inspect_sqlite(db); assert {x["name"] for x in report["tables"]}=={"parent","child"}; assert report["size"]>0
 rows=[{"id":1,"amount":Decimal("1.20")}]; assert fingerprint(rows,["id","amount"])==fingerprint(rows,["id","amount"])
def test_sqlite_remains_default_backend():
 text=(ROOT/"config.ini").read_text(encoding="utf-8"); assert "backend = sqlite" in text
 assert (ROOT/"data"/"persistence"/"database.py").read_text(encoding="utf-8").find("import sqlite3")>=0
