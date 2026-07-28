from __future__ import annotations
import hashlib, re, time
from dataclasses import dataclass
from pathlib import Path

LOCK_ID=618_517_391_204
@dataclass(frozen=True)
class Migration:
    version:int; name:str; checksum:str; sql:str

def discover(directory: Path):
    result=[]
    for path in sorted(directory.glob("[0-9][0-9][0-9]_*.sql")):
        m=re.fullmatch(r"(\d{3})_(.+)\.sql",path.name)
        raw=path.read_bytes(); result.append(Migration(int(m.group(1)),m.group(2),hashlib.sha256(raw).hexdigest(),raw.decode()))
    if len({m.version for m in result})!=len(result): raise RuntimeError("Versiones de migración duplicadas")
    return result

def migrate(connection, directory:Path, application_version=None):
    connection.execute("SELECT pg_advisory_lock(%s)",(LOCK_ID,))
    applied=[]
    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS liquidaciones")
        connection.execute("""CREATE TABLE IF NOT EXISTS liquidaciones.schema_migrations(
          version bigint PRIMARY KEY,name text NOT NULL,checksum varchar(64) NOT NULL,
          applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,execution_ms bigint NOT NULL DEFAULT 0,
          application_version text)""")
        connection.execute("ALTER TABLE liquidaciones.schema_migrations ADD COLUMN IF NOT EXISTS execution_ms bigint NOT NULL DEFAULT 0")
        known={r[0]:(r[1],r[2]) for r in connection.execute("SELECT version,name,checksum FROM liquidaciones.schema_migrations")}
        for item in discover(directory):
            if item.version in known:
                if known[item.version] != (item.name,item.checksum): raise RuntimeError(f"Checksum modificado en migración {item.version:03d}")
                continue
            started=time.monotonic()
            with connection.transaction():
                connection.execute(item.sql)
                elapsed=max(0,round((time.monotonic()-started)*1000))
                connection.execute("INSERT INTO liquidaciones.schema_migrations(version,name,checksum,execution_ms,application_version) VALUES(%s,%s,%s,%s,%s)",(item.version,item.name,item.checksum,elapsed,application_version))
            applied.append(item.version)
        return applied
    finally:
        connection.execute("SELECT pg_advisory_unlock(%s)",(LOCK_ID,))
