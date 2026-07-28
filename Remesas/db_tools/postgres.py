from __future__ import annotations
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

class PostgresConfigurationError(RuntimeError): pass

@dataclass(frozen=True)
class PostgresSettings:
    host: str = "192.168.1.3"
    port: int = 5432
    database: str = "perceco"
    user: str = "perceco_engine"
    sslmode: str = "prefer"
    connect_timeout: int = 5
    application_name: str = "liquidaciones-migration"
    @property
    def password(self) -> str:
        value=os.environ.get("POSTGRES_PASSWORD")
        if not value: raise PostgresConfigurationError("POSTGRES_PASSWORD es obligatoria para conectar a PostgreSQL.")
        return value
    def kwargs(self):
        return {"host":self.host,"port":self.port,"dbname":self.database,"user":self.user,"password":self.password,
                "sslmode":self.sslmode,"connect_timeout":self.connect_timeout,"application_name":self.application_name}
    def safe_target(self): return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"

class PostgresConnectionFactory:
    def __init__(self, settings=None): self.settings=settings or PostgresSettings()
    def connect(self):
        import psycopg
        return psycopg.connect(**self.settings.kwargs())
    @contextmanager
    def transaction(self):
        with self.connect() as connection:
            with connection.transaction(): yield connection
