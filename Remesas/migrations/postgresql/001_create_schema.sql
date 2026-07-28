CREATE SCHEMA IF NOT EXISTS liquidaciones;
CREATE TABLE IF NOT EXISTS liquidaciones.schema_migrations(version bigint PRIMARY KEY,name text NOT NULL,checksum varchar(64) NOT NULL,applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,application_version text);
