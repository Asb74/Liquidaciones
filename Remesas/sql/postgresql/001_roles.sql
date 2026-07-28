-- Ejecutar como administrador. No contiene ni solicita contraseñas.
DO $$ BEGIN CREATE ROLE liquidaciones_app NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
