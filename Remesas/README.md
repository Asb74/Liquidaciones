# Liquidaciones — Remesas

Aplicación de liquidaciones conectada exclusivamente a PostgreSQL mediante psycopg3.

## Configuración

Los parámetros no secretos y el backend están centralizados en `config.ini`. La contraseña
se obtiene únicamente de la variable de entorno `POSTGRES_PASSWORD`:

```bash
export POSTGRES_PASSWORD='…'
python app.py
```

La aplicación usa un pool compartido y consulta directamente los esquemas
`legacy_dbfruta`, `legacy_eepp`, `legacy_calidad`, `integracion` e `informes`. Toda la
persistencia propia (liquidaciones, líneas, snapshots, documentos, exportaciones,
rectificaciones, anulaciones, historial y maestros) reside en `liquidaciones`.

Al arrancar se aplican una sola vez las migraciones versionadas, protegidas por advisory
lock y registradas con checksum en `liquidaciones.schema_migrations`. Si PostgreSQL no está disponible,
se informa al usuario, se registra el error y la aplicación no continúa con datos parciales.

## Instalación y pruebas

```bash
python -m pip install -r requirements.txt
pytest -q tests
```

Los comandos administrativos no abren Tkinter: `python app.py db check`,
`python app.py db migrate` y `python app.py db migrate-data --source <copia.sqlite>`.
El inventario y el plan de corte están en `docs/postgresql_final_migration_inventory.md`
y `docs/postgresql_cutover_and_rollback.md`.
