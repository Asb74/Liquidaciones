# Liquidaciones — Remesas

Aplicación de liquidaciones conectada exclusivamente a PostgreSQL mediante psycopg3.

## Configuración

Los parámetros no secretos están centralizados en `config/postgresql.toml`. La contraseña
se obtiene únicamente de la variable de entorno `POSTGRES_PASSWORD`:

```bash
export POSTGRES_PASSWORD='…'
python app.py
```

La aplicación usa un pool compartido y consulta directamente los esquemas
`legacy_dbfruta`, `legacy_eepp`, `legacy_calidad`, `integracion` e `informes`. Toda la
persistencia propia (liquidaciones, líneas, snapshots, documentos, exportaciones,
rectificaciones, anulaciones, historial y maestros) reside en `liquidaciones`.

Al arrancar se crea el esquema propio y se aplican automáticamente las migraciones
pendientes registradas en `liquidaciones.schema_version`. Si PostgreSQL no está disponible,
se informa al usuario, se registra el error y la aplicación no continúa con datos parciales.

## Instalación y pruebas

```bash
python -m pip install -r requirements.txt
pytest -q tests
```

El inventario previo al corte está en `docs/postgresql_phase_1_access_report.md`.
