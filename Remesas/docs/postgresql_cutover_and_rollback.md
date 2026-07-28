# Corte a PostgreSQL y rollback operativo

## Preparación

1. Congelar escrituras SQLite y anotar su `LastWriteTime`, último `id_liq` y SHA-256.
2. Crear backup sin contraseña en el comando: `pg_dump --host=192.168.1.3 --username=perceco_engine --dbname=perceco --schema=liquidaciones --format=custom --file=liquidaciones_pre_cutover.dump`.
3. Aplicar migraciones con `python -m db_tools db migrate` y migrar una copia de sólo lectura con `python -m db_tools db migrate-data --source 'C:\Liquidaciones\datos\liquidaciones.sqlite' --target postgresql --resume`.
4. Ejecutar `python -m db_tools db validate --source 'C:\Liquidaciones\datos\liquidaciones.sqlite'`; no cortar si no indica `VALIDATION PASSED`.
5. Registrar fecha/hora, último registro SQLite, primer registro PostgreSQL, conteos y responsables.

## Activación

Distribuir `backend=postgresql`, configurar `POSTGRES_PASSWORD` fuera de archivos y comprobar `python -m db_tools db check`. Desde ese instante SQLite queda congelado: nunca dual-write.

## Fallo y rollback

Cerrar todos los clientes para impedir escrituras divergentes. Si aún no hubo escrituras PostgreSQL, volver temporalmente al binario/commit anterior y reabrir exclusivamente el SQLite congelado. Si ya hubo escrituras PostgreSQL, **no reactivar SQLite**: restaurar PostgreSQL en pruebas (`pg_restore --clean --if-exists --dbname=perceco_test liquidaciones_pre_cutover.dump`), diagnosticar, corregir y repetir validación. Toda vuelta al código anterior requiere una decisión operativa y una única fuente de verdad.

## Lista manual

- Antes: variable presente, servidor accesible, migraciones completas y pool disponible.
- Apertura: indicador PostgreSQL conectado; ninguna preparación/copia/fallback local.
- Liquidación: lote, líneas, snapshot, auditoría y documentos registrados en un commit.
- PostgreSQL: consultar las tablas `liquidaciones.*` por `created_at DESC`.
- SQLite: confirmar que `LastWriteTime` y SHA-256 no cambiaron.
