# Inventario del esquema SQLite de liquidaciones

## Alcance y evidencia

La ruta configurada inicialmente era `C:\Liquidaciones\datos\liquidaciones.sqlite`. El archivo no existe en este entorno y **no se ha creado, abierto ni modificado**. Por ello no se inventan recuentos, nulos, duplicados, extremos, triggers o vistas. Antes del corte debe ejecutarse, sobre una copia autorizada y en sólo lectura:

```bash
python -m db_tools db inventory --source 'C:\Liquidaciones\datos\liquidaciones.sqlite' --output sqlite_inventory.json
```

El comando abre `file:...?mode=ro`, consulta `sqlite_master`, `table_info`, `foreign_key_list`, `index_list` e `index_info`, y calcula conteos y SHA-256. El modelo conocido procede de las 13 migraciones históricas versionadas en `data/persistence/migrations.py`; las tablas son: `liquidation_prefixes`, `liquidation_sequences`, `split_rules`, `split_rule_recipients`, `liquidation_batches`, `liquidaciones`, `liquidation_audit`, `legacy_imports`, `generated_documents`, `exported_draft_documents`, `accounting_exports`, `accounting_export_items` y `liquidation_document_snapshots`.

## Estado de validación

**PENDIENTE DE EJECUCIÓN EN EL EQUIPO DE CORTE.** La ausencia del origen impide afirmar conteos o validación económica. El JSON generado será la evidencia de esquema real y prevalecerá ante este inventario derivado del código.
