# Liquidaciones - Remesas

Primera versión visual y funcional del módulo `Remesas`, ejecutable de forma independiente e integrable como `ttk.Frame` mediante `ui.remesas_frame.RemesasFrame`.

## Arranque en Windows

```bat
cd /d C:\Liquidaciones\Remesas
python app.py
```

## Configuración

Las rutas SQLite se leen desde `config.ini`. Las conexiones se abren con `sqlite3` en modo lectura y `PRAGMA query_only = ON`. `DBEEPPL.sqlite` se adjunta como esquema `eepp` para cruzar `PesosFres` con `eepp.DSocio`.

## Tablas usadas

- `PesosFres`: entregas por campaña, empresa, cultivo y periodo.
- `PesosFresCon`: se valida su existencia para fases posteriores.
- `PagosCIT`: lectura de remesas existentes y precios `P0` a `P11`, `PDESTRIO`, `PDMESA`, `PPODRIDO`.
- `DLiquidaciones`: se valida su existencia, sin escritura.
- `eepp.DEEPP`: variedades reales por contexto.
- `eepp.DSocio`: nombre de socio por `IdSocio`.

## Limitaciones de fase 1

No calcula importes económicos, no guarda liquidaciones, no modifica SQLite, no toca Access y no genera PDF final. Los botones de cálculo, guardado, anulación y PDF quedan deshabilitados.

## Flujo de generación PDF masiva

La herramienta **Generación masiva de documentos** se abre desde el menú principal y
consulta `generated_documents` mediante `PdfMergeService.list_available_documents`.
Antes de este cambio, el botón combinado validaba los ficheros del disco y los unía;
la regeneración individual era opcional y utilizaba snapshots persistidos.

El botón **Generar PDF combinado** trabaja ahora obligatoriamente en modo
`REBUILD_AND_VALIDATE`: valida los snapshots seleccionados, reconstruye y consolida
sus magnitudes por campaña, empresa, cultivo, grupo varietal, tipo y categoría,
consulta una única superficie por socio/grupo y ejecuta `GroupBenchmarkService` con
el identificador padre de la ejecución masiva. Sólo después regenera cada documento
desde su snapshot y combina los PDF recién generados. La operación independiente de
`PdfMergeService.merge_documents` continúa siendo el modo técnico
`MERGE_EXISTING_PDFS`, pero no es el modo usado por ese botón.

La consolidación y sus incidencias se escriben en
`logs/mass_pdf_benchmark_audit.log`; el detalle de las consultas de parcelas se
mantiene en `logs/group_benchmark_surface_audit.log`. Los bloques
`MassPdfFlowTrace` del log general permiten seguir reconstrucción, generación y
combinación.
