# Resolución de comparativas durante la unificación de PDF

## Investigación del fallo

- **archivo:** `Remesas/services/individual_pdf_refresh_service.py`
- **función:** `IndividualPdfRefreshService.refresh_documents`
- **mensaje anterior:** `La auditoría no produjo una comparativa única para el documento.`
- **condición que disparaba el error:** `len(matches) != 1`; por tanto, tanto cero
  como más de una coincidencia cancelaban la regeneración individual y, después, el
  combinado.
- **número de comparativas encontradas:** no se registraba. La resolución actual lo
  escribe como `candidate_count` junto con `UNIQUE`, `NOT_FOUND` o `AMBIGUOUS`.
- **clave anterior:** `(member_id, group_label, campaign, company)` extraída de una
  clave de cálculo de siete componentes. `crop`, tipo y categoría estaban presentes
  en la clave pero se ignoraban; documento, liquidación, lote, snapshot y ejecución
  no formaban parte de ella.

La causa era una correlación parcial, no el cálculo estadístico. Además, una clave
sin ejecución permitía que una búsqueda reconstruida desde un log completo mezclara
bloques repetidos. Varias coincidencias del mismo socio son legítimas cuando existen
grupos, documentos, liquidaciones o ejecuciones diferentes.

## Corrección

La auditoría masiva crea un `generation_run_id` nuevo y construye una clave por
documento con: socio, campaña, empresa, cultivo, grupo, subgrupo, variedad,
liquidaciones, documento, lote, snapshot, ejecución, tipo y categoría. La resolución
descarta primero cualquier candidato de otra ejecución y aplica solamente criterios
disponibles, dando prioridad a identificadores documentales.

Cada intento queda en `Remesas/logs/pdf_comparison_resolution.log`. Si quedan varias
candidatas se escribe un bloque `PdfComparisonCandidate` para cada una, incluido el
valor **Usted** y sus extremos estadísticos. Esto mantiene la trazabilidad específica
del socio 1623 sin consultar ni seleccionar silenciosamente una ejecución anterior.

`NOT_FOUND` y `AMBIGUOUS` son estados diagnósticos: se conserva la comparativa del
snapshot (si existe) y el PDF continúa. Sólo `UNIQUE` sustituye el valor persistido
por el resultado de la auditoría actual.
