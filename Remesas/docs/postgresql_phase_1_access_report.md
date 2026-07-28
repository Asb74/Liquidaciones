# Fase 1 — informe de accesos a datos previo al corte definitivo

Este informe conserva el inventario levantado antes de sustituir la capa de datos. El
detalle de tablas, columnas, incompatibilidades y consultas históricas se encuentra en
[`postgresql_phase_0_inventory.md`](postgresql_phase_0_inventory.md). La tabla siguiente
resume **archivo, clase/método, cometido, tabla/consulta y dirección** de cada familia.

| Archivo | Clase / método | Qué hace | Tablas y forma de consulta | Tipo |
|---|---|---|---|---|
| `data/metadata_repository.py` | `MetadataRepository.*` | Carga campañas, empresas, cultivos, variedades y metadatos | `PesosFres`, `PesosFresCon`, `DEEPP`; `SELECT DISTINCT`, catálogo de columnas | Lectura |
| `data/deliveries_repository.py` | `DeliveriesRepository.fetch` | Recupera entregas y sus costes, calcula sólo estadísticas de consulta | `PesosFres` + `DSocio`; `SELECT`, `COUNT`, `SUM`, `LEFT JOIN` | Lectura |
| `data/remesas_repository.py` | `RemesasRepository.*` | Lista y recupera remesas | `PagosCIT`; `SELECT` filtrado por contexto e identificador | Lectura |
| `data/variety_repository.py` | `VarietyRepository.*` | Resuelve variedades, grupos y artículos | `MVariedad`, `PesosFres`; `SELECT DISTINCT` | Lectura |
| `data/globalgap_repository.py` | `GlobalGapRepository.*` | Obtiene certificados, niveles e índices de bonificación | `DEEPP`, `MNivelGlobal`, `BonGlobal`; `SELECT` | Lectura |
| `data/quality_repository.py` | `QualityRepository.*` | Recupera bonificaciones/penalizaciones de calidad | `BonCalidad`; `SELECT` | Lectura |
| `data/hectare_repository.py` | `HectareRepository.*` | Recupera superficies, parcelas, boletas y entregas aplicables | `DSocio`, `DParcela`, `Parcelas`, `DEEPP`, `PesosFres`; `SELECT`, agregaciones | Lectura |
| `data/fiscal_regime_repository.py` | `FiscalRegimeRepository.*` | Resuelve régimen fiscal de socios | `DSocio`, `MRegimenFiscal`; `SELECT` | Lectura |
| `data/group_benchmark_repository.py` | `GroupBenchmarkRepository.*` | Obtiene comparativas históricas | `DLiquidaciones`, `MVariedad`; `SELECT`, agrupación | Lectura |
| `data/postgres_legacy_repository.py` | `PostgresLegacyRepository.*` | IDs históricos, socio, autofactura, artículo y divisiones | `DLiquidaciones`, `DSocio`, `MVariedad`, `DDividirLiq`; `SELECT` | Lectura |
| `data/persistence/liquidation_repository.py` | `LiquidationRepository.*` | Batches, líneas, snapshots, documentos, CSV, anulaciones, historial y comparativas | tablas `liquidaciones.*`; `SELECT`, `INSERT`, `UPDATE`, UPSERT | Lectura/escritura |
| `data/persistence/master_repository.py` | `LiquidationMasterRepository.*` | Prefijos, secuencias y reglas de reparto | `liquidation_prefixes`, `liquidation_sequences`, `split_rules`, `split_rule_recipients` | Lectura/escritura |
| `data/persistence/migrations.py` | `migrate` | Crea y versiona la persistencia propia | `liquidaciones.schema_version` y DDL v1–v13 | Escritura DDL |
| `liquidacion_2026/extractor_postgres.py` | `PostgresExtractor.*` | Obtiene entradas de la liquidación KAKIS | `PesosFres`, `CorrespondenciasCalibres`, `DEEPP`, `MNivelGlobal`, `BonGlobal`; `SELECT` | Lectura |

## Resultado del corte

Las tablas legacy se consultan ahora en `legacy_dbfruta`, `legacy_eepp` y
`legacy_calidad`; las consultas propias usan `liquidaciones`. No hay importación desde
Access, réplica local, fichero de persistencia ni proceso de sincronización. Los
repositorios reciben `IDataRepository`; `PostgresRepository` es la única implementación.

