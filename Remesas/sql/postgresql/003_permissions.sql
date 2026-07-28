GRANT USAGE ON SCHEMA liquidaciones TO liquidaciones_app;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA liquidaciones TO liquidaciones_app;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA liquidaciones TO liquidaciones_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA liquidaciones GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO liquidaciones_app;
GRANT USAGE ON SCHEMA legacy_dbfruta,legacy_pedidos,legacy_loteado,legacy_calidad,legacy_eepp,legacy_rrhh,integracion,informes TO liquidaciones_app;
GRANT SELECT ON ALL TABLES IN SCHEMA legacy_dbfruta,legacy_pedidos,legacy_loteado,legacy_calidad,legacy_eepp,legacy_rrhh,integracion,informes TO liquidaciones_app;
