-- Aplicar sólo después de que el comando validate confirme que no hay duplicados históricos.
DO $$ BEGIN IF EXISTS(SELECT 1 FROM liquidaciones.liquidation_batches WHERE status IN('ACTIVE','PARTIAL') AND operation_type IN('ORIGINAL','REPLACEMENT') GROUP BY campaign,company,crop,remesa_id HAVING count(*)>1) THEN RAISE EXCEPTION 'ACTIVE_SCOPE_CONFLICT_DETECTED'; END IF; END $$;
CREATE UNIQUE INDEX ux_liquidation_active_scope ON liquidaciones.liquidation_batches(campaign,company,crop,remesa_id) WHERE status IN('ACTIVE','PARTIAL') AND operation_type IN('ORIGINAL','REPLACEMENT');
