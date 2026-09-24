-- @desc: REGRESSÃO — join pushdown FT × FT num plano genérico cacheado.
-- Antes, o fdw_private guardava um ponteiro cru para o JoinScanState, que o
-- EndForeignScan liberava: do 2º EXECUTE em diante o conn_str lido era lixo
-- ("Data source name not found") e às vezes o backend levava SIGSEGV.
-- @expect: ok

DEALLOCATE ALL;
SET plan_cache_mode = force_generic_plan;

PREPARE join_generic AS
SELECT e.nome, ca.nome AS cargo
  FROM dominio.foempregados_slow e
  JOIN dominio.focargos_slow ca ON ca.i_cargos = e.i_cargos
                                AND ca.codi_emp = e.codi_emp
 WHERE e.codi_emp = 51800
 LIMIT 5;

EXPLAIN (COSTS OFF) EXECUTE join_generic;
EXECUTE join_generic;
EXECUTE join_generic;
EXECUTE join_generic;
EXECUTE join_generic;
EXECUTE join_generic;
EXECUTE join_generic;
EXECUTE join_generic;

DEALLOCATE join_generic;
RESET plan_cache_mode;
