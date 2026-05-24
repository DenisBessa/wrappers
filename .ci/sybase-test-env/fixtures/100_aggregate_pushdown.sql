-- @desc: Reproduz a CTE materializada que OOM-killou supabase-db em prod
-- (PIDs 3841, 4582, 5343, 834 em 2026-05-24). Sem aggregate pushdown o
-- Sybase FDW puxava todas as linhas casando o WHERE pra dentro de
-- scan_result: Vec<FetchedRow> e o postgres aggregava localmente —
-- pra efentradaspag com ~600k linhas isso explode a memória do backend.
-- Após o pushdown a query do GROUP BY roda no Sybase e só as linhas
-- agregadas atravessam o ODBC.
--
-- O EXPLAIN VERBOSE deve mostrar:
--   Foreign Scan ... with Wrappers: aggregates = [...] e group_by = [...]
-- (sem rescanear o FT em loop e sem HashAggregate local).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

EXPLAIN (VERBOSE)
WITH pagamentos AS MATERIALIZED (
    SELECT codi_emp, codi_sai, SUM(valor_sai) AS valor_pago
      FROM dominio.efsaidas_slow
     WHERE codi_emp = 51800
     GROUP BY codi_emp, codi_sai
)
SELECT COUNT(*) FROM pagamentos;

WITH pagamentos AS MATERIALIZED (
    SELECT codi_emp, codi_sai, SUM(valor_sai) AS valor_pago
      FROM dominio.efsaidas_slow
     WHERE codi_emp = 51800
     GROUP BY codi_emp, codi_sai
)
SELECT COUNT(*) FROM pagamentos;

COMMIT;
