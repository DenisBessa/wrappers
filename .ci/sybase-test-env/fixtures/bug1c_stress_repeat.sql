-- @desc: Stress test pra cached_conn — repete a mesma query parametrizada 3×
-- contra a FT. Cada execução faz N iterações de Foreign Scan parametrizado
-- (uma por linha de nfe). Usamos array_agg pra que o planner NÃO descarte
-- a subquery (COUNT(*) sozinho seria otimizado pra fora).
--
-- Se cached_conn ficar zumbi entre runs, alguma das repetições deve cuspir
-- erro ODBC ou crash do backend.
-- @expect: ok

WITH r AS MATERIALIZED (
    SELECT n.access_key,
           (SELECT s.chave_nfe_sai FROM dominio.efsaidas_slow s
              WHERE s.chave_nfe_sai = n.access_key LIMIT 1) AS sai_key
      FROM nfe n WHERE n.customer_id = 'cust-001'
)
SELECT 'run1' AS run, COUNT(sai_key) AS matched FROM r;

WITH r AS MATERIALIZED (
    SELECT n.access_key,
           (SELECT s.chave_nfe_sai FROM dominio.efsaidas_slow s
              WHERE s.chave_nfe_sai = n.access_key LIMIT 1) AS sai_key
      FROM nfe n WHERE n.customer_id = 'cust-001'
)
SELECT 'run2' AS run, COUNT(sai_key) AS matched FROM r;

WITH r AS MATERIALIZED (
    SELECT n.access_key,
           (SELECT s.chave_nfe_sai FROM dominio.efsaidas_slow s
              WHERE s.chave_nfe_sai = n.access_key LIMIT 1) AS sai_key
      FROM nfe n WHERE n.customer_id = 'cust-001'
)
SELECT 'run3' AS run, COUNT(sai_key) AS matched FROM r;
