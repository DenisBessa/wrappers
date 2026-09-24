-- Pacote de coleta a ser executado contra o Postgres de produção que
-- reproduz os bugs. Output todo concentrado num psql session — copie tudo e
-- mande de volta.
--
-- COMO RODAR (no host que tem acesso ao Postgres de produção):
--   psql -h <prod-host> -U <user> -d <db> -X -f collect-prod-explain.sql \
--        2>&1 | tee prod-explain-$(date +%Y%m%d-%H%M).log
--
-- O script NÃO executa as queries problemáticas em si — só usa EXPLAIN
-- (sem ANALYZE) que NÃO toca o Sybase, só analisa o plano. Seguro.
--
-- ATENÇÃO: ajuste $1/$2/$3 inline se o seu cliente não suporta -v vars.
-- A query do bug #1 usa customer_id real e access_keys reais. Use um
-- customer_id de teste OU substitua os literais por valores conhecidos.

\echo
\echo '=========================================================================='
\echo '1. AMBIENTE'
\echo '=========================================================================='
SELECT version();
SELECT current_setting('server_version_num') AS pg_num;
SELECT name, setting FROM pg_settings WHERE name IN (
    'shared_preload_libraries','jit','jit_above_cost','jit_optimize_above_cost',
    'jit_inline_above_cost','random_page_cost','effective_cache_size','work_mem',
    'enable_partitionwise_join','enable_partitionwise_aggregate',
    'enable_nestloop','enable_hashjoin','enable_mergejoin',
    'from_collapse_limit','join_collapse_limit',
    'plan_cache_mode'
) ORDER BY name;

\echo
\echo '=========================================================================='
\echo '2. EXTENSAO E FT'
\echo '=========================================================================='
SELECT extname, extversion FROM pg_extension WHERE extname = 'wrappers';
SELECT srvname, srvoptions FROM pg_foreign_server WHERE srvname IN (
    SELECT DISTINCT srvname FROM pg_foreign_server fs
    JOIN pg_foreign_table ft ON ft.ftserver = fs.oid
    JOIN pg_class c ON c.oid = ft.ftrelid
    WHERE c.relname IN ('efsaidas','efentradas','geempre','foempregados')
);

\echo
\echo '=========================================================================='
\echo '3. ESTATISTICAS DAS TABELAS (locais)'
\echo '=========================================================================='
SELECT
    schemaname, relname, n_live_tup, n_dead_tup, last_analyze, last_autoanalyze
FROM pg_stat_user_tables
WHERE relname IN ('nfe','customers_tax_numbers')
ORDER BY schemaname, relname;

SELECT
    schemaname, tablename, attname, n_distinct, correlation, null_frac
FROM pg_stats
WHERE tablename IN ('nfe','customers_tax_numbers')
  AND attname IN ('customer_id','access_key','tax_number','type','date')
ORDER BY tablename, attname;

\echo
\echo '=========================================================================='
\echo '4. EXPLAIN do Bug #2 — IN (SELECT ...) com correlation'
\echo '=========================================================================='
\echo 'Substitua :cust pelo customer_id real que você sabe que dispara o erro.'
\set cust '\'0fbd5897-211e-49cf-9eb0-60375d0616e0\''
EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT
    n.access_key,
    (
        SELECT row_to_json(t.*) FROM (
            SELECT s.chave_nfe_sai, s.situacao_sai
              FROM dominio.efsaidas s
             WHERE s.chave_nfe_sai = n.access_key
               AND s.codi_emp IN (
                   SELECT g.codi_emp
                     FROM dominio.geempre g
                     JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
                    WHERE c.customer_id = n.customer_id
                      AND c.type = 'CNPJ'
               )
             LIMIT 1
        ) t
    ) AS erp_data_saidas
FROM nfe n
WHERE n.customer_id = :cust
  AND n.date BETWEEN '2025-01-01' AND '2025-12-31'
  AND n.type = 'Saída'
ORDER BY n.date DESC
LIMIT 20;

\echo
\echo '=========================================================================='
\echo '5. EXPLAIN do Bug #1 — JOIN amplo correlated'
\echo '=========================================================================='
EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT
    n.access_key,
    (
        SELECT row_to_json(t.*) FROM (
            SELECT s.chave_nfe_sai, s.situacao_sai
              FROM dominio.efsaidas s
              JOIN dominio.geempre g ON g.codi_emp = s.codi_emp
              JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
             WHERE c.customer_id = n.customer_id
               AND c.type = 'CNPJ'
               AND s.chave_nfe_sai = n.access_key
             LIMIT 1
        ) t
    ) AS erp_data_saidas
FROM nfe n
WHERE n.customer_id = :cust
  AND n.date BETWEEN '2025-01-01' AND '2025-12-31'
  AND n.type = 'Saída'
ORDER BY n.date DESC;

\echo
\echo '=========================================================================='
\echo '6. EXPLAIN com (VERBOSE, FORMAT JSON) das mesmas duas queries'
\echo '=========================================================================='
\echo 'JSON é o que vou comparar lado-a-lado pra encontrar diferença de caminho.'

EXPLAIN (VERBOSE, FORMAT JSON)
SELECT
    n.access_key,
    (
        SELECT row_to_json(t.*) FROM (
            SELECT s.chave_nfe_sai, s.situacao_sai
              FROM dominio.efsaidas s
             WHERE s.chave_nfe_sai = n.access_key
               AND s.codi_emp IN (
                   SELECT g.codi_emp
                     FROM dominio.geempre g
                     JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
                    WHERE c.customer_id = n.customer_id
                      AND c.type = 'CNPJ'
               )
             LIMIT 1
        ) t
    ) AS erp_data_saidas
FROM nfe n
WHERE n.customer_id = :cust
  AND n.date BETWEEN '2025-01-01' AND '2025-12-31'
  AND n.type = 'Saída'
ORDER BY n.date DESC
LIMIT 20;

\echo
\echo '=========================================================================='
\echo '7. Cardinalidade real (NÃO faz EXPLAIN ANALYZE pra não disparar o bug)'
\echo '=========================================================================='
SELECT 'nfe rows for customer' AS metric, COUNT(*) AS value
  FROM nfe WHERE customer_id = :cust;
SELECT 'tax_numbers for customer' AS metric, COUNT(*) AS value
  FROM customers_tax_numbers WHERE customer_id = :cust AND type = 'CNPJ';
SELECT 'geempre rows matching' AS metric, COUNT(*) AS value
  FROM dominio.geempre g
  JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
  WHERE c.customer_id = :cust AND c.type = 'CNPJ';

\echo
\echo '=========================================================================='
\echo 'Fim. Mande este log inteiro de volta.'
\echo '=========================================================================='
