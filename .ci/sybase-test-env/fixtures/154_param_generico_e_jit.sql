-- @desc: KOROK-224 (05/11/2026). OOM do supabase-db: (1) cada linha lida pelo
-- Foreign Scan parametrizado refazia o ExecInitExpr dos parâmetros, e com JIT
-- cada um virava uma função compilada (173 mil funções e ~2 GB por backend);
-- (2) no plano genérico (o Orchid manda `$n`), `IN ($1, $2)` e `= ANY($1)` não
-- chegavam ao Sybase. Cada consulta é conferida contra a mesma consulta com
-- literais, e o plano precisa mostrar o array no qual empurrado.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '120s';
SET LOCAL plan_cache_mode = force_generic_plan;

CREATE FUNCTION pg_temp.confere(ok boolean, msg text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF ok IS NOT TRUE THEN RAISE EXCEPTION 'falhou: %', msg; END IF;
END $$;

CREATE FUNCTION pg_temp.plano(sql text, opcoes text DEFAULT 'ANALYZE, VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF')
RETURNS text LANGUAGE plpgsql AS $$
DECLARE l text; r text := '';
BEGIN
    FOR l IN EXECUTE format('EXPLAIN (%s) %s', opcoes, sql) LOOP
        r := r || l || E'\n';
    END LOOP;
    RETURN r;
END $$;

-- Esperados, com literais (caminho coberto pelas fixtures 010/144).
SELECT count(*) AS e_in FROM dominio.efsaidas_slow WHERE codi_emp IN (51800, 51801) \gset
SELECT count(*) AS e_um FROM dominio.efsaidas_slow WHERE codi_emp = 51800 \gset
SELECT count(*) AS e_fora FROM dominio.efsaidas_slow WHERE codi_emp <> 51800 \gset
SELECT count(*) AS e_todas FROM dominio.efsaidas_slow \gset
SELECT pg_temp.confere(:e_um > 0 AND :e_fora > 0, 'base sem linhas de 51800');

-- OFFSET 0: count(*) local, o Foreign Scan é um scan comum.
PREPARE p_in(int, int) AS SELECT count(*) FROM (
    SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp IN ($1, $2) OFFSET 0) s;
PREPARE p_any(int[]) AS SELECT count(*) FROM (
    SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = ANY($1) OFFSET 0) s;
PREPARE p_not_all(int[]) AS SELECT count(*) FROM (
    SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp <> ALL($1) OFFSET 0) s;
PREPARE p_not_any(int[]) AS SELECT count(*) FROM (
    SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp <> ANY($1) OFFSET 0) s;
-- Sem OFFSET: agregação empurrada com o array do bind.
PREPARE p_agg(int, int) AS
    SELECT count(*) FROM dominio.efsaidas_slow WHERE codi_emp IN ($1, $2);

-- 1. IN ($1, $2)
SELECT pg_temp.confere(
    pg_temp.plano('EXECUTE p_in(51800, 51801)') LIKE '%value: Array([I32(51800), I32(51801)])%',
    'IN ($1, $2) não empurrado');
EXECUTE p_in(51800, 51801) \gset r_
SELECT pg_temp.confere(:r_count = :e_in, 'IN ($1, $2) com total diferente');

-- 2. = ANY($1), inclusive com NULL no array, array vazio e NULL
SELECT pg_temp.confere(
    pg_temp.plano('EXECUTE p_any(''{51800,51801}'')') LIKE '%value: Array([I32(51800), I32(51801)])%',
    '= ANY($1) não empurrado');
EXECUTE p_any('{51800,51801}') \gset r_
SELECT pg_temp.confere(:r_count = :e_in, '= ANY($1) com total diferente');
EXECUTE p_any('{51800,NULL}') \gset r_
SELECT pg_temp.confere(:r_count = :e_um, '= ANY($1) com NULL no array');
EXECUTE p_any('{}') \gset r_
SELECT pg_temp.confere(:r_count = 0, '= ANY($1) com array vazio');
EXECUTE p_any(NULL) \gset r_
SELECT pg_temp.confere(:r_count = 0, '= ANY($1) com array NULL');

-- 3. <> ALL e <> ANY (NOT IN só vale para ALL)
EXECUTE p_not_all('{51800}') \gset r_
SELECT pg_temp.confere(:r_count = :e_fora, '<> ALL($1) com total diferente');
EXECUTE p_not_all('{51800,NULL}') \gset r_
SELECT pg_temp.confere(:r_count = 0, '<> ALL($1) com NULL no array');
EXECUTE p_not_all('{}') \gset r_
SELECT pg_temp.confere(:r_count = :e_todas, '<> ALL($1) com array vazio');
EXECUTE p_not_any('{51800,51801}') \gset r_
SELECT pg_temp.confere(:r_count = :e_todas, '<> ANY($1) virou NOT IN');

-- 4. Agregação empurrada com o array do bind
EXECUTE p_agg(51800, 51801) \gset r_
SELECT pg_temp.confere(:r_count = :e_in, 'count(*) com IN ($1, $2)');
EXECUTE p_agg(51800, 51801) \gset r_
EXECUTE p_agg(51800, 51801) \gset r_
SELECT pg_temp.confere(:r_count = :e_in, 'count(*) com IN ($1, $2) no 3º EXECUTE');

-- 5. = ANY(array(subconsulta)): InitPlan avaliado no primeiro iterate
CREATE TEMP TABLE alvo AS SELECT 51800 AS codi_emp;
SELECT pg_temp.confere(
    pg_temp.plano('SELECT count(*) FROM (SELECT codi_sai FROM dominio.efsaidas_slow
                    WHERE codi_emp = ANY(array(SELECT codi_emp FROM alvo)) OFFSET 0) s')
      LIKE '%value: Array([I32(51800)])%',
    '= ANY(array(subconsulta)) não empurrado');
SELECT count(*) AS r_count FROM (SELECT codi_sai FROM dominio.efsaidas_slow
  WHERE codi_emp = ANY(array(SELECT codi_emp FROM alvo)) OFFSET 0) s \gset
SELECT pg_temp.confere(:r_count = :e_um, '= ANY(array(subconsulta)) com total diferente');

-- 6. numeric: IN ($1, $2, $3) e LATERAL com expressão sobre a coluna externa,
-- avaliada no rescan e lida no iterate seguinte.
SELECT count(*) AS e_num FROM dominio.efsaidas_slow WHERE valor_sai IN (101.00, 102.00, 250.00) \gset
SELECT pg_temp.confere(:e_num = 3, 'base sem os valores 101, 102 e 250');
PREPARE p_num(numeric, numeric, numeric) AS SELECT count(*) FROM (
    SELECT codi_sai FROM dominio.efsaidas_slow WHERE valor_sai IN ($1, $2, $3) OFFSET 0) s;
SELECT pg_temp.confere(
    pg_temp.plano('EXECUTE p_num(101.00, 102.00, 250.00)') LIKE '%value: Array([Numeric(%',
    'numeric IN ($1, $2, $3) não empurrado');
EXECUTE p_num(101.00, 102.00, 250.00) \gset r_
EXECUTE p_num(101.00, 102.00, 250.00) \gset r_
EXECUTE p_num(101.00, 102.00, 250.00) \gset r_
SELECT pg_temp.confere(:r_count = :e_num, 'numeric IN ($1, $2, $3) no 3º EXECUTE');
CREATE TEMP TABLE valores AS SELECT 100.004 + g AS v FROM generate_series(1, 30) g;
ANALYZE valores;
SELECT count(*) AS e_lat FROM valores x
  JOIN (SELECT valor_sai FROM dominio.efsaidas_slow OFFSET 0) s ON s.valor_sai = round(x.v, 2) \gset
SELECT count(*) AS r_lat FROM valores x CROSS JOIN LATERAL (
    SELECT codi_sai FROM dominio.efsaidas_slow s WHERE s.valor_sai = round(x.v, 2)) l \gset
SELECT pg_temp.confere(:e_lat = 30 AND :r_lat = :e_lat, format('LATERAL numeric: %s de %s', :r_lat, :e_lat));

-- 7. JIT: um NestLoop parametrizado com muitas linhas por reescaneamento
-- compilava uma função por linha (176 para 166 linhas, 385cbaf).
SET LOCAL jit = on;
SET LOCAL jit_above_cost = 0;
SET LOCAL jit_inline_above_cost = -1;
SET LOCAL jit_optimize_above_cost = -1;
CREATE TEMP TABLE externa AS SELECT 51800 AS codi_emp, g AS n FROM generate_series(1, 20) g;
ANALYZE externa;
SELECT (regexp_match(pg_temp.plano(
           'SELECT count(*) FROM externa x JOIN dominio.efsaidas_slow s ON s.codi_emp = x.codi_emp',
           'ANALYZE, TIMING OFF, SUMMARY ON'),
        'Functions: (\d+)'))[1]::int AS jit_funcoes \gset
SELECT pg_temp.confere(:jit_funcoes < 40, format('JIT compilou %s funções', :jit_funcoes));

ROLLBACK;
DEALLOCATE ALL;
