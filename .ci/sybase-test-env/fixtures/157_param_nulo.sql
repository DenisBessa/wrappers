-- @desc: KOROK-258/261 (05/11/2026). Subconsulta correlacionada do Korok
-- (`gemunicipio.codigo_municipio = geempre.codigo_municipio`) com a chave
-- externa NULL falhou com "SQL Anywhere Error -157: Cannot convert 'null' to
-- numeric": o parâmetro NULL deixava no WHERE remoto o placeholder de texto
-- 'null'. Toda comparação empurrada é estrita, então NULL não casa nada: o
-- qual vira `1 = 0`. Cada caso é conferido contra as linhas cruas.
--   1. Subconsulta escalar com a chave NULL na PRIMEIRA linha externa (o caso
--      de produção), e depois 51800 → NULL → 51800.
--   2. LATERAL + LIMIT.
--   3. LEFT JOIN com NestLoop parametrizado.
--   4. `$n` NULL (PARAM_EXTERN) no plano genérico: count empurrado dá 0 (antes,
--      o mesmo -157), e `<> ALL(NULL)` não devolve linhas (antes ia `1 = 1` e
--      só o filtro local refeito pelo Postgres salvava o resultado).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '60s';

CREATE TEMP TABLE externa (ordem int, emp int);
INSERT INTO externa VALUES (1, NULL), (2, 51800), (3, NULL), (4, 51800);

CREATE TEMP TABLE esperado AS
WITH s AS MATERIALIZED (
    SELECT codi_emp, codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800 OFFSET 0
)
SELECT e.ordem,
       (SELECT min(s.codi_sai) FROM s WHERE s.codi_emp = e.emp) AS codi,
       (SELECT count(*) FROM s WHERE s.codi_emp = e.emp) AS n
  FROM externa e;

DO $$
DECLARE
    falhas int;
BEGIN
    IF (SELECT codi FROM esperado WHERE ordem = 2) IS NULL THEN
        RAISE EXCEPTION 'fixture sem saídas de 51800';
    END IF;

    -- 1. Subconsulta escalar, chave NULL primeiro
    SELECT count(*) INTO falhas
      FROM esperado x
      JOIN externa e ON e.ordem = x.ordem
     WHERE (SELECT s.codi_sai FROM dominio.efsaidas_slow s
             WHERE s.codi_emp = e.emp ORDER BY s.codi_sai LIMIT 1)
           IS DISTINCT FROM x.codi;
    IF falhas > 0 THEN RAISE EXCEPTION '1. subconsulta escalar: % linhas divergentes', falhas; END IF;

    -- 2. LATERAL + LIMIT
    SELECT count(*) INTO falhas
      FROM esperado x
      JOIN externa e ON e.ordem = x.ordem
      LEFT JOIN LATERAL (
          SELECT s.codi_sai FROM dominio.efsaidas_slow s
           WHERE s.codi_emp = e.emp
           ORDER BY s.codi_sai
           LIMIT 1
      ) l ON true
     WHERE l.codi_sai IS DISTINCT FROM x.codi;
    IF falhas > 0 THEN RAISE EXCEPTION '2. LATERAL + LIMIT: % linhas divergentes', falhas; END IF;
END $$;

-- 3. LEFT JOIN, NestLoop parametrizado
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;
DO $$
DECLARE
    falhas int;
BEGIN
    SELECT count(*) INTO falhas
      FROM esperado x
      JOIN (
          SELECT e.ordem, count(s.codi_sai) AS n
            FROM externa e
            LEFT JOIN dominio.efsaidas_slow s ON s.codi_emp = e.emp
           GROUP BY e.ordem
      ) j ON j.ordem = x.ordem
     WHERE j.n <> x.n;
    IF falhas > 0 THEN RAISE EXCEPTION '3. NestLoop parametrizado: % linhas divergentes', falhas; END IF;
END $$;

-- 4. `$n` NULL no plano genérico (variável PL/pgSQL vira PARAM_EXTERN)
SET LOCAL plan_cache_mode = force_generic_plan;
DO $$
DECLARE
    emp int := NULL;
    excluidos int[] := NULL;
    n bigint;
    linhas int;
BEGIN
    SELECT count(*) INTO n FROM dominio.efsaidas_slow WHERE codi_emp = emp;
    IF n <> 0 THEN RAISE EXCEPTION '4. count com $1 NULL: % (esperado 0)', n; END IF;

    SELECT count(*) INTO linhas
      FROM (SELECT codi_sai FROM dominio.efsaidas_slow
             WHERE codi_emp = 51800 AND codi_sai <> ALL(excluidos) OFFSET 0) t;
    IF linhas <> 0 THEN RAISE EXCEPTION '4. <> ALL(NULL): % linhas (esperado 0)', linhas; END IF;
END $$;

ROLLBACK;
