-- @desc: KOROK-213 (05/11/2026). `left join lateral (... where codi_emp = <lit>
-- and nume_ser = substr(n.access_key, 24, 13)::bigint limit 1)` derrubou o
-- Postgres de produção (signal 11 em text_substr). Uma expressão sem Var cujo
-- argumento vem da linha externa (PARAM_EXEC) era avaliada no
-- BeginForeignScan, quando o slot do parâmetro ainda está zerado e marcado como
-- não nulo. Cada consulta abaixo é conferida contra o mesmo cálculo sobre as
-- linhas cruas (CTE MATERIALIZED, sem consulta parametrizada à FT).
--   1. LATERAL + LIMIT (não sobe para join) com função sobre a coluna externa.
--   2. Subconsulta escalar correlacionada com função sobre a coluna externa.
--   3. LATERAL sem LIMIT, NestLoop parametrizado forçado (fdw_exprs).
--   4. count(*) correlacionado: a agregação não pode ser empurrada com o valor
--      fictício do BeginForeignScan.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '60s';

CREATE TEMP TABLE externa AS
SELECT DISTINCT ' ' || upper(chave_nfe_sai) || ' ' AS chave
  FROM dominio.efsaidas_slow
 WHERE codi_emp = 51800 AND chave_nfe_sai IS NOT NULL
 LIMIT 20;
INSERT INTO externa VALUES (' NAO-EXISTE ');

CREATE TEMP TABLE esperado AS
WITH s AS MATERIALIZED (
    SELECT codi_sai, chave_nfe_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800 OFFSET 0
)
SELECT e.chave,
       (SELECT min(s.codi_sai) FROM s WHERE s.chave_nfe_sai = lower(trim(e.chave))) AS codi,
       (SELECT count(*) FROM s WHERE s.chave_nfe_sai = lower(trim(e.chave))) AS n
  FROM externa e;

DO $$
DECLARE
    falhas int;
    linhas int;
BEGIN
    SELECT count(*) INTO linhas FROM esperado WHERE codi IS NOT NULL;
    IF linhas = 0 THEN RAISE EXCEPTION 'fixture sem chaves de 51800'; END IF;

    -- 1. LATERAL + LIMIT
    SELECT count(*) INTO falhas
      FROM esperado x
      JOIN externa e ON e.chave = x.chave
      LEFT JOIN LATERAL (
          SELECT s.codi_sai FROM dominio.efsaidas_slow s
           WHERE s.codi_emp = 51800 AND s.chave_nfe_sai = lower(trim(e.chave))
           ORDER BY s.codi_sai
           LIMIT 1
      ) l ON true
     WHERE l.codi_sai IS DISTINCT FROM x.codi;
    IF falhas > 0 THEN RAISE EXCEPTION '1. LATERAL + LIMIT: % chaves divergentes', falhas; END IF;

    -- 2. Subconsulta escalar correlacionada
    SELECT count(*) INTO falhas
      FROM esperado x
     WHERE (SELECT min(s.codi_sai) FROM dominio.efsaidas_slow s
             WHERE s.codi_emp = 51800 AND s.chave_nfe_sai = lower(trim(x.chave)))
           IS DISTINCT FROM x.codi;
    IF falhas > 0 THEN RAISE EXCEPTION '2. subconsulta escalar: % chaves divergentes', falhas; END IF;
END $$;

-- 3. LATERAL sem LIMIT, NestLoop parametrizado
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;
DO $$
DECLARE
    falhas int;
BEGIN
    SELECT count(*) INTO falhas
      FROM esperado x
      LEFT JOIN (
          SELECT e.chave, count(s.codi_sai) AS n
            FROM externa e
            LEFT JOIN dominio.efsaidas_slow s
              ON s.codi_emp = 51800 AND s.chave_nfe_sai = lower(trim(e.chave))
           GROUP BY e.chave
      ) j ON j.chave = x.chave
     WHERE j.n IS DISTINCT FROM x.n;
    IF falhas > 0 THEN RAISE EXCEPTION '3. NestLoop parametrizado: % chaves divergentes', falhas; END IF;

    -- 4. count(*) correlacionado
    SELECT count(*) INTO falhas
      FROM esperado x
     WHERE (SELECT count(*) FROM dominio.efsaidas_slow s
             WHERE s.codi_emp = 51800 AND s.chave_nfe_sai = lower(trim(x.chave)))
           <> x.n;
    IF falhas > 0 THEN RAISE EXCEPTION '4. count correlacionado: % chaves divergentes', falhas; END IF;
END $$;

ROLLBACK;
