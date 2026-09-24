-- @desc: Verifica que o join pushdown injeta baserestrictinfo de cada rel
-- como WHERE no remote SQL. Antes do fix, `SELECT ... FROM a JOIN b ON ...`
-- saia SEM os filtros — o Sybase devolvia o JOIN inteiro pro PG filtrar
-- localmente (em prod, getNotasEmAberto puxava milhoes de linhas).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

DO $$
DECLARE
    plan_line text;
    remote_sql text := NULL;
BEGIN
    FOR plan_line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT m.codi_emp, e.nome
              FROM dominio.fomovto_slow m
              JOIN dominio.foempregados_slow e
                ON m.i_empregados = e.i_empregados AND m.codi_emp = e.codi_emp
             WHERE m.codi_emp = 51800
               AND m.valor_cal > 100
        $q$
    LOOP
        IF plan_line LIKE '%SybaseFdw Join: Remote SQL:%' THEN
            remote_sql := plan_line;
            EXIT;
        END IF;
    END LOOP;

    IF remote_sql IS NULL THEN
        RAISE EXCEPTION 'Join pushdown nao selecionado pelo planner.';
    END IF;
    IF remote_sql NOT LIKE '%WHERE%' THEN
        RAISE EXCEPTION 'Join pushdown sem WHERE: %', remote_sql;
    END IF;
    IF remote_sql NOT LIKE '%valor_cal > 100%' THEN
        RAISE EXCEPTION 'WHERE nao contem valor_cal > 100: %', remote_sql;
    END IF;
    IF remote_sql NOT LIKE '%codi_emp = 51800%' THEN
        RAISE EXCEPTION 'WHERE nao contem codi_emp = 51800: %', remote_sql;
    END IF;
END $$;

-- Smoke: a query completa em tempo razoavel.
SELECT count(*)
  FROM dominio.fomovto_slow m
  JOIN dominio.foempregados_slow e
    ON m.i_empregados = e.i_empregados AND m.codi_emp = e.codi_emp
 WHERE m.codi_emp = 51800
   AND m.valor_cal > 100;

COMMIT;
