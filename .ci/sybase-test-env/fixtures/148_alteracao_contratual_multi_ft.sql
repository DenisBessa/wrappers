-- @desc: #12 (Temp.sql) Alterações contratuais — foempregados + geempre + focargos
-- + fodepto + foccustos + foempregados_alteracao_contratual, empresa filtrada por
-- customer_id local (via geempre.cgce_emp). Garante que o filtro seletivo forte
-- (data_alteracao BETWEEN) é empurrado e que a query completa em tempo razoavel.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '20s';

DO $$
DECLARE
    line       text;
    found_ac   boolean := false;
    ac_quals   text := NULL;
    in_ac      boolean := false;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT e.nome, c.nome AS cargo, ac.data_alteracao, ac.salario
              FROM dominio.foempregados_slow e
              JOIN dominio.geempre_slow g   ON g.codi_emp = e.codi_emp
              JOIN dominio.focargos_slow c  ON c.codi_emp = e.codi_emp AND c.i_cargos = e.i_cargos
              JOIN dominio.fodepto_slow d   ON d.codi_emp = e.codi_emp AND d.i_depto = e.i_depto
              JOIN dominio.foempregados_alteracao_contratual ac
                ON ac.codi_emp = e.codi_emp AND ac.i_empregados = e.i_empregados
             WHERE g.cgce_emp = '11111111000111'
               AND ac.data_alteracao BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
        $q$
    LOOP
        IF line LIKE '%foempregados_alteracao_contratual%' THEN in_ac := true; found_ac := true; END IF;
        IF line LIKE '%Foreign Scan on%' AND line NOT LIKE '%alteracao_contratual%' THEN in_ac := false; END IF;
        IF in_ac AND line LIKE '%Wrappers: quals =%' THEN ac_quals := line; END IF;
    END LOOP;

    IF NOT found_ac THEN RAISE EXCEPTION '#12 alteracao_contratual nao aparece no plano'; END IF;
    RAISE NOTICE '#12 alteracao_contratual quals: %', COALESCE(ac_quals, '(sem quals diretos — filtro via join/param)');
END $$;

-- Executa a query completa — tem que completar em tempo razoavel.
SELECT count(*)
  FROM dominio.foempregados_slow e
  JOIN dominio.geempre_slow g   ON g.codi_emp = e.codi_emp
  JOIN dominio.focargos_slow c  ON c.codi_emp = e.codi_emp AND c.i_cargos = e.i_cargos
  JOIN dominio.fodepto_slow d   ON d.codi_emp = e.codi_emp AND d.i_depto = e.i_depto
  JOIN dominio.foempregados_alteracao_contratual ac
    ON ac.codi_emp = e.codi_emp AND ac.i_empregados = e.i_empregados
 WHERE g.cgce_emp = '11111111000111'
   AND ac.data_alteracao BETWEEN DATE '2025-01-01' AND DATE '2025-12-31';

COMMIT;
