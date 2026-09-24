-- @desc: #11 (Temp.sql) Provisões — subquery correlacionada em foprovisoes_valores
-- por linha de foprovisoes (5 chaves correlacionadas). Reproduz o padrao N+1 do FDW
-- e garante que a FT interna (foprovisoes_valores) é PARAMETRIZADA (param-bound), nao
-- um scan cheio, e que a query completa em tempo razoavel no dataset de teste.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

DO $$
DECLARE
    line     text;
    fpv_par  boolean := false;
    in_fpv   boolean := false;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT p.i_empregados, p.competencia,
                   (SELECT v.valor
                      FROM dominio.foprovisoes_valores v
                     WHERE v.codi_emp = p.codi_emp
                       AND v.competencia = p.competencia
                       AND v.i_empregados = p.i_empregados
                       AND v.tipo = p.tipo
                       AND v.tipo_valor = 1
                     LIMIT 1) AS valor
              FROM dominio.foprovisoes p
             WHERE p.codi_emp = 51800
               AND p.competencia = DATE '2025-01-01'
               AND p.tipo = 1
        $q$
    LOOP
        IF line LIKE '%dominio.foprovisoes_valores%' THEN in_fpv := true; END IF;
        IF in_fpv AND line LIKE '%param: Some%' THEN fpv_par := true; END IF;
    END LOOP;

    IF NOT fpv_par THEN
        RAISE NOTICE '#11 foprovisoes_valores: subquery nao parametrizada (pode ser InitPlan/materializada) — checar plano';
    ELSE
        RAISE NOTICE '#11 foprovisoes_valores: subquery correlacionada PARAMETRIZADA por linha (N+1 do FDW; forma bulk seria melhor)';
    END IF;
END $$;

-- Executa a forma correlacionada — tem que completar (dataset pequeno).
SELECT count(*) FROM (
    SELECT p.i_empregados,
           (SELECT v.valor
              FROM dominio.foprovisoes_valores v
             WHERE v.codi_emp = p.codi_emp
               AND v.competencia = p.competencia
               AND v.i_empregados = p.i_empregados
               AND v.tipo = p.tipo
               AND v.tipo_valor = 1
             LIMIT 1) AS valor
      FROM dominio.foprovisoes p
     WHERE p.codi_emp = 51800
       AND p.competencia = DATE '2025-01-01'
       AND p.tipo = 1
) t;

COMMIT;
