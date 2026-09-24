-- @desc: #1 (Temp.sql) Saldo contábil histórico em ctvlancto — SUM(CASE)+GROUP BY
-- com filtro só de limite superior de data. Pior máximo da janela de prod (111s).
-- Garante que codi_emp e data_lan vão como WHERE pushdown e que a projeção é
-- mínima (só as 4 colunas do SELECT). Documenta se o SUM(CASE) empurra remotamente
-- (hipótese: NÃO — o framework só modela agregações sobre coluna simples, não
-- expressão CASE — então a soma é local sobre o scan já filtrado/projetado).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '20s';

DO $$
DECLARE
    line       text;
    quals_line text := NULL;
    tgts_line  text := NULL;
    agg_pushed boolean := false;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT codi_cta, codi_emp_plano,
                   sum(CASE WHEN tipo_cta = 'D' THEN vlor_lan ELSE -vlor_lan END) AS value
              FROM dominio.ctvlancto
             WHERE codi_emp IN (51800)
               AND data_lan <= DATE '2024-06-30'
             GROUP BY codi_cta, codi_emp_plano
        $q$
    LOOP
        IF line LIKE '%Wrappers: quals =%'      THEN quals_line := line; END IF;
        IF line LIKE '%Wrappers: tgts =%'       THEN tgts_line  := line; END IF;
        IF line LIKE '%Wrappers: aggregates =%' THEN agg_pushed := true; END IF;
    END LOOP;

    IF quals_line IS NULL THEN
        RAISE EXCEPTION 'Sem "Wrappers: quals" — foreign scan nao planejado';
    END IF;
    IF quals_line NOT LIKE '%codi_emp%' THEN
        RAISE EXCEPTION 'codi_emp NAO empurrado como WHERE: %', quals_line;
    END IF;
    IF quals_line NOT LIKE '%data_lan%' THEN
        RAISE EXCEPTION 'data_lan NAO empurrado como WHERE: %', quals_line;
    END IF;
    -- Projeção mínima: nao deve trazer i_lancto (coluna nao usada).
    IF tgts_line IS NOT NULL AND tgts_line LIKE '%i_lancto%' THEN
        RAISE EXCEPTION 'Projeção traz coluna nao usada (i_lancto): %', tgts_line;
    END IF;

    RAISE NOTICE '#1 ctvlancto: WHERE pushdown OK (codi_emp + data_lan); SUM(CASE) pushdown remoto = %', agg_pushed;
END $$;

-- Executa de verdade: precisa completar dentro do statement_timeout.
SELECT codi_cta, codi_emp_plano,
       sum(CASE WHEN tipo_cta = 'D' THEN vlor_lan ELSE -vlor_lan END) AS value
  FROM dominio.ctvlancto
 WHERE codi_emp IN (51800)
   AND data_lan <= DATE '2024-06-30'
 GROUP BY codi_cta, codi_emp_plano
 ORDER BY 1, 2;

COMMIT;
