-- @desc: Reproduz o padrao da query getLancamentos do app korok (caminho
-- autoConciliarCrossPeriod.test.ts). A query principal faz JOIN de tabelas
-- locais com uma FT (ctlancto/equivalente), e para cada linha do outer
-- avalia subqueries CORRELATED que internamente fazem JOIN entre 2 FTs do
-- Sybase com LIMIT 1.
--
-- Padrao critico:
--   (a) FT pequena com qual param (referenciando colunas do outer)
--   (b) FT grande sem qual direto (joinada com (a) via outra coluna)
--   (c) LIMIT 1 envolvendo as duas
--
-- Antes do fix em apply_quals_selectivity: o param qual reduzia (a) para 1-2
-- rows estimadas, o planner achava (a) era inner barato Materialize, escolhia
-- (b) como NL outer SEM qual → full-scan ODBC de centenas de milhares de rows
-- POR EXECUCAO da subquery → timeout (em prod, ~55s para 5 outer rows).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

DROP TABLE IF EXISTS drivers;
CREATE TEMP TABLE drivers AS
SELECT * FROM (VALUES (51800, 1), (51800, 2), (51800, 3), (51800, 4), (51800, 5))
  AS t(codi_emp, ndoc);

DO $$
DECLARE
    plan_line text;
    outer_scan text := NULL;
BEGIN
    FOR plan_line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT ld.codi_emp, (
                SELECT m.valor_cal
                  FROM dominio.fomovto_slow m
                  JOIN dominio.foempregados_slow e ON e.i_empregados = m.i_empregados
                  WHERE m.codi_emp = ld.codi_emp
                    AND m.i_eventos = ld.ndoc
                  LIMIT 1
            )
            FROM drivers ld
        $q$
    LOOP
        -- Achar o primeiro Foreign Scan abaixo do Nested Loop interno
        IF plan_line LIKE '%Foreign Scan on dominio.fomovto_slow%'
           OR plan_line LIKE '%Foreign Scan on dominio.foempregados_slow%' THEN
            IF outer_scan IS NULL THEN
                outer_scan := plan_line;
            END IF;
        END IF;
    END LOOP;

    IF outer_scan IS NULL THEN
        RAISE EXCEPTION 'Nao encontrou Foreign Scan no plano.';
    END IF;

    -- O outer da NL na subquery DEVE ser a tabela com filter param (fomovto),
    -- nao a sem qual (foempregados). Antes do fix de selectivity, a ordem
    -- era invertida e o full-scan de fomovto (1M rows hint) explodia.
    IF outer_scan NOT LIKE '%fomovto_slow%' THEN
        RAISE EXCEPTION 'Plano sub-otimo: outer e foempregados (sem qual), nao fomovto (com filter param). Linha: %', outer_scan;
    END IF;
END $$;

-- Smoke: executa em tempo razoavel.
SELECT count(*) FROM (
    SELECT ld.codi_emp, (
        SELECT m.valor_cal
          FROM dominio.fomovto_slow m
          JOIN dominio.foempregados_slow e ON e.i_empregados = m.i_empregados
          WHERE m.codi_emp = ld.codi_emp
            AND m.i_eventos = ld.ndoc
          LIMIT 1
    ) AS valor
    FROM drivers ld
) sub;

COMMIT;
