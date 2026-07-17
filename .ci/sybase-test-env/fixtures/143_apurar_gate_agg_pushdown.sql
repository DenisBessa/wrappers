-- @desc: #7 (Temp.sql) Gate "apurar domínio" — duas FTs agregadas (efsdoimp e
-- efparametro_vigencia) filtradas só por data/vigência (sem codi_emp). Garante
-- que o MAX(...) + GROUP BY codi_emp é EMPURRADO para o Sybase em cada FT — assim
-- só a cardinalidade do GROUP BY cruza o fio, em vez de todas as empresas.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

-- Sub-agregado 1: efsdoimp -> MAX(data_sim) GROUP BY codi_emp
DO $$
DECLARE
    line       text;
    agg_line   text := NULL;
    group_line text := NULL;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT codi_emp, max(data_sim) AS max_data_sim
              FROM dominio.efsdoimp
             WHERE data_sim BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
             GROUP BY codi_emp
        $q$
    LOOP
        IF line LIKE '%Wrappers: aggregates =%' THEN agg_line   := line; END IF;
        IF line LIKE '%Wrappers: group_by =%'   THEN group_line := line; END IF;
    END LOOP;

    IF agg_line IS NULL THEN
        RAISE EXCEPTION '#7 efsdoimp: MAX NAO empurrado (agregacao local sobre scan cheio)';
    END IF;
    IF agg_line NOT LIKE '%Max%' THEN
        RAISE EXCEPTION '#7 efsdoimp: aggregates sem Max: %', agg_line;
    END IF;
    IF group_line IS NULL OR group_line NOT LIKE '%codi_emp%' THEN
        RAISE EXCEPTION '#7 efsdoimp: GROUP BY codi_emp nao empurrado: %', group_line;
    END IF;
    RAISE NOTICE '#7 efsdoimp: MAX+GROUP BY pushdown OK -> %', agg_line;
END $$;

-- Sub-agregado 2: efparametro_vigencia -> MAX(vigencia_par) GROUP BY codi_emp
DO $$
DECLARE
    line     text;
    agg_line text := NULL;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT codi_emp, max(vigencia_par) AS max_vig
              FROM dominio.efparametro_vigencia
             WHERE vigencia_par <= DATE '2025-12-31'
             GROUP BY codi_emp
        $q$
    LOOP
        IF line LIKE '%Wrappers: aggregates =%' THEN agg_line := line; END IF;
    END LOOP;

    IF agg_line IS NULL OR agg_line NOT LIKE '%Max%' THEN
        RAISE EXCEPTION '#7 efparametro_vigencia: MAX NAO empurrado: %', agg_line;
    END IF;
    RAISE NOTICE '#7 efparametro_vigencia: MAX+GROUP BY pushdown OK -> %', agg_line;
END $$;

-- Query completa do gate (customers local x geempre x 2 sub-agregados) — smoke.
SELECT g.codi_emp, s.max_data_sim, p.max_vig
  FROM dominio.geempre g
  LEFT JOIN (
      SELECT codi_emp, max(data_sim) AS max_data_sim
        FROM dominio.efsdoimp
       WHERE data_sim BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
       GROUP BY codi_emp
  ) s ON s.codi_emp = g.codi_emp
  LEFT JOIN (
      SELECT codi_emp, max(vigencia_par) AS max_vig
        FROM dominio.efparametro_vigencia
       WHERE vigencia_par <= DATE '2025-12-31'
       GROUP BY codi_emp
  ) p ON p.codi_emp = g.codi_emp
 ORDER BY g.codi_emp;

COMMIT;
