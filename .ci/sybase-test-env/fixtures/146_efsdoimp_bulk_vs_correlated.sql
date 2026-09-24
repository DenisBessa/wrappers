-- @desc: #2 (Temp.sql) Controle fiscal — em prod a forma antiga correlaciona
-- dominio.efsdoimp por customer (round-trip remoto por linha). A forma EFICIENTE
-- (que o worktree atual usa) resolve os codiEmps antes e consulta efsdoimp UMA vez
-- com codi_imp IN + período, cruzando local. Esta fixture reproduz a forma bulk e
-- garante que efsdoimp empurra codi_imp IN e data_sim BETWEEN num único scan.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

DO $$
DECLARE
    line       text;
    quals_line text := NULL;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT s.codi_emp, s.codi_imp, s.sdev_sim
              FROM dominio.efsdoimp s
             WHERE s.codi_imp IN (1,2,3,4,5,6,7,8,9)
               AND s.data_sim BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
               AND s.codi_emp IN (51800, 51801, 51802)
        $q$
    LOOP
        IF line LIKE '%Wrappers: quals =%' THEN quals_line := line; END IF;
    END LOOP;

    IF quals_line IS NULL THEN RAISE EXCEPTION '#2 sem foreign scan'; END IF;
    IF quals_line NOT LIKE '%codi_imp%' THEN RAISE EXCEPTION '#2 codi_imp IN nao empurrado: %', quals_line; END IF;
    IF quals_line NOT LIKE '%data_sim%' THEN RAISE EXCEPTION '#2 data_sim BETWEEN nao empurrado: %', quals_line; END IF;
    IF quals_line NOT LIKE '%codi_emp%' THEN RAISE EXCEPTION '#2 codi_emp IN nao empurrado: %', quals_line; END IF;
    RAISE NOTICE '#2 efsdoimp bulk: codi_imp IN + data_sim BETWEEN + codi_emp IN empurrados (1 scan, sem N+1)';
END $$;

-- Forma bulk completa: geempre (cache local) x efsdoimp (FT), cruzando por codi_emp.
SELECT count(*)
  FROM dominio.geempre g
  JOIN dominio.efsdoimp s ON s.codi_emp = g.codi_emp
 WHERE s.codi_imp IN (1,2,3,4,5,6,7,8,9)
   AND s.data_sim BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
   AND g.cgce_emp IN ('11111111000111', '22222222000122', '33333333000133');

COMMIT;
