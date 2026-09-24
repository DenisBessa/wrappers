-- @desc: #3 (Temp.sql) efsaidas em bulk — codi_emp IN + data_sai BETWEEN + NOT IN
-- de situacao. Garante que os TRES recortes vão como WHERE pushdown (o remoto
-- filtra tudo; PG nao recebe a tabela inteira).
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
            SELECT codi_emp, codi_sai, chave_nfe_sai, data_sai
              FROM dominio.efsaidas_slow
             WHERE codi_emp IN (51800, 51801, 51802)
               AND data_sai BETWEEN DATE '2025-01-01' AND DATE '2025-06-30'
               AND situacao_sai NOT IN (100, 200)
        $q$
    LOOP
        IF line LIKE '%Wrappers: quals =%' THEN quals_line := line; END IF;
    END LOOP;

    IF quals_line IS NULL THEN RAISE EXCEPTION '#3 sem foreign scan'; END IF;
    IF quals_line NOT LIKE '%codi_emp%'    THEN RAISE EXCEPTION '#3 codi_emp nao empurrado: %', quals_line; END IF;
    IF quals_line NOT LIKE '%data_sai%'    THEN RAISE EXCEPTION '#3 data_sai (BETWEEN) nao empurrado: %', quals_line; END IF;
    IF quals_line NOT LIKE '%situacao_sai%' THEN RAISE EXCEPTION '#3 situacao_sai (NOT IN) nao empurrado: %', quals_line; END IF;
    RAISE NOTICE '#3 efsaidas: codi_emp IN + data_sai BETWEEN + situacao NOT IN todos empurrados';
END $$;

SELECT count(*)
  FROM dominio.efsaidas_slow
 WHERE codi_emp IN (51800, 51801, 51802)
   AND data_sai BETWEEN DATE '2025-01-01' AND DATE '2025-06-30'
   AND situacao_sai NOT IN (100, 200);

COMMIT;
