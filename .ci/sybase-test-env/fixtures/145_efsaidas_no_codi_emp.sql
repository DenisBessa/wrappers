-- @desc: #8 (Temp.sql) efsaidas SEM codi_emp — só data_sai BETWEEN + NOT IN de
-- situacao + valor_sai >= X. Recorte forte é só o período. Garante que os quals
-- que EXISTEM sao empurrados (o remoto filtra o que da; sem codi_emp o volume e
-- inerente a query do app). Sem codi_emp, o unico jeito de reduzir mais e o app
-- informar as empresas — nao ha o que o FDW empurre alem disso.
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
            SELECT codi_emp, data_sai, codi_sai, valor_sai
              FROM dominio.efsaidas_slow
             WHERE data_sai BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
               AND situacao_sai NOT IN (100, 200)
               AND valor_sai >= 100
        $q$
    LOOP
        IF line LIKE '%Wrappers: quals =%' THEN quals_line := line; END IF;
    END LOOP;

    IF quals_line IS NULL THEN RAISE EXCEPTION '#8 sem foreign scan'; END IF;
    IF quals_line NOT LIKE '%data_sai%'     THEN RAISE EXCEPTION '#8 data_sai nao empurrado: %', quals_line; END IF;
    IF quals_line NOT LIKE '%situacao_sai%' THEN RAISE EXCEPTION '#8 situacao_sai nao empurrado: %', quals_line; END IF;
    IF quals_line NOT LIKE '%valor_sai%'    THEN RAISE EXCEPTION '#8 valor_sai nao empurrado: %', quals_line; END IF;
    RAISE NOTICE '#8 efsaidas sem codi_emp: data_sai BETWEEN + situacao NOT IN + valor_sai >= empurrados';
END $$;

SELECT count(*)
  FROM dominio.efsaidas_slow
 WHERE data_sai BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
   AND situacao_sai NOT IN (100, 200)
   AND valor_sai >= 100;

COMMIT;
