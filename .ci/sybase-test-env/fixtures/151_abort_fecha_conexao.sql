-- @desc: REGRESSÃO — scan abortado no meio do cursor não deixa a conexão aberta.
-- O framework só libera o estado do scan no EndForeignScan, que uma query
-- abortada (cancelamento, timeout, ERROR) nunca alcança. A conexão ODBC do
-- cursor ficava viva até o backend morrer, 2 descritores por abort, até o
-- FreeTDS falhar com "Memory allocation failure" no SQLDriverConnect.
-- @expect: ok

CREATE TEMP TABLE fds AS
    SELECT count(*) AS n FROM pg_ls_dir('/proc/' || pg_backend_pid() || '/fd');

-- Cancelamento no topo da transação, com o cursor aberto.
\set ON_ERROR_STOP off
SET statement_timeout = '200ms';
SELECT pg_sleep(0.02), codi_emp FROM dominio.efsaidas_slow;
SELECT pg_sleep(0.02), codi_emp FROM dominio.efsaidas_slow;
SELECT pg_sleep(0.02), codi_emp FROM dominio.efsaidas_slow;
SELECT pg_sleep(0.02), codi_emp FROM dominio.efsaidas_slow;
SELECT pg_sleep(0.02), codi_emp FROM dominio.efsaidas_slow;
RESET statement_timeout;
\set ON_ERROR_STOP on

-- ERROR dentro de subtransação: a conexão fecha no fim da transação.
DO $$
BEGIN
    FOR i IN 1..5 LOOP
        BEGIN
            PERFORM codi_emp / (codi_emp - codi_emp) FROM dominio.efsaidas_slow;
        EXCEPTION WHEN division_by_zero THEN NULL;
        END;
    END LOOP;
END $$;

DO $$
DECLARE
    vazados int := (SELECT count(*) FROM pg_ls_dir('/proc/' || pg_backend_pid() || '/fd'))
        - (SELECT n FROM fds);
BEGIN
    -- Antes da correção: 20 descritores (10 aborts × 2).
    IF vazados > 4 THEN
        RAISE EXCEPTION 'vazaram % descritores em 10 scans abortados', vazados;
    END IF;
END $$;

-- Cancelamento com linhas ainda em trânsito: fechar o cursor sem drená-lo
-- deixava o FreeTDS em poll para sempre, à espera do ack de um cancel que o
-- SQL Anywhere não manda. Se regredir, esta fixture trava em vez de falhar.
\set ON_ERROR_STOP off
SET statement_timeout = '1s';
SELECT pg_sleep(0.001), id, texto FROM dominio.grande;
RESET statement_timeout;
\set ON_ERROR_STOP on

-- O backend segue conectando depois dos aborts.
SELECT count(*) FROM dominio.geempre_slow;
