-- @desc: REGRESSÃO — scan que o Postgres nunca executa não conecta no Sybase.
-- O BeginForeignScan roda para todo nó do plano, inclusive o InitPlan do CASE
-- cujo ramo nunca é tomado. Antes, o begin_scan já executava a consulta
-- remota: a FT apontando para tabela inexistente abortava a query; agora a
-- consulta só sai no 1º iter_scan.
-- @expect: ok

CREATE FOREIGN TABLE pg_temp.tabela_inexistente (codi_emp integer)
    SERVER sybase_test_server
    OPTIONS (table 'bethadba.tabela_que_nao_existe');

SELECT CASE WHEN random() < 0
            THEN (SELECT codi_emp FROM pg_temp.tabela_inexistente LIMIT 1) END;

-- Agregação empurrada também é preguiçosa.
SELECT CASE WHEN random() < 0
            THEN (SELECT count(*) FROM pg_temp.tabela_inexistente) END;

-- Consultas seguidas na mesma sessão reaproveitam a conexão ociosa do pool.
SELECT count(*) FROM dominio.efsaidas_slow WHERE codi_emp = 51800;
SELECT count(*) FROM dominio.efsaidas_slow WHERE codi_emp = 51800;
SELECT codi_emp FROM dominio.efentradas_slow WHERE codi_emp = 51800 LIMIT 1;
