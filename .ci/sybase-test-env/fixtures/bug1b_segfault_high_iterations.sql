-- @desc: REPRO Bug #1 — versão de alta cardinalidade. Espelha o cenário de
-- produção onde a app ORM faz join nfe-FT com 4627 linhas de nfe para um único
-- customer, forçando N iterations do nested-loop parametrizado contra a FT.
--
-- Hipótese a validar: cached_conn em sybase_fdw.rs reusado por N iterações
-- pode acabar com handle ODBC zumbi → segfault na próxima reutilização.
--
-- @expect: ok

-- Mostra o plano primeiro pra confirmar nested-loop + foreign scan parametrizado.
EXPLAIN (VERBOSE)
SELECT n.access_key,
       (SELECT s.chave_nfe_sai
          FROM dominio.efsaidas_slow s
         WHERE s.chave_nfe_sai = n.access_key
         LIMIT 1) AS sai_key
FROM nfe n
WHERE n.customer_id = 'cust-001'
  AND n.type = 'Saída'
ORDER BY n.date DESC;

-- Executa de verdade. Se o backend morrer, o crash aparece no log do container.
SELECT n.access_key,
       (SELECT s.chave_nfe_sai
          FROM dominio.efsaidas_slow s
         WHERE s.chave_nfe_sai = n.access_key
         LIMIT 1) AS sai_key
FROM nfe n
WHERE n.customer_id = 'cust-001'
  AND n.type = 'Saída'
ORDER BY n.date DESC;
