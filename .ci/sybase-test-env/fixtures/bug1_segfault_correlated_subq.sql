-- @desc: REPRO BUG #1 — SIGSEGV em JOIN amplo entre FT Sybase e tabela/MV
-- local quando o WHERE não tem codi_emp (forçando full-scan na FT) e há
-- correlated subqueries iterando dezenas/centenas de vezes.
--
-- @expect: ok
--
-- Versão simplificada da query original do enunciado: para cada linha de nfe,
-- roda um subselect tocando dominio.efsaidas_slow e dominio.geempre. A combinação
-- exata varia ligeiramente para forçar caminhos diferentes do planner em
-- relação ao bug2 — aqui o WHERE da FT é puramente baseado em colunas SEM
-- índice (chave_nfe_sai), e a outer table tem N>1 linhas, fazendo o
-- nested-loop disparar N execuções remotas.

SELECT
    n.access_key,
    (
        SELECT row_to_json(t.*) FROM (
            SELECT s.chave_nfe_sai, s.situacao_sai, s.valor_sai
              FROM dominio.efsaidas_slow s
              JOIN dominio.geempre g ON g.codi_emp = s.codi_emp
              JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
             WHERE c.customer_id = n.customer_id
               AND c.type = 'CNPJ'
               AND s.chave_nfe_sai = n.access_key
             LIMIT 1
        ) t
    ) AS erp_data_saidas
FROM nfe n
WHERE n.customer_id = 'cust-001'
  AND n.date BETWEEN '2025-01-01' AND '2025-12-31'
  AND n.type = 'Saída'
ORDER BY n.date DESC;
