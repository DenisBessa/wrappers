-- @desc: REPRO BUG #2 — ERROR: unrecognized node type: 0 ao usar IN (SELECT)
-- com tabela local dentro do WHERE de uma FT, correlated com a outer query.
-- Espera-se que após o fix: rode sem erro (ok). Antes do fix: error XX000.
--
-- @expect: ok
--
-- Query do enunciado (versão simplificada equivalente):
--   FT efsaidas com WHERE
--     chave_nfe_sai = nfe.access_key
--     AND codi_emp IN (
--       SELECT g.codi_emp FROM dominio.geempre g
--       JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
--       WHERE c.customer_id = nfe.customer_id AND c.type = 'CNPJ'
--     )
--   onde tudo dentro do IN é tabela/MV LOCAL Postgres, e a subquery é
--   correlated com nfe.customer_id (relação externa à FT).

SELECT
    n.access_key,
    (
        SELECT row_to_json(t.*) FROM (
            SELECT s.chave_nfe_sai, s.situacao_sai
              FROM dominio.efsaidas_slow s
             WHERE s.chave_nfe_sai = n.access_key
               AND s.codi_emp IN (
                   SELECT g.codi_emp
                     FROM dominio.geempre g
                     JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
                    WHERE c.customer_id = n.customer_id
                      AND c.type = 'CNPJ'
               )
             LIMIT 1
        ) t
    ) AS erp_data_saidas
FROM nfe n
WHERE n.customer_id = 'cust-001'
ORDER BY n.date DESC
LIMIT 20;
