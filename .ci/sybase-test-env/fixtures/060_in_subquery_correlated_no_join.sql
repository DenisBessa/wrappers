-- @desc: Outra variante — IN (subquery) onde a subquery referencia uma Var
-- da outer query (`n.customer_id`) mas SEM JOIN dentro do IN. Isola se o
-- problema é só ter um SubPlan com correlation no WHERE da FT.
-- @expect: ok

SELECT
    n.access_key,
    (SELECT chave_nfe_sai FROM dominio.efsaidas_slow s
      WHERE s.chave_nfe_sai = n.access_key
        AND s.codi_emp IN (
            SELECT codi_emp FROM customers_tax_numbers c
             JOIN dominio.geempre g ON g.cgce_emp = c.tax_number
             WHERE c.customer_id = n.customer_id
        )
      LIMIT 1)
FROM nfe n
WHERE n.customer_id = 'cust-001'
LIMIT 5;
