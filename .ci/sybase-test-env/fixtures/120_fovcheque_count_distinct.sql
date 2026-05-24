-- @desc: COUNT(DISTINCT) sobre JOIN fovcheque + geempre + customers_tax_numbers,
-- com filtro de data — query que em prod levava 75+ s antes de pushdown
-- de qual no competencia. Porta de pg_test sybase_fovcheque_count_distinct.
-- @expect: ok

SELECT
    COUNT(DISTINCT ct.customer_id) AS count
 FROM dominio.fovcheque_slow vc
 JOIN dominio.geempre_slow ge ON ge.codi_emp = vc.codi_emp
 JOIN customers_tax_numbers ct ON ct.tax_number = ge.cgce_emp
 WHERE ct.type = 'CNPJ'
   AND vc.competencia BETWEEN '2025-01-01' AND '2025-12-31'
 LIMIT 1;
