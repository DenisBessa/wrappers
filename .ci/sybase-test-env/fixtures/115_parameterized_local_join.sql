-- @desc: JOIN entre FTs e tabela local (customers_tax_numbers). Caminho
-- parametrizado PARAM_EXEC: scan local materializa, depois FT scan recebe
-- valores. Porta de pg_test sybase_parameterized_local_join.
-- @expect: ok

SELECT e.nome
  FROM dominio.foempregados_slow e
  JOIN dominio.geempre_slow g ON g.codi_emp = e.codi_emp
  JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
 WHERE c.customer_id = 'cust-001'
   AND e.vinculo = 1
 LIMIT 5;
