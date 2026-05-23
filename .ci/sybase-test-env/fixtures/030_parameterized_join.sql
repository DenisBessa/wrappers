-- @desc: Join parametrizado entre tabela local Postgres e FT — caminho do
-- nested-loop com PARAM_EXEC. Deve funcionar sem crash.
-- @expect: ok

SELECT n.access_key, s.chave_nfe_sai, s.valor_sai
  FROM nfe n
  JOIN dominio.efsaidas_slow s ON s.chave_nfe_sai = n.access_key
 WHERE n.customer_id = 'cust-001'
 LIMIT 10;
