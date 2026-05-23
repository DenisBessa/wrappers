-- @desc: Join FT-FT entre efsaidas e geempre_ft (no mesmo server) deve ser
-- pushed-down (join pushdown em join.rs). Resultado deve ter valores.
-- @expect: ok

EXPLAIN (VERBOSE)
SELECT s.chave_nfe_sai, g.nome_emp
  FROM dominio.efsaidas_slow s
  JOIN dominio.geempre_slow g ON g.codi_emp = s.codi_emp
 WHERE s.codi_emp = 51800
 LIMIT 5;

SELECT s.chave_nfe_sai, g.nome_emp
  FROM dominio.efsaidas_slow s
  JOIN dominio.geempre_slow g ON g.codi_emp = s.codi_emp
 WHERE s.codi_emp = 51800
 LIMIT 5;
