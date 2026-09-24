-- @desc: Variante reduzida do bug #2 — IN (subquery) com tabela local mas
-- SEM correlation com outer query. Esse caso deveria sempre funcionar (PG
-- vira InitPlan). Útil pra isolar se o bug é especificamente correlated.
-- @expect: ok

SELECT chave_nfe_sai, valor_sai
  FROM dominio.efsaidas_slow
 WHERE codi_emp IN (SELECT codi_emp FROM dominio.geempre)
   AND codi_emp = 51800
 LIMIT 5;
