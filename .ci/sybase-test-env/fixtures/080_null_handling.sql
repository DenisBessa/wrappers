-- @desc: NULL handling — colunas com NULL devem retornar como NULL no PG.
-- @expect: ok

SELECT codi_emp, i_empregados, i_ccustos, i_bancos
  FROM dominio.foempregados_slow
 WHERE codi_emp = 51800 AND i_empregados <= 3;

-- IS NULL pushdown
SELECT COUNT(*) FROM dominio.foempregados_slow
 WHERE codi_emp = 51800 AND i_ccustos IS NULL;
