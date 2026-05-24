-- @desc: JOIN FT × FT no mesmo server. join.rs decide se pushdown vai pro
-- Sybase ou se planner faz Nested Loop local. Porta de pg_test
-- sybase_join_pushdown.
-- @expect: ok

SELECT e.nome, ca.nome AS cargo
  FROM dominio.foempregados_slow e
  JOIN dominio.focargos_slow ca ON ca.i_cargos = e.i_cargos
                                AND ca.codi_emp = e.codi_emp
 WHERE e.codi_emp = 51800
 LIMIT 5;
