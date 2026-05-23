-- @desc: COUNT(*) deve ser pushed-down ao Sybase (não materializar todas as linhas).
-- @expect: ok

EXPLAIN (VERBOSE)
SELECT COUNT(*) FROM dominio.efsaidas_slow WHERE codi_emp = 51800;

SELECT COUNT(*) FROM dominio.efsaidas_slow WHERE codi_emp = 51800;
