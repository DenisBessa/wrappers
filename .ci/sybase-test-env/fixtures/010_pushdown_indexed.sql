-- @desc: Filtro por PK (codi_emp + codi_sai) deve ir como WHERE pushdown.
-- @expect: ok

EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT chave_nfe_sai, valor_sai FROM dominio.efsaidas_slow
WHERE codi_emp = 51800 AND codi_sai = 3;

SELECT chave_nfe_sai, valor_sai FROM dominio.efsaidas_slow
WHERE codi_emp = 51800 AND codi_sai IN (3, 6, 9);
