-- @desc: Scan básico com qual simples sobre FT — porta de
-- pg_test sybase_basic_scan. Empresas 51800/51801/51802 têm 30 empregados
-- cada, então LIMIT 3 cabe.
-- @expect: ok

SELECT nome FROM dominio.foempregados_slow
 WHERE codi_emp = 51800
 LIMIT 3;
