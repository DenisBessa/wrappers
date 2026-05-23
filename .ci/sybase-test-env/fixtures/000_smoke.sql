-- @desc: Smoke test — conexão funciona, SELECT trivial em FT retorna linhas.
-- @expect: ok

SELECT COUNT(*) > 0 AS has_rows FROM dominio.efsaidas_slow WHERE codi_emp = 51800;
SELECT COUNT(*) > 0 AS has_rows FROM dominio.geempre_slow;
