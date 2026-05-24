-- @desc: Pattern da query lenta de produção — JOIN entre FTs com LATERAL +
-- agregação. Antes do fix de seletividade em get_rel_size, o planner via
-- estimativas astronômicas (trilhões de linhas) e escolhia
-- NestedLoop-LATERAL com milhares de round-trips ao Sybase. Após o fix,
-- a estimativa cai pra ordem de centenas e o planner prefere Hash Join.
--
-- Validamos via statement_timeout — se o caminho ruim for escolhido, a
-- query estoura 15s.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

-- Simula a query do bug usando schema disponível: geempre (outer pequeno)
-- × efsaidas (inner grande agregado). Padrão equivalente ao original
-- efclientes × efservicos × efservicospag.
SELECT COUNT(*) FROM (
    SELECT g.codi_emp, g.nome_emp, pag.total
    FROM dominio.geempre_slow g
    LEFT JOIN LATERAL (
        SELECT codi_emp, SUM(valor_sai) AS total
          FROM dominio.efsaidas_slow s
         WHERE s.codi_emp = g.codi_emp
         GROUP BY 1
    ) pag ON TRUE
    WHERE g.codi_emp >= 51800
) x;
COMMIT;
