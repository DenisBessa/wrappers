-- @desc: Reproduz o padrão CTE MATERIALIZED + JOIN 3 FTs + LEFT JOIN da CTE
-- usado por getNotasEmAberto (app/korok/contabil/integrador/_actions/getNotasEmAberto.ts).
-- Em prod (oracle-004) essa forma de query — variante tipo='servico' — levou
-- 205871 ms (PID 6214 em 2026-05-24 16:08:32) e contribuiu pro OOM-kill do
-- supabase-db às 16:04:53. Aqui usamos foempregados/geempre/fomovto/fobancos
-- pra exercitar o mesmo padrão estrutural sem precisar do schema fiscal completo:
--
--   • CTE MATERIALIZED faz SUM/GROUP BY sobre uma FT Sybase (fomovto)
--   • SELECT principal junta 3 FTs Sybase (foempregados × geempre × fobancos)
--   • LEFT JOIN com a CTE + filtro WHERE comparando coluna principal × CTE
--
-- Verifica que:
--   (a) o backend sobrevive ao padrão (sem segfault/OOM)
--   (b) a query completa em <30s no dataset de teste (streaming entrega <30s)
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '30s';

WITH pagamentos AS MATERIALIZED (
    SELECT codi_emp, i_empregados, SUM(valor_cal) AS valor_pago
      FROM dominio.fomovto_slow
     WHERE codi_emp = 51800
     GROUP BY codi_emp, i_empregados
)
SELECT
    e.nome AS empregado,
    ge.nome_emp AS empresa,
    b.nome AS banco,
    COALESCE(p.valor_pago, 0)::float AS pago,
    e.matricula
  FROM dominio.foempregados_slow e
  JOIN dominio.geempre_slow ge ON ge.codi_emp = e.codi_emp
  JOIN dominio.fobancos_slow b ON b.i_bancos = COALESCE(e.i_bancos, 1)
  LEFT JOIN pagamentos p ON p.codi_emp = e.codi_emp
                         AND p.i_empregados = e.i_empregados
 WHERE e.codi_emp = 51800
   AND (1000000 > p.valor_pago OR p.valor_pago IS NULL)
 ORDER BY e.nome
 LIMIT 50;

COMMIT;
