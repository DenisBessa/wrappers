-- @desc: Agregação SUM com GROUP BY mensal sobre fovcheque (VIEW), com
-- filtro de competência. WHERE deve ir pushed-down pra Sybase e PG faz
-- só a agregação local. Porta de pg_test sybase_fovcheque_aggregation.
-- @expect: ok

SELECT
    to_char(vc.competencia, 'YYYY-MM') AS month,
    sum(vc.cp_valor) AS valor
 FROM dominio.fovcheque_slow vc
 WHERE vc.competencia BETWEEN '2025-01-01' AND '2025-12-31'
 GROUP BY to_char(vc.competencia, 'YYYY-MM')
 ORDER BY month ASC;
