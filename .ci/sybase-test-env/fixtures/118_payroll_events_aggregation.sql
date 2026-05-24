-- @desc: Agregação SUM com GROUP BY sobre fomovto (VIEW), join com geempre
-- e foeventos, e subquery correlated em foeventosbases checando base FGTS.
-- Porta de pg_test sybase_payroll_events_aggregation.
-- @expect: ok

SELECT
    ev.nome,
    COALESCE((
        SELECT true
        FROM dominio.foeventosbases_slow eb
        WHERE eb.codi_emp = m.codi_emp
          AND eb.i_cadbases IN (1, 2, 3, 4)
          AND eb.i_eventos = m.i_eventos
        LIMIT 1
    ), false) AS is_base_fgts,
    SUM(m.valor_cal) AS total
 FROM dominio.fomovto_slow m
 JOIN dominio.geempre_slow ge ON ge.codi_emp = m.codi_emp
 JOIN customers_tax_numbers ct ON ct.tax_number = ge.cgce_emp
 JOIN dominio.foeventos_slow ev ON m.i_eventos = ev.i_eventos
                                AND m.codi_emp = ev.codi_emp
 WHERE ct.customer_id = 'cust-001'
   AND m.data BETWEEN '2025-01-01' AND '2025-01-31'
 GROUP BY ev.nome, is_base_fgts
 ORDER BY ev.nome ASC
 LIMIT 10;
