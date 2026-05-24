-- @desc: Padrão de relatório de folha com 4 subqueries correlated em
-- foeventosbases (verificando FGTS, INSS, INSS empregado, IRRF), GROUP BY
-- + SUM. Cada subquery vira PARAM_EXEC pra mesma FT em loop. Porta de
-- pg_test sybase_payroll_events_full.
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
    COALESCE((
        SELECT true
        FROM dominio.foeventosbases_slow eb
        WHERE eb.codi_emp = m.codi_emp
          AND eb.i_cadbases IN (5, 6, 7, 8, 9, 10)
          AND eb.i_eventos = m.i_eventos
        LIMIT 1
    ), false) AS is_base_inss,
    COALESCE((
        SELECT true
        FROM dominio.foeventosbases_slow eb
        WHERE eb.codi_emp = m.codi_emp
          AND eb.i_cadbases IN (11, 12, 13, 14)
          AND eb.i_eventos = m.i_eventos
        LIMIT 1
    ), false) AS is_base_inss_employee,
    COALESCE((
        SELECT true
        FROM dominio.foeventosbases_slow eb
        WHERE eb.codi_emp = m.codi_emp
          AND eb.i_cadbases IN (15, 16, 17, 18, 19, 20)
          AND eb.i_eventos = m.i_eventos
        LIMIT 1
    ), false) AS is_base_irrf,
    SUM(m.valor_cal) AS total
 FROM dominio.fomovto_slow m
 JOIN dominio.geempre_slow ge ON ge.codi_emp = m.codi_emp
 JOIN customers_tax_numbers ct ON ct.tax_number = ge.cgce_emp
 JOIN dominio.foeventos_slow ev ON m.i_eventos = ev.i_eventos
                                AND m.codi_emp = ev.codi_emp
 WHERE ct.customer_id = 'cust-001'
   AND ct.type = 'CNPJ'
   AND m.data BETWEEN '2025-01-01' AND '2025-01-31'
 GROUP BY ev.nome, is_base_fgts, is_base_inss,
          is_base_inss_employee, is_base_irrf
 ORDER BY ev.nome ASC
 LIMIT 10;
