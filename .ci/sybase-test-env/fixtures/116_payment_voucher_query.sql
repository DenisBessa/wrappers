-- @desc: Query de cheques de pagamento juntando fovcheque (VIEW no Sybase)
-- com geempre, fobancos, foempregados e a tabela local
-- customers_tax_numbers. Sem view_estimated_rows o planner trata
-- fovcheque como 0 linhas e gera plano horrível. Porta de pg_test
-- sybase_payment_voucher_query.
-- @expect: ok

SELECT
    vc.nome AS receiver_name,
    e.cpf AS receiver_tax_number,
    b.agencia AS agency_number,
    b.numero AS bank_code,
    vc.cp_valor AS value,
    vc.cp_data_pagto AS due_date,
    vc.competencia AS reference_date,
    vc.cp_tipo_process AS process_type
 FROM dominio.fovcheque_slow vc
 JOIN dominio.geempre_slow ge ON ge.codi_emp = vc.codi_emp
 JOIN customers_tax_numbers ct ON ct.tax_number = ge.cgce_emp
 JOIN dominio.fobancos_slow b ON b.i_bancos = vc.i_bancos
 JOIN dominio.foempregados_slow e ON e.codi_emp = vc.codi_emp
                                  AND e.i_empregados = vc.i_empregados
 WHERE ct.customer_id = 'cust-001'
 ORDER BY vc.cp_data_pagto ASC, vc.cp_tipo_process ASC, e.nome ASC
 LIMIT 10;
