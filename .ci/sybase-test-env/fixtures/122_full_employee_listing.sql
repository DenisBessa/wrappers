-- @desc: Listagem completa de empregados — 7 FTs + 1 local (foempregados,
-- geempre, focargos, fodepto, forescisoes, foccustos, foafastamentos_tipos,
-- customers_tax_numbers). Em prod tomava 35s antes do ajuste de
-- fdw_startup_cost / get_rel_size selectivity. Porta de pg_test
-- sybase_full_employee_listing.
-- @expect: ok

SELECT
    e.admissao,
    ca.cbo_2002,
    cc.nome AS centro_custo,
    d.nome AS depto,
    e.nome,
    e.matricula,
    CASE e.vinculo
        WHEN 1 THEN 'Celetista'
        WHEN 50 THEN 'Estagiário'
        WHEN 11 THEN 'Contribuinte'
        WHEN 53 THEN 'Menor Aprendiz'
        ELSE 'Outros'
    END AS vinculo_desc,
    ca.nome AS cargo,
    at.descricao AS afastamento,
    r.demissao
 FROM dominio.foempregados_slow e
 JOIN dominio.geempre_slow ge ON ge.codi_emp = e.codi_emp
 JOIN customers_tax_numbers ct ON ct.tax_number = ge.cgce_emp
 JOIN dominio.focargos_slow ca ON ca.i_cargos = e.i_cargos
                               AND ca.codi_emp = e.codi_emp
 JOIN dominio.fodepto_slow d ON d.i_depto = e.i_depto
                             AND d.codi_emp = e.codi_emp
 LEFT JOIN dominio.forescisoes_slow r ON r.i_empregados = e.i_empregados
                                      AND r.codi_emp = e.codi_emp
 LEFT JOIN dominio.foccustos_slow cc ON cc.i_ccustos = e.i_ccustos
                                     AND cc.codi_emp = e.codi_emp
 LEFT JOIN dominio.foafastamentos_tipos_slow at ON at.i_afastamentos = e.i_afastamentos
 WHERE ct.customer_id = 'cust-001'
   AND e.i_afastamentos <> 8
 ORDER BY e.nome ASC
 LIMIT 10;
