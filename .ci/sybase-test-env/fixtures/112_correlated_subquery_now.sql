-- @desc: Correlated subquery exercitando reuso de cached_conn entre iterações
-- (cada linha do outer foempregados dispara um scan no foferias_aquisitivos
-- com codi_emp/i_empregados parametrizados). Porta de pg_test
-- sybase_correlated_subquery_with_now.
-- @expect: ok

SELECT
    e.nome,
    (
        SELECT fa.limite_para_gozo
        FROM dominio.foferias_aquisitivos_slow fa
        WHERE fa.codi_emp = e.codi_emp
          AND fa.i_empregados = e.i_empregados
          AND fa.data_inicio < NOW()
          AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
        ORDER BY fa.limite_para_gozo ASC
        LIMIT 1
    ) AS earliest_expiration
 FROM dominio.foempregados_slow e
 WHERE e.codi_emp = 51800
   AND e.vinculo = 1
   AND e.i_afastamentos <> 8
 LIMIT 10;
