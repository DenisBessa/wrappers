-- @desc: NOW() vira PARAM_EXEC e o filtro deve ser pushed-down como qual
-- com Cell::Timestamp. Porta de pg_test sybase_now_pushdown.
-- @expect: ok

SELECT codi_emp, i_empregados, data_inicio
  FROM dominio.foferias_aquisitivos_slow
 WHERE codi_emp = 51800
   AND data_inicio < NOW()
 LIMIT 5;
