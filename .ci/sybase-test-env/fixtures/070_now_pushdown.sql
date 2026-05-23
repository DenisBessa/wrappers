-- @desc: WHERE com NOW() / CURRENT_DATE deve ser empurrado via PARAM_EXEC.
-- Esse é o branch SYBASE FORK em qual.rs:244-260. Importante exercitar
-- explicitamente porque o ramo armazena um ponteiro raw para o expr node.
-- @expect: ok

SELECT COUNT(*) FROM dominio.efsaidas_slow
WHERE codi_emp = 51800 AND data_sai < NOW();

SELECT COUNT(*) FROM dominio.efsaidas_slow
WHERE codi_emp = 51800 AND data_sai < CURRENT_DATE;
