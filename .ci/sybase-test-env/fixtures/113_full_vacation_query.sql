-- @desc: Query completa de relatório de férias com múltiplas subqueries
-- correlated, JSON_AGG e JOIN entre 7 FTs. Porta de pg_test
-- sybase_full_vacation_query.
-- @expect: ok

SELECT
    e.admissao AS admission,
    d.nome AS department,
    (
        SELECT fa.limite_para_gozo
        FROM dominio.foferias_aquisitivos_slow fa
        WHERE fa.codi_emp = e.codi_emp
          AND fa.data_inicio < NOW()
          AND fa.i_empregados = e.i_empregados
          AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
        ORDER BY fa.limite_para_gozo ASC
        LIMIT 1
    ) AS earliest_expiration,
    e.i_empregados AS employee_code,
    e.nome AS name,
    (
        SELECT COALESCE(JSON_AGG(ROW_TO_JSON(t.*)), '[]')
        FROM (
            SELECT
                fa.dias_abono AS days_bonus,
                fa.dias_direito - (fa.dias_gozados + fa.dias_abono) AS days_left,
                fa.dias_gozados AS days_used,
                fa.limite_para_gozo AS expiration,
                fa.data_inicio AS period_from,
                fa.data_fim AS period_to,
                (
                    SELECT COALESCE(JSON_AGG(ROW_TO_JSON(tp.*)), '[]')
                    FROM (
                        SELECT fp.gozo_inicio, fp.dias_gozo
                        FROM dominio.foferias_programacao_slow fp
                        WHERE fp.codi_emp = fa.codi_emp
                          AND fp.i_empregados = fa.i_empregados
                          AND fp.i_ferias_aquisitivos = fa.i_ferias_aquisitivos
                    ) tp
                ) AS scheduled,
                (
                    SELECT COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'inicioGozo', tf.inicio_gozo,
                                'fimGozo', tf.fim_gozo,
                                'diasFerias', tf.dias_ferias,
                                'liquido', (tf.proventos - tf.descontos)::TEXT
                            )
                        ),
                        '[]'
                    )
                    FROM (
                        SELECT f.inicio_gozo, f.fim_gozo, f.dias_ferias,
                               f.proventos, f.descontos
                        FROM dominio.foferias_slow f
                        WHERE f.codi_emp = fa.codi_emp
                          AND f.fim_aquisitivo = fa.data_fim
                          AND f.i_empregados = fa.i_empregados
                          AND f.inicio_aquisitivo = fa.data_inicio
                    ) tf
                ) AS status
            FROM dominio.foferias_aquisitivos_slow fa
            WHERE fa.codi_emp = e.codi_emp
              AND fa.data_inicio < NOW()
              AND fa.i_empregados = e.i_empregados
              AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
            ORDER BY fa.limite_para_gozo ASC
        ) t
    ) AS periods,
    ca.nome AS role
 FROM dominio.foempregados_slow e
 JOIN dominio.geempre_slow ge ON ge.codi_emp = e.codi_emp
 JOIN dominio.focargos_slow ca ON ca.i_cargos = e.i_cargos AND ca.codi_emp = e.codi_emp
 JOIN dominio.fodepto_slow d ON d.i_depto = e.i_depto AND d.codi_emp = e.codi_emp
 WHERE e.codi_emp = 51800
   AND e.vinculo = 1
   AND e.i_afastamentos <> 8
 ORDER BY e.nome ASC
 LIMIT 5;
