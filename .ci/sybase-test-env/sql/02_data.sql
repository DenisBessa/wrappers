-- Dados sintéticos para reproduzir os bugs. Volume baixo (≤ 1000 linhas por
-- tabela) é suficiente — o crash do bug #1 não depende de escala, depende de
-- o planejador escolher caminhos parametrizados sobre full-scan da FT.

-- 3 empresas. cgce_emp = CNPJ.
INSERT INTO bethadba.geempre (codi_emp, cgce_emp, nome_emp) VALUES
    (51800, '11111111000111', 'Empresa Teste 1'),
    (51801, '22222222000122', 'Empresa Teste 2'),
    (51802, '33333333000133', 'Empresa Teste 3');

-- ~500 notas de saída espalhadas pelas 3 empresas. chave_nfe_sai com prefixo
-- da empresa para garantir matches determinísticos com a tabela local 'nfe'.
-- Esse volume é alto o bastante pra forçar o planner a considerar caminhos
-- diferentes e mostrar diferença entre full-scan e parametrizado.
BEGIN
    DECLARE i INT;
    SET i = 1;
    WHILE i <= 500 LOOP
        INSERT INTO bethadba.efsaidas (
            codi_emp, codi_sai, chave_nfe_sai, situacao_sai, valor_sai,
            data_sai, cliente_sai, cnpj_dest, obs_sai
        ) VALUES (
            51800 + MOD(i, 3),
            i,
            CAST(51800 + MOD(i, 3) AS VARCHAR(5)) ||
                REPEAT('0', 37 - LENGTH(CAST(i AS VARCHAR(10)))) ||
                CAST(i AS VARCHAR(10)) ||
                '01',
            CASE MOD(i, 4) WHEN 0 THEN 100 WHEN 1 THEN 200 ELSE 300 END,
            100.00 + i,
            DATEADD(DAY, MOD(i, 365), '2025-01-01'),
            'Cliente ' || CAST(i AS VARCHAR(10)),
            '99999999000' || CAST(MOD(i, 100) AS VARCHAR(3)),
            'Observação longa repetida para forçar tipo LONG VARCHAR ' || REPEAT('x', 200)
        );
        SET i = i + 1;
    END LOOP;
END;

-- ~300 notas de entrada.
BEGIN
    DECLARE i INT;
    SET i = 1;
    WHILE i <= 300 LOOP
        INSERT INTO bethadba.efentradas (
            codi_emp, codi_ent, chave_nfe_ent, situacao_ent, valor_ent,
            data_ent, fornecedor_ent, cnpj_orig
        ) VALUES (
            51800 + MOD(i, 3),
            i,
            CAST(51800 + MOD(i, 3) AS VARCHAR(5)) ||
                REPEAT('0', 37 - LENGTH(CAST(i AS VARCHAR(10)))) ||
                CAST(i AS VARCHAR(10)) ||
                '02',
            100,
            50.00 + i,
            DATEADD(DAY, MOD(i, 365), '2025-01-01'),
            'Fornecedor ' || CAST(i AS VARCHAR(10)),
            '88888888000' || CAST(MOD(i, 100) AS VARCHAR(3))
        );
        SET i = i + 1;
    END LOOP;
END;

-- Cargos / departamentos (5 cada por empresa). focargos.cbo_2002 é populado
-- com um CBO sintético para exercitar projeção de coluna no full_employee_listing.
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 5 LOOP
            INSERT INTO bethadba.focargos VALUES (51800 + e, k,
                'Cargo ' || CAST(k AS VARCHAR(3)), 100000 + k);
            INSERT INTO bethadba.fodepto  VALUES (51800 + e, k, 'Depto ' || CAST(k AS VARCHAR(3)));
            SET k = k + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- 30 empregados por empresa.
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 30 LOOP
            INSERT INTO bethadba.foempregados (
                codi_emp, i_empregados, nome, admissao, vinculo,
                i_afastamentos, i_cargos, i_depto, i_ccustos, i_bancos,
                conta_corr, digito_conta_pagamento, tipo_conta, cpf, matricula
            ) VALUES (
                51800 + e, k,
                'Empregado ' || CAST(e AS VARCHAR(3)) || '-' || CAST(k AS VARCHAR(5)),
                DATEADD(DAY, -k, '2024-01-01'),
                1, 0, MOD(k, 5) + 1, MOD(k, 5) + 1, NULL, NULL,
                '0000' || CAST(k AS VARCHAR(10)), '1', '01',
                '111111111' || CAST(k AS VARCHAR(5)),
                'M' || CAST(k AS VARCHAR(10))
            );
            SET k = k + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- ============================================================================
-- Módulo de Folha (Payroll) — dados sintéticos pros tests migrados de pg_test.
-- Mantemos 51800/51801/51802 como codi_emp; nenhum teste novo introduz outro.
-- ============================================================================

-- Bancos cadastrados (8 bancos).
BEGIN
    DECLARE b INT;
    SET b = 1;
    WHILE b <= 8 LOOP
        INSERT INTO bethadba.fobancos (i_bancos, numero, agencia, nome)
        VALUES (b, 100 + b, '0001-' || CAST(b AS VARCHAR(3)), 'Banco ' || CAST(b AS VARCHAR(3)));
        SET b = b + 1;
    END LOOP;
END;

-- 2 períodos aquisitivos por empregado: um já vencendo (limite_para_gozo no
-- futuro), outro ainda no período de gozo. dias_gozados + dias_abono < dias_direito
-- para sempre passar pelo filtro do teste full_vacation_query.
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 30 LOOP
            -- período 1 — antigo, com saldo
            INSERT INTO bethadba.foferias_aquisitivos
                (codi_emp, i_empregados, i_ferias_aquisitivos, data_inicio,
                 data_fim, dias_direito, dias_gozados, dias_abono, limite_para_gozo)
            VALUES (51800 + e, k, 1, '2023-01-01', '2023-12-31', 30, 10, 0, '2025-06-30');
            -- período 2 — recente, sem gozo
            INSERT INTO bethadba.foferias_aquisitivos
                (codi_emp, i_empregados, i_ferias_aquisitivos, data_inicio,
                 data_fim, dias_direito, dias_gozados, dias_abono, limite_para_gozo)
            VALUES (51800 + e, k, 2, '2024-01-01', '2024-12-31', 30,  0, 0, '2026-06-30');
            SET k = k + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- 1 programação por empregado (gozo planejado pro período 1).
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 30 LOOP
            INSERT INTO bethadba.foferias_programacao
                (codi_emp, i_empregados, gozo_inicio, dias_gozo, i_ferias_aquisitivos)
            VALUES (51800 + e, k, '2025-07-01', 20, 1);
            SET k = k + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- 1 férias efetivamente gozada por empregado, alinhada ao período 1.
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 30 LOOP
            INSERT INTO bethadba.foferias
                (codi_emp, i_empregados, inicio_aquisitivo, fim_aquisitivo,
                 inicio_gozo, fim_gozo, dias_ferias, data_aviso, data_pagto,
                 proventos, descontos)
            VALUES (51800 + e, k, '2023-01-01', '2023-12-31',
                    '2024-07-01', '2024-07-20', 20, '2024-06-15', '2024-06-30',
                    2500.00, 500.00);
            SET k = k + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- Centros de custo (3 por empresa).
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 3 LOOP
            INSERT INTO bethadba.foccustos (codi_emp, i_ccustos, nome)
            VALUES (51800 + e, k, 'CC ' || CAST(k AS VARCHAR(3)));
            SET k = k + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- Tipos de afastamento (poucos, 0 significa "sem afastamento").
INSERT INTO bethadba.foafastamentos_tipos VALUES (0, 'Sem afastamento');
INSERT INTO bethadba.foafastamentos_tipos VALUES (1, 'Doença');
INSERT INTO bethadba.foafastamentos_tipos VALUES (2, 'Acidente');
INSERT INTO bethadba.foafastamentos_tipos VALUES (8, 'Demitido');

-- Rescisões pra alguns empregados (mostra LEFT JOIN sendo exercitado).
INSERT INTO bethadba.forescisoes VALUES (51800, 5,  '2024-05-15', 2);
INSERT INTO bethadba.forescisoes VALUES (51801, 10, '2024-09-10', 1);

-- Cadastro de eventos da folha (cobertura suficiente pros 4 grupos do
-- payroll_events_full: FGTS, INSS, INSS empregado, IRRF).
BEGIN
    DECLARE e INT;
    DECLARE ev INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET ev = 1;
        WHILE ev <= 6 LOOP
            INSERT INTO bethadba.foeventos (codi_emp, i_eventos, nome)
            VALUES (51800 + e, ev, 'Evento ' || CAST(ev AS VARCHAR(3)));
            SET ev = ev + 1;
        END LOOP;
        SET e = e + 1;
    END LOOP;
END;

-- Associa eventos a bases para cobrir cada um dos i_cadbases IN (1..20)
-- testados pelo payroll_events_full.
INSERT INTO bethadba.foeventosbases VALUES (51800, 1,  1);   -- FGTS
INSERT INTO bethadba.foeventosbases VALUES (51800, 2,  5);   -- INSS
INSERT INTO bethadba.foeventosbases VALUES (51800, 3, 11);   -- INSS empregado
INSERT INTO bethadba.foeventosbases VALUES (51800, 4, 15);   -- IRRF
INSERT INTO bethadba.foeventosbases VALUES (51800, 5,  1);
INSERT INTO bethadba.foeventosbases VALUES (51800, 6,  5);

-- Movimento (fomovto_base) — janeiro de 2025, para casar com
-- WHERE m.data BETWEEN '2025-01-01' AND '2025-01-31'.
BEGIN
    DECLARE ev INT;
    DECLARE emp INT;
    SET ev = 1;
    WHILE ev <= 6 LOOP
        SET emp = 1;
        WHILE emp <= 10 LOOP
            INSERT INTO bethadba.fomovto_base
                (codi_emp, i_empregados, i_eventos, data, valor_cal)
            VALUES (51800, emp, ev,
                    DATEADD(DAY, MOD(emp, 28), '2025-01-01'),
                    100.00 + emp + ev);
            SET emp = emp + 1;
        END LOOP;
        SET ev = ev + 1;
    END LOOP;
END;

-- Cheques de pagamento (focheque) — distribuídos ao longo de 2025 pra que o
-- aggregation by month (fovcheque_aggregation) retorne 12 buckets.
BEGIN
    DECLARE emp INT;
    DECLARE mo INT;
    SET emp = 1;
    WHILE emp <= 10 LOOP
        SET mo = 0;
        WHILE mo < 12 LOOP
            INSERT INTO bethadba.focheque
                (cp_tipo, i_empregados, i_bancos, codi_emp,
                 cp_tipo_process, competencia, cp_valor, cp_data_pagto)
            VALUES (1, emp, MOD(emp, 8) + 1, 51800,
                    1,
                    DATEADD(MONTH, mo, '2025-01-01'),
                    1500.00 + emp,
                    DATEADD(MONTH, mo, '2025-01-05'));
            SET mo = mo + 1;
        END LOOP;
        SET emp = emp + 1;
    END LOOP;
END;

COMMIT;
