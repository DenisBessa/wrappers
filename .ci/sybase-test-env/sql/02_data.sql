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

-- Cargos / departamentos (5 cada por empresa).
BEGIN
    DECLARE e INT;
    DECLARE k INT;
    SET e = 0;
    WHILE e < 3 LOOP
        SET k = 1;
        WHILE k <= 5 LOOP
            INSERT INTO bethadba.focargos VALUES (51800 + e, k, 'Cargo ' || CAST(k AS VARCHAR(3)));
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

COMMIT;
