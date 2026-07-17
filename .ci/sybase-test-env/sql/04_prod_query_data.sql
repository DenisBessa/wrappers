-- Dados sintéticos para as tabelas de 03_prod_query_tables.sql.
-- Empresas 51800/51801/51802 (mesmas de 02_data.sql).
-- ctvlancto recebe volume maior (~3000 linhas p/ 51800) para que a diferença
-- entre "empurrar WHERE + projetar" e "trazer todo o histórico" seja visível.

-- ---------------------------------------------------------------------------
-- #1 ctvlancto — 3000 lançamentos p/ 51800, 800 p/ 51801, 400 p/ 51802.
-- data_lan espalhada em ~3 anos; tipo_cta alterna D/C; contas 1..50.
-- ---------------------------------------------------------------------------
BEGIN
    DECLARE emp INT;
    DECLARE n   INT;
    DECLARE i   INT;
    DECLARE lim INT;
    SET emp = 0;
    WHILE emp < 3 LOOP
        IF emp = 0 THEN SET lim = 3000;
        ELSEIF emp = 1 THEN SET lim = 800;
        ELSE SET lim = 400;
        END IF;
        SET i = 1;
        WHILE i <= lim LOOP
            INSERT INTO bethadba.ctvlancto
                (codi_emp, codi_cta, codi_emp_plano, tipo_cta, vlor_lan, data_lan, i_lancto)
            VALUES (
                51800 + emp,
                MOD(i, 50) + 1,
                51800 + emp,
                CASE MOD(i, 2) WHEN 0 THEN 'D' ELSE 'C' END,
                10.00 + MOD(i, 5000),
                DATEADD(DAY, MOD(i, 1095), '2022-01-01'),
                i
            );
            SET i = i + 1;
        END LOOP;
        SET emp = emp + 1;
    END LOOP;
END;

-- ---------------------------------------------------------------------------
-- #2 / #7 efsdoimp — saldos apurados. codi_imp 1..15, 1 apuração/mês em 2025.
-- ---------------------------------------------------------------------------
BEGIN
    DECLARE emp INT;
    DECLARE imp INT;
    DECLARE mo  INT;
    SET emp = 0;
    WHILE emp < 3 LOOP
        SET imp = 1;
        WHILE imp <= 15 LOOP
            SET mo = 0;
            WHILE mo < 12 LOOP
                INSERT INTO bethadba.efsdoimp (codi_emp, codi_imp, data_sim, sdev_sim)
                VALUES (51800 + emp, imp, DATEADD(MONTH, mo, '2025-01-01'),
                        100.00 + imp * 10 + mo);
                SET mo = mo + 1;
            END LOOP;
            SET imp = imp + 1;
        END LOOP;
        SET emp = emp + 1;
    END LOOP;
END;

-- efsdoimp_por_recolhimento — imposto 19 (IRRF serviços) com 3 códigos.
BEGIN
    DECLARE emp INT;
    DECLARE mo  INT;
    DECLARE cr  INT;
    SET emp = 0;
    WHILE emp < 3 LOOP
        SET mo = 0;
        WHILE mo < 12 LOOP
            SET cr = 1;
            WHILE cr <= 3 LOOP
                INSERT INTO bethadba.efsdoimp_por_recolhimento
                    (codi_emp, codi_imp, codigo_recolhimento, data_sim, saldo_recolher)
                VALUES (51800 + emp, 19, '010' || CAST(cr AS VARCHAR(2)),
                        DATEADD(MONTH, mo, '2025-01-01'), 50.00 + cr + mo);
                SET cr = cr + 1;
            END LOOP;
            SET mo = mo + 1;
        END LOOP;
        SET emp = emp + 1;
    END LOOP;
END;

-- ---------------------------------------------------------------------------
-- #7 efparametro_vigencia — 3 vigências por empresa.
-- ---------------------------------------------------------------------------
BEGIN
    DECLARE emp INT;
    DECLARE v   INT;
    SET emp = 0;
    WHILE emp < 3 LOOP
        SET v = 0;
        WHILE v < 3 LOOP
            INSERT INTO bethadba.efparametro_vigencia (codi_emp, vigencia_par, par_valor)
            VALUES (51800 + emp, DATEADD(YEAR, v, '2023-01-01'), 1.00 + v);
            SET v = v + 1;
        END LOOP;
        SET emp = emp + 1;
    END LOOP;
END;

-- ---------------------------------------------------------------------------
-- #6 geimposto + geimposto_vigencia — 8 impostos por empresa, 2 vigências cada.
-- ---------------------------------------------------------------------------
BEGIN
    DECLARE emp INT;
    DECLARE imp INT;
    DECLARE v   INT;
    SET emp = 0;
    WHILE emp < 3 LOOP
        SET imp = 1;
        WHILE imp <= 8 LOOP
            INSERT INTO bethadba.geimposto (codi_emp, codi_imp, nome_imp)
            VALUES (51800 + emp, imp, 'Imposto ' || CAST(imp AS VARCHAR(3)));
            SET v = 0;
            WHILE v < 2 LOOP
                INSERT INTO bethadba.geimposto_vigencia
                    (codi_emp, codi_imp, vigencia_imp, cdrn_imp, lanc_imp)
                VALUES (51800 + emp, imp, DATEADD(YEAR, v, '2023-01-01'),
                        CAST(1000 + imp AS VARCHAR(20)),
                        CASE MOD(imp, 2) WHEN 0 THEN 'S' ELSE 'N' END);
                SET v = v + 1;
            END LOOP;
            SET imp = imp + 1;
        END LOOP;
        SET emp = emp + 1;
    END LOOP;
END;

-- ---------------------------------------------------------------------------
-- #11 foprovisoes + foprovisoes_valores — 1 provisão/empregado p/ 51800,
-- competência 2025-01-01, tipo 1, com valores tipo_valor=1.
-- ---------------------------------------------------------------------------
BEGIN
    DECLARE k INT;
    SET k = 1;
    WHILE k <= 30 LOOP
        INSERT INTO bethadba.foprovisoes
            (codi_emp, i_empregados, competencia, tipo, avos, salario)
        VALUES (51800, k, '2025-01-01', 1, 12.00, 3000.00 + k);
        INSERT INTO bethadba.foprovisoes_valores
            (codi_emp, i_empregados, competencia, tipo, tipo_valor,
             valor_adicional_ferias, valor_fgts, valor_inss_empresa, valor_inss_rat,
             valor_inss_terceiros, valor_medias_vantagens, valor_pis, valor)
        VALUES (51800, k, '2025-01-01', 1, 1,
                100.00 + k, 240.00 + k, 660.00 + k, 30.00 + k,
                58.00 + k, 20.00 + k, 30.00 + k, 1200.00 + k);
        SET k = k + 1;
    END LOOP;
END;

-- ---------------------------------------------------------------------------
-- #12 foempregados_alteracao_contratual — 1-2 alterações p/ alguns empregados.
-- ---------------------------------------------------------------------------
BEGIN
    DECLARE k INT;
    SET k = 1;
    WHILE k <= 30 LOOP
        INSERT INTO bethadba.foempregados_alteracao_contratual
            (codi_emp, i_empregados, data_alteracao, salario)
        VALUES (51800, k, DATEADD(MONTH, MOD(k, 6), '2025-01-01'), 2500.00 + k * 10);
        SET k = k + 1;
    END LOOP;
END;

COMMIT;
