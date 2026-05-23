-- @desc: REGRESSÃO bugs #1+#2 — antes da correção, PREPARE+EXECUTE×7
-- crashava no 7º (custom→generic transition: bug #1 SIGSEGV em ambiente local
-- ou bug #2 XX000 unrecognized node type em produção; ambos derivam do mesmo
-- root cause — use-after-free de FdwState quando o plano cacheado é reusado
-- e quals stale do fdw_exprs acumulam ponteiros para memcontexts já freed).
-- Após o fix em supabase-wrappers/src/scan.rs (callback de memcontext +
-- gate `first_time` na extração de fdw_exprs), todos os 7 executes passam.
-- @expect: ok

-- Garante limpeza
DEALLOCATE ALL;

-- Mesma query do bug com placeholders. Note que ela usa `dominio.efsaidas`
-- (FT direta, NÃO efsaidas_slow) e `dominio.geempre` (MV local).
PREPARE bug2 (text, date, date, text) AS
SELECT
    n.access_key,
    (
        SELECT row_to_json(t.*) FROM (
            SELECT s.chave_nfe_sai, s.situacao_sai
              FROM dominio.efsaidas s
             WHERE s.chave_nfe_sai = n.access_key
               AND s.codi_emp IN (
                   SELECT g.codi_emp
                     FROM dominio.geempre g
                     JOIN customers_tax_numbers c ON c.tax_number = g.cgce_emp
                    WHERE c.customer_id = n.customer_id
                      AND c.type = $4
               )
             LIMIT 1
        ) t
    ) AS erp_data_saidas
FROM nfe n
WHERE n.customer_id = $1
  AND n.date BETWEEN $2 AND $3
  AND n.type = 'Saída'
ORDER BY n.date DESC
LIMIT 3;

-- 7 execuções consecutivas. A 6ª/7ª deve disparar generic plan e o erro.
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');
EXECUTE bug2('cust-001', '2025-01-01', '2025-12-31', 'CNPJ');

DEALLOCATE bug2;
