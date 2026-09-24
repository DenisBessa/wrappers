-- @desc: Reproduz o padrão de `getPayrollSummary` (app/korok/pessoal/folha/_data)
-- que em prod (2026-05-26) deu timeout >55s em supabase.bessa.digital.
--
-- Forma: SELECT foeventos.nome, sum(fomovto.valor_cal)
--   FROM fomovto JOIN geempre ON codi_emp
--                JOIN customers_tax_numbers ON tax_number=cgce_emp
--                JOIN foeventos ON i_eventos AND codi_emp
--  WHERE ctn.customer_id = X AND fomovto.data BETWEEN ...
--  GROUP BY foeventos.nome
--
-- Padrão do bug (idêntico ao 134_korok_getpayrolldata_pattern):
--   fomovto FT em prod tem OPTIONS (rows '0') → apply_quals_selectivity(0)
--   colapsa pra max(1) → FT join (fomovto+foeventos) estimado em rows=1.
--   Planner escolhe NL outer=local-join(geempre+ctn) inner=FT-join, mas a
--   semântica é catastrófica: a FT join NÃO recebe filtro de codi_emp via
--   pushdown (codi_emp vem do join local com geempre — não é literal). Então
--   a Remote SQL é "SELECT ... FROM foeventos JOIN fomovto WHERE data BETWEEN".
--   Em prod, isso retorna milhões de linhas para um ano. Mesmo com streaming,
--   o tempo de ODBC transfer + filtragem local é dominante (45s+ timeout).
--
-- Após o fix em sybase_fdw.rs: opção `rows '0'` cai pro view_estimated_rows
-- fallback. FT join estimate sobe pra um número realista, e PG escolhe Hash
-- Join, que é mais previsível (lista pretendida embora ainda fetch tudo, mas
-- as estatísticas do plano refletem o custo verdadeiro e a falha é melhor
-- detectada por monitoring).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '60s';

ALTER FOREIGN TABLE dominio.fomovto_slow OPTIONS (ADD rows '0');

-- Verifica que com rows='0' o plano da FT join sai com rows=1 (sintoma).
DO $$
DECLARE
    plan_line text;
    last_fs_rows int := -1;
    saw_join_with_rows_1 boolean := false;
BEGIN
    FOR plan_line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT ev.nome, sum(m.valor_cal) AS total
              FROM dominio.fomovto_slow m
              JOIN dominio.geempre g ON g.codi_emp = m.codi_emp
              JOIN customers_tax_numbers ctn ON ctn.tax_number = g.cgce_emp
              JOIN dominio.foeventos_slow ev
                ON ev.i_eventos = m.i_eventos AND ev.codi_emp = m.codi_emp
             WHERE ctn.customer_id = 'cust-001' AND ctn.type = 'CNPJ'
               AND m.data BETWEEN '2024-01-01' AND '2024-12-31'
             GROUP BY ev.nome
             ORDER BY ev.nome
        $q$
    LOOP
        IF plan_line ~ 'Foreign Scan  \(cost=.*rows=([0-9]+)' THEN
            last_fs_rows := (regexp_match(plan_line, 'rows=([0-9]+)'))[1]::int;
        END IF;

        IF plan_line LIKE '%SybaseFdw Join:%' THEN
            IF last_fs_rows = 1 THEN
                saw_join_with_rows_1 := true;
            END IF;
            last_fs_rows := -1;
        END IF;
    END LOOP;

    IF saw_join_with_rows_1 THEN
        RAISE EXCEPTION 'Plano regrediu: SybaseFdw Join (fomovto+foeventos) estimado em rows=1 — sintoma de rows=0 colapsado para 1. Em prod isso virou timeout 55s+ no getPayrollSummary.';
    END IF;
END $$;

ROLLBACK;
