-- @desc: Reproduz o padrão de `getPayrollData` (app/korok/pessoal/folha/extrato)
-- que em prod (2026-05-26) deu timeout >55s em supabase.bessa.digital.
--
-- Forma: SELECT FROM fomovto JOIN foempregados USING (codi_emp, i_empregados)
-- JOIN foeventos USING (codi_emp, i_eventos) WHERE codi_emp=X AND data BETWEEN ...
--
-- Padrão do bug:
--   fomovto FT em prod tem OPTIONS (rows '0') — set por job de refresh quando
--   sys.systable.count = 0 (stale stats, comum em VIEWs). O override `rows '0'`
--   passa por apply_quals_selectivity(0, ...) → max(1) → FT estima 1 linha.
--   Com fomovto rows=1 + foempregados rows=N, o join (fomovto+foempregados)
--   estima rows = max(1, 1*N*0.01) ≈ 1, com cost ínfimo (10..11). Planner escolhe
--   Nested Loop com outer=esse foreign join e inner=foeventos parametrizado.
--   Em runtime, outer retorna 30k+ linhas reais — cada uma faz uma ODBC call
--   para foeventos (`WHERE i_eventos=$1`). 30k × ~10ms = 5+ minutos.
--
-- Após o fix em sybase_fdw.rs: opção `rows '0'` é tratada como "stale stats" e
-- cai pro caminho de catalog query / view_estimated_rows fallback. Com rows
-- realistas em fomovto, o join é estimado em N linhas, NL com inner
-- parametrizado fica caro, e o planner escolhe Hash Join (3 ODBC calls totais)
-- ou usa foeventos como outer (1 ODBC call cada).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '60s';

-- Aplicar o que prod tem: rows='0' explicitamente em fomovto_slow.
-- O setup-pg.sql cria com view_estimated_rows '1000000', mas em prod tem rows='0'.
ALTER FOREIGN TABLE dominio.fomovto_slow OPTIONS (ADD rows '0');

-- Inspeciona o plano: o anti-padrão é
--   Nested Loop
--     -> Foreign Scan (SybaseFdw Join)      [outer, rows=1 estimado]
--     -> Foreign Scan on foeventos_slow     [inner, com `param: Some(...)` em quals]
-- O sintoma exato é: FT join virou OUTER com rows=1 e foeventos virou INNER parametrizado.
-- O fix deve evitar essa topologia através de estimativa razoável da FT join.
DO $$
DECLARE
    plan_line text;
    last_fs_rows int := -1;
    saw_join_with_rows_1 boolean := false;
    saw_foeventos_parametrized boolean := false;
BEGIN
    FOR plan_line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT m.valor_cal, e.cpf, e.nome, ev.nome AS event_name, m.data
              FROM dominio.fomovto_slow m
              JOIN dominio.foempregados_slow e
                ON e.codi_emp = m.codi_emp AND e.i_empregados = m.i_empregados
              JOIN dominio.foeventos_slow ev
                ON ev.codi_emp = m.codi_emp AND ev.i_eventos = m.i_eventos
             WHERE m.codi_emp = 51800
               AND m.data BETWEEN '2024-01-01' AND '2024-12-31'
             ORDER BY e.nome, ev.nome
        $q$
    LOOP
        -- Captura rows estimado da última linha "Foreign Scan" anônima
        -- (i.e., sem "on <table>" — esse é o pattern do join pushdown).
        IF plan_line ~ 'Foreign Scan  \(cost=.*rows=([0-9]+)' THEN
            last_fs_rows := (regexp_match(plan_line, 'rows=([0-9]+)'))[1]::int;
        END IF;

        -- Confirma que aquela Foreign Scan era um SybaseFdw Join
        IF plan_line LIKE '%SybaseFdw Join:%' THEN
            IF last_fs_rows = 1 THEN
                saw_join_with_rows_1 := true;
            END IF;
            last_fs_rows := -1;
        END IF;

        -- Detecta foeventos com qual parametrizado (sintoma de NL inner com $param)
        IF plan_line LIKE '%Wrappers: quals%'
           AND plan_line LIKE '%i_eventos%'
           AND plan_line LIKE '%param: Some%' THEN
            saw_foeventos_parametrized := true;
        END IF;
    END LOOP;

    IF saw_join_with_rows_1 AND saw_foeventos_parametrized THEN
        RAISE EXCEPTION 'Plano regrediu: SybaseFdw Join estimado em rows=1 e foeventos como inner parametrizado. Esse é o anti-padrão do getPayrollData que em prod virou 55s+ timeout (5+ min de ODBC calls).';
    END IF;
END $$;

ROLLBACK;
