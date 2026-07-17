-- @desc: #6 (Temp.sql) geimposto_vigencia JOIN geimposto com filtro só num lado.
-- Garante a forma EFICIENTE: (a) os 3 quals do lado filtrado (gv) são empurrados;
-- (b) geimposto NÃO sofre full-scan sem filtro — ou o join é empurrado como SQL
-- único, ou geimposto é consultado parametrizado por linha (param-bound), nunca
-- um Foreign Scan de geimposto com quals=[] (que traria a tabela inteira pro PG).
-- Observação: hoje o planner escolhe Nested Loop parametrizado (não join pushdown);
-- ambos evitam o full-scan, mas o join pushdown (1 SQL remoto) seria ideal quando
-- gv retorna muitas linhas. A fixture documenta qual caminho foi escolhido.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

DO $$
DECLARE
    line          text;
    in_g_scan     boolean := false;   -- estamos dentro do Foreign Scan de geimposto g?
    gv_quals      text := NULL;
    g_quals       text := NULL;
    join_pushed   boolean := false;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT gv.cdrn_imp, gv.codi_emp, gv.codi_imp, gv.lanc_imp,
                   g.nome_imp, gv.vigencia_imp
              FROM dominio.geimposto_vigencia gv
              JOIN dominio.geimposto g
                ON g.codi_emp = gv.codi_emp AND g.codi_imp = gv.codi_imp
             WHERE gv.codi_emp IN (51800, 51801, 51802)
               AND gv.codi_imp IN (1, 2, 3, 4, 5, 6, 7, 8)
               AND gv.vigencia_imp <= DATE '2024-12-31'
        $q$
    LOOP
        IF line LIKE '%SybaseFdw Join: Remote SQL:%' THEN join_pushed := true; END IF;
        IF line LIKE '%Foreign Scan on dominio.geimposto_vigencia%' THEN in_g_scan := false; END IF;
        IF line LIKE '%Foreign Scan on dominio.geimposto %'
           OR line LIKE '%Foreign Scan on dominio.geimposto(%' THEN in_g_scan := true; END IF;
        IF line LIKE '%Wrappers: quals =%' THEN
            IF in_g_scan THEN g_quals := line; ELSE gv_quals := line; END IF;
        END IF;
    END LOOP;

    IF NOT join_pushed THEN
        -- Caminho Nested Loop: valida os dois lados.
        IF gv_quals IS NULL OR gv_quals NOT LIKE '%codi_emp%'
           OR gv_quals NOT LIKE '%codi_imp%' OR gv_quals NOT LIKE '%vigencia_imp%' THEN
            RAISE EXCEPTION '#6 lado filtrado (gv) sem os 3 quals empurrados: %', gv_quals;
        END IF;
        -- geimposto deve ser parametrizado (param-bound), NUNCA scan cheio (quals=[]).
        IF g_quals IS NULL THEN
            RAISE EXCEPTION '#6 nao achei o scan de geimposto no plano';
        END IF;
        IF g_quals LIKE '%quals = []%' THEN
            RAISE EXCEPTION '#6 geimposto sofre FULL SCAN (quals=[]) — traz tabela inteira: %', g_quals;
        END IF;
        IF g_quals NOT LIKE '%param: Some%' THEN
            RAISE EXCEPTION '#6 geimposto nao esta parametrizado nem join-pushed: %', g_quals;
        END IF;
        RAISE NOTICE '#6 geimposto: Nested Loop PARAMETRIZADO (nao full-scan). Join pushdown seria melhor p/ N grande.';
    ELSE
        RAISE NOTICE '#6 geimposto: JOIN PUSHDOWN (1 SQL remoto) — otimo.';
    END IF;
END $$;

-- Executa: precisa completar rapido.
SELECT gv.cdrn_imp, gv.codi_emp, gv.codi_imp, gv.lanc_imp, g.nome_imp, gv.vigencia_imp
  FROM dominio.geimposto_vigencia gv
  JOIN dominio.geimposto g
    ON g.codi_emp = gv.codi_emp AND g.codi_imp = gv.codi_imp
 WHERE gv.codi_emp IN (51800, 51801, 51802)
   AND gv.codi_imp IN (1, 2, 3, 4, 5, 6, 7, 8)
   AND gv.vigencia_imp <= DATE '2024-12-31'
 ORDER BY gv.codi_emp, gv.codi_imp, gv.vigencia_imp;

COMMIT;
