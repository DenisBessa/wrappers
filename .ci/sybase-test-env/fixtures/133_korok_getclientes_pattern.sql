-- @desc: Reproduz o padrão de `getClientes` (app/korok/contabil/...) que em
-- prod (2026-05-25) levou >18 min e crashou o backend Postgres em
-- supabase.bessa.digital.
--
-- Forma: duas CTEs MATERIALIZED com SUM/GROUP BY sobre FT, depois SELECT
-- INNER JOIN com FT principal + LEFT JOIN com a segunda CTE + filtro
-- `cte1.X > COALESCE(cte2.Y, 0)`. PG aplica o filtro `codi_emp = X` no
-- CTE Scan via constant-fold da equijoin → default eq selectivity (0.005)
-- colapsa HashAggregate rows=200 para CTE Scan rows=1. Com outer=1,
-- planner escolhe NL não-parametrizado, re-executando a Foreign Scan
-- full-table por cada row real da CTE (não 1, mas centenas).
--
-- Após o fix em supabase-wrappers/src/scan.rs (inflação do total_cost
-- não-parametrizado quando há `seen_ppis`), a parametrizada via codi_cli=$X
-- vira a path mais barata e cada execução transfere O(1) linhas em vez de
-- O(rows da FT).
--
-- Aqui mapeamos o triângulo (efclientes ↔ efsaidas/efsaidaspar/efsaidaspag)
-- para (foempregados ↔ fomovto × 2 CTEs) — fomovto serve como base para
-- ambas as CTEs porque o schema de teste não tem tabelas separadas de
-- parcelas/pagamentos. O ponto é o padrão estrutural, não os números.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '30s';

-- Inspeciona o plano: a FT principal (foempregados_slow) DEVE virar inner
-- parametrizado, isto é, o Wrappers: quals do Foreign Scan precisa conter
-- pelo menos um qual com `param: Some(...)`. Se o planner voltar a NL
-- unparameterized, o Foreign Scan aparece com `loops > 1` e quals só com
-- constantes — exatamente o anti-padrão de prod (2026-05-25, getClientes).
DO $$
DECLARE
    plan_line text;
    in_ft_block boolean := false;
    found_param_qual boolean := false;
    found_outer_ft boolean := false;
BEGIN
    FOR plan_line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            WITH "parc" AS MATERIALIZED (
                SELECT i_empregados, codi_emp, SUM(valor_cal) AS "totalParcela"
                  FROM dominio.fomovto_slow
                 WHERE codi_emp = 51800
                 GROUP BY 2, 1
            ),
            "pag" AS MATERIALIZED (
                SELECT i_empregados, codi_emp, SUM(valor_cal) AS "totalPago"
                  FROM dominio.fomovto_slow
                 WHERE codi_emp = 51800
                 GROUP BY 2, 1
            )
            SELECT e.nome, e.i_empregados
              FROM dominio.foempregados_slow e
              JOIN parc ON parc.codi_emp = e.codi_emp
                       AND parc.i_empregados = e.i_empregados
              LEFT JOIN pag ON pag.codi_emp = e.codi_emp
                           AND pag.i_empregados = e.i_empregados
             WHERE e.codi_emp = 51800
               AND parc."totalParcela" > COALESCE(pag."totalPago", 0)
             ORDER BY e.nome
             LIMIT 20
        $q$
    LOOP
        IF plan_line LIKE '%Foreign Scan on dominio.foempregados_slow%' THEN
            in_ft_block := true;
            found_outer_ft := true;
        ELSIF in_ft_block AND plan_line LIKE '%Wrappers: quals%' THEN
            IF plan_line LIKE '%param: Some%' THEN
                found_param_qual := true;
            END IF;
            in_ft_block := false;
        END IF;
    END LOOP;

    IF NOT found_outer_ft THEN
        RAISE EXCEPTION 'Foreign Scan on foempregados_slow não apareceu no plano.';
    END IF;
    IF NOT found_param_qual THEN
        RAISE EXCEPTION 'Plano regrediu: foempregados_slow não está parametrizado (Wrappers: quals sem param). Risco de NL × full FT scan por outer row.';
    END IF;
END $$;

WITH "parc" AS MATERIALIZED (
    SELECT i_empregados, codi_emp, SUM(valor_cal) AS "totalParcela"
      FROM dominio.fomovto_slow
     WHERE codi_emp = 51800
     GROUP BY 2, 1
),
"pag" AS MATERIALIZED (
    SELECT i_empregados, codi_emp, SUM(valor_cal) AS "totalPago"
      FROM dominio.fomovto_slow
     WHERE codi_emp = 51800
     GROUP BY 2, 1
)
SELECT e.nome, e.i_empregados
  FROM dominio.foempregados_slow e
  JOIN parc ON parc.codi_emp = e.codi_emp
           AND parc.i_empregados = e.i_empregados
  LEFT JOIN pag ON pag.codi_emp = e.codi_emp
               AND pag.i_empregados = e.i_empregados
 WHERE e.codi_emp = 51800
   AND parc."totalParcela" > COALESCE(pag."totalPago", 0)
 ORDER BY e.nome
 LIMIT 20;

COMMIT;
