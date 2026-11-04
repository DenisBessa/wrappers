-- @desc: KOROK-131. Cada consulta abaixo derrubou o Postgres de produção
-- (signal 11) ou devolveu dado errado sem erro. Cada uma é conferida contra o
-- mesmo cálculo feito no Postgres sobre as linhas cruas (CTE MATERIALIZED,
-- sem pushdown de agregação).
--   1. JOIN empurrado pelo join.rs + agregação (04/11, 16/10): o upper.rs lia
--      o JoinPlanData do joinrel como FdwState.
--   2. Constante no SELECT + agregação (11/10, 18/10, 29/10).
--   3. SELECT em ordem diferente do GROUP BY (TESTES-DOMINIO 5.5) e agregado
--      antes da coluna: a tupla era montada como [group_by, agregados].
--   4. WHERE que não vira Qual: o agregado remoto ignorava o filtro.
--   5. to_jsonb(linha): as colunas fora do WHERE vinham null.
--   6. IN sobre CHAR (bpchar[]) não era empurrado (TESTES-DOMINIO 5.9).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '30s';

CREATE FOREIGN TABLE dominio.k131_saidas_char (
    codi_emp      integer NOT NULL,
    codi_sai      integer NOT NULL,
    chave_nfe_sai char(44)
) SERVER sybase_test_server OPTIONS (table 'bethadba.efsaidas');

CREATE TEMP TABLE esperado AS
SELECT * FROM (
    WITH s AS MATERIALIZED (SELECT * FROM dominio.efsaidas_slow WHERE codi_emp = 51800 OFFSET 0)
    SELECT
        (SELECT count(*) FROM s) AS linhas,
        (SELECT sum(valor_sai) FROM s) AS total,
        (SELECT count(*) FROM s WHERE situacao_sai = 0) AS regulares,
        (SELECT count(*) FROM s WHERE codi_sai % 2 = 0) AS pares,
        (SELECT min(codi_sai) FROM s WHERE cliente_sai IS NOT NULL) AS sai_com_cliente,
        (SELECT jsonb_agg(x ORDER BY x) FROM (SELECT situacao_sai || '/' || codi_emp || '/' || count(*) AS x FROM s GROUP BY codi_emp, situacao_sai) g) AS grupos
) e;

DO $$
DECLARE
    e esperado;
    n bigint;
    t numeric;
    n2 bigint;
    t2 numeric;
    j jsonb;
    c text;
    l text;
BEGIN
    SELECT * INTO e FROM esperado;
    IF e.linhas = 0 THEN RAISE EXCEPTION 'fixture sem dados para 51800'; END IF;

    -- 1. JOIN empurrado pelo join.rs + agregação
    WITH m AS MATERIALIZED (SELECT * FROM dominio.fomovto_slow WHERE codi_emp = 51800 OFFSET 0),
         f AS MATERIALIZED (SELECT * FROM dominio.foempregados_slow WHERE codi_emp = 51800 OFFSET 0)
    SELECT count(*), sum(m.valor_cal) INTO n2, t2
      FROM m JOIN f ON m.i_empregados = f.i_empregados AND m.codi_emp = f.codi_emp;
    SELECT sum(x.n), sum(x.t) INTO n, t
      FROM (SELECT f.i_empregados, count(*) AS n, sum(m.valor_cal) AS t
              FROM dominio.fomovto_slow m
              JOIN dominio.foempregados_slow f
                ON m.i_empregados = f.i_empregados AND m.codi_emp = f.codi_emp
             WHERE m.codi_emp = 51800
             GROUP BY 1) x;
    IF n2 = 0 OR n <> n2 OR t <> t2 THEN RAISE EXCEPTION '1: % / % <> % / %', n, t, n2, t2; END IF;

    -- 2. constante + agregação
    SELECT x.n, x.t INTO n, t FROM (SELECT 'saidas' AS k, count(*) AS n, sum(valor_sai) AS t
        FROM dominio.efsaidas_slow WHERE codi_emp = 51800) x;
    IF n <> e.linhas OR t <> e.total THEN RAISE EXCEPTION '2: % / % <> % / %', n, t, e.linhas, e.total; END IF;

    -- 3. ordem do SELECT diferente do GROUP BY, agregado antes da coluna
    SELECT jsonb_agg(x ORDER BY x) INTO j FROM (
        SELECT g.situacao_sai || '/' || g.codi_emp || '/' || g.n AS x
          FROM (SELECT count(*) AS n, situacao_sai, codi_emp FROM dominio.efsaidas_slow
                 WHERE codi_emp = 51800 GROUP BY codi_emp, situacao_sai) g) y;
    IF j <> e.grupos THEN RAISE EXCEPTION '3: % <> %', j, e.grupos; END IF;

    -- 4. WHERE que fica local
    SELECT count(*) INTO n FROM dominio.efsaidas_slow WHERE codi_emp = 51800 AND codi_sai % 2 = 0;
    IF n <> e.pares THEN RAISE EXCEPTION '4: % <> %', n, e.pares; END IF;

    -- 5. linha inteira: cliente_sai fica fora do WHERE
    SELECT to_jsonb(s) ->> 'cliente_sai' INTO c FROM dominio.efsaidas_slow s
     WHERE codi_emp = 51800 AND codi_sai = e.sai_com_cliente;
    IF c IS NULL THEN RAISE EXCEPTION '5: to_jsonb(linha) sem cliente_sai'; END IF;

    -- 6. IN sobre CHAR
    SELECT chave_nfe_sai INTO c FROM dominio.efsaidas_slow
     WHERE codi_emp = 51800 AND chave_nfe_sai IS NOT NULL LIMIT 1;
    SELECT count(*) INTO n FROM dominio.k131_saidas_char
     WHERE codi_emp = 51800 AND chave_nfe_sai IN (c::char(44), 'nenhuma'::char(44));
    IF n = 0 THEN RAISE EXCEPTION '6: IN sobre CHAR não achou %', c; END IF;
    n := 0;
    FOR l IN EXECUTE 'EXPLAIN (VERBOSE) SELECT codi_sai FROM dominio.k131_saidas_char
                      WHERE codi_emp = 51800 AND chave_nfe_sai IN (''a'', ''b'')' LOOP
        IF l LIKE '%Wrappers: quals = %chave_nfe_sai%' THEN n := 1; END IF;
    END LOOP;
    IF n = 0 THEN RAISE EXCEPTION '6: IN sobre CHAR fora do Sybase'; END IF;
END $$;

EXPLAIN (VERBOSE) SELECT count(*), codi_emp FROM dominio.efsaidas_slow
 WHERE codi_emp = 51800 GROUP BY codi_emp;

ROLLBACK;
