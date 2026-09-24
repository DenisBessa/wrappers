-- @desc: #5 (Temp.sql) Padrao "OFFSET antes do WHERE" (a query ad hoc de
-- dominio.syscolumns). O OFFSET dentro do subselect vira uma barreira: o WHERE/ILIKE
-- externo NAO desce para o foreign scan, entao o remoto produz/descarta linhas antes
-- de filtrar (quals = []). Esta fixture prova o problema E a correcao: mover o WHERE
-- para dentro do mesmo SELECT (sem OFFSET) faz o filtro ser empurrado. Reproduzido
-- com efsaidas (o comportamento e agnostico de tabela; syscolumns e so o caso ad hoc).
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '15s';

-- (A) Forma problematica: OFFSET antes do WHERE -> foreign scan com quals=[]
DO $$
DECLARE
    line       text;
    fs_quals   text := NULL;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT codi_emp, cliente_sai
              FROM (SELECT codi_emp, cliente_sai FROM dominio.efsaidas_slow OFFSET 10) s
             WHERE cliente_sai ILIKE '%Cliente 1%'
        $q$
    LOOP
        IF line LIKE '%Wrappers: quals =%' THEN fs_quals := line; END IF;
    END LOOP;
    IF fs_quals IS NULL THEN RAISE EXCEPTION '#5 sem foreign scan'; END IF;
    IF fs_quals NOT LIKE '%quals = []%' THEN
        RAISE NOTICE '#5 (A) inesperado: OFFSET nao bloqueou pushdown? -> %', fs_quals;
    ELSE
        RAISE NOTICE '#5 (A) confirmado: OFFSET-antes-de-WHERE => foreign scan quals=[] (ILIKE filtrado local, scan cheio remoto)';
    END IF;
END $$;

-- (B) Forma corrigida: WHERE dentro do mesmo SELECT -> ILIKE empurrado
DO $$
DECLARE
    line       text;
    fs_quals   text := NULL;
BEGIN
    FOR line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE)
            SELECT codi_emp, cliente_sai
              FROM dominio.efsaidas_slow
             WHERE cliente_sai ILIKE '%Cliente 1%'
        $q$
    LOOP
        IF line LIKE '%Wrappers: quals =%' THEN fs_quals := line; END IF;
    END LOOP;
    IF fs_quals IS NULL THEN RAISE EXCEPTION '#5 (B) sem foreign scan'; END IF;
    IF fs_quals NOT LIKE '%cliente_sai%' THEN
        RAISE EXCEPTION '#5 (B) ILIKE deveria empurrar com WHERE inline, mas nao: %', fs_quals;
    END IF;
    RAISE NOTICE '#5 (B) corrigido: WHERE inline => ILIKE empurrado -> %', fs_quals;
END $$;

-- (C) EXECUTA o ILIKE empurrado. Antes do fix de deparse de `~~*` no sybase_fdw,
-- isto quebrava com "SQL Anywhere Error -131: Syntax error near '~'" (o FDW emitia
-- `~~*` literal). Depois do fix (UPPER(col) LIKE UPPER(pat)), retorna linhas.
SELECT count(*) AS ilike_rows
  FROM dominio.efsaidas_slow
 WHERE cliente_sai ILIKE '%cliente 1%';   -- minusculo de proposito: prova case-insensitive

COMMIT;
