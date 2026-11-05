-- @desc: KOROK-224 (05/11/2026). Na R-4020 (efentradas × effornece × efimpent,
-- com codi_emp nas três e IN de duas empresas), a efimpent entrava num NestLoop
-- parametrizado só por codi_emp = effornece.codi_emp, e o codi_ent ficava como
-- Join Filter: cada entrada trazia do Sybase todos os impostos da empresa
-- (148 mil linhas descartadas em produção). Sem estatísticas, todo ppi_rows
-- vale 1 e as parametrizações empatavam; agora a que empurra mais cláusulas
-- custa menos.
-- @expect: ok

BEGIN;
SET LOCAL statement_timeout = '120s';

CREATE FUNCTION pg_temp.confere(ok boolean, msg text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF ok IS NOT TRUE THEN RAISE EXCEPTION 'falhou: %', msg; END IF;
END $$;

CREATE FUNCTION pg_temp.plano(sql text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE l text; r text := '';
BEGIN
    FOR l IN EXECUTE 'EXPLAIN (VERBOSE, COSTS OFF) ' || sql LOOP
        r := r || l || E'\n';
    END LOOP;
    RETURN r;
END $$;

CREATE TEMP TABLE consulta AS SELECT $q$
SELECT e.codi_emp, e.codi_ent, sum(i.vlor_ien) AS total
FROM dominio.ppi_ent e
JOIN dominio.ppi_for f ON f.codi_emp = e.codi_emp AND f.codi_for = e.codi_for
    AND f.codi_emp IN (1, 2) AND f.tins_for IN (1, 4)
JOIN dominio.ppi_imp i ON i.codi_emp = e.codi_emp AND i.codi_ent = e.codi_ent
    AND i.codi_emp IN (1, 2) AND i.codi_imp IN (16, 22, 23, 24, 25)
WHERE e.codi_emp IN (1, 2) AND e.dent_ent BETWEEN '2026-09-01' AND '2026-09-30'
GROUP BY 1, 2
$q$::text AS sql;

SELECT pg_temp.plano(sql) AS p FROM consulta \gset
SELECT pg_temp.confere(
    :'p' NOT LIKE '%Join Filter: (i.codi_ent = e.codi_ent)%'
        AND :'p' LIKE '%Foreign Scan on dominio.ppi_imp i%(i.codi_ent = e.codi_ent)%',
    'efimpent sem o codi_ent no parâmetro: ' || :'p');

-- Mesmo resultado que o Hash Join, que lê cada tabela uma vez.
DO $$ BEGIN EXECUTE 'CREATE TEMP TABLE r1 AS ' || (SELECT sql FROM consulta); END $$;
SET LOCAL enable_nestloop = off;
DO $$ BEGIN EXECUTE 'CREATE TEMP TABLE r2 AS ' || (SELECT sql FROM consulta); END $$;
SELECT pg_temp.confere((SELECT count(*) FROM r1) = 60, 'esperava 60 entradas com imposto');
SELECT pg_temp.confere(NOT EXISTS (SELECT * FROM r1 EXCEPT SELECT * FROM r2)
    AND NOT EXISTS (SELECT * FROM r2 EXCEPT SELECT * FROM r1), 'NestLoop e Hash Join divergem');

ROLLBACK;
