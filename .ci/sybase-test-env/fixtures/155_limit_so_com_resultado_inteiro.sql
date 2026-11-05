-- @desc: KOROK-224 (05/11/2026). O TOP N só saía com ORDER BY e, nesse caso,
-- mesmo com filtro local: `WHERE codi_emp = 51800 AND codi_sai % 2 = 0
-- ORDER BY codi_sai LIMIT 50` devolvia 25 linhas (o Sybase cortava antes do
-- filtro). Sem ORDER BY o LIMIT não era empurrado (ctvlancto LIMIT 1001: 23 s).
-- Agora o LIMIT só desce quando a FT sozinha é o resultado: uma relação, todos
-- os filtros e toda a ordenação empurrados, nenhum filtro ou ordenação em texto
-- (o Domínio compara sem distinguir maiúsculas) e ordenação só por coluna NOT
-- NULL (no SQL Anywhere o NULL vem primeiro).
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

SELECT count(*) AS pares FROM (SELECT codi_sai FROM dominio.efsaidas_slow
  WHERE codi_emp = 51800 AND codi_sai % 2 = 0 OFFSET 0) x \gset
SELECT pg_temp.confere(:pares > 50, 'base com poucas saídas pares');

-- 1. Filtro local + ORDER BY + LIMIT: 50 linhas, LIMIT não empurrado
SELECT count(*) AS n FROM (SELECT codi_sai FROM dominio.efsaidas_slow
  WHERE codi_emp = 51800 AND codi_sai % 2 = 0 ORDER BY codi_sai LIMIT 50) x \gset
SELECT pg_temp.confere(:n = 50, format('filtro local + ORDER BY + LIMIT 50 devolveu %s', :n));
SELECT pg_temp.confere(
    pg_temp.plano('SELECT codi_sai FROM dominio.efsaidas_slow
                    WHERE codi_emp = 51800 AND codi_sai % 2 = 0 ORDER BY codi_sai LIMIT 50')
      LIKE '%limit = None%',
    'LIMIT empurrado com filtro local');

-- 2. ORDER BY de expressão (não empurrável) + LIMIT: os 5 maiores de verdade
SELECT array_agg(codi_sai ORDER BY codi_sai) AS esperado FROM (
  SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800
   ORDER BY codi_sai % 7 DESC, codi_sai LIMIT 5 OFFSET 0) x \gset
SELECT array_agg(codi_sai ORDER BY codi_sai) AS obtido FROM (
  SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800
   ORDER BY codi_sai % 7 DESC, codi_sai LIMIT 5) x \gset
SELECT pg_temp.confere(:'esperado' = :'obtido', 'ORDER BY de expressão + LIMIT');

-- 3. Tudo empurrado, sem ORDER BY: TOP N no Sybase
SELECT pg_temp.confere(
    pg_temp.plano('SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800 LIMIT 5')
      LIKE '%limit = Some(Limit { count: 5, offset: 0 })%',
    'LIMIT sem ORDER BY não empurrado');
SELECT count(*) AS n FROM (SELECT codi_sai FROM dominio.efsaidas_slow
  WHERE codi_emp = 51800 LIMIT 5) x \gset
SELECT pg_temp.confere(:n = 5, 'LIMIT 5 sem ORDER BY');

-- 4. Tudo empurrado, com ORDER BY e OFFSET: as mesmas linhas da leitura inteira
SELECT array_agg(codi_sai ORDER BY codi_sai) AS esperado FROM (
  SELECT codi_sai FROM (SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800 OFFSET 0) t
   ORDER BY codi_sai DESC LIMIT 7 OFFSET 3) x \gset
SELECT array_agg(codi_sai ORDER BY codi_sai) AS obtido FROM (
  SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800
   ORDER BY codi_sai DESC LIMIT 7 OFFSET 3) x \gset
SELECT pg_temp.confere(:'esperado' = :'obtido', 'ORDER BY + LIMIT + OFFSET');

-- 5. Texto: o Sybase compara sem distinguir maiúsculas ('CLIENTE 3' casa
-- 'Cliente 3'); com TOP 3 ele devolvia 3, 6 e 9 e o filtro local deixava 1.
SELECT count(*) AS n FROM (SELECT codi_sai FROM dominio.efsaidas_slow
  WHERE codi_emp = 51800
    AND cliente_sai IN ('CLIENTE 3', 'Cliente 6', 'CLIENTE 9', 'Cliente 12', 'CLIENTE 15', 'Cliente 18')
  LIMIT 3) x \gset
SELECT pg_temp.confere(:n = 3, format('filtro de texto + LIMIT 3 devolveu %s', :n));

-- 6. Ordenação: só coluna NOT NULL e não texto (no SQL Anywhere o NULL vem primeiro)
SELECT pg_temp.confere(
    pg_temp.plano('SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800 ORDER BY data_sai LIMIT 5')
      LIKE '%limit = None%',
    'LIMIT empurrado com ORDER BY de coluna anulável');
SELECT pg_temp.confere(
    pg_temp.plano('SELECT codi_sai FROM dominio.efsaidas_slow WHERE codi_emp = 51800 ORDER BY codi_sai LIMIT 5')
      LIKE '%limit = Some(Limit { count: 5, offset: 0 })%',
    'LIMIT não empurrado com ORDER BY de coluna NOT NULL');

-- 7. Join com tabela local + ORDER BY + LIMIT: a FT não pode parar em N
CREATE TEMP TABLE pares AS SELECT g AS codi_sai FROM generate_series(0, 100000, 2) g;
ANALYZE pares;
SELECT count(*) AS n FROM (SELECT s.codi_sai FROM dominio.efsaidas_slow s
  JOIN pares p ON p.codi_sai = s.codi_sai
 WHERE s.codi_emp = 51800 ORDER BY s.codi_sai LIMIT 50) x \gset
SELECT pg_temp.confere(:n = 50, format('join local + ORDER BY + LIMIT 50 devolveu %s', :n));

ROLLBACK;
