-- @desc: Reproduz bug de binding de parâmetro VARCHAR (PG type 1043) descoberto
-- após o deploy do fix `rows='0'` em prod (2026-05-26): `getDominioId` em
-- app/korok/pessoal passou a falhar com "Record is not found" mesmo com dados
-- presentes em customers_tax_numbers + dominio.geempre.
--
-- Forma: SELECT FROM tabela_local JOIN ft ON ft.text_col = local.varchar_col
-- WHERE local.pk = X. PG escolhe NL outer=local inner=Foreign Scan(ft) com qual
-- parametrizado `text_col = $param`.
--
-- Bug: `supabase-wrappers/src/interface.rs::from_polymorphic_datum` só trata
-- TEXTOID (25), não VARCHAROID (1043) — então quando o param é varchar, a
-- conversão Datum→Cell falha, `qual.value` permanece no placeholder
-- `Cell::I64(0)` (inserido em qual.rs:247), e o FDW deparseia o SQL como
-- `cgce_emp = 0`. Sybase coage tipos e retorna rows que não satisfazem a
-- equality real (em prod retornava 3 rows aleatórios, filtrados localmente,
-- → query principal acabava com `rows=0` no resultado).
--
-- O sintoma exato em prod: `EXPLAIN` mostrava
-- `Wrappers: quals = [..., type_oid: 1043, eval_value: Mutex { data: None }]`
-- e `Filter: ... Rows Removed by Filter: 3`.
--
-- Fix: adicionar `PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)` (e demais variantes
-- text-like: BPCHAR, NAME) no match de `from_polymorphic_datum`, mapeando para
-- `Cell::String` igual TEXT.
-- @expect: ok

BEGIN;

-- Tabela local com varchar (mimic customers_tax_numbers.tax_number em prod).
CREATE TEMP TABLE ctn_varchar (
    customer_id text NOT NULL,
    tax_number varchar NOT NULL,
    PRIMARY KEY (customer_id)
);
INSERT INTO ctn_varchar VALUES
    ('cust-001', '11111111000111'),
    ('cust-002', '33333333000133');

-- Sanity: a JOIN deveria retornar a linha matching ('11111111000111' → codi_emp=51800).
DO $$
DECLARE
    plan_line text;
    saw_data_none boolean := false;
    actual_rows int := -1;
BEGIN
    -- Garante NL inner parametrizado (cenário onde o bug fica visível).
    SET LOCAL enable_hashjoin = off;
    SET LOCAL enable_mergejoin = off;

    FOR plan_line IN
        EXECUTE $q$
            EXPLAIN (VERBOSE, ANALYZE)
            SELECT g.codi_emp
              FROM ctn_varchar c
              JOIN dominio.geempre_slow g ON g.cgce_emp = c.tax_number
             WHERE c.customer_id = 'cust-001'
        $q$
    LOOP
        IF plan_line LIKE '%Wrappers: quals%'
           AND plan_line LIKE '%type_oid: 1043%'
           AND plan_line LIKE '%data: None%' THEN
            saw_data_none := true;
        END IF;
        -- captura rows do Foreign Scan no NL inner
        IF plan_line ~ 'Foreign Scan on dominio\.geempre_slow' THEN
            actual_rows := (regexp_match(plan_line, 'actual time=[^ ]+ rows=([0-9]+)'))[1]::int;
        END IF;
    END LOOP;

    IF saw_data_none THEN
        RAISE EXCEPTION 'Plano regrediu: param eval falhou (eval_value=None) com type_oid=1043 (varchar). Bug em supabase-wrappers::interface.rs::from_polymorphic_datum — VARCHAROID não está no match. SQL enviado ao Sybase com placeholder I64(0).';
    END IF;
END $$;

-- Validação direta: a JOIN deve retornar 1 linha.
DO $$
DECLARE
    result_count int;
BEGIN
    SET LOCAL enable_hashjoin = off;
    SET LOCAL enable_mergejoin = off;
    SELECT count(*) INTO result_count
      FROM ctn_varchar c
      JOIN dominio.geempre_slow g ON g.cgce_emp = c.tax_number
     WHERE c.customer_id = 'cust-001';

    IF result_count <> 1 THEN
        RAISE EXCEPTION 'Esperava 1 row, obteve %. Param eval de VARCHAR não está vinculando corretamente.', result_count;
    END IF;
END $$;

ROLLBACK;
