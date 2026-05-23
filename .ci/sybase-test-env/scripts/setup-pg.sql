-- Setup do lado Postgres: cria extensão, server, foreign tables e tabelas
-- locais que aparecem nas reproduções de Bug #1 e Bug #2.
--
-- Topologia espelha produção:
--   dominio.<nome>_slow  → FOREIGN TABLE apontando para bethadba.<nome>
--   dominio.<nome>       → MATERIALIZED VIEW que SELECT FROM <nome>_slow
-- Exceção: dominio.efsaidas é uma FT DIRETA (sem MV intermediária) — espelha
-- a configuração de prod onde efsaidas_slow foi renomeada para efsaidas, que
-- é exatamente o caminho que aciona os bugs do PREPARE+EXECUTE.
--
-- Idempotente: pode ser re-executado.

CREATE EXTENSION IF NOT EXISTS wrappers;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'sybase_wrapper') THEN
        CREATE FOREIGN DATA WRAPPER sybase_wrapper
            HANDLER sybase_fdw_handler VALIDATOR sybase_fdw_validator;
    END IF;
END $$;

DROP SERVER IF EXISTS sybase_test_server CASCADE;
CREATE SERVER sybase_test_server
    FOREIGN DATA WRAPPER sybase_wrapper
    OPTIONS (
        host 'sybase',
        port '2638',
        database 'bethadba',
        "user" 'externo',
        password 'externo123'
    );

CREATE SCHEMA IF NOT EXISTS dominio;

-- ============================================================================
-- FOREIGN TABLES (sufixo _slow) — apontam direto pro Sybase
-- ============================================================================

DROP FOREIGN TABLE IF EXISTS dominio.efsaidas_slow CASCADE;
CREATE FOREIGN TABLE dominio.efsaidas_slow (
    codi_emp        integer NOT NULL,
    codi_sai        integer NOT NULL,
    chave_nfe_sai   text,
    situacao_sai    smallint,
    valor_sai       numeric(15,2),
    data_sai        date,
    cliente_sai     text,
    cnpj_dest       text,
    obs_sai         text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.efsaidas');

DROP FOREIGN TABLE IF EXISTS dominio.efentradas_slow CASCADE;
CREATE FOREIGN TABLE dominio.efentradas_slow (
    codi_emp        integer NOT NULL,
    codi_ent        integer NOT NULL,
    chave_nfe_ent   text,
    situacao_ent    smallint,
    valor_ent       numeric(15,2),
    data_ent        date,
    fornecedor_ent  text,
    cnpj_orig       text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.efentradas');

DROP FOREIGN TABLE IF EXISTS dominio.geempre_slow CASCADE;
CREATE FOREIGN TABLE dominio.geempre_slow (
    codi_emp  integer NOT NULL,
    cgce_emp  text,
    nome_emp  text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.geempre');

DROP FOREIGN TABLE IF EXISTS dominio.foempregados_slow CASCADE;
CREATE FOREIGN TABLE dominio.foempregados_slow (
    codi_emp                integer NOT NULL,
    i_empregados            integer NOT NULL,
    nome                    text,
    admissao                date,
    vinculo                 smallint NOT NULL,
    i_afastamentos          integer NOT NULL,
    i_cargos                integer NOT NULL,
    i_depto                 integer NOT NULL,
    i_ccustos               integer,
    i_bancos                integer,
    conta_corr              text,
    digito_conta_pagamento  text,
    tipo_conta              text,
    cpf                     text,
    matricula               text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foempregados');

DROP FOREIGN TABLE IF EXISTS dominio.focargos_slow CASCADE;
CREATE FOREIGN TABLE dominio.focargos_slow (
    codi_emp    integer NOT NULL,
    i_cargos    integer NOT NULL,
    nome        text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.focargos');

DROP FOREIGN TABLE IF EXISTS dominio.fodepto_slow CASCADE;
CREATE FOREIGN TABLE dominio.fodepto_slow (
    codi_emp    integer NOT NULL,
    i_depto     integer NOT NULL,
    nome        text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.fodepto');

-- ============================================================================
-- MATERIALIZED VIEWS (sem sufixo) — cache local do que está no Sybase.
-- IMPORTANTE: em produção `dominio.efsaidas` é DIRETAMENTE a FT (sem MV
-- intermediário). É a topologia que dispara o bug #2.
-- Para espelhar isso, criamos VIEW que aponta direto para efsaidas_slow:
-- ============================================================================

DROP VIEW IF EXISTS dominio.efsaidas CASCADE;
DROP MATERIALIZED VIEW IF EXISTS dominio.efsaidas CASCADE;
DROP FOREIGN TABLE IF EXISTS dominio.efsaidas CASCADE;
-- Em produção, efsaidas FOI renomeada de efsaidas_slow → uma única FT direta.
-- Espelhamos criando uma SEGUNDA FT com mesmas options apontando para a mesma
-- tabela Sybase. A FT efsaidas_slow continua existindo para testes legados.
CREATE FOREIGN TABLE dominio.efsaidas (
    codi_emp        integer NOT NULL,
    codi_sai        integer NOT NULL,
    chave_nfe_sai   text,
    situacao_sai    smallint,
    valor_sai       numeric(15,2),
    data_sai        date,
    cliente_sai     text,
    cnpj_dest       text,
    obs_sai         text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.efsaidas');

DROP MATERIALIZED VIEW IF EXISTS dominio.efentradas CASCADE;
CREATE MATERIALIZED VIEW dominio.efentradas AS
    SELECT * FROM dominio.efentradas_slow;

DROP MATERIALIZED VIEW IF EXISTS dominio.geempre CASCADE;
CREATE MATERIALIZED VIEW dominio.geempre AS
    SELECT * FROM dominio.geempre_slow;
CREATE INDEX IF NOT EXISTS idx_geempre_cnpj ON dominio.geempre(cgce_emp);

DROP MATERIALIZED VIEW IF EXISTS dominio.foempregados CASCADE;
CREATE MATERIALIZED VIEW dominio.foempregados AS
    SELECT * FROM dominio.foempregados_slow;

-- ============================================================================
-- Tabelas locais Postgres (espelham produção)
-- ============================================================================

CREATE TABLE IF NOT EXISTS customers_tax_numbers (
    customer_id     text    NOT NULL,
    tax_number      text    NOT NULL,
    type            text    NOT NULL,
    PRIMARY KEY (customer_id, tax_number, type)
);

CREATE TABLE IF NOT EXISTS nfe (
    id              bigserial PRIMARY KEY,
    customer_id     text    NOT NULL,
    access_key      text    NOT NULL,
    type            text    NOT NULL,
    date            date    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nfe_customer_date ON nfe(customer_id, date);
CREATE INDEX IF NOT EXISTS idx_nfe_access_key    ON nfe(access_key);

TRUNCATE customers_tax_numbers;
INSERT INTO customers_tax_numbers (customer_id, tax_number, type) VALUES
    ('cust-001', '11111111000111', 'CNPJ'),
    ('cust-001', '22222222000122', 'CNPJ'),
    ('cust-002', '33333333000133', 'CNPJ');

-- Populamos nfe com chaves que casam parcialmente com efsaidas para forçar a
-- subquery correlated a tocar a FT em todas as iterações.
-- 5000 linhas — alto o suficiente pra exercitar reuso prolongado de cached_conn.
TRUNCATE nfe;
INSERT INTO nfe (customer_id, access_key, type, date)
SELECT
    'cust-001',
    -- chave_nfe_sai = "<5 chars empresa><37 zeros + seq><01>"
    lpad((51800 + (i % 3))::text, 5, '0') ||
        repeat('0', 37 - length(i::text)) || i::text || '01',
    'Saída',
    '2025-01-01'::date + ((i % 365) || ' days')::interval
FROM generate_series(1, 5000) AS i;

REFRESH MATERIALIZED VIEW dominio.geempre;
REFRESH MATERIALIZED VIEW dominio.efentradas;
REFRESH MATERIALIZED VIEW dominio.foempregados;

ANALYZE customers_tax_numbers;
ANALYZE nfe;
ANALYZE dominio.geempre;

SELECT 'setup-pg.sql done' AS status;
