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
    nome        text,
    cbo_2002    integer
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
-- Módulo de Folha (Payroll). Espelha a topologia em que os ex-pg_tests
-- consumiam tabelas Sybase reais — agora sustentado pelo schema em
-- .ci/sybase-test-env/sql/01_schema.sql + 02_data.sql.
-- ============================================================================

DROP FOREIGN TABLE IF EXISTS dominio.foferias_aquisitivos_slow CASCADE;
CREATE FOREIGN TABLE dominio.foferias_aquisitivos_slow (
    codi_emp                integer NOT NULL,
    i_empregados            integer NOT NULL,
    i_ferias_aquisitivos    integer NOT NULL,
    data_inicio             date,
    data_fim                date,
    dias_direito            numeric,
    dias_gozados            numeric,
    dias_abono              numeric,
    limite_para_gozo        date
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foferias_aquisitivos');

DROP FOREIGN TABLE IF EXISTS dominio.foferias_programacao_slow CASCADE;
CREATE FOREIGN TABLE dominio.foferias_programacao_slow (
    codi_emp                integer NOT NULL,
    i_empregados            integer NOT NULL,
    gozo_inicio             date,
    dias_gozo               numeric,
    i_ferias_aquisitivos    integer NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foferias_programacao');

DROP FOREIGN TABLE IF EXISTS dominio.foferias_slow CASCADE;
CREATE FOREIGN TABLE dominio.foferias_slow (
    codi_emp           integer NOT NULL,
    i_empregados       integer NOT NULL,
    inicio_aquisitivo  date,
    fim_aquisitivo     date,
    inicio_gozo        date,
    fim_gozo           date,
    dias_ferias        integer,
    data_aviso         date,
    data_pagto         date,
    proventos          numeric,
    descontos          numeric
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foferias');

DROP FOREIGN TABLE IF EXISTS dominio.fobancos_slow CASCADE;
CREATE FOREIGN TABLE dominio.fobancos_slow (
    i_bancos    integer NOT NULL,
    numero      integer NOT NULL,
    agencia     text NOT NULL,
    nome        text NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.fobancos');

DROP FOREIGN TABLE IF EXISTS dominio.foeventos_slow CASCADE;
CREATE FOREIGN TABLE dominio.foeventos_slow (
    codi_emp    integer NOT NULL,
    i_eventos   integer NOT NULL,
    nome        text NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foeventos');

DROP FOREIGN TABLE IF EXISTS dominio.foeventosbases_slow CASCADE;
CREATE FOREIGN TABLE dominio.foeventosbases_slow (
    codi_emp    integer NOT NULL,
    i_eventos   integer NOT NULL,
    i_cadbases  integer NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foeventosbases');

DROP FOREIGN TABLE IF EXISTS dominio.forescisoes_slow CASCADE;
CREATE FOREIGN TABLE dominio.forescisoes_slow (
    codi_emp        integer NOT NULL,
    i_empregados    integer NOT NULL,
    demissao        date,
    motivo_esocial  integer
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.forescisoes');

DROP FOREIGN TABLE IF EXISTS dominio.foccustos_slow CASCADE;
CREATE FOREIGN TABLE dominio.foccustos_slow (
    codi_emp    integer NOT NULL,
    i_ccustos   integer NOT NULL,
    nome        text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foccustos');

DROP FOREIGN TABLE IF EXISTS dominio.foafastamentos_tipos_slow CASCADE;
CREATE FOREIGN TABLE dominio.foafastamentos_tipos_slow (
    i_afastamentos  integer NOT NULL,
    descricao       text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foafastamentos_tipos');

-- FOVCHEQUE é VIEW no Sybase (sys.systable.count = 0): view_estimated_rows
-- evita que o planner trate como zero linhas e produza plano horrível.
DROP FOREIGN TABLE IF EXISTS dominio.fovcheque_slow CASCADE;
CREATE FOREIGN TABLE dominio.fovcheque_slow (
    cp_tipo         smallint NOT NULL,
    i_empregados    integer NOT NULL,
    nome            text NOT NULL,
    i_bancos        integer,
    codi_emp        integer NOT NULL,
    cp_tipo_process integer NOT NULL,
    competencia     date,
    cp_valor        numeric NOT NULL,
    cp_data_pagto   date NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.fovcheque', view_estimated_rows '500000');

-- FOMOVTO também é VIEW; mesmo tratamento.
DROP FOREIGN TABLE IF EXISTS dominio.fomovto_slow CASCADE;
CREATE FOREIGN TABLE dominio.fomovto_slow (
    codi_emp        integer NOT NULL,
    i_empregados    integer NOT NULL,
    i_eventos       integer NOT NULL,
    data            date NOT NULL,
    valor_cal       numeric NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.fomovto', view_estimated_rows '1000000');

-- ============================================================================
-- FOREIGN TABLES adicionais para as 12 famílias de queries de prod (Temp.sql).
-- Espelham bethadba.* de 03_prod_query_tables.sql.
-- ============================================================================

-- #1 saldo contábil histórico
DROP FOREIGN TABLE IF EXISTS dominio.ctvlancto CASCADE;
CREATE FOREIGN TABLE dominio.ctvlancto (
    codi_emp        integer NOT NULL,
    codi_cta        integer NOT NULL,
    codi_emp_plano  integer NOT NULL,
    tipo_cta        text    NOT NULL,
    vlor_lan        numeric(15,2) NOT NULL,
    data_lan        date    NOT NULL,
    i_lancto        integer NOT NULL
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.ctvlancto');

-- #2 / #7 saldos de imposto
DROP FOREIGN TABLE IF EXISTS dominio.efsdoimp CASCADE;
CREATE FOREIGN TABLE dominio.efsdoimp (
    codi_emp    integer NOT NULL,
    codi_imp    integer NOT NULL,
    data_sim    date    NOT NULL,
    sdev_sim    numeric(15,2)
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.efsdoimp');

DROP FOREIGN TABLE IF EXISTS dominio.efsdoimp_por_recolhimento CASCADE;
CREATE FOREIGN TABLE dominio.efsdoimp_por_recolhimento (
    codi_emp             integer NOT NULL,
    codi_imp             integer NOT NULL,
    codigo_recolhimento  text    NOT NULL,
    data_sim             date    NOT NULL,
    saldo_recolher       numeric(15,2)
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.efsdoimp_por_recolhimento');

-- #7 vigência de parâmetros
DROP FOREIGN TABLE IF EXISTS dominio.efparametro_vigencia CASCADE;
CREATE FOREIGN TABLE dominio.efparametro_vigencia (
    codi_emp     integer NOT NULL,
    vigencia_par date    NOT NULL,
    par_valor    numeric(15,2)
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.efparametro_vigencia');

-- #6 cadastro de impostos e vigências
DROP FOREIGN TABLE IF EXISTS dominio.geimposto CASCADE;
CREATE FOREIGN TABLE dominio.geimposto (
    codi_emp  integer NOT NULL,
    codi_imp  integer NOT NULL,
    nome_imp  text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.geimposto');

DROP FOREIGN TABLE IF EXISTS dominio.geimposto_vigencia CASCADE;
CREATE FOREIGN TABLE dominio.geimposto_vigencia (
    codi_emp     integer NOT NULL,
    codi_imp     integer NOT NULL,
    vigencia_imp date    NOT NULL,
    cdrn_imp     text,
    lanc_imp     text
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.geimposto_vigencia');

-- #11 provisões
DROP FOREIGN TABLE IF EXISTS dominio.foprovisoes CASCADE;
CREATE FOREIGN TABLE dominio.foprovisoes (
    codi_emp     integer NOT NULL,
    i_empregados integer NOT NULL,
    competencia  date    NOT NULL,
    tipo         smallint NOT NULL,
    avos         numeric(5,2),
    salario      numeric(15,2)
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foprovisoes');

DROP FOREIGN TABLE IF EXISTS dominio.foprovisoes_valores CASCADE;
CREATE FOREIGN TABLE dominio.foprovisoes_valores (
    codi_emp                integer NOT NULL,
    i_empregados            integer NOT NULL,
    competencia             date    NOT NULL,
    tipo                    smallint NOT NULL,
    tipo_valor              smallint NOT NULL,
    valor_adicional_ferias  numeric(15,2),
    valor_fgts              numeric(15,2),
    valor_inss_empresa      numeric(15,2),
    valor_inss_rat          numeric(15,2),
    valor_inss_terceiros    numeric(15,2),
    valor_medias_vantagens  numeric(15,2),
    valor_pis               numeric(15,2),
    valor                   numeric(15,2)
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foprovisoes_valores');

-- #12 alterações contratuais
DROP FOREIGN TABLE IF EXISTS dominio.foempregados_alteracao_contratual CASCADE;
CREATE FOREIGN TABLE dominio.foempregados_alteracao_contratual (
    codi_emp       integer NOT NULL,
    i_empregados   integer NOT NULL,
    data_alteracao date    NOT NULL,
    salario        numeric(15,2)
) SERVER sybase_test_server
  OPTIONS (table 'bethadba.foempregados_alteracao_contratual');

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
