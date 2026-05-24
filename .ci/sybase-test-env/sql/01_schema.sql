-- Schema mínimo da Domínio Contábil (bethadba) para testes do sybase_fdw.
-- Espelha as tabelas/colunas que aparecem nas reproduções dos bugs e nos
-- tests.rs existentes. Quantidade de colunas é proposital: o bug #1 envolve
-- full-scan em FT com 30+ colunas (efsaidas), e o bug #2 depende da existência
-- das colunas chave referenciadas pelas subqueries.
--
-- Convenção SQL Anywhere: TIMESTAMP é equivalente a DATETIME do ASE.

-- Todas as tabelas são criadas no schema do usuário bethadba (criado em
-- 00_users.sql). Esse script roda conectado como externo (DBA), e cada
-- CREATE TABLE qualifica com bethadba.<tabela> para que o owner correto fique
-- registrado em sys.systable.

-- Tabela de empresas (referenciada via geempre.codi_emp por outras FTs)
CREATE TABLE bethadba.geempre (
    codi_emp        INTEGER NOT NULL,
    cgce_emp        VARCHAR(20),
    nome_emp        VARCHAR(120),
    PRIMARY KEY (codi_emp)
);

-- Notas fiscais de saída (NF-e emitida). Espelha bethadba.efsaidas.
CREATE TABLE bethadba.efsaidas (
    codi_emp        INTEGER NOT NULL,
    codi_sai        INTEGER NOT NULL,
    chave_nfe_sai   VARCHAR(44),
    situacao_sai    SMALLINT,
    valor_sai       NUMERIC(15,2),
    data_sai        DATE,
    cliente_sai     VARCHAR(120),
    cnpj_dest       VARCHAR(20),
    obs_sai         LONG VARCHAR,
    PRIMARY KEY (codi_emp, codi_sai)
);

-- Notas fiscais de entrada (NF-e recebida). Espelha bethadba.efentradas.
CREATE TABLE bethadba.efentradas (
    codi_emp        INTEGER NOT NULL,
    codi_ent        INTEGER NOT NULL,
    chave_nfe_ent   VARCHAR(44),
    situacao_ent    SMALLINT,
    valor_ent       NUMERIC(15,2),
    data_ent        DATE,
    fornecedor_ent  VARCHAR(120),
    cnpj_orig       VARCHAR(20),
    PRIMARY KEY (codi_emp, codi_ent)
);

-- Tabela de empregados.
CREATE TABLE bethadba.foempregados (
    codi_emp         INTEGER NOT NULL,
    i_empregados     INTEGER NOT NULL,
    nome             VARCHAR(120),
    admissao         DATE,
    vinculo          SMALLINT NOT NULL,
    i_afastamentos   INTEGER NOT NULL,
    i_cargos         INTEGER NOT NULL,
    i_depto          INTEGER NOT NULL,
    i_ccustos        INTEGER,
    i_bancos         INTEGER,
    conta_corr       VARCHAR(40),
    digito_conta_pagamento VARCHAR(5),
    tipo_conta       VARCHAR(5),
    cpf              VARCHAR(15),
    matricula        VARCHAR(20),
    PRIMARY KEY (codi_emp, i_empregados)
);

CREATE TABLE bethadba.focargos (
    codi_emp    INTEGER NOT NULL,
    i_cargos    INTEGER NOT NULL,
    nome        VARCHAR(60),
    cbo_2002    INTEGER,
    PRIMARY KEY (codi_emp, i_cargos)
);

CREATE TABLE bethadba.fodepto (
    codi_emp    INTEGER NOT NULL,
    i_depto     INTEGER NOT NULL,
    nome        VARCHAR(60),
    PRIMARY KEY (codi_emp, i_depto)
);

-- ============================================================================
-- Tabelas do módulo de Folha (Payroll). Espelham as estruturas referenciadas
-- pelos pg_tests migrados em wrappers/src/fdw/sybase_fdw/tests.rs.
-- Mantemos o mesmo conjunto de colunas mínimo; o dataset em 02_data.sql é
-- pequeno mas determinístico, suficiente pra exercitar pushdown, joins,
-- agregação e subqueries correlated.
-- ============================================================================

-- Períodos aquisitivos de férias.
CREATE TABLE bethadba.foferias_aquisitivos (
    codi_emp                INTEGER NOT NULL,
    i_empregados            INTEGER NOT NULL,
    i_ferias_aquisitivos    INTEGER NOT NULL,
    data_inicio             DATE,
    data_fim                DATE,
    dias_direito            NUMERIC(5,2),
    dias_gozados            NUMERIC(5,2),
    dias_abono              NUMERIC(5,2),
    limite_para_gozo        DATE,
    PRIMARY KEY (codi_emp, i_empregados, i_ferias_aquisitivos)
);

-- Programação de gozo de férias (planejada).
CREATE TABLE bethadba.foferias_programacao (
    codi_emp                INTEGER NOT NULL,
    i_empregados            INTEGER NOT NULL,
    gozo_inicio             DATE NOT NULL,
    dias_gozo               NUMERIC(5,2),
    i_ferias_aquisitivos    INTEGER NOT NULL,
    PRIMARY KEY (codi_emp, i_empregados, i_ferias_aquisitivos, gozo_inicio)
);

-- Férias efetivamente gozadas (folha de pagamento).
CREATE TABLE bethadba.foferias (
    codi_emp           INTEGER NOT NULL,
    i_empregados       INTEGER NOT NULL,
    inicio_aquisitivo  DATE NOT NULL,
    fim_aquisitivo     DATE,
    inicio_gozo        DATE,
    fim_gozo           DATE,
    dias_ferias        INTEGER,
    data_aviso         DATE,
    data_pagto         DATE,
    proventos          NUMERIC(15,2),
    descontos          NUMERIC(15,2),
    PRIMARY KEY (codi_emp, i_empregados, inicio_aquisitivo)
);

-- Bancos cadastrados.
CREATE TABLE bethadba.fobancos (
    i_bancos    INTEGER NOT NULL,
    numero      INTEGER NOT NULL,
    agencia     VARCHAR(20) NOT NULL,
    nome        VARCHAR(60) NOT NULL,
    PRIMARY KEY (i_bancos)
);

-- Cadastro de eventos da folha.
CREATE TABLE bethadba.foeventos (
    codi_emp    INTEGER NOT NULL,
    i_eventos   INTEGER NOT NULL,
    nome        VARCHAR(60) NOT NULL,
    PRIMARY KEY (codi_emp, i_eventos)
);

-- Associação evento × base (FGTS, INSS, IRRF etc.).
CREATE TABLE bethadba.foeventosbases (
    codi_emp    INTEGER NOT NULL,
    i_eventos   INTEGER NOT NULL,
    i_cadbases  INTEGER NOT NULL,
    PRIMARY KEY (codi_emp, i_eventos, i_cadbases)
);

-- Rescisões.
CREATE TABLE bethadba.forescisoes (
    codi_emp        INTEGER NOT NULL,
    i_empregados    INTEGER NOT NULL,
    demissao        DATE,
    motivo_esocial  INTEGER,
    PRIMARY KEY (codi_emp, i_empregados)
);

-- Centros de custo.
CREATE TABLE bethadba.foccustos (
    codi_emp    INTEGER NOT NULL,
    i_ccustos   INTEGER NOT NULL,
    nome        VARCHAR(60),
    PRIMARY KEY (codi_emp, i_ccustos)
);

-- Tipos de afastamento.
CREATE TABLE bethadba.foafastamentos_tipos (
    i_afastamentos  INTEGER NOT NULL,
    descricao       VARCHAR(120),
    PRIMARY KEY (i_afastamentos)
);

-- Cheques de pagamento (vencimento ↔ valor). Tabela-base que sustenta a
-- view FOVCHEQUE: em produção FOVCHEQUE é uma VIEW (sys.systable.count = 0).
-- Aqui criamos a VIEW para preservar o comportamento de planejamento (view_estimated_rows).
CREATE TABLE bethadba.focheque (
    cp_tipo             SMALLINT NOT NULL,
    i_empregados        INTEGER NOT NULL,
    i_bancos            INTEGER,
    codi_emp            INTEGER NOT NULL,
    cp_tipo_process     INTEGER NOT NULL,
    competencia         DATE,
    cp_valor            NUMERIC(15,2) NOT NULL,
    cp_data_pagto       DATE NOT NULL,
    PRIMARY KEY (codi_emp, i_empregados, cp_tipo, cp_tipo_process, cp_data_pagto)
);

CREATE VIEW bethadba.fovcheque AS
    SELECT fc.cp_tipo,
           fc.i_empregados,
           e.nome,
           fc.i_bancos,
           fc.codi_emp,
           fc.cp_tipo_process,
           fc.competencia,
           fc.cp_valor,
           fc.cp_data_pagto
      FROM bethadba.focheque fc
      JOIN bethadba.foempregados e ON e.codi_emp = fc.codi_emp
                                  AND e.i_empregados = fc.i_empregados;

-- Movimento (lançamentos) da folha. Base da view FOMOVTO.
CREATE TABLE bethadba.fomovto_base (
    codi_emp        INTEGER NOT NULL,
    i_empregados    INTEGER NOT NULL,
    i_eventos       INTEGER NOT NULL,
    data            DATE NOT NULL,
    valor_cal       NUMERIC(15,2) NOT NULL,
    PRIMARY KEY (codi_emp, i_empregados, i_eventos, data)
);

CREATE VIEW bethadba.fomovto AS
    SELECT codi_emp, i_empregados, i_eventos, data, valor_cal
      FROM bethadba.fomovto_base;

-- Índices nos campos PK existem implicitamente. Propositalmente NÃO criamos
-- índice em chave_nfe_sai/chave_nfe_ent para reproduzir o caso "full scan
-- por filtro sem índice" do bug #1.

-- Concede acesso ao usuário 'externo'. dbinit já criou 'externo' como DBA
-- (passamos -dba externo,externo123). Tudo pertence a externo então não
-- precisa GRANT — manter um schema 'bethadba' que aliase para o user 'externo'
-- emularia a produção, mas para o que importa nos testes o nome do schema
-- referenciado no FOREIGN TABLE options vai ser 'externo' (nome do owner).
