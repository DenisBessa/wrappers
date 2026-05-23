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
    PRIMARY KEY (codi_emp, i_cargos)
);

CREATE TABLE bethadba.fodepto (
    codi_emp    INTEGER NOT NULL,
    i_depto     INTEGER NOT NULL,
    nome        VARCHAR(60),
    PRIMARY KEY (codi_emp, i_depto)
);

-- Índices nos campos PK existem implicitamente. Propositalmente NÃO criamos
-- índice em chave_nfe_sai/chave_nfe_ent para reproduzir o caso "full scan
-- por filtro sem índice" do bug #1.

-- Concede acesso ao usuário 'externo'. dbinit já criou 'externo' como DBA
-- (passamos -dba externo,externo123). Tudo pertence a externo então não
-- precisa GRANT — manter um schema 'bethadba' que aliase para o user 'externo'
-- emularia a produção, mas para o que importa nos testes o nome do schema
-- referenciado no FOREIGN TABLE options vai ser 'externo' (nome do owner).
