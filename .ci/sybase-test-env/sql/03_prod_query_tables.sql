-- Tabelas bethadba adicionais para reproduzir as 12 famílias de queries
-- problemáticas capturadas em produção (Temp.sql). Espelham as colunas
-- referenciadas por cada família; volume pequeno mas suficiente para
-- exercitar pushdown (WHERE / agregação / join). Idempotente: dropa antes
-- de criar, então pode ser re-aplicado com `make setup-sa-extra`.
--
-- Convenção SQL Anywhere (Watcom-SQL), igual a 01_schema.sql.

-- ============================================================================
-- #1 — Saldo contábil histórico. Em prod `ctvlancto` é FT sobre view/UNION;
-- aqui é tabela-base. O SUM(CASE WHEN tipo_cta = ... THEN vlor ELSE -vlor) +
-- GROUP BY roda sobre TODO o histórico da empresa (filtro só tem limite
-- superior de data). Colunas mínimas do SELECT real.
-- ============================================================================
DROP TABLE IF EXISTS bethadba.ctvlancto;
CREATE TABLE bethadba.ctvlancto (
    codi_emp        INTEGER NOT NULL,
    codi_cta        INTEGER NOT NULL,
    codi_emp_plano  INTEGER NOT NULL,
    tipo_cta        VARCHAR(1)  NOT NULL,   -- 'D' débito / 'C' crédito
    vlor_lan        NUMERIC(15,2) NOT NULL,
    data_lan        DATE NOT NULL,
    i_lancto        INTEGER NOT NULL,
    PRIMARY KEY (codi_emp, i_lancto)
);

-- ============================================================================
-- #2 / #7 — Saldos de imposto apurados (efsdoimp) e por recolhimento.
-- ============================================================================
DROP TABLE IF EXISTS bethadba.efsdoimp;
CREATE TABLE bethadba.efsdoimp (
    codi_emp    INTEGER NOT NULL,
    codi_imp    INTEGER NOT NULL,
    data_sim    DATE NOT NULL,
    sdev_sim    NUMERIC(15,2),
    PRIMARY KEY (codi_emp, codi_imp, data_sim)
);

DROP TABLE IF EXISTS bethadba.efsdoimp_por_recolhimento;
CREATE TABLE bethadba.efsdoimp_por_recolhimento (
    codi_emp             INTEGER NOT NULL,
    codi_imp             INTEGER NOT NULL,
    codigo_recolhimento  VARCHAR(10) NOT NULL,
    data_sim             DATE NOT NULL,
    saldo_recolher       NUMERIC(15,2),
    PRIMARY KEY (codi_emp, codi_imp, codigo_recolhimento, data_sim)
);

-- ============================================================================
-- #7 — Vigência de parâmetros fiscais (só filtro por limite de vigência).
-- ============================================================================
DROP TABLE IF EXISTS bethadba.efparametro_vigencia;
CREATE TABLE bethadba.efparametro_vigencia (
    codi_emp     INTEGER NOT NULL,
    vigencia_par DATE NOT NULL,
    par_valor    NUMERIC(15,2),
    PRIMARY KEY (codi_emp, vigencia_par)
);

-- ============================================================================
-- #6 — Cadastro de impostos e suas vigências (join FT×FT com filtro só num lado).
-- ============================================================================
DROP TABLE IF EXISTS bethadba.geimposto;
CREATE TABLE bethadba.geimposto (
    codi_emp  INTEGER NOT NULL,
    codi_imp  INTEGER NOT NULL,
    nome_imp  VARCHAR(60),
    PRIMARY KEY (codi_emp, codi_imp)
);

DROP TABLE IF EXISTS bethadba.geimposto_vigencia;
CREATE TABLE bethadba.geimposto_vigencia (
    codi_emp     INTEGER NOT NULL,
    codi_imp     INTEGER NOT NULL,
    vigencia_imp DATE NOT NULL,
    cdrn_imp     VARCHAR(20),
    lanc_imp     VARCHAR(1),
    PRIMARY KEY (codi_emp, codi_imp, vigencia_imp)
);

-- ============================================================================
-- #11 — Provisões e seus valores (subquery correlacionada por linha).
-- ============================================================================
DROP TABLE IF EXISTS bethadba.foprovisoes;
CREATE TABLE bethadba.foprovisoes (
    codi_emp     INTEGER NOT NULL,
    i_empregados INTEGER NOT NULL,
    competencia  DATE NOT NULL,
    tipo         SMALLINT NOT NULL,
    avos         NUMERIC(5,2),
    salario      NUMERIC(15,2),
    PRIMARY KEY (codi_emp, i_empregados, competencia, tipo)
);

DROP TABLE IF EXISTS bethadba.foprovisoes_valores;
CREATE TABLE bethadba.foprovisoes_valores (
    codi_emp                 INTEGER NOT NULL,
    i_empregados             INTEGER NOT NULL,
    competencia              DATE NOT NULL,
    tipo                     SMALLINT NOT NULL,
    tipo_valor               SMALLINT NOT NULL,
    valor_adicional_ferias   NUMERIC(15,2),
    valor_fgts               NUMERIC(15,2),
    valor_inss_empresa       NUMERIC(15,2),
    valor_inss_rat           NUMERIC(15,2),
    valor_inss_terceiros     NUMERIC(15,2),
    valor_medias_vantagens   NUMERIC(15,2),
    valor_pis                NUMERIC(15,2),
    valor                    NUMERIC(15,2),
    PRIMARY KEY (codi_emp, i_empregados, competencia, tipo, tipo_valor)
);

-- ============================================================================
-- #12 — Alterações contratuais dos empregados.
-- ============================================================================
DROP TABLE IF EXISTS bethadba.foempregados_alteracao_contratual;
CREATE TABLE bethadba.foempregados_alteracao_contratual (
    codi_emp        INTEGER NOT NULL,
    i_empregados    INTEGER NOT NULL,
    data_alteracao  DATE NOT NULL,
    salario         NUMERIC(15,2),
    PRIMARY KEY (codi_emp, i_empregados, data_alteracao)
);
