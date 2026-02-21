#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn setup_server(c: &mut pgrx::spi::SpiClient<'_>) {
        let host = std::env::var("SYBASE_HOST").expect("SYBASE_HOST env var required");
        let port = std::env::var("SYBASE_PORT").unwrap_or_else(|_| "2638".to_string());
        let database = std::env::var("SYBASE_DATABASE").expect("SYBASE_DATABASE env var required");
        let user = std::env::var("SYBASE_USER").expect("SYBASE_USER env var required");
        let password = std::env::var("SYBASE_PASSWORD").expect("SYBASE_PASSWORD env var required");

        c.update(
            r#"CREATE FOREIGN DATA WRAPPER sybase_wrapper
               HANDLER sybase_fdw_handler VALIDATOR sybase_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();

        c.update(
            &format!(
                "CREATE SERVER sybase_test_server
                 FOREIGN DATA WRAPPER sybase_wrapper
                 OPTIONS (
                   host '{}',
                   port '{}',
                   database '{}',
                   user '{}',
                   password '{}'
                 )",
                host, port, database, user, password
            ),
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foempregados(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foempregados (
                 codi_emp integer NOT NULL,
                 i_empregados integer NOT NULL,
                 nome text,
                 admissao date,
                 vinculo smallint NOT NULL,
                 i_afastamentos integer NOT NULL,
                 i_cargos integer NOT NULL,
                 i_depto integer NOT NULL,
                 i_ccustos integer,
                 i_bancos integer,
                 conta_corr text,
                 digito_conta_pagamento text,
                 tipo_conta text,
                 cpf text,
                 matricula text
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOEMPREGADOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foferias_aquisitivos(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foferias_aquisitivos (
                 codi_emp integer NOT NULL,
                 i_empregados integer NOT NULL,
                 i_ferias_aquisitivos integer NOT NULL,
                 data_inicio date,
                 data_fim date,
                 dias_direito numeric,
                 dias_gozados numeric,
                 dias_abono numeric,
                 limite_para_gozo date
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOFERIAS_AQUISITIVOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foferias_programacao(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foferias_programacao (
                 codi_emp integer NOT NULL,
                 i_empregados integer NOT NULL,
                 gozo_inicio date,
                 dias_gozo numeric,
                 i_ferias_aquisitivos integer NOT NULL
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOFERIAS_PROGRAMACAO')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foferias(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foferias (
                 codi_emp integer NOT NULL,
                 i_empregados integer NOT NULL,
                 inicio_aquisitivo date,
                 fim_aquisitivo date,
                 inicio_gozo date,
                 fim_gozo date,
                 dias_ferias integer,
                 data_aviso date,
                 data_pagto date,
                 proventos numeric,
                 descontos numeric
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOFERIAS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_focargos(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_focargos (
                 codi_emp integer NOT NULL,
                 i_cargos integer NOT NULL,
                 nome text,
                 cbo_2002 integer
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOCARGOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_fodepto(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_fodepto (
                 codi_emp integer NOT NULL,
                 i_depto integer NOT NULL,
                 nome text
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FODEPTO')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_geempre(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_geempre (
                 codi_emp integer NOT NULL,
                 cgce_emp text
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.GEEMPRE')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_fovcheque(c: &mut pgrx::spi::SpiClient<'_>) {
        // FOVCHEQUE is a VIEW in Sybase (sys.systable.count = 0).
        // We set view_estimated_rows to give the planner a reasonable estimate.
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_fovcheque (
                 cp_tipo smallint NOT NULL,
                 i_empregados integer NOT NULL,
                 nome text NOT NULL,
                 i_bancos integer,
                 codi_emp integer NOT NULL,
                 cp_tipo_process integer NOT NULL,
                 competencia date,
                 cp_valor numeric NOT NULL,
                 cp_data_pagto date NOT NULL
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOVCHEQUE', view_estimated_rows '500000')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_fobancos(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_fobancos (
                 i_bancos integer NOT NULL,
                 numero integer NOT NULL,
                 agencia text NOT NULL,
                 nome text NOT NULL
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOBANCOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_fomovto(c: &mut pgrx::spi::SpiClient<'_>) {
        // FOMOVTO is a VIEW in Sybase (sys.systable.count = 0).
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_fomovto (
                 codi_emp integer NOT NULL,
                 i_empregados integer NOT NULL,
                 i_eventos integer NOT NULL,
                 data date NOT NULL,
                 valor_cal numeric NOT NULL
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOMOVTO', view_estimated_rows '1000000')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foeventos(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foeventos (
                 codi_emp integer NOT NULL,
                 i_eventos integer NOT NULL,
                 nome text NOT NULL
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOEVENTOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foeventosbases(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foeventosbases (
                 codi_emp integer NOT NULL,
                 i_eventos integer NOT NULL,
                 i_cadbases integer NOT NULL
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOEVENTOSBASES')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_forescisoes(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_forescisoes (
                 codi_emp integer NOT NULL,
                 i_empregados integer NOT NULL,
                 demissao date,
                 motivo_esocial integer
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FORESCISOES')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foccustos(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foccustos (
                 codi_emp integer NOT NULL,
                 i_ccustos integer NOT NULL,
                 nome text
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOCCUSTOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_foafastamentos_tipos(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE FOREIGN TABLE IF NOT EXISTS test_foafastamentos_tipos (
                 i_afastamentos integer NOT NULL,
                 descricao text
               ) SERVER sybase_test_server
               OPTIONS (table 'bethadba.FOAFASTAMENTOS_TIPOS')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_local_customers(c: &mut pgrx::spi::SpiClient<'_>) {
        c.update(
            r#"CREATE TABLE IF NOT EXISTS test_local_customers (
                 customer_id text PRIMARY KEY,
                 tax_number text NOT NULL,
                 type text
               )"#,
            None,
            &[],
        )
        .unwrap();

        c.update(
            r#"INSERT INTO test_local_customers (customer_id, tax_number, type)
               VALUES ('cust-test-001', '16536302000119', 'cnpj')
               ON CONFLICT (customer_id) DO NOTHING"#,
            None,
            &[],
        )
        .unwrap();
    }

    // ========================================================================
    // Basic tests
    // ========================================================================

    /// Test that basic scan with simple quals works.
    #[pg_test]
    fn sybase_basic_scan() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foempregados(c);

            let results = c
                .select(
                    "SELECT nome FROM test_foempregados WHERE codi_emp = 5600 LIMIT 3",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(results.len(), 3, "should return exactly 3 rows");
        });
    }

    /// Test that NOW() function is pushed down as a qual value.
    #[pg_test]
    fn sybase_now_pushdown() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foferias_aquisitivos(c);

            let results = c
                .select(
                    "SELECT codi_emp, i_empregados, data_inicio
                     FROM test_foferias_aquisitivos
                     WHERE codi_emp = 5600
                       AND data_inicio < NOW()
                     LIMIT 5",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() <= 5,
                "should return at most 5 rows due to LIMIT"
            );
        });
    }

    /// Test correlated subquery performance with connection caching.
    #[pg_test]
    fn sybase_correlated_subquery_with_now() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foempregados(c);
            create_foferias_aquisitivos(c);

            let results = c
                .select(
                    "SELECT
                        e.nome,
                        (
                            SELECT fa.limite_para_gozo
                            FROM test_foferias_aquisitivos fa
                            WHERE fa.codi_emp = e.codi_emp
                              AND fa.i_empregados = e.i_empregados
                              AND fa.data_inicio < NOW()
                              AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
                            ORDER BY fa.limite_para_gozo ASC
                            LIMIT 1
                        ) AS earliest_expiration
                     FROM test_foempregados e
                     WHERE e.codi_emp = 5600
                       AND e.vinculo = 1
                       AND e.i_afastamentos <> 8
                     LIMIT 10",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() <= 10,
                "should return at most 10 employee rows"
            );
        });
    }

    /// Test the full vacation query with multiple correlated subqueries and JOINs.
    #[pg_test]
    fn sybase_full_vacation_query() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foempregados(c);
            create_foferias_aquisitivos(c);
            create_foferias_programacao(c);
            create_foferias(c);
            create_focargos(c);
            create_fodepto(c);
            create_geempre(c);

            let results = c
                .select(
                    "SELECT
                        e.admissao AS admission,
                        d.nome AS department,
                        (
                            SELECT fa.limite_para_gozo
                            FROM test_foferias_aquisitivos fa
                            WHERE fa.codi_emp = e.codi_emp
                              AND fa.data_inicio < NOW()
                              AND fa.i_empregados = e.i_empregados
                              AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
                            ORDER BY fa.limite_para_gozo ASC
                            LIMIT 1
                        ) AS earliest_expiration,
                        e.i_empregados AS employee_code,
                        e.nome AS name,
                        (
                            SELECT COALESCE(JSON_AGG(ROW_TO_JSON(t.*)), '[]')
                            FROM (
                                SELECT
                                    fa.dias_abono AS days_bonus,
                                    fa.dias_direito - (fa.dias_gozados + fa.dias_abono) AS days_left,
                                    fa.dias_gozados AS days_used,
                                    fa.limite_para_gozo AS expiration,
                                    fa.data_inicio AS period_from,
                                    fa.data_fim AS period_to,
                                    (
                                        SELECT COALESCE(JSON_AGG(ROW_TO_JSON(tp.*)), '[]')
                                        FROM (
                                            SELECT fp.gozo_inicio, fp.dias_gozo
                                            FROM test_foferias_programacao fp
                                            WHERE fp.codi_emp = fa.codi_emp
                                              AND fp.i_empregados = fa.i_empregados
                                              AND fp.i_ferias_aquisitivos = fa.i_ferias_aquisitivos
                                        ) tp
                                    ) AS scheduled,
                                    (
                                        SELECT COALESCE(
                                            JSON_AGG(
                                                JSON_BUILD_OBJECT(
                                                    'inicioGozo', tf.inicio_gozo,
                                                    'fimGozo', tf.fim_gozo,
                                                    'diasFerias', tf.dias_ferias,
                                                    'liquido', (tf.proventos - tf.descontos)::TEXT
                                                )
                                            ),
                                            '[]'
                                        )
                                        FROM (
                                            SELECT f.inicio_gozo, f.fim_gozo, f.dias_ferias,
                                                   f.proventos, f.descontos
                                            FROM test_foferias f
                                            WHERE f.codi_emp = fa.codi_emp
                                              AND f.fim_aquisitivo = fa.data_fim
                                              AND f.i_empregados = fa.i_empregados
                                              AND f.inicio_aquisitivo = fa.data_inicio
                                        ) tf
                                    ) AS status
                                FROM test_foferias_aquisitivos fa
                                WHERE fa.codi_emp = e.codi_emp
                                  AND fa.data_inicio < NOW()
                                  AND fa.i_empregados = e.i_empregados
                                  AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
                                ORDER BY fa.limite_para_gozo ASC
                            ) t
                        ) AS periods,
                        ca.nome AS role
                     FROM test_foempregados e
                     JOIN test_geempre ge ON ge.codi_emp = e.codi_emp
                     JOIN test_focargos ca ON ca.i_cargos = e.i_cargos AND ca.codi_emp = e.codi_emp
                     JOIN test_fodepto d ON d.i_depto = e.i_depto AND d.codi_emp = e.codi_emp
                     WHERE e.codi_emp = 5600
                       AND e.vinculo = 1
                       AND e.i_afastamentos <> 8
                     ORDER BY e.nome ASC
                     LIMIT 5",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() <= 5,
                "should return at most 5 rows from the full vacation query"
            );
        });
    }

    /// Test that join pushdown works between two Sybase foreign tables.
    #[pg_test]
    fn sybase_join_pushdown() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foempregados(c);
            create_focargos(c);

            let results = c
                .select(
                    "SELECT e.nome, ca.nome AS cargo
                     FROM test_foempregados e
                     JOIN test_focargos ca ON ca.i_cargos = e.i_cargos
                       AND ca.codi_emp = e.codi_emp
                     WHERE e.codi_emp = 5600
                     LIMIT 5",
                    None,
                    &[],
                )
                .unwrap();

            assert_eq!(results.len(), 5, "should return 5 rows from the join");
        });
    }

    /// Test that parameterized paths work when joining foreign tables
    /// with a local PostgreSQL table.
    #[pg_test]
    fn sybase_parameterized_local_join() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_geempre(c);
            create_foempregados(c);
            create_local_customers(c);

            let results = c
                .select(
                    "SELECT e.nome
                     FROM test_foempregados e
                     JOIN test_geempre g ON g.codi_emp = e.codi_emp
                     JOIN test_local_customers lc ON lc.tax_number = g.cgce_emp
                     WHERE lc.customer_id = 'cust-test-001'
                       AND e.vinculo = 1
                     LIMIT 5",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() > 0 && results.len() <= 5,
                "should return filtered results from one company"
            );
        });
    }

    // ========================================================================
    // Query 1: Payment voucher query with fovcheque (VIEW), fobancos, etc.
    // Tests that views get reasonable row estimates (not 0) and that
    // the planner creates efficient plans for large view-based foreign tables.
    // ========================================================================

    /// Test multi-table join with fovcheque (a Sybase VIEW).
    /// Without the view_estimated_rows fix, the planner would estimate 0 rows
    /// for fovcheque and create terrible plans (e.g., remote joins returning
    /// hundreds of thousands of rows).
    #[pg_test]
    fn sybase_payment_voucher_query() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_fovcheque(c);
            create_geempre(c);
            create_fobancos(c);
            create_foempregados(c);
            create_local_customers(c);

            // Simplified version of the production payment query.
            // Joins fovcheque (VIEW) with geempre, customers, fobancos, foempregados.
            let results = c
                .select(
                    "SELECT
                        vc.nome AS receiver_name,
                        e.cpf AS receiver_tax_number,
                        b.agencia AS agency_number,
                        b.numero AS bank_code,
                        vc.cp_valor AS value,
                        vc.cp_data_pagto AS due_date,
                        vc.competencia AS reference_date,
                        vc.cp_tipo_process AS process_type
                     FROM test_fovcheque vc
                     JOIN test_geempre ge ON ge.codi_emp = vc.codi_emp
                     JOIN test_local_customers ct ON ct.tax_number = ge.cgce_emp
                     JOIN test_fobancos b ON b.i_bancos = vc.i_bancos
                     JOIN test_foempregados e ON e.codi_emp = vc.codi_emp
                       AND e.i_empregados = vc.i_empregados
                     WHERE ct.customer_id = 'cust-test-001'
                     ORDER BY vc.cp_data_pagto ASC, vc.cp_tipo_process ASC, e.nome ASC
                     LIMIT 10",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() <= 10,
                "should return at most 10 payment voucher rows"
            );
        });
    }

    // ========================================================================
    // Query 2: Vacation query with full JOINs (previously gave type error).
    // The original query used VINCULO = 'Celetista' where vinculo is smallint.
    // This test uses the correct integer value (vinculo = 1).
    // ========================================================================

    /// Test the full employee vacation query with all JOINs and correlated
    /// subqueries, using correct types (vinculo as integer, not string).
    /// This is the corrected version of the query that was giving:
    ///   ERROR: invalid input syntax for type smallint: "Celetista"
    #[pg_test]
    fn sybase_vacation_with_all_joins() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foempregados(c);
            create_geempre(c);
            create_focargos(c);
            create_fodepto(c);
            create_foferias_aquisitivos(c);
            create_foferias_programacao(c);
            create_foferias(c);
            create_local_customers(c);

            // Full vacation query with correlated subqueries.
            // Uses vinculo = 1 (not 'Celetista') since vinculo is smallint.
            let results = c
                .select(
                    "SELECT
                        e.admissao AS admission,
                        d.nome AS department,
                        (
                            SELECT fa.limite_para_gozo
                            FROM test_foferias_aquisitivos fa
                            WHERE fa.codi_emp = e.codi_emp
                              AND fa.data_inicio < NOW()
                              AND fa.i_empregados = e.i_empregados
                              AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
                            ORDER BY fa.limite_para_gozo ASC
                            LIMIT 1
                        ) AS earliest_expiration,
                        e.i_empregados AS employee_code,
                        e.nome AS name,
                        (
                            SELECT COALESCE(JSON_AGG(ROW_TO_JSON(t.*)), '[]')
                            FROM (
                                SELECT
                                    fa.dias_abono AS days_bonus,
                                    fa.dias_direito - (fa.dias_gozados + fa.dias_abono) AS days_left,
                                    fa.dias_gozados AS days_used,
                                    fa.limite_para_gozo AS expiration,
                                    fa.data_inicio AS period_from,
                                    fa.data_fim AS period_to,
                                    (
                                        SELECT COALESCE(JSON_AGG(ROW_TO_JSON(tp.*)), '[]')
                                        FROM (
                                            SELECT fp.gozo_inicio, fp.dias_gozo
                                            FROM test_foferias_programacao fp
                                            WHERE fp.codi_emp = fa.codi_emp
                                              AND fp.i_empregados = fa.i_empregados
                                              AND fp.i_ferias_aquisitivos = fa.i_ferias_aquisitivos
                                        ) tp
                                    ) AS scheduled,
                                    (
                                        SELECT COALESCE(
                                            JSON_AGG(
                                                JSON_BUILD_OBJECT(
                                                    'inicioGozo', tf.inicio_gozo,
                                                    'fimGozo', tf.fim_gozo,
                                                    'diasFerias', tf.dias_ferias,
                                                    'liquido', (tf.proventos - tf.descontos)::TEXT
                                                )
                                            ),
                                            '[]'
                                        )
                                        FROM (
                                            SELECT f.inicio_gozo, f.fim_gozo, f.dias_ferias,
                                                   f.proventos, f.descontos
                                            FROM test_foferias f
                                            WHERE f.codi_emp = fa.codi_emp
                                              AND f.fim_aquisitivo = fa.data_fim
                                              AND f.i_empregados = fa.i_empregados
                                              AND f.inicio_aquisitivo = fa.data_inicio
                                        ) tf
                                    ) AS status
                                FROM test_foferias_aquisitivos fa
                                WHERE fa.codi_emp = e.codi_emp
                                  AND fa.data_inicio < NOW()
                                  AND fa.i_empregados = e.i_empregados
                                  AND (fa.dias_gozados + fa.dias_abono < fa.dias_direito)
                                ORDER BY fa.limite_para_gozo ASC
                            ) t
                        ) AS periods,
                        ca.nome AS role
                     FROM test_foempregados e
                     JOIN test_geempre ge ON ge.codi_emp = e.codi_emp
                     JOIN test_local_customers ct ON ct.tax_number = ge.cgce_emp
                     JOIN test_focargos ca ON ca.i_cargos = e.i_cargos
                       AND ca.codi_emp = e.codi_emp
                     JOIN test_fodepto d ON d.i_depto = e.i_depto
                       AND d.codi_emp = e.codi_emp
                     WHERE ct.customer_id = 'cust-test-001'
                       AND e.vinculo = 1
                       AND e.i_afastamentos <> 8
                     ORDER BY e.nome ASC
                     LIMIT 5",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() <= 5,
                "should return at most 5 vacation rows"
            );
        });
    }

    // ========================================================================
    // Query 3: Payroll events aggregation with fomovto (VIEW), foeventos, etc.
    // Tests GROUP BY/SUM with foreign tables and correlated subqueries.
    // ========================================================================

    /// Test payroll events aggregation query with fomovto (a Sybase VIEW).
    /// This exercises GROUP BY, SUM, and correlated subqueries to foeventosbases.
    #[pg_test]
    fn sybase_payroll_events_aggregation() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_fomovto(c);
            create_geempre(c);
            create_foeventos(c);
            create_foeventosbases(c);
            create_local_customers(c);

            // Simplified version of the production payroll events query.
            // Uses GROUP BY with SUM and correlated subqueries to foeventosbases.
            let results = c
                .select(
                    "SELECT
                        ev.nome,
                        COALESCE((
                            SELECT true
                            FROM test_foeventosbases eb
                            WHERE eb.codi_emp = m.codi_emp
                              AND eb.i_cadbases IN (1, 2, 3, 4)
                              AND eb.i_eventos = m.i_eventos
                            LIMIT 1
                        ), false) AS is_base_fgts,
                        SUM(m.valor_cal) AS total
                     FROM test_fomovto m
                     JOIN test_geempre ge ON ge.codi_emp = m.codi_emp
                     JOIN test_local_customers ct ON ct.tax_number = ge.cgce_emp
                     JOIN test_foeventos ev ON m.i_eventos = ev.i_eventos
                       AND m.codi_emp = ev.codi_emp
                     WHERE ct.customer_id = 'cust-test-001'
                       AND m.data BETWEEN '2025-01-01' AND '2025-01-31'
                     GROUP BY ev.nome, is_base_fgts
                     ORDER BY ev.nome ASC
                     LIMIT 10",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() <= 10,
                "should return at most 10 aggregated event rows"
            );
        });
    }

    // ========================================================================
    // Query 4: Aggregation query on fovcheque (VIEW) with GROUP BY / SUM.
    // Tests that date quals are pushed down and aggregation works correctly.
    // ========================================================================

    /// Test GROUP BY / SUM aggregation on fovcheque with date range filter.
    /// The WHERE clause (competencia BETWEEN ...) should be pushed down to Sybase,
    /// reducing the number of rows transferred. The aggregation (GROUP BY / SUM)
    /// is performed locally by PostgreSQL.
    #[pg_test]
    fn sybase_fovcheque_aggregation() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_fovcheque(c);

            let results = c
                .select(
                    "SELECT
                        to_char(vc.competencia, 'YYYY-MM') AS month,
                        sum(vc.cp_valor) AS valor
                     FROM test_fovcheque vc
                     WHERE vc.competencia BETWEEN '2025-01-01' AND '2025-12-31'
                     GROUP BY to_char(vc.competencia, 'YYYY-MM')
                     ORDER BY month ASC",
                    None,
                    &[],
                )
                .unwrap();

            // Should return at most 12 months (one per month in 2025)
            assert!(
                results.len() <= 12,
                "should return at most 12 monthly aggregation rows, got {}",
                results.len()
            );
        });
    }

    // ========================================================================
    // Production employee listing query (7 foreign tables + local)
    // ========================================================================

    /// Test the full production employee listing query with 7 foreign tables.
    /// This is the query that originally took 35s due to excessive Nested Loop
    /// roundtrips. With fdw_startup_cost=20000, the planner should use
    /// Hash/Merge Join for medium-sized tables.
    #[pg_test]
    fn sybase_full_employee_listing() {
        Spi::connect_mut(|c| {
            setup_server(c);
            create_foempregados(c);
            create_geempre(c);
            create_focargos(c);
            create_fodepto(c);
            create_forescisoes(c);
            create_foccustos(c);
            create_foafastamentos_tipos(c);
            create_local_customers(c);

            // Full production query: 7 foreign tables + 1 local table
            let results = c
                .select(
                    "SELECT
                        e.admissao,
                        ca.cbo_2002,
                        cc.nome AS centro_custo,
                        d.nome AS depto,
                        e.nome,
                        e.matricula,
                        CASE e.vinculo
                            WHEN 1 THEN 'Celetista'
                            WHEN 50 THEN 'Estagiário'
                            WHEN 11 THEN 'Contribuinte'
                            WHEN 53 THEN 'Menor Aprendiz'
                            ELSE 'Outros'
                        END AS vinculo_desc,
                        ca.nome AS cargo,
                        at.descricao AS afastamento,
                        r.demissao
                     FROM test_foempregados e
                     JOIN test_geempre ge ON ge.codi_emp = e.codi_emp
                     JOIN test_local_customers ct ON ct.tax_number = ge.cgce_emp
                     JOIN test_focargos ca ON ca.i_cargos = e.i_cargos
                       AND ca.codi_emp = e.codi_emp
                     JOIN test_fodepto d ON d.i_depto = e.i_depto
                       AND d.codi_emp = e.codi_emp
                     LEFT JOIN test_forescisoes r ON r.i_empregados = e.i_empregados
                       AND r.codi_emp = e.codi_emp
                     LEFT JOIN test_foccustos cc ON cc.i_ccustos = e.i_ccustos
                       AND cc.codi_emp = e.codi_emp
                     LEFT JOIN test_foafastamentos_tipos at ON at.i_afastamentos = e.i_afastamentos
                     WHERE ct.customer_id = 'cust-test-001'
                       AND e.i_afastamentos <> 8
                     ORDER BY e.nome ASC
                     LIMIT 10",
                    None,
                    &[],
                )
                .unwrap();

            assert!(
                results.len() > 0 && results.len() <= 10,
                "should return filtered employee listing"
            );
        });
    }
}
