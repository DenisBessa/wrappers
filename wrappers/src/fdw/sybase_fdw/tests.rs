#[cfg(test)]
mod deparse_qual_unit_tests {
    use super::super::sybase_fdw::{SybaseCellFormatter, deparse_qual_to_sybase};
    use supabase_wrappers::prelude::{Cell, Qual, Value};

    fn qual(field: &str, operator: &str, value: Value, use_or: bool) -> Qual {
        Qual {
            field: field.to_string(),
            operator: operator.to_string(),
            value,
            use_or,
            param: None,
        }
    }

    // IN (...) — the bug fix
    #[test]
    fn renders_in_for_integer_array() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "codi_sai",
            "=",
            Value::Array(vec![Cell::I32(164858), Cell::I32(164907)]),
            true,
        );
        assert_eq!(
            deparse_qual_to_sybase(&q, &mut fmt),
            "codi_sai IN (164858, 164907)"
        );
    }

    #[test]
    fn renders_in_for_single_value_array() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual("id", "=", Value::Array(vec![Cell::I64(42)]), true);
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "id IN (42)");
    }

    #[test]
    fn renders_in_for_string_array() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "tipo",
            "=",
            Value::Array(vec![
                Cell::String("A".to_string()),
                Cell::String("B".to_string()),
            ]),
            true,
        );
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "tipo IN ('A', 'B')");
    }

    // NOT IN — Postgres emits `<>` with useOr=false (semantics: `<> ALL`).
    // Without this branch the framework's deparse_with_fmt panics
    // (`Value::Array(_) => unreachable!()`), surfaced in prod as
    // XX000 "internal error: entered unreachable code".
    #[test]
    fn renders_not_in_for_integer_array_no_or() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "situacao_ent",
            "<>",
            Value::Array(vec![Cell::I32(1), Cell::I32(2), Cell::I32(3)]),
            false,
        );
        assert_eq!(
            deparse_qual_to_sybase(&q, &mut fmt),
            "situacao_ent NOT IN (1, 2, 3)"
        );
    }

    #[test]
    fn renders_not_in_for_string_array_no_or() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "situacao_ent",
            "<>",
            Value::Array(vec![
                Cell::String("X".to_string()),
                Cell::String("Y".to_string()),
            ]),
            false,
        );
        assert_eq!(
            deparse_qual_to_sybase(&q, &mut fmt),
            "situacao_ent NOT IN ('X', 'Y')",
        );
    }

    // Defensive: in case the planner ever emits `<>` with useOr=true.
    #[test]
    fn renders_not_in_for_integer_array_with_or() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "status",
            "<>",
            Value::Array(vec![Cell::I32(1), Cell::I32(2)]),
            true,
        );
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "status NOT IN (1, 2)");
    }

    // Non-`=`/`<>` array operators (e.g. `>`) — keep parenthesised OR list.
    #[test]
    fn renders_parenthesised_or_for_other_operators() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "v",
            ">",
            Value::Array(vec![Cell::I32(10), Cell::I32(20)]),
            true,
        );
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "(v > 10 OR v > 20)");
    }

    // Scalar fall-through: delegates to framework deparse, no precedence risk.
    #[test]
    fn renders_scalar_equality() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual("codi_emp", "=", Value::Cell(Cell::I32(51800)), false);
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "codi_emp = 51800");
    }

    #[test]
    fn renders_boolean_is_null_replacement() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual("active", "is", Value::Cell(Cell::Bool(true)), false);
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "active = 1");
    }

    #[test]
    fn renders_boolean_is_not() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual("active", "is not", Value::Cell(Cell::Bool(false)), false);
        assert_eq!(deparse_qual_to_sybase(&q, &mut fmt), "active <> 0");
    }

    // ILIKE (`~~*`) — Sybase has no such operator; the framework only maps
    // `~~`→LIKE. Without translation the FDW emits `~~*` verbatim and Sybase
    // rejects it ("Syntax error near '~'"), failing any query that pushes an
    // ILIKE on a foreign column. Translate to a collation-independent
    // UPPER()/UPPER() case-insensitive match.
    #[test]
    fn renders_ilike_as_upper_like() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "cname",
            "~~*",
            Value::Cell(Cell::String("%saldo%".to_string())),
            false,
        );
        assert_eq!(
            deparse_qual_to_sybase(&q, &mut fmt),
            "UPPER(cname) LIKE UPPER('%saldo%')"
        );
    }

    #[test]
    fn renders_not_ilike_as_upper_not_like() {
        let mut fmt = SybaseCellFormatter {};
        let q = qual(
            "cname",
            "!~~*",
            Value::Cell(Cell::String("%tmp%".to_string())),
            false,
        );
        assert_eq!(
            deparse_qual_to_sybase(&q, &mut fmt),
            "UPPER(cname) NOT LIKE UPPER('%tmp%')"
        );
    }
}

#[cfg(test)]
mod deparse_aggregate_unit_tests {
    use super::super::sybase_fdw::{deparse_agg_sybase, deparse_agg_with_alias_sybase};
    use pgrx::pg_sys;
    use supabase_wrappers::prelude::{Aggregate, AggregateKind, Column};

    fn col(name: &str, num: usize, type_oid: pg_sys::Oid) -> Column {
        Column {
            name: name.to_string(),
            num,
            type_oid,
        }
    }

    #[test]
    fn renders_count_star() {
        let agg = Aggregate {
            kind: AggregateKind::Count,
            column: None,
            distinct: false,
            alias: "cnt".to_string(),
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(deparse_agg_sybase(&agg), "COUNT(*)");
        assert_eq!(deparse_agg_with_alias_sybase(&agg), "COUNT(*) AS cnt");
    }

    #[test]
    fn renders_sum_with_alias() {
        // Mirrors the prod CTE that OOM-killed Postgres before aggregate
        // pushdown landed: `SUM(vpag_entp) AS valor_pago`. Without the
        // pushdown, every matching row of `efentradaspag` is buffered in
        // `scan_result` for PG to aggregate locally; with it, Sybase does
        // the SUM remotely and only the grouped rows cross the wire.
        let agg = Aggregate {
            kind: AggregateKind::Sum,
            column: Some(col("vpag_entp", 5, pg_sys::NUMERICOID)),
            distinct: false,
            alias: "valor_pago".to_string(),
            type_oid: pg_sys::NUMERICOID,
        };
        assert_eq!(deparse_agg_sybase(&agg), "SUM(vpag_entp)");
        assert_eq!(
            deparse_agg_with_alias_sybase(&agg),
            "SUM(vpag_entp) AS valor_pago"
        );
    }

    #[test]
    fn renders_count_column_with_distinct() {
        let agg = Aggregate {
            kind: AggregateKind::CountColumn,
            column: Some(col("codi_emp", 1, pg_sys::INT4OID)),
            distinct: true,
            alias: "n".to_string(),
            type_oid: pg_sys::INT8OID,
        };
        assert_eq!(deparse_agg_sybase(&agg), "COUNT(DISTINCT codi_emp)");
    }

    #[test]
    fn renders_min_max_avg() {
        for (kind, sql) in [
            (AggregateKind::Min, "MIN(valor)"),
            (AggregateKind::Max, "MAX(valor)"),
            (AggregateKind::Avg, "AVG(valor)"),
        ] {
            let agg = Aggregate {
                kind,
                column: Some(col("valor", 1, pg_sys::NUMERICOID)),
                distinct: false,
                alias: "out".to_string(),
                type_oid: pg_sys::NUMERICOID,
            };
            assert_eq!(deparse_agg_sybase(&agg), sql);
        }
    }
}

#[cfg(test)]
mod deparse_aggregate_query_tests {
    use super::super::sybase_fdw::deparse_aggregate_sybase;
    use pgrx::pg_sys;
    use supabase_wrappers::prelude::{Aggregate, AggregateKind, Cell, Column, Qual, Value};

    fn col(name: &str, num: usize, type_oid: pg_sys::Oid) -> Column {
        Column {
            name: name.to_string(),
            num,
            type_oid,
        }
    }

    fn qual(field: &str, operator: &str, value: Value, use_or: bool) -> Qual {
        Qual {
            field: field.to_string(),
            operator: operator.to_string(),
            value,
            use_or,
            param: None,
        }
    }

    #[test]
    fn count_star_no_quals_no_group() {
        let aggs = vec![Aggregate {
            kind: AggregateKind::Count,
            column: None,
            distinct: false,
            alias: "cnt".to_string(),
            type_oid: pg_sys::INT8OID,
        }];
        let sql = deparse_aggregate_sybase("bethadba.efentradaspag", &aggs, &[], &[]);
        assert_eq!(
            sql,
            "SELECT COUNT(*) AS cnt FROM bethadba.efentradaspag AS _wrappers_tbl"
        );
    }

    // Mirrors the prod CTE that OOM-killed `supabase-db` (PIDs 3841, 4582,
    // 5343, 834): `SELECT codi_emp, codi_ent, parc_entp, SUM(vpag_entp) AS
    // valor_pago FROM efentradaspag WHERE codi_emp = 1210700 AND tipo_entp
    // NOT IN (2, 3) GROUP BY codi_emp, codi_ent, parc_entp`. Without
    // aggregate pushdown the Sybase FDW pulls every matching row (≈ all
    // 609k of the table for that codi_emp) into `scan_result` so Postgres
    // can aggregate locally — that's the OOM. The expected SQL below proves
    // we send a single grouped query and only the GROUP BY cardinality
    // (orders of magnitude smaller) crosses the wire.
    #[test]
    fn prod_oom_query_pushes_sum_and_group_by() {
        let group_by = vec![
            col("codi_emp", 1, pg_sys::INT4OID),
            col("codi_ent", 2, pg_sys::INT4OID),
            col("parc_entp", 9, pg_sys::INT4OID),
        ];
        let aggs = vec![Aggregate {
            kind: AggregateKind::Sum,
            column: Some(col("vpag_entp", 5, pg_sys::NUMERICOID)),
            distinct: false,
            alias: "valor_pago".to_string(),
            type_oid: pg_sys::NUMERICOID,
        }];
        let quals = vec![
            qual("codi_emp", "=", Value::Cell(Cell::I32(1210700)), false),
            qual(
                "tipo_entp",
                "<>",
                Value::Array(vec![Cell::I16(2), Cell::I16(3)]),
                false,
            ),
        ];

        let sql = deparse_aggregate_sybase("bethadba.efentradaspag", &aggs, &group_by, &quals);

        assert_eq!(
            sql,
            "SELECT codi_emp, codi_ent, parc_entp, SUM(vpag_entp) AS valor_pago \
             FROM bethadba.efentradaspag AS _wrappers_tbl \
             WHERE codi_emp = 1210700 AND tipo_entp NOT IN (2, 3) \
             GROUP BY codi_emp, codi_ent, parc_entp"
        );
    }

    // Sanity: GROUP BY alone (no WHERE) still emits a valid query.
    #[test]
    fn group_by_without_quals() {
        let group_by = vec![col("codi_emp", 1, pg_sys::INT4OID)];
        let aggs = vec![Aggregate {
            kind: AggregateKind::Count,
            column: None,
            distinct: false,
            alias: "n".to_string(),
            type_oid: pg_sys::INT8OID,
        }];
        let sql = deparse_aggregate_sybase("bethadba.geempre", &aggs, &group_by, &[]);
        assert_eq!(
            sql,
            "SELECT codi_emp, COUNT(*) AS n FROM bethadba.geempre AS _wrappers_tbl GROUP BY codi_emp"
        );
    }
}

// ============================================================================
// Integration tests
// ============================================================================
//
// The 13 #[pgrx::pg_test] integration tests that used to live in this file
// (sybase_basic_scan, sybase_full_vacation_query, sybase_payment_voucher_query,
// etc.) were migrated to SQL fixtures under
// `.ci/sybase-test-env/fixtures/11x_*.sql` and `12x_*.sql`. The `14x_*.sql`
// fixtures reproduce the 12 problematic prod query families captured in
// `Temp.sql` (saldo SUM(CASE), geimposto join, OFFSET-before-WHERE / ILIKE
// pushdown, apurar-gate aggregate pushdown, efsaidas bulk quals, efsdoimp bulk,
// foprovisoes N+1, alteracao-contratual multi-FT). They run against the Docker
// stack documented in `.ci/sybase-test-env/README.md` (`make test`),
// which spins up SQL Anywhere with a synthetic `bethadba` schema and exercises
// each test path without needing the production Sybase server (the old
// pg_tests required `SYBASE_HOST` env var and a live connection).
//
// Reasons to migrate:
//   - the pgrx test runner can't bring its own Sybase; the fixtures can
//     because Docker owns the database lifecycle
//   - the fixtures double as bug reproductions for prod regressions
//   - faster feedback loop: edit Rust → `make rebuild && make test` keeps the
//     same backend warm
