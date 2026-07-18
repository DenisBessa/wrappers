use odbc_api::{
    BlockCursor, Connection, ConnectionOptions, Cursor, CursorImpl, Environment,
    buffers::TextRowSet, handles::StatementImpl,
};
use pgrx::{PgBuiltInOids, PgOid, pg_sys::Oid, prelude::Date};
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::sync::{LazyLock, Mutex};

use supabase_wrappers::prelude::*;

use super::{SybaseFdwError, SybaseFdwResult};

// Global ODBC environment - thread-safe singleton
pub(super) static ODBC_ENV: LazyLock<Environment> =
    LazyLock::new(|| Environment::new().expect("Failed to create ODBC environment"));

const LOGIN_TIMEOUT_SEC: u32 = 10;

/// Open an ODBC connection with READ UNCOMMITTED isolation level.
///
/// Sybase SQL Anywhere uses read locks by default, which causes blocking
/// when multiple foreign scans run concurrently within a single PostgreSQL
/// query (each scan opens its own connection). Setting isolation_level=0
/// (READ UNCOMMITTED) eliminates read locks entirely.
///
/// For payroll/HR data that is essentially read-only during queries, the
/// risk of dirty reads is negligible compared to the benefit of avoiding
/// deadlocks and lock waits.
pub(super) fn connect_unlocked(conn_str: &str) -> Result<Connection<'static>, odbc_api::Error> {
    let options = ConnectionOptions {
        login_timeout_sec: Some(LOGIN_TIMEOUT_SEC),
        ..ConnectionOptions::default()
    };
    let conn = ODBC_ENV.connect_with_connection_string(conn_str, options)?;
    // SET TEMPORARY OPTION scopes to this connection only, no side effects
    conn.execute("SET TEMPORARY OPTION isolation_level = 0", ())?;
    Ok(conn)
}

// Cache for table row counts, avoiding repeated COUNT(*) queries during planning.
// Key: table name (e.g., "bethadba.FOEMPREGADOS"), Value: row count.
static ROW_COUNT_CACHE: LazyLock<Mutex<HashMap<String, i64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Apply PG-style selectivity defaults to a base row count given a set of
/// WHERE quals. Mirrors `DEFAULT_EQ_SEL` (0.005) and `DEFAULT_INEQ_SEL`
/// (0.3333) from `src/include/utils/selfuncs.h`, used by the planner when no
/// column statistics are available — which is always the case for FTs.
///
/// Without this, get_rel_size returns the full table count regardless of
/// WHERE clauses, so multi-FT joins explode into trillion-row estimates,
/// pushing the planner toward NestedLoop+LATERAL with thousands of per-row
/// remote round-trips. See the slow-query investigation in
/// `.ci/sybase-test-env/fixtures/090_*` for the case that motivated this.
fn apply_quals_selectivity(base_rows: i64, quals: &[Qual]) -> i64 {
    if quals.is_empty() {
        return base_rows;
    }
    let mut selectivity = 1.0_f64;
    for q in quals {
        // Param-bound quals (correlated subquery) are evaluated at runtime.
        // Skip selectivity entirely: leaving the rel at its base count keeps
        // the cost of `NL outer=<this FT> inner=Materialize(<other FT>)`
        // proportional to base_rows × other_rows, so the planner doesn't
        // prefer plans where another (large, unfiltered) FT becomes the NL
        // outer — which forces a remote full-scan per outer row in
        // correlated subqueries. Reproduced in prod by
        // `autoConciliarCrossPeriod.test.ts` (subquery em getLancamentos).
        if q.param.is_some() {
            continue;
        }
        let s = match q.operator.as_str() {
            // Equality + IN: very selective by default.
            "=" => {
                if let Value::Array(items) = &q.value {
                    // `IN (...)` / `= ANY(ARRAY[...])`: roughly 0.005 per item.
                    (0.005_f64 * items.len() as f64).min(1.0)
                } else {
                    0.005
                }
            }
            // Inequality / NULL tests / LIKE: PG-default ineq selectivity.
            "<>" | "!=" => 0.995,
            "<" | "<=" | ">" | ">=" => 0.333,
            "is" => 0.005,
            "is not" => 0.995,
            "~~" | "!~~" | "~~*" | "!~~*" => 0.005,
            _ => 1.0, // unknown operator → no opinion
        };
        selectivity *= s;
    }
    // Clamp to at least 1 row — 0 rows confuses the planner ("this FT is
    // empty, no need to join").
    ((base_rows as f64 * selectivity).round() as i64).max(1)
}

// Convert ODBC value to Cell based on target column type
pub(super) fn value_to_cell(value: Option<&str>, type_oid: Oid) -> SybaseFdwResult<Option<Cell>> {
    let value = match value {
        Some(v) => v,
        None => return Ok(None),
    };

    let cell = match PgOid::from(type_oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
            let v = value == "1" || value.to_lowercase() == "true";
            Some(Cell::Bool(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
            let v: i8 = value.parse().unwrap_or(0);
            Some(Cell::I8(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            let v: i16 = value.parse().unwrap_or(0);
            Some(Cell::I16(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
            let v: f32 = value.parse().unwrap_or(0.0);
            Some(Cell::F32(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            let v: i32 = value.parse().unwrap_or(0);
            Some(Cell::I32(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
            let v: f64 = value.parse().unwrap_or(0.0);
            Some(Cell::F64(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
            let v: i64 = value.parse().unwrap_or(0);
            Some(Cell::I64(v))
        }
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
            let v: f64 = value.parse().unwrap_or(0.0);
            Some(Cell::Numeric(pgrx::AnyNumeric::try_from(v).unwrap()))
        }
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => Some(Cell::String(value.to_owned())),
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
            if let Some((year, rest)) = value.split_once('-')
                && let Some((month, day)) = rest.split_once('-')
            {
                let y: i32 = year.parse().unwrap_or(1970);
                let m: u8 = month.parse().unwrap_or(1);
                let d: u8 = day
                    .split_whitespace()
                    .next()
                    .unwrap_or("1")
                    .parse()
                    .unwrap_or(1);
                if let Ok(date) = Date::new(y, m, d) {
                    return Ok(Some(Cell::Date(date)));
                }
            }
            None
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => parse_timestamp(value).map(Cell::Timestamp),
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => parse_timestamp(value)
            .map(|ts| Cell::Timestamptz(pgrx::prelude::TimestampWithTimeZone::from(ts))),
        _ => {
            // Default to string for unknown types
            Some(Cell::String(value.to_owned()))
        }
    };

    Ok(cell)
}

fn parse_timestamp(value: &str) -> Option<pgrx::prelude::Timestamp> {
    let parts: Vec<&str> = value.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let date_parts: Vec<&str> = parts[0].split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }

    let year: i32 = date_parts[0].parse().ok()?;
    let month: u8 = date_parts[1].parse().ok()?;
    let day: u8 = date_parts[2].parse().ok()?;

    let (hour, minute, second, micro) = if parts.len() > 1 {
        let time_str = parts[1].split('.').next().unwrap_or("00:00:00");
        let time_parts: Vec<&str> = time_str.split(':').collect();
        let h: u8 = time_parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let m: u8 = time_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let s: f64 = time_parts
            .get(2)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let micro = ((s - s.floor()) * 1_000_000.0) as u32;
        (h, m, s.floor() as u8, micro)
    } else {
        (0, 0, 0, 0)
    };

    pgrx::prelude::Timestamp::new(
        year,
        month,
        day,
        hour,
        minute,
        second as f64 + (micro as f64 / 1_000_000.0),
    )
    .ok()
}

pub(super) struct SybaseCellFormatter {}

impl CellFormatter for SybaseCellFormatter {
    fn fmt_cell(&mut self, cell: &Cell) -> String {
        match cell {
            Cell::Bool(v) => format!("{}", *v as u8),
            _ => format!("{cell}"),
        }
    }
}

/// Render a single qual to Sybase SQL.
///
/// For `IN (...)` / `= ANY (ARRAY[...])`, Postgres delivers a single qual with
/// `use_or=true`, `operator="="`, and `value=Value::Array([...])`. The framework's
/// `deparse_with_fmt` would render this as `f = v1 or f = v2 or ...` — and once
/// joined with other quals via ` AND `, SQL precedence (AND binds tighter than
/// OR) silently rewrites the WHERE clause. For example:
///
/// ```sql
/// WHERE codi_emp = 51800 AND codi_sai = 100 OR codi_sai = 200
/// -- parses as: (codi_emp = 51800 AND codi_sai = 100) OR codi_sai = 200
/// -- → returns every row with codi_sai = 200, ignoring codi_emp
/// ```
///
/// We emit `field IN (...)` instead, which has unambiguous precedence and lets
/// the remote optimizer use an index scan. For non-`=` operators with an array
/// (rare in practice) we still parenthesise the OR list.
pub(super) fn deparse_qual_to_sybase(qual: &Qual, fmt: &mut SybaseCellFormatter) -> String {
    let oper = qual.operator.as_str();

    if let Value::Cell(cell) = &qual.value
        && let Cell::Bool(_) = cell
    {
        if oper == "is" {
            return format!("{} = {}", qual.field, fmt.fmt_cell(cell));
        } else if oper == "is not" {
            return format!("{} <> {}", qual.field, fmt.fmt_cell(cell));
        }
    }

    // ILIKE / NOT ILIKE — Postgres operators `~~*` / `!~~*`. Sybase SQL Anywhere
    // has no such operator and rejects it at parse time (SQL Anywhere Error -131:
    // "Syntax error near '~'"), so a pushed-down ILIKE on a foreign column would
    // fail the whole scan. The framework only maps `~~`/`!~~` to LIKE/NOT LIKE;
    // translate the case-insensitive variants to a collation-independent
    // UPPER(col) LIKE UPPER(pattern). (`~~`/`!~~` fall through to the framework,
    // which renders them as LIKE/NOT LIKE unchanged.)
    if let Value::Cell(cell) = &qual.value {
        match oper {
            "~~*" => return format!("UPPER({}) LIKE UPPER({})", qual.field, fmt.fmt_cell(cell)),
            "!~~*" => {
                return format!(
                    "UPPER({}) NOT LIKE UPPER({})",
                    qual.field,
                    fmt.fmt_cell(cell)
                );
            }
            _ => {}
        }
    }

    if let Value::Array(cells) = &qual.value {
        let values: Vec<String> = cells.iter().map(|c| fmt.fmt_cell(c)).collect();
        // PG emits `IN (...)` as `=` with useOr=true (ANY) and `NOT IN (...)`
        // as `<>` with useOr=false (ALL). Both collapse to the canonical SQL
        // form on the remote — equivalent semantics, index-friendly, and they
        // sidestep the framework's deparse_with_fmt which panics on
        // Value::Array with useOr=false (interface.rs:589).
        return match (oper, qual.use_or) {
            ("=", true) => format!("{} IN ({})", qual.field, values.join(", ")),
            ("<>", false) | ("<>", true) => {
                format!("{} NOT IN ({})", qual.field, values.join(", "))
            }
            (_, use_or) => {
                let joiner = if use_or { " OR " } else { " AND " };
                let conds: Vec<String> = cells
                    .iter()
                    .map(|c| format!("{} {} {}", qual.field, oper, fmt.fmt_cell(c)))
                    .collect();
                format!("({})", conds.join(joiner))
            }
        };
    }

    qual.deparse_with_fmt(fmt)
}

/// Render a single aggregate function as Sybase SQL.
///
/// Sybase SQL Anywhere accepts the same syntax as ANSI SQL for the aggregates
/// we push down (COUNT, SUM, AVG, MIN, MAX) — so we delegate the basic shape
/// to the framework's `Aggregate::deparse()` and only diverge for the alias
/// form. The remote query never quotes column names (matches the rest of the
/// Sybase FDW: identifiers are written unquoted, relying on Sybase's
/// case-insensitive resolution).
pub(super) fn deparse_agg_sybase(agg: &Aggregate) -> String {
    agg.deparse()
}

pub(super) fn deparse_agg_with_alias_sybase(agg: &Aggregate) -> String {
    format!("{} AS {}", deparse_agg_sybase(agg), agg.alias)
}

/// Build the SQL for an aggregate scan pushed down to Sybase.
///
/// Without aggregate pushdown, queries like
/// `SELECT codi_emp, SUM(vpag_entp) FROM efentradaspag WHERE ... GROUP BY codi_emp`
/// drag every matching row across the ODBC connection so Postgres can
/// aggregate locally. For a CTE materialising hundreds of thousands of rows
/// from `efentradaspag` (observed in prod) this buffers the entire result
/// set in `scan_result: Vec<FetchedRow>` and OOM-kills the backend. Pushing
/// SUM/COUNT/MIN/MAX/AVG (+ GROUP BY) down to Sybase keeps the memory
/// footprint bounded by the cardinality of the group key.
///
/// `tgt_cols` (set in `begin_aggregate_scan`) must mirror this SELECT order
/// exactly — group-by columns first, then aggregate aliases — so that
/// `iter_scan` can read each value by column name.
pub(super) fn deparse_aggregate_sybase(
    table: &str,
    aggregates: &[Aggregate],
    group_by: &[Column],
    quals: &[Qual],
) -> String {
    let mut select_items: Vec<String> = group_by.iter().map(|c| c.name.clone()).collect();
    for agg in aggregates {
        select_items.push(deparse_agg_with_alias_sybase(agg));
    }

    let mut sql = format!(
        "SELECT {} FROM {} AS _wrappers_tbl",
        select_items.join(", "),
        table
    );

    // Keep every pushed-down qual: with aggregate pushdown there is no lower
    // ForeignScan left to re-apply filters locally, so dropping any qual
    // here would silently widen the aggregate.
    if !quals.is_empty() {
        let mut fmt = SybaseCellFormatter {};
        let cond = quals
            .iter()
            .map(|q| deparse_qual_to_sybase(q, &mut fmt))
            .collect::<Vec<String>>()
            .join(" AND ");

        if !cond.is_empty() {
            sql.push_str(&format!(" WHERE {cond}"));
        }
    }

    if !group_by.is_empty() {
        let cols = group_by
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" GROUP BY {cols}"));
    }

    sql
}

// Default batch size for streaming reads. Small enough that the underlying
// `TextRowSet` (which allocates `batch_size * num_cols * max_str` bytes upfront)
// stays modest even for wide tables (e.g. 143 cols × 4096 × 100 ≈ 60 MB), large
// enough that the per-batch ODBC roundtrip overhead is amortized.
const STREAM_BATCH_SIZE: usize = 100;

// Maximum width per text column. Sybase strings beyond this are truncated by
// the ODBC driver; matches the historical buffering implementation's choice.
const STREAM_MAX_STR_LEN: usize = 4096;

/// Holds an in-progress ODBC cursor across `iter_scan` calls.
///
/// Self-referential: the `BlockCursor` borrows the statement which borrows the
/// `Connection`. We sidestep the lifetime issue by:
///   1. Boxing the `Connection<'static>` for a stable heap address.
///   2. Transmuting the cursor's borrow to `'static` (the real lifetime is
///      tied to `_conn`, which lives as long as `StreamingScan`).
///   3. Enforcing drop order via `Drop` so `block` (which holds the cursor and
///      statement handle) is dropped before `_conn` is released.
///
/// The connection itself is genuinely `'static`-compatible because all
/// connections in this module are derived from the [`ODBC_ENV`] `LazyLock`,
/// which lives for the process.
struct StreamingScan {
    /// Owned `BlockCursor`. Holds the statement (which references the
    /// connection) and the text-row buffer. Declared first so the explicit
    /// `Drop` impl can run it down before `_conn`.
    block: ManuallyDrop<BlockCursor<CursorImpl<StatementImpl<'static>>, TextRowSet>>,
    /// Boxed so its address stays stable across moves of `StreamingScan`.
    /// The `_` prefix marks that we never read it directly — the cursor in
    /// `block` borrows from this connection internally.
    _conn: Box<Connection<'static>>,
    /// Rows from the most recently fetched batch, extracted into owned Strings
    /// so the borrow on `block`'s buffer doesn't outlive the next `fetch()`.
    current_batch: Vec<Vec<Option<String>>>,
    pos: usize,
    /// True once the cursor signaled end-of-result-set. Subsequent reads short-
    /// circuit to `None` without touching the cursor.
    eof: bool,
}

impl Drop for StreamingScan {
    fn drop(&mut self) {
        // FreeTDS requires the client to consume every row of the result set
        // before `SQLCloseCursor` is called — otherwise the wire protocol
        // gets out of sync and SQLCloseCursor panics with "Bad token from
        // the server: Datastream processing out of sync". When iteration
        // stops early (LIMIT, JOIN early-exit, planner SubPlan), drain the
        // remaining batches before letting `block` drop. SQLCancel would be
        // cheaper but odbc-api 8 doesn't expose it.
        if !self.eof {
            let block = &mut *self.block;
            while let Ok(Some(_)) = block.fetch() {
                // Discard the batch; we only need the cursor consumed.
            }
        }
        // SAFETY: drop `block` (which closes the cursor and statement
        // handle) BEFORE `_conn` is released, because the statement handle
        // borrows from the connection.
        unsafe {
            ManuallyDrop::drop(&mut self.block);
        }
        // `_conn` drops as a normal `Box<Connection<'static>>` after this
        // function returns.
    }
}

impl StreamingScan {
    /// Open a streaming scan over `sql` against `conn`. Returns `None` if the
    /// statement produced no result set (e.g. a DDL); for SELECTs always
    /// returns `Some`.
    fn open(conn: Connection<'static>, sql: &str) -> SybaseFdwResult<Option<Self>> {
        let conn_box = Box::new(conn);
        // SAFETY: `conn_box` is heap-allocated so its address is stable. We
        // derive `conn_ref` from a raw pointer so the borrow checker doesn't
        // tie its lifetime to `conn_box`'s lexical scope — without this,
        // moving `conn_box` into `_conn` below conflicts with the (apparent)
        // borrow held by the cursor returned from `execute`. The actual
        // safety invariant is enforced at runtime by our `Drop` impl:
        //   - `conn_box` lives until `StreamingScan` is dropped;
        //   - `Drop` drops `block` (which holds the cursor and statement)
        //     BEFORE `_conn` is released, so the statement handle is freed
        //     while the connection is still valid.
        let conn_ptr: *const Connection<'static> = &*conn_box;
        let conn_ref: &Connection<'static> = unsafe { &*conn_ptr };
        let cursor_opt = conn_ref.execute(sql, ())?;
        let mut cursor = match cursor_opt {
            Some(c) => c,
            None => return Ok(None),
        };
        let buffers =
            TextRowSet::for_cursor(STREAM_BATCH_SIZE, &mut cursor, Some(STREAM_MAX_STR_LEN))?;
        let block = cursor.bind_buffer(buffers)?;
        // SAFETY: stretch the inner statement borrow lifetime from `&conn_box`
        // (the implicit lifetime of `block`) to `'static`. Same justification
        // as above; size and layout of `BlockCursor` do not depend on the
        // lifetime parameter, so this `transmute` is a no-op at runtime.
        let block_static: BlockCursor<CursorImpl<StatementImpl<'static>>, TextRowSet> =
            unsafe { std::mem::transmute(block) };
        Ok(Some(StreamingScan {
            block: ManuallyDrop::new(block_static),
            _conn: conn_box,
            current_batch: Vec::new(),
            pos: 0,
            eof: false,
        }))
    }

    /// Read the next row from the result set. Returns `None` on EOF.
    fn next_row(&mut self) -> SybaseFdwResult<Option<&[Option<String>]>> {
        if self.pos >= self.current_batch.len() {
            if self.eof {
                return Ok(None);
            }
            self.current_batch.clear();
            self.pos = 0;
            // SAFETY: `block` is initialized (see Drop). Mutable access here
            // is exclusive (caller holds `&mut self`).
            let block = &mut *self.block;
            match block.fetch()? {
                Some(buf) => {
                    let n_rows = buf.num_rows();
                    let n_cols = buf.num_cols();
                    self.current_batch.reserve(n_rows);
                    for row_idx in 0..n_rows {
                        let mut row = Vec::with_capacity(n_cols);
                        for col_idx in 0..n_cols {
                            let v = buf
                                .at(col_idx, row_idx)
                                .map(|bytes| String::from_utf8_lossy(bytes).to_string());
                            row.push(v);
                        }
                        self.current_batch.push(row);
                    }
                    if self.current_batch.is_empty() {
                        // Empty batch — driver signaled "no more rows" by
                        // returning a zero-row batch instead of `None`.
                        self.eof = true;
                        return Ok(None);
                    }
                }
                None => {
                    self.eof = true;
                    return Ok(None);
                }
            }
        }
        let row = &self.current_batch[self.pos];
        self.pos += 1;
        Ok(Some(row.as_slice()))
    }

    /// Consume the stream and return the underlying `Connection` so it can
    /// be re-cached for later scans. Drops the cursor first to release the
    /// statement handle.
    fn into_connection(self) -> Connection<'static> {
        // Wrap the entire `self` in ManuallyDrop so the compiler-generated
        // destructor for `self` is suppressed for every field. Then we drop
        // what needs dropping (`block`) and read out what we want to keep
        // (`_conn`). `current_batch` leaks until function return — fine, it's
        // a small `Vec` that's owned by `self`, and the caller doesn't need
        // it. The cursor handle (held by `block`) is released before `_conn`
        // is touched, matching the Drop ordering invariant.
        let mut me = ManuallyDrop::new(self);
        unsafe {
            ManuallyDrop::drop(&mut me.block);
            // Drop `current_batch` explicitly — without ManuallyDrop on it,
            // those `Vec` allocations would leak.
            std::ptr::drop_in_place(&mut me.current_batch);
            // Move the boxed connection out without dropping it.
            let boxed = std::ptr::read(&me._conn);
            // `me` itself is `ManuallyDrop`, so falling out of scope is safe.
            *boxed
        }
    }
}

#[wrappers_fdw(
    version = "0.1.0",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/sybase_fdw",
    error_type = "SybaseFdwError"
)]
pub(crate) struct SybaseFdw {
    conn_str: String,
    table: String,
    tgt_cols: Vec<Column>,

    // Active streaming scan, set by `execute_query` and consumed by
    // `iter_scan`. Replaces the historical `scan_result: Vec<FetchedRow>`
    // which materialised every matching row into Rust memory before
    // returning the first one to Postgres — a pattern that OOM-killed
    // supabase-db on prod when a CTE forced full-table SUM/GROUP BY over
    // ~609k rows of `efentradaspag`.
    stream: Option<StreamingScan>,

    // Lazy execution support for parameterized queries.
    // When quals contain parameters (from JOINs), we defer query execution
    // until iter_scan, when the parameter values have been evaluated.
    query_executed: bool,
    stored_quals: Vec<Qual>,
    stored_sorts: Vec<Sort>,
    stored_limit: Option<Limit>,

    // Cached ODBC connection, reused by rescans while an executor scan is
    // active to avoid connection overhead on correlated subqueries.
    // `execute_query` `take()`s it to hand ownership to `StreamingScan` and
    // `close_stream` recovers it via `StreamingScan::into_connection`.
    // `end_scan` must drop it because FdwState can outlive the executor scan.
    cached_conn: Option<Connection<'static>>,

    // Aggregate pushdown can be disabled per-server with `aggregate_pushdown=false`.
    // Default is ON: the upper-rel planner integration was previously gated
    // because the join hooks misidentified upper rels as join scans (scanrelid=0
    // for both) and corrupted state, causing a >90 TB phantom palloc that
    // aborted the backend during planning. The discriminator marker added to
    // join_state serialization (see join.rs::JOIN_PRIVATE_MARKER) lets the
    // begin/iterate/end callbacks tell them apart, so pushdown is safe by default.
    aggregate_pushdown_enabled: bool,
}

impl SybaseFdw {
    // Map Sybase SQL Anywhere data types to PostgreSQL types
    fn map_sybase_type(sybase_type: &str) -> Option<&'static str> {
        let type_lower = sybase_type.to_lowercase();
        let base_type = type_lower.split('(').next().unwrap_or(&type_lower).trim();

        match base_type {
            "integer" | "int" | "signed int" => Some("integer"),
            "bigint" | "signed bigint" => Some("bigint"),
            "smallint" | "signed smallint" => Some("smallint"),
            "tinyint" | "unsigned tinyint" => Some("smallint"),
            "unsigned int" | "unsigned integer" => Some("bigint"),
            "unsigned bigint" => Some("numeric"),
            "decimal" | "numeric" | "money" | "smallmoney" => Some("numeric"),
            "float" | "double" | "double precision" => Some("double precision"),
            "real" => Some("real"),
            "bit" => Some("boolean"),
            "char" | "character" => Some("text"),
            "varchar" | "character varying" | "long varchar" => Some("text"),
            "nchar" | "nvarchar" | "long nvarchar" => Some("text"),
            "text" | "ntext" => Some("text"),
            "uniqueidentifierstr" | "uniqueidentifier" => Some("text"),
            "binary" | "varbinary" | "long binary" => Some("bytea"),
            "image" => Some("bytea"),
            "date" => Some("date"),
            "time" => Some("time"),
            "datetime" | "smalldatetime" | "timestamp" => Some("timestamp"),
            "datetimeoffset" => Some("timestamptz"),
            "xml" => Some("xml"),
            _ => Some("text"),
        }
    }

    fn get_schema_tables(&self, schema: &str, table_filter: &str) -> SybaseFdwResult<Vec<String>> {
        let conn = connect_unlocked(&self.conn_str)?;

        let sql = format!(
            "SELECT t.table_name \
             FROM sys.systable t \
             JOIN sys.sysuser u ON t.creator = u.user_id \
             WHERE u.user_name = '{}' \
             {} \
             ORDER BY t.table_name",
            schema.replace('\'', "''"),
            table_filter
        );

        let mut tables = Vec::new();

        if let Some(mut cursor) = conn.execute(&sql, ())? {
            let batch_size = 5000;
            let mut buffers = TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096))?;
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            while let Some(batch) = row_set_cursor.fetch()? {
                for row_idx in 0..batch.num_rows() {
                    if let Some(value) = batch.at(0, row_idx) {
                        let table_name = String::from_utf8_lossy(value).trim().to_string();
                        tables.push(table_name);
                    }
                }
            }
        }

        Ok(tables)
    }

    fn get_table_columns(
        &self,
        schema: &str,
        table: &str,
    ) -> SybaseFdwResult<Vec<(String, String, bool)>> {
        let conn = connect_unlocked(&self.conn_str)?;

        let sql = format!(
            "SELECT c.column_name, d.domain_name, c.nulls \
             FROM sys.syscolumn c \
             JOIN sys.systable t ON c.table_id = t.table_id \
             JOIN sys.sysuser u ON t.creator = u.user_id \
             JOIN sys.sysdomain d ON c.domain_id = d.domain_id \
             WHERE u.user_name = '{}' \
             AND t.table_name = '{}' \
             ORDER BY c.column_id",
            schema.replace('\'', "''"),
            table.replace('\'', "''")
        );

        let mut columns = Vec::new();

        if let Some(mut cursor) = conn.execute(&sql, ())? {
            let batch_size = 500;
            let mut buffers = TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096))?;
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            while let Some(batch) = row_set_cursor.fetch()? {
                for row_idx in 0..batch.num_rows() {
                    let col_name = batch
                        .at(0, row_idx)
                        .map(|v| String::from_utf8_lossy(v).trim().to_string())
                        .unwrap_or_default();
                    let col_type = batch
                        .at(1, row_idx)
                        .map(|v| String::from_utf8_lossy(v).trim().to_string())
                        .unwrap_or_default();
                    let nullable = batch
                        .at(2, row_idx)
                        .map(|v| String::from_utf8_lossy(v).trim() == "Y")
                        .unwrap_or(true);

                    if !col_name.is_empty() {
                        columns.push((col_name, col_type, nullable));
                    }
                }
            }
        }

        Ok(columns)
    }

    fn generate_create_table_ddl(
        &self,
        schema: &str,
        table: &str,
        server_name: &str,
        local_schema: &str,
        columns: &[(String, String, bool)],
    ) -> Option<String> {
        if columns.is_empty() {
            return None;
        }

        let column_defs: Vec<String> = columns
            .iter()
            .filter_map(|(col_name, col_type, nullable)| {
                Self::map_sybase_type(col_type).map(|pg_type| {
                    let null_str = if *nullable { "" } else { " NOT NULL" };
                    let escaped_col = if col_name.chars().all(|c| c.is_alphanumeric() || c == '_')
                        && !col_name
                            .chars()
                            .next()
                            .map(|c| c.is_numeric())
                            .unwrap_or(false)
                    {
                        col_name.to_lowercase()
                    } else {
                        format!("\"{}\"", col_name.to_lowercase())
                    };
                    format!("    {escaped_col} {pg_type}{null_str}")
                })
            })
            .collect();

        if column_defs.is_empty() {
            return None;
        }

        let pg_table_name = table.to_lowercase();

        Some(format!(
            "CREATE FOREIGN TABLE IF NOT EXISTS {local_schema}.{pg_table_name} (\n{}\n) SERVER {server_name} OPTIONS (table '{schema}.{table}')",
            column_defs.join(",\n"),
        ))
    }

    fn deparse(
        &self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> SybaseFdwResult<String> {
        let tgts = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        };

        // Only push down LIMIT when there is also ORDER BY.
        // This prevents issues with nested loop JOINs where PostgreSQL executes
        // the foreign scan multiple times. Without ORDER BY, the LIMIT could
        // cause incorrect results by returning only the first N rows on each
        // iteration, missing rows that should match the JOIN conditions.
        let mut sql = if let Some(limit) = limit {
            if !sorts.is_empty() {
                let real_limit = limit.offset + limit.count;
                format!(
                    "SELECT TOP {real_limit} {tgts} FROM {} AS _wrappers_tbl",
                    &self.table
                )
            } else {
                format!("SELECT {tgts} FROM {} AS _wrappers_tbl", &self.table)
            }
        } else {
            format!("SELECT {tgts} FROM {} AS _wrappers_tbl", &self.table)
        };

        if !quals.is_empty() {
            let mut fmt = SybaseCellFormatter {};
            let cond = quals
                .iter()
                .map(|q| deparse_qual_to_sybase(q, &mut fmt))
                .collect::<Vec<String>>()
                .join(" AND ");

            if !cond.is_empty() {
                sql.push_str(&format!(" WHERE {cond}"));
            }
        }

        if !sorts.is_empty() {
            let order_by = sorts
                .iter()
                .map(|sort| {
                    let mut clause = sort.field.to_string();
                    if sort.reversed {
                        clause.push_str(" DESC");
                    } else {
                        clause.push_str(" ASC");
                    }
                    clause
                })
                .collect::<Vec<String>>()
                .join(", ");
            sql.push_str(&format!(" ORDER BY {order_by}"));
        }

        Ok(sql)
    }

    /// Open a streaming cursor for `sql`. Replaces any in-progress stream.
    ///
    /// Connection management: we take the cached connection (or create one
    /// if absent), hand it to `StreamingScan::open`, and store the resulting
    /// stream. The connection is recovered later by `close_stream`.
    fn execute_query(&mut self, sql: &str) -> SybaseFdwResult<()> {
        // Close any previous stream and recover its connection. This handles
        // re-execution (e.g. parameterized scans where iter_scan triggers a
        // late execute_query, or aggregate vs base-rel paths racing on the
        // same state).
        self.close_stream();

        let conn = match self.cached_conn.take() {
            Some(c) => c,
            None => connect_unlocked(&self.conn_str)?,
        };

        match StreamingScan::open(conn, sql) {
            Ok(Some(stream)) => {
                self.stream = Some(stream);
                Ok(())
            }
            Ok(None) => {
                // Statement produced no result set (DDL, etc.) — leave stream
                // unset; `iter_scan` will return None on the first call.
                // The connection was consumed; create a fresh one for the
                // next scan.
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Drop any in-progress stream. If the cursor was drained to EOF, the
    /// underlying connection is recycled into `cached_conn` for the next
    /// rescan; if not (e.g. PG stopped iterating early because of LIMIT or a
    /// JOIN early-exit), the connection is dropped — closing a FreeTDS
    /// cursor mid-stream leaves the wire protocol out of sync ("Bad token
    /// from the server: Datastream processing out of sync") on the next
    /// query over the same connection.
    fn close_stream(&mut self) {
        if let Some(stream) = self.stream.take() {
            if stream.eof {
                self.cached_conn = Some(stream.into_connection());
            }
            // else: drop the whole StreamingScan including the connection;
            // the next scan will open a fresh one.
        }
    }

    fn do_execute_query_with_stats(
        &mut self,
        quals: &[Qual],
        sorts: &[Sort],
        limit: &Option<Limit>,
    ) -> SybaseFdwResult<()> {
        let tgt_cols = self.tgt_cols.clone();
        let sql = self.deparse(quals, &tgt_cols, sorts, limit)?;
        self.execute_query(&sql)?;
        self.query_executed = true;

        Ok(())
    }
}

impl ForeignDataWrapper<SybaseFdwError> for SybaseFdw {
    fn new(server: ForeignServer) -> SybaseFdwResult<Self> {
        let conn_str = if let Some(dsn) = server.options.get("dsn") {
            let user = server.options.get("user").map(|s| s.as_str()).unwrap_or("");
            let password = server
                .options
                .get("password")
                .map(|s| s.as_str())
                .unwrap_or("");
            format!("DSN={dsn};UID={user};PWD={password}")
        } else if let Some(conn_string) = server.options.get("conn_string") {
            conn_string.to_owned()
        } else if let Some(conn_string_id) = server.options.get("conn_string_id") {
            get_vault_secret(conn_string_id).unwrap_or_default()
        } else {
            let host = require_option("host", &server.options)?;
            let port = server
                .options
                .get("port")
                .map(|s| s.as_str())
                .unwrap_or("2638");
            let database = server
                .options
                .get("database")
                .map(|s| s.as_str())
                .unwrap_or("");
            let user = server.options.get("user").map(|s| s.as_str()).unwrap_or("");
            let password = server
                .options
                .get("password")
                .map(|s| s.as_str())
                .unwrap_or("");

            format!(
                "DRIVER={{FreeTDS}};SERVER={host};PORT={port};DATABASE={database};UID={user};PWD={password};TDS_Version=5.0"
            )
        };

        // Default ON; set `aggregate_pushdown=false` to fall back to local
        // aggregation. Without pushdown, queries like the prod CTE that
        // materialised SUM/GROUP BY over ~600k rows of efservicospag (see
        // sybase_fdw_oom_buffering memory) still buffer every row in the
        // backend and risk OOM regardless of streaming.
        let aggregate_pushdown_enabled = server
            .options
            .get("aggregate_pushdown")
            .map(|v| !(v.eq_ignore_ascii_case("false") || v == "0"))
            .unwrap_or(true);

        Ok(SybaseFdw {
            conn_str,
            table: String::default(),
            tgt_cols: Vec::new(),
            stream: None,
            query_executed: false,
            stored_quals: Vec::new(),
            stored_sorts: Vec::new(),
            stored_limit: None,
            cached_conn: None,
            aggregate_pushdown_enabled,
        })
    }

    fn get_rel_size(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> SybaseFdwResult<(i64, i32)> {
        // Allow explicit override via table option. A value <= 0 is treated
        // as "stale stats" — common when an automated job sets `rows` from
        // sys.systable.count and the engine hasn't refreshed yet (and
        // **always** the case for VIEWs, whose count column is 0). Honoring
        // 0 as literal-zero collapses the FT to 1 estimated row via
        // apply_quals_selectivity, which then poisons join cardinalities and
        // forces NestedLoop with parameterized inner FTs (5+ minutes of ODBC
        // round-trips per outer-row in prod for the getPayrollData /
        // getPayrollSummary patterns). Fall through to the catalog query
        // path, which handles views via `view_estimated_rows`.
        if let Some(rows) = options.get("rows").and_then(|s| s.parse::<i64>().ok())
            && rows > 0
        {
            let width = if columns.is_empty() {
                100
            } else {
                (columns.len() * 50) as i32
            };
            return Ok((apply_quals_selectivity(rows, quals), width));
        }

        let table = options.get("table").cloned().unwrap_or_default();

        // Check cache first. The cache holds the BASE row count (without
        // qual selectivity applied) so we can recompute per-query selectivity.
        if let Ok(cache) = ROW_COUNT_CACHE.lock() {
            if let Some(&count) = cache.get(&table) {
                let width = if columns.is_empty() {
                    100
                } else {
                    (columns.len() * 50) as i32
                };
                return Ok((apply_quals_selectivity(count, quals), width));
            }
        }

        // Query Sybase system catalog for row count estimate.
        // Uses sys.systable.count which is maintained by the engine and
        // is nearly instant, unlike SELECT COUNT(*) which does a full scan
        // (e.g., 16 seconds for a 28k-row table).
        //
        // Also fetches table_type: VIEWs always have count=0 in sys.systable,
        // so we use a configurable default for them (option `view_estimated_rows`,
        // default 100000). Without this, the planner would think views have
        // zero rows and create terrible plans (e.g., pushing joins that return
        // hundreds of thousands of rows).
        let view_estimated_rows = options
            .get("view_estimated_rows")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(100_000);

        let start = std::time::Instant::now();
        let rows = if !table.is_empty() {
            let parts: Vec<&str> = table.split('.').collect();
            if parts.len() == 2 {
                let schema = parts[0].replace('\'', "''");
                let table_name = parts[1].replace('\'', "''");
                match connect_unlocked(&self.conn_str) {
                    Ok(conn) => {
                        let sql = format!(
                            "SELECT t.count, t.table_type FROM sys.systable t \
                             JOIN sys.sysuser u ON t.creator = u.user_id \
                             WHERE u.user_name = '{schema}' AND t.table_name = '{table_name}'"
                        );
                        match conn.execute(&sql, ()) {
                            Ok(Some(mut cursor)) => {
                                let mut buffers =
                                    TextRowSet::for_cursor(1, &mut cursor, Some(256)).unwrap();
                                let mut row_set = cursor.bind_buffer(&mut buffers).unwrap();
                                if let Some(batch) = row_set.fetch().unwrap_or(None) {
                                    let count = batch
                                        .at(0, 0)
                                        .and_then(|b| std::str::from_utf8(b).ok())
                                        .and_then(|s| s.trim().parse::<i64>().ok())
                                        .unwrap_or(0);
                                    let table_type = batch
                                        .at(1, 0)
                                        .and_then(|b| std::str::from_utf8(b).ok())
                                        .map(|s| s.trim().to_uppercase())
                                        .unwrap_or_default();

                                    if count > 0 {
                                        count
                                    } else if table_type == "VIEW" {
                                        // Views always have count=0 in sys.systable.
                                        view_estimated_rows
                                    } else {
                                        // Genuinely empty BASE table — use small default
                                        // so the planner doesn't assume zero rows.
                                        100
                                    }
                                } else {
                                    1000
                                }
                            }
                            _ => 1000,
                        }
                    }
                    Err(e) => {
                        pgrx::debug1!("get_rel_size: connection error for {}: {:?}", table, e);
                        1000
                    }
                }
            } else {
                1000
            }
        } else {
            1000
        };
        let elapsed = start.elapsed();
        pgrx::debug1!(
            "get_rel_size: table={}, rows={}, elapsed={:.3}s",
            table,
            rows,
            elapsed.as_secs_f64()
        );

        // Cache the BASE count (no quals applied yet).
        if let Ok(mut cache) = ROW_COUNT_CACHE.lock() {
            cache.insert(table.clone(), rows);
        }

        // Apply qual selectivity (PG defaults from selfuncs.h).
        //
        // Without column stats, PG falls back to DEFAULT_EQ_SEL=0.005,
        // DEFAULT_INEQ_SEL=0.3333. For Foreign Tables there are no stats at
        // all, so the planner would otherwise treat every WHERE as
        // non-restrictive (selectivity=1.0) and estimate full tables on every
        // join, producing astronomical row counts (37e12 was observed on a
        // 4-FT join, leading the planner to pick NestedLoop-LATERAL with
        // ~22k remote round-trips). Apply these defaults manually so the
        // join cardinalities stay sane.
        let rows = apply_quals_selectivity(rows, quals);

        let width = if columns.is_empty() {
            100
        } else {
            (columns.len() * 50) as i32
        };

        Ok((rows, width))
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> SybaseFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.tgt_cols = columns.to_vec();
        self.close_stream();
        self.query_executed = false;

        self.stored_quals = quals.to_vec();
        self.stored_sorts = sorts.to_vec();
        self.stored_limit = limit.clone();

        // When quals have unevaluated parameters (from JOINs), defer query
        // execution to iter_scan where the parameter values will be available.
        let has_params = quals.iter().any(|q| q.param.is_some());

        if has_params {
            Ok(())
        } else {
            self.do_execute_query_with_stats(quals, sorts, limit)
        }
    }

    fn iter_scan(&mut self, row: &mut Row) -> SybaseFdwResult<Option<()>> {
        // Lazy execution: parameters from JOINs are now evaluated
        if !self.query_executed {
            // Refresh stored quals with current parameter values.
            // The framework's assign_paramenter_value updates the shared
            // eval_value (Arc<Mutex<...>>) but not our cloned qual.value.
            for qual in &mut self.stored_quals {
                if let Some(ref param) = qual.param {
                    if let Ok(guard) = param.eval_value.lock() {
                        if let Some(value) = guard.clone() {
                            qual.value = value;
                        }
                    }
                }
            }

            let quals = self.stored_quals.clone();
            let sorts = self.stored_sorts.clone();
            let limit = self.stored_limit.clone();

            self.do_execute_query_with_stats(&quals, &sorts, &limit)?;
        }

        // Pull the next row from the streaming cursor. `next_row` handles
        // batch refills internally; it returns `None` once the result set
        // is exhausted.
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return Ok(None),
        };

        // Collect the raw values out of the borrow before calling
        // `value_to_cell` (which can't run while `stream` is borrowed).
        let owned_values: Vec<Option<String>> = match stream.next_row()? {
            Some(values) => values.to_vec(),
            None => return Ok(None),
        };

        let mut tgt_row = Row::new();
        for (col_idx, tgt_col) in self.tgt_cols.iter().enumerate() {
            let value = owned_values.get(col_idx).and_then(|v| v.as_deref());
            let cell = value_to_cell(value, tgt_col.type_oid)?;
            tgt_row.push(&tgt_col.name, cell);
        }

        row.replace_with(tgt_row);
        Ok(Some(()))
    }

    fn re_scan(&mut self) -> SybaseFdwResult<()> {
        // With streaming we cannot rewind the ODBC cursor — re-executing the
        // remote query is the only correct behavior. Upstream's
        // re_scan_foreign_scan only invokes re_scan when the parameter
        // fingerprint is unchanged (fresh-param scans go through
        // end_scan + begin_scan), so this is the in-loop rescan path.
        self.close_stream();
        self.query_executed = false;

        if self.stored_quals.iter().all(|q| q.param.is_none()) {
            let quals = self.stored_quals.clone();
            let sorts = self.stored_sorts.clone();
            let limit = self.stored_limit.clone();
            self.do_execute_query_with_stats(&quals, &sorts, &limit)?;
        }
        // Parameterized rescans fall back to the lazy path in `iter_scan`,
        // matching `begin_scan`'s deferred-execution behavior.

        Ok(())
    }

    fn end_scan(&mut self) -> SybaseFdwResult<()> {
        self.close_stream();

        // FdwState follows the cached plan's lifetime and can survive until
        // DEALLOCATE or session end.  An ODBC connection is a per-executor-
        // scan resource and must not survive EndForeignScan with that state.
        // Keep the cache only for rescans performed before this hook.
        self.cached_conn = None;
        Ok(())
    }

    fn supported_aggregates(&self) -> Vec<AggregateKind> {
        if !self.aggregate_pushdown_enabled {
            return Vec::new();
        }
        vec![
            AggregateKind::Count,
            AggregateKind::CountColumn,
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
        ]
    }

    fn supports_group_by(&self) -> bool {
        self.aggregate_pushdown_enabled
    }

    fn begin_aggregate_scan(
        &mut self,
        aggregates: &[Aggregate],
        group_by: &[Column],
        quals: &[Qual],
        options: &HashMap<String, String>,
    ) -> SybaseFdwResult<()> {
        self.table = require_option("table", options)?.to_string();
        self.close_stream();
        self.query_executed = false;
        self.stored_quals.clear();
        self.stored_sorts.clear();
        self.stored_limit = None;

        // tgt_cols must match the SELECT order exactly: group-by columns
        // first, then aggregate result columns named by their aliases.
        // iter_scan uses tgt_col.name to read the matching value from
        // the ODBC result row.
        let mut tgt_cols: Vec<Column> = group_by.to_vec();
        for (i, agg) in aggregates.iter().enumerate() {
            tgt_cols.push(Column {
                name: agg.alias.clone(),
                num: group_by.len() + i + 1,
                type_oid: agg.type_oid,
            });
        }
        self.tgt_cols = tgt_cols;

        let sql = deparse_aggregate_sybase(&self.table, aggregates, group_by, quals);
        self.execute_query(&sql)?;
        self.query_executed = true;
        Ok(())
    }

    fn fdw_routine_hook(
        routine: &mut pgrx::PgBox<pgrx::pg_sys::FdwRoutine, pgrx::AllocatedByRust>,
    ) {
        super::join::install_join_hooks(routine);
    }

    fn import_foreign_schema(
        &mut self,
        stmt: ImportForeignSchemaStmt,
    ) -> SybaseFdwResult<Vec<String>> {
        let mut ret: Vec<String> = Vec::new();

        let schema = &stmt.remote_schema;

        let table_list = stmt
            .table_list
            .iter()
            .map(|name| format!("'{}'", name.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");

        let table_filter = match stmt.list_type {
            ImportSchemaType::FdwImportSchemaAll => String::new(),
            ImportSchemaType::FdwImportSchemaLimitTo => {
                format!("AND t.table_name IN ({table_list})")
            }
            ImportSchemaType::FdwImportSchemaExcept => {
                format!("AND t.table_name NOT IN ({table_list})")
            }
        };

        let tables = self.get_schema_tables(schema, &table_filter)?;

        for table in tables {
            let columns = self.get_table_columns(schema, &table)?;

            if let Some(ddl) = self.generate_create_table_ddl(
                schema,
                &table,
                &stmt.server_name,
                &stmt.local_schema,
                &columns,
            ) {
                ret.push(ddl);
            }
        }

        Ok(ret)
    }
}
