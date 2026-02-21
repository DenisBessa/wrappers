use crate::stats;
use odbc_api::{ConnectionOptions, Cursor, Environment, ResultSetMetadata, buffers::TextRowSet};
use pgrx::{PgBuiltInOids, PgOid, pg_sys::Oid, prelude::Date};
use std::collections::HashMap;
use std::sync::LazyLock;

use supabase_wrappers::prelude::*;

use super::{SybaseFdwError, SybaseFdwResult};

// Global ODBC environment - thread-safe singleton
pub(super) static ODBC_ENV: LazyLock<Environment> =
    LazyLock::new(|| Environment::new().expect("Failed to create ODBC environment"));

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

struct SybaseCellFormatter {}

impl CellFormatter for SybaseCellFormatter {
    fn fmt_cell(&mut self, cell: &Cell) -> String {
        match cell {
            Cell::Bool(v) => format!("{}", *v as u8),
            _ => format!("{cell}"),
        }
    }
}

// Store fetched rows
struct FetchedRow {
    values: Vec<Option<String>>,
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
    scan_result: Vec<FetchedRow>,
    iter_idx: usize,

    // Lazy execution support for parameterized queries.
    // When quals contain parameters (from JOINs), we defer query execution
    // until iter_scan, when the parameter values have been evaluated.
    query_executed: bool,
    stored_quals: Vec<Qual>,
    stored_sorts: Vec<Sort>,
    stored_limit: Option<Limit>,
}

impl SybaseFdw {
    const FDW_NAME: &'static str = "SybaseFdw";

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
        let conn = ODBC_ENV
            .connect_with_connection_string(&self.conn_str, ConnectionOptions::default())?;

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
            let batch_size = 1000;
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
        let conn = ODBC_ENV
            .connect_with_connection_string(&self.conn_str, ConnectionOptions::default())?;

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
            let cond = quals
                .iter()
                .map(|q| {
                    let oper = q.operator.as_str();
                    let mut fmt = SybaseCellFormatter {};
                    if let Value::Cell(cell) = &q.value
                        && let Cell::Bool(_) = cell
                    {
                        if oper == "is" {
                            return format!("{} = {}", q.field, fmt.fmt_cell(cell));
                        } else if oper == "is not" {
                            return format!("{} <> {}", q.field, fmt.fmt_cell(cell));
                        }
                    }
                    q.deparse_with_fmt(&mut fmt)
                })
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

    fn execute_query(&mut self, sql: &str) -> SybaseFdwResult<()> {
        let conn = ODBC_ENV
            .connect_with_connection_string(&self.conn_str, ConnectionOptions::default())?;

        if let Some(mut cursor) = conn.execute(sql, ())? {
            let num_cols = cursor.num_result_cols()? as usize;

            let batch_size = 1000;
            let mut buffers = TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096))?;
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            while let Some(batch) = row_set_cursor.fetch()? {
                for row_idx in 0..batch.num_rows() {
                    let mut values = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        let value = batch
                            .at(col_idx, row_idx)
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string());
                        values.push(value);
                    }
                    self.scan_result.push(FetchedRow { values });
                }
            }
        }

        Ok(())
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

        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsIn,
            self.scan_result.len() as i64,
        );
        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::RowsOut,
            self.scan_result.len() as i64,
        );

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

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(SybaseFdw {
            conn_str,
            table: String::default(),
            tgt_cols: Vec::new(),
            scan_result: Vec::new(),
            iter_idx: 0,
            query_executed: false,
            stored_quals: Vec::new(),
            stored_sorts: Vec::new(),
            stored_limit: None,
        })
    }

    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> SybaseFdwResult<(i64, i32)> {
        let rows = options
            .get("rows")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(1000);

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
        self.iter_idx = 0;
        self.scan_result.clear();
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
            let quals = self.stored_quals.clone();
            let sorts = self.stored_sorts.clone();
            let limit = self.stored_limit.clone();

            self.do_execute_query_with_stats(&quals, &sorts, &limit)?;
        }

        if self.iter_idx >= self.scan_result.len() {
            return Ok(None);
        }

        let src_row = &self.scan_result[self.iter_idx];
        let mut tgt_row = Row::new();

        for (col_idx, tgt_col) in self.tgt_cols.iter().enumerate() {
            let value = src_row.values.get(col_idx).and_then(|v| v.as_deref());
            let cell = value_to_cell(value, tgt_col.type_oid)?;
            tgt_row.push(&tgt_col.name, cell);
        }

        row.replace_with(tgt_row);
        self.iter_idx += 1;

        Ok(Some(()))
    }

    fn re_scan(&mut self) -> SybaseFdwResult<()> {
        self.iter_idx = 0;

        // If we have parameterized quals, re-execute with new parameter values
        let has_params = self.stored_quals.iter().any(|q| q.param.is_some());
        if has_params {
            self.scan_result.clear();
            self.query_executed = false;
        }

        Ok(())
    }

    fn end_scan(&mut self) -> SybaseFdwResult<()> {
        self.scan_result.clear();
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
