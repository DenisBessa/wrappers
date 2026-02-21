//! Join Pushdown support for the Sybase FDW.
//!
//! When two Sybase foreign tables on the same server are joined, this module
//! pushes the JOIN operation down to Sybase, executing a single remote query
//! instead of fetching each table separately and joining locally.

#![allow(unsafe_op_in_unsafe_fn)]

use odbc_api::{ConnectionOptions, Cursor, ResultSetMetadata, buffers::TextRowSet};
use pgrx::{
    AllocatedByRust, FromDatum, IntoDatum, PgBox,
    list::List,
    memcxt::PgMemoryContexts,
    pg_guard, pg_sys,
    pg_sys::{Datum, Oid},
};
use std::collections::HashMap;
use std::ffi::{CStr, c_void};
use std::os::raw::c_int;
use std::ptr;
use std::sync::OnceLock;

use super::sybase_fdw::{ODBC_ENV, value_to_cell};
use crate::stats;
use supabase_wrappers::prelude::*;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A column in the join result target list.
#[derive(Debug, Clone)]
struct JoinTargetColumn {
    /// SQL expression for this column in the join query (e.g. "t_o.id")
    sql_expr: String,
    /// Output column name
    #[allow(dead_code)]
    output_name: String,
    /// PostgreSQL type OID
    type_oid: Oid,
}

/// Planning data stored in joinrel->fdw_private and carried to execution.
#[derive(Debug, Clone)]
struct JoinPlanData {
    /// The complete join SQL to execute remotely
    sql: String,
    /// ODBC connection string
    conn_str: String,
    /// Target columns in result order
    target_columns: Vec<JoinTargetColumn>,
    /// fdw_scan_tlist (only valid during planning, must be copied before use)
    fdw_scan_tlist: *mut pg_sys::List,
}

unsafe impl Send for JoinPlanData {}
unsafe impl Sync for JoinPlanData {}

/// Execution state for a join scan.
struct JoinScanState {
    plan_data: JoinPlanData,
    /// Fetched result rows (string values from ODBC)
    rows: Vec<Vec<Option<String>>>,
    /// Current row index during iteration
    row_idx: usize,
    /// Pre-allocated values array for ExecStoreVirtualTuple
    values: Vec<Datum>,
    /// Pre-allocated nulls array for ExecStoreVirtualTuple
    nulls: Vec<bool>,
}

/// Information about one relation participating in the join.
#[derive(Debug, Clone)]
struct JoinRelInfo {
    /// Remote table name (from foreign table option "table")
    table_name: String,
    /// SQL alias (e.g., "t_o", "t_i")
    alias: String,
    /// Server OID
    #[allow(dead_code)]
    server_oid: Oid,
}

// ---------------------------------------------------------------------------
// Saved original callbacks
// ---------------------------------------------------------------------------

type GetForeignPlanFn = unsafe extern "C-unwind" fn(
    *mut pg_sys::PlannerInfo,
    *mut pg_sys::RelOptInfo,
    Oid,
    *mut pg_sys::ForeignPath,
    *mut pg_sys::List,
    *mut pg_sys::List,
    *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan;

type BeginForeignScanFn = unsafe extern "C-unwind" fn(*mut pg_sys::ForeignScanState, c_int);

type IterateForeignScanFn =
    unsafe extern "C-unwind" fn(*mut pg_sys::ForeignScanState) -> *mut pg_sys::TupleTableSlot;

type ReScanForeignScanFn = unsafe extern "C-unwind" fn(*mut pg_sys::ForeignScanState);

type EndForeignScanFn = unsafe extern "C-unwind" fn(*mut pg_sys::ForeignScanState);

type ExplainForeignScanFn =
    unsafe extern "C-unwind" fn(*mut pg_sys::ForeignScanState, *mut pg_sys::ExplainState);

struct OriginalCallbacks {
    get_foreign_plan: Option<GetForeignPlanFn>,
    begin_foreign_scan: Option<BeginForeignScanFn>,
    iterate_foreign_scan: Option<IterateForeignScanFn>,
    re_scan_foreign_scan: Option<ReScanForeignScanFn>,
    end_foreign_scan: Option<EndForeignScanFn>,
    explain_foreign_scan: Option<ExplainForeignScanFn>,
}

// SAFETY: PostgreSQL backends are single-threaded. OnceLock ensures safe init.
unsafe impl Send for OriginalCallbacks {}
unsafe impl Sync for OriginalCallbacks {}

static ORIGINAL_CALLBACKS: OnceLock<OriginalCallbacks> = OnceLock::new();

// ---------------------------------------------------------------------------
// Hook installation (called from SybaseFdw::fdw_routine_hook)
// ---------------------------------------------------------------------------

pub(crate) fn install_join_hooks(routine: &mut PgBox<pg_sys::FdwRoutine, AllocatedByRust>) {
    let _ = ORIGINAL_CALLBACKS.set(OriginalCallbacks {
        get_foreign_plan: routine.GetForeignPlan,
        begin_foreign_scan: routine.BeginForeignScan,
        iterate_foreign_scan: routine.IterateForeignScan,
        re_scan_foreign_scan: routine.ReScanForeignScan,
        end_foreign_scan: routine.EndForeignScan,
        explain_foreign_scan: routine.ExplainForeignScan,
    });

    routine.GetForeignJoinPaths = Some(sybase_get_foreign_join_paths);
    routine.GetForeignPlan = Some(sybase_get_foreign_plan);
    routine.BeginForeignScan = Some(sybase_begin_foreign_scan);
    routine.IterateForeignScan = Some(sybase_iterate_foreign_scan);
    routine.ReScanForeignScan = Some(sybase_re_scan_foreign_scan);
    routine.EndForeignScan = Some(sybase_end_foreign_scan);
    routine.ExplainForeignScan = Some(sybase_explain_foreign_scan);
}

// ---------------------------------------------------------------------------
// GetForeignJoinPaths
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_get_foreign_join_paths(
    root: *mut pg_sys::PlannerInfo,
    joinrel: *mut pg_sys::RelOptInfo,
    outerrel: *mut pg_sys::RelOptInfo,
    innerrel: *mut pg_sys::RelOptInfo,
    jointype: pg_sys::JoinType::Type,
    extra: *mut pg_sys::JoinPathExtraData,
) {
    unsafe {
        // Only support INNER, LEFT, RIGHT joins
        if jointype != pg_sys::JoinType::JOIN_INNER
            && jointype != pg_sys::JoinType::JOIN_LEFT
            && jointype != pg_sys::JoinType::JOIN_RIGHT
        {
            return;
        }

        // Both rels must be foreign tables (base or already-pushed joins)
        if !is_pushable_relation(outerrel) || !is_pushable_relation(innerrel) {
            return;
        }

        // Must be on the same server
        let outer_sid = (*outerrel).serverid;
        let inner_sid = (*innerrel).serverid;
        if outer_sid == pg_sys::InvalidOid || inner_sid == pg_sys::InvalidOid {
            return;
        }
        if outer_sid != inner_sid {
            return;
        }

        // Extract join clauses from extra->restrictlist and check pushability
        let join_conditions = match extract_join_conditions(root, extra, outerrel, innerrel) {
            Some(c) if !c.is_empty() => c,
            _ => return,
        };

        // Extract relation info
        let outer_info = match extract_rel_info(root, outerrel, "t_o") {
            Some(i) => i,
            None => return,
        };
        let inner_info = match extract_rel_info(root, innerrel, "t_i") {
            Some(i) => i,
            None => return,
        };

        // Build target columns and fdw_scan_tlist
        let (fdw_scan_tlist, target_columns) = build_target_columns_and_tlist(
            root,
            joinrel,
            outerrel,
            innerrel,
            &outer_info,
            &inner_info,
        );
        if target_columns.is_empty() {
            return;
        }

        // Build the join SQL
        let join_sql = deparse_join_sql(
            &outer_info,
            &inner_info,
            jointype,
            &target_columns,
            &join_conditions,
        );

        // Get connection string from the server
        let conn_str = match get_server_conn_str(outer_sid) {
            Some(s) => s,
            None => return,
        };

        let plan_data = JoinPlanData {
            sql: join_sql,
            conn_str,
            target_columns,
            fdw_scan_tlist,
        };

        // Store plan_data in joinrel->fdw_private
        let boxed = Box::new(plan_data);
        (*joinrel).fdw_private = Box::into_raw(boxed) as *mut c_void;

        // Cost estimation
        let rows = (*outerrel).rows * (*innerrel).rows * 0.01;
        let rows = if rows < 1.0 { 1.0 } else { rows };
        let startup_cost = 10.0;
        let total_cost = startup_cost + rows * 0.01;

        let path = pg_sys::create_foreign_join_path(
            root,
            joinrel,
            ptr::null_mut(), // default pathtarget
            rows,
            #[cfg(feature = "pg18")]
            0, // disabled_nodes
            startup_cost,
            total_cost,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no required_outer
            ptr::null_mut(), // no fdw_outerpath
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(), // no fdw_restrictinfo
            ptr::null_mut(), // no fdw_private on path
        );
        pg_sys::add_path(joinrel, &mut (*path).path);
    }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

unsafe fn is_pushable_relation(rel: *mut pg_sys::RelOptInfo) -> bool {
    unsafe {
        // Only push down joins between two base foreign tables.
        // Multi-way join pushdown (where one input is already a pushed join)
        // is disabled because nested subquery aliasing with t_o/t_i produces
        // incorrect SQL for complex join trees.
        let kind = (*rel).reloptkind;
        kind == pg_sys::RelOptKind::RELOPT_BASEREL && (*rel).serverid != pg_sys::InvalidOid
    }
}

/// Parse a DefElem list (from server/table options) into a HashMap.
/// Local reimplementation since the framework's `options_to_hashmap` is pub(super).
unsafe fn defelem_list_to_hashmap(options: *mut pg_sys::List) -> Option<HashMap<String, String>> {
    unsafe {
        let mut ret = HashMap::new();
        pgrx::memcx::current_context(|mcx| {
            if let Some(list) = List::<*mut c_void>::downcast_ptr_in_memcx(options, mcx) {
                for item in list.iter() {
                    let elem = *item as *mut pg_sys::DefElem;
                    if elem.is_null() {
                        continue;
                    }
                    let name = CStr::from_ptr((*elem).defname).to_str().ok()?;
                    let value = CStr::from_ptr(pg_sys::defGetString(elem)).to_str().ok()?;
                    ret.insert(name.to_string(), value.to_string());
                }
            }
            Some(())
        })?;
        Some(ret)
    }
}

/// Get connection string from server options.
unsafe fn get_server_conn_str(server_oid: Oid) -> Option<String> {
    unsafe {
        let fserver = pg_sys::GetForeignServer(server_oid);
        if fserver.is_null() {
            return None;
        }

        let opts = defelem_list_to_hashmap((*fserver).options)?;

        if let Some(dsn) = opts.get("dsn") {
            let user = opts.get("user").map(|s| s.as_str()).unwrap_or("");
            let password = opts.get("password").map(|s| s.as_str()).unwrap_or("");
            Some(format!("DSN={dsn};UID={user};PWD={password}"))
        } else if let Some(conn_string) = opts.get("conn_string") {
            Some(conn_string.to_owned())
        } else if let Some(conn_string_id) = opts.get("conn_string_id") {
            get_vault_secret(conn_string_id)
        } else {
            let host = opts.get("host").map(|s| s.as_str()).unwrap_or("localhost");
            let port = opts.get("port").map(|s| s.as_str()).unwrap_or("2638");
            let database = opts.get("database").map(|s| s.as_str()).unwrap_or("");
            let user = opts.get("user").map(|s| s.as_str()).unwrap_or("");
            let password = opts.get("password").map(|s| s.as_str()).unwrap_or("");
            Some(format!(
                "DRIVER={{FreeTDS}};SERVER={host};PORT={port};DATABASE={database};UID={user};PWD={password};TDS_Version=5.0"
            ))
        }
    }
}

/// Extract info about a relation for SQL construction.
unsafe fn extract_rel_info(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    alias: &str,
) -> Option<JoinRelInfo> {
    unsafe {
        if (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL {
            return None;
        }

        let relid = (*rel).relid;
        let rte = pg_sys::planner_rt_fetch(relid as _, root);
        if rte.is_null() {
            return None;
        }
        let ftable_oid = (*rte).relid;
        let ftable = pg_sys::GetForeignTable(ftable_oid);
        if ftable.is_null() {
            return None;
        }
        let opts = defelem_list_to_hashmap((*ftable).options)?;
        let table_name = opts.get("table")?.clone();

        Some(JoinRelInfo {
            table_name,
            alias: alias.to_string(),
            server_oid: (*rel).serverid,
        })
    }
}

// ---------------------------------------------------------------------------
// Join condition extraction
// ---------------------------------------------------------------------------

/// Extract and deparse join conditions from the restrict list.
/// Returns None if any condition is not pushable.
unsafe fn extract_join_conditions(
    root: *mut pg_sys::PlannerInfo,
    extra: *mut pg_sys::JoinPathExtraData,
    outerrel: *mut pg_sys::RelOptInfo,
    innerrel: *mut pg_sys::RelOptInfo,
) -> Option<Vec<String>> {
    unsafe {
        let restrict_list = (*extra).restrictlist;
        if restrict_list.is_null() {
            return Some(Vec::new());
        }

        let mut conditions = Vec::new();

        pgrx::memcx::current_context(|mcx| {
            let list = List::<*mut c_void>::downcast_ptr_in_memcx(restrict_list, mcx)?;

            for item in list.iter() {
                let rinfo = *item as *mut pg_sys::RestrictInfo;
                if rinfo.is_null() {
                    return None;
                }
                let clause = (*rinfo).clause as *mut pg_sys::Node;
                if clause.is_null() {
                    return None;
                }

                let node_tag = (*clause).type_;
                if node_tag == pg_sys::NodeTag::T_OpExpr {
                    let op_expr = clause as *mut pg_sys::OpExpr;
                    match deparse_op_expr(root, op_expr, outerrel, innerrel) {
                        Some(s) => conditions.push(s),
                        None => return None,
                    }
                } else {
                    return None;
                }
            }

            Some(())
        })?;

        Some(conditions)
    }
}

/// Deparse a single OpExpr as a join condition.
unsafe fn deparse_op_expr(
    root: *mut pg_sys::PlannerInfo,
    expr: *mut pg_sys::OpExpr,
    outerrel: *mut pg_sys::RelOptInfo,
    innerrel: *mut pg_sys::RelOptInfo,
) -> Option<String> {
    unsafe {
        let opno = (*expr).opno;
        let opname_ptr = pg_sys::get_opname(opno);
        if opname_ptr.is_null() {
            return None;
        }
        let opname = CStr::from_ptr(opname_ptr).to_str().ok()?.to_string();

        let args_list = (*expr).args;
        if args_list.is_null() {
            return None;
        }

        let (left, right) = pgrx::memcx::current_context(|mcx| {
            let args = List::<*mut c_void>::downcast_ptr_in_memcx(args_list, mcx)?;
            if args.len() != 2 {
                return None;
            }
            let left = *args.get(0)? as *mut pg_sys::Node;
            let right = *args.get(1)? as *mut pg_sys::Node;
            Some((left, right))
        })?;

        let left_str = deparse_expr(root, left, outerrel, innerrel)?;
        let right_str = deparse_expr(root, right, outerrel, innerrel)?;

        Some(format!("{left_str} {opname} {right_str}"))
    }
}

/// Deparse a single expression (Var or Const) for use in join SQL.
unsafe fn deparse_expr(
    root: *mut pg_sys::PlannerInfo,
    node: *mut pg_sys::Node,
    outerrel: *mut pg_sys::RelOptInfo,
    innerrel: *mut pg_sys::RelOptInfo,
) -> Option<String> {
    unsafe {
        let tag = (*node).type_;

        if tag == pg_sys::NodeTag::T_Var {
            let var = node as *mut pg_sys::Var;
            let varno = (*var).varno;
            let attno = (*var).varattno;

            let rte = pg_sys::planner_rt_fetch(varno as _, root);
            if rte.is_null() {
                return None;
            }
            let attname = pg_sys::get_attname((*rte).relid, attno, true);
            if attname.is_null() {
                return None;
            }
            let col_name = CStr::from_ptr(attname).to_str().ok()?;

            let alias = if pg_sys::bms_is_member(varno as c_int, (*outerrel).relids) {
                "t_o"
            } else if pg_sys::bms_is_member(varno as c_int, (*innerrel).relids) {
                "t_i"
            } else {
                return None;
            };

            Some(format!("{alias}.{col_name}"))
        } else if tag == pg_sys::NodeTag::T_Const {
            let c = node as *mut pg_sys::Const;
            if (*c).constisnull {
                return Some("NULL".to_string());
            }
            let cell =
                Cell::from_polymorphic_datum((*c).constvalue, (*c).constisnull, (*c).consttype);
            cell.map(|cell| format!("{cell}"))
        } else if tag == pg_sys::NodeTag::T_RelabelType {
            let relabel = node as *mut pg_sys::RelabelType;
            deparse_expr(
                root,
                (*relabel).arg as *mut pg_sys::Node,
                outerrel,
                innerrel,
            )
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Target list construction
// ---------------------------------------------------------------------------

/// Build fdw_scan_tlist and target column list from joinrel->reltarget.
unsafe fn build_target_columns_and_tlist(
    root: *mut pg_sys::PlannerInfo,
    joinrel: *mut pg_sys::RelOptInfo,
    outerrel: *mut pg_sys::RelOptInfo,
    _innerrel: *mut pg_sys::RelOptInfo,
    outer_info: &JoinRelInfo,
    inner_info: &JoinRelInfo,
) -> (*mut pg_sys::List, Vec<JoinTargetColumn>) {
    unsafe {
        let mut tlist: *mut pg_sys::List = ptr::null_mut();
        let mut target_columns = Vec::new();
        let mut resno: i16 = 1;

        let reltarget = (*joinrel).reltarget;
        if reltarget.is_null() {
            return (ptr::null_mut(), Vec::new());
        }

        let exprs = (*reltarget).exprs;
        if exprs.is_null() {
            return (ptr::null_mut(), Vec::new());
        }

        let col_vars = pg_sys::pull_var_clause(
            exprs as *mut pg_sys::Node,
            (pg_sys::PVC_RECURSE_AGGREGATES | pg_sys::PVC_RECURSE_PLACEHOLDERS)
                .try_into()
                .unwrap(),
        );

        pgrx::memcx::current_context(|mcx| {
            if let Some(var_list) = List::<*mut c_void>::downcast_ptr_in_memcx(col_vars, mcx) {
                for var_ptr in var_list.iter() {
                    let var = *var_ptr as *mut pg_sys::Var;
                    let varno = (*var).varno;
                    let attno = (*var).varattno;

                    let rte = pg_sys::planner_rt_fetch(varno as _, root);
                    if rte.is_null() {
                        continue;
                    }
                    let attname_ptr = pg_sys::get_attname((*rte).relid, attno, true);
                    if attname_ptr.is_null() {
                        continue;
                    }
                    let col_name = CStr::from_ptr(attname_ptr).to_str().unwrap().to_string();
                    let type_oid = pg_sys::get_atttype((*rte).relid, attno);

                    let alias = if pg_sys::bms_is_member(varno as c_int, (*outerrel).relids) {
                        &outer_info.alias
                    } else {
                        &inner_info.alias
                    };

                    let sql_expr = format!("{alias}.{col_name}");

                    target_columns.push(JoinTargetColumn {
                        sql_expr,
                        output_name: col_name.clone(),
                        type_oid,
                    });

                    let resname = PgMemoryContexts::CurrentMemoryContext.pstrdup(&col_name);
                    let tle =
                        pg_sys::makeTargetEntry(var as *mut pg_sys::Expr, resno, resname, false);
                    tlist = pg_sys::lappend(tlist, tle as *mut c_void);
                    resno += 1;
                }
            }
        });

        (tlist, target_columns)
    }
}

// ---------------------------------------------------------------------------
// Join SQL generation
// ---------------------------------------------------------------------------

fn deparse_join_sql(
    outer_info: &JoinRelInfo,
    inner_info: &JoinRelInfo,
    jointype: pg_sys::JoinType::Type,
    target_columns: &[JoinTargetColumn],
    join_conditions: &[String],
) -> String {
    let join_keyword = match jointype {
        pg_sys::JoinType::JOIN_INNER => "INNER JOIN",
        pg_sys::JoinType::JOIN_LEFT => "LEFT OUTER JOIN",
        pg_sys::JoinType::JOIN_RIGHT => "RIGHT OUTER JOIN",
        _ => "INNER JOIN",
    };

    let select_list = target_columns
        .iter()
        .map(|tc| tc.sql_expr.clone())
        .collect::<Vec<_>>()
        .join(", ");

    let on_clause = if join_conditions.is_empty() {
        "1=1".to_string()
    } else {
        join_conditions.join(" AND ")
    };

    format!(
        "SELECT {select_list} FROM {} AS {} {join_keyword} {} AS {} ON {on_clause}",
        outer_info.table_name, outer_info.alias, inner_info.table_name, inner_info.alias,
    )
}

// ---------------------------------------------------------------------------
// Serialization (same pattern as framework's SerdeList)
// ---------------------------------------------------------------------------

unsafe fn serialize_join_state(state: *mut JoinScanState) -> *mut pg_sys::List {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            let mut ret = List::<*mut c_void>::Nil;
            let val = state as i64;
            let cst = pg_sys::makeConst(
                pg_sys::INT8OID,
                -1,
                pg_sys::InvalidOid,
                8,
                val.into_datum().unwrap(),
                false,
                true,
            );
            ret.unstable_push_in_context(cst as *mut c_void, mcx);
            ret.into_ptr()
        })
    }
}

unsafe fn deserialize_join_state(list: *mut pg_sys::List) -> *mut JoinScanState {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(list) = List::<*mut c_void>::downcast_ptr_in_memcx(list, mcx)
                && let Some(cst_ptr) = list.get(0)
            {
                let cst = *(*cst_ptr as *mut pg_sys::Const);
                if let Some(ptr) = i64::from_datum(cst.constvalue, cst.constisnull) {
                    return ptr as *mut JoinScanState;
                }
            }
            ptr::null_mut()
        })
    }
}

// ---------------------------------------------------------------------------
// Overridden callbacks: GetForeignPlan
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_get_foreign_plan(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: Oid,
    best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    unsafe {
        let is_join = (*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_JOINREL;

        if !is_join {
            let orig = ORIGINAL_CALLBACKS.get().unwrap();
            return (orig.get_foreign_plan.unwrap())(
                root,
                baserel,
                foreigntableid,
                best_path,
                tlist,
                scan_clauses,
                outer_plan,
            );
        }

        // --- Join path ---
        let plan_data_ptr = (*baserel).fdw_private as *mut JoinPlanData;
        if plan_data_ptr.is_null() {
            pgrx::error!("SybaseFdw join: missing plan data");
        }
        let plan_data = &*plan_data_ptr;

        let join_state = Box::new(JoinScanState {
            plan_data: plan_data.clone(),
            rows: Vec::new(),
            row_idx: 0,
            values: Vec::new(),
            nulls: Vec::new(),
        });
        let state_ptr = Box::into_raw(join_state);

        let fdw_private = serialize_join_state(state_ptr);
        let fdw_scan_tlist = plan_data.fdw_scan_tlist;
        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        pg_sys::make_foreignscan(
            tlist,
            scan_clauses,
            0, // scanrelid = 0 for joins
            ptr::null_mut(),
            fdw_private as _,
            fdw_scan_tlist,
            ptr::null_mut(),
            outer_plan,
        )
    }
}

// ---------------------------------------------------------------------------
// Overridden callbacks: BeginForeignScan
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_begin_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let scanrelid = (*plan).scan.scanrelid;

        if scanrelid != 0 {
            let orig = ORIGINAL_CALLBACKS.get().unwrap();
            return (orig.begin_foreign_scan.unwrap())(node, eflags);
        }

        // --- Join scan ---
        let state_ptr = deserialize_join_state((*plan).fdw_private as _);
        if state_ptr.is_null() {
            pgrx::error!("SybaseFdw join: failed to deserialize state");
        }

        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int > 0 {
            (*node).fdw_state = state_ptr as *mut c_void;
            return;
        }

        let state = &mut *state_ptr;

        // Execute the join SQL via ODBC
        execute_join_query(state);

        // Initialize values/nulls arrays
        let slot = (*node).ss.ss_ScanTupleSlot;
        let tup_desc = (*slot).tts_tupleDescriptor;
        let natts = (*tup_desc).natts as usize;
        state.values.resize(natts, Datum::from(0));
        state.nulls.resize(natts, true);

        (*node).fdw_state = state_ptr as *mut c_void;
    }
}

fn execute_join_query(state: &mut JoinScanState) {
    let conn = match ODBC_ENV
        .connect_with_connection_string(&state.plan_data.conn_str, ConnectionOptions::default())
    {
        Ok(c) => c,
        Err(e) => {
            pgrx::error!("SybaseFdw join: ODBC connection failed: {e}");
        }
    };

    match conn.execute(&state.plan_data.sql, ()) {
        Ok(Some(mut cursor)) => {
            let num_cols = match cursor.num_result_cols() {
                Ok(n) => n as usize,
                Err(e) => {
                    pgrx::error!("SybaseFdw join: failed to get column count: {e}");
                }
            };

            let batch_size = 1000;
            let mut buffers = match TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096)) {
                Ok(b) => b,
                Err(e) => {
                    pgrx::error!("SybaseFdw join: failed to create buffers: {e}");
                }
            };
            let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
                Ok(c) => c,
                Err(e) => {
                    pgrx::error!("SybaseFdw join: failed to bind buffer: {e}");
                }
            };

            loop {
                match row_set_cursor.fetch() {
                    Ok(Some(batch)) => {
                        for row_idx in 0..batch.num_rows() {
                            let mut values = Vec::with_capacity(num_cols);
                            for col_idx in 0..num_cols {
                                let value = batch
                                    .at(col_idx, row_idx)
                                    .map(|bytes| String::from_utf8_lossy(bytes).to_string());
                                values.push(value);
                            }
                            state.rows.push(values);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        pgrx::error!("SybaseFdw join: fetch error: {e}");
                    }
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            pgrx::error!(
                "SybaseFdw join: query execution failed: {e}\nSQL: {}",
                state.plan_data.sql
            );
        }
    }

    stats::inc_stats("SybaseFdw", stats::Metric::RowsIn, state.rows.len() as i64);
    stats::inc_stats("SybaseFdw", stats::Metric::RowsOut, state.rows.len() as i64);
}

// ---------------------------------------------------------------------------
// Overridden callbacks: IterateForeignScan
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let scanrelid = (*plan).scan.scanrelid;

        if scanrelid != 0 {
            let orig = ORIGINAL_CALLBACKS.get().unwrap();
            return (orig.iterate_foreign_scan.unwrap())(node);
        }

        // --- Join scan ---
        let state = &mut *((*node).fdw_state as *mut JoinScanState);
        let slot = (*node).ss.ss_ScanTupleSlot;

        // Clear the slot
        if let Some(clear) = (*(*slot).tts_ops).clear {
            clear(slot);
        }

        if state.row_idx >= state.rows.len() {
            return slot;
        }

        let row = &state.rows[state.row_idx];

        for (i, tc) in state.plan_data.target_columns.iter().enumerate() {
            let val = row.get(i).and_then(|v| v.as_deref());
            let cell = value_to_cell(val, tc.type_oid).unwrap_or(None);

            let att_idx = i;
            match cell {
                Some(cell) => {
                    state.values[att_idx] = cell.into_datum().unwrap();
                    state.nulls[att_idx] = false;
                }
                None => {
                    state.nulls[att_idx] = true;
                }
            }
        }

        (*slot).tts_values = state.values.as_mut_ptr();
        (*slot).tts_isnull = state.nulls.as_mut_ptr();
        pg_sys::ExecStoreVirtualTuple(slot);

        state.row_idx += 1;
        slot
    }
}

// ---------------------------------------------------------------------------
// Overridden callbacks: ReScanForeignScan
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_re_scan_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let scanrelid = (*plan).scan.scanrelid;

        if scanrelid != 0 {
            let orig = ORIGINAL_CALLBACKS.get().unwrap();
            return (orig.re_scan_foreign_scan.unwrap())(node);
        }

        let state = &mut *((*node).fdw_state as *mut JoinScanState);
        state.row_idx = 0;
    }
}

// ---------------------------------------------------------------------------
// Overridden callbacks: ExplainForeignScan
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_explain_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let scanrelid = (*plan).scan.scanrelid;

        if scanrelid != 0 {
            // Base table scan: delegate to original framework callback
            if let Some(orig_fn) = ORIGINAL_CALLBACKS.get().unwrap().explain_foreign_scan {
                return orig_fn(node, es);
            }
            return;
        }

        // --- Join scan ---
        let state_ptr = (*node).fdw_state as *mut JoinScanState;
        if state_ptr.is_null() {
            return;
        }
        let state = &*state_ptr;
        let label = PgMemoryContexts::CurrentMemoryContext.pstrdup("SybaseFdw Join");
        let value = PgMemoryContexts::CurrentMemoryContext
            .pstrdup(&format!("Remote SQL: {}", state.plan_data.sql));
        pg_sys::ExplainPropertyText(label, value, es);
    }
}

// ---------------------------------------------------------------------------
// Overridden callbacks: EndForeignScan
// ---------------------------------------------------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn sybase_end_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    unsafe {
        let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
        let scanrelid = (*plan).scan.scanrelid;

        if scanrelid != 0 {
            let orig = ORIGINAL_CALLBACKS.get().unwrap();
            return (orig.end_foreign_scan.unwrap())(node);
        }

        let state_ptr = (*node).fdw_state as *mut JoinScanState;
        if !state_ptr.is_null() {
            let _ = Box::from_raw(state_ptr);
            (*node).fdw_state = ptr::null_mut();
        }
    }
}
