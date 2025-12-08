use pgrx::FromDatum;
use pgrx::{
    debug2,
    is_a,
    list::List,
    memcxt::PgMemoryContexts,
    pg_sys::{Datum, MemoryContext, MemoryContextData, Oid, ParamKind},
    prelude::*,
    IntoDatum, PgSqlErrorCode,
};
use std::collections::HashMap;
use std::ffi::c_void;
use std::marker::PhantomData;

use pgrx::pg_sys::panic::ErrorReport;
use std::os::raw::c_int;
use std::ptr;

use crate::instance;
use crate::interface::{Cell, Column, Limit, Qual, Row, Sort, Value};
use crate::limit::*;
use crate::memctx;
use crate::options::options_to_hashmap;
use crate::polyfill;
use crate::prelude::ForeignDataWrapper;
use crate::qual::*;
use crate::sort::*;
use crate::utils::{self, report_error, ReportableError, SerdeList};

/// Information about a join clause that can be parameterized
/// This is used to create parameterized paths where the FDW can receive
/// values from outer relations (e.g., in nested loop joins)
#[derive(Debug, Clone)]
pub struct JoinClauseInfo {
    /// The column in the foreign table being compared
    pub field: String,
    /// The operator used in the comparison
    pub operator: String,
    /// The outer relation IDs that provide the parameter value
    pub outer_relids: pg_sys::Relids,
    /// Type OID of the parameter
    pub type_oid: Oid,
}

// Fdw private state for scan
struct FdwState<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> {
    // foreign data wrapper instance
    instance: Option<W>,

    // query conditions
    quals: Vec<Qual>,

    // query target column list
    tgts: Vec<Column>,

    // sort list
    sorts: Vec<Sort>,

    // limit
    limit: Option<Limit>,

    // foreign table options
    opts: HashMap<String, String>,

    // temporary memory context per foreign table, created under Wrappers root
    // memory context
    tmp_ctx: MemoryContext,

    // query result list
    values: Vec<Datum>,
    nulls: Vec<bool>,
    row: Row,
    
    // join clause info for parameterized paths
    join_clauses: Vec<JoinClauseInfo>,
    
    _phantom: PhantomData<E>,
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> FdwState<E, W> {
    unsafe fn new(foreigntableid: Oid, tmp_ctx: MemoryContext) -> Self {
        Self {
            instance: Some(instance::create_fdw_instance_from_table_id(foreigntableid)),
            quals: Vec::new(),
            tgts: Vec::new(),
            sorts: Vec::new(),
            limit: None,
            opts: HashMap::new(),
            tmp_ctx,
            values: Vec::new(),
            nulls: Vec::new(),
            row: Row::new(),
            join_clauses: Vec::new(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn get_rel_size(&mut self) -> Result<(i64, i32), E> {
        if let Some(ref mut instance) = self.instance {
            instance.get_rel_size(
                &self.quals,
                &self.tgts,
                &self.sorts,
                &self.limit,
                &self.opts,
            )
        } else {
            Ok((0, 0))
        }
    }

    #[inline]
    fn begin_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.begin_scan(
                &self.quals,
                &self.tgts,
                &self.sorts,
                &self.limit,
                &self.opts,
            )
        } else {
            Ok(())
        }
    }

    #[inline]
    fn iter_scan(&mut self) -> Result<Option<()>, E> {
        if let Some(ref mut instance) = self.instance {
            instance.iter_scan(&mut self.row)
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn re_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.re_scan()
        } else {
            Ok(())
        }
    }

    #[inline]
    fn end_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.end_scan()
        } else {
            Ok(())
        }
    }
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> utils::SerdeList for FdwState<E, W> {}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> Drop for FdwState<E, W> {
    fn drop(&mut self) {
        // drop foreign data wrapper instance
        self.instance.take();

        // remove the allocated memory context
        unsafe {
            memctx::delete_wrappers_memctx(self.tmp_ctx);
            self.tmp_ctx = ptr::null::<MemoryContextData>() as _;
        }
    }
}

/// Extract join clauses from baserel->joininfo that can be used for parameterized paths
/// Returns a list of JoinClauseInfo for clauses where the foreign table column
/// is compared to a value from an outer relation.
unsafe fn extract_join_clauses(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    baserel_id: pg_sys::Oid,
) -> Vec<JoinClauseInfo> {
    let mut join_clauses = Vec::new();
    
    // Safety check: verify baserel is valid
    if baserel.is_null() {
        return join_clauses;
    }
    
    let baserel_relids = (*baserel).relids;
    let joininfo = (*baserel).joininfo;
    
    // Safety check: joininfo can be null if there are no join conditions
    if joininfo.is_null() {
        return join_clauses;
    }
    
    pgrx::memcx::current_context(|mcx| {
        // Iterate over joininfo - these are RestrictInfo nodes for join conditions
        if let Some(join_list) = List::<*mut c_void>::downcast_ptr_in_memcx(joininfo, mcx) {
            for item in join_list.iter() {
                let rinfo = *item as *mut pg_sys::RestrictInfo;
                if rinfo.is_null() {
                    continue;
                }
                
                let clause = (*rinfo).clause as *mut pg_sys::Node;
                if clause.is_null() {
                    continue;
                }
                
                // Only handle OpExpr (e.g., col = outer_val)
                if !is_a(clause, pg_sys::NodeTag::T_OpExpr) {
                    continue;
                }
                
                let expr = clause as *mut pg_sys::OpExpr;
                
                // Extract join clause info if it's parameterizable
                if let Some(jc) = extract_join_clause_info(
                    baserel_id,
                    baserel_relids,
                    rinfo,
                    expr,
                ) {
                    join_clauses.push(jc);
                }
            }
        }
    });
    
    join_clauses
}

/// Extract join clause info from an OpExpr if it can be parameterized
unsafe fn extract_join_clause_info(
    baserel_id: pg_sys::Oid,
    baserel_relids: pg_sys::Relids,
    rinfo: *mut pg_sys::RestrictInfo,
    expr: *mut pg_sys::OpExpr,
) -> Option<JoinClauseInfo> {
    pgrx::memcx::current_context(|mcx| {
        // Get args list
        let args = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx)?;
        
        // Only handle binary operators
        if args.len() != 2 {
            return None;
        }
        
        // Get operator
        let opno = (*expr).opno;
        let opr = get_operator(opno);
        if opr.is_null() {
            return None;
        }
        
        let mut left = unnest_clause(*args.get(0)? as _);
        let mut right = unnest_clause(*args.get(1)? as _);
        
        // Swap operands if needed to put our table's Var on the left
        if is_a(right, pg_sys::NodeTag::T_Var) && !is_a(left, pg_sys::NodeTag::T_Var) {
            std::mem::swap(&mut left, &mut right);
        }
        
        // Check if left is a Var from our foreign table
        if !is_a(left, pg_sys::NodeTag::T_Var) {
            return None;
        }
        
        let left_var = left as *mut pg_sys::Var;
        
        // Verify it's from our base relation
        if !pg_sys::bms_is_member((*left_var).varno as c_int, baserel_relids) {
            return None;
        }
        
        if (*left_var).varattno < 1 {
            return None;
        }
        
        // Check if right is a Var from an outer relation
        if !is_a(right, pg_sys::NodeTag::T_Var) {
            return None;
        }
        
        let right_var = right as *mut pg_sys::Var;
        
        // The right side should NOT be from our relation (it's from outer)
        if pg_sys::bms_is_member((*right_var).varno as c_int, baserel_relids) {
            return None;
        }
        
        // Get field name
        let field = pg_sys::get_attname(baserel_id, (*left_var).varattno, false);
        if field.is_null() {
            return None;
        }
        let field_str = std::ffi::CStr::from_ptr(field).to_str().ok()?.to_string();
        
        // Get operator name
        let op_name = pgrx::name_data_to_str(&(*opr).oprname).to_string();
        
        // Get required_relids from RestrictInfo - this tells us which outer relations
        // provide the parameter value
        let required_relids = (*rinfo).required_relids;
        if required_relids.is_null() {
            return None;
        }
        
        // Calculate outer relids by removing our own relids
        let outer_relids = pg_sys::bms_difference(required_relids, baserel_relids);
        
        // If no outer relids, this isn't a parameterizable clause
        if outer_relids.is_null() {
            return None;
        }
        
        // Copy bitmap to persistent memory to avoid use-after-free
        // The original bitmap may be in temporary memory that gets freed
        let outer_relids_copy = pg_sys::bms_copy(outer_relids);
        if outer_relids_copy.is_null() {
            return None;
        }
        
        Some(JoinClauseInfo {
            field: field_str,
            operator: op_name,
            outer_relids: outer_relids_copy,
            type_oid: (*right_var).vartype,
        })
    })
}

// drop the scan state, so the inner fdw instance can be dropped too
unsafe fn drop_fdw_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    fdw_state: *mut FdwState<E, W>,
) {
    let boxed_fdw_state = Box::from_raw(fdw_state);
    drop(boxed_fdw_state);
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_rel_size<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_rel_size");
    unsafe {
        // create memory context for scan
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.to_u32());
        let ctx = memctx::create_wrappers_memctx(&ctx_name);

        // create scan state
        let mut state = FdwState::<E, W>::new(foreigntableid, ctx);

        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            // extract qual list
            state.quals = extract_quals(root, baserel, foreigntableid);

            // extract target column list from target and restriction expression
            state.tgts = utils::extract_target_columns(root, baserel);

            // extract sort list
            state.sorts = extract_sorts(root, baserel, foreigntableid);

            // extract limit
            state.limit = extract_limit(root, baserel, foreigntableid);

            // get foreign table options
            let ftable = pg_sys::GetForeignTable(foreigntableid);
            state.opts = options_to_hashmap((*ftable).options).report_unwrap();

            // add additional metadata to the options
            state.opts.insert(
                "wrappers.fserver_oid".into(),
                (*ftable).serverid.to_u32().to_string(),
            );
            state.opts.insert(
                "wrappers.ftable_oid".into(),
                (*ftable).relid.to_u32().to_string(),
            );
            
            // extract join clauses for parameterized paths
            // These are conditions from JOINs that can be pushed down when
            // the outer relation provides parameter values
            state.join_clauses = extract_join_clauses(root, baserel, foreigntableid);
            
            if !state.join_clauses.is_empty() {
                debug2!(
                    "Wrappers: found {} join clauses for parameterized paths",
                    state.join_clauses.len()
                );
            }
        });

        // get estimate row count and mean row width
        let (rows, width) = state.get_rel_size().report_unwrap();
        (*baserel).rows = rows as f64;
        (*(*baserel).reltarget).width = width;

        // save the state for following callbacks
        (*baserel).fdw_private = Box::leak(Box::new(state)) as *mut FdwState<E, W> as _;
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_paths<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_paths");
    unsafe {
        let state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);

        // get startup cost from foreign table options
        let startup_cost = state
            .opts
            .get("startup_cost")
            .map(|c| match c.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    pgrx::error!("invalid option startup_cost: {}", c);
                }
            })
            .unwrap_or(0.0);
        let total_cost = startup_cost + (*baserel).rows;

        // Create the unparameterized (base) path
        // This is the default path that fetches all rows from the foreign table
        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(), // default pathtarget
            (*baserel).rows,
            #[cfg(feature = "pg18")]
            0, // disabled_nodes
            startup_cost,
            total_cost,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no outer rel either (required_outer = NULL)
            ptr::null_mut(), // no extra plan
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(), // no restrict info
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(baserel, &mut ((*path).path));
        
        // Create parameterized paths for join clauses
        // This allows the planner to push down join conditions to the FDW
        // NOTE: Temporarily disabled - without proper runtime value passing via
        // extract_parameterized_quals and build_fdw_exprs, these paths cause
        // the planner to choose inefficient plans that never receive parameter values
        let _enable_parameterized_paths = false;
        for join_clause in &state.join_clauses {
            if !_enable_parameterized_paths {
                break;
            }
            debug2!(
                "Wrappers: processing join clause for field '{}'",
                join_clause.field
            );
            
            // Validation 1: outer_relids must not be null
            if join_clause.outer_relids.is_null() {
                debug2!("Wrappers: outer_relids is null, skipping");
                continue;
            }
            
            // Validation 2: Copy bitmap again to ensure it's in valid memory
            let safe_relids = pg_sys::bms_copy(join_clause.outer_relids);
            if safe_relids.is_null() {
                debug2!("Wrappers: bms_copy returned null, skipping");
                continue;
            }
            
            // Validation 3: Get param_info from PostgreSQL
            debug2!("Wrappers: calling get_baserel_parampathinfo");
            let param_info = pg_sys::get_baserel_parampathinfo(root, baserel, safe_relids);
            if param_info.is_null() {
                debug2!("Wrappers: param_info is null, skipping");
                continue;
            }
            
            // Validation 4: ppi_req_outer must be valid
            let req_outer = (*param_info).ppi_req_outer;
            if req_outer.is_null() {
                debug2!("Wrappers: ppi_req_outer is null, skipping");
                continue;
            }
            
            // Calculate costs for parameterized path
            // Parameterized paths should have lower estimated rows since they filter
            let param_rows = if (*param_info).ppi_rows > 0.0 {
                (*param_info).ppi_rows
            } else {
                ((*baserel).rows * 0.01).max(1.0)
            };
            let param_total_cost = startup_cost + param_rows;
            
            debug2!(
                "Wrappers: creating parameterized path with rows={}, cost={}",
                param_rows,
                param_total_cost
            );
            
            // Create the parameterized path
            let param_path = pg_sys::create_foreignscan_path(
                root,
                baserel,
                ptr::null_mut(), // default pathtarget
                param_rows,
                #[cfg(feature = "pg18")]
                0, // disabled_nodes
                startup_cost,
                param_total_cost,
                ptr::null_mut(), // no pathkeys
                req_outer,       // CRITICAL: use req_outer from param_info
                ptr::null_mut(), // no extra plan
                #[cfg(any(feature = "pg17", feature = "pg18"))]
                ptr::null_mut(), // no restrict info
                ptr::null_mut(), // no fdw_private data
            );
            
            if param_path.is_null() {
                debug2!("Wrappers: create_foreignscan_path returned null");
                continue;
            }
            
            // Add path to baserel
            debug2!("Wrappers: adding parameterized path to baserel");
            pg_sys::add_path(baserel, &mut ((*param_path).path));
            debug2!("Wrappers: parameterized path added successfully");
            
            // Only create one parameterized path for now to minimize risk
            break;
        }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_plan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
    best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug2!("---> get_foreign_plan");
    unsafe {
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);

        // Check if this is a parameterized path
        let is_parameterized = !best_path.is_null() && !(*best_path).path.param_info.is_null();
        
        if is_parameterized {
            debug2!("Wrappers: using parameterized path in get_foreign_plan");
            // NOTE: extract_parameterized_quals and build_fdw_exprs temporarily disabled
            // They cause segfaults during query execution. The paths are being created
            // but the runtime value passing needs more investigation.
        }

        // fdw_exprs disabled for now - causes segfaults
        let fdw_exprs = ptr::null_mut();

        // Extract actual clauses for local evaluation
        let actual_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        // 'serialize' state to list, basically what we're doing here is to store
        // the state pointer as an integer constant in the list, so it can be
        // `deserialized` when executing the plan later.
        // Note that the state itself is not serialized to any memory contexts,
        // it just sits in Rust managed Box'ed memory and will be dropped when
        // end_foreign_scan() is called.
        let fdw_private =
            PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| FdwState::serialize_to_list(state));

        pg_sys::make_foreignscan(
            tlist,
            actual_clauses,
            (*baserel).relid,
            fdw_exprs,
            fdw_private as _,
            ptr::null_mut(),
            ptr::null_mut(),
            outer_plan,
        )
    }
}

/// Extract quals from scan_clauses that have parameters (from parameterized paths)
/// and add them to the existing quals list
#[allow(dead_code)]
unsafe fn extract_parameterized_quals(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    baserel_id: pg_sys::Oid,
    scan_clauses: *mut pg_sys::List,
    quals: &mut Vec<Qual>,
) {
    pgrx::memcx::current_context(|mcx| {
        if let Some(clauses) = List::<*mut c_void>::downcast_ptr_in_memcx(scan_clauses, mcx) {
            for item in clauses.iter() {
                let rinfo = *item as *mut pg_sys::RestrictInfo;
                if rinfo.is_null() {
                    continue;
                }
                
                let clause = (*rinfo).clause as *mut pg_sys::Node;
                if clause.is_null() {
                    continue;
                }
                
                // Check if this clause has parameters (from outer relations)
                // A clause with parameters will have variables from relations not in baserel
                if is_a(clause, pg_sys::NodeTag::T_OpExpr) {
                    if let Some(qual) = extract_parameterized_op_expr(
                        root,
                        baserel_id,
                        (*baserel).relids,
                        clause as *mut pg_sys::OpExpr,
                    ) {
                        // Check if we already have this qual (avoid duplicates)
                        let already_exists = quals.iter().any(|q| {
                            q.field == qual.field && q.operator == qual.operator
                        });
                        
                        if !already_exists {
                            debug2!(
                                "Wrappers: adding parameterized qual: {} {} <param>",
                                qual.field,
                                qual.operator
                            );
                            quals.push(qual);
                        }
                    }
                }
            }
        }
    });
}

/// Extract a parameterized qual from an OpExpr
/// Returns Some(Qual) if the expression has a parameter from an outer relation
#[allow(dead_code)]
unsafe fn extract_parameterized_op_expr(
    _root: *mut pg_sys::PlannerInfo,
    baserel_id: pg_sys::Oid,
    baserel_relids: pg_sys::Relids,
    expr: *mut pg_sys::OpExpr,
) -> Option<Qual> {
    use crate::interface::{ExprEval, Param};
    use std::sync::Mutex;
    
    pgrx::memcx::current_context(|mcx| {
        if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
            if args.len() != 2 {
                return None;
            }
            
            let opno = (*expr).opno;
            let opr = get_operator(opno);
            if opr.is_null() {
                return None;
            }
            
            let mut left = unnest_clause(*args.get(0).unwrap() as _);
            let mut right = unnest_clause(*args.get(1).unwrap() as _);
            
            // Swap if needed to put our Var on the left
            if is_a(right, pg_sys::NodeTag::T_Var) {
                let right_var = right as *mut pg_sys::Var;
                if pg_sys::bms_is_member((*right_var).varno as c_int, baserel_relids) {
                    std::mem::swap(&mut left, &mut right);
                }
            }
            
            // Check if left is our Var and right is from outer relation
            if is_a(left, pg_sys::NodeTag::T_Var) {
                let left_var = left as *mut pg_sys::Var;
                
                if !pg_sys::bms_is_member((*left_var).varno as c_int, baserel_relids)
                    || (*left_var).varattno < 1 
                {
                    return None;
                }
                
                // Check if right is a Var from outer relation
                if is_a(right, pg_sys::NodeTag::T_Var) {
                    let right_var = right as *mut pg_sys::Var;
                    
                    // Right must NOT be from our relation
                    if pg_sys::bms_is_member((*right_var).varno as c_int, baserel_relids) {
                        return None;
                    }
                    
                    let field = pg_sys::get_attname(baserel_id, (*left_var).varattno, false);
                    if field.is_null() {
                        return None;
                    }
                    
                    let field_str = std::ffi::CStr::from_ptr(field).to_str().ok()?.to_string();
                    let op_name = pgrx::name_data_to_str(&(*opr).oprname).to_string();
                    
                    // Create a Param for lazy evaluation
                    // The parameter value will be evaluated during iteration
                    let param = Param {
                        kind: pg_sys::ParamKind::PARAM_EXEC,
                        id: 0, // Will be determined at execution time
                        type_oid: (*right_var).vartype,
                        eval_value: Mutex::new(None).into(),
                        expr_eval: ExprEval {
                            expr: right as *mut pg_sys::Expr,
                            expr_state: ptr::null_mut(),
                        },
                    };
                    
                    return Some(Qual {
                        field: field_str,
                        operator: op_name,
                        value: Value::Cell(Cell::I64(0)), // Placeholder, will be filled at runtime
                        use_or: false,
                        param: Some(param),
                    });
                }
            }
        }
        
        None
    })
}

/// Build list of expressions that need runtime evaluation for parameterized paths
#[allow(dead_code)]
unsafe fn build_fdw_exprs(
    scan_clauses: *mut pg_sys::List,
    baserel_relids: pg_sys::Relids,
) -> *mut pg_sys::List {
    let mut fdw_exprs: *mut pg_sys::List = ptr::null_mut();
    
    pgrx::memcx::current_context(|mcx| {
        if let Some(clauses) = List::<*mut c_void>::downcast_ptr_in_memcx(scan_clauses, mcx) {
            for item in clauses.iter() {
                let rinfo = *item as *mut pg_sys::RestrictInfo;
                if rinfo.is_null() {
                    continue;
                }
                
                let clause = (*rinfo).clause as *mut pg_sys::Node;
                if clause.is_null() {
                    continue;
                }
                
                // Check if this clause references outer relations
                if is_a(clause, pg_sys::NodeTag::T_OpExpr) {
                    let expr = clause as *mut pg_sys::OpExpr;
                    if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                        if args.len() == 2 {
                            let right = unnest_clause(*args.get(1).unwrap() as _);
                            
                            // If right side is a Var from outer relation, add it to fdw_exprs
                            if is_a(right, pg_sys::NodeTag::T_Var) {
                                let right_var = right as *mut pg_sys::Var;
                                if !pg_sys::bms_is_member((*right_var).varno as c_int, baserel_relids) {
                                    fdw_exprs = pg_sys::lappend(fdw_exprs, right as *mut c_void);
                                }
                            }
                        }
                    }
                }
            }
        }
    });
    
    fdw_exprs
}

#[pg_guard]
pub(super) extern "C-unwind" fn explain_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    debug2!("---> explain_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if fdw_state.is_null() {
            return;
        }

        let state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);

        let ctx = PgMemoryContexts::For(state.tmp_ctx);

        let label = ctx.pstrdup("Wrappers");

        let value = ctx.pstrdup(&format!("quals = {:?}", state.quals));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("tgts = {:?}", state.tgts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("sorts = {:?}", state.sorts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("limit = {:?}", state.limit));
        pg_sys::ExplainPropertyText(label, value, es);
    }
}

// extract paramter value and assign it to qual in scan state
unsafe fn assign_paramenter_value<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
    state: &mut FdwState<E, W>,
) {
    let estate = (*node).ss.ps.state;
    let econtext = (*node).ss.ps.ps_ExprContext;

    // assign parameter value to qual
    for qual in &mut state.quals.iter_mut() {
        if let Some(param) = &mut qual.param {
            match param.kind {
                ParamKind::PARAM_EXTERN => {
                    // get parameter list in execution state
                    let plist_info = (*estate).es_param_list_info;
                    if plist_info.is_null() {
                        continue;
                    }
                    let params_cnt = (*plist_info).numParams as usize;
                    let plist = (*plist_info).params.as_slice(params_cnt);
                    let p: pg_sys::ParamExternData = plist[param.id - 1];
                    if let Some(cell) = Cell::from_polymorphic_datum(p.value, p.isnull, p.ptype) {
                        qual.value = Value::Cell(cell);
                    }
                }
                ParamKind::PARAM_EXEC => {
                    // evaluate parameter value
                    param.expr_eval.expr_state =
                        pg_sys::ExecInitExpr(param.expr_eval.expr, node as *mut pg_sys::PlanState);
                    let mut isnull = false;
                    if let Some(datum) =
                        polyfill::exec_eval_expr(param.expr_eval.expr_state, econtext, &mut isnull)
                    {
                        if let Some(cell) =
                            Cell::from_polymorphic_datum(datum, isnull, param.type_oid)
                        {
                            let mut eval_value = param
                                .eval_value
                                .lock()
                                .expect("param.eval_value should be locked");
                            *eval_value = Some(Value::Cell(cell.clone()));
                            qual.value = Value::Cell(cell);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn begin_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    debug2!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let mut state = FdwState::<E, W>::deserialize_from_list((*plan).fdw_private as _);
        assert!(!state.is_null());

        // assign parameter values to qual
        assign_paramenter_value(node, &mut state);

        // begin scan if it is not EXPLAIN statement
        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int <= 0 {
            let result = state.begin_scan();
            if result.is_err() {
                drop_fdw_state(state.as_ptr());
                (*plan).fdw_private = ptr::null::<FdwState<E, W>>() as _;
                result.report_unwrap();
            }

            let rel = scan_state.ss_currentRelation;
            let tup_desc = (*rel).rd_att;
            let natts = (*tup_desc).natts as usize;

            // initialize scan result lists
            state
                .values
                .extend_from_slice(&vec![0.into_datum().unwrap(); natts]);
            state.nulls.extend_from_slice(&vec![true; natts]);
        }

        (*node).fdw_state = state.into_pg() as _;
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn iterate_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    // `debug!` macros are quite expensive at the moment, so avoid logging in the inner loop
    // debug2!("---> iterate_foreign_scan");
    unsafe {
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*node).fdw_state as _);

        // evaluate parameter values
        assign_paramenter_value(node, &mut state);

        // clear slot
        let slot = (*node).ss.ss_ScanTupleSlot;
        polyfill::exec_clear_tuple(slot);

        state.row.clear();

        let result = state.iter_scan();
        if result.is_err() {
            drop_fdw_state(state.as_ptr());
            (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;
        }
        if result.report_unwrap().is_some() {
            if state.row.cols.len() != state.tgts.len() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                    "target column number not match",
                );
                return slot;
            }

            PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
                for i in 0..state.row.cells.len() {
                    let att_idx = state.tgts[i].num - 1;
                    let cell = state.row.cells.get_unchecked_mut(i);
                    match cell.take() {
                        Some(cell) => {
                            state.values[att_idx] = cell.into_datum().unwrap();
                            state.nulls[att_idx] = false;
                        }
                        None => state.nulls[att_idx] = true,
                    }
                }

                (*slot).tts_values = state.values.as_mut_ptr();
                (*slot).tts_isnull = state.nulls.as_mut_ptr();
                pg_sys::ExecStoreVirtualTuple(slot);
            });
        }

        slot
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn re_scan_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> re_scan_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if !fdw_state.is_null() {
            let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
            let result = state.re_scan();
            if result.is_err() {
                drop_fdw_state(state.as_ptr());
                (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;
                result.report_unwrap();
            }
        }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn end_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> end_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if fdw_state.is_null() {
            return;
        }

        // the scan state is actually not allocated by PG, but we use 'from_pg()'
        // here just to tell PgBox don't free the state, instead we will handle
        // drop the state by ourselves
        let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
        let result = state.end_scan();
        drop_fdw_state(state.as_ptr());
        (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;

        result.report_unwrap();
    }
}
