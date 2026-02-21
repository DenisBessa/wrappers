use pgrx::FromDatum;
use pgrx::{
    IntoDatum, PgSqlErrorCode, debug2,
    memcxt::PgMemoryContexts,
    pg_sys::{Datum, MemoryContext, MemoryContextData, Oid, ParamKind},
    prelude::*,
};
use std::collections::HashMap;
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
use crate::utils::{self, ReportableError, SerdeList, report_error};

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
    _phantom: PhantomData<E>,
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> FdwState<E, W> {
    unsafe fn new(foreigntableid: Oid, tmp_ctx: MemoryContext) -> Self {
        Self {
            instance: Some(unsafe { instance::create_fdw_instance_from_table_id(foreigntableid) }),
            quals: Vec::new(),
            tgts: Vec::new(),
            sorts: Vec::new(),
            limit: None,
            opts: HashMap::new(),
            tmp_ctx,
            values: Vec::new(),
            nulls: Vec::new(),
            row: Row::new(),
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

// drop the scan state, so the inner fdw instance can be dropped too
unsafe fn drop_fdw_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    fdw_state: *mut FdwState<E, W>,
) {
    let boxed_fdw_state = unsafe { Box::from_raw(fdw_state) };
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

        // create a ForeignPath node and add it as the only possible path
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
            ptr::null_mut(), // no outer rel either
            ptr::null_mut(), // no extra plan
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(), // no restrict info
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(baserel, &mut ((*path).path));

        // Per-scan overhead for parameterized paths. This accounts for the
        // network roundtrip cost of each remote query execution. Without this,
        // the planner always prefers Nested Loop (cost ≈ ppi_rows per iteration)
        // over Hash Join (cost = total_rows), even when Nested Loop means N*M
        // network roundtrips for N outer rows × M lookup tables.
        //
        // The default of 20000 is intentionally high. When the planner
        // underestimates the number of outer rows (a common case with FDWs
        // where statistics are imprecise), it sees:
        //   NL cost  = est_outer_rows × (fdw_startup_cost + ppi_rows)
        //   Hash cost = total_rows (one full scan)
        //
        // With est_outer_rows=1 (typical underestimate) and default=20000:
        // - Tables < 20k rows: 20001 > total_rows → Hash/Merge Join chosen
        //   (one roundtrip, e.g. 2-3s for 14k rows)
        // - Tables > 20k rows: 20001 < total_rows → Nested Loop chosen
        //   (parameterized scan, good when actual outer rows are low)
        //
        // Override per table: ALTER FOREIGN TABLE ... OPTIONS (ADD fdw_startup_cost '500');
        let fdw_startup_cost = state
            .opts
            .get("fdw_startup_cost")
            .map(|c| match c.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    pgrx::error!("invalid option fdw_startup_cost: {}", c);
                }
            })
            .unwrap_or(20000.0);

        // Create parameterized paths for this foreign scan.
        // This allows the planner to use Nested Loop joins that pass
        // join keys as parameters to the inner foreign scan, avoiding
        // full table scans when joining with other tables.
        //
        // Equality join clauses (e.g., a.id = b.id) are absorbed into
        // equivalence classes (root->eq_classes), NOT stored in joininfo.
        // Non-equality join clauses (e.g., a.x < b.y) remain in joininfo.
        // We must check BOTH sources to find all potential parameterizations.
        let mut seen_ppis: Vec<*mut pg_sys::ParamPathInfo> = Vec::new();

        // 1. From equivalence classes (handles equality join conditions)
        let eq_classes = (*root).eq_classes;
        if !eq_classes.is_null() {
            pgrx::memcx::current_context(|mcx| {
                if let Some(ecs) =
                    pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                        eq_classes, mcx,
                    )
                {
                    for ec_ptr in ecs.iter() {
                        let ec = *ec_ptr as *mut pg_sys::EquivalenceClass;
                        if (*ec).ec_has_volatile || (*ec).ec_broken {
                            continue;
                        }

                        // Check if this EC has members for our relation
                        // and members from other relations
                        let mut has_our_member = false;
                        let mut other_member_relids: Vec<pg_sys::Relids> = Vec::new();

                        if let Some(members) =
                            pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                                (*ec).ec_members, mcx,
                            )
                        {
                            for em_ptr in members.iter() {
                                let em = *em_ptr as *mut pg_sys::EquivalenceMember;
                                if (*em).em_is_const {
                                    continue;
                                }
                                // Skip child members (for inheritance/partitioning)
                                if (*em).em_is_child {
                                    continue;
                                }

                                let em_relids = (*em).em_relids;
                                if pg_sys::bms_is_subset(em_relids, (*baserel).relids) {
                                    has_our_member = true;
                                } else if !pg_sys::bms_overlap(em_relids, (*baserel).relids) {
                                    other_member_relids.push(em_relids);
                                }
                            }
                        }

                        if has_our_member {
                            for relids in other_member_relids {
                                let required_outer =
                                    if !(*baserel).lateral_relids.is_null() {
                                        pg_sys::bms_union(relids, (*baserel).lateral_relids)
                                    } else {
                                        relids
                                    };

                                let ppi = pg_sys::get_baserel_parampathinfo(
                                    root,
                                    baserel,
                                    required_outer,
                                );
                                if !ppi.is_null() && !seen_ppis.contains(&ppi) {
                                    seen_ppis.push(ppi);
                                }
                            }
                        }
                    }
                }
            });
        }

        // 2. From joininfo (handles non-equality join conditions)
        let joininfo = (*baserel).joininfo;
        if !joininfo.is_null() {
            pgrx::memcx::current_context(|mcx| {
                if let Some(items) =
                    pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                        joininfo, mcx,
                    )
                {
                    for item in items.iter() {
                        let rinfo = *item as *mut pg_sys::RestrictInfo;

                        if !pg_sys::join_clause_is_movable_to(rinfo, baserel) {
                            continue;
                        }

                        let required_outer = pg_sys::bms_difference(
                            (*rinfo).clause_relids,
                            (*baserel).relids,
                        );
                        let required_outer = if !(*baserel).lateral_relids.is_null() {
                            pg_sys::bms_union(required_outer, (*baserel).lateral_relids)
                        } else {
                            required_outer
                        };

                        if required_outer.is_null() {
                            continue;
                        }

                        let ppi = pg_sys::get_baserel_parampathinfo(
                            root,
                            baserel,
                            required_outer,
                        );
                        if !ppi.is_null() && !seen_ppis.contains(&ppi) {
                            seen_ppis.push(ppi);
                        }
                    }
                }
            });
        }

        // Create a parameterized foreign scan path for each unique parameterization
        for ppi in seen_ppis {
            let param_rows = (*ppi).ppi_rows;
            let param_startup = startup_cost + fdw_startup_cost;
            let param_total = param_startup + param_rows;

            let param_path = pg_sys::create_foreignscan_path(
                root,
                baserel,
                ptr::null_mut(), // default pathtarget
                param_rows,
                #[cfg(feature = "pg18")]
                0, // disabled_nodes
                param_startup,
                param_total,
                ptr::null_mut(),            // no pathkeys
                (*ppi).ppi_req_outer,       // required outer rels
                ptr::null_mut(),            // no extra plan
                #[cfg(any(feature = "pg17", feature = "pg18"))]
                ptr::null_mut(),            // no restrict info
                ptr::null_mut(),            // no fdw_private data
            );
            pg_sys::add_path(baserel, &mut ((*param_path).path));
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

        // For parameterized paths, scan_clauses includes join conditions
        // referencing outer Vars (from other relations). At plan time these
        // are still Var nodes, but PostgreSQL's replace_nestloop_params will
        // convert them to PARAM_EXEC after make_foreignscan returns.
        //
        // Strategy:
        // 1. Extract already-parameterized quals (SubPlan PARAM_EXEC) directly
        // 2. Collect join OpExprs with outer Vars into fdw_exprs
        // 3. After replace_nestloop_params converts them, begin_foreign_scan
        //    will extract Quals from the converted fdw_exprs
        let is_parameterized = !(*best_path).path.param_info.is_null();
        let mut fdw_expr_list: *mut pg_sys::List = ptr::null_mut();

        if is_parameterized {
            pgrx::memcx::current_context(|mcx| {
                if let Some(clauses) =
                    pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                        scan_clauses, mcx,
                    )
                {
                    for item in clauses.iter() {
                        let rinfo = *item as *mut pg_sys::RestrictInfo;
                        if (*rinfo).pseudoconstant {
                            continue;
                        }

                        let expr = (*rinfo).clause as *mut pg_sys::Node;

                        if pgrx::is_a(expr, pg_sys::NodeTag::T_OpExpr) {
                            // Check if this clause involves other relations (join condition).
                            // Join conditions have clause_relids spanning multiple relations.
                            // We handle these via fdw_exprs (replace_nestloop_params converts
                            // outer Vars to PARAM_EXEC). Calling extract_from_op_expr on them
                            // would emit spurious "unsupported operator expression" warnings.
                            let clause_relids = (*rinfo).clause_relids;
                            if !pg_sys::bms_is_subset(clause_relids, (*baserel).relids) {
                                // Join condition: add to fdw_exprs for replace_nestloop_params
                                let opexpr = expr as *mut pg_sys::OpExpr;
                                if let Some(args) =
                                    pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                                        (*opexpr).args, mcx,
                                    )
                                {
                                    if args.len() == 2 {
                                        let left = unnest_clause(*args.get(0).unwrap() as _);
                                        let right = unnest_clause(*args.get(1).unwrap() as _);

                                        let has_our_var = (pgrx::is_a(left, pg_sys::NodeTag::T_Var)
                                            && pg_sys::bms_is_member(
                                                (*(left as *mut pg_sys::Var)).varno as c_int,
                                                (*baserel).relids,
                                            ))
                                            || (pgrx::is_a(right, pg_sys::NodeTag::T_Var)
                                                && pg_sys::bms_is_member(
                                                    (*(right as *mut pg_sys::Var)).varno as c_int,
                                                    (*baserel).relids,
                                                ));

                                        let has_outer_var = (pgrx::is_a(left, pg_sys::NodeTag::T_Var)
                                            && !pg_sys::bms_is_member(
                                                (*(left as *mut pg_sys::Var)).varno as c_int,
                                                (*baserel).relids,
                                            ))
                                            || (pgrx::is_a(right, pg_sys::NodeTag::T_Var)
                                                && !pg_sys::bms_is_member(
                                                    (*(right as *mut pg_sys::Var)).varno as c_int,
                                                    (*baserel).relids,
                                                ));

                                        if has_our_var && has_outer_var {
                                            fdw_expr_list = pg_sys::lappend(
                                                fdw_expr_list,
                                                opexpr as *mut std::ffi::c_void,
                                            );
                                        }
                                    }
                                }
                                continue;
                            }

                            // Local condition: try to extract as Qual (handles SubPlan PARAM_EXEC)
                            if let Some(qual) = extract_from_op_expr(
                                root,
                                foreigntableid,
                                (*baserel).relids,
                                expr as _,
                            ) {
                                if qual.param.is_some() {
                                    state.quals.push(qual);
                                    continue;
                                }
                            }
                        }
                    }
                }
            });
        }

        // make foreign scan plan
        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

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
            scan_clauses,
            (*baserel).relid,
            fdw_expr_list, // outer Var exprs → converted to PARAM_EXEC by replace_nestloop_params
            fdw_private as _,
            ptr::null_mut(),
            ptr::null_mut(),
            outer_plan,
        )
    }
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
    unsafe {
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
                        if let Some(cell) = Cell::from_polymorphic_datum(p.value, p.isnull, p.ptype)
                        {
                            qual.value = Value::Cell(cell);
                        }
                    }
                    ParamKind::PARAM_EXEC => {
                        // evaluate parameter value
                        param.expr_eval.expr_state = pg_sys::ExecInitExpr(
                            param.expr_eval.expr,
                            node as *mut pg_sys::PlanState,
                        );
                        let mut isnull = false;
                        if let Some(datum) = polyfill::exec_eval_expr(
                            param.expr_eval.expr_state,
                            econtext,
                            &mut isnull,
                        ) && let Some(cell) =
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
                    _ => {}
                }
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

        // Extract parameterized quals from fdw_exprs.
        // At plan time (get_foreign_plan), join conditions with outer Vars
        // were placed in fdw_exprs. PostgreSQL's replace_nestloop_params has
        // now converted those outer Vars to PARAM_EXEC nodes. We can extract
        // them as Quals with Param references for remote pushdown.
        let fdw_exprs = (*plan).fdw_exprs;
        if !fdw_exprs.is_null() {
            let scanrelid = (*plan).scan.scanrelid;
            let rel = scan_state.ss_currentRelation;
            let foreigntableid = (*rel).rd_id;
            let baserel_ids = pg_sys::bms_make_singleton(scanrelid as c_int);

            pgrx::memcx::current_context(|mcx| {
                if let Some(exprs) =
                    pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                        fdw_exprs, mcx,
                    )
                {
                    for expr_ptr in exprs.iter() {
                        let expr = *expr_ptr as *mut pg_sys::Node;
                        if pgrx::is_a(expr, pg_sys::NodeTag::T_OpExpr) {
                            if let Some(qual) = extract_from_op_expr(
                                ptr::null_mut(), // root not needed
                                foreigntableid,
                                baserel_ids,
                                expr as *mut pg_sys::OpExpr,
                            ) {
                                state.quals.push(qual);
                            }
                        }
                    }
                }
            });
        }

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
