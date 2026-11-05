use crate::prelude::*;
use pgrx::pg_sys::Oid;
use pgrx::{
    AnyNumeric, FromDatum, PgBuiltInOids, PgOid,
    datum::{Array, Date, JsonB, Timestamp},
    is_a,
    list::List,
    pg_sys,
    pg_sys::Datum,
};
use std::ffi::CStr;
use std::ffi::c_void;
use std::os::raw::c_int;
use std::ptr;
use std::sync::Mutex;

use crate::interface::Param;

/// Parses a Postgres array `Datum` (given its element/array type OID) into a `Vec<Cell>`.
///
/// Returns `None` if `is_null` is set or if `typoid` isn't one of the supported array types.
/// NULL array elements are skipped, so the resulting `Vec` does not preserve positional
/// correspondence with the source array.
///
/// # Safety
///
/// `datum` must be a valid array `Datum` of the Postgres type identified by `typoid`, or
/// `is_null` must be `true`.
pub unsafe fn form_array_from_datum(
    datum: Datum,
    is_null: bool,
    typoid: pg_sys::Oid,
) -> Option<Vec<Cell>> {
    let oid = PgOid::from(typoid);
    match oid {
        PgOid::BuiltIn(PgBuiltInOids::BOOLARRAYOID) => {
            unsafe { Array::<bool>::from_polymorphic_datum(datum, is_null, pg_sys::BOOLOID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::Bool(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::CHARARRAYOID) => {
            unsafe { Array::<i8>::from_polymorphic_datum(datum, is_null, pg_sys::CHAROID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I8(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::INT2ARRAYOID) => {
            unsafe { Array::<i16>::from_polymorphic_datum(datum, is_null, pg_sys::INT2OID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I16(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4ARRAYOID) => {
            unsafe { Array::<f32>::from_polymorphic_datum(datum, is_null, pg_sys::FLOAT4OID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::F32(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID) => {
            unsafe { Array::<i32>::from_polymorphic_datum(datum, is_null, pg_sys::INT4OID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I32(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8ARRAYOID) => {
            unsafe { Array::<f64>::from_polymorphic_datum(datum, is_null, pg_sys::FLOAT8OID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::F64(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID) => {
            unsafe { Array::<i64>::from_polymorphic_datum(datum, is_null, pg_sys::INT8OID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::I64(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        // SYBASE FORK: `IN (...)` on CHAR/VARCHAR columns (varlena like text).
        PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID)
        | PgOid::BuiltIn(PgBuiltInOids::BPCHARARRAYOID)
        | PgOid::BuiltIn(PgBuiltInOids::VARCHARARRAYOID) => {
            unsafe { Array::<String>::from_polymorphic_datum(datum, is_null, pg_sys::TEXTOID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::String(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        // SYBASE FORK: `IN (...)` on NUMERIC columns (valor, aliquota).
        PgOid::BuiltIn(PgBuiltInOids::NUMERICARRAYOID) => unsafe {
            Array::<AnyNumeric>::from_polymorphic_datum(datum, is_null, pg_sys::NUMERICOID)
        }
        .map(|arr| {
            arr.iter()
                .filter(|v| v.is_some())
                .map(|v| Cell::Numeric(v.expect("non-null array element")))
                .collect::<Vec<_>>()
        }),
        PgOid::BuiltIn(PgBuiltInOids::DATEARRAYOID) => {
            unsafe { Array::<Date>::from_polymorphic_datum(datum, is_null, pg_sys::DATEOID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::Date(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPARRAYOID) => unsafe {
            Array::<Timestamp>::from_polymorphic_datum(datum, is_null, pg_sys::TIMESTAMPOID)
        }
        .map(|arr| {
            arr.iter()
                .filter(|v| v.is_some())
                .map(|v| Cell::Timestamp(v.expect("non-null array element")))
                .collect::<Vec<_>>()
        }),
        PgOid::BuiltIn(PgBuiltInOids::JSONBARRAYOID) => {
            unsafe { Array::<JsonB>::from_polymorphic_datum(datum, is_null, pg_sys::JSONBOID) }.map(
                |arr| {
                    arr.iter()
                        .filter(|v| v.is_some())
                        .map(|v| Cell::Json(v.expect("non-null array element")))
                        .collect::<Vec<_>>()
                },
            )
        }
        _ => None,
    }
}

// SYBASE FORK BEGIN: KOROK-224
/// Converts an evaluated parameter into a qual value: arrays (`= ANY($1)`,
/// `IN ($1, $2)`) become `Value::Array`, a NULL array an empty one.
pub(crate) unsafe fn value_from_datum(
    datum: Datum,
    is_null: bool,
    typoid: pg_sys::Oid,
) -> Option<Value> {
    unsafe {
        if pg_sys::get_element_type(typoid) != Oid::INVALID {
            if is_null {
                return Some(Value::Array(Vec::new()));
            }
            return form_array_from_datum(datum, is_null, typoid).map(Value::Array);
        }
        Cell::from_polymorphic_datum(datum, is_null, typoid).map(Value::Cell)
    }
}

/// Array types `form_array_from_datum` can read.
fn is_supported_array_type(typoid: pg_sys::Oid) -> bool {
    matches!(
        PgOid::from(typoid),
        PgOid::BuiltIn(
            PgBuiltInOids::BOOLARRAYOID
                | PgBuiltInOids::CHARARRAYOID
                | PgBuiltInOids::INT2ARRAYOID
                | PgBuiltInOids::FLOAT4ARRAYOID
                | PgBuiltInOids::INT4ARRAYOID
                | PgBuiltInOids::FLOAT8ARRAYOID
                | PgBuiltInOids::INT8ARRAYOID
                | PgBuiltInOids::TEXTARRAYOID
                | PgBuiltInOids::BPCHARARRAYOID
                | PgBuiltInOids::VARCHARARRAYOID
                | PgBuiltInOids::NUMERICARRAYOID
                | PgBuiltInOids::DATEARRAYOID
                | PgBuiltInOids::TIMESTAMPARRAYOID
                | PgBuiltInOids::JSONBARRAYOID
        )
    )
}
// SYBASE FORK END

// SYBASE FORK BEGIN: KOROK-213
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn exec_param_walker(node: *mut pg_sys::Node, ctx: *mut c_void) -> bool {
    unsafe {
        if node.is_null() {
            return false;
        }
        if is_a(node, pg_sys::NodeTag::T_Param)
            && (*(node as *mut pg_sys::Param)).paramkind == pg_sys::ParamKind::PARAM_EXEC
        {
            return true;
        }
        pg_sys::expression_tree_walker(node, Some(exec_param_walker), ctx)
    }
}

/// True when `expr` reads a PARAM_EXEC Param: a value from the outer row
/// (correlated subquery, LATERAL, parameterized NestLoop) or from an InitPlan.
/// It is only set right before the node runs; in BeginForeignScan the slot is
/// still zeroed and not null, so `substr(<param>, …)` dereferences NULL.
pub(crate) unsafe fn uses_exec_param(expr: *mut pg_sys::Node) -> bool {
    unsafe { exec_param_walker(expr, ptr::null_mut()) }
}
// SYBASE FORK END

pub(crate) unsafe fn get_operator(opno: pg_sys::Oid) -> pg_sys::Form_pg_operator {
    unsafe {
        let htup = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::OPEROID.try_into().unwrap(),
            opno.into(),
        );
        if htup.is_null() {
            pg_sys::ReleaseSysCache(htup);
            pgrx::error!("cache lookup operator {:?} failed", opno);
        }
        let op = pg_sys::GETSTRUCT(htup) as pg_sys::Form_pg_operator;
        pg_sys::ReleaseSysCache(htup);
        op
    }
}

pub(crate) unsafe fn unnest_clause(node: *mut pg_sys::Node) -> *mut pg_sys::Node {
    unsafe {
        if is_a(node, pg_sys::NodeTag::T_RelabelType) {
            (*(node as *mut pg_sys::RelabelType)).arg as _
        } else if is_a(node, pg_sys::NodeTag::T_ArrayCoerceExpr) {
            (*(node as *mut pg_sys::ArrayCoerceExpr)).arg as _
        } else {
            node
        }
    }
}

pub(crate) unsafe fn extract_from_op_expr(
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::OpExpr,
) -> Option<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                // only deal with binary operator
                if args.len() != 2 {
                    report_warning("only support binary operator expression");
                    return None;
                }

                // get operator
                let opno = (*expr).opno;
                let opr = get_operator(opno);
                if opr.is_null() {
                    report_warning("operator is empty");
                    return None;
                }

                let mut left = unnest_clause(*args.get(0).unwrap() as _);
                let mut right = unnest_clause(*args.get(1).unwrap() as _);

                // swap operands if needed
                if is_a(right, pg_sys::NodeTag::T_Var)
                    && !is_a(left, pg_sys::NodeTag::T_Var)
                    && (*opr).oprcom != Oid::INVALID
                {
                    std::mem::swap(&mut left, &mut right);
                }

                if is_a(left, pg_sys::NodeTag::T_Var) {
                    let left = left as *mut pg_sys::Var;

                    if pg_sys::bms_is_member((*left).varno as c_int, baserel_ids)
                        && (*left).varattno >= 1
                    {
                        let field = pg_sys::get_attname(baserel_id, (*left).varattno, false);

                        let (value, param, value_const) = if is_a(right, pg_sys::NodeTag::T_Const) {
                            let right = right as *mut pg_sys::Const;
                            (
                                Cell::from_polymorphic_datum(
                                    (*right).constvalue,
                                    (*right).constisnull,
                                    (*right).consttype,
                                ),
                                None,
                                Some(right as usize),
                            )
                        } else if is_a(right, pg_sys::NodeTag::T_Param) {
                            // add a dummy value if this is query parameter, the actual value
                            // will be extracted from execution state
                            let right = right as *mut pg_sys::Param;
                            let param = Param {
                                kind: (*right).paramkind,
                                id: (*right).paramid as _,
                                type_oid: (*right).paramtype,
                                eval_value: Mutex::new(None).into(),
                                eval_null: Default::default(),
                                expr_eval: ExprEval {
                                    expr: if (*right).paramkind == pg_sys::ParamKind::PARAM_EXEC {
                                        right as _
                                    } else {
                                        ptr::null_mut()
                                    },
                                    expr_state: ptr::null_mut(),
                                },
                            };
                            (Some(Cell::I64(0)), Some(param), None)
                        // SYBASE FORK BEGIN: pushdown of evaluable expressions
                        // without Var (NOW(), CURRENT_DATE, arithmetic on constants)
                        // via PARAM_EXEC. Required by sybase_now_pushdown test and
                        // production queries with `data_inicio < NOW()`. Not present
                        // in upstream supabase/wrappers.
                        } else if !pg_sys::contain_var_clause(right) {
                            // Handle evaluable expressions without variable references
                            // (e.g., NOW(), CURRENT_DATE, arithmetic on constants).
                            // They are evaluated at execution time via the PARAM_EXEC
                            // path; `FdwScanPrivate` serializes the expression node so
                            // it survives the plan-cache `copyObject`.
                            let type_oid = pg_sys::exprType(right);
                            let param = Param {
                                kind: pg_sys::ParamKind::PARAM_EXEC,
                                id: 0,
                                type_oid,
                                eval_value: Mutex::new(None).into(),
                                eval_null: Default::default(),
                                expr_eval: ExprEval {
                                    expr: right as _,
                                    expr_state: ptr::null_mut(),
                                },
                            };
                            (Some(Cell::I64(0)), Some(param), None)
                        // SYBASE FORK END
                        } else {
                            (None, None, None)
                        };

                        if let Some(value) = value {
                            let qual = Qual {
                                field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                                operator: pgrx::name_data_to_str(&(*opr).oprname).to_string(),
                                value: Value::Cell(value),
                                use_or: false,
                                param,
                                value_const,
                            };
                            return Some(qual);
                        }
                    }
                }

                if let Some(stm) = pgrx::nodes::node_to_string(expr as _) {
                    report_warning(format!("unsupported operator expression in qual: {stm}",));
                }
            }

            None
        })
    }
}

pub(crate) unsafe fn extract_from_null_test(
    baserel_id: pg_sys::Oid,
    expr: *mut pg_sys::NullTest,
) -> Option<Qual> {
    unsafe {
        let var = (*expr).arg as *mut pg_sys::Var;
        if !is_a(var as _, pg_sys::NodeTag::T_Var) || (*var).varattno < 1 {
            return None;
        }

        let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

        let opname = if (*expr).nulltesttype == pg_sys::NullTestType::IS_NULL {
            "is".to_string()
        } else {
            "is not".to_string()
        };

        let qual = Qual {
            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
            operator: opname,
            value: Value::Cell(Cell::String("null".to_string())),
            use_or: false,
            param: None,
            value_const: None,
        };

        Some(qual)
    }
}

pub(crate) unsafe fn extract_from_scalar_array_op_expr(
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::ScalarArrayOpExpr,
) -> Option<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                // only deal with binary operator
                if args.len() != 2 {
                    return None;
                }

                // get operator
                let opno = (*expr).opno;
                let opr = get_operator(opno);
                if opr.is_null() {
                    return None;
                }

                let left = unnest_clause(*args.get(0).unwrap() as _);
                let right = unnest_clause(*args.get(1).unwrap() as _);

                if is_a(left, pg_sys::NodeTag::T_Var) && is_a(right, pg_sys::NodeTag::T_Const) {
                    let left = left as *mut pg_sys::Var;
                    let right = right as *mut pg_sys::Const;

                    if pg_sys::bms_is_member((*left).varno as c_int, baserel_ids)
                        && (*left).varattno >= 1
                    {
                        let field = pg_sys::get_attname(baserel_id, (*left).varattno, false);

                        let value: Option<Vec<Cell>> = form_array_from_datum(
                            (*right).constvalue,
                            (*right).constisnull,
                            (*right).consttype,
                        );
                        if let Some(value) = value {
                            let qual = Qual {
                                field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                                operator: pgrx::name_data_to_str(&(*opr).oprname).to_string(),
                                value: Value::Array(value),
                                use_or: (*expr).useOr,
                                param: None,
                                value_const: Some(right as usize),
                            };
                            return Some(qual);
                        }
                    }
                }

                // SYBASE FORK BEGIN: KOROK-224. Array known only at execution:
                // `IN ($1, $2)` and `= ANY($1)` in a generic plan, or
                // `= ANY(array(subquery))` (InitPlan). Evaluated in the scan like
                // the scalar Param in extract_from_op_expr.
                if is_a(left, pg_sys::NodeTag::T_Var)
                    && !pg_sys::contain_var_clause(right)
                    && !pg_sys::contain_volatile_functions(right)
                    && is_supported_array_type(pg_sys::exprType(right))
                {
                    let left = left as *mut pg_sys::Var;
                    if pg_sys::bms_is_member((*left).varno as c_int, baserel_ids)
                        && (*left).varattno >= 1
                    {
                        let field = pg_sys::get_attname(baserel_id, (*left).varattno, false);
                        let type_oid = pg_sys::exprType(right);
                        // A bare Param keeps kind/id (rebuilt in from_scan_private);
                        // any other expression is shipped whole as PARAM_EXEC id 0.
                        let (kind, id, param_expr) = if is_a(right, pg_sys::NodeTag::T_Param) {
                            let p = right as *mut pg_sys::Param;
                            let expr: *mut pg_sys::Expr =
                                if (*p).paramkind == pg_sys::ParamKind::PARAM_EXEC {
                                    right as _
                                } else {
                                    ptr::null_mut()
                                };
                            ((*p).paramkind, (*p).paramid as usize, expr)
                        } else {
                            (pg_sys::ParamKind::PARAM_EXEC, 0, right as _)
                        };
                        return Some(Qual {
                            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                            operator: pgrx::name_data_to_str(&(*opr).oprname).to_string(),
                            value: Value::Array(Vec::new()),
                            use_or: (*expr).useOr,
                            param: Some(Param {
                                kind,
                                id,
                                type_oid,
                                eval_value: Mutex::new(None).into(),
                                eval_null: Default::default(),
                                expr_eval: ExprEval {
                                    expr: param_expr,
                                    expr_state: ptr::null_mut(),
                                },
                            }),
                            value_const: None,
                        });
                    }
                }
                // SYBASE FORK END

                if let Some(stm) = pgrx::nodes::node_to_string(expr as _) {
                    report_warning(format!("only support const scalar array in qual: {stm}",));
                }
            }

            None
        })
    }
}

pub(crate) unsafe fn extract_from_var(
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    var: *mut pg_sys::Var,
) -> Option<Qual> {
    unsafe {
        if (*var).varattno < 1
            || (*var).vartype != pg_sys::BOOLOID
            || !pg_sys::bms_is_member((*var).varno as c_int, baserel_ids)
        {
            return None;
        }

        let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

        let qual = Qual {
            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
            operator: "=".to_string(),
            value: Value::Cell(Cell::Bool(true)),
            use_or: false,
            param: None,
            value_const: None,
        };

        Some(qual)
    }
}

pub(crate) unsafe fn extract_from_bool_expr(
    baserel_id: pg_sys::Oid,
    baserel_ids: pg_sys::Relids,
    expr: *mut pg_sys::BoolExpr,
) -> Option<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            if let Some(args) = List::<*mut c_void>::downcast_ptr_in_memcx((*expr).args, mcx) {
                if (*expr).boolop != pg_sys::BoolExprType::NOT_EXPR || args.len() != 1 {
                    return None;
                }

                let var = *args.get(0).unwrap() as *mut pg_sys::Var;
                if (*var).varattno < 1
                    || (*var).vartype != pg_sys::BOOLOID
                    || !pg_sys::bms_is_member((*var).varno as c_int, baserel_ids)
                {
                    return None;
                }

                let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

                let qual = Qual {
                    field: CStr::from_ptr(field).to_str().unwrap().to_string(),
                    operator: "=".to_string(),
                    value: Value::Cell(Cell::Bool(false)),
                    use_or: false,
                    param: None,
                    value_const: None,
                };

                return Some(qual);
            }

            None
        })
    }
}

pub(crate) unsafe fn extract_from_boolean_test(
    baserel_id: pg_sys::Oid,
    expr: *mut pg_sys::BooleanTest,
) -> Option<Qual> {
    unsafe {
        let var = (*expr).arg as *mut pg_sys::Var;
        if !is_a(var as _, pg_sys::NodeTag::T_Var) || (*var).varattno < 1 {
            return None;
        }

        let field = pg_sys::get_attname(baserel_id, (*var).varattno, false);

        let (opname, value) = match (*expr).booltesttype {
            pg_sys::BoolTestType::IS_TRUE => ("is".to_string(), true),
            pg_sys::BoolTestType::IS_FALSE => ("is".to_string(), false),
            pg_sys::BoolTestType::IS_NOT_TRUE => ("is not".to_string(), true),
            pg_sys::BoolTestType::IS_NOT_FALSE => ("is not".to_string(), false),
            _ => return None,
        };

        let qual = Qual {
            field: CStr::from_ptr(field).to_str().unwrap().to_string(),
            operator: opname,
            value: Value::Cell(Cell::Bool(value)),
            use_or: false,
            param: None,
            value_const: None,
        };

        Some(qual)
    }
}

pub(crate) unsafe fn extract_quals(
    baserel: *mut pg_sys::RelOptInfo,
    baserel_id: pg_sys::Oid,
) -> Vec<Qual> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            let mut quals = Vec::new();

            if let Some(conds) =
                List::<*mut c_void>::downcast_ptr_in_memcx((*baserel).baserestrictinfo, mcx)
            {
                for cond in conds.iter() {
                    // SYBASE FORK (KOROK-224): a qual with no column of this
                    // table (`$outer = 4`) gates the whole scan in a Result
                    // node; there is nothing to push and nothing to warn about.
                    if (*(*cond as *mut pg_sys::RestrictInfo)).pseudoconstant {
                        continue;
                    }
                    let expr = (*(*cond as *mut pg_sys::RestrictInfo)).clause as *mut pg_sys::Node;
                    let extracted = if is_a(expr, pg_sys::NodeTag::T_OpExpr) {
                        extract_from_op_expr(baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_NullTest) {
                        extract_from_null_test(baserel_id, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_ScalarArrayOpExpr) {
                        extract_from_scalar_array_op_expr(baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_Var) {
                        extract_from_var(baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_BoolExpr) {
                        extract_from_bool_expr(baserel_id, (*baserel).relids, expr as _)
                    } else if is_a(expr, pg_sys::NodeTag::T_BooleanTest) {
                        extract_from_boolean_test(baserel_id, expr as _)
                    } else {
                        if let Some(stm) = pgrx::nodes::node_to_string(expr) {
                            report_warning(format!("unsupported qual: {stm}",));
                        }
                        None
                    };

                    if let Some(qual) = extracted {
                        quals.push(qual);
                    }
                }
            }

            quals
        })
    }
}
