use crate::interface::Limit;
use pgrx::{is_a, pg_sys, FromDatum};

// Count the number of base relations in the query
// This helps determine if the query involves JOINs
unsafe fn count_base_relations(root: *mut pg_sys::PlannerInfo) -> usize {
    let mut count = 0;

    // simple_rel_array contains RelOptInfo for each base relation
    // Index 0 is unused, so we start from 1
    let array_size = (*root).simple_rel_array_size as usize;

    if array_size <= 1 {
        return 0;
    }

    let rel_array = (*root).simple_rel_array;
    if rel_array.is_null() {
        return 0;
    }

    // Count non-null entries that represent base relations
    for i in 1..array_size {
        let rel = *rel_array.add(i);
        if !rel.is_null() {
            // Check if this is a base relation (not a join relation)
            // RELOPT_BASEREL = 0
            if (*rel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL {
                count += 1;
            }
        }
    }

    count
}

// extract limit
pub(crate) unsafe fn extract_limit(
    root: *mut pg_sys::PlannerInfo,
    _baserel: *mut pg_sys::RelOptInfo,
    _baserel_id: pg_sys::Oid,
) -> Option<Limit> {
    let parse = (*root).parse;

    // don't push down LIMIT if the query has a GROUP BY clause or aggregates
    if !(*parse).groupClause.is_null() || (*parse).hasAggs {
        return None;
    }

    // don't push down LIMIT if the query involves JOINs (more than one base relation)
    // because the LIMIT applies to the final result after the JOIN, not to individual tables
    if count_base_relations(root) > 1 {
        return None;
    }

    // only push down constant LIMITs that are not NULL
    let limit_count = (*parse).limitCount as *mut pg_sys::Const;
    if limit_count.is_null() || !is_a(limit_count as *mut pg_sys::Node, pg_sys::NodeTag::T_Const) {
        return None;
    }

    let mut limit = Limit::default();

    if let Some(count) = i64::from_polymorphic_datum(
        (*limit_count).constvalue,
        (*limit_count).constisnull,
        (*limit_count).consttype,
    ) {
        limit.count = count;
    } else {
        return None;
    }

    // only consider OFFSETS that are non-NULL constants
    let limit_offset = (*parse).limitOffset as *mut pg_sys::Const;
    if !limit_offset.is_null() && is_a(limit_offset as *mut pg_sys::Node, pg_sys::NodeTag::T_Const)
    {
        if let Some(offset) = i64::from_polymorphic_datum(
            (*limit_offset).constvalue,
            (*limit_offset).constisnull,
            (*limit_offset).consttype,
        ) {
            limit.offset = offset;
        }
    }

    Some(limit)
}
