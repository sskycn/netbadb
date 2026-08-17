//! Physical planning kept separate from logical relational meaning.

use std::cmp::Ordering;

use netbadb_index::{
    BTreeHandle, IndexBound, IndexRange, IndexStatistics, TableStatistics, compare_values,
};
use netbadb_rel::{
    AggregateOutput, Assignment, BinaryOp, ColumnRef, Expr, ExprKind, JoinKind, LogicalPlan,
    LogicalStatement, OutputField, SortKey,
};
use netbadb_types::{ColumnId, RelationBindingId, ScalarValue, TableId};

/// One registered single-column lookup capability available to physical planning.
///
/// Callers preserve registration order in the slice. The planner receives
/// immutable domain snapshots, never storage objects or catalog pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAccessPath {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub handle: BTreeHandle,
    pub statistics: Option<IndexStatistics>,
}

/// Optional optimizer snapshot for one table visible to physical planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAccessStatistics {
    pub table_id: TableId,
    pub statistics: Option<TableStatistics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    SeqScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    IndexScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        index_column: ColumnRef,
        handle: BTreeHandle,
        key: ScalarValue,
    },
    RangeIndexScan {
        binding_id: RelationBindingId,
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
        index_column: ColumnRef,
        handle: BTreeHandle,
        range: IndexRange,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        predicate: Expr,
        columns: Vec<ColumnRef>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        kind: JoinKind,
        left_key: ColumnRef,
        right_key: ColumnRef,
        predicate: Expr,
        columns: Vec<ColumnRef>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<SortKey>,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ColumnRef>,
    },
    Aggregate {
        input: Box<PhysicalPlan>,
        group_keys: Vec<ColumnRef>,
        outputs: Vec<AggregateOutput>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalStatement {
    Query(PhysicalPlan),
    Insert {
        table_id: TableId,
        table_name: String,
        values: Vec<Expr>,
    },
    Update {
        input: PhysicalPlan,
        table_id: TableId,
        assignments: Vec<Assignment>,
    },
    Delete {
        input: PhysicalPlan,
        table_id: TableId,
    },
}

impl PhysicalPlan {
    #[must_use]
    pub fn output_fields(&self) -> Vec<OutputField> {
        match self {
            Self::SeqScan { columns, .. }
            | Self::IndexScan { columns, .. }
            | Self::RangeIndexScan { columns, .. }
            | Self::NestedLoopJoin { columns, .. }
            | Self::HashJoin { columns, .. }
            | Self::Project { columns, .. } => {
                columns.iter().cloned().map(OutputField::Source).collect()
            }
            Self::Aggregate { outputs, .. } => {
                outputs.iter().map(AggregateOutput::output_field).collect()
            }
            Self::Filter { input, .. } | Self::Sort { input, .. } | Self::Limit { input, .. } => {
                input.output_fields()
            }
        }
    }
}

#[must_use]
pub fn plan(logical: &netbadb_rel::LogicalPlan) -> PhysicalPlan {
    plan_with_access_paths(logical, &[])
}

/// Selects physical operators from logical meaning and an ordered snapshot of
/// registered single-column access paths.
#[must_use]
pub fn plan_with_access_paths(
    logical: &LogicalPlan,
    access_paths: &[IndexAccessPath],
) -> PhysicalPlan {
    plan_with_statistics(logical, &[], access_paths)
}

/// Selects physical operators using ordered access paths and optional explicit
/// `ANALYZE` snapshots. Missing table statistics preserve the Phase 4E rule.
#[must_use]
pub fn plan_with_statistics(
    logical: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[IndexAccessPath],
) -> PhysicalPlan {
    match logical {
        LogicalPlan::Scan {
            binding_id,
            table_id,
            table_name,
            columns,
        } => PhysicalPlan::SeqScan {
            binding_id: *binding_id,
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.clone(),
        },
        LogicalPlan::Join {
            left,
            right,
            kind,
            predicate,
            columns,
        } => {
            let physical_left =
                Box::new(plan_with_statistics(left, table_statistics, access_paths));
            let physical_right =
                Box::new(plan_with_statistics(right, table_statistics, access_paths));
            if let Some((left_key, right_key)) =
                eligible_simple_hash_join(*kind, left, right, predicate, table_statistics)
            {
                PhysicalPlan::HashJoin {
                    left: physical_left,
                    right: physical_right,
                    kind: *kind,
                    left_key,
                    right_key,
                    predicate: predicate.clone(),
                    columns: columns.clone(),
                }
            } else {
                PhysicalPlan::NestedLoopJoin {
                    left: physical_left,
                    right: physical_right,
                    kind: *kind,
                    predicate: predicate.clone(),
                    columns: columns.clone(),
                }
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            let input = match input.as_ref() {
                LogicalPlan::Scan {
                    binding_id,
                    table_id,
                    table_name,
                    columns,
                } => choose_index_access(
                    predicate,
                    *binding_id,
                    *table_id,
                    table_name,
                    columns,
                    table_statistics,
                    access_paths,
                )
                .unwrap_or_else(|| plan_with_statistics(input, table_statistics, access_paths)),
                _ => plan_with_statistics(input, table_statistics, access_paths),
            };
            PhysicalPlan::Filter {
                input: Box::new(input),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Sort { input, keys } => PhysicalPlan::Sort {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            keys: keys.clone(),
        },
        LogicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            columns: columns.clone(),
        },
        LogicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => PhysicalPlan::Aggregate {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            group_keys: group_keys.clone(),
            outputs: outputs.clone(),
        },
        LogicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(plan_with_statistics(input, table_statistics, access_paths)),
            limit: *limit,
        },
    }
}

fn eligible_simple_hash_join(
    kind: JoinKind,
    left: &LogicalPlan,
    right: &LogicalPlan,
    predicate: &Expr,
    table_statistics: &[TableAccessStatistics],
) -> Option<(ColumnRef, ColumnRef)> {
    if !matches!(kind, JoinKind::Inner) {
        return None;
    }
    let left_rows = direct_scan_row_count(left, table_statistics)?;
    let right_rows = direct_scan_row_count(right, table_statistics)?;
    let nested_loop_work = u128::from(left_rows).checked_mul(u128::from(right_rows))?;
    let hash_join_work = u128::from(left_rows).checked_add(u128::from(right_rows))?;
    if hash_join_work >= nested_loop_work {
        return None;
    }
    find_hash_equality(predicate, left, right)
}

fn direct_scan_row_count(
    plan: &LogicalPlan,
    table_statistics: &[TableAccessStatistics],
) -> Option<u64> {
    let LogicalPlan::Scan { table_id, .. } = plan else {
        return None;
    };
    table_statistics
        .iter()
        .find(|candidate| candidate.table_id == *table_id)?
        .statistics
        .as_ref()
        .map(|statistics| statistics.row_count)
}

fn find_hash_equality(
    predicate: &Expr,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Option<(ColumnRef, ColumnRef)> {
    match &predicate.kind {
        ExprKind::Binary {
            operator: BinaryOp::And,
            left: first,
            right: second,
        } => find_hash_equality(first, left, right)
            .or_else(|| find_hash_equality(second, left, right)),
        ExprKind::Binary {
            operator: BinaryOp::Eq,
            left: first,
            right: second,
        } => {
            let (ExprKind::Column(first), ExprKind::Column(second)) = (&first.kind, &second.kind)
            else {
                return None;
            };
            if !first.data_type.is_compatible_with(&second.data_type) {
                return None;
            }
            if scan_contains_column(left, first) && scan_contains_column(right, second) {
                Some((first.clone(), second.clone()))
            } else if scan_contains_column(left, second) && scan_contains_column(right, first) {
                Some((second.clone(), first.clone()))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn scan_contains_column(plan: &LogicalPlan, column: &ColumnRef) -> bool {
    let LogicalPlan::Scan { columns, .. } = plan else {
        return false;
    };
    columns.iter().any(|candidate| {
        candidate.binding_id == column.binding_id && candidate.column_id == column.column_id
    })
}

#[derive(Debug)]
enum IndexLookupCandidate {
    Point {
        key: ScalarValue,
    },
    Range {
        range: IndexRange,
        possible_integer_keys: u128,
    },
}

#[derive(Debug)]
struct IndexCandidate<'a> {
    access_path: &'a IndexAccessPath,
    index_column: ColumnRef,
    lookup: IndexLookupCandidate,
}

fn choose_index_access(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
    table_statistics: &[TableAccessStatistics],
    access_paths: &[IndexAccessPath],
) -> Option<PhysicalPlan> {
    let mut eligible = Vec::new();
    for access_path in access_paths {
        if access_path.table_id != table_id {
            continue;
        }
        if let Some((index_column, key)) =
            find_point_constraint(predicate, binding_id, table_id, access_path.column_id)
        {
            eligible.push(IndexCandidate {
                access_path,
                index_column,
                lookup: IndexLookupCandidate::Point { key },
            });
        }
        if let Some((index_column, range, possible_integer_keys)) =
            find_range_constraint(predicate, binding_id, table_id, access_path.column_id)
        {
            eligible.push(IndexCandidate {
                access_path,
                index_column,
                lookup: IndexLookupCandidate::Range {
                    range,
                    possible_integer_keys,
                },
            });
        }
    }
    let first_point = eligible
        .iter()
        .find(|candidate| matches!(candidate.lookup, IndexLookupCandidate::Point { .. }));
    let analyzed_table = table_statistics
        .iter()
        .find(|entry| entry.table_id == table_id)
        .and_then(|entry| entry.statistics.as_ref());
    let selected = match analyzed_table {
        None => first_point?,
        Some(table) => {
            let mut best: Option<(&IndexCandidate<'_>, u128)> = None;
            for candidate in &eligible {
                let Some(index) = candidate.access_path.statistics.as_ref() else {
                    continue;
                };
                let Some(cost) = candidate_cost(table, index, &candidate.lookup) else {
                    continue;
                };
                if best.is_none_or(|(_, best_cost)| cost < best_cost) {
                    best = Some((candidate, cost));
                }
            }
            let Some((candidate, cost)) = best else {
                return first_point.map(|candidate| {
                    build_index_scan(candidate, binding_id, table_id, table_name, columns)
                });
            };
            if cost >= seq_scan_cost(table) {
                return None;
            }
            candidate
        }
    };
    Some(build_index_scan(
        selected, binding_id, table_id, table_name, columns,
    ))
}

fn build_index_scan(
    candidate: &IndexCandidate<'_>,
    binding_id: RelationBindingId,
    table_id: TableId,
    table_name: &str,
    columns: &[ColumnRef],
) -> PhysicalPlan {
    match &candidate.lookup {
        IndexLookupCandidate::Point { key } => PhysicalPlan::IndexScan {
            binding_id,
            table_id,
            table_name: table_name.to_owned(),
            columns: columns.to_vec(),
            index_column: candidate.index_column.clone(),
            handle: candidate.access_path.handle,
            key: key.clone(),
        },
        IndexLookupCandidate::Range { range, .. } => PhysicalPlan::RangeIndexScan {
            binding_id,
            table_id,
            table_name: table_name.to_owned(),
            columns: columns.to_vec(),
            index_column: candidate.index_column.clone(),
            handle: candidate.access_path.handle,
            range: range.clone(),
        },
    }
}

fn seq_scan_cost(statistics: &TableStatistics) -> u128 {
    u128::from(statistics.managed_page_count)
}

fn candidate_cost(
    table: &TableStatistics,
    index: &IndexStatistics,
    candidate: &IndexLookupCandidate,
) -> Option<u128> {
    let estimated_matches = match candidate {
        IndexLookupCandidate::Point { key } => estimate_point_rows(table, index, key)?,
        IndexLookupCandidate::Range {
            possible_integer_keys,
            ..
        } => estimate_range_rows(table, index, *possible_integer_keys)?,
    };
    Some(1 + u128::from(index.tree_height) + estimated_matches)
}

fn estimate_range_rows(
    table: &TableStatistics,
    index: &IndexStatistics,
    possible_integer_keys: u128,
) -> Option<u128> {
    let non_null_rows = table.row_count.checked_sub(index.null_count)?;
    if non_null_rows == 0 {
        return Some(0);
    }
    if index.distinct_non_null_keys == 0 {
        return None;
    }
    let quotient = non_null_rows / index.distinct_non_null_keys;
    let remainder = non_null_rows % index.distinct_non_null_keys;
    let average_duplicates = u128::from(quotient) + u128::from(remainder != 0);
    let estimated = possible_integer_keys.checked_mul(average_duplicates)?;
    Some(estimated.min(u128::from(non_null_rows)))
}

fn estimate_point_rows(
    table: &TableStatistics,
    index: &IndexStatistics,
    key: &ScalarValue,
) -> Option<u128> {
    if matches!(key, ScalarValue::Null) {
        return Some(u128::from(index.null_count));
    }
    let non_null_rows = table.row_count.checked_sub(index.null_count)?;
    if non_null_rows == 0 {
        return Some(0);
    }
    if index.distinct_non_null_keys == 0 {
        return None;
    }
    let quotient = non_null_rows / index.distinct_non_null_keys;
    let remainder = non_null_rows % index.distinct_non_null_keys;
    Some(u128::from(quotient) + u128::from(remainder != 0))
}

fn find_point_constraint(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, ScalarValue)> {
    match &predicate.kind {
        ExprKind::Binary {
            operator: BinaryOp::And,
            left,
            right,
        } => find_point_constraint(left, binding_id, table_id, column_id)
            .or_else(|| find_point_constraint(right, binding_id, table_id, column_id)),
        ExprKind::Binary {
            operator: BinaryOp::Eq,
            left,
            right,
        } => point_equality(left, right, binding_id, table_id, column_id)
            .or_else(|| point_equality(right, left, binding_id, table_id, column_id)),
        ExprKind::IsNull {
            expression,
            negated: false,
        } => match &expression.kind {
            ExprKind::Column(column)
                if column_matches(column, binding_id, table_id, column_id) && column.nullable =>
            {
                Some((column.clone(), ScalarValue::Null))
            }
            _ => None,
        },
        _ => None,
    }
}

fn find_range_constraint(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, IndexRange, u128)> {
    let mut column = None;
    let mut lower = None;
    let mut upper = None;
    collect_range_bounds(
        predicate,
        binding_id,
        table_id,
        column_id,
        &mut column,
        &mut lower,
        &mut upper,
    );
    let column = column?;
    if !matches!(
        column.data_type.physical,
        netbadb_types::PhysicalType::Int64 | netbadb_types::PhysicalType::UInt64
    ) {
        return None;
    }
    let range = IndexRange {
        lower: lower?,
        upper: upper?,
    };
    let possible_integer_keys = estimated_integer_key_count(&range)?;
    Some((column, range, possible_integer_keys))
}

fn collect_range_bounds(
    predicate: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
    column: &mut Option<ColumnRef>,
    lower: &mut Option<IndexBound>,
    upper: &mut Option<IndexBound>,
) {
    let ExprKind::Binary {
        operator,
        left,
        right,
    } = &predicate.kind
    else {
        return;
    };
    if *operator == BinaryOp::And {
        collect_range_bounds(left, binding_id, table_id, column_id, column, lower, upper);
        collect_range_bounds(right, binding_id, table_id, column_id, column, lower, upper);
        return;
    }
    let Some((matched_column, bound, is_lower)) =
        comparison_bound(*operator, left, right, binding_id, table_id, column_id)
    else {
        return;
    };
    *column = Some(matched_column);
    if is_lower {
        tighten_lower(lower, bound);
    } else {
        tighten_upper(upper, bound);
    }
}

fn comparison_bound(
    operator: BinaryOp,
    left: &Expr,
    right: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, IndexBound, bool)> {
    comparison_bound_ordered(operator, left, right, binding_id, table_id, column_id).or_else(|| {
        comparison_bound_ordered(
            reverse_comparison(operator)?,
            right,
            left,
            binding_id,
            table_id,
            column_id,
        )
    })
}

fn comparison_bound_ordered(
    operator: BinaryOp,
    column: &Expr,
    literal: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, IndexBound, bool)> {
    let ExprKind::Column(column) = &column.kind else {
        return None;
    };
    let ExprKind::Literal(value) = &literal.kind else {
        return None;
    };
    if matches!(value, ScalarValue::Null)
        || !column_matches(column, binding_id, table_id, column_id)
    {
        return None;
    }
    let (bound, is_lower) = match operator {
        BinaryOp::Gt => (IndexBound::Excluded(value.clone()), true),
        BinaryOp::GtEq => (IndexBound::Included(value.clone()), true),
        BinaryOp::Lt => (IndexBound::Excluded(value.clone()), false),
        BinaryOp::LtEq => (IndexBound::Included(value.clone()), false),
        BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or => return None,
    };
    Some((column.clone(), bound, is_lower))
}

const fn reverse_comparison(operator: BinaryOp) -> Option<BinaryOp> {
    match operator {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::And | BinaryOp::Or => None,
    }
}

fn tighten_lower(current: &mut Option<IndexBound>, candidate: IndexBound) {
    let replace = current
        .as_ref()
        .is_none_or(|existing| compare_bounds(existing, &candidate, true) == Ordering::Less);
    if replace {
        *current = Some(candidate);
    }
}

fn tighten_upper(current: &mut Option<IndexBound>, candidate: IndexBound) {
    let replace = current
        .as_ref()
        .is_none_or(|existing| compare_bounds(existing, &candidate, false) == Ordering::Greater);
    if replace {
        *current = Some(candidate);
    }
}

fn compare_bounds(left: &IndexBound, right: &IndexBound, lower: bool) -> Ordering {
    let Some((left_value, left_included)) = bound_value(left) else {
        return Ordering::Equal;
    };
    let Some((right_value, right_included)) = bound_value(right) else {
        return Ordering::Equal;
    };
    compare_values(left_value, right_value).then_with(|| {
        if left_included == right_included {
            Ordering::Equal
        } else if lower {
            left_included.cmp(&right_included).reverse()
        } else {
            left_included.cmp(&right_included)
        }
    })
}

fn bound_value(bound: &IndexBound) -> Option<(&ScalarValue, bool)> {
    match bound {
        IndexBound::Included(value) => Some((value, true)),
        IndexBound::Excluded(value) => Some((value, false)),
        IndexBound::Unbounded => None,
    }
}

fn estimated_integer_key_count(range: &IndexRange) -> Option<u128> {
    match (&range.lower, &range.upper) {
        (IndexBound::Included(ScalarValue::Int64(lower)), _) => {
            let lower = i128::from(*lower);
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::Int64(value)) => i128::from(*value) + 1,
                IndexBound::Excluded(ScalarValue::Int64(value)) => i128::from(*value),
                _ => return None,
            };
            if upper <= lower {
                Some(0)
            } else {
                u128::try_from(upper - lower).ok()
            }
        }
        (IndexBound::Excluded(ScalarValue::Int64(lower)), _) => {
            let lower = i128::from(*lower) + 1;
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::Int64(value)) => i128::from(*value) + 1,
                IndexBound::Excluded(ScalarValue::Int64(value)) => i128::from(*value),
                _ => return None,
            };
            if upper <= lower {
                Some(0)
            } else {
                u128::try_from(upper - lower).ok()
            }
        }
        (IndexBound::Included(ScalarValue::UInt64(lower)), _) => {
            let lower = u128::from(*lower);
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::UInt64(value)) => {
                    u128::from(*value).checked_add(1)?
                }
                IndexBound::Excluded(ScalarValue::UInt64(value)) => u128::from(*value),
                _ => return None,
            };
            Some(upper.saturating_sub(lower))
        }
        (IndexBound::Excluded(ScalarValue::UInt64(lower)), _) => {
            let lower = u128::from(*lower).checked_add(1)?;
            let upper = match &range.upper {
                IndexBound::Included(ScalarValue::UInt64(value)) => {
                    u128::from(*value).checked_add(1)?
                }
                IndexBound::Excluded(ScalarValue::UInt64(value)) => u128::from(*value),
                _ => return None,
            };
            Some(upper.saturating_sub(lower))
        }
        _ => None,
    }
}

fn point_equality(
    column: &Expr,
    literal: &Expr,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> Option<(ColumnRef, ScalarValue)> {
    let ExprKind::Column(column) = &column.kind else {
        return None;
    };
    let ExprKind::Literal(value) = &literal.kind else {
        return None;
    };
    if matches!(value, ScalarValue::Null)
        || !column_matches(column, binding_id, table_id, column_id)
    {
        return None;
    }
    Some((column.clone(), value.clone()))
}

fn column_matches(
    column: &ColumnRef,
    binding_id: RelationBindingId,
    table_id: TableId,
    column_id: ColumnId,
) -> bool {
    column.binding_id == binding_id && column.table_id == table_id && column.column_id == column_id
}

#[must_use]
pub fn plan_statement(logical: &LogicalStatement) -> PhysicalStatement {
    plan_statement_with_access_paths(logical, &[])
}

/// Plans one statement using access paths in caller-provided priority order.
#[must_use]
pub fn plan_statement_with_access_paths(
    logical: &LogicalStatement,
    access_paths: &[IndexAccessPath],
) -> PhysicalStatement {
    plan_statement_with_statistics(logical, &[], access_paths)
}

/// Plans one statement with the same access-path statistics context used for
/// queries, UPDATE, and DELETE.
#[must_use]
pub fn plan_statement_with_statistics(
    logical: &LogicalStatement,
    table_statistics: &[TableAccessStatistics],
    access_paths: &[IndexAccessPath],
) -> PhysicalStatement {
    match logical {
        LogicalStatement::Query(query) => {
            PhysicalStatement::Query(plan_with_statistics(query, table_statistics, access_paths))
        }
        LogicalStatement::Insert {
            table_id,
            table_name,
            values,
        } => PhysicalStatement::Insert {
            table_id: *table_id,
            table_name: table_name.clone(),
            values: values.clone(),
        },
        LogicalStatement::Update {
            input,
            table_id,
            assignments,
        } => PhysicalStatement::Update {
            input: plan_with_statistics(input, table_statistics, access_paths),
            table_id: *table_id,
            assignments: assignments.clone(),
        },
        LogicalStatement::Delete { input, table_id } => PhysicalStatement::Delete {
            input: plan_with_statistics(input, table_statistics, access_paths),
            table_id: *table_id,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IndexAccessPath, PhysicalPlan, PhysicalStatement, TableAccessStatistics, plan,
        plan_statement, plan_statement_with_access_paths, plan_statement_with_statistics,
        plan_with_access_paths, plan_with_statistics,
    };
    use netbadb_index::{BTreeHandle, IndexBound, IndexRange, IndexStatistics, TableStatistics};
    use netbadb_rel::{BinaryOp, ColumnRef, Expr, ExprKind, LogicalPlan, LogicalStatement};
    use netbadb_types::{
        ColumnId, ExprType, PageId, PhysicalType, RelationBindingId, ScalarValue, SemanticType,
        TableId,
    };

    #[test]
    fn creates_a_sequence_scan_physical_plan() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let logical = LogicalPlan::Scan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "users".into(),
            columns: vec![column],
        };
        assert!(matches!(plan(&logical), PhysicalPlan::SeqScan { .. }));
    }

    #[test]
    fn preserves_a_dml_operator_above_its_physical_input() {
        let logical = LogicalStatement::Delete {
            input: LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: Vec::new(),
            },
            table_id: TableId(1),
        };
        assert!(matches!(
            plan_statement(&logical),
            PhysicalStatement::Delete {
                input: PhysicalPlan::SeqScan { .. },
                table_id: TableId(1),
            }
        ));
    }

    #[test]
    fn lowers_logical_join_directly_to_nested_loop_join() {
        let logical = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "employees".into(),
                columns: Vec::new(),
            }),
            right: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(1),
                table_id: TableId(1),
                table_name: "employees".into(),
                columns: Vec::new(),
            }),
            kind: netbadb_rel::JoinKind::Inner,
            predicate: netbadb_rel::Expr {
                kind: netbadb_rel::ExprKind::Literal(netbadb_types::ScalarValue::Bool(true)),
                expr_type: netbadb_types::ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            columns: Vec::new(),
        };
        assert!(matches!(
            plan(&logical),
            PhysicalPlan::NestedLoopJoin { .. }
        ));
    }

    fn join_column_ref(
        binding_id: u32,
        table_id: u64,
        column_id: u32,
        name: &str,
        data_type: SemanticType,
        nullable: bool,
    ) -> ColumnRef {
        ColumnRef {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(table_id),
            column_id: ColumnId(column_id),
            relation_name: format!("r{binding_id}"),
            name: name.into(),
            data_type,
            nullable,
        }
    }

    fn join_scan(binding_id: u32, table_id: u64, columns: Vec<ColumnRef>) -> LogicalPlan {
        LogicalPlan::Scan {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(table_id),
            table_name: format!("table_{table_id}"),
            columns,
        }
    }

    fn join_expr(column: &ColumnRef) -> Expr {
        Expr {
            kind: ExprKind::Column(column.clone()),
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
        }
    }

    fn join_literal(value: ScalarValue, physical: PhysicalType) -> Expr {
        Expr {
            kind: ExprKind::Literal(value.clone()),
            expr_type: ExprType {
                data_type: SemanticType::physical(physical),
                nullable: matches!(value, ScalarValue::Null),
            },
        }
    }

    fn join_binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr {
            kind: ExprKind::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: true,
            },
        }
    }

    fn logical_join(left: LogicalPlan, right: LogicalPlan, predicate: Expr) -> LogicalPlan {
        let mut columns = left
            .output_fields()
            .into_iter()
            .filter_map(|field| match field {
                netbadb_rel::OutputField::Source(column) => Some(column),
                netbadb_rel::OutputField::Derived(_) => None,
            })
            .collect::<Vec<_>>();
        columns.extend(
            right
                .output_fields()
                .into_iter()
                .filter_map(|field| match field {
                    netbadb_rel::OutputField::Source(column) => Some(column),
                    netbadb_rel::OutputField::Derived(_) => None,
                }),
        );
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            kind: netbadb_rel::JoinKind::Inner,
            predicate,
            columns,
        }
    }

    fn join_table_statistics(table_id: u64, row_count: u64) -> TableAccessStatistics {
        TableAccessStatistics {
            table_id: TableId(table_id),
            statistics: Some(TableStatistics {
                row_count,
                managed_page_count: 1,
            }),
        }
    }

    fn simple_join_fixture(
        left_type: SemanticType,
        right_type: SemanticType,
    ) -> (LogicalPlan, LogicalPlan, ColumnRef, ColumnRef) {
        let left_key = join_column_ref(10, 1, 1, "key", left_type, true);
        let right_key = join_column_ref(20, 2, 1, "key", right_type, true);
        (
            join_scan(10, 1, vec![left_key.clone()]),
            join_scan(20, 2, vec![right_key.clone()]),
            left_key,
            right_key,
        )
    }

    #[test]
    fn analyzed_direct_equality_uses_hash_join_and_normalizes_orientation() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let statistics = [join_table_statistics(1, 500), join_table_statistics(2, 500)];
        for predicate in [
            join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key)),
            join_binary(BinaryOp::Eq, join_expr(&right_key), join_expr(&left_key)),
        ] {
            assert!(matches!(
                plan_with_statistics(
                    &logical_join(left.clone(), right.clone(), predicate),
                    &statistics,
                    &[],
                ),
                PhysicalPlan::HashJoin {
                    left_key: actual_left,
                    right_key: actual_right,
                    ..
                } if actual_left == left_key && actual_right == right_key
            ));
        }
    }

    #[test]
    fn hash_join_requires_both_statistics_and_strictly_cheaper_work() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let predicate = join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key));
        let logical = logical_join(left, right, predicate);
        for statistics in [
            Vec::new(),
            vec![join_table_statistics(1, 500)],
            vec![join_table_statistics(2, 500)],
        ] {
            assert!(matches!(
                plan_with_statistics(&logical, &statistics, &[]),
                PhysicalPlan::NestedLoopJoin { .. }
            ));
        }
        for (left_rows, right_rows, hash_expected) in [(1, 100, false), (2, 2, false), (2, 3, true)]
        {
            let statistics = [
                join_table_statistics(1, left_rows),
                join_table_statistics(2, right_rows),
            ];
            assert_eq!(
                matches!(
                    plan_with_statistics(&logical, &statistics, &[]),
                    PhysicalPlan::HashJoin { .. }
                ),
                hash_expected
            );
        }
    }

    #[test]
    fn hash_equality_extraction_is_necessary_typed_and_deterministic() {
        let left_a = join_column_ref(
            10,
            1,
            1,
            "a",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left_b = join_column_ref(
            10,
            1,
            2,
            "b",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left_active = join_column_ref(
            10,
            1,
            3,
            "active",
            SemanticType::physical(PhysicalType::Bool),
            false,
        );
        let right_a = join_column_ref(
            20,
            2,
            1,
            "a",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let right_b = join_column_ref(
            20,
            2,
            2,
            "b",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let left = join_scan(
            10,
            1,
            vec![left_a.clone(), left_b.clone(), left_active.clone()],
        );
        let right = join_scan(20, 2, vec![right_a.clone(), right_b.clone()]);
        let statistics = [join_table_statistics(1, 500), join_table_statistics(2, 500)];
        let first_equality = join_binary(BinaryOp::Eq, join_expr(&left_a), join_expr(&right_a));
        let second_equality = join_binary(BinaryOp::Eq, join_expr(&left_b), join_expr(&right_b));
        let nested = join_binary(
            BinaryOp::And,
            join_binary(
                BinaryOp::Eq,
                join_expr(&left_active),
                join_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            ),
            join_binary(
                BinaryOp::And,
                first_equality.clone(),
                second_equality.clone(),
            ),
        );
        assert!(matches!(
            plan_with_statistics(
                &logical_join(left.clone(), right.clone(), nested.clone()),
                &statistics,
                &[],
            ),
            PhysicalPlan::HashJoin {
                left_key,
                right_key,
                predicate,
                ..
            } if left_key == left_a && right_key == right_a && predicate == nested
        ));

        let rejected = [
            join_binary(
                BinaryOp::Or,
                first_equality.clone(),
                join_expr(&left_active),
            ),
            Expr {
                kind: ExprKind::Unary {
                    operator: netbadb_rel::UnaryOp::Not,
                    expression: Box::new(first_equality.clone()),
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            join_binary(BinaryOp::NotEq, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::Lt, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::LtEq, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::Gt, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::GtEq, join_expr(&left_a), join_expr(&right_a)),
            join_binary(BinaryOp::Eq, join_expr(&left_a), join_expr(&left_b)),
            join_binary(
                BinaryOp::Eq,
                join_expr(&left_a),
                join_literal(ScalarValue::Int64(42), PhysicalType::Int64),
            ),
        ];
        for predicate in rejected {
            assert!(matches!(
                plan_with_statistics(
                    &logical_join(left.clone(), right.clone(), predicate),
                    &statistics,
                    &[],
                ),
                PhysicalPlan::NestedLoopJoin { .. }
            ));
        }

        let (nominal_left, nominal_right, nominal_left_key, nominal_right_key) =
            simple_join_fixture(
                SemanticType::named("UserId", PhysicalType::UInt64),
                SemanticType::named("TeamId", PhysicalType::UInt64),
            );
        assert!(matches!(
            plan_with_statistics(
                &logical_join(
                    nominal_left,
                    nominal_right,
                    join_binary(
                        BinaryOp::Eq,
                        join_expr(&nominal_left_key),
                        join_expr(&nominal_right_key),
                    ),
                ),
                &statistics,
                &[],
            ),
            PhysicalPlan::NestedLoopJoin { .. }
        ));
    }

    #[test]
    fn only_the_current_direct_scan_join_is_hash_eligible() {
        let (left, right, left_key, right_key) = simple_join_fixture(
            SemanticType::physical(PhysicalType::Int64),
            SemanticType::physical(PhysicalType::Int64),
        );
        let predicate = join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&right_key));
        let statistics = [
            join_table_statistics(1, 500),
            join_table_statistics(2, 500),
            join_table_statistics(3, 500),
        ];
        let filtered_left = LogicalPlan::Filter {
            input: Box::new(left.clone()),
            predicate: join_literal(ScalarValue::Bool(true), PhysicalType::Bool),
        };
        assert!(matches!(
            plan_with_statistics(
                &logical_join(filtered_left, right.clone(), predicate.clone()),
                &statistics,
                &[],
            ),
            PhysicalPlan::NestedLoopJoin { .. }
        ));

        let inner = logical_join(left, right, predicate);
        let third_key = join_column_ref(
            30,
            3,
            1,
            "key",
            SemanticType::physical(PhysicalType::Int64),
            false,
        );
        let outer = logical_join(
            inner,
            join_scan(30, 3, vec![third_key.clone()]),
            join_binary(BinaryOp::Eq, join_expr(&left_key), join_expr(&third_key)),
        );
        assert!(matches!(
            plan_with_statistics(&outer, &statistics, &[]),
            PhysicalPlan::NestedLoopJoin { left, .. }
                if matches!(*left, PhysicalPlan::HashJoin { .. })
        ));
    }

    #[test]
    fn lowers_sort_without_changing_output_columns() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let logical = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![column.clone()],
            }),
            keys: vec![netbadb_rel::SortKey {
                column: column.clone(),
                direction: netbadb_rel::SortDirection::Desc,
                null_order: netbadb_rel::NullOrder::Last,
            }],
        };
        assert_eq!(
            logical.output_fields(),
            vec![netbadb_rel::OutputField::Source(column)]
        );
        assert!(matches!(
            plan(&logical),
            PhysicalPlan::Sort { keys, .. }
                if keys[0].direction == netbadb_rel::SortDirection::Desc
        ));
    }

    #[test]
    fn lowers_global_aggregate_with_derived_output_fields() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "score".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: true,
        };
        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![column.clone()],
            }),
            group_keys: Vec::new(),
            outputs: vec![netbadb_rel::AggregateOutput::Aggregate(
                netbadb_rel::AggregateExpr {
                    function: netbadb_rel::AggregateFunction::Min,
                    input: netbadb_rel::AggregateInput::Column(column),
                    output: netbadb_rel::DerivedField {
                        name: "MIN(score)".into(),
                        data_type: SemanticType::physical(PhysicalType::Int64),
                        nullable: true,
                    },
                },
            )],
        };
        let physical = plan(&logical);
        assert!(matches!(
            physical.output_fields().as_slice(),
            [netbadb_rel::OutputField::Derived(field)] if field.name == "MIN(score)"
        ));
        assert!(matches!(physical, PhysicalPlan::Aggregate { .. }));
    }

    #[test]
    fn preserves_group_keys_and_interleaved_aggregate_outputs() {
        let column = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "team_id".into(),
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: true,
        };
        let count = netbadb_rel::AggregateExpr {
            function: netbadb_rel::AggregateFunction::Count,
            input: netbadb_rel::AggregateInput::All,
            output: netbadb_rel::DerivedField {
                name: "COUNT(*)".into(),
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
        };
        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "users".into(),
                columns: vec![column.clone()],
            }),
            group_keys: vec![column.clone()],
            outputs: vec![
                netbadb_rel::AggregateOutput::Aggregate(count.clone()),
                netbadb_rel::AggregateOutput::GroupKey(column),
                netbadb_rel::AggregateOutput::Aggregate(count),
            ],
        };
        let physical = plan(&logical);
        assert!(matches!(
            physical.output_fields().as_slice(),
            [
                netbadb_rel::OutputField::Derived(_),
                netbadb_rel::OutputField::Source(_),
                netbadb_rel::OutputField::Derived(_)
            ]
        ));
        assert!(matches!(
            physical,
            PhysicalPlan::Aggregate {
                group_keys,
                outputs,
                ..
            } if group_keys.len() == 1 && outputs.len() == 3
        ));
    }

    fn test_column(column_id: u32, name: &str, nullable: bool) -> ColumnRef {
        typed_column(column_id, name, PhysicalType::Int64, nullable)
    }

    fn typed_column(
        column_id: u32,
        name: &str,
        physical: PhysicalType,
        nullable: bool,
    ) -> ColumnRef {
        ColumnRef {
            binding_id: RelationBindingId(7),
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            relation_name: "u".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable,
        }
    }

    fn column_expr(column: &ColumnRef) -> Expr {
        Expr {
            kind: ExprKind::Column(column.clone()),
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
        }
    }

    fn literal(value: ScalarValue) -> Expr {
        let nullable = matches!(value, ScalarValue::Null);
        Expr {
            kind: ExprKind::Literal(value),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable,
            },
        }
    }

    fn binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr {
            kind: ExprKind::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: true,
            },
        }
    }

    fn filtered_scan(predicate: Expr, columns: Vec<ColumnRef>) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(7),
                table_id: TableId(1),
                table_name: "users".into(),
                columns,
            }),
            predicate,
        }
    }

    fn access_path(column_id: u32, page_id: u64) -> IndexAccessPath {
        IndexAccessPath {
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            handle: BTreeHandle {
                meta_page: PageId(page_id),
            },
            statistics: None,
        }
    }

    fn analyzed_path(
        column_id: u32,
        page_id: u64,
        distinct_non_null_keys: u64,
        null_count: u64,
        tree_height: u32,
    ) -> IndexAccessPath {
        let mut path = access_path(column_id, page_id);
        path.statistics = Some(IndexStatistics {
            distinct_non_null_keys,
            null_count,
            tree_height,
        });
        path
    }

    fn analyzed_table(row_count: u64, managed_page_count: u64) -> TableAccessStatistics {
        TableAccessStatistics {
            table_id: TableId(1),
            statistics: Some(TableStatistics {
                row_count,
                managed_page_count,
            }),
        }
    }

    fn index_scan_input(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::Filter { input, .. } => Some(input),
            _ => None,
        }
    }

    fn bounded(column: &ColumnRef, lower: i64, upper: i64) -> Expr {
        binary(
            BinaryOp::And,
            binary(
                BinaryOp::GtEq,
                column_expr(column),
                literal(ScalarValue::Int64(lower)),
            ),
            binary(
                BinaryOp::Lt,
                column_expr(column),
                literal(ScalarValue::Int64(upper)),
            ),
        )
    }

    #[test]
    fn bounded_integer_ranges_require_statistics_and_compare_costs() {
        let id = test_column(1, "id", false);
        let access = [analyzed_path(1, 40, 10_000, 0, 2)];
        let table = [analyzed_table(10_000, 1_000)];
        let narrow = plan_with_statistics(
            &filtered_scan(bounded(&id, 5_000, 5_100), vec![id.clone()]),
            &table,
            &access,
        );
        assert!(matches!(
            index_scan_input(&narrow),
            Some(PhysicalPlan::RangeIndexScan {
                range: IndexRange {
                    lower: IndexBound::Included(ScalarValue::Int64(5_000)),
                    upper: IndexBound::Excluded(ScalarValue::Int64(5_100)),
                },
                ..
            })
        ));

        let wide = plan_with_statistics(
            &filtered_scan(bounded(&id, 2_500, 7_500), vec![id.clone()]),
            &table,
            &access,
        );
        assert!(matches!(
            index_scan_input(&wide),
            Some(PhysicalPlan::SeqScan { .. })
        ));
        let without_statistics = plan_with_access_paths(
            &filtered_scan(bounded(&id, 5_000, 5_100), vec![id]),
            &[access_path(1, 40)],
        );
        assert!(matches!(
            index_scan_input(&without_statistics),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn range_extraction_reverses_and_tightens_nested_bounds() {
        let id = test_column(1, "id", false);
        let predicate = binary(
            BinaryOp::And,
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::LtEq,
                    literal(ScalarValue::Int64(100)),
                    column_expr(&id),
                ),
                binary(
                    BinaryOp::Gt,
                    column_expr(&id),
                    literal(ScalarValue::Int64(200)),
                ),
            ),
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::Gt,
                    literal(ScalarValue::Int64(1_000)),
                    column_expr(&id),
                ),
                binary(
                    BinaryOp::LtEq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(900)),
                ),
            ),
        );
        let physical = plan_with_statistics(
            &filtered_scan(predicate, vec![id]),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::RangeIndexScan {
                range: IndexRange {
                    lower: IndexBound::Excluded(ScalarValue::Int64(200)),
                    upper: IndexBound::Included(ScalarValue::Int64(900)),
                },
                ..
            })
        ));
    }

    #[test]
    fn unsupported_range_shapes_remain_sequence_scans() {
        let analyzed = [analyzed_table(10_000, 2_000)];
        for physical in [PhysicalType::Int64, PhysicalType::Text, PhysicalType::Bool] {
            let column = typed_column(1, "value", physical, false);
            let (lower, upper) = match physical {
                PhysicalType::Int64 => (ScalarValue::Int64(10), ScalarValue::Int64(20)),
                PhysicalType::Text => {
                    (ScalarValue::Text("a".into()), ScalarValue::Text("b".into()))
                }
                PhysicalType::Bool => (ScalarValue::Bool(false), ScalarValue::Bool(true)),
                PhysicalType::UInt64 => return,
            };
            let predicate = binary(
                BinaryOp::And,
                binary(BinaryOp::GtEq, column_expr(&column), literal(lower)),
                binary(BinaryOp::Lt, column_expr(&column), literal(upper)),
            );
            let plan = plan_with_statistics(
                &filtered_scan(predicate, vec![column]),
                &analyzed,
                &[analyzed_path(1, 40, 10_000, 0, 2)],
            );
            if physical == PhysicalType::Int64 {
                assert!(matches!(
                    index_scan_input(&plan),
                    Some(PhysicalPlan::RangeIndexScan { .. })
                ));
            } else {
                assert!(matches!(
                    index_scan_input(&plan),
                    Some(PhysicalPlan::SeqScan { .. })
                ));
            }
        }

        let id = test_column(1, "id", false);
        let one_sided = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::GtEq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(10)),
                ),
                vec![id.clone()],
            ),
            &analyzed,
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&one_sided),
            Some(PhysicalPlan::SeqScan { .. })
        ));
        let disjunction = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::Or,
                    binary(
                        BinaryOp::Lt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(10)),
                    ),
                    binary(
                        BinaryOp::Gt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(9_000)),
                    ),
                ),
                vec![id],
            ),
            &analyzed,
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&disjunction),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn integer_key_count_is_checked_at_signed_and_unsigned_boundaries() {
        assert_eq!(
            super::estimated_integer_key_count(&IndexRange {
                lower: IndexBound::Included(ScalarValue::Int64(i64::MIN)),
                upper: IndexBound::Included(ScalarValue::Int64(i64::MAX)),
            }),
            Some(1_u128 << 64)
        );
        assert_eq!(
            super::estimated_integer_key_count(&IndexRange {
                lower: IndexBound::Included(ScalarValue::UInt64(0)),
                upper: IndexBound::Included(ScalarValue::UInt64(u64::MAX)),
            }),
            Some(1_u128 << 64)
        );
        assert_eq!(
            super::estimated_integer_key_count(&IndexRange {
                lower: IndexBound::Excluded(ScalarValue::Int64(10)),
                upper: IndexBound::Excluded(ScalarValue::Int64(5)),
            }),
            Some(0)
        );
    }

    #[test]
    fn point_range_and_contradiction_candidates_share_one_costed_choice() {
        let id = test_column(1, "id", false);
        let bucket = test_column(2, "bucket", false);
        let point_and_range = binary(
            BinaryOp::And,
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            bounded(&id, 0, 100),
        );
        let point = plan_with_statistics(
            &filtered_scan(point_and_range, vec![id.clone()]),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&point),
            Some(PhysicalPlan::IndexScan {
                key: ScalarValue::Int64(42),
                ..
            })
        ));

        let contradiction = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::And,
                    binary(
                        BinaryOp::Gt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(100)),
                    ),
                    binary(
                        BinaryOp::Lt,
                        column_expr(&id),
                        literal(ScalarValue::Int64(100)),
                    ),
                ),
                vec![id.clone()],
            ),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&contradiction),
            Some(PhysicalPlan::RangeIndexScan { .. })
        ));

        let mixed = plan_with_statistics(
            &filtered_scan(
                binary(
                    BinaryOp::And,
                    bounded(&id, 0, 100),
                    binary(
                        BinaryOp::Eq,
                        column_expr(&bucket),
                        literal(ScalarValue::Int64(7)),
                    ),
                ),
                vec![id, bucket],
            ),
            &[analyzed_table(10_000, 2_000)],
            &[
                analyzed_path(1, 40, 10_000, 0, 2),
                analyzed_path(2, 50, 10_000, 0, 2),
            ],
        );
        assert!(matches!(
            index_scan_input(&mixed),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
                ..
            })
        ));
    }

    #[test]
    fn bounded_uint64_range_is_costable() {
        let id = typed_column(1, "id", PhysicalType::UInt64, false);
        let predicate = binary(
            BinaryOp::And,
            binary(
                BinaryOp::GtEq,
                column_expr(&id),
                literal(ScalarValue::UInt64(u64::MAX - 9)),
            ),
            binary(
                BinaryOp::LtEq,
                column_expr(&id),
                literal(ScalarValue::UInt64(u64::MAX)),
            ),
        );
        let physical = plan_with_statistics(
            &filtered_scan(predicate, vec![id]),
            &[analyzed_table(10_000, 2_000)],
            &[analyzed_path(1, 40, 10_000, 0, 2)],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::RangeIndexScan { .. })
        ));
    }

    #[test]
    fn point_equality_and_commuted_equality_retain_the_full_filter() {
        let id = test_column(1, "id", false);
        let path = access_path(1, 40);
        for predicate in [
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            binary(
                BinaryOp::Eq,
                literal(ScalarValue::Int64(42)),
                column_expr(&id),
            ),
        ] {
            let logical = filtered_scan(predicate.clone(), vec![id.clone()]);
            let physical = plan_with_access_paths(&logical, std::slice::from_ref(&path));
            assert!(matches!(
                &physical,
                PhysicalPlan::Filter { predicate: actual, .. } if actual == &predicate
            ));
            assert!(matches!(
                index_scan_input(&physical),
                Some(PhysicalPlan::IndexScan {
                    binding_id: RelationBindingId(7),
                    handle,
                    key: ScalarValue::Int64(42),
                    index_column,
                    ..
                }) if *handle == path.handle && index_column.relation_name == "u"
            ));
            assert_eq!(
                physical.output_fields(),
                vec![netbadb_rel::OutputField::Source(id.clone())]
            );
        }
    }

    #[test]
    fn null_and_non_point_predicates_only_use_safe_access_paths() {
        let nullable = test_column(1, "value", true);
        let required = test_column(2, "required", false);
        let path_nullable = access_path(1, 40);
        let path_required = access_path(2, 41);

        let is_null = Expr {
            kind: ExprKind::IsNull {
                expression: Box::new(column_expr(&nullable)),
                negated: false,
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let physical = plan_with_access_paths(
            &filtered_scan(is_null, vec![nullable.clone(), required.clone()]),
            &[path_nullable.clone(), path_required.clone()],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::IndexScan {
                key: ScalarValue::Null,
                ..
            })
        ));

        let unsupported = [
            binary(
                BinaryOp::Eq,
                column_expr(&nullable),
                literal(ScalarValue::Null),
            ),
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(column_expr(&required)),
                    negated: false,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(column_expr(&nullable)),
                    negated: true,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            binary(
                BinaryOp::Lt,
                column_expr(&nullable),
                literal(ScalarValue::Int64(42)),
            ),
            binary(
                BinaryOp::Or,
                binary(
                    BinaryOp::Eq,
                    column_expr(&nullable),
                    literal(ScalarValue::Int64(1)),
                ),
                binary(
                    BinaryOp::Eq,
                    column_expr(&nullable),
                    literal(ScalarValue::Int64(2)),
                ),
            ),
        ];
        for predicate in unsupported {
            let physical = plan_with_access_paths(
                &filtered_scan(predicate, vec![nullable.clone(), required.clone()]),
                &[path_nullable.clone(), path_required.clone()],
            );
            assert!(matches!(
                index_scan_input(&physical),
                Some(PhysicalPlan::SeqScan { .. })
            ));
        }
    }

    #[test]
    fn and_uses_first_eligible_registered_path_not_predicate_order() {
        let team = test_column(2, "team_id", false);
        let name = test_column(3, "name", false);
        let predicate = binary(
            BinaryOp::And,
            binary(
                BinaryOp::Eq,
                column_expr(&name),
                literal(ScalarValue::Int64(9)),
            ),
            binary(
                BinaryOp::Eq,
                column_expr(&team),
                literal(ScalarValue::Int64(10)),
            ),
        );
        let physical = plan_with_access_paths(
            &filtered_scan(predicate, vec![team, name]),
            &[access_path(2, 50), access_path(3, 60)],
        );
        assert!(matches!(
            index_scan_input(&physical),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
                key: ScalarValue::Int64(10),
                ..
            })
        ));
    }

    #[test]
    fn analyzed_costs_compare_point_indexes_with_sequence_scan() {
        let id = test_column(1, "id", false);
        let predicate = binary(
            BinaryOp::Eq,
            column_expr(&id),
            literal(ScalarValue::Int64(42)),
        );
        let logical = filtered_scan(predicate, vec![id]);

        let small = plan_with_statistics(
            &logical,
            &[analyzed_table(1, 3)],
            &[analyzed_path(1, 40, 1, 0, 1)],
        );
        assert!(matches!(
            index_scan_input(&small),
            Some(PhysicalPlan::SeqScan { .. })
        ));

        let selective = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 100)],
            &[analyzed_path(1, 40, 1_000, 0, 1)],
        );
        assert!(matches!(
            index_scan_input(&selective),
            Some(PhysicalPlan::IndexScan { .. })
        ));

        let duplicate_heavy = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 10)],
            &[analyzed_path(1, 40, 2, 0, 1)],
        );
        assert!(matches!(
            index_scan_input(&duplicate_heavy),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn analyzed_null_cost_uses_null_count() {
        let nullable = test_column(2, "team_id", true);
        let logical = filtered_scan(
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(column_expr(&nullable)),
                    negated: false,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
            vec![nullable],
        );
        let sparse = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 100)],
            &[analyzed_path(2, 40, 999, 1, 1)],
        );
        assert!(matches!(
            index_scan_input(&sparse),
            Some(PhysicalPlan::IndexScan { .. })
        ));
        let dense = plan_with_statistics(
            &logical,
            &[analyzed_table(1_000, 20)],
            &[analyzed_path(2, 40, 900, 100, 1)],
        );
        assert!(matches!(
            index_scan_input(&dense),
            Some(PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn analyzed_candidates_use_cost_then_registration_order_and_ignore_unknowns() {
        let team = test_column(2, "team_id", false);
        let name = test_column(3, "name", false);
        let logical = filtered_scan(
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::Eq,
                    column_expr(&team),
                    literal(ScalarValue::Int64(10)),
                ),
                binary(
                    BinaryOp::Eq,
                    column_expr(&name),
                    literal(ScalarValue::Int64(9)),
                ),
            ),
            vec![team, name],
        );
        let table = [analyzed_table(1_000, 100)];
        let cheaper_later = plan_with_statistics(
            &logical,
            &table,
            &[
                analyzed_path(2, 50, 2, 0, 1),
                analyzed_path(3, 60, 1_000, 0, 1),
            ],
        );
        assert!(matches!(
            index_scan_input(&cheaper_later),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(60)
                },
                ..
            })
        ));

        let tied = plan_with_statistics(
            &logical,
            &table,
            &[
                analyzed_path(2, 50, 1_000, 0, 1),
                analyzed_path(3, 60, 1_000, 0, 1),
            ],
        );
        assert!(matches!(
            index_scan_input(&tied),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
                ..
            })
        ));

        let unknown_first = access_path(2, 50);
        let known_second = analyzed_path(3, 60, 1_000, 0, 1);
        let partial = plan_with_statistics(&logical, &table, &[unknown_first, known_second]);
        assert!(matches!(
            index_scan_input(&partial),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(60)
                },
                ..
            })
        ));

        let only_unknown = plan_with_statistics(&logical, &table, &[access_path(2, 50)]);
        assert!(matches!(
            index_scan_input(&only_unknown),
            Some(PhysicalPlan::IndexScan {
                handle: BTreeHandle {
                    meta_page: PageId(50)
                },
                ..
            })
        ));
    }

    #[test]
    fn dml_uses_the_same_costed_access_path_context() {
        let id = test_column(1, "id", false);
        let logical = LogicalStatement::Delete {
            input: filtered_scan(
                binary(
                    BinaryOp::Eq,
                    column_expr(&id),
                    literal(ScalarValue::Int64(42)),
                ),
                vec![id],
            ),
            table_id: TableId(1),
        };
        assert!(matches!(
            plan_statement_with_statistics(
                &logical,
                &[analyzed_table(1_000, 3)],
                &[analyzed_path(1, 40, 1_000, 0, 1)],
            ),
            PhysicalStatement::Delete {
                input: PhysicalPlan::Filter { input, .. },
                ..
            } if matches!(*input, PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn statement_planning_uses_access_paths_for_dml_inputs_only() {
        let id = test_column(1, "id", false);
        let input = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            vec![id],
        );
        let logical = LogicalStatement::Delete {
            input,
            table_id: TableId(1),
        };
        assert!(matches!(
            plan_statement_with_access_paths(&logical, &[access_path(1, 40)]),
            PhysicalStatement::Delete {
                input: PhysicalPlan::Filter { input, .. },
                ..
            } if matches!(*input, PhysicalPlan::IndexScan { .. })
        ));
        assert!(matches!(
            plan_statement(&logical),
            PhysicalStatement::Delete {
                input: PhysicalPlan::Filter { input, .. },
                ..
            } if matches!(*input, PhysicalPlan::SeqScan { .. })
        ));
    }

    #[test]
    fn enclosing_sort_and_aggregate_preserve_the_point_scan_without_elision() {
        let id = test_column(1, "id", false);
        let filtered = filtered_scan(
            binary(
                BinaryOp::Eq,
                column_expr(&id),
                literal(ScalarValue::Int64(42)),
            ),
            vec![id.clone()],
        );
        let sorted = LogicalPlan::Sort {
            input: Box::new(filtered.clone()),
            keys: vec![netbadb_rel::SortKey {
                column: id.clone(),
                direction: netbadb_rel::SortDirection::Desc,
                null_order: netbadb_rel::NullOrder::Last,
            }],
        };
        let sorted = plan_with_access_paths(&sorted, &[access_path(1, 40)]);
        let PhysicalPlan::Sort { input, .. } = sorted else {
            panic!("expected sort");
        };
        assert!(matches!(
            index_scan_input(&input),
            Some(PhysicalPlan::IndexScan { .. })
        ));

        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(filtered),
            group_keys: Vec::new(),
            outputs: vec![netbadb_rel::AggregateOutput::Aggregate(
                netbadb_rel::AggregateExpr {
                    function: netbadb_rel::AggregateFunction::Count,
                    input: netbadb_rel::AggregateInput::All,
                    output: netbadb_rel::DerivedField {
                        name: "COUNT(*)".into(),
                        data_type: SemanticType::physical(PhysicalType::UInt64),
                        nullable: false,
                    },
                },
            )],
        };
        let aggregate = plan_with_access_paths(&aggregate, &[access_path(1, 40)]);
        let PhysicalPlan::Aggregate { input, .. } = aggregate else {
            panic!("expected aggregate");
        };
        assert!(matches!(
            index_scan_input(&input),
            Some(PhysicalPlan::IndexScan { .. })
        ));
    }
}
