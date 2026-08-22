//! Synchronous execution of typed query and DML physical statements.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt;

use netbadb_planner::{PhysicalPlan, PhysicalStatement};
use netbadb_rel::{
    AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, Assignment, BinaryOp,
    ColumnRef, Expr, ExprKind, NullOrder, OutputField, SortDirection, SortKey, UnaryOp,
};
use netbadb_storage::{HeapStorage, PresenceCountSummary, StorageError, Transaction};
use netbadb_types::{RowId, ScalarValue, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    pub name: String,
    pub data_type: netbadb_types::SemanticType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<ScalarValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionResult {
    Query(QueryResult),
    AffectedRows(u64),
}

#[derive(Debug)]
pub enum ExecutionError {
    Storage(StorageError),
    MissingColumn(String),
    ExpectedBoolean,
    TypeMismatch,
    TransactionRequired,
    AffectedRowsOverflow,
    AggregateOverflow {
        function: AggregateFunction,
        output: String,
    },
    InvalidAggregateInput {
        function: AggregateFunction,
    },
    MissingRowIdentity,
    MissingTableStorage(TableId),
    TableMismatch {
        planned: TableId,
        storage: TableId,
    },
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::MissingColumn(name) => write!(
                formatter,
                "execution input does not contain column `{name}`"
            ),
            Self::ExpectedBoolean => {
                formatter.write_str("predicate evaluated to a non-boolean value")
            }
            Self::TypeMismatch => formatter.write_str("values have incompatible runtime types"),
            Self::TransactionRequired => {
                formatter.write_str("a mutating statement requires an active transaction")
            }
            Self::AffectedRowsOverflow => formatter.write_str("affected row count overflowed u64"),
            Self::AggregateOverflow { function, output } => write!(
                formatter,
                "{} overflowed while computing `{output}`",
                function.as_str()
            ),
            Self::InvalidAggregateInput { function } => {
                write!(
                    formatter,
                    "{} does not accept `*` as input",
                    function.as_str()
                )
            }
            Self::MissingRowIdentity => {
                formatter.write_str("mutation input does not contain a base row identity")
            }
            Self::MissingTableStorage(table_id) => {
                write!(formatter, "no storage is attached for table {}", table_id.0)
            }
            Self::TableMismatch { planned, storage } => write!(
                formatter,
                "physical plan targets table {}, but storage contains table {}",
                planned.0, storage.0
            ),
        }
    }
}

impl Error for ExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for ExecutionError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

pub fn execute(
    plan: &PhysicalPlan,
    storage: &mut HeapStorage,
) -> Result<QueryResult, ExecutionError> {
    execute_with_storages(plan, std::slice::from_mut(storage))
}

/// Executes a read-only physical query against the table heaps in `storages`.
/// Each planned `TableId` must have exactly one corresponding heap.
pub fn execute_with_storages(
    plan: &PhysicalPlan,
    storages: &mut [HeapStorage],
) -> Result<QueryResult, ExecutionError> {
    let result = execute_rows(plan, storages)?;
    Ok(QueryResult {
        columns: result
            .fields
            .into_iter()
            .map(|field| ResultColumn {
                name: field.name().to_owned(),
                data_type: field.data_type().clone(),
                nullable: field.nullable(),
            })
            .collect(),
        rows: result.rows.into_iter().map(|row| row.values).collect(),
    })
}

pub fn execute_statement(
    statement: &PhysicalStatement,
    storage: &mut HeapStorage,
    transaction: Option<&mut Transaction>,
) -> Result<ExecutionResult, ExecutionError> {
    match statement {
        PhysicalStatement::Query(plan) => execute(plan, storage).map(ExecutionResult::Query),
        PhysicalStatement::Insert {
            table_id, values, ..
        } => {
            let transaction = transaction.ok_or(ExecutionError::TransactionRequired)?;
            storage.validate_transaction(transaction)?;
            ensure_table(*table_id, storage)?;
            let row = values
                .iter()
                .map(|value| evaluate(value, &[], &[]))
                .collect::<Result<Vec<_>, _>>()?;
            storage.insert_in(transaction, &row)?;
            Ok(ExecutionResult::AffectedRows(1))
        }
        PhysicalStatement::Update {
            input,
            table_id,
            assignments,
        } => {
            let transaction = transaction.ok_or(ExecutionError::TransactionRequired)?;
            storage.validate_transaction(transaction)?;
            ensure_table(*table_id, storage)?;
            let input = execute_rows(input, std::slice::from_mut(storage))?;
            let replacements = build_replacements(&input, assignments)?;
            let affected = u64::try_from(replacements.len())
                .map_err(|_| ExecutionError::AffectedRowsOverflow)?;
            for (row_id, values) in replacements {
                let _current_row_id = storage.update_in(transaction, row_id, &values)?;
            }
            Ok(ExecutionResult::AffectedRows(affected))
        }
        PhysicalStatement::Delete { input, table_id } => {
            let transaction = transaction.ok_or(ExecutionError::TransactionRequired)?;
            storage.validate_transaction(transaction)?;
            ensure_table(*table_id, storage)?;
            let input = execute_rows(input, std::slice::from_mut(storage))?;
            let affected = u64::try_from(input.rows.len())
                .map_err(|_| ExecutionError::AffectedRowsOverflow)?;
            for row in input.rows {
                storage.delete_in(
                    transaction,
                    row.row_id.ok_or(ExecutionError::MissingRowIdentity)?,
                )?;
            }
            Ok(ExecutionResult::AffectedRows(affected))
        }
    }
}

#[derive(Debug)]
struct ExecutionRow {
    row_id: Option<RowId>,
    values: Vec<ScalarValue>,
}

#[derive(Debug)]
struct ExecutionRows {
    fields: Vec<OutputField>,
    rows: Vec<ExecutionRow>,
}

#[derive(Debug, PartialEq, Eq)]
struct ProjectionPlan {
    positions: Vec<usize>,
    last_use: Vec<Option<usize>>,
    identity: bool,
}

impl ProjectionPlan {
    fn from_positions(input_width: usize, positions: Vec<usize>) -> Result<Self, ExecutionError> {
        let mut last_use = vec![None; input_width];
        for (output_position, input_position) in positions.iter().copied().enumerate() {
            let slot = last_use
                .get_mut(input_position)
                .ok_or(ExecutionError::TypeMismatch)?;
            *slot = Some(output_position);
        }
        let identity =
            positions.len() == input_width && positions.iter().copied().eq(0..input_width);
        Ok(Self {
            positions,
            last_use,
            identity,
        })
    }
}

fn build_projection_plan(
    fields: &[OutputField],
    columns: &[ColumnRef],
) -> Result<ProjectionPlan, ExecutionError> {
    let positions = columns
        .iter()
        .map(|column| find_source_position(fields, column))
        .collect::<Result<Vec<_>, _>>()?;
    ProjectionPlan::from_positions(fields.len(), positions)
}

fn project_execution_row(
    row: ExecutionRow,
    projection: &ProjectionPlan,
) -> Result<ExecutionRow, ExecutionError> {
    if projection.identity {
        return Ok(row);
    }
    if row.values.len() != projection.last_use.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    let ExecutionRow { row_id, values } = row;
    let mut slots = values.into_iter().map(Some).collect::<Vec<_>>();
    let mut projected = Vec::with_capacity(projection.positions.len());
    for (output_position, input_position) in projection.positions.iter().copied().enumerate() {
        let last_use = projection
            .last_use
            .get(input_position)
            .copied()
            .flatten()
            .ok_or(ExecutionError::TypeMismatch)?;
        let slot = slots
            .get_mut(input_position)
            .ok_or(ExecutionError::TypeMismatch)?;
        let value = if output_position == last_use {
            slot.take().ok_or(ExecutionError::TypeMismatch)?
        } else {
            slot.as_ref().cloned().ok_or(ExecutionError::TypeMismatch)?
        };
        projected.push(value);
    }
    Ok(ExecutionRow {
        row_id,
        values: projected,
    })
}

fn project_join_values(
    left: &[ScalarValue],
    right: &[ScalarValue],
    positions: &[usize],
) -> Result<Vec<ScalarValue>, ExecutionError> {
    positions
        .iter()
        .map(|position| {
            if *position < left.len() {
                left.get(*position)
            } else {
                right.get(position - left.len())
            }
            .cloned()
            .ok_or(ExecutionError::TypeMismatch)
        })
        .collect()
}

fn execute_rows(
    plan: &PhysicalPlan,
    storages: &mut [HeapStorage],
) -> Result<ExecutionRows, ExecutionError> {
    match plan {
        PhysicalPlan::SeqScan {
            table_id, columns, ..
        } => {
            let storage = storage_for_table(storages, *table_id)?;
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let rows = storage
                .scan_columns(&column_ids)?
                .into_iter()
                .map(|(row_id, values)| ExecutionRow {
                    row_id: Some(row_id),
                    values,
                })
                .collect();
            Ok(ExecutionRows {
                fields: columns.iter().cloned().map(OutputField::Source).collect(),
                rows,
            })
        }
        PhysicalPlan::IndexScan {
            table_id,
            columns,
            handle,
            key,
            ..
        } => {
            let storage = storage_for_table(storages, *table_id)?;
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let row_ids = storage.btree().lookup(*handle, key)?;
            let rows = row_ids
                .into_iter()
                .map(|row_id| {
                    Ok(ExecutionRow {
                        row_id: Some(row_id),
                        values: storage.read_row_columns(row_id, &column_ids)?,
                    })
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?;
            Ok(ExecutionRows {
                fields: columns.iter().cloned().map(OutputField::Source).collect(),
                rows,
            })
        }
        PhysicalPlan::RangeIndexScan {
            table_id,
            columns,
            handle,
            range,
            ..
        } => {
            let storage = storage_for_table(storages, *table_id)?;
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let row_ids = storage.btree().lookup_range(*handle, range)?;
            let rows = row_ids
                .into_iter()
                .map(|row_id| {
                    Ok(ExecutionRow {
                        row_id: Some(row_id),
                        values: storage.read_row_columns(row_id, &column_ids)?,
                    })
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?;
            Ok(ExecutionRows {
                fields: columns.iter().cloned().map(OutputField::Source).collect(),
                rows,
            })
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            predicate,
            columns,
            ..
        } => {
            let left = execute_rows(left, storages)?;
            let right = execute_rows(right, storages)?;
            let mut joined_fields = left.fields.clone();
            joined_fields.extend(right.fields.clone());
            let output_positions = columns
                .iter()
                .map(|column| find_source_position(&joined_fields, column))
                .collect::<Result<Vec<_>, _>>()?;
            let projection = ProjectionPlan::from_positions(joined_fields.len(), output_positions)?;
            let fields = columns
                .iter()
                .cloned()
                .map(OutputField::Source)
                .collect::<Vec<_>>();
            let bound_predicate = bind_expression(predicate, &joined_fields)?;
            let rows = match find_required_inequality(&bound_predicate, left.fields.len()) {
                Some(inequality) => {
                    let Some(extreme) = required_right_extreme(&inequality, &right.rows)? else {
                        return Ok(ExecutionRows {
                            fields,
                            rows: Vec::new(),
                        });
                    };
                    let potential_left = potential_left_indices(&inequality, &left.rows, extreme)?;
                    if potential_left.is_empty() {
                        return Ok(ExecutionRows {
                            fields,
                            rows: Vec::new(),
                        });
                    }
                    if all_candidate_pairs_match(
                        &inequality,
                        &left.rows,
                        &potential_left,
                        &right.rows,
                    )? {
                        execute_nested_loop_join(
                            &bound_predicate,
                            &left.rows,
                            &right.rows,
                            potential_left.iter().copied(),
                        )?
                    } else {
                        let sorted_left = sorted_non_null_indices(
                            &left.rows,
                            potential_left.iter().copied(),
                            inequality.left_position,
                            inequality.left_name,
                        )?;
                        let sorted_right = sorted_non_null_indices(
                            &right.rows,
                            0..right.rows.len(),
                            inequality.right_position,
                            inequality.right_name,
                        )?;
                        let candidate_pairs = exact_candidate_pair_count(
                            &inequality,
                            &left.rows,
                            &sorted_left,
                            &right.rows,
                            &sorted_right,
                        )?;
                        let strategy = candidate_pairs.map_or(
                            InequalityExecutionStrategy::NestedLoop,
                            |candidate_pairs| {
                                choose_inequality_strategy(
                                    potential_left.len(),
                                    right.rows.len(),
                                    sorted_right.len(),
                                    candidate_pairs,
                                )
                            },
                        );
                        match strategy {
                            InequalityExecutionStrategy::NestedLoop => execute_nested_loop_join(
                                &bound_predicate,
                                &left.rows,
                                &right.rows,
                                potential_left.iter().copied(),
                            )?,
                            InequalityExecutionStrategy::Sweep => execute_inequality_sweep(
                                &bound_predicate,
                                &inequality,
                                &left.rows,
                                &sorted_left,
                                &right.rows,
                                &sorted_right,
                            )?,
                        }
                    }
                }
                None => execute_nested_loop_join(
                    &bound_predicate,
                    &left.rows,
                    &right.rows,
                    0..left.rows.len(),
                )?,
            }
            .into_iter()
            .map(|row| project_execution_row(row, &projection))
            .collect::<Result<Vec<_>, _>>()?;
            Ok(ExecutionRows { fields, rows })
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
            predicate,
            columns,
            ..
        } => {
            if !left_key.data_type.is_compatible_with(&right_key.data_type) {
                return Err(ExecutionError::TypeMismatch);
            }
            let left = execute_rows(left, storages)?;
            let right = execute_rows(right, storages)?;
            let left_key_position = find_source_position(&left.fields, left_key)?;
            let right_key_position = find_source_position(&right.fields, right_key)?;
            let mut buckets = HashMap::<ScalarValue, Vec<usize>>::new();
            for (right_index, right_row) in right.rows.iter().enumerate() {
                let key = hash_join_key(right_row, right_key_position, right_key)?;
                if let Some(key) = key {
                    buckets.entry(key.clone()).or_default().push(right_index);
                }
            }

            let mut joined_fields = left.fields.clone();
            joined_fields.extend(right.fields.clone());
            let output_positions = columns
                .iter()
                .map(|column| find_source_position(&joined_fields, column))
                .collect::<Result<Vec<_>, _>>()?;
            let fields = columns
                .iter()
                .cloned()
                .map(OutputField::Source)
                .collect::<Vec<_>>();
            let bound_predicate = bind_expression(predicate, &joined_fields)?;
            let mut rows = Vec::new();
            for left_row in left.rows {
                let Some(key) = hash_join_key(&left_row, left_key_position, left_key)? else {
                    continue;
                };
                let Some(right_indices) = buckets.get(key) else {
                    continue;
                };
                for right_index in right_indices {
                    let Some(right_row) = right.rows.get(*right_index) else {
                        return Err(ExecutionError::TypeMismatch);
                    };
                    if evaluate_bound_truth(
                        &bound_predicate,
                        EvaluationValues::Joined {
                            left: &left_row.values,
                            right: &right_row.values,
                        },
                    )? == TruthValue::True
                    {
                        let values = project_join_values(
                            &left_row.values,
                            &right_row.values,
                            &output_positions,
                        )?;
                        rows.push(ExecutionRow {
                            row_id: None,
                            values,
                        });
                    }
                }
            }
            Ok(ExecutionRows { fields, rows })
        }
        PhysicalPlan::Filter { input, predicate } => {
            let mut result = execute_rows(input, storages)?;
            let fields = result.fields.clone();
            result.rows = result
                .rows
                .into_iter()
                .filter_map(
                    |row| match evaluate_truth(predicate, &row.values, &fields) {
                        Ok(TruthValue::True) => Some(Ok(row)),
                        Ok(TruthValue::False | TruthValue::Unknown) => None,
                        Err(error) => Some(Err(error)),
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;
            Ok(result)
        }
        PhysicalPlan::Sort { input, keys } => {
            let mut result = execute_rows(input, storages)?;
            let positions = resolve_sort_positions(&result.fields, keys)?;
            validate_sort_values(&result.rows, &positions, keys)?;

            let mut comparison_error = None;
            result.rows.sort_by(|left, right| {
                if comparison_error.is_some() {
                    return Ordering::Equal;
                }
                match compare_sort_rows(left, right, &positions, keys) {
                    Ok(ordering) => ordering,
                    Err(error) => {
                        comparison_error = Some(error);
                        Ordering::Equal
                    }
                }
            });
            if let Some(error) = comparison_error {
                return Err(error);
            }
            Ok(result)
        }
        PhysicalPlan::Project { input, columns } => {
            let input_result = execute_rows(input, storages)?;
            let projection = build_projection_plan(&input_result.fields, columns)?;
            let rows = if projection.identity {
                input_result.rows
            } else {
                input_result
                    .rows
                    .into_iter()
                    .map(|row| project_execution_row(row, &projection))
                    .collect::<Result<Vec<_>, _>>()?
            };
            Ok(ExecutionRows {
                fields: columns.iter().cloned().map(OutputField::Source).collect(),
                rows,
            })
        }
        PhysicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => {
            if let Some(result) = try_execute_direct_counts(input, group_keys, outputs, storages)? {
                Ok(result)
            } else {
                let input = execute_rows(input, storages)?;
                execute_aggregate(input, group_keys, outputs)
            }
        }
        PhysicalPlan::Limit { input, limit } => {
            let mut result = execute_rows(input, storages)?;
            let limit = usize::try_from(*limit).unwrap_or(usize::MAX);
            result.rows.truncate(limit);
            Ok(result)
        }
    }
}

fn hash_join_key<'a>(
    row: &'a ExecutionRow,
    position: usize,
    column: &ColumnRef,
) -> Result<Option<&'a ScalarValue>, ExecutionError> {
    let value = row
        .values
        .get(position)
        .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))?;
    if matches!(value, ScalarValue::Null) {
        return Ok(None);
    }
    if !value.matches_type(&column.data_type) {
        return Err(ExecutionError::TypeMismatch);
    }
    Ok(Some(value))
}

fn resolve_sort_positions(
    fields: &[OutputField],
    keys: &[SortKey],
) -> Result<Vec<usize>, ExecutionError> {
    keys.iter()
        .map(|key| find_source_position(fields, &key.column))
        .collect()
}

fn validate_sort_values(
    rows: &[ExecutionRow],
    positions: &[usize],
    keys: &[SortKey],
) -> Result<(), ExecutionError> {
    for row in rows {
        for (position, key) in positions.iter().zip(keys) {
            let value = row
                .values
                .get(*position)
                .ok_or_else(|| ExecutionError::MissingColumn(key.column.name.clone()))?;
            if !value.matches_type(&key.column.data_type) {
                return Err(ExecutionError::TypeMismatch);
            }
        }
    }
    Ok(())
}

fn compare_sort_rows(
    left: &ExecutionRow,
    right: &ExecutionRow,
    positions: &[usize],
    keys: &[SortKey],
) -> Result<Ordering, ExecutionError> {
    for (position, key) in positions.iter().zip(keys) {
        let left = left
            .values
            .get(*position)
            .ok_or_else(|| ExecutionError::MissingColumn(key.column.name.clone()))?;
        let right = right
            .values
            .get(*position)
            .ok_or_else(|| ExecutionError::MissingColumn(key.column.name.clone()))?;
        let ordering = compare_sort_values(left, right, key)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_sort_values(
    left: &ScalarValue,
    right: &ScalarValue,
    key: &SortKey,
) -> Result<Ordering, ExecutionError> {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => Ok(Ordering::Equal),
        (ScalarValue::Null, _) => Ok(match key.null_order {
            NullOrder::First => Ordering::Less,
            NullOrder::Last => Ordering::Greater,
        }),
        (_, ScalarValue::Null) => Ok(match key.null_order {
            NullOrder::First => Ordering::Greater,
            NullOrder::Last => Ordering::Less,
        }),
        _ => {
            let ordering = compare_values(left, right)?;
            Ok(match key.direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
            })
        }
    }
}

fn find_source_position(
    fields: &[OutputField],
    column: &ColumnRef,
) -> Result<usize, ExecutionError> {
    fields
        .iter()
        .position(|field| {
            field.source_column().is_some_and(|candidate| {
                candidate.binding_id == column.binding_id && candidate.column_id == column.column_id
            })
        })
        .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))
}

#[derive(Debug)]
enum AggregateState {
    Count(u64),
    SumInt(Option<i64>),
    SumUInt(Option<u64>),
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
}

#[derive(Debug)]
struct GroupState {
    key_values: Vec<ScalarValue>,
    aggregate_states: Vec<AggregateState>,
}

#[derive(Debug, Clone, Copy)]
enum AggregateOutputPosition {
    GroupKey(usize),
    Aggregate(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectCountSource {
    All,
    Column(usize),
}

#[derive(Debug, Clone, Copy)]
struct DirectCountOutput<'a> {
    source: DirectCountSource,
    aggregate: &'a AggregateExpr,
}

#[derive(Debug)]
struct DirectCountPlan<'a> {
    table_id: TableId,
    scan_columns: &'a [ColumnRef],
    outputs: Vec<DirectCountOutput<'a>>,
}

fn direct_count_eligibility<'a>(
    input: &'a PhysicalPlan,
    group_keys: &[ColumnRef],
    outputs: &'a [AggregateOutput],
) -> Option<DirectCountPlan<'a>> {
    if !group_keys.is_empty() || outputs.is_empty() {
        return None;
    }
    let PhysicalPlan::SeqScan {
        binding_id,
        table_id,
        columns,
        ..
    } = input
    else {
        return None;
    };
    if columns
        .iter()
        .any(|column| column.binding_id != *binding_id || column.table_id != *table_id)
    {
        return None;
    }

    let mut used_scan_columns = vec![false; columns.len()];
    let mut has_column_count = false;
    let mut direct_outputs = Vec::with_capacity(outputs.len());
    for output in outputs {
        let AggregateOutput::Aggregate(aggregate) = output else {
            return None;
        };
        if aggregate.function != AggregateFunction::Count {
            return None;
        }
        let source = match &aggregate.input {
            AggregateInput::All => DirectCountSource::All,
            AggregateInput::Column(column) => {
                if column.binding_id != *binding_id || column.table_id != *table_id {
                    return None;
                }
                let position = columns.iter().position(|scan_column| {
                    scan_column.binding_id == column.binding_id
                        && scan_column.table_id == column.table_id
                        && scan_column.column_id == column.column_id
                })?;
                used_scan_columns[position] = true;
                has_column_count = true;
                DirectCountSource::Column(position)
            }
        };
        direct_outputs.push(DirectCountOutput { source, aggregate });
    }
    if !has_column_count || used_scan_columns.iter().any(|used| !used) {
        return None;
    }
    Some(DirectCountPlan {
        table_id: *table_id,
        scan_columns: columns,
        outputs: direct_outputs,
    })
}

fn try_execute_direct_counts(
    input: &PhysicalPlan,
    group_keys: &[ColumnRef],
    outputs: &[AggregateOutput],
    storages: &mut [HeapStorage],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(plan) = direct_count_eligibility(input, group_keys, outputs) else {
        return Ok(None);
    };
    let column_ids = plan
        .scan_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let summary = storage_for_table(storages, plan.table_id)?.scan_presence_counts(&column_ids)?;
    let values = materialize_direct_count_values(&plan, &summary)?;
    Ok(Some(ExecutionRows {
        fields: outputs.iter().map(AggregateOutput::output_field).collect(),
        rows: vec![ExecutionRow {
            row_id: None,
            values,
        }],
    }))
}

fn materialize_direct_count_values(
    plan: &DirectCountPlan<'_>,
    summary: &PresenceCountSummary,
) -> Result<Vec<ScalarValue>, ExecutionError> {
    if summary.non_null_counts.len() != plan.scan_columns.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    plan.outputs
        .iter()
        .map(|output| {
            let count = match output.source {
                DirectCountSource::All => summary.live_rows,
                DirectCountSource::Column(position) => *summary
                    .non_null_counts
                    .get(position)
                    .ok_or(ExecutionError::TypeMismatch)?,
            };
            Ok(ScalarValue::UInt64(count_to_sql_u64(
                count,
                output.aggregate,
            )?))
        })
        .collect()
}

fn count_to_sql_u64(count: u128, aggregate: &AggregateExpr) -> Result<u64, ExecutionError> {
    u64::try_from(count).map_err(|_| aggregate_overflow(aggregate))
}

fn execute_aggregate(
    input: ExecutionRows,
    group_keys: &[ColumnRef],
    outputs: &[AggregateOutput],
) -> Result<ExecutionRows, ExecutionError> {
    let aggregates = outputs
        .iter()
        .filter_map(|output| match output {
            AggregateOutput::GroupKey(_) => None,
            AggregateOutput::Aggregate(aggregate) => Some(aggregate),
        })
        .collect::<Vec<_>>();
    for aggregate in &aggregates {
        if matches!(aggregate.input, AggregateInput::All)
            && aggregate.function != AggregateFunction::Count
        {
            return Err(ExecutionError::InvalidAggregateInput {
                function: aggregate.function,
            });
        }
    }
    let group_key_positions = group_keys
        .iter()
        .map(|column| find_source_position(&input.fields, column))
        .collect::<Result<Vec<_>, _>>()?;
    let aggregate_positions = aggregates
        .iter()
        .map(|aggregate| match &aggregate.input {
            AggregateInput::All => Ok(None),
            AggregateInput::Column(column) => find_source_position(&input.fields, column).map(Some),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut aggregate_index = 0;
    let output_positions = outputs
        .iter()
        .map(|output| match output {
            AggregateOutput::GroupKey(column) => group_keys
                .iter()
                .position(|group_key| same_source_column(group_key, column))
                .map(AggregateOutputPosition::GroupKey)
                .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone())),
            AggregateOutput::Aggregate(_) => {
                let position = AggregateOutputPosition::Aggregate(aggregate_index);
                aggregate_index += 1;
                Ok(position)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut group_lookup = HashMap::<Vec<ScalarValue>, usize>::new();
    let mut groups = Vec::<GroupState>::new();
    if group_keys.is_empty() {
        groups.push(new_group_state(Vec::new(), &aggregates)?);
        group_lookup.insert(Vec::new(), 0);
    }

    for row in &input.rows {
        let key_values = group_key_positions
            .iter()
            .zip(group_keys)
            .map(|(position, column)| {
                let value = row
                    .values
                    .get(*position)
                    .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))?;
                if !value.matches_type(&column.data_type) {
                    return Err(ExecutionError::TypeMismatch);
                }
                Ok(value.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let group_index = match group_lookup.get(&key_values).copied() {
            Some(index) => index,
            None => {
                let index = groups.len();
                groups.push(new_group_state(key_values.clone(), &aggregates)?);
                group_lookup.insert(key_values, index);
                index
            }
        };
        let group = groups
            .get_mut(group_index)
            .ok_or(ExecutionError::TypeMismatch)?;
        for ((aggregate, position), state) in aggregates
            .iter()
            .zip(&aggregate_positions)
            .zip(&mut group.aggregate_states)
        {
            let value = match position {
                Some(position) => {
                    let value = row.values.get(*position).ok_or_else(|| {
                        let name = match &aggregate.input {
                            AggregateInput::Column(column) => column.name.clone(),
                            AggregateInput::All => aggregate.output.name.clone(),
                        };
                        ExecutionError::MissingColumn(name)
                    })?;
                    let AggregateInput::Column(column) = &aggregate.input else {
                        return Err(ExecutionError::TypeMismatch);
                    };
                    if !value.matches_type(&column.data_type) {
                        return Err(ExecutionError::TypeMismatch);
                    }
                    Some(value)
                }
                None => None,
            };
            update_aggregate_state(state, aggregate, value)?;
        }
    }

    let rows = groups
        .into_iter()
        .map(|group| {
            let aggregate_values = group
                .aggregate_states
                .into_iter()
                .map(finalize_aggregate_state)
                .collect::<Vec<_>>();
            let values = output_positions
                .iter()
                .map(|position| match position {
                    AggregateOutputPosition::GroupKey(position) => group
                        .key_values
                        .get(*position)
                        .cloned()
                        .ok_or(ExecutionError::TypeMismatch),
                    AggregateOutputPosition::Aggregate(position) => aggregate_values
                        .get(*position)
                        .cloned()
                        .ok_or(ExecutionError::TypeMismatch),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ExecutionRow {
                row_id: None,
                values,
            })
        })
        .collect::<Result<Vec<_>, ExecutionError>>()?;
    Ok(ExecutionRows {
        fields: outputs.iter().map(AggregateOutput::output_field).collect(),
        rows,
    })
}

fn new_group_state(
    key_values: Vec<ScalarValue>,
    aggregates: &[&AggregateExpr],
) -> Result<GroupState, ExecutionError> {
    Ok(GroupState {
        key_values,
        aggregate_states: aggregates
            .iter()
            .map(|aggregate| initial_aggregate_state(aggregate))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn same_source_column(left: &ColumnRef, right: &ColumnRef) -> bool {
    left.binding_id == right.binding_id && left.column_id == right.column_id
}

fn initial_aggregate_state(aggregate: &AggregateExpr) -> Result<AggregateState, ExecutionError> {
    match aggregate.function {
        AggregateFunction::Count => Ok(AggregateState::Count(0)),
        AggregateFunction::Sum => match aggregate.output.data_type.physical {
            netbadb_types::PhysicalType::Int64 => Ok(AggregateState::SumInt(None)),
            netbadb_types::PhysicalType::UInt64 => Ok(AggregateState::SumUInt(None)),
            netbadb_types::PhysicalType::Bool | netbadb_types::PhysicalType::Text => {
                Err(ExecutionError::TypeMismatch)
            }
        },
        AggregateFunction::Min => Ok(AggregateState::Min(None)),
        AggregateFunction::Max => Ok(AggregateState::Max(None)),
    }
}

fn update_aggregate_state(
    state: &mut AggregateState,
    aggregate: &AggregateExpr,
    value: Option<&ScalarValue>,
) -> Result<(), ExecutionError> {
    match state {
        AggregateState::Count(count) => {
            if value.is_none_or(|value| !matches!(value, ScalarValue::Null)) {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| aggregate_overflow(aggregate))?;
            }
        }
        AggregateState::SumInt(sum) => {
            if let Some(value) = value.filter(|value| !matches!(value, ScalarValue::Null)) {
                let ScalarValue::Int64(value) = value else {
                    return Err(ExecutionError::TypeMismatch);
                };
                *sum = Some(match sum {
                    Some(sum) => sum
                        .checked_add(*value)
                        .ok_or_else(|| aggregate_overflow(aggregate))?,
                    None => *value,
                });
            }
        }
        AggregateState::SumUInt(sum) => {
            if let Some(value) = value.filter(|value| !matches!(value, ScalarValue::Null)) {
                let ScalarValue::UInt64(value) = value else {
                    return Err(ExecutionError::TypeMismatch);
                };
                *sum = Some(match sum {
                    Some(sum) => sum
                        .checked_add(*value)
                        .ok_or_else(|| aggregate_overflow(aggregate))?,
                    None => *value,
                });
            }
        }
        AggregateState::Min(current) => {
            if let Some(value) = value.filter(|value| !matches!(value, ScalarValue::Null)) {
                let replace = match current {
                    None => true,
                    Some(current) => compare_values(value, current)? == Ordering::Less,
                };
                if replace {
                    *current = Some(value.clone());
                }
            }
        }
        AggregateState::Max(current) => {
            if let Some(value) = value.filter(|value| !matches!(value, ScalarValue::Null)) {
                let replace = match current {
                    None => true,
                    Some(current) => compare_values(value, current)? == Ordering::Greater,
                };
                if replace {
                    *current = Some(value.clone());
                }
            }
        }
    }
    Ok(())
}

fn aggregate_overflow(aggregate: &AggregateExpr) -> ExecutionError {
    ExecutionError::AggregateOverflow {
        function: aggregate.function,
        output: aggregate.output.name.clone(),
    }
}

fn finalize_aggregate_state(state: AggregateState) -> ScalarValue {
    match state {
        AggregateState::Count(value) => ScalarValue::UInt64(value),
        AggregateState::SumInt(value) => value.map_or(ScalarValue::Null, ScalarValue::Int64),
        AggregateState::SumUInt(value) => value.map_or(ScalarValue::Null, ScalarValue::UInt64),
        AggregateState::Min(value) | AggregateState::Max(value) => {
            value.unwrap_or(ScalarValue::Null)
        }
    }
}

fn storage_for_table(
    storages: &mut [HeapStorage],
    table_id: TableId,
) -> Result<&mut HeapStorage, ExecutionError> {
    storages
        .iter_mut()
        .find(|storage| storage.table().id == table_id)
        .ok_or(ExecutionError::MissingTableStorage(table_id))
}

fn ensure_table(table_id: TableId, storage: &HeapStorage) -> Result<(), ExecutionError> {
    let storage_table_id = storage.table().id;
    if table_id != storage_table_id {
        return Err(ExecutionError::TableMismatch {
            planned: table_id,
            storage: storage_table_id,
        });
    }
    Ok(())
}

fn build_replacements(
    input: &ExecutionRows,
    assignments: &[Assignment],
) -> Result<Vec<(RowId, Vec<ScalarValue>)>, ExecutionError> {
    input
        .rows
        .iter()
        .map(|row| {
            let evaluated = assignments
                .iter()
                .map(|assignment| {
                    let position = find_source_position(&input.fields, &assignment.column)?;
                    let value = evaluate(&assignment.value, &row.values, &input.fields)?;
                    Ok((position, value))
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?;
            let mut replacement = row.values.clone();
            for (position, value) in evaluated {
                replacement[position] = value;
            }
            Ok((
                row.row_id.ok_or(ExecutionError::MissingRowIdentity)?,
                replacement,
            ))
        })
        .collect()
}

struct BoundExpr<'a> {
    kind: BoundExprKind<'a>,
}

enum BoundExprKind<'a> {
    Column {
        position: usize,
        name: &'a str,
    },
    Literal(&'a ScalarValue),
    Binary {
        operator: BinaryOp,
        left: Box<BoundExpr<'a>>,
        right: Box<BoundExpr<'a>>,
    },
    Unary {
        operator: UnaryOp,
        expression: Box<BoundExpr<'a>>,
    },
    IsNull {
        expression: Box<BoundExpr<'a>>,
        negated: bool,
    },
}

fn bind_expression<'a>(
    expression: &'a Expr,
    fields: &[OutputField],
) -> Result<BoundExpr<'a>, ExecutionError> {
    let kind = match &expression.kind {
        ExprKind::Column(column) => BoundExprKind::Column {
            position: find_source_position(fields, column)?,
            name: &column.name,
        },
        ExprKind::Literal(value) => BoundExprKind::Literal(value),
        ExprKind::Binary {
            operator,
            left,
            right,
        } => BoundExprKind::Binary {
            operator: *operator,
            left: Box::new(bind_expression(left, fields)?),
            right: Box::new(bind_expression(right, fields)?),
        },
        ExprKind::Unary {
            operator,
            expression,
        } => BoundExprKind::Unary {
            operator: *operator,
            expression: Box::new(bind_expression(expression, fields)?),
        },
        ExprKind::IsNull {
            expression,
            negated,
        } => BoundExprKind::IsNull {
            expression: Box::new(bind_expression(expression, fields)?),
            negated: *negated,
        },
    };
    Ok(BoundExpr { kind })
}

#[derive(Clone, Copy)]
struct BoundInequality<'a> {
    operator: BinaryOp,
    left_position: usize,
    left_name: &'a str,
    right_position: usize,
    right_name: &'a str,
}

fn find_required_inequality<'a>(
    expression: &'a BoundExpr<'_>,
    left_width: usize,
) -> Option<BoundInequality<'a>> {
    let BoundExprKind::Binary {
        operator,
        left,
        right,
    } = &expression.kind
    else {
        return None;
    };
    if *operator == BinaryOp::And {
        return find_required_inequality(left, left_width)
            .or_else(|| find_required_inequality(right, left_width));
    }
    if !matches!(
        operator,
        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
    ) {
        return None;
    }
    let BoundExprKind::Column {
        position: first_position,
        name: first_name,
    } = &left.kind
    else {
        return None;
    };
    let BoundExprKind::Column {
        position: second_position,
        name: second_name,
    } = &right.kind
    else {
        return None;
    };

    match (
        first_position.checked_sub(left_width),
        second_position.checked_sub(left_width),
    ) {
        (None, Some(right_position)) => Some(BoundInequality {
            operator: *operator,
            left_position: *first_position,
            left_name: first_name,
            right_position,
            right_name: second_name,
        }),
        (Some(right_position), None) => Some(BoundInequality {
            operator: reverse_inequality(*operator),
            left_position: *second_position,
            left_name: second_name,
            right_position,
            right_name: first_name,
        }),
        (None, None) | (Some(_), Some(_)) => None,
    }
}

const fn reverse_inequality(operator: BinaryOp) -> BinaryOp {
    match operator {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => operator,
    }
}

fn required_right_extreme<'a>(
    inequality: &BoundInequality<'_>,
    right_rows: &'a [ExecutionRow],
) -> Result<Option<&'a ScalarValue>, ExecutionError> {
    let kind = match inequality.operator {
        BinaryOp::Gt | BinaryOp::GtEq => ExtremeKind::Minimum,
        BinaryOp::Lt | BinaryOp::LtEq => ExtremeKind::Maximum,
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
            return Err(ExecutionError::TypeMismatch);
        }
    };
    right_extreme(
        right_rows,
        inequality.right_position,
        inequality.right_name,
        kind,
    )
}

#[derive(Clone, Copy)]
enum ExtremeKind {
    Minimum,
    Maximum,
}

fn right_extreme<'a>(
    right_rows: &'a [ExecutionRow],
    position: usize,
    name: &str,
    kind: ExtremeKind,
) -> Result<Option<&'a ScalarValue>, ExecutionError> {
    let mut extreme = None;
    for row in right_rows {
        let value = row
            .values
            .get(position)
            .ok_or_else(|| ExecutionError::MissingColumn(name.to_owned()))?;
        if matches!(value, ScalarValue::Null) {
            continue;
        }
        extreme = match extreme {
            Some(current) => {
                let ordering = compare_values(value, current)?;
                let replace = match kind {
                    ExtremeKind::Minimum => ordering == Ordering::Less,
                    ExtremeKind::Maximum => ordering == Ordering::Greater,
                };
                Some(if replace { value } else { current })
            }
            None => Some(value),
        };
    }
    Ok(extreme)
}

fn inequality_can_match(
    inequality: &BoundInequality<'_>,
    left_row: &ExecutionRow,
    extreme: &ScalarValue,
) -> Result<bool, ExecutionError> {
    let left = left_row
        .values
        .get(inequality.left_position)
        .ok_or_else(|| ExecutionError::MissingColumn(inequality.left_name.to_owned()))?;
    if matches!(left, ScalarValue::Null) {
        return Ok(false);
    }
    let ordering = compare_values(left, extreme)?;
    match inequality.operator {
        BinaryOp::Gt => Ok(ordering == Ordering::Greater),
        BinaryOp::GtEq => Ok(ordering != Ordering::Less),
        BinaryOp::Lt => Ok(ordering == Ordering::Less),
        BinaryOp::LtEq => Ok(ordering != Ordering::Greater),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
            Err(ExecutionError::TypeMismatch)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InequalityExecutionStrategy {
    NestedLoop,
    Sweep,
}

fn potential_left_indices(
    inequality: &BoundInequality<'_>,
    left_rows: &[ExecutionRow],
    required_right_extreme: &ScalarValue,
) -> Result<Vec<usize>, ExecutionError> {
    left_rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            match inequality_can_match(inequality, row, required_right_extreme) {
                Ok(true) => Some(Ok(index)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}

fn all_candidate_pairs_match(
    inequality: &BoundInequality<'_>,
    left_rows: &[ExecutionRow],
    potential_left: &[usize],
    right_rows: &[ExecutionRow],
) -> Result<bool, ExecutionError> {
    let right_kind = match inequality.operator {
        BinaryOp::Gt | BinaryOp::GtEq => ExtremeKind::Maximum,
        BinaryOp::Lt | BinaryOp::LtEq => ExtremeKind::Minimum,
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
            return Err(ExecutionError::TypeMismatch);
        }
    };
    let Some(right_boundary) = right_extreme(
        right_rows,
        inequality.right_position,
        inequality.right_name,
        right_kind,
    )?
    else {
        return Ok(false);
    };
    let left_kind = match inequality.operator {
        BinaryOp::Gt | BinaryOp::GtEq => ExtremeKind::Minimum,
        BinaryOp::Lt | BinaryOp::LtEq => ExtremeKind::Maximum,
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
            return Err(ExecutionError::TypeMismatch);
        }
    };
    let mut limiting_left = None;
    for left_index in potential_left {
        let row = left_rows
            .get(*left_index)
            .ok_or(ExecutionError::TypeMismatch)?;
        let value = row
            .values
            .get(inequality.left_position)
            .ok_or_else(|| ExecutionError::MissingColumn(inequality.left_name.to_owned()))?;
        if matches!(value, ScalarValue::Null) {
            return Ok(false);
        }
        limiting_left = match limiting_left {
            Some(current) => {
                let ordering = compare_values(value, current)?;
                let replace = match left_kind {
                    ExtremeKind::Minimum => ordering == Ordering::Less,
                    ExtremeKind::Maximum => ordering == Ordering::Greater,
                };
                Some(if replace { value } else { current })
            }
            None => Some(value),
        };
    }
    let Some(limiting_left) = limiting_left else {
        return Ok(false);
    };
    let ordering = compare_values(limiting_left, right_boundary)?;
    match inequality.operator {
        BinaryOp::Gt => Ok(ordering == Ordering::Greater),
        BinaryOp::GtEq => Ok(ordering != Ordering::Less),
        BinaryOp::Lt => Ok(ordering == Ordering::Less),
        BinaryOp::LtEq => Ok(ordering != Ordering::Greater),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
            Err(ExecutionError::TypeMismatch)
        }
    }
}

fn sorted_non_null_indices<I>(
    rows: &[ExecutionRow],
    indices: I,
    position: usize,
    name: &str,
) -> Result<Vec<usize>, ExecutionError>
where
    I: IntoIterator<Item = usize>,
{
    let mut indices = indices
        .into_iter()
        .filter_map(|index| {
            let value = match rows.get(index).and_then(|row| row.values.get(position)) {
                Some(value) => value,
                None => return Some(Err(ExecutionError::MissingColumn(name.to_owned()))),
            };
            if matches!(value, ScalarValue::Null) {
                None
            } else {
                Some(Ok(index))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut comparison_error = None;
    indices.sort_by(|left_index, right_index| {
        if comparison_error.is_some() {
            return Ordering::Equal;
        }
        let ordering = rows
            .get(*left_index)
            .and_then(|row| row.values.get(position))
            .zip(
                rows.get(*right_index)
                    .and_then(|row| row.values.get(position)),
            )
            .ok_or_else(|| ExecutionError::MissingColumn(name.to_owned()))
            .and_then(|(left, right)| compare_values(left, right));
        match ordering {
            Ok(Ordering::Equal) => left_index.cmp(right_index),
            Ok(ordering) => ordering,
            Err(error) => {
                comparison_error = Some(error);
                Ordering::Equal
            }
        }
    });
    if let Some(error) = comparison_error {
        return Err(error);
    }
    Ok(indices)
}

fn row_key<'a>(
    rows: &'a [ExecutionRow],
    index: usize,
    position: usize,
    name: &str,
) -> Result<&'a ScalarValue, ExecutionError> {
    rows.get(index)
        .and_then(|row| row.values.get(position))
        .ok_or_else(|| ExecutionError::MissingColumn(name.to_owned()))
}

fn right_precedes_left_boundary(
    operator: BinaryOp,
    right_to_left: Ordering,
) -> Result<bool, ExecutionError> {
    match operator {
        BinaryOp::Gt => Ok(right_to_left == Ordering::Less),
        BinaryOp::GtEq => Ok(right_to_left != Ordering::Greater),
        BinaryOp::Lt => Ok(right_to_left != Ordering::Greater),
        BinaryOp::LtEq => Ok(right_to_left == Ordering::Less),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
            Err(ExecutionError::TypeMismatch)
        }
    }
}

fn exact_candidate_pair_count(
    inequality: &BoundInequality<'_>,
    left_rows: &[ExecutionRow],
    sorted_left: &[usize],
    right_rows: &[ExecutionRow],
    sorted_right: &[usize],
) -> Result<Option<u128>, ExecutionError> {
    let mut right_cursor = 0usize;
    let mut total = 0u128;
    for left_index in sorted_left {
        let left_key = row_key(
            left_rows,
            *left_index,
            inequality.left_position,
            inequality.left_name,
        )?;
        while let Some(right_index) = sorted_right.get(right_cursor) {
            let right_key = row_key(
                right_rows,
                *right_index,
                inequality.right_position,
                inequality.right_name,
            )?;
            let ordering = compare_values(right_key, left_key)?;
            if !right_precedes_left_boundary(inequality.operator, ordering)? {
                break;
            }
            right_cursor = right_cursor.saturating_add(1);
        }
        let candidate_count = match inequality.operator {
            BinaryOp::Gt | BinaryOp::GtEq => right_cursor,
            BinaryOp::Lt | BinaryOp::LtEq => sorted_right.len().saturating_sub(right_cursor),
            BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::NotEq => {
                return Err(ExecutionError::TypeMismatch);
            }
        };
        let Some(candidate_count) = u128::try_from(candidate_count).ok() else {
            return Ok(None);
        };
        let Some(next_total) = total.checked_add(candidate_count) else {
            return Ok(None);
        };
        total = next_total;
    }
    Ok(Some(total))
}

fn ceil_log2(value: usize) -> u128 {
    if value <= 1 {
        0
    } else {
        u128::from(usize::BITS - (value - 1).leading_zeros())
    }
}

fn sort_work(value: usize) -> Option<u128> {
    u128::try_from(value).ok()?.checked_mul(ceil_log2(value))
}

fn choose_inequality_strategy(
    potential_left_count: usize,
    right_total_count: usize,
    right_non_null_count: usize,
    candidate_pairs: u128,
) -> InequalityExecutionStrategy {
    let work = || {
        let left = u128::try_from(potential_left_count).ok()?;
        let right_total = u128::try_from(right_total_count).ok()?;
        let right_non_null = u128::try_from(right_non_null_count).ok()?;
        let nested_work = left.checked_mul(right_total)?;
        let left_sort_work = sort_work(potential_left_count)?;
        let right_sort_work = sort_work(right_non_null_count)?;
        let set_log = ceil_log2(right_non_null_count.checked_add(1)?);
        let ordered_set_work = right_non_null.checked_mul(set_log)?.checked_mul(2)?;
        let sweep_work = candidate_pairs
            .checked_add(left_sort_work)?
            .checked_add(right_sort_work)?
            .checked_add(ordered_set_work)?;
        Some((nested_work, sweep_work))
    };
    match work() {
        Some((nested_work, sweep_work)) if sweep_work < nested_work => {
            InequalityExecutionStrategy::Sweep
        }
        Some(_) | None => InequalityExecutionStrategy::NestedLoop,
    }
}

fn materialize_join_candidate(
    predicate: &BoundExpr<'_>,
    left_row: &ExecutionRow,
    right_row: &ExecutionRow,
) -> Result<Option<ExecutionRow>, ExecutionError> {
    if evaluate_bound_truth(
        predicate,
        EvaluationValues::Joined {
            left: &left_row.values,
            right: &right_row.values,
        },
    )? != TruthValue::True
    {
        return Ok(None);
    }
    let mut values =
        Vec::with_capacity(left_row.values.len().saturating_add(right_row.values.len()));
    values.extend(left_row.values.iter().cloned());
    values.extend(right_row.values.iter().cloned());
    Ok(Some(ExecutionRow {
        row_id: None,
        values,
    }))
}

fn execute_nested_loop_join<I>(
    predicate: &BoundExpr<'_>,
    left_rows: &[ExecutionRow],
    right_rows: &[ExecutionRow],
    left_indices: I,
) -> Result<Vec<ExecutionRow>, ExecutionError>
where
    I: IntoIterator<Item = usize>,
{
    let mut output = Vec::new();
    for left_index in left_indices {
        let left_row = left_rows
            .get(left_index)
            .ok_or(ExecutionError::TypeMismatch)?;
        for right_row in right_rows {
            if let Some(row) = materialize_join_candidate(predicate, left_row, right_row)? {
                output.push(row);
            }
        }
    }
    Ok(output)
}

fn execute_inequality_sweep(
    predicate: &BoundExpr<'_>,
    inequality: &BoundInequality<'_>,
    left_rows: &[ExecutionRow],
    sorted_left: &[usize],
    right_rows: &[ExecutionRow],
    sorted_right: &[usize],
) -> Result<Vec<ExecutionRow>, ExecutionError> {
    let growing_candidates = matches!(inequality.operator, BinaryOp::Gt | BinaryOp::GtEq);
    let mut candidate_rights = if growing_candidates {
        BTreeSet::new()
    } else {
        sorted_right.iter().copied().collect()
    };
    let mut right_cursor = 0usize;
    let mut outputs_by_left = (0..left_rows.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for left_index in sorted_left {
        let left_row = left_rows
            .get(*left_index)
            .ok_or(ExecutionError::TypeMismatch)?;
        let left_key = left_row
            .values
            .get(inequality.left_position)
            .ok_or_else(|| ExecutionError::MissingColumn(inequality.left_name.to_owned()))?;
        while let Some(right_index) = sorted_right.get(right_cursor) {
            let right_key = row_key(
                right_rows,
                *right_index,
                inequality.right_position,
                inequality.right_name,
            )?;
            let ordering = compare_values(right_key, left_key)?;
            if !right_precedes_left_boundary(inequality.operator, ordering)? {
                break;
            }
            if growing_candidates {
                candidate_rights.insert(*right_index);
            } else {
                candidate_rights.remove(right_index);
            }
            right_cursor = right_cursor.saturating_add(1);
        }
        let output = outputs_by_left
            .get_mut(*left_index)
            .ok_or(ExecutionError::TypeMismatch)?;
        for right_index in &candidate_rights {
            let right_row = right_rows
                .get(*right_index)
                .ok_or(ExecutionError::TypeMismatch)?;
            if let Some(row) = materialize_join_candidate(predicate, left_row, right_row)? {
                output.push(row);
            }
        }
    }
    Ok(outputs_by_left.into_iter().flatten().collect())
}

#[derive(Clone, Copy)]
enum EvaluationValues<'a> {
    Contiguous(&'a [ScalarValue]),
    Joined {
        left: &'a [ScalarValue],
        right: &'a [ScalarValue],
    },
}

impl<'a> EvaluationValues<'a> {
    fn get(self, position: usize) -> Option<&'a ScalarValue> {
        match self {
            Self::Contiguous(values) => values.get(position),
            Self::Joined { left, right } => left.get(position).or_else(|| {
                position
                    .checked_sub(left.len())
                    .and_then(|index| right.get(index))
            }),
        }
    }
}

enum EvaluatedScalar<'a> {
    Borrowed(&'a ScalarValue),
    Owned(ScalarValue),
}

impl EvaluatedScalar<'_> {
    fn as_ref(&self) -> &ScalarValue {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }
}

fn evaluate_bound_values<'a>(
    expression: &'a BoundExpr<'_>,
    values: EvaluationValues<'a>,
) -> Result<EvaluatedScalar<'a>, ExecutionError> {
    match &expression.kind {
        BoundExprKind::Column { position, name } => values
            .get(*position)
            .map(EvaluatedScalar::Borrowed)
            .ok_or_else(|| ExecutionError::MissingColumn((*name).to_owned())),
        BoundExprKind::Literal(value) => Ok(EvaluatedScalar::Borrowed(value)),
        BoundExprKind::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_bound_values(left, values)?;
            let right = evaluate_bound_values(right, values)?;
            evaluate_binary_refs(*operator, left.as_ref(), right.as_ref())
                .map(EvaluatedScalar::Owned)
        }
        BoundExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(EvaluatedScalar::Owned(
            evaluate_bound_truth(expression, values)?
                .not()
                .into_scalar(),
        )),
        BoundExprKind::IsNull {
            expression,
            negated,
        } => {
            let value = evaluate_bound_values(expression, values)?;
            let is_null = matches!(value.as_ref(), ScalarValue::Null);
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(if *negated {
                !is_null
            } else {
                is_null
            })))
        }
    }
}

fn evaluate_bound_truth<'a>(
    expression: &'a BoundExpr<'_>,
    values: EvaluationValues<'a>,
) -> Result<TruthValue, ExecutionError> {
    let value = evaluate_bound_values(expression, values)?;
    TruthValue::from_scalar_ref(value.as_ref())
}

fn evaluate_values(
    expression: &Expr,
    values: EvaluationValues<'_>,
    fields: &[OutputField],
) -> Result<ScalarValue, ExecutionError> {
    match &expression.kind {
        ExprKind::Column(column) => {
            let position = find_source_position(fields, column)?;
            values
                .get(position)
                .cloned()
                .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))
        }
        ExprKind::Literal(value) => Ok(value.clone()),
        ExprKind::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_values(left, values, fields)?;
            let right = evaluate_values(right, values, fields)?;
            evaluate_binary(*operator, left, right)
        }
        ExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(evaluate_truth_values(expression, values, fields)?
            .not()
            .into_scalar()),
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            let is_null = matches!(
                evaluate_values(expression, values, fields)?,
                ScalarValue::Null
            );
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
    }
}

fn evaluate(
    expression: &Expr,
    row: &[ScalarValue],
    fields: &[OutputField],
) -> Result<ScalarValue, ExecutionError> {
    evaluate_values(expression, EvaluationValues::Contiguous(row), fields)
}

fn evaluate_truth_values(
    expression: &Expr,
    values: EvaluationValues<'_>,
    fields: &[OutputField],
) -> Result<TruthValue, ExecutionError> {
    TruthValue::from_scalar(evaluate_values(expression, values, fields)?)
}

fn evaluate_truth(
    expression: &Expr,
    row: &[ScalarValue],
    fields: &[OutputField],
) -> Result<TruthValue, ExecutionError> {
    evaluate_truth_values(expression, EvaluationValues::Contiguous(row), fields)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TruthValue {
    True,
    False,
    Unknown,
}

impl TruthValue {
    fn from_scalar(value: ScalarValue) -> Result<Self, ExecutionError> {
        Self::from_scalar_ref(&value)
    }

    fn from_scalar_ref(value: &ScalarValue) -> Result<Self, ExecutionError> {
        match value {
            ScalarValue::Bool(true) => Ok(Self::True),
            ScalarValue::Bool(false) => Ok(Self::False),
            ScalarValue::Null => Ok(Self::Unknown),
            _ => Err(ExecutionError::ExpectedBoolean),
        }
    }

    const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    const fn into_scalar(self) -> ScalarValue {
        match self {
            Self::True => ScalarValue::Bool(true),
            Self::False => ScalarValue::Bool(false),
            Self::Unknown => ScalarValue::Null,
        }
    }
}

fn evaluate_binary(
    operator: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, ExecutionError> {
    evaluate_binary_refs(operator, &left, &right)
}

fn evaluate_binary_refs(
    operator: BinaryOp,
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<ScalarValue, ExecutionError> {
    match operator {
        BinaryOp::And | BinaryOp::Or => {
            let left = TruthValue::from_scalar_ref(left)?;
            let right = TruthValue::from_scalar_ref(right)?;
            let value = if operator == BinaryOp::And {
                left.and(right)
            } else {
                left.or(right)
            };
            Ok(value.into_scalar())
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
            if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
                return Ok(ScalarValue::Null);
            }
            let ordering = compare_values(left, right)?;
            let result = match operator {
                BinaryOp::Eq => ordering == Ordering::Equal,
                BinaryOp::NotEq => ordering != Ordering::Equal,
                BinaryOp::Lt => ordering == Ordering::Less,
                BinaryOp::LtEq => ordering != Ordering::Greater,
                BinaryOp::Gt => ordering == Ordering::Greater,
                BinaryOp::GtEq => ordering != Ordering::Less,
                _ => return Err(ExecutionError::TypeMismatch),
            };
            Ok(ScalarValue::Bool(result))
        }
    }
}

fn compare_values(left: &ScalarValue, right: &ScalarValue) -> Result<Ordering, ExecutionError> {
    match (left, right) {
        (ScalarValue::Bool(left), ScalarValue::Bool(right)) => Ok(left.cmp(right)),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => Ok(left.cmp(right)),
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => Ok(left.cmp(right)),
        (ScalarValue::Text(left), ScalarValue::Text(right)) => Ok(left.cmp(right)),
        _ => Err(ExecutionError::TypeMismatch),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BoundExpr, BoundExprKind, BoundInequality, EvaluatedScalar, EvaluationValues,
        ExecutionError, ExecutionRow, InequalityExecutionStrategy, ProjectionPlan, QueryResult,
        TruthValue, bind_expression, choose_inequality_strategy, count_to_sql_u64,
        direct_count_eligibility, evaluate, evaluate_binary, evaluate_binary_refs,
        evaluate_bound_truth, evaluate_bound_values, evaluate_truth, evaluate_truth_values,
        evaluate_values, exact_candidate_pair_count, execute, execute_inequality_sweep,
        execute_nested_loop_join, execute_rows, execute_with_storages, find_required_inequality,
        inequality_can_match, materialize_direct_count_values, potential_left_indices,
        project_execution_row, required_right_extreme, sorted_non_null_indices,
    };
    use netbadb_planner::{
        IndexAccessPath, PhysicalPlan, TableAccessStatistics, plan, plan_with_statistics,
    };
    use netbadb_rel::{
        AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, BinaryOp, ColumnRef,
        DerivedField, Expr, ExprKind, JoinKind, LogicalPlan, NullOrder, OutputField, SortDirection,
        SortKey, UnaryOp,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_storage::{HeapStorage, IndexStatistics, PresenceCountSummary, TableStatistics};
    use netbadb_types::{
        ColumnId, ExprType, PageId, PhysicalType, RelationBindingId, RowId, ScalarValue,
        SemanticType, TableId,
    };

    fn text_pointer(value: &ScalarValue) -> *const u8 {
        match value {
            ScalarValue::Text(value) => value.as_ptr(),
            _ => panic!("expected Text value"),
        }
    }

    #[test]
    fn projection_identity_moves_the_complete_row_without_rebuilding_values() {
        let text = String::from("payload");
        let pointer = text.as_ptr();
        let row_id = RowId {
            page: PageId(7),
            slot: 2,
            generation: 3,
        };
        let row = ExecutionRow {
            row_id: Some(row_id),
            values: vec![ScalarValue::Text(text)],
        };
        let projection = ProjectionPlan::from_positions(1, vec![0]).expect("identity plan");
        assert!(projection.identity);

        let projected = project_execution_row(row, &projection).expect("project identity");
        assert_eq!(projected.row_id, Some(row_id));
        assert_eq!(projected.values, vec![ScalarValue::Text("payload".into())]);
        assert_eq!(text_pointer(&projected.values[0]), pointer);
    }

    #[test]
    fn projection_moves_unique_reordered_and_subset_text_values() {
        let first = String::from("first");
        let second = String::from("second");
        let first_pointer = first.as_ptr();
        let second_pointer = second.as_ptr();
        let row = ExecutionRow {
            row_id: None,
            values: vec![
                ScalarValue::Int64(9),
                ScalarValue::Text(first),
                ScalarValue::Text(second),
            ],
        };
        let reorder = ProjectionPlan::from_positions(3, vec![2, 1]).expect("reorder plan");
        let projected = project_execution_row(row, &reorder).expect("project reorder");
        assert_eq!(text_pointer(&projected.values[0]), second_pointer);
        assert_eq!(text_pointer(&projected.values[1]), first_pointer);

        let retained = String::from("retained");
        let retained_pointer = retained.as_ptr();
        let subset = ProjectionPlan::from_positions(3, vec![2]).expect("subset plan");
        let projected = project_execution_row(
            ExecutionRow {
                row_id: None,
                values: vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("dropped".into()),
                    ScalarValue::Text(retained),
                ],
            },
            &subset,
        )
        .expect("project subset");
        assert_eq!(text_pointer(&projected.values[0]), retained_pointer);
    }

    #[test]
    fn projection_duplicates_clone_only_before_the_original_last_use() {
        let text = String::from("payload");
        let original_pointer = text.as_ptr();
        let projection = ProjectionPlan::from_positions(1, vec![0, 0]).expect("duplicate plan");
        let projected = project_execution_row(
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Text(text)],
            },
            &projection,
        )
        .expect("project duplicate");
        assert_eq!(
            projected.values,
            vec![
                ScalarValue::Text("payload".into()),
                ScalarValue::Text("payload".into())
            ]
        );
        assert_ne!(text_pointer(&projected.values[0]), original_pointer);
        assert_eq!(text_pointer(&projected.values[1]), original_pointer);
        assert_ne!(
            text_pointer(&projected.values[0]),
            text_pointer(&projected.values[1])
        );
    }

    #[test]
    fn projection_handles_duplicates_empty_output_and_invalid_shapes() {
        let duplicate = ProjectionPlan::from_positions(1, vec![0, 0]).expect("duplicate plan");
        let projected = project_execution_row(
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(7)],
            },
            &duplicate,
        )
        .expect("duplicate integer");
        assert_eq!(
            projected.values,
            vec![ScalarValue::Int64(7), ScalarValue::Int64(7)]
        );

        let row_id = RowId {
            page: PageId(8),
            slot: 1,
            generation: 4,
        };
        let empty = ProjectionPlan::from_positions(1, Vec::new()).expect("empty plan");
        let projected = project_execution_row(
            ExecutionRow {
                row_id: Some(row_id),
                values: vec![ScalarValue::Int64(7)],
            },
            &empty,
        )
        .expect("empty projection");
        assert_eq!(projected.row_id, Some(row_id));
        assert!(projected.values.is_empty());

        assert!(matches!(
            ProjectionPlan::from_positions(1, vec![1]),
            Err(ExecutionError::TypeMismatch)
        ));
        assert!(matches!(
            project_execution_row(
                ExecutionRow {
                    row_id: None,
                    values: Vec::new(),
                },
                &empty,
            ),
            Err(ExecutionError::TypeMismatch)
        ));
    }

    #[test]
    fn executes_filter_projection_and_limit() {
        let table = TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        );
        let path = std::env::temp_dir().join(format!("netbadb-executor-{}", std::process::id()));
        let mut storage = HeapStorage::create(&path, table).expect("create heap");
        storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert");
        storage
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("Lin".into())])
            .expect("insert");
        let id = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(1),
            relation_name: "users".into(),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let name = ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(2),
            relation_name: "users".into(),
            name: "name".into(),
            data_type: SemanticType::physical(PhysicalType::Text),
            nullable: false,
        };
        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Scan {
                        binding_id: RelationBindingId(0),
                        table_id: TableId(1),
                        table_name: "users".into(),
                        columns: vec![id.clone(), name.clone()],
                    }),
                    predicate: Expr {
                        expr_type: ExprType {
                            data_type: SemanticType::physical(PhysicalType::Bool),
                            nullable: false,
                        },
                        kind: ExprKind::Binary {
                            operator: BinaryOp::Gt,
                            left: Box::new(Expr {
                                expr_type: ExprType {
                                    data_type: SemanticType::physical(PhysicalType::Int64),
                                    nullable: false,
                                },
                                kind: ExprKind::Column(id),
                            }),
                            right: Box::new(Expr {
                                expr_type: ExprType {
                                    data_type: SemanticType::physical(PhysicalType::Int64),
                                    nullable: false,
                                },
                                kind: ExprKind::Literal(ScalarValue::Int64(1)),
                            }),
                        },
                    },
                }),
                columns: vec![name],
            }),
            limit: 1,
        };
        let result: QueryResult = execute(&plan(&logical), &mut storage).expect("execute");
        assert_eq!(result.rows, vec![vec![ScalarValue::Text("Lin".into())]]);
        storage.close().expect("close storage");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn point_index_scan_fetches_complete_rows_and_retains_residual_filtering() {
        let table = TableDef::new(
            TableId(101),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "team_id",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(
                    ColumnId(3),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
            ],
        );
        let path = std::env::temp_dir().join(format!(
            "netbadb-executor-index-scan-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut storage = HeapStorage::create(&path, table).expect("create indexed heap");
        let first = storage
            .insert(&[
                ScalarValue::Int64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Bool(true),
            ])
            .expect("insert first duplicate");
        storage
            .insert(&[
                ScalarValue::Int64(2),
                ScalarValue::UInt64(10),
                ScalarValue::Bool(false),
            ])
            .expect("insert second duplicate");
        storage
            .insert(&[
                ScalarValue::Int64(3),
                ScalarValue::UInt64(20),
                ScalarValue::Bool(true),
            ])
            .expect("insert other key");
        let definition = storage
            .create_index(ColumnId(2))
            .expect("create team index");

        let columns = [
            (1, "id", PhysicalType::Int64),
            (2, "team_id", PhysicalType::UInt64),
            (3, "active", PhysicalType::Bool),
        ]
        .map(|(id, name, physical)| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(101),
            column_id: ColumnId(id),
            relation_name: "users".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable: false,
        })
        .to_vec();
        let scan = PhysicalPlan::IndexScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(101),
            table_name: "users".into(),
            columns: columns.clone(),
            index_column: columns[1].clone(),
            handle: definition.handle,
            key: ScalarValue::UInt64(10),
        };

        let count_id = AggregateOutput::Aggregate(AggregateExpr {
            function: AggregateFunction::Count,
            input: AggregateInput::Column(columns[0].clone()),
            output: DerivedField {
                name: "COUNT(id)".into(),
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
        });
        assert!(direct_count_eligibility(&scan, &[], std::slice::from_ref(&count_id)).is_none());

        let team_expression = || Expr {
            kind: ExprKind::Column(columns[1].clone()),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
        };
        let bound = |operator, value| Expr {
            kind: ExprKind::Binary {
                operator,
                left: Box::new(team_expression()),
                right: Box::new(Expr {
                    kind: ExprKind::Literal(ScalarValue::UInt64(value)),
                    expr_type: ExprType {
                        data_type: SemanticType::physical(PhysicalType::UInt64),
                        nullable: false,
                    },
                }),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let range_logical = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(101),
                table_name: "users".into(),
                columns: columns.clone(),
            }),
            predicate: Expr {
                kind: ExprKind::Binary {
                    operator: BinaryOp::And,
                    left: Box::new(bound(BinaryOp::GtEq, 10)),
                    right: Box::new(bound(BinaryOp::Lt, 11)),
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
        };
        let range_plan = plan_with_statistics(
            &range_logical,
            &[TableAccessStatistics {
                table_id: TableId(101),
                statistics: Some(TableStatistics {
                    row_count: 10_000,
                    managed_page_count: 100,
                }),
            }],
            &[IndexAccessPath {
                table_id: TableId(101),
                column_id: ColumnId(2),
                handle: definition.handle,
                statistics: Some(IndexStatistics {
                    distinct_non_null_keys: 10_000,
                    null_count: 0,
                    tree_height: 2,
                }),
            }],
        );
        let PhysicalPlan::Filter { input, .. } = &range_plan else {
            panic!("bounded range must retain its residual filter");
        };
        assert!(matches!(
            input.as_ref(),
            PhysicalPlan::RangeIndexScan { .. }
        ));
        assert!(direct_count_eligibility(input, &[], std::slice::from_ref(&count_id)).is_none());

        let candidates =
            execute_rows(&scan, std::slice::from_mut(&mut storage)).expect("execute point lookup");
        assert_eq!(candidates.rows.len(), 2);
        assert!(candidates.rows.iter().all(|row| row.row_id.is_some()));
        assert_eq!(candidates.rows[0].values.len(), 3);

        let active = Expr {
            kind: ExprKind::Column(columns[2].clone()),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let predicate = Expr {
            kind: ExprKind::Binary {
                operator: BinaryOp::Eq,
                left: Box::new(active),
                right: Box::new(Expr {
                    kind: ExprKind::Literal(ScalarValue::Bool(true)),
                    expr_type: ExprType {
                        data_type: SemanticType::physical(PhysicalType::Bool),
                        nullable: false,
                    },
                }),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let result = execute(
            &PhysicalPlan::Filter {
                input: Box::new(scan.clone()),
                predicate,
            },
            &mut storage,
        )
        .expect("execute residual filter");
        assert_eq!(
            result.rows,
            vec![vec![
                ScalarValue::Int64(1),
                ScalarValue::UInt64(10),
                ScalarValue::Bool(true),
            ]]
        );

        storage.delete(first).expect("delete indexed row");
        storage
            .btree()
            .insert(definition.handle, ScalarValue::UInt64(10), first)
            .expect("inject deleted locator");
        assert!(matches!(
            execute(&scan, &mut storage),
            Err(ExecutionError::Storage(
                netbadb_storage::StorageError::RowDeleted { .. }
            ))
        ));

        storage.close().expect("close indexed heap");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn implements_complete_three_valued_truth_tables() {
        use TruthValue::{False, True, Unknown};
        let values = [True, False, Unknown];
        let expected_and = [
            [True, False, Unknown],
            [False, False, False],
            [Unknown, False, Unknown],
        ];
        let expected_or = [
            [True, True, True],
            [True, False, Unknown],
            [True, Unknown, Unknown],
        ];
        for (left_index, left) in values.iter().copied().enumerate() {
            for (right_index, right) in values.iter().copied().enumerate() {
                assert_eq!(left.and(right), expected_and[left_index][right_index]);
                assert_eq!(left.or(right), expected_or[left_index][right_index]);
            }
        }
        assert_eq!(True.not(), False);
        assert_eq!(False.not(), True);
        assert_eq!(Unknown.not(), Unknown);
    }

    #[test]
    fn every_comparison_with_null_is_unknown() {
        for operator in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            assert_eq!(
                evaluate_binary(operator, ScalarValue::Null, ScalarValue::Int64(1))
                    .expect("comparison"),
                ScalarValue::Null
            );
            assert_eq!(
                evaluate_binary(operator, ScalarValue::Null, ScalarValue::Null)
                    .expect("comparison"),
                ScalarValue::Null
            );
        }
    }

    #[test]
    fn owned_and_reference_scalar_semantics_are_equivalent() {
        let comparison_pairs = [
            (ScalarValue::Bool(false), ScalarValue::Bool(true)),
            (ScalarValue::Int64(-1), ScalarValue::Int64(2)),
            (ScalarValue::UInt64(1), ScalarValue::UInt64(2)),
            (
                ScalarValue::Text("alpha".into()),
                ScalarValue::Text("beta".into()),
            ),
            (ScalarValue::Null, ScalarValue::Null),
            (ScalarValue::Null, ScalarValue::Text("value".into())),
        ];
        for operator in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            for (left, right) in &comparison_pairs {
                assert_eq!(
                    evaluate_binary(operator, left.clone(), right.clone())
                        .expect("owned comparison"),
                    evaluate_binary_refs(operator, left, right).expect("reference comparison")
                );
            }
        }

        let truth_scalars = [
            ScalarValue::Bool(true),
            ScalarValue::Bool(false),
            ScalarValue::Null,
        ];
        for operator in [BinaryOp::And, BinaryOp::Or] {
            for left in &truth_scalars {
                for right in &truth_scalars {
                    assert_eq!(
                        evaluate_binary(operator, left.clone(), right.clone())
                            .expect("owned truth operation"),
                        evaluate_binary_refs(operator, left, right)
                            .expect("reference truth operation")
                    );
                }
            }
        }
        for value in &truth_scalars {
            assert_eq!(
                TruthValue::from_scalar(value.clone()).expect("owned truth conversion"),
                TruthValue::from_scalar_ref(value).expect("reference truth conversion")
            );
        }
        for invalid in [
            ScalarValue::Int64(1),
            ScalarValue::UInt64(1),
            ScalarValue::Text("true".into()),
        ] {
            assert!(matches!(
                TruthValue::from_scalar(invalid.clone()),
                Err(ExecutionError::ExpectedBoolean)
            ));
            assert!(matches!(
                TruthValue::from_scalar_ref(&invalid),
                Err(ExecutionError::ExpectedBoolean)
            ));
        }
    }

    #[test]
    fn required_inequality_extraction_is_normalized_necessary_and_deterministic() {
        fn column(position: usize, name: &'static str) -> BoundExpr<'static> {
            BoundExpr {
                kind: BoundExprKind::Column { position, name },
            }
        }

        fn binary<'a>(
            operator: BinaryOp,
            left: BoundExpr<'a>,
            right: BoundExpr<'a>,
        ) -> BoundExpr<'a> {
            BoundExpr {
                kind: BoundExprKind::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            }
        }

        let cases = [
            (BinaryOp::Gt, 0, "left", 2, "right", BinaryOp::Gt),
            (BinaryOp::Lt, 2, "right", 0, "left", BinaryOp::Gt),
            (BinaryOp::GtEq, 0, "left", 2, "right", BinaryOp::GtEq),
            (BinaryOp::LtEq, 2, "right", 0, "left", BinaryOp::GtEq),
            (BinaryOp::Lt, 0, "left", 2, "right", BinaryOp::Lt),
            (BinaryOp::Gt, 2, "right", 0, "left", BinaryOp::Lt),
            (BinaryOp::LtEq, 0, "left", 2, "right", BinaryOp::LtEq),
            (BinaryOp::GtEq, 2, "right", 0, "left", BinaryOp::LtEq),
        ];
        for (operator, first, first_name, second, second_name, normalized) in cases {
            let expression = binary(
                operator,
                column(first, first_name),
                column(second, second_name),
            );
            let inequality = find_required_inequality(&expression, 2).expect("extract inequality");
            assert_eq!(inequality.operator, normalized);
            assert_eq!(inequality.left_position, 0);
            assert_eq!(inequality.left_name, "left");
            assert_eq!(inequality.right_position, 0);
            assert_eq!(inequality.right_name, "right");
        }

        let invalid = || binary(BinaryOp::Eq, column(0, "left"), column(2, "right"));
        let eligible = || binary(BinaryOp::Gt, column(0, "first"), column(2, "right_first"));
        for expression in [
            binary(BinaryOp::And, eligible(), invalid()),
            binary(BinaryOp::And, invalid(), eligible()),
            binary(
                BinaryOp::And,
                invalid(),
                binary(BinaryOp::And, invalid(), eligible()),
            ),
        ] {
            assert_eq!(
                find_required_inequality(&expression, 2)
                    .expect("extract nested inequality")
                    .operator,
                BinaryOp::Gt
            );
        }

        let first = binary(BinaryOp::Lt, column(1, "first"), column(3, "right_first"));
        let second = eligible();
        let multiple = binary(BinaryOp::And, first, second);
        let extracted = find_required_inequality(&multiple, 2).expect("extract first inequality");
        assert_eq!(extracted.operator, BinaryOp::Lt);
        assert_eq!(extracted.left_position, 1);
        assert_eq!(extracted.right_position, 1);

        let literal = ScalarValue::Int64(1);
        for expression in [
            binary(BinaryOp::Or, eligible(), invalid()),
            BoundExpr {
                kind: BoundExprKind::Unary {
                    operator: UnaryOp::Not,
                    expression: Box::new(eligible()),
                },
            },
            binary(BinaryOp::Gt, column(0, "left_a"), column(1, "left_b")),
            binary(BinaryOp::Lt, column(2, "right_a"), column(3, "right_b")),
            binary(BinaryOp::Eq, column(0, "left"), column(2, "right")),
            binary(BinaryOp::NotEq, column(0, "left"), column(2, "right")),
            binary(
                BinaryOp::Gt,
                column(0, "left"),
                BoundExpr {
                    kind: BoundExprKind::Literal(&literal),
                },
            ),
        ] {
            assert!(find_required_inequality(&expression, 2).is_none());
        }
    }

    #[test]
    fn right_extremes_are_borrowed_typed_and_null_safe() {
        fn inequality(operator: BinaryOp) -> BoundInequality<'static> {
            BoundInequality {
                operator,
                left_position: 0,
                left_name: "left_key",
                right_position: 0,
                right_name: "right_key",
            }
        }

        let cases = [
            (
                vec![
                    ScalarValue::Null,
                    ScalarValue::Int64(5),
                    ScalarValue::Int64(1),
                    ScalarValue::Int64(1),
                ],
                2,
                1,
            ),
            (
                vec![
                    ScalarValue::Null,
                    ScalarValue::UInt64(5),
                    ScalarValue::UInt64(1),
                ],
                2,
                1,
            ),
            (
                vec![
                    ScalarValue::Null,
                    ScalarValue::Bool(true),
                    ScalarValue::Bool(false),
                ],
                2,
                1,
            ),
            (
                vec![
                    ScalarValue::Null,
                    ScalarValue::Text("zulu".into()),
                    ScalarValue::Text("alpha".into()),
                ],
                2,
                1,
            ),
        ];
        for (values, minimum, maximum) in cases {
            let rows = values
                .into_iter()
                .map(|value| ExecutionRow {
                    row_id: None,
                    values: vec![value],
                })
                .collect::<Vec<_>>();
            let min = required_right_extreme(&inequality(BinaryOp::Gt), &rows)
                .expect("minimum")
                .expect("non-null minimum");
            let max = required_right_extreme(&inequality(BinaryOp::Lt), &rows)
                .expect("maximum")
                .expect("non-null maximum");
            assert!(std::ptr::eq(min, &rows[minimum].values[0]));
            assert!(std::ptr::eq(max, &rows[maximum].values[0]));
        }

        let all_null = [ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Null],
        }];
        assert!(
            required_right_extreme(&inequality(BinaryOp::Gt), &all_null)
                .expect("all-null extreme")
                .is_none()
        );
        assert!(
            required_right_extreme(&inequality(BinaryOp::Lt), &[])
                .expect("empty extreme")
                .is_none()
        );
        assert!(matches!(
            required_right_extreme(
                &inequality(BinaryOp::Gt),
                &[ExecutionRow {
                    row_id: None,
                    values: Vec::new(),
                }]
            ),
            Err(ExecutionError::MissingColumn(name)) if name == "right_key"
        ));
        assert!(matches!(
            required_right_extreme(
                &inequality(BinaryOp::Gt),
                &[
                    ExecutionRow {
                        row_id: None,
                        values: vec![ScalarValue::Int64(1)],
                    },
                    ExecutionRow {
                        row_id: None,
                        values: vec![ScalarValue::Text("one".into())],
                    },
                ]
            ),
            Err(ExecutionError::TypeMismatch)
        ));
    }

    #[test]
    fn inequality_existence_checks_strict_boundaries_nulls_and_errors() {
        fn inequality(operator: BinaryOp) -> BoundInequality<'static> {
            BoundInequality {
                operator,
                left_position: 0,
                left_name: "left_key",
                right_position: 0,
                right_name: "right_key",
            }
        }

        let equal = ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Int64(5)],
        };
        for (operator, expected) in [
            (BinaryOp::Gt, false),
            (BinaryOp::GtEq, true),
            (BinaryOp::Lt, false),
            (BinaryOp::LtEq, true),
        ] {
            assert_eq!(
                inequality_can_match(&inequality(operator), &equal, &ScalarValue::Int64(5))
                    .expect("boundary check"),
                expected
            );
        }
        assert!(
            inequality_can_match(&inequality(BinaryOp::Gt), &equal, &ScalarValue::Int64(4))
                .expect("greater check")
        );
        assert!(
            inequality_can_match(&inequality(BinaryOp::Lt), &equal, &ScalarValue::Int64(6))
                .expect("less check")
        );
        assert!(
            !inequality_can_match(
                &inequality(BinaryOp::Gt),
                &ExecutionRow {
                    row_id: None,
                    values: vec![ScalarValue::Null],
                },
                &ScalarValue::Int64(1)
            )
            .expect("null check")
        );
        assert!(matches!(
            inequality_can_match(
                &inequality(BinaryOp::Gt),
                &ExecutionRow {
                    row_id: None,
                    values: Vec::new(),
                },
                &ScalarValue::Int64(1)
            ),
            Err(ExecutionError::MissingColumn(name)) if name == "left_key"
        ));
        assert!(matches!(
            inequality_can_match(
                &inequality(BinaryOp::Gt),
                &equal,
                &ScalarValue::Text("five".into())
            ),
            Err(ExecutionError::TypeMismatch)
        ));
    }

    #[test]
    fn exact_candidate_counts_cover_all_operators_types_and_duplicate_boundaries() {
        fn rows(values: Vec<ScalarValue>) -> Vec<ExecutionRow> {
            values
                .into_iter()
                .map(|value| ExecutionRow {
                    row_id: None,
                    values: vec![value],
                })
                .collect()
        }

        fn inequality(operator: BinaryOp) -> BoundInequality<'static> {
            BoundInequality {
                operator,
                left_position: 0,
                left_name: "left_key",
                right_position: 0,
                right_name: "right_key",
            }
        }

        let ordered_cases = [
            (
                rows(vec![ScalarValue::UInt64(1), ScalarValue::UInt64(2)]),
                rows(vec![
                    ScalarValue::UInt64(0),
                    ScalarValue::UInt64(1),
                    ScalarValue::UInt64(2),
                ]),
                [3, 5, 1, 3],
            ),
            (
                rows(vec![ScalarValue::Bool(false), ScalarValue::Bool(true)]),
                rows(vec![ScalarValue::Bool(false), ScalarValue::Bool(true)]),
                [1, 3, 1, 3],
            ),
            (
                rows(vec![
                    ScalarValue::Text("b".into()),
                    ScalarValue::Text("c".into()),
                ]),
                rows(vec![
                    ScalarValue::Text("a".into()),
                    ScalarValue::Text("b".into()),
                    ScalarValue::Text("c".into()),
                ]),
                [3, 5, 1, 3],
            ),
        ];
        for (left, right, expected) in ordered_cases {
            for (index, operator) in [BinaryOp::Gt, BinaryOp::GtEq, BinaryOp::Lt, BinaryOp::LtEq]
                .into_iter()
                .enumerate()
            {
                let inequality = inequality(operator);
                let sorted_left = sorted_non_null_indices(
                    &left,
                    0..left.len(),
                    inequality.left_position,
                    inequality.left_name,
                )
                .expect("sort typed left keys");
                let sorted_right = sorted_non_null_indices(
                    &right,
                    0..right.len(),
                    inequality.right_position,
                    inequality.right_name,
                )
                .expect("sort typed right keys");
                assert_eq!(
                    exact_candidate_pair_count(
                        &inequality,
                        &left,
                        &sorted_left,
                        &right,
                        &sorted_right,
                    )
                    .expect("count typed candidates"),
                    Some(expected[index])
                );
            }
        }

        let left = rows(vec![ScalarValue::Int64(5)]);
        let right = rows(vec![
            ScalarValue::Int64(5),
            ScalarValue::Int64(4),
            ScalarValue::Int64(5),
            ScalarValue::Int64(6),
            ScalarValue::Null,
        ]);
        for (operator, expected) in [
            (BinaryOp::Gt, 1),
            (BinaryOp::GtEq, 3),
            (BinaryOp::Lt, 1),
            (BinaryOp::LtEq, 3),
        ] {
            let inequality = inequality(operator);
            let sorted_left = sorted_non_null_indices(
                &left,
                0..left.len(),
                inequality.left_position,
                inequality.left_name,
            )
            .expect("sort duplicate left keys");
            let sorted_right = sorted_non_null_indices(
                &right,
                0..right.len(),
                inequality.right_position,
                inequality.right_name,
            )
            .expect("sort duplicate right keys");
            assert_eq!(
                exact_candidate_pair_count(
                    &inequality,
                    &left,
                    &sorted_left,
                    &right,
                    &sorted_right,
                )
                .expect("count duplicate candidates"),
                Some(expected)
            );
        }

        assert!(matches!(
            sorted_non_null_indices(
                &[ExecutionRow {
                    row_id: None,
                    values: Vec::new(),
                }],
                0..1,
                0,
                "missing",
            ),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));
        assert!(matches!(
            sorted_non_null_indices(
                &rows(vec![ScalarValue::Int64(1), ScalarValue::Text("one".into()),]),
                0..2,
                0,
                "mixed",
            ),
            Err(ExecutionError::TypeMismatch)
        ));
    }

    #[test]
    fn exact_integer_work_model_selects_partial_and_rejects_dense_or_full_sweeps() {
        assert_eq!(
            choose_inequality_strategy(499, 1_000, 1_000, 124_750),
            InequalityExecutionStrategy::Sweep
        );
        assert_eq!(
            choose_inequality_strategy(1_000, 1_000, 1_000, 968_625),
            InequalityExecutionStrategy::NestedLoop
        );
        assert_eq!(
            choose_inequality_strategy(1_000, 1_000, 1_000, 1_000_000),
            InequalityExecutionStrategy::NestedLoop
        );
        assert_eq!(
            choose_inequality_strategy(0, 0, 0, 0),
            InequalityExecutionStrategy::NestedLoop
        );
        assert_eq!(
            choose_inequality_strategy(1, 1, 1, u128::MAX),
            InequalityExecutionStrategy::NestedLoop
        );
    }

    #[test]
    fn sweep_preserves_nested_order_duplicate_identity_and_nullable_residual_truth() {
        fn column(position: usize, name: &'static str) -> BoundExpr<'static> {
            BoundExpr {
                kind: BoundExprKind::Column { position, name },
            }
        }

        fn binary(
            operator: BinaryOp,
            left: BoundExpr<'static>,
            right: BoundExpr<'static>,
        ) -> BoundExpr<'static> {
            BoundExpr {
                kind: BoundExprKind::Binary {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            }
        }

        let inequality = BoundInequality {
            operator: BinaryOp::Gt,
            left_position: 0,
            left_name: "left_key",
            right_position: 0,
            right_name: "right_key",
        };
        let predicate = binary(
            BinaryOp::And,
            binary(BinaryOp::Gt, column(0, "left_key"), column(2, "right_key")),
            binary(
                BinaryOp::Eq,
                column(1, "left_flag"),
                column(3, "right_flag"),
            ),
        );
        let left = vec![
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(7), ScalarValue::Bool(true)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(1), ScalarValue::Bool(false)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(5), ScalarValue::Bool(true)],
            },
        ];
        let right = vec![
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(4), ScalarValue::Bool(true)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(0), ScalarValue::Bool(false)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(6), ScalarValue::Bool(true)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(2), ScalarValue::Null],
            },
        ];
        let sorted_left = sorted_non_null_indices(&left, 0..left.len(), 0, "left_key")
            .expect("sort unsorted left input");
        let sorted_right = sorted_non_null_indices(&right, 0..right.len(), 0, "right_key")
            .expect("sort unsorted right input");
        let swept = execute_inequality_sweep(
            &predicate,
            &inequality,
            &left,
            &sorted_left,
            &right,
            &sorted_right,
        )
        .expect("sweep residual candidates");
        let nested = execute_nested_loop_join(&predicate, &left, &right, 0..left.len())
            .expect("reference nested loop");
        assert_eq!(
            swept
                .iter()
                .map(|row| (&row.row_id, &row.values))
                .collect::<Vec<_>>(),
            nested
                .iter()
                .map(|row| (&row.row_id, &row.values))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            swept
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    ScalarValue::Int64(7),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(4),
                    ScalarValue::Bool(true),
                ],
                vec![
                    ScalarValue::Int64(7),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(6),
                    ScalarValue::Bool(true),
                ],
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Bool(false),
                    ScalarValue::Int64(0),
                    ScalarValue::Bool(false),
                ],
                vec![
                    ScalarValue::Int64(5),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(4),
                    ScalarValue::Bool(true),
                ],
            ]
        );

        let duplicate_right = [5, 4, 5, 6]
            .into_iter()
            .enumerate()
            .map(|(index, key)| ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(key), ScalarValue::UInt64(index as u64)],
            })
            .collect::<Vec<_>>();
        let duplicate_left = vec![ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Int64(5)],
        }];
        for (operator, expected_right_ids) in [
            (BinaryOp::Gt, vec![1]),
            (BinaryOp::GtEq, vec![0, 1, 2]),
            (BinaryOp::Lt, vec![3]),
            (BinaryOp::LtEq, vec![0, 2, 3]),
        ] {
            let inequality = BoundInequality {
                operator,
                left_position: 0,
                left_name: "left_key",
                right_position: 0,
                right_name: "right_key",
            };
            let predicate = binary(operator, column(0, "left_key"), column(1, "right_key"));
            let sorted_left = vec![0];
            let sorted_right =
                sorted_non_null_indices(&duplicate_right, 0..duplicate_right.len(), 0, "right_key")
                    .expect("sort duplicate right input");
            let output = execute_inequality_sweep(
                &predicate,
                &inequality,
                &duplicate_left,
                &sorted_left,
                &duplicate_right,
                &sorted_right,
            )
            .expect("sweep duplicate boundary");
            assert_eq!(
                output
                    .iter()
                    .map(|row| match row.values.get(2) {
                        Some(ScalarValue::UInt64(value)) => *value,
                        _ => panic!("right identity must be UInt64"),
                    })
                    .collect::<Vec<_>>(),
                expected_right_ids
            );
        }
    }

    #[test]
    fn text_partial_range_chooses_sweep_and_matches_nested_loop_exactly() {
        fn text_row(value: usize) -> ExecutionRow {
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Text(format!("K-{value:03}"))],
            }
        }

        let left = (0..128).rev().map(text_row).collect::<Vec<_>>();
        let right = (64..192).rev().map(text_row).collect::<Vec<_>>();
        let inequality = BoundInequality {
            operator: BinaryOp::Gt,
            left_position: 0,
            left_name: "left_key",
            right_position: 0,
            right_name: "right_key",
        };
        let predicate = BoundExpr {
            kind: BoundExprKind::Binary {
                operator: BinaryOp::Gt,
                left: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 0,
                        name: "left_key",
                    },
                }),
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 1,
                        name: "right_key",
                    },
                }),
            },
        };
        let extreme = required_right_extreme(&inequality, &right)
            .expect("Text right minimum")
            .expect("non-empty Text right");
        let potential =
            potential_left_indices(&inequality, &left, extreme).expect("Text potential probes");
        let sorted_left = sorted_non_null_indices(&left, potential.iter().copied(), 0, "left_key")
            .expect("sort Text left keys");
        let sorted_right = sorted_non_null_indices(&right, 0..right.len(), 0, "right_key")
            .expect("sort Text right keys");
        let candidates =
            exact_candidate_pair_count(&inequality, &left, &sorted_left, &right, &sorted_right)
                .expect("count Text candidates")
                .expect("Text candidate count fits u128");
        assert_eq!(candidates, 2_016);
        assert_eq!(
            choose_inequality_strategy(
                potential.len(),
                right.len(),
                sorted_right.len(),
                candidates,
            ),
            InequalityExecutionStrategy::Sweep
        );
        let swept = execute_inequality_sweep(
            &predicate,
            &inequality,
            &left,
            &sorted_left,
            &right,
            &sorted_right,
        )
        .expect("execute Text sweep");
        let nested = execute_nested_loop_join(&predicate, &left, &right, potential.iter().copied())
            .expect("execute Text nested reference");
        assert_eq!(
            swept
                .iter()
                .map(|row| (&row.row_id, &row.values))
                .collect::<Vec<_>>(),
            nested
                .iter()
                .map(|row| (&row.row_id, &row.values))
                .collect::<Vec<_>>()
        );
        assert_eq!(swept.len(), 2_016);
        assert_eq!(
            swept.first().map(|row| &row.values),
            Some(&vec![
                ScalarValue::Text("K-127".into()),
                ScalarValue::Text("K-126".into()),
            ])
        );
        assert_eq!(
            swept.last().map(|row| &row.values),
            Some(&vec![
                ScalarValue::Text("K-065".into()),
                ScalarValue::Text("K-064".into()),
            ])
        );
    }

    #[test]
    fn joined_and_contiguous_evaluation_are_equivalent() {
        fn column_expr(column: &ColumnRef) -> Expr {
            Expr {
                kind: ExprKind::Column(column.clone()),
                expr_type: ExprType {
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                },
            }
        }

        fn literal(value: ScalarValue, physical: PhysicalType) -> Expr {
            Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(physical),
                    nullable: matches!(value, ScalarValue::Null),
                },
                kind: ExprKind::Literal(value),
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

        fn not(expression: Expr) -> Expr {
            Expr {
                kind: ExprKind::Unary {
                    operator: UnaryOp::Not,
                    expression: Box::new(expression),
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: true,
                },
            }
        }

        fn is_null(expression: Expr, negated: bool) -> Expr {
            Expr {
                kind: ExprKind::IsNull {
                    expression: Box::new(expression),
                    negated,
                },
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            }
        }

        let column = |binding_id: u32,
                      column_id: u32,
                      name: &str,
                      physical: PhysicalType,
                      nullable: bool| ColumnRef {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(u64::from(binding_id)),
            column_id: ColumnId(column_id),
            relation_name: format!("side_{binding_id}"),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable,
        };
        let left_columns = [
            column(1, 1, "flag", PhysicalType::Bool, false),
            column(1, 2, "signed", PhysicalType::Int64, false),
            column(1, 3, "unsigned", PhysicalType::UInt64, false),
            column(1, 4, "text", PhysicalType::Text, false),
            column(1, 5, "nullable", PhysicalType::Int64, true),
        ];
        let right_columns = [
            column(2, 1, "flag", PhysicalType::Bool, false),
            column(2, 2, "signed", PhysicalType::Int64, false),
            column(2, 3, "unsigned", PhysicalType::UInt64, false),
            column(2, 4, "text", PhysicalType::Text, false),
            column(2, 5, "nullable", PhysicalType::Int64, true),
        ];
        let fields = left_columns
            .iter()
            .chain(&right_columns)
            .cloned()
            .map(OutputField::Source)
            .collect::<Vec<_>>();
        let left = vec![
            ScalarValue::Bool(true),
            ScalarValue::Int64(7),
            ScalarValue::UInt64(9),
            ScalarValue::Text("shared".into()),
            ScalarValue::Null,
        ];
        let right = vec![
            ScalarValue::Bool(false),
            ScalarValue::Int64(7),
            ScalarValue::UInt64(11),
            ScalarValue::Text("shared".into()),
            ScalarValue::Null,
        ];
        let mut contiguous = left.clone();
        contiguous.extend(right.iter().cloned());

        let expressions = vec![
            binary(
                BinaryOp::Eq,
                column_expr(&left_columns[1]),
                column_expr(&right_columns[1]),
            ),
            binary(
                BinaryOp::NotEq,
                column_expr(&left_columns[1]),
                column_expr(&right_columns[1]),
            ),
            binary(
                BinaryOp::Eq,
                column_expr(&right_columns[3]),
                column_expr(&left_columns[3]),
            ),
            binary(
                BinaryOp::Lt,
                column_expr(&left_columns[2]),
                column_expr(&right_columns[2]),
            ),
            binary(
                BinaryOp::LtEq,
                column_expr(&left_columns[1]),
                column_expr(&right_columns[1]),
            ),
            binary(
                BinaryOp::Gt,
                column_expr(&right_columns[2]),
                column_expr(&left_columns[2]),
            ),
            binary(
                BinaryOp::GtEq,
                column_expr(&right_columns[1]),
                column_expr(&left_columns[1]),
            ),
            binary(
                BinaryOp::And,
                column_expr(&left_columns[0]),
                binary(
                    BinaryOp::Eq,
                    column_expr(&left_columns[1]),
                    literal(ScalarValue::Int64(7), PhysicalType::Int64),
                ),
            ),
            binary(
                BinaryOp::Or,
                column_expr(&right_columns[0]),
                binary(
                    BinaryOp::Eq,
                    column_expr(&right_columns[3]),
                    literal(ScalarValue::Text("shared".into()), PhysicalType::Text),
                ),
            ),
            not(column_expr(&right_columns[0])),
            is_null(column_expr(&left_columns[4]), false),
            is_null(column_expr(&right_columns[4]), true),
            binary(
                BinaryOp::Eq,
                column_expr(&left_columns[4]),
                literal(ScalarValue::Null, PhysicalType::Int64),
            ),
            binary(
                BinaryOp::Eq,
                column_expr(&left_columns[0]),
                literal(ScalarValue::Bool(true), PhysicalType::Bool),
            ),
            binary(
                BinaryOp::Eq,
                column_expr(&left_columns[2]),
                literal(ScalarValue::UInt64(9), PhysicalType::UInt64),
            ),
            binary(
                BinaryOp::Eq,
                column_expr(&left_columns[1]),
                column_expr(&left_columns[1]),
            ),
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::Lt,
                    column_expr(&left_columns[1]),
                    column_expr(&right_columns[1]),
                ),
                binary(
                    BinaryOp::And,
                    not(column_expr(&right_columns[0])),
                    is_null(column_expr(&left_columns[4]), true),
                ),
            ),
        ];

        for expression in expressions {
            let joined = EvaluationValues::Joined {
                left: &left,
                right: &right,
            };
            let bound = bind_expression(&expression, &fields).expect("bind expression");
            assert_eq!(
                evaluate(&expression, &contiguous, &fields).expect("contiguous evaluation"),
                evaluate_values(&expression, joined, &fields).expect("joined evaluation")
            );
            let dynamic =
                evaluate_values(&expression, joined, &fields).expect("dynamic evaluation");
            let evaluated = evaluate_bound_values(&bound, joined).expect("bound evaluation");
            assert_eq!(&dynamic, evaluated.as_ref());
            assert_eq!(
                evaluate_truth(&expression, &contiguous, &fields).expect("contiguous truth"),
                evaluate_truth_values(&expression, joined, &fields).expect("joined truth")
            );
            assert_eq!(
                evaluate_truth_values(&expression, joined, &fields).expect("dynamic truth"),
                evaluate_bound_truth(&bound, joined).expect("bound truth")
            );
        }

        let left_signed = column_expr(&left_columns[1]);
        let right_signed = column_expr(&right_columns[1]);
        let bound_left = bind_expression(&left_signed, &fields).expect("bind left identity");
        let bound_right = bind_expression(&right_signed, &fields).expect("bind right identity");
        assert!(matches!(
            bound_left.kind,
            BoundExprKind::Column { position: 1, .. }
        ));
        assert!(matches!(
            bound_right.kind,
            BoundExprKind::Column { position: 6, .. }
        ));

        let missing = column(3, 99, "missing", PhysicalType::Int64, false);
        assert!(matches!(
            bind_expression(&column_expr(&missing), &fields),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));

        let bound_right_signed =
            bind_expression(&right_signed, &fields).expect("bind right signed column");
        assert!(matches!(
            evaluate_bound_values(
                &bound_right_signed,
                EvaluationValues::Contiguous(&left[..1])
            ),
            Err(ExecutionError::MissingColumn(name)) if name == "signed"
        ));
    }

    #[test]
    fn bound_leaf_values_borrow_and_computed_values_are_owned() {
        let values = vec![
            ScalarValue::Int64(7),
            ScalarValue::Text("alpha".into()),
            ScalarValue::Text("beta".into()),
            ScalarValue::Null,
            ScalarValue::Bool(true),
        ];
        let evaluation_values = EvaluationValues::Contiguous(&values);

        for (position, name) in [(0, "number"), (1, "text")] {
            let expression = BoundExpr {
                kind: BoundExprKind::Column { position, name },
            };
            let evaluated =
                evaluate_bound_values(&expression, evaluation_values).expect("evaluate column");
            match evaluated {
                EvaluatedScalar::Borrowed(value) => {
                    assert!(std::ptr::eq(value, &values[position]));
                }
                EvaluatedScalar::Owned(_) => panic!("bound column must remain borrowed"),
            }
        }

        let literal_value = ScalarValue::Text("constant-value".into());
        let literal = BoundExpr {
            kind: BoundExprKind::Literal(&literal_value),
        };
        let evaluated =
            evaluate_bound_values(&literal, evaluation_values).expect("evaluate literal");
        match evaluated {
            EvaluatedScalar::Borrowed(value) => assert!(std::ptr::eq(value, &literal_value)),
            EvaluatedScalar::Owned(_) => panic!("bound literal must remain borrowed"),
        }

        let text_comparison = BoundExpr {
            kind: BoundExprKind::Binary {
                operator: BinaryOp::Lt,
                left: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 1,
                        name: "left_text",
                    },
                }),
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 2,
                        name: "right_text",
                    },
                }),
            },
        };
        assert!(matches!(
            evaluate_bound_values(&text_comparison, evaluation_values),
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(true)))
        ));

        let null_comparison = BoundExpr {
            kind: BoundExprKind::Binary {
                operator: BinaryOp::Eq,
                left: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 3,
                        name: "nullable",
                    },
                }),
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 3,
                        name: "nullable",
                    },
                }),
            },
        };
        assert!(matches!(
            evaluate_bound_values(&null_comparison, evaluation_values),
            Ok(EvaluatedScalar::Owned(ScalarValue::Null))
        ));

        let is_null = BoundExpr {
            kind: BoundExprKind::IsNull {
                expression: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 3,
                        name: "nullable",
                    },
                }),
                negated: false,
            },
        };
        assert!(matches!(
            evaluate_bound_values(&is_null, evaluation_values),
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(true)))
        ));

        let and = BoundExpr {
            kind: BoundExprKind::Binary {
                operator: BinaryOp::And,
                left: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 4,
                        name: "flag",
                    },
                }),
                right: Box::new(BoundExpr {
                    kind: BoundExprKind::Column {
                        position: 3,
                        name: "nullable_bool",
                    },
                }),
            },
        };
        assert!(matches!(
            evaluate_bound_values(&and, evaluation_values),
            Ok(EvaluatedScalar::Owned(ScalarValue::Null))
        ));
    }

    #[test]
    fn hash_join_matches_nested_loop_for_all_key_types_nulls_duplicates_and_residuals() {
        fn column(
            binding_id: u32,
            table_id: u64,
            column_id: u32,
            name: &str,
            physical: PhysicalType,
            nullable: bool,
        ) -> ColumnRef {
            ColumnRef {
                binding_id: RelationBindingId(binding_id),
                table_id: TableId(table_id),
                column_id: ColumnId(column_id),
                relation_name: format!("r{binding_id}"),
                name: name.into(),
                data_type: SemanticType::physical(physical),
                nullable,
            }
        }

        fn expression(column: &ColumnRef) -> Expr {
            Expr {
                kind: ExprKind::Column(column.clone()),
                expr_type: ExprType {
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                },
            }
        }

        fn literal(value: ScalarValue, physical: PhysicalType) -> Expr {
            Expr {
                kind: ExprKind::Literal(value),
                expr_type: ExprType {
                    data_type: SemanticType::physical(physical),
                    nullable: false,
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

        fn key_values(physical: PhysicalType) -> (ScalarValue, ScalarValue) {
            match physical {
                PhysicalType::Bool => (ScalarValue::Bool(true), ScalarValue::Bool(false)),
                PhysicalType::Int64 => (ScalarValue::Int64(-7), ScalarValue::Int64(9)),
                PhysicalType::UInt64 => (ScalarValue::UInt64(7), ScalarValue::UInt64(9)),
                PhysicalType::Text => (
                    ScalarValue::Text("alpha".into()),
                    ScalarValue::Text("omega".into()),
                ),
            }
        }

        for (case, physical) in [
            PhysicalType::Bool,
            PhysicalType::Int64,
            PhysicalType::UInt64,
            PhysicalType::Text,
        ]
        .into_iter()
        .enumerate()
        {
            let left_table_id = TableId(1_000 + u64::try_from(case).expect("case ID"));
            let right_table_id = TableId(2_000 + u64::try_from(case).expect("case ID"));
            let left_path = std::env::temp_dir().join(format!(
                "netbadb-hash-left-{physical:?}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let right_path = std::env::temp_dir().join(format!(
                "netbadb-hash-right-{physical:?}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let table = |table_id, name| {
                TableDef::new(
                    table_id,
                    name,
                    vec![
                        ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                        ColumnDef::new(ColumnId(2), "key", TypeSpec::Physical(physical))
                            .nullable(true),
                        ColumnDef::new(
                            ColumnId(3),
                            "enabled",
                            TypeSpec::Physical(PhysicalType::Bool),
                        ),
                        ColumnDef::new(
                            ColumnId(4),
                            "marker",
                            TypeSpec::Physical(PhysicalType::Int64),
                        )
                        .nullable(true),
                    ],
                )
            };
            let mut left_storage =
                HeapStorage::create(&left_path, table(left_table_id, "left_rows"))
                    .expect("create left heap");
            let mut right_storage =
                HeapStorage::create(&right_path, table(right_table_id, "right_rows"))
                    .expect("create right heap");
            let (first_key, second_key) = key_values(physical);
            for row in [
                vec![
                    ScalarValue::Int64(1),
                    first_key.clone(),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(1),
                ],
                vec![
                    ScalarValue::Int64(2),
                    first_key.clone(),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(2),
                ],
                vec![
                    ScalarValue::Int64(3),
                    ScalarValue::Null,
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(3),
                ],
                vec![
                    ScalarValue::Int64(4),
                    second_key.clone(),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(4),
                ],
            ] {
                left_storage.insert(&row).expect("insert left row");
            }
            for row in [
                vec![
                    ScalarValue::Int64(10),
                    first_key.clone(),
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(10),
                ],
                vec![
                    ScalarValue::Int64(11),
                    first_key,
                    ScalarValue::Bool(false),
                    ScalarValue::Null,
                ],
                vec![
                    ScalarValue::Int64(12),
                    ScalarValue::Null,
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(12),
                ],
                vec![
                    ScalarValue::Int64(13),
                    second_key,
                    ScalarValue::Bool(true),
                    ScalarValue::Int64(13),
                ],
            ] {
                right_storage.insert(&row).expect("insert right row");
            }

            let left_columns = vec![
                column(10, left_table_id.0, 1, "id", PhysicalType::Int64, false),
                column(10, left_table_id.0, 2, "key", physical, true),
                column(10, left_table_id.0, 3, "enabled", PhysicalType::Bool, false),
                column(10, left_table_id.0, 4, "marker", PhysicalType::Int64, true),
            ];
            let right_columns = vec![
                column(20, right_table_id.0, 1, "id", PhysicalType::Int64, false),
                column(20, right_table_id.0, 2, "key", physical, true),
                column(
                    20,
                    right_table_id.0,
                    3,
                    "enabled",
                    PhysicalType::Bool,
                    false,
                ),
                column(20, right_table_id.0, 4, "marker", PhysicalType::Int64, true),
            ];
            let left_scan = PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(10),
                table_id: left_table_id,
                table_name: "left_rows".into(),
                columns: left_columns.clone(),
            };
            let right_scan = PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(20),
                table_id: right_table_id,
                table_name: "right_rows".into(),
                columns: right_columns.clone(),
            };
            let equality = binary(
                BinaryOp::Eq,
                expression(&left_columns[1]),
                expression(&right_columns[1]),
            );
            let mut columns = left_columns.clone();
            columns.extend(right_columns.clone());
            let nested = PhysicalPlan::NestedLoopJoin {
                left: Box::new(left_scan.clone()),
                right: Box::new(right_scan.clone()),
                kind: JoinKind::Inner,
                predicate: equality.clone(),
                columns: columns.clone(),
            };
            let hash = PhysicalPlan::HashJoin {
                left: Box::new(left_scan.clone()),
                right: Box::new(right_scan.clone()),
                kind: JoinKind::Inner,
                left_key: left_columns[1].clone(),
                right_key: right_columns[1].clone(),
                predicate: equality.clone(),
                columns: columns.clone(),
            };
            let mut storages = [left_storage, right_storage];
            let nested_result =
                execute_with_storages(&nested, &mut storages).expect("nested-loop execution");
            let hash_result = execute_with_storages(&hash, &mut storages).expect("hash execution");
            assert_eq!(hash_result, nested_result);
            assert_eq!(
                hash_result
                    .rows
                    .iter()
                    .map(|row| (row[0].clone(), row[4].clone()))
                    .collect::<Vec<_>>(),
                [(1, 10), (1, 11), (2, 10), (2, 11), (4, 13)]
                    .map(|(left, right)| { (ScalarValue::Int64(left), ScalarValue::Int64(right)) })
            );
            for _ in 0..5 {
                assert_eq!(
                    execute_with_storages(&hash, &mut storages).expect("repeat hash execution"),
                    hash_result
                );
            }

            if physical == PhysicalType::Int64 {
                let mut invalid_left_key = left_columns[1].clone();
                invalid_left_key.data_type = SemanticType::physical(PhysicalType::UInt64);
                let mut invalid_right_key = right_columns[1].clone();
                invalid_right_key.data_type = SemanticType::physical(PhysicalType::UInt64);
                let invalid_runtime_key = PhysicalPlan::HashJoin {
                    left: Box::new(left_scan.clone()),
                    right: Box::new(right_scan.clone()),
                    kind: JoinKind::Inner,
                    left_key: invalid_left_key,
                    right_key: invalid_right_key,
                    predicate: equality.clone(),
                    columns: columns.clone(),
                };
                assert!(matches!(
                    execute_with_storages(&invalid_runtime_key, &mut storages),
                    Err(ExecutionError::TypeMismatch)
                ));

                let residual = binary(
                    BinaryOp::And,
                    equality,
                    binary(
                        BinaryOp::And,
                        binary(
                            BinaryOp::Eq,
                            expression(&right_columns[2]),
                            literal(ScalarValue::Bool(true), PhysicalType::Bool),
                        ),
                        Expr {
                            kind: ExprKind::IsNull {
                                expression: Box::new(expression(&right_columns[3])),
                                negated: true,
                            },
                            expr_type: ExprType {
                                data_type: SemanticType::physical(PhysicalType::Bool),
                                nullable: false,
                            },
                        },
                    ),
                );
                let nested_residual = PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_scan.clone()),
                    right: Box::new(right_scan.clone()),
                    kind: JoinKind::Inner,
                    predicate: residual.clone(),
                    columns: columns.clone(),
                };
                let hash_residual = PhysicalPlan::HashJoin {
                    left: Box::new(left_scan),
                    right: Box::new(right_scan),
                    kind: JoinKind::Inner,
                    left_key: left_columns[1].clone(),
                    right_key: right_columns[1].clone(),
                    predicate: residual,
                    columns,
                };
                assert_eq!(
                    execute_with_storages(&hash_residual, &mut storages)
                        .expect("hash residual execution"),
                    execute_with_storages(&nested_residual, &mut storages)
                        .expect("nested residual execution")
                );
            }

            for storage in storages {
                storage.close().expect("close hash fixture");
            }
            let _ = std::fs::remove_file(&left_path);
            let _ = std::fs::remove_file(netbadb_storage::wal_path(&left_path));
            let _ = std::fs::remove_file(&right_path);
            let _ = std::fs::remove_file(netbadb_storage::wal_path(&right_path));
        }
    }

    #[test]
    fn stable_sort_covers_all_types_null_orders_and_runtime_validation() {
        let table = TableDef::new(
            TableId(2),
            "sortable",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "unsigned",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(ColumnId(3), "text", TypeSpec::Physical(PhysicalType::Text)),
                ColumnDef::new(
                    ColumnId(4),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
                ColumnDef::new(
                    ColumnId(5),
                    "value",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
            ],
        );
        let path = std::env::temp_dir().join(format!(
            "netbadb-executor-sort-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut storage = HeapStorage::create(&path, table).expect("create heap");
        for row in [
            vec![
                ScalarValue::Int64(1),
                ScalarValue::UInt64(2),
                ScalarValue::Text("b".into()),
                ScalarValue::Bool(true),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::UInt64(1),
                ScalarValue::Text("c".into()),
                ScalarValue::Bool(false),
                ScalarValue::Int64(3),
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::UInt64(3),
                ScalarValue::Text("a".into()),
                ScalarValue::Bool(true),
                ScalarValue::Int64(1),
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::UInt64(4),
                ScalarValue::Text("d".into()),
                ScalarValue::Bool(false),
                ScalarValue::Null,
            ],
            vec![
                ScalarValue::Int64(5),
                ScalarValue::UInt64(5),
                ScalarValue::Text("e".into()),
                ScalarValue::Bool(true),
                ScalarValue::Int64(2),
            ],
        ] {
            storage.insert(&row).expect("insert sortable row");
        }

        let column = |id: u32, name: &str, physical: PhysicalType, nullable: bool| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(2),
            column_id: ColumnId(id),
            relation_name: "sortable".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable,
        };
        let id = column(1, "id", PhysicalType::Int64, false);
        let unsigned = column(2, "unsigned", PhysicalType::UInt64, false);
        let text = column(3, "text", PhysicalType::Text, false);
        let active = column(4, "active", PhysicalType::Bool, false);
        let value = column(5, "value", PhysicalType::Int64, true);
        let columns = vec![
            id.clone(),
            unsigned.clone(),
            text.clone(),
            active.clone(),
            value.clone(),
        ];
        {
            let mut sorted_ids = |key: ColumnRef, direction, null_order| {
                let logical = LogicalPlan::Project {
                    input: Box::new(LogicalPlan::Sort {
                        input: Box::new(LogicalPlan::Scan {
                            binding_id: RelationBindingId(0),
                            table_id: TableId(2),
                            table_name: "sortable".into(),
                            columns: columns.clone(),
                        }),
                        keys: vec![SortKey {
                            column: key,
                            direction,
                            null_order,
                        }],
                    }),
                    columns: vec![id.clone()],
                };
                execute(&plan(&logical), &mut storage)
                    .expect("sort executes")
                    .rows
                    .into_iter()
                    .map(|row| row[0].clone())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                sorted_ids(unsigned, SortDirection::Asc, NullOrder::Last),
                [2, 1, 3, 4, 5].map(ScalarValue::Int64)
            );
            assert_eq!(
                sorted_ids(text, SortDirection::Asc, NullOrder::Last),
                [3, 1, 2, 4, 5].map(ScalarValue::Int64)
            );
            assert_eq!(
                sorted_ids(active, SortDirection::Asc, NullOrder::Last),
                [2, 4, 1, 3, 5].map(ScalarValue::Int64)
            );
            assert_eq!(
                sorted_ids(value.clone(), SortDirection::Asc, NullOrder::Last),
                [3, 5, 2, 1, 4].map(ScalarValue::Int64)
            );
            assert_eq!(
                sorted_ids(value.clone(), SortDirection::Desc, NullOrder::First),
                [1, 4, 2, 5, 3].map(ScalarValue::Int64)
            );
            assert_eq!(
                sorted_ids(value.clone(), SortDirection::Asc, NullOrder::First),
                [1, 4, 3, 5, 2].map(ScalarValue::Int64)
            );
            assert_eq!(
                sorted_ids(value, SortDirection::Desc, NullOrder::Last),
                [2, 5, 3, 1, 4].map(ScalarValue::Int64)
            );
        }

        let mut mismatched = id.clone();
        mismatched.data_type = SemanticType::physical(PhysicalType::UInt64);
        let invalid = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(2),
                table_name: "sortable".into(),
                columns,
            }),
            keys: vec![SortKey {
                column: mismatched,
                direction: SortDirection::Asc,
                null_order: NullOrder::Last,
            }],
        };
        assert!(matches!(
            execute(&plan(&invalid), &mut storage),
            Err(ExecutionError::TypeMismatch)
        ));

        storage.close().expect("close storage");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn grouped_aggregate_keeps_nulls_and_first_seen_output_order() {
        let table = TableDef::new(
            TableId(4),
            "grouped",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "team_id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
                ColumnDef::new(
                    ColumnId(2),
                    "score",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
            ],
        );
        let path = std::env::temp_dir().join(format!(
            "netbadb-executor-grouped-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut storage = HeapStorage::create(&path, table).expect("create grouped heap");
        for row in [
            vec![ScalarValue::Int64(20), ScalarValue::Int64(1)],
            vec![ScalarValue::Int64(10), ScalarValue::Int64(2)],
            vec![ScalarValue::Int64(20), ScalarValue::Null],
            vec![ScalarValue::Null, ScalarValue::Int64(4)],
            vec![ScalarValue::Null, ScalarValue::Null],
        ] {
            storage.insert(&row).expect("insert grouped row");
        }
        let column = |id: u32, name: &str, nullable| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(4),
            column_id: ColumnId(id),
            relation_name: "grouped".into(),
            name: name.into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable,
        };
        let team_id = column(1, "team_id", true);
        let score = column(2, "score", true);
        let count = AggregateExpr {
            function: AggregateFunction::Count,
            input: AggregateInput::All,
            output: DerivedField {
                name: "COUNT(*)".into(),
                data_type: SemanticType::physical(PhysicalType::UInt64),
                nullable: false,
            },
        };
        let sum = AggregateExpr {
            function: AggregateFunction::Sum,
            input: AggregateInput::Column(score),
            output: DerivedField {
                name: "SUM(score)".into(),
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable: true,
            },
        };
        let logical = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                binding_id: RelationBindingId(0),
                table_id: TableId(4),
                table_name: "grouped".into(),
                columns: vec![team_id.clone(), column(2, "score", true)],
            }),
            group_keys: vec![team_id.clone()],
            outputs: vec![
                AggregateOutput::Aggregate(count),
                AggregateOutput::GroupKey(team_id),
                AggregateOutput::Aggregate(sum),
            ],
        };
        let result = execute(&plan(&logical), &mut storage).expect("execute grouped aggregate");
        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["COUNT(*)", "team_id", "SUM(score)"]
        );
        assert_eq!(
            result.rows,
            vec![
                vec![
                    ScalarValue::UInt64(2),
                    ScalarValue::Int64(20),
                    ScalarValue::Int64(1)
                ],
                vec![
                    ScalarValue::UInt64(1),
                    ScalarValue::Int64(10),
                    ScalarValue::Int64(2)
                ],
                vec![
                    ScalarValue::UInt64(2),
                    ScalarValue::Null,
                    ScalarValue::Int64(4)
                ],
            ]
        );
        storage.close().expect("close grouped heap");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn direct_count_eligibility_maps_outputs_and_is_conservative() {
        let column = |binding_id: u32, table_id: u64, column_id: u32, name: &str| ColumnRef {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(table_id),
            column_id: ColumnId(column_id),
            relation_name: format!("t{table_id}"),
            name: name.into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let value = column(0, 7, 1, "value");
        let other = column(0, 7, 2, "other");
        let single_scan = PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns: vec![value.clone()],
        };
        let aggregate = |function, input| {
            AggregateOutput::Aggregate(AggregateExpr {
                function,
                input,
                output: DerivedField {
                    name: format!("{}(value)", function.as_str()),
                    data_type: SemanticType::physical(PhysicalType::UInt64),
                    nullable: false,
                },
            })
        };
        let count = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(value.clone()),
        );
        assert!(direct_count_eligibility(&single_scan, &[], &[]).is_none());
        assert!(
            direct_count_eligibility(
                &single_scan,
                &[],
                &[AggregateOutput::GroupKey(value.clone())]
            )
            .is_none()
        );
        let AggregateOutput::Aggregate(count_expression) = &count else {
            panic!("expected aggregate output");
        };
        assert!(matches!(
            count_to_sql_u64(u128::from(u64::MAX) + 1, count_expression),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Count,
                output
            }) if output == "COUNT(value)"
        ));
        let single = direct_count_eligibility(&single_scan, &[], std::slice::from_ref(&count))
            .expect("single column COUNT is eligible");
        assert_eq!(single.table_id, TableId(7));
        assert_eq!(single.scan_columns, std::slice::from_ref(&value));
        assert_eq!(single.outputs.len(), 1);
        assert_eq!(
            single.outputs[0].source,
            super::DirectCountSource::Column(0)
        );

        let count_other = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(other.clone()),
        );
        let pair_scan = PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns: vec![value.clone(), other.clone()],
        };
        let pair_outputs = [count.clone(), count_other.clone()];
        let pair = direct_count_eligibility(&pair_scan, &[], &pair_outputs)
            .expect("pair column COUNT is eligible");
        assert_eq!(
            pair.outputs
                .iter()
                .map(|output| output.source)
                .collect::<Vec<_>>(),
            [
                super::DirectCountSource::Column(0),
                super::DirectCountSource::Column(1)
            ]
        );
        let duplicate_outputs = [count.clone(), count.clone()];
        let duplicate = direct_count_eligibility(&single_scan, &[], &duplicate_outputs)
            .expect("duplicate column COUNT is eligible");
        assert_eq!(
            duplicate
                .outputs
                .iter()
                .map(|output| output.source)
                .collect::<Vec<_>>(),
            [
                super::DirectCountSource::Column(0),
                super::DirectCountSource::Column(0)
            ]
        );
        let count_all = aggregate(AggregateFunction::Count, AggregateInput::All);
        let mixed_outputs = [count_all.clone(), count.clone(), count_all.clone()];
        let mixed = direct_count_eligibility(&single_scan, &[], &mixed_outputs)
            .expect("star mixed with column COUNT is eligible");
        assert_eq!(
            mixed
                .outputs
                .iter()
                .map(|output| output.source)
                .collect::<Vec<_>>(),
            [
                super::DirectCountSource::All,
                super::DirectCountSource::Column(0),
                super::DirectCountSource::All
            ]
        );

        let named_count = |name: &str, input| {
            AggregateOutput::Aggregate(AggregateExpr {
                function: AggregateFunction::Count,
                input,
                output: DerivedField {
                    name: name.into(),
                    data_type: SemanticType::physical(PhysicalType::UInt64),
                    nullable: false,
                },
            })
        };
        let named_outputs = [
            named_count("first_count", AggregateInput::Column(value.clone())),
            named_count("row_count", AggregateInput::All),
            named_count("second_count", AggregateInput::Column(other.clone())),
        ];
        let named_plan = direct_count_eligibility(&pair_scan, &[], &named_outputs)
            .expect("named mixed counts are eligible");
        assert_eq!(
            materialize_direct_count_values(
                &named_plan,
                &PresenceCountSummary {
                    live_rows: 9,
                    non_null_counts: vec![3, 4],
                }
            )
            .expect("materialize direct counts"),
            vec![
                ScalarValue::UInt64(3),
                ScalarValue::UInt64(9),
                ScalarValue::UInt64(4),
            ]
        );
        assert!(matches!(
            materialize_direct_count_values(
                &named_plan,
                &PresenceCountSummary {
                    live_rows: 9,
                    non_null_counts: vec![3, u128::from(u64::MAX) + 1],
                }
            ),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Count,
                output,
            }) if output == "second_count"
        ));
        assert!(matches!(
            materialize_direct_count_values(
                &named_plan,
                &PresenceCountSummary {
                    live_rows: 9,
                    non_null_counts: vec![3],
                }
            ),
            Err(ExecutionError::TypeMismatch)
        ));
        assert!(
            direct_count_eligibility(&single_scan, &[], std::slice::from_ref(&count_all)).is_none()
        );
        assert!(
            direct_count_eligibility(&single_scan, &[], &[count_all.clone(), count_all.clone()])
                .is_none()
        );
        for function in [
            AggregateFunction::Sum,
            AggregateFunction::Min,
            AggregateFunction::Max,
        ] {
            assert!(
                direct_count_eligibility(
                    &single_scan,
                    &[],
                    &[aggregate(function, AggregateInput::Column(value.clone()))]
                )
                .is_none()
            );
        }
        assert!(
            direct_count_eligibility(
                &single_scan,
                std::slice::from_ref(&value),
                std::slice::from_ref(&count)
            )
            .is_none()
        );
        assert!(
            direct_count_eligibility(
                &single_scan,
                &[],
                &[
                    count.clone(),
                    aggregate(
                        AggregateFunction::Sum,
                        AggregateInput::Column(value.clone())
                    )
                ]
            )
            .is_none()
        );

        let missing_count_column = PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns: vec![other.clone()],
        };
        assert!(
            direct_count_eligibility(&missing_count_column, &[], std::slice::from_ref(&count))
                .is_none()
        );
        assert!(direct_count_eligibility(&pair_scan, &[], std::slice::from_ref(&count)).is_none());
        let mismatched_table = PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(8),
            table_name: "t8".into(),
            columns: vec![value.clone()],
        };
        assert!(
            direct_count_eligibility(&mismatched_table, &[], std::slice::from_ref(&count))
                .is_none()
        );
        let mismatched_binding = PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(1),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns: vec![value.clone()],
        };
        assert!(
            direct_count_eligibility(&mismatched_binding, &[], std::slice::from_ref(&count))
                .is_none()
        );

        let mismatched_count_table = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(column(0, 8, 1, "value")),
        );
        let mismatched_count_binding = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(column(1, 7, 1, "value")),
        );
        assert!(
            direct_count_eligibility(
                &single_scan,
                &[],
                std::slice::from_ref(&mismatched_count_table)
            )
            .is_none()
        );
        assert!(
            direct_count_eligibility(
                &single_scan,
                &[],
                std::slice::from_ref(&mismatched_count_binding)
            )
            .is_none()
        );

        let true_predicate = Expr {
            kind: ExprKind::Literal(ScalarValue::Bool(true)),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let filtered = PhysicalPlan::Filter {
            input: Box::new(single_scan.clone()),
            predicate: true_predicate.clone(),
        };
        assert!(direct_count_eligibility(&filtered, &[], std::slice::from_ref(&count)).is_none());
        let right_value = column(1, 8, 1, "value");
        let joined = PhysicalPlan::NestedLoopJoin {
            left: Box::new(single_scan.clone()),
            right: Box::new(PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(1),
                table_id: TableId(8),
                table_name: "t8".into(),
                columns: vec![right_value.clone()],
            }),
            kind: JoinKind::Inner,
            predicate: true_predicate.clone(),
            columns: vec![value.clone()],
        };
        assert!(direct_count_eligibility(&joined, &[], std::slice::from_ref(&count)).is_none());
        let hash_joined = PhysicalPlan::HashJoin {
            left: Box::new(single_scan.clone()),
            right: Box::new(PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(1),
                table_id: TableId(8),
                table_name: "t8".into(),
                columns: vec![right_value.clone()],
            }),
            kind: JoinKind::Inner,
            left_key: value.clone(),
            right_key: right_value.clone(),
            predicate: true_predicate,
            columns: vec![value.clone(), right_value],
        };
        assert!(
            direct_count_eligibility(&hash_joined, &[], std::slice::from_ref(&count)).is_none()
        );
        let sorted = PhysicalPlan::Sort {
            input: Box::new(single_scan),
            keys: vec![SortKey {
                column: value,
                direction: SortDirection::Asc,
                null_order: NullOrder::First,
            }],
        };
        assert!(direct_count_eligibility(&sorted, &[], &[count]).is_none());
    }

    #[test]
    fn global_aggregate_reports_checked_numeric_overflow_and_runtime_mismatch() {
        let table = TableDef::new(
            TableId(3),
            "numbers",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "signed",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(
                    ColumnId(2),
                    "unsigned",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(
                    ColumnId(3),
                    "group_key",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
            ],
        );
        let path = std::env::temp_dir().join(format!(
            "netbadb-aggregate-overflow-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut storage = HeapStorage::create(&path, table).expect("create heap");
        storage
            .insert(&[
                ScalarValue::Int64(i64::MAX),
                ScalarValue::UInt64(u64::MAX),
                ScalarValue::Bool(true),
            ])
            .expect("insert max values");
        storage
            .insert(&[
                ScalarValue::Int64(1),
                ScalarValue::UInt64(1),
                ScalarValue::Bool(true),
            ])
            .expect("insert overflow values");
        let column = |id: u32, name: &str, physical| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(3),
            column_id: ColumnId(id),
            relation_name: "numbers".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable: false,
        };
        let signed = column(1, "signed", PhysicalType::Int64);
        let unsigned = column(2, "unsigned", PhysicalType::UInt64);
        let group_key = column(3, "group_key", PhysicalType::Bool);
        let scan = || LogicalPlan::Scan {
            binding_id: RelationBindingId(0),
            table_id: TableId(3),
            table_name: "numbers".into(),
            columns: vec![signed.clone(), unsigned.clone(), group_key.clone()],
        };
        let sum = |input: ColumnRef, physical, name: &str| LogicalPlan::Aggregate {
            input: Box::new(scan()),
            group_keys: Vec::new(),
            outputs: vec![AggregateOutput::Aggregate(AggregateExpr {
                function: AggregateFunction::Sum,
                input: AggregateInput::Column(input),
                output: DerivedField {
                    name: name.into(),
                    data_type: SemanticType::physical(physical),
                    nullable: true,
                },
            })],
        };
        assert!(matches!(
            execute(
                &plan(&sum(signed.clone(), PhysicalType::Int64, "SUM(signed)")),
                &mut storage
            ),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Sum,
                ..
            })
        ));
        assert!(matches!(
            execute(
                &plan(&sum(
                    unsigned.clone(),
                    PhysicalType::UInt64,
                    "SUM(unsigned)"
                )),
                &mut storage
            ),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Sum,
                ..
            })
        ));

        let mut mismatched = signed.clone();
        mismatched.data_type = SemanticType::physical(PhysicalType::UInt64);
        assert!(matches!(
            execute(
                &plan(&sum(mismatched, PhysicalType::UInt64, "SUM(signed)")),
                &mut storage
            ),
            Err(ExecutionError::TypeMismatch)
        ));

        for function in [
            AggregateFunction::Sum,
            AggregateFunction::Min,
            AggregateFunction::Max,
        ] {
            let invalid = LogicalPlan::Aggregate {
                input: Box::new(scan()),
                group_keys: Vec::new(),
                outputs: vec![AggregateOutput::Aggregate(AggregateExpr {
                    function,
                    input: AggregateInput::All,
                    output: DerivedField {
                        name: format!("{}(*)", function.as_str()),
                        data_type: SemanticType::physical(PhysicalType::Int64),
                        nullable: true,
                    },
                })],
            };
            assert!(matches!(
                execute(&plan(&invalid), &mut storage),
                Err(ExecutionError::InvalidAggregateInput {
                    function: actual
                }) if actual == function
            ));
        }

        let grouped_overflow = LogicalPlan::Aggregate {
            input: Box::new(scan()),
            group_keys: vec![group_key.clone()],
            outputs: vec![AggregateOutput::Aggregate(AggregateExpr {
                function: AggregateFunction::Sum,
                input: AggregateInput::Column(signed.clone()),
                output: DerivedField {
                    name: "SUM(signed)".into(),
                    data_type: SemanticType::physical(PhysicalType::Int64),
                    nullable: true,
                },
            })],
        };
        assert!(matches!(
            execute(&plan(&grouped_overflow), &mut storage),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Sum,
                ..
            })
        ));

        let mut mismatched_group_key = unsigned.clone();
        mismatched_group_key.data_type = SemanticType::physical(PhysicalType::Int64);
        let invalid_group_key = LogicalPlan::Aggregate {
            input: Box::new(scan()),
            group_keys: vec![mismatched_group_key.clone()],
            outputs: vec![AggregateOutput::GroupKey(mismatched_group_key)],
        };
        assert!(matches!(
            execute(&plan(&invalid_group_key), &mut storage),
            Err(ExecutionError::TypeMismatch)
        ));
        storage.close().expect("close storage");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }
}
