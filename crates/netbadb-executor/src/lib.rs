//! Synchronous execution of typed query and DML physical statements.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use netbadb_planner::{PhysicalPlan, PhysicalStatement};
use netbadb_rel::{
    AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, Assignment, BinaryOp,
    ColumnRef, Expr, ExprKind, NullOrder, OutputField, SortDirection, SortKey, UnaryOp,
};
use netbadb_storage::{HeapStorage, StorageError, Transaction};
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

fn execute_rows(
    plan: &PhysicalPlan,
    storages: &mut [HeapStorage],
) -> Result<ExecutionRows, ExecutionError> {
    match plan {
        PhysicalPlan::SeqScan {
            table_id, columns, ..
        } => {
            let storage = storage_for_table(storages, *table_id)?;
            let rows = storage
                .scan()?
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
            let row_ids = storage.btree().lookup(*handle, key)?;
            let rows = row_ids
                .into_iter()
                .map(|row_id| {
                    Ok(ExecutionRow {
                        row_id: Some(row_id),
                        values: storage.read_row(row_id)?,
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
            let row_ids = storage.btree().lookup_range(*handle, range)?;
            let rows = row_ids
                .into_iter()
                .map(|row_id| {
                    Ok(ExecutionRow {
                        row_id: Some(row_id),
                        values: storage.read_row(row_id)?,
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
            let fields = columns
                .iter()
                .cloned()
                .map(OutputField::Source)
                .collect::<Vec<_>>();
            let bound_predicate = bind_expression(predicate, &fields)?;
            let mut rows = Vec::new();
            for left_row in left.rows {
                for right_row in &right.rows {
                    if evaluate_bound_truth(
                        &bound_predicate,
                        EvaluationValues::Joined {
                            left: &left_row.values,
                            right: &right_row.values,
                        },
                    )? == TruthValue::True
                    {
                        let mut values = Vec::with_capacity(
                            left_row.values.len().saturating_add(right_row.values.len()),
                        );
                        values.extend(left_row.values.iter().cloned());
                        values.extend(right_row.values.iter().cloned());
                        rows.push(ExecutionRow {
                            row_id: None,
                            values,
                        });
                    }
                }
            }
            Ok(ExecutionRows {
                fields: columns.iter().cloned().map(OutputField::Source).collect(),
                rows,
            })
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

            let fields = columns
                .iter()
                .cloned()
                .map(OutputField::Source)
                .collect::<Vec<_>>();
            let bound_predicate = bind_expression(predicate, &fields)?;
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
                        let mut values = Vec::with_capacity(
                            left_row.values.len().saturating_add(right_row.values.len()),
                        );
                        values.extend(left_row.values.iter().cloned());
                        values.extend(right_row.values.iter().cloned());
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
            let positions = columns
                .iter()
                .map(|column| find_source_position(&input_result.fields, column))
                .collect::<Result<Vec<_>, _>>()?;
            let rows = input_result
                .rows
                .into_iter()
                .map(|row| {
                    let values = positions
                        .iter()
                        .map(|position| row.values[*position].clone())
                        .collect();
                    ExecutionRow {
                        row_id: row.row_id,
                        values,
                    }
                })
                .collect();
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
            let input = execute_rows(input, storages)?;
            execute_aggregate(input, group_keys, outputs)
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

fn evaluate_bound_values(
    expression: &BoundExpr<'_>,
    values: EvaluationValues<'_>,
) -> Result<ScalarValue, ExecutionError> {
    match &expression.kind {
        BoundExprKind::Column { position, name } => values
            .get(*position)
            .cloned()
            .ok_or_else(|| ExecutionError::MissingColumn((*name).to_owned())),
        BoundExprKind::Literal(value) => Ok((*value).clone()),
        BoundExprKind::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_bound_values(left, values)?;
            let right = evaluate_bound_values(right, values)?;
            evaluate_binary(*operator, left, right)
        }
        BoundExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(evaluate_bound_truth(expression, values)?
            .not()
            .into_scalar()),
        BoundExprKind::IsNull {
            expression,
            negated,
        } => {
            let is_null = matches!(
                evaluate_bound_values(expression, values)?,
                ScalarValue::Null
            );
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
    }
}

fn evaluate_bound_truth(
    expression: &BoundExpr<'_>,
    values: EvaluationValues<'_>,
) -> Result<TruthValue, ExecutionError> {
    TruthValue::from_scalar(evaluate_bound_values(expression, values)?)
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
    match operator {
        BinaryOp::And | BinaryOp::Or => {
            let left = TruthValue::from_scalar(left)?;
            let right = TruthValue::from_scalar(right)?;
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
            let ordering = compare_values(&left, &right)?;
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
        BoundExprKind, EvaluationValues, ExecutionError, QueryResult, TruthValue, bind_expression,
        evaluate, evaluate_binary, evaluate_bound_truth, evaluate_bound_values, evaluate_truth,
        evaluate_truth_values, evaluate_values, execute, execute_rows, execute_with_storages,
    };
    use netbadb_planner::{PhysicalPlan, plan};
    use netbadb_rel::{
        AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, BinaryOp, ColumnRef,
        DerivedField, Expr, ExprKind, JoinKind, LogicalPlan, NullOrder, OutputField, SortDirection,
        SortKey, UnaryOp,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_storage::HeapStorage;
    use netbadb_types::{
        ColumnId, ExprType, PhysicalType, RelationBindingId, ScalarValue, SemanticType, TableId,
    };

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
            assert_eq!(
                evaluate_values(&expression, joined, &fields).expect("dynamic evaluation"),
                evaluate_bound_values(&bound, joined).expect("bound evaluation")
            );
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
