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
            let mut rows = Vec::new();
            for left_row in left.rows {
                for right_row in &right.rows {
                    let mut values = Vec::with_capacity(
                        left_row.values.len().saturating_add(right_row.values.len()),
                    );
                    values.extend(left_row.values.iter().cloned());
                    values.extend(right_row.values.iter().cloned());
                    if evaluate_truth(predicate, &values, &fields)? == TruthValue::True {
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

fn evaluate(
    expression: &Expr,
    row: &[ScalarValue],
    fields: &[OutputField],
) -> Result<ScalarValue, ExecutionError> {
    match &expression.kind {
        ExprKind::Column(column) => {
            let position = find_source_position(fields, column)?;
            row.get(position)
                .cloned()
                .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))
        }
        ExprKind::Literal(value) => Ok(value.clone()),
        ExprKind::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate(left, row, fields)?;
            let right = evaluate(right, row, fields)?;
            evaluate_binary(*operator, left, right)
        }
        ExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(evaluate_truth(expression, row, fields)?.not().into_scalar()),
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            let is_null = matches!(evaluate(expression, row, fields)?, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
    }
}

fn evaluate_truth(
    expression: &Expr,
    row: &[ScalarValue],
    fields: &[OutputField],
) -> Result<TruthValue, ExecutionError> {
    TruthValue::from_scalar(evaluate(expression, row, fields)?)
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
    use super::{ExecutionError, QueryResult, TruthValue, evaluate_binary, execute, execute_rows};
    use netbadb_planner::{PhysicalPlan, plan};
    use netbadb_rel::{
        AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, BinaryOp, ColumnRef,
        DerivedField, Expr, ExprKind, LogicalPlan, NullOrder, SortDirection, SortKey,
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
