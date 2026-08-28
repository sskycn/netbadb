//! Synchronous execution of typed query and DML physical statements.

use std::cmp::Ordering;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::ControlFlow;

use netbadb_planner::{PartitionAccessPlan, PhysicalPlan, PhysicalStatement};
use netbadb_rel::{
    AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, Assignment, BinaryOp,
    ColumnRef, Expr, ExprKind, JoinKind, NullOrder, OutputField, SortDirection, SortKey, UnaryOp,
};
use netbadb_storage::{
    PresenceCountSummary, StorageError, StorageReadView, StorageRowHandle, StorageTransaction,
    TableStorage,
};
use netbadb_types::{
    ColumnId, PhysicalType, RelationBindingId, ScalarRef, ScalarValue, StorageId, TableId,
};

/// Runtime row capacity for the first owned batch-at-a-time execution path.
///
/// This is deliberately small and explicit so intermediate scan/operator work
/// is bounded. It is a starting point for measurement, not an optimality claim.
const EXECUTION_BATCH_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionStorageBinding {
    pub table_id: TableId,
    pub storage_id: StorageId,
}

pub struct ExecutionStorage<'a> {
    pub storage_id: StorageId,
    pub storage: &'a mut TableStorage,
}

#[derive(Clone, Copy)]
pub struct ExecutionReadView<'a> {
    pub storage_id: StorageId,
    pub view: &'a StorageReadView,
}

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
pub struct PreparedUpdateRow {
    pub row: StorageRowHandle,
    pub values: Vec<ScalarValue>,
}

#[derive(Debug)]
pub enum PreparedMutation {
    Insert {
        table_id: TableId,
        values: Vec<ScalarValue>,
    },
    Update {
        table_id: TableId,
        rows: Vec<PreparedUpdateRow>,
    },
    Delete {
        table_id: TableId,
        rows: Vec<StorageRowHandle>,
    },
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
    MissingPhysicalStorage(StorageId),
    MissingStorageReadView(StorageId),
    StorageIdentityOverflow,
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
            Self::MissingPhysicalStorage(storage_id) => write!(
                formatter,
                "physical storage {} is not attached for execution",
                storage_id.0
            ),
            Self::MissingStorageReadView(storage_id) => write!(
                formatter,
                "physical storage {} has no statement read view",
                storage_id.0
            ),
            Self::StorageIdentityOverflow => {
                formatter.write_str("execution storage identity allocation overflowed")
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
    storage: &mut TableStorage,
) -> Result<QueryResult, ExecutionError> {
    execute_with_storages(plan, std::slice::from_mut(storage))
}

/// Executes a read-only physical query through table-storage capabilities.
/// Each planned `TableId` must have exactly one corresponding storage.
pub fn execute_with_storages(
    plan: &PhysicalPlan,
    storages: &mut [TableStorage],
) -> Result<QueryResult, ExecutionError> {
    let read_views = storages
        .iter()
        .map(TableStorage::read_view)
        .collect::<Result<Vec<_>, _>>()?;
    execute_with_read_views(plan, storages, &read_views)
}

pub fn execute_with_read_views(
    plan: &PhysicalPlan,
    storages: &mut [TableStorage],
    read_views: &[StorageReadView],
) -> Result<QueryResult, ExecutionError> {
    let bindings = compatibility_bindings(storages)?;
    let mut execution_storages = storages
        .iter_mut()
        .zip(&bindings)
        .map(|(storage, binding)| ExecutionStorage {
            storage_id: binding.storage_id,
            storage,
        })
        .collect::<Vec<_>>();
    let execution_views = read_views
        .iter()
        .zip(&bindings)
        .map(|(view, binding)| ExecutionReadView {
            storage_id: binding.storage_id,
            view,
        })
        .collect::<Vec<_>>();
    execute_with_storage_context(plan, &bindings, &mut execution_storages, &execution_views)
}

pub fn execute_with_storage_context(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<QueryResult, ExecutionError> {
    let result = execute_rows_with_views(plan, bindings, storages, read_views)?;
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

fn compatibility_bindings(
    storages: &[TableStorage],
) -> Result<Vec<ExecutionStorageBinding>, ExecutionError> {
    storages
        .iter()
        .enumerate()
        .map(|(position, storage)| {
            let ordinal = position
                .checked_add(1)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or(ExecutionError::StorageIdentityOverflow)?;
            Ok(ExecutionStorageBinding {
                table_id: storage.table().id,
                storage_id: StorageId(ordinal),
            })
        })
        .collect()
}

pub fn execute_statement(
    statement: &PhysicalStatement,
    storage: &mut TableStorage,
    transaction: Option<&mut StorageTransaction>,
) -> Result<ExecutionResult, ExecutionError> {
    let mut transaction = transaction;
    let read_view = match transaction.as_deref_mut() {
        Some(transaction) => transaction.begin_statement()?,
        None => storage.read_view()?,
    };
    match statement {
        PhysicalStatement::Query(plan) => execute_with_read_views(
            plan,
            std::slice::from_mut(storage),
            std::slice::from_ref(&read_view),
        )
        .map(ExecutionResult::Query),
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
            let input = execute_rows_legacy_with_views(
                input,
                &[ExecutionStorageBinding {
                    table_id: storage.table().id,
                    storage_id: StorageId(1),
                }],
                &mut [ExecutionStorage {
                    storage_id: StorageId(1),
                    storage,
                }],
                &[ExecutionReadView {
                    storage_id: StorageId(1),
                    view: &read_view,
                }],
            )?;
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
            let input = execute_rows_legacy_with_views(
                input,
                &[ExecutionStorageBinding {
                    table_id: storage.table().id,
                    storage_id: StorageId(1),
                }],
                &mut [ExecutionStorage {
                    storage_id: StorageId(1),
                    storage,
                }],
                &[ExecutionReadView {
                    storage_id: StorageId(1),
                    view: &read_view,
                }],
            )?;
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

/// Materializes and validates every target/replacement for one DML statement
/// without performing physical mutation. Core uses this boundary to route all
/// destinations before an atomic multi-storage write begins.
pub fn prepare_mutation_with_storage_context(
    statement: &PhysicalStatement,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<PreparedMutation, ExecutionError> {
    match statement {
        PhysicalStatement::Query(_) => Err(ExecutionError::TransactionRequired),
        PhysicalStatement::Insert {
            table_id, values, ..
        } => Ok(PreparedMutation::Insert {
            table_id: *table_id,
            values: values
                .iter()
                .map(|value| evaluate(value, &[], &[]))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        PhysicalStatement::Update {
            input,
            table_id,
            assignments,
        } => {
            let input = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
            let rows = build_replacements(&input, assignments)?
                .into_iter()
                .map(|(row, values)| PreparedUpdateRow { row, values })
                .collect();
            Ok(PreparedMutation::Update {
                table_id: *table_id,
                rows,
            })
        }
        PhysicalStatement::Delete { input, table_id } => {
            let input = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
            let rows = input
                .rows
                .into_iter()
                .map(|row| row.row_id.ok_or(ExecutionError::MissingRowIdentity))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PreparedMutation::Delete {
                table_id: *table_id,
                rows,
            })
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ExecutionRow {
    row_id: Option<StorageRowHandle>,
    values: Vec<ScalarValue>,
}

#[derive(Debug, PartialEq, Eq)]
struct ExecutionRows {
    fields: Vec<OutputField>,
    rows: Vec<ExecutionRow>,
}

#[derive(Debug)]
struct ExecutionBatch {
    rows: Vec<ExecutionRow>,
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq, Eq)]
struct StreamingHashJoinStats {
    probe_rows_seen: usize,
    probe_batches_seen: usize,
    max_probe_batch_rows: usize,
    build_rows_materialized: usize,
    candidate_pairs_checked: usize,
    output_rows: usize,
}

impl ExecutionBatch {
    fn with_capacity() -> Self {
        Self {
            rows: Vec::with_capacity(EXECUTION_BATCH_CAPACITY),
        }
    }

    fn is_full_at(&self, capacity: usize) -> bool {
        self.rows.len() == capacity
    }
}

struct BatchPipeline<'a> {
    table_id: TableId,
    scan_columns: Vec<ColumnId>,
    fields: Vec<OutputField>,
    operators: Vec<BatchOperator<'a>>,
}

struct TopNPlan<'a> {
    pipeline: BatchPipeline<'a>,
    keys: &'a [SortKey],
    sort_positions: Vec<usize>,
    projection: ProjectionPlan,
    fields: Vec<OutputField>,
    limit: usize,
}

#[derive(Debug)]
struct TopNCandidate {
    input_ordinal: usize,
    row: ExecutionRow,
}

struct TopNState<'a> {
    limit: usize,
    keys: &'a [SortKey],
    sort_positions: &'a [usize],
    candidates: Vec<TopNCandidate>,
    rows_seen: usize,
    #[cfg(test)]
    candidates_inserted: usize,
    #[cfg(test)]
    max_retained: usize,
}

impl<'a> TopNState<'a> {
    fn new(limit: usize, keys: &'a [SortKey], sort_positions: &'a [usize]) -> Self {
        Self {
            limit,
            keys,
            sort_positions,
            candidates: Vec::with_capacity(limit.min(EXECUTION_BATCH_CAPACITY)),
            rows_seen: 0,
            #[cfg(test)]
            candidates_inserted: 0,
            #[cfg(test)]
            max_retained: 0,
        }
    }

    fn consider(&mut self, row: ExecutionRow) -> Result<(), ExecutionError> {
        validate_sort_row(&row, self.sort_positions, self.keys)?;
        let input_ordinal = self.rows_seen;
        self.rows_seen = self
            .rows_seen
            .checked_add(1)
            .ok_or(ExecutionError::TypeMismatch)?;
        if self.limit == 0 {
            return Ok(());
        }

        let candidate = TopNCandidate { input_ordinal, row };
        if self.candidates.len() < self.limit {
            self.candidates.push(candidate);
            let position = self.candidates.len() - 1;
            sift_top_n_candidate_up(
                &mut self.candidates,
                position,
                self.sort_positions,
                self.keys,
            )?;
            #[cfg(test)]
            {
                self.candidates_inserted += 1;
                self.max_retained = self.max_retained.max(self.candidates.len());
            }
            return Ok(());
        }

        if compare_top_n_candidates(
            &candidate,
            &self.candidates[0],
            self.sort_positions,
            self.keys,
        )? == Ordering::Less
        {
            self.candidates[0] = candidate;
            sift_top_n_candidate_down(&mut self.candidates, 0, self.sort_positions, self.keys)?;
            #[cfg(test)]
            {
                self.candidates_inserted += 1;
            }
        }
        Ok(())
    }

    fn into_sorted_rows(mut self) -> Result<Vec<ExecutionRow>, ExecutionError> {
        let mut comparison_error = None;
        self.candidates.sort_by(|left, right| {
            if comparison_error.is_some() {
                return Ordering::Equal;
            }
            match compare_top_n_candidates(left, right, self.sort_positions, self.keys) {
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
        Ok(self
            .candidates
            .into_iter()
            .map(|candidate| candidate.row)
            .collect())
    }
}

enum BatchOperator<'a> {
    Filter(BoundExpr<'a>),
    Project(ProjectionPlan),
    Limit { remaining: usize },
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

#[cfg(test)]
fn execute_rows(
    plan: &PhysicalPlan,
    storages: &mut [TableStorage],
) -> Result<ExecutionRows, ExecutionError> {
    let views = storages
        .iter()
        .map(TableStorage::read_view)
        .collect::<Result<Vec<_>, _>>()?;
    let bindings = compatibility_bindings(storages)?;
    let mut execution_storages = storages
        .iter_mut()
        .zip(&bindings)
        .map(|(storage, binding)| ExecutionStorage {
            storage_id: binding.storage_id,
            storage,
        })
        .collect::<Vec<_>>();
    let execution_views = views
        .iter()
        .zip(&bindings)
        .map(|(view, binding)| ExecutionReadView {
            storage_id: binding.storage_id,
            view,
        })
        .collect::<Vec<_>>();
    execute_rows_with_views(plan, &bindings, &mut execution_storages, &execution_views)
}

#[cfg(test)]
fn execute_rows_legacy(
    plan: &PhysicalPlan,
    storages: &mut [TableStorage],
) -> Result<ExecutionRows, ExecutionError> {
    let views = storages
        .iter()
        .map(TableStorage::read_view)
        .collect::<Result<Vec<_>, _>>()?;
    let bindings = compatibility_bindings(storages)?;
    let mut execution_storages = storages
        .iter_mut()
        .zip(&bindings)
        .map(|(storage, binding)| ExecutionStorage {
            storage_id: binding.storage_id,
            storage,
        })
        .collect::<Vec<_>>();
    let execution_views = views
        .iter()
        .zip(&bindings)
        .map(|(view, binding)| ExecutionReadView {
            storage_id: binding.storage_id,
            view,
        })
        .collect::<Vec<_>>();
    execute_rows_test_legacy_with_views(plan, &bindings, &mut execution_storages, &execution_views)
}

#[cfg(test)]
fn execute_rows_test_legacy_with_views(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<ExecutionRows, ExecutionError> {
    match plan {
        PhysicalPlan::Aggregate {
            input,
            group_keys,
            outputs,
        } => {
            let input = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
            execute_aggregate(input, group_keys, outputs)
        }
        PhysicalPlan::Limit { input, limit } => {
            let mut result =
                execute_rows_test_legacy_with_views(input, bindings, storages, read_views)?;
            result
                .rows
                .truncate(usize::try_from(*limit).unwrap_or(usize::MAX));
            Ok(result)
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
            predicate,
            columns,
            ..
        } => execute_hash_join_materialized(
            left, right, left_key, right_key, predicate, columns, bindings, storages, read_views,
        ),
        _ => execute_rows_legacy_with_views(plan, bindings, storages, read_views),
    }
}

fn execute_rows_with_views(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<ExecutionRows, ExecutionError> {
    if let Some(result) = try_execute_top_n(plan, bindings, storages, read_views)? {
        return Ok(result);
    }
    if let Some(result) =
        try_execute_streaming_filter_pipeline(plan, bindings, storages, read_views)?
    {
        return Ok(result);
    }
    if let Some(result) = try_execute_batch_pipeline(plan, bindings, storages, read_views)? {
        return Ok(result);
    }
    execute_rows_legacy_with_views(plan, bindings, storages, read_views)
}

fn try_execute_top_n(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(TopNPlan {
        mut pipeline,
        keys,
        sort_positions,
        projection,
        fields,
        limit,
    }) = build_top_n_plan(plan)
    else {
        return Ok(None);
    };
    let mut state = TopNState::new(limit, keys, &sort_positions);
    let _ = visit_batch_pipeline(&mut pipeline, bindings, storages, read_views, |batch| {
        for row in batch.rows.drain(..) {
            state.consider(row)?;
        }
        Ok(ControlFlow::Continue(()))
    })?;
    let rows = state
        .into_sorted_rows()?
        .into_iter()
        .map(|row| project_execution_row(row, &projection))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(ExecutionRows { fields, rows }))
}

fn build_top_n_plan(plan: &PhysicalPlan) -> Option<TopNPlan<'_>> {
    let PhysicalPlan::Limit { input, limit } = plan else {
        return None;
    };
    let PhysicalPlan::Project { input, columns } = input.as_ref() else {
        return None;
    };
    let PhysicalPlan::Sort { input, keys } = input.as_ref() else {
        return None;
    };
    let pipeline = match build_batch_pipeline(input) {
        Ok(Some(pipeline)) => pipeline,
        Ok(None) | Err(_) => return None,
    };
    let sort_positions = resolve_sort_positions(&pipeline.fields, keys).ok()?;
    let projection = build_projection_plan(&pipeline.fields, columns).ok()?;
    Some(TopNPlan {
        pipeline,
        keys,
        sort_positions,
        projection,
        fields: columns.iter().cloned().map(OutputField::Source).collect(),
        limit: usize::try_from(*limit).unwrap_or(usize::MAX),
    })
}

fn try_execute_streaming_filter_pipeline(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            try_execute_streaming_seq_filter(input, predicate, bindings, storages, read_views)
        }
        PhysicalPlan::Project { input, columns } => {
            let PhysicalPlan::Filter { .. } = input.as_ref() else {
                return Ok(None);
            };
            try_execute_projected_streaming_seq_filter(
                input, columns, bindings, storages, read_views,
            )
        }
        _ => Ok(None),
    }
}

fn build_batch_pipeline(plan: &PhysicalPlan) -> Result<Option<BatchPipeline<'_>>, ExecutionError> {
    match plan {
        PhysicalPlan::SeqScan {
            table_id, columns, ..
        } => Ok(Some(BatchPipeline {
            table_id: *table_id,
            scan_columns: columns.iter().map(|column| column.column_id).collect(),
            fields: columns.iter().cloned().map(OutputField::Source).collect(),
            operators: Vec::new(),
        })),
        PhysicalPlan::Filter { input, predicate } => {
            let Some(mut pipeline) = build_batch_pipeline(input)? else {
                return Ok(None);
            };
            let Ok(predicate) = bind_expression(predicate, &pipeline.fields) else {
                // Malformed hand-built plans retain the legacy evaluator's
                // row-dependent error behavior (including an empty input).
                return Ok(None);
            };
            pipeline.operators.push(BatchOperator::Filter(predicate));
            Ok(Some(pipeline))
        }
        PhysicalPlan::Project { input, columns } => {
            let Some(mut pipeline) = build_batch_pipeline(input)? else {
                return Ok(None);
            };
            let projection = build_projection_plan(&pipeline.fields, columns)?;
            pipeline.operators.push(BatchOperator::Project(projection));
            pipeline.fields = columns.iter().cloned().map(OutputField::Source).collect();
            Ok(Some(pipeline))
        }
        PhysicalPlan::Limit { input, limit } => {
            let Some(mut pipeline) = build_batch_pipeline(input)? else {
                return Ok(None);
            };
            pipeline.operators.push(BatchOperator::Limit {
                remaining: usize::try_from(*limit).unwrap_or(usize::MAX),
            });
            Ok(Some(pipeline))
        }
        PhysicalPlan::IndexScan { .. }
        | PhysicalPlan::RangeIndexScan { .. }
        | PhysicalPlan::PartitionedScan { .. }
        | PhysicalPlan::NestedLoopJoin { .. }
        | PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::Sort { .. }
        | PhysicalPlan::Aggregate { .. } => Ok(None),
    }
}

fn try_execute_batch_pipeline(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(mut pipeline) = build_batch_pipeline(plan)? else {
        return Ok(None);
    };
    let mut result_rows = Vec::new();
    let _ = visit_batch_pipeline(&mut pipeline, bindings, storages, read_views, |batch| {
        result_rows.append(&mut batch.rows);
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(Some(ExecutionRows {
        fields: pipeline.fields,
        rows: result_rows,
    }))
}

fn visit_batch_pipeline<F>(
    pipeline: &mut BatchPipeline<'_>,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
    mut consumer: F,
) -> Result<ControlFlow<()>, ExecutionError>
where
    F: FnMut(&mut ExecutionBatch) -> Result<ControlFlow<()>, ExecutionError>,
{
    let view = read_view_for_table(bindings, read_views, pipeline.table_id)?;
    let storage = storage_for_table(bindings, storages, pipeline.table_id)?;
    if pipeline
        .operators
        .iter()
        .any(|operator| matches!(operator, BatchOperator::Limit { remaining: 0 }))
    {
        return Ok(ControlFlow::Continue(()));
    }

    let mut batch = ExecutionBatch::with_capacity();
    let batch_capacity = batch_input_capacity(&pipeline.operators);
    let mut pending_operator_error = None;
    let flow = storage.visit_rows_with_view_control::<ExecutionError, _>(
        &pipeline.scan_columns,
        view,
        |row_id, values| {
            if pending_operator_error.is_some() {
                return Ok(ControlFlow::Continue(()));
            }
            batch.rows.push(ExecutionRow {
                row_id: Some(row_id),
                values,
            });
            if !batch.is_full_at(batch_capacity) {
                return Ok(ControlFlow::Continue(()));
            }
            match deliver_execution_batch(&mut batch, &mut pipeline.operators, &mut consumer) {
                Ok(flow) => Ok(flow),
                Err(error) => {
                    pending_operator_error = Some(error);
                    batch.rows.clear();
                    Ok(ControlFlow::Continue(()))
                }
            }
        },
    )?;
    if let Some(error) = pending_operator_error {
        return Err(error);
    }
    if flow.is_continue() && !batch.rows.is_empty() {
        return deliver_execution_batch(&mut batch, &mut pipeline.operators, &mut consumer);
    }
    Ok(flow)
}

fn batch_input_capacity(operators: &[BatchOperator<'_>]) -> usize {
    for operator in operators {
        match operator {
            BatchOperator::Project(_) => {}
            BatchOperator::Filter(_) => return EXECUTION_BATCH_CAPACITY,
            BatchOperator::Limit { remaining } => {
                return EXECUTION_BATCH_CAPACITY.min(*remaining);
            }
        }
    }
    EXECUTION_BATCH_CAPACITY
}

fn deliver_execution_batch<F>(
    batch: &mut ExecutionBatch,
    operators: &mut [BatchOperator<'_>],
    consumer: &mut F,
) -> Result<ControlFlow<()>, ExecutionError>
where
    F: FnMut(&mut ExecutionBatch) -> Result<ControlFlow<()>, ExecutionError>,
{
    let upstream_exhausted = process_execution_batch(batch, operators)?;
    let downstream = consumer(batch)?;
    batch.rows.clear();
    if upstream_exhausted || downstream.is_break() {
        Ok(ControlFlow::Break(()))
    } else {
        Ok(ControlFlow::Continue(()))
    }
}

fn process_execution_batch(
    batch: &mut ExecutionBatch,
    operators: &mut [BatchOperator<'_>],
) -> Result<bool, ExecutionError> {
    let mut upstream_exhausted = false;
    for operator in operators {
        match operator {
            BatchOperator::Filter(predicate) => {
                let mut evaluation_error = None;
                batch.rows.retain(|row| {
                    if evaluation_error.is_some() {
                        return false;
                    }
                    match evaluate_bound_truth(predicate, EvaluationValues::Contiguous(&row.values))
                    {
                        Ok(TruthValue::True) => true,
                        Ok(TruthValue::False | TruthValue::Unknown) => false,
                        Err(error) => {
                            evaluation_error = Some(error);
                            false
                        }
                    }
                });
                if let Some(error) = evaluation_error {
                    return Err(error);
                }
            }
            BatchOperator::Project(projection) => {
                if !projection.identity {
                    for row in &mut batch.rows {
                        let owned = std::mem::replace(
                            row,
                            ExecutionRow {
                                row_id: None,
                                values: Vec::new(),
                            },
                        );
                        *row = project_execution_row(owned, projection)?;
                    }
                }
            }
            BatchOperator::Limit { remaining } => {
                if batch.rows.len() > *remaining {
                    batch.rows.truncate(*remaining);
                }
                *remaining -= batch.rows.len();
                upstream_exhausted |= *remaining == 0;
            }
        }
    }
    Ok(upstream_exhausted)
}

fn execute_rows_legacy_with_views(
    plan: &PhysicalPlan,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<ExecutionRows, ExecutionError> {
    match plan {
        PhysicalPlan::SeqScan {
            table_id, columns, ..
        } => {
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let view = read_view_for_table(bindings, read_views, *table_id)?;
            let storage = storage_for_table(bindings, storages, *table_id)?;
            let rows = storage
                .scan_columns_with_view(&column_ids, view)?
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
            access_path,
            key,
            ..
        } => {
            let view = read_view_for_table(bindings, read_views, *table_id)?;
            let storage = storage_for_table(bindings, storages, *table_id)?;
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let rows = storage
                .point_lookup_columns_with_view(*access_path, key, &column_ids, view)?
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
        PhysicalPlan::RangeIndexScan {
            table_id,
            columns,
            access_path,
            range,
            ..
        } => {
            let view = read_view_for_table(bindings, read_views, *table_id)?;
            let storage = storage_for_table(bindings, storages, *table_id)?;
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let rows = storage
                .range_lookup_columns_with_view(*access_path, range, &column_ids, view)?
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
        PhysicalPlan::PartitionedScan {
            table_id,
            columns,
            partitions,
            ..
        } => {
            let column_ids = columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            let mut rows = Vec::new();
            for partition in partitions {
                let view = read_view_for_storage(read_views, partition.storage_id)?;
                let storage = storage_for_id(storages, partition.storage_id)?;
                ensure_table(*table_id, storage)?;
                let partition_rows = match &partition.access {
                    PartitionAccessPlan::SeqScan => {
                        storage.scan_columns_with_view(&column_ids, view)?
                    }
                    PartitionAccessPlan::IndexScan {
                        access_path, key, ..
                    } => storage.point_lookup_columns_with_view(
                        *access_path,
                        key,
                        &column_ids,
                        view,
                    )?,
                    PartitionAccessPlan::RangeIndexScan {
                        access_path, range, ..
                    } => storage.range_lookup_columns_with_view(
                        *access_path,
                        range,
                        &column_ids,
                        view,
                    )?,
                };
                rows.extend(
                    partition_rows
                        .into_iter()
                        .map(|(row_id, values)| ExecutionRow {
                            row_id: Some(row_id),
                            values,
                        }),
                );
            }
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
            let left = execute_rows_legacy_with_views(left, bindings, storages, read_views)?;
            let right = execute_rows_legacy_with_views(right, bindings, storages, read_views)?;
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
            kind,
            left_key,
            right_key,
            predicate,
            columns,
            ..
        } => {
            if let Some(result) = try_execute_streaming_hash_join_probe(
                left,
                right,
                *kind,
                left_key,
                right_key,
                predicate,
                columns,
                bindings,
                storages,
                read_views,
                #[cfg(test)]
                None,
            )? {
                return Ok(result);
            }
            execute_hash_join_materialized(
                left, right, left_key, right_key, predicate, columns, bindings, storages,
                read_views,
            )
        }
        PhysicalPlan::Filter { input, predicate } => {
            if let Some(result) =
                try_execute_streaming_seq_filter(input, predicate, bindings, storages, read_views)?
            {
                return Ok(result);
            }
            let mut result = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
            let fields = result.fields.clone();
            result.rows = match bind_expression(predicate, &fields) {
                Ok(bound_predicate) => result
                    .rows
                    .into_iter()
                    .filter_map(|row| {
                        match evaluate_bound_truth(
                            &bound_predicate,
                            EvaluationValues::Contiguous(&row.values),
                        ) {
                            Ok(TruthValue::True) => Some(Ok(row)),
                            Ok(TruthValue::False | TruthValue::Unknown) => None,
                            Err(error) => Some(Err(error)),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Err(_) => result
                    .rows
                    .into_iter()
                    .filter_map(|row| {
                        match evaluate_dynamic_borrowed_truth_values(
                            predicate,
                            EvaluationValues::Contiguous(&row.values),
                            &fields,
                        ) {
                            Ok(TruthValue::True) => Some(Ok(row)),
                            Ok(TruthValue::False | TruthValue::Unknown) => None,
                            Err(error) => Some(Err(error)),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            };
            Ok(result)
        }
        PhysicalPlan::Sort { input, keys } => {
            let mut result = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
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
            if let Some(result) = try_execute_projected_streaming_seq_filter(
                input, columns, bindings, storages, read_views,
            )? {
                return Ok(result);
            }
            let input_result =
                execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
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
            if let Some(result) = try_execute_filtered_counts(
                input, group_keys, outputs, bindings, storages, read_views,
            )? {
                Ok(result)
            } else if let Some(result) = try_execute_direct_counts(
                input, group_keys, outputs, bindings, storages, read_views,
            )? {
                Ok(result)
            } else if let Some(result) = try_execute_batch_aggregate(
                input, group_keys, outputs, bindings, storages, read_views,
            )? {
                Ok(result)
            } else {
                let input = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
                execute_aggregate(input, group_keys, outputs)
            }
        }
        PhysicalPlan::Limit { input, limit } => {
            let mut result = execute_rows_legacy_with_views(input, bindings, storages, read_views)?;
            let limit = usize::try_from(*limit).unwrap_or(usize::MAX);
            result.rows.truncate(limit);
            Ok(result)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn try_execute_streaming_hash_join_probe(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    kind: JoinKind,
    left_key: &ColumnRef,
    right_key: &ColumnRef,
    predicate: &Expr,
    columns: &[ColumnRef],
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
    #[cfg(test)] mut stats: Option<&mut StreamingHashJoinStats>,
) -> Result<Option<ExecutionRows>, ExecutionError> {
    if !matches!(kind, JoinKind::Inner)
        || !left_key.data_type.is_compatible_with(&right_key.data_type)
    {
        return Ok(None);
    }
    let PhysicalPlan::SeqScan { .. } = left else {
        return Ok(None);
    };
    let PhysicalPlan::SeqScan {
        columns: right_columns,
        ..
    } = right
    else {
        return Ok(None);
    };
    let Some(mut pipeline) = build_batch_pipeline(left).ok().flatten() else {
        return Ok(None);
    };
    let Ok(left_key_position) = find_exact_source_position(&pipeline.fields, left_key) else {
        return Ok(None);
    };
    let right_fields = right_columns
        .iter()
        .cloned()
        .map(OutputField::Source)
        .collect::<Vec<_>>();
    let Ok(right_key_position) = find_exact_source_position(&right_fields, right_key) else {
        return Ok(None);
    };
    let mut joined_fields = pipeline.fields.clone();
    joined_fields.extend(right_fields);
    let Ok(output_positions) = columns
        .iter()
        .map(|column| find_exact_source_position(&joined_fields, column))
        .collect::<Result<Vec<_>, _>>()
    else {
        return Ok(None);
    };
    if !expression_sources_match_fields(predicate, &joined_fields) {
        return Ok(None);
    }
    let Ok(bound_predicate) = bind_expression(predicate, &joined_fields) else {
        return Ok(None);
    };
    let fields = columns
        .iter()
        .cloned()
        .map(OutputField::Source)
        .collect::<Vec<_>>();

    let right = execute_rows_legacy_with_views(right, bindings, storages, read_views)?;
    #[cfg(test)]
    if let Some(stats) = stats.as_deref_mut() {
        stats.build_rows_materialized = right.rows.len();
    }
    let buckets = build_hash_join_buckets(&right, right_key_position, right_key)?;
    let mut rows = Vec::new();
    let _ = visit_batch_pipeline(&mut pipeline, bindings, storages, read_views, |batch| {
        #[cfg(test)]
        if let Some(stats) = stats.as_deref_mut() {
            stats.probe_batches_seen += 1;
            stats.max_probe_batch_rows = stats.max_probe_batch_rows.max(batch.rows.len());
        }
        for left_row in &batch.rows {
            #[cfg(test)]
            if let Some(stats) = stats.as_deref_mut() {
                stats.probe_rows_seen += 1;
            }
            probe_hash_join_row(
                left_row,
                left_key_position,
                left_key,
                &right,
                &buckets,
                &bound_predicate,
                &output_positions,
                &mut rows,
                #[cfg(test)]
                stats.as_deref_mut(),
            )?;
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(Some(ExecutionRows { fields, rows }))
}

fn find_exact_source_position(
    fields: &[OutputField],
    column: &ColumnRef,
) -> Result<usize, ExecutionError> {
    let position = find_source_position(fields, column)?;
    if fields.get(position).and_then(OutputField::source_column) == Some(column) {
        Ok(position)
    } else {
        Err(ExecutionError::TypeMismatch)
    }
}

fn expression_sources_match_fields(expression: &Expr, fields: &[OutputField]) -> bool {
    match &expression.kind {
        ExprKind::Column(column) => find_exact_source_position(fields, column).is_ok(),
        ExprKind::Literal(_) => true,
        ExprKind::Binary { left, right, .. } => {
            expression_sources_match_fields(left, fields)
                && expression_sources_match_fields(right, fields)
        }
        ExprKind::Unary { expression, .. } | ExprKind::IsNull { expression, .. } => {
            expression_sources_match_fields(expression, fields)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_hash_join_materialized(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    left_key: &ColumnRef,
    right_key: &ColumnRef,
    predicate: &Expr,
    columns: &[ColumnRef],
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<ExecutionRows, ExecutionError> {
    if !left_key.data_type.is_compatible_with(&right_key.data_type) {
        return Err(ExecutionError::TypeMismatch);
    }
    let left = execute_rows_legacy_with_views(left, bindings, storages, read_views)?;
    let right = execute_rows_legacy_with_views(right, bindings, storages, read_views)?;
    let left_key_position = find_source_position(&left.fields, left_key)?;
    let right_key_position = find_source_position(&right.fields, right_key)?;
    let buckets = build_hash_join_buckets(&right, right_key_position, right_key)?;
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
        probe_hash_join_row(
            &left_row,
            left_key_position,
            left_key,
            &right,
            &buckets,
            &bound_predicate,
            &output_positions,
            &mut rows,
            #[cfg(test)]
            None,
        )?;
    }
    Ok(ExecutionRows { fields, rows })
}

fn build_hash_join_buckets(
    right: &ExecutionRows,
    right_key_position: usize,
    right_key: &ColumnRef,
) -> Result<HashMap<ScalarValue, Vec<usize>>, ExecutionError> {
    let mut buckets = HashMap::<ScalarValue, Vec<usize>>::new();
    for (right_index, right_row) in right.rows.iter().enumerate() {
        let key = hash_join_key(right_row, right_key_position, right_key)?;
        if let Some(key) = key {
            buckets.entry(key.clone()).or_default().push(right_index);
        }
    }
    Ok(buckets)
}

#[allow(clippy::too_many_arguments)]
fn probe_hash_join_row(
    left_row: &ExecutionRow,
    left_key_position: usize,
    left_key: &ColumnRef,
    right: &ExecutionRows,
    buckets: &HashMap<ScalarValue, Vec<usize>>,
    bound_predicate: &BoundExpr<'_>,
    output_positions: &[usize],
    rows: &mut Vec<ExecutionRow>,
    #[cfg(test)] mut stats: Option<&mut StreamingHashJoinStats>,
) -> Result<(), ExecutionError> {
    let Some(key) = hash_join_key(left_row, left_key_position, left_key)? else {
        return Ok(());
    };
    let Some(right_indices) = buckets.get(key) else {
        return Ok(());
    };
    for right_index in right_indices {
        #[cfg(test)]
        if let Some(stats) = stats.as_deref_mut() {
            stats.candidate_pairs_checked += 1;
        }
        let Some(right_row) = right.rows.get(*right_index) else {
            return Err(ExecutionError::TypeMismatch);
        };
        if evaluate_bound_truth(
            bound_predicate,
            EvaluationValues::Joined {
                left: &left_row.values,
                right: &right_row.values,
            },
        )? == TruthValue::True
        {
            let values =
                project_join_values(&left_row.values, &right_row.values, output_positions)?;
            rows.push(ExecutionRow {
                row_id: None,
                values,
            });
            #[cfg(test)]
            if let Some(stats) = stats.as_deref_mut() {
                stats.output_rows += 1;
            }
        }
    }
    Ok(())
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
        validate_sort_row(row, positions, keys)?;
    }
    Ok(())
}

fn validate_sort_row(
    row: &ExecutionRow,
    positions: &[usize],
    keys: &[SortKey],
) -> Result<(), ExecutionError> {
    for (position, key) in positions.iter().zip(keys) {
        let value = row
            .values
            .get(*position)
            .ok_or_else(|| ExecutionError::MissingColumn(key.column.name.clone()))?;
        if !value.matches_type(&key.column.data_type) {
            return Err(ExecutionError::TypeMismatch);
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

fn compare_top_n_candidates(
    left: &TopNCandidate,
    right: &TopNCandidate,
    positions: &[usize],
    keys: &[SortKey],
) -> Result<Ordering, ExecutionError> {
    let ordering = compare_sort_rows(&left.row, &right.row, positions, keys)?;
    Ok(if ordering == Ordering::Equal {
        left.input_ordinal.cmp(&right.input_ordinal)
    } else {
        ordering
    })
}

fn sift_top_n_candidate_up(
    candidates: &mut [TopNCandidate],
    mut position: usize,
    sort_positions: &[usize],
    keys: &[SortKey],
) -> Result<(), ExecutionError> {
    while position > 0 {
        let parent = (position - 1) / 2;
        if compare_top_n_candidates(
            &candidates[parent],
            &candidates[position],
            sort_positions,
            keys,
        )? != Ordering::Less
        {
            break;
        }
        candidates.swap(parent, position);
        position = parent;
    }
    Ok(())
}

fn sift_top_n_candidate_down(
    candidates: &mut [TopNCandidate],
    mut position: usize,
    sort_positions: &[usize],
    keys: &[SortKey],
) -> Result<(), ExecutionError> {
    loop {
        let left = position * 2 + 1;
        if left >= candidates.len() {
            return Ok(());
        }
        let right = left + 1;
        let mut worse = left;
        if right < candidates.len()
            && compare_top_n_candidates(
                &candidates[left],
                &candidates[right],
                sort_positions,
                keys,
            )? == Ordering::Less
        {
            worse = right;
        }
        if compare_top_n_candidates(
            &candidates[position],
            &candidates[worse],
            sort_positions,
            keys,
        )? != Ordering::Less
        {
            return Ok(());
        }
        candidates.swap(position, worse);
        position = worse;
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
    Min(ExtremeState),
    Max(ExtremeState),
}

#[derive(Debug)]
enum ExtremeState {
    Bool(Option<bool>),
    Int64(Option<i64>),
    UInt64(Option<u64>),
    Text(Option<String>),
}

impl ExtremeState {
    const fn empty(physical: PhysicalType) -> Self {
        match physical {
            PhysicalType::Bool => Self::Bool(None),
            PhysicalType::Int64 => Self::Int64(None),
            PhysicalType::UInt64 => Self::UInt64(None),
            PhysicalType::Text => Self::Text(None),
        }
    }

    fn compare_candidate(
        &self,
        candidate: &ScalarValue,
    ) -> Result<Option<Ordering>, ExecutionError> {
        match (self, candidate) {
            (Self::Bool(None), ScalarValue::Bool(_))
            | (Self::Int64(None), ScalarValue::Int64(_))
            | (Self::UInt64(None), ScalarValue::UInt64(_))
            | (Self::Text(None), ScalarValue::Text(_)) => Ok(None),
            (Self::Bool(Some(current)), ScalarValue::Bool(candidate)) => {
                Ok(Some(candidate.cmp(current)))
            }
            (Self::Int64(Some(current)), ScalarValue::Int64(candidate)) => {
                Ok(Some(candidate.cmp(current)))
            }
            (Self::UInt64(Some(current)), ScalarValue::UInt64(candidate)) => {
                Ok(Some(candidate.cmp(current)))
            }
            (Self::Text(Some(current)), ScalarValue::Text(candidate)) => {
                Ok(Some(candidate.as_str().cmp(current.as_str())))
            }
            _ => Err(ExecutionError::TypeMismatch),
        }
    }

    fn replace(&mut self, candidate: ScalarValue) -> Result<(), ExecutionError> {
        match (self, candidate) {
            (Self::Bool(current), ScalarValue::Bool(candidate)) => *current = Some(candidate),
            (Self::Int64(current), ScalarValue::Int64(candidate)) => *current = Some(candidate),
            (Self::UInt64(current), ScalarValue::UInt64(candidate)) => *current = Some(candidate),
            (Self::Text(current), ScalarValue::Text(candidate)) => *current = Some(candidate),
            _ => return Err(ExecutionError::TypeMismatch),
        }
        Ok(())
    }

    fn into_scalar(self) -> ScalarValue {
        match self {
            Self::Bool(value) => value.map_or(ScalarValue::Null, ScalarValue::Bool),
            Self::Int64(value) => value.map_or(ScalarValue::Null, ScalarValue::Int64),
            Self::UInt64(value) => value.map_or(ScalarValue::Null, ScalarValue::UInt64),
            Self::Text(value) => value.map_or(ScalarValue::Null, ScalarValue::Text),
        }
    }
}

#[derive(Debug)]
struct GroupState {
    key_values: Vec<ScalarValue>,
    aggregate_states: Vec<AggregateState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrehashedKey(u64);

impl Hash for PrehashedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PrehashedBuildHasher;

impl BuildHasher for PrehashedBuildHasher {
    type Hasher = PrehashedHasher;

    fn build_hasher(&self) -> Self::Hasher {
        PrehashedHasher {
            value: 0,
            written: false,
        }
    }
}

#[derive(Debug)]
struct PrehashedHasher {
    value: u64,
    written: bool,
}

impl Hasher for PrehashedHasher {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, _bytes: &[u8]) {
        panic!("PrehashedHasher accepts only one opaque u64 prehash")
    }

    fn write_u64(&mut self, value: u64) {
        if self.written {
            panic!("PrehashedHasher accepts only one opaque u64 prehash");
        }
        self.value = value;
        self.written = true;
    }
}

struct GroupLookup {
    key_hasher: RandomState,
    bucket_heads: HashMap<PrehashedKey, usize, PrehashedBuildHasher>,
    collision_next: Vec<Option<usize>>,
    #[cfg(test)]
    stats: GroupLookupStats,
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct GroupLookupStats {
    lookups: usize,
    hits: usize,
    misses: usize,
    owned_key_materializations: usize,
    owned_key_moves: usize,
    owned_key_clones: usize,
    exact_collision_checks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GroupLookupProbe {
    hash: u64,
    group_index: Option<usize>,
}

impl GroupLookup {
    fn new() -> Self {
        Self {
            key_hasher: RandomState::new(),
            bucket_heads: HashMap::with_hasher(PrehashedBuildHasher),
            collision_next: Vec::new(),
            #[cfg(test)]
            stats: GroupLookupStats::default(),
        }
    }

    fn probe(
        &mut self,
        row: &ExecutionRow,
        positions: &[usize],
        group_keys: &[ColumnRef],
        groups: &[GroupState],
    ) -> Result<GroupLookupProbe, ExecutionError> {
        let hash = hash_group_key(&self.key_hasher, row, positions, group_keys)?;
        #[cfg(test)]
        {
            self.stats.lookups += 1;
        }
        let head = self.bucket_heads.get(&PrehashedKey(hash)).copied();
        let group_index = self.find_in_bucket(row, positions, group_keys, groups, head)?;
        #[cfg(test)]
        if group_index.is_some() {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        Ok(GroupLookupProbe { hash, group_index })
    }

    fn find_in_bucket(
        &mut self,
        row: &ExecutionRow,
        positions: &[usize],
        group_keys: &[ColumnRef],
        groups: &[GroupState],
        mut candidate: Option<usize>,
    ) -> Result<Option<usize>, ExecutionError> {
        while let Some(index) = candidate {
            let group = groups.get(index).ok_or(ExecutionError::TypeMismatch)?;
            #[cfg(test)]
            {
                self.stats.exact_collision_checks += 1;
            }
            if group_key_matches(row, positions, group_keys, &group.key_values)? {
                return Ok(Some(index));
            }
            candidate = self
                .collision_next
                .get(index)
                .copied()
                .ok_or(ExecutionError::TypeMismatch)?;
        }
        Ok(None)
    }

    fn register_group(&mut self, hash: u64, group_index: usize) -> Result<(), ExecutionError> {
        if self.collision_next.len() != group_index {
            return Err(ExecutionError::TypeMismatch);
        }
        let previous_head = self.bucket_heads.insert(PrehashedKey(hash), group_index);
        self.collision_next.push(previous_head);
        Ok(())
    }

    fn record_owned_key_materialization(&mut self) {
        #[cfg(test)]
        {
            self.stats.owned_key_materializations += 1;
        }
    }

    #[cfg(test)]
    fn record_owned_key_transfer(&mut self, key_owners: usize, durable_owners: usize) {
        if key_owners != 0 {
            self.stats.owned_key_moves += 1;
            self.stats.owned_key_clones += durable_owners.saturating_sub(1);
        }
    }

    #[cfg(test)]
    const fn stats(&self) -> GroupLookupStats {
        self.stats
    }
}

fn hash_group_key(
    build_hasher: &RandomState,
    row: &ExecutionRow,
    positions: &[usize],
    group_keys: &[ColumnRef],
) -> Result<u64, ExecutionError> {
    if positions.len() != group_keys.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    let mut hasher = build_hasher.build_hasher();
    positions.len().hash(&mut hasher);
    for (position, column) in positions.iter().zip(group_keys) {
        let value = row
            .values
            .get(*position)
            .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))?;
        value.hash(&mut hasher);
    }
    Ok(hasher.finish())
}

fn group_key_matches(
    row: &ExecutionRow,
    positions: &[usize],
    group_keys: &[ColumnRef],
    owned_key: &[ScalarValue],
) -> Result<bool, ExecutionError> {
    if positions.len() != group_keys.len() || positions.len() != owned_key.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    for ((position, column), expected) in positions.iter().zip(group_keys).zip(owned_key) {
        let value = row
            .values
            .get(*position)
            .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))?;
        if !value.matches_type(&column.data_type) {
            return Err(ExecutionError::TypeMismatch);
        }
        if value != expected {
            return Ok(false);
        }
    }
    Ok(true)
}

fn materialize_group_key(
    row: &ExecutionRow,
    positions: &[usize],
    group_keys: &[ColumnRef],
) -> Result<Vec<ScalarValue>, ExecutionError> {
    if positions.len() != group_keys.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    positions
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
        .collect()
}

fn validate_group_key(
    row: &ExecutionRow,
    positions: &[usize],
    group_keys: &[ColumnRef],
) -> Result<(), ExecutionError> {
    if positions.len() != group_keys.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    for (position, column) in positions.iter().zip(group_keys) {
        let value = row
            .values
            .get(*position)
            .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))?;
        if !value.matches_type(&column.data_type) {
            return Err(ExecutionError::TypeMismatch);
        }
    }
    Ok(())
}

struct AggregateAccumulator<'a> {
    group_keys: &'a [ColumnRef],
    aggregates: Vec<&'a AggregateExpr>,
    has_extremes: bool,
    group_key_positions: Vec<usize>,
    group_key_targets: Vec<Vec<usize>>,
    aggregate_positions: Vec<Option<usize>>,
    replacement_targets: Vec<Vec<usize>>,
    output_projection: ProjectionPlan,
    output_fields: Vec<OutputField>,
    group_lookup: GroupLookup,
    groups: Vec<GroupState>,
    #[cfg(test)]
    grouped_batch_stats: GroupedBatchStats,
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct GroupedBatchStats {
    grouped_rows: usize,
    borrow_only_hits: usize,
    miss_rows: usize,
    rows_with_scalar_transfer: usize,
    extrema_replacement_rows: usize,
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

#[derive(Debug)]
struct FilteredCountPlan<'a> {
    table_id: TableId,
    predicate: &'a Expr,
    predicate_columns: Vec<&'a ColumnRef>,
    presence_columns: Vec<&'a ColumnRef>,
    outputs: Vec<DirectCountOutput<'a>>,
    star_aggregate: Option<&'a AggregateExpr>,
    presence_aggregates: Vec<&'a AggregateExpr>,
}

#[derive(Debug)]
struct FilteredCountSummary {
    qualified_rows: u128,
    non_null_counts: Vec<u128>,
}

struct StreamingSeqFilterPlan<'a> {
    table_id: TableId,
    columns: &'a [ColumnRef],
    predicate: &'a Expr,
}

struct ProjectedStreamingSeqFilterPlan<'a> {
    filter: StreamingSeqFilterPlan<'a>,
    output_positions: Vec<usize>,
    output_fields: Vec<OutputField>,
}

type SourceIdentity = (RelationBindingId, TableId, ColumnId);

fn source_identity(column: &ColumnRef) -> SourceIdentity {
    (column.binding_id, column.table_id, column.column_id)
}

fn collect_filter_columns(predicate: &Expr) -> BTreeSet<SourceIdentity> {
    fn collect(expression: &Expr, columns: &mut BTreeSet<SourceIdentity>) {
        match &expression.kind {
            ExprKind::Column(column) => {
                columns.insert(source_identity(column));
            }
            ExprKind::Literal(_) => {}
            ExprKind::Binary { left, right, .. } => {
                collect(left, columns);
                collect(right, columns);
            }
            ExprKind::Unary { expression, .. } | ExprKind::IsNull { expression, .. } => {
                collect(expression, columns);
            }
        }
    }

    let mut columns = BTreeSet::new();
    collect(predicate, &mut columns);
    columns
}

fn streaming_seq_filter_eligibility<'a>(
    input: &'a PhysicalPlan,
    predicate: &'a Expr,
) -> Option<StreamingSeqFilterPlan<'a>> {
    let PhysicalPlan::SeqScan {
        binding_id,
        table_id,
        columns,
        ..
    } = input
    else {
        return None;
    };
    let scan_identities = columns.iter().map(source_identity).collect::<BTreeSet<_>>();
    if scan_identities.len() != columns.len()
        || columns
            .iter()
            .any(|column| column.binding_id != *binding_id || column.table_id != *table_id)
    {
        return None;
    }
    if collect_filter_columns(predicate)
        .iter()
        .any(|identity| !scan_identities.contains(identity))
    {
        return None;
    }
    Some(StreamingSeqFilterPlan {
        table_id: *table_id,
        columns,
        predicate,
    })
}

fn try_execute_streaming_seq_filter(
    input: &PhysicalPlan,
    predicate: &Expr,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(plan) = streaming_seq_filter_eligibility(input, predicate) else {
        return Ok(None);
    };
    let output_fields = plan
        .columns
        .iter()
        .cloned()
        .map(OutputField::Source)
        .collect::<Vec<_>>();
    let output_positions = (0..plan.columns.len()).collect::<Vec<_>>();
    execute_streaming_seq_filter_with_projection(
        &plan,
        &output_positions,
        output_fields,
        bindings,
        storages,
        read_views,
    )
    .map(Some)
}

fn projected_streaming_seq_filter_eligibility<'a>(
    input: &'a PhysicalPlan,
    project_columns: &[ColumnRef],
) -> Option<ProjectedStreamingSeqFilterPlan<'a>> {
    let PhysicalPlan::Filter { input, predicate } = input else {
        return None;
    };
    let filter = streaming_seq_filter_eligibility(input, predicate)?;
    let predicate_identities = collect_filter_columns(predicate);
    let project_identities = project_columns
        .iter()
        .map(|column| (column.binding_id, column.column_id))
        .collect::<BTreeSet<_>>();
    let predicate_only_exists = filter.columns.iter().any(|column| {
        predicate_identities.contains(&source_identity(column))
            && !project_identities.contains(&(column.binding_id, column.column_id))
    });
    if !predicate_only_exists
        || filter.columns.iter().any(|column| {
            !predicate_identities.contains(&source_identity(column))
                && !project_identities.contains(&(column.binding_id, column.column_id))
        })
    {
        return None;
    }
    let predicate_fields = filter
        .columns
        .iter()
        .cloned()
        .map(OutputField::Source)
        .collect::<Vec<_>>();
    let output_positions = project_columns
        .iter()
        .map(|column| find_source_position(&predicate_fields, column))
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some(ProjectedStreamingSeqFilterPlan {
        filter,
        output_positions,
        output_fields: project_columns
            .iter()
            .cloned()
            .map(OutputField::Source)
            .collect(),
    })
}

fn try_execute_projected_streaming_seq_filter(
    input: &PhysicalPlan,
    project_columns: &[ColumnRef],
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(plan) = projected_streaming_seq_filter_eligibility(input, project_columns) else {
        return Ok(None);
    };
    execute_streaming_seq_filter_with_projection(
        &plan.filter,
        &plan.output_positions,
        plan.output_fields,
        bindings,
        storages,
        read_views,
    )
    .map(Some)
}

fn execute_streaming_seq_filter_with_projection(
    plan: &StreamingSeqFilterPlan<'_>,
    output_positions: &[usize],
    output_fields: Vec<OutputField>,
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<ExecutionRows, ExecutionError> {
    let predicate_fields = plan
        .columns
        .iter()
        .cloned()
        .map(OutputField::Source)
        .collect::<Vec<_>>();
    let column_ids = plan
        .columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let view = read_view_for_table(bindings, read_views, plan.table_id)?;
    let storage = storage_for_table(bindings, storages, plan.table_id)?;
    let mut rows = Vec::new();
    let mut pending_predicate_error = None;
    match bind_expression(plan.predicate, &predicate_fields) {
        Ok(bound_predicate) => {
            storage.visit_row_scalar_refs_with_presence_view::<ExecutionError, _>(
                &column_ids,
                &[],
                view,
                |row_id, values, _presence| {
                    collect_streaming_filter_row(
                        &bound_predicate,
                        output_positions,
                        Some(row_id),
                        values,
                        &mut rows,
                        &mut pending_predicate_error,
                    );
                    Ok(())
                },
            )?;
        }
        Err(_) => {
            // Hand-built malformed plans keep the row-dependent error timing
            // of the authoritative dynamic evaluator, including empty scans.
            storage.visit_row_scalar_refs_with_presence_view::<ExecutionError, _>(
                &column_ids,
                &[],
                view,
                |row_id, values, _presence| {
                    collect_dynamic_streaming_filter_row(
                        plan.predicate,
                        &predicate_fields,
                        output_positions,
                        Some(row_id),
                        values,
                        &mut rows,
                        &mut pending_predicate_error,
                    );
                    Ok(())
                },
            )?;
        }
    }
    if let Some(error) = pending_predicate_error {
        return Err(error);
    }
    Ok(ExecutionRows {
        fields: output_fields,
        rows,
    })
}

fn collect_streaming_filter_row(
    predicate: &BoundExpr<'_>,
    output_positions: &[usize],
    row_id: Option<StorageRowHandle>,
    values: &[ScalarRef<'_>],
    rows: &mut Vec<ExecutionRow>,
    pending_predicate_error: &mut Option<ExecutionError>,
) {
    if pending_predicate_error.is_some() {
        return;
    }
    collect_evaluated_streaming_filter_row(
        evaluate_bound_scalar_ref_truth(predicate, values),
        output_positions,
        row_id,
        values,
        rows,
        pending_predicate_error,
    );
}

fn collect_dynamic_streaming_filter_row(
    predicate: &Expr,
    predicate_fields: &[OutputField],
    output_positions: &[usize],
    row_id: Option<StorageRowHandle>,
    values: &[ScalarRef<'_>],
    rows: &mut Vec<ExecutionRow>,
    pending_predicate_error: &mut Option<ExecutionError>,
) {
    if pending_predicate_error.is_some() {
        return;
    }
    collect_evaluated_streaming_filter_row(
        evaluate_dynamic_scalar_ref_truth(predicate, values, predicate_fields),
        output_positions,
        row_id,
        values,
        rows,
        pending_predicate_error,
    );
}

fn collect_evaluated_streaming_filter_row(
    truth: Result<TruthValue, ExecutionError>,
    output_positions: &[usize],
    row_id: Option<StorageRowHandle>,
    values: &[ScalarRef<'_>],
    rows: &mut Vec<ExecutionRow>,
    pending_predicate_error: &mut Option<ExecutionError>,
) {
    match truth {
        Ok(TruthValue::True) => {
            let projected_values = output_positions
                .iter()
                .map(|position| {
                    values
                        .get(*position)
                        .copied()
                        .map(ScalarRef::to_owned)
                        .ok_or(ExecutionError::TypeMismatch)
                })
                .collect::<Result<Vec<_>, _>>();
            match projected_values {
                Ok(values) => rows.push(ExecutionRow { row_id, values }),
                Err(error) => *pending_predicate_error = Some(error),
            }
        }
        Ok(TruthValue::False | TruthValue::Unknown) => {}
        Err(error) => *pending_predicate_error = Some(error),
    }
}

fn filtered_count_eligibility<'a>(
    input: &'a PhysicalPlan,
    group_keys: &[ColumnRef],
    outputs: &'a [AggregateOutput],
) -> Option<FilteredCountPlan<'a>> {
    if !group_keys.is_empty() || outputs.is_empty() {
        return None;
    }
    let PhysicalPlan::Filter { input, predicate } = input else {
        return None;
    };
    let PhysicalPlan::SeqScan {
        binding_id,
        table_id,
        columns,
        ..
    } = input.as_ref()
    else {
        return None;
    };
    let scan_identities = columns.iter().map(source_identity).collect::<BTreeSet<_>>();
    if scan_identities.len() != columns.len()
        || columns
            .iter()
            .any(|column| column.binding_id != *binding_id || column.table_id != *table_id)
    {
        return None;
    }

    let predicate_identities = collect_filter_columns(predicate);
    if predicate_identities
        .iter()
        .any(|(binding, table, _)| binding != binding_id || table != table_id)
    {
        return None;
    }
    let predicate_columns = columns
        .iter()
        .filter(|column| predicate_identities.contains(&source_identity(column)))
        .collect::<Vec<_>>();
    if predicate_columns.len() != predicate_identities.len() {
        return None;
    }

    let mut count_identities = BTreeSet::new();
    let mut star_aggregate = None;
    for output in outputs {
        let AggregateOutput::Aggregate(aggregate) = output else {
            return None;
        };
        if aggregate.function != AggregateFunction::Count {
            return None;
        }
        match &aggregate.input {
            AggregateInput::All => {
                if star_aggregate.is_none() {
                    star_aggregate = Some(aggregate);
                }
            }
            AggregateInput::Column(column) => {
                if column.binding_id != *binding_id || column.table_id != *table_id {
                    return None;
                }
                count_identities.insert(source_identity(column));
            }
        }
    }
    if count_identities.is_empty() {
        return None;
    }
    let presence_columns = columns
        .iter()
        .filter(|column| count_identities.contains(&source_identity(column)))
        .collect::<Vec<_>>();
    if presence_columns.len() != count_identities.len()
        || columns.iter().any(|column| {
            let identity = source_identity(column);
            !predicate_identities.contains(&identity) && !count_identities.contains(&identity)
        })
    {
        return None;
    }

    let presence_aggregates = presence_columns
        .iter()
        .map(|column| {
            outputs.iter().find_map(|output| {
                let AggregateOutput::Aggregate(aggregate) = output else {
                    return None;
                };
                match &aggregate.input {
                    AggregateInput::Column(candidate)
                        if source_identity(candidate) == source_identity(column) =>
                    {
                        Some(aggregate)
                    }
                    AggregateInput::All | AggregateInput::Column(_) => None,
                }
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let direct_outputs = outputs
        .iter()
        .map(|output| {
            let AggregateOutput::Aggregate(aggregate) = output else {
                return None;
            };
            let source = match &aggregate.input {
                AggregateInput::All => DirectCountSource::All,
                AggregateInput::Column(column) => {
                    DirectCountSource::Column(presence_columns.iter().position(|candidate| {
                        source_identity(candidate) == source_identity(column)
                    })?)
                }
            };
            Some(DirectCountOutput { source, aggregate })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(FilteredCountPlan {
        table_id: *table_id,
        predicate,
        predicate_columns,
        presence_columns,
        outputs: direct_outputs,
        star_aggregate,
        presence_aggregates,
    })
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
                DirectCountSource::Column(position)
            }
        };
        direct_outputs.push(DirectCountOutput { source, aggregate });
    }
    if used_scan_columns.iter().any(|used| !used) {
        return None;
    }
    Some(DirectCountPlan {
        table_id: *table_id,
        scan_columns: columns,
        outputs: direct_outputs,
    })
}

fn try_execute_filtered_counts(
    input: &PhysicalPlan,
    group_keys: &[ColumnRef],
    outputs: &[AggregateOutput],
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(plan) = filtered_count_eligibility(input, group_keys, outputs) else {
        return Ok(None);
    };
    let predicate_column_ids = plan
        .predicate_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let presence_column_ids = plan
        .presence_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let predicate_fields = plan
        .predicate_columns
        .iter()
        .map(|column| OutputField::Source((*column).clone()))
        .collect::<Vec<_>>();
    let bound_predicate = bind_expression(plan.predicate, &predicate_fields)?;
    let mut summary = FilteredCountSummary {
        qualified_rows: 0,
        non_null_counts: vec![0; plan.presence_columns.len()],
    };
    let view = read_view_for_table(bindings, read_views, plan.table_id)?;
    storage_for_table(bindings, storages, plan.table_id)?
        .visit_scalar_refs_with_presence_view::<ExecutionError, _>(
            &predicate_column_ids,
            &presence_column_ids,
            view,
            |values, presence| {
                if evaluate_bound_scalar_ref_truth(&bound_predicate, values)? == TruthValue::True {
                    update_filtered_count_summary(&plan, &mut summary, presence)?;
                }
                Ok(())
            },
        )?;
    let values = materialize_count_values(
        &plan.outputs,
        summary.qualified_rows,
        &summary.non_null_counts,
    )?;
    Ok(Some(ExecutionRows {
        fields: outputs.iter().map(AggregateOutput::output_field).collect(),
        rows: vec![ExecutionRow {
            row_id: None,
            values,
        }],
    }))
}

fn update_filtered_count_summary(
    plan: &FilteredCountPlan<'_>,
    summary: &mut FilteredCountSummary,
    presence: &[bool],
) -> Result<(), ExecutionError> {
    if presence.len() != plan.presence_aggregates.len()
        || summary.non_null_counts.len() != plan.presence_aggregates.len()
    {
        return Err(ExecutionError::TypeMismatch);
    }
    if let Some(aggregate) = plan.star_aggregate {
        summary.qualified_rows = summary
            .qualified_rows
            .checked_add(1)
            .ok_or_else(|| aggregate_overflow(aggregate))?;
    }
    for ((count, present), aggregate) in summary
        .non_null_counts
        .iter_mut()
        .zip(presence.iter().copied())
        .zip(&plan.presence_aggregates)
    {
        if present {
            *count = count
                .checked_add(1)
                .ok_or_else(|| aggregate_overflow(aggregate))?;
        }
    }
    Ok(())
}

fn try_execute_direct_counts(
    input: &PhysicalPlan,
    group_keys: &[ColumnRef],
    outputs: &[AggregateOutput],
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let Some(plan) = direct_count_eligibility(input, group_keys, outputs) else {
        return Ok(None);
    };
    let column_ids = plan
        .scan_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<Vec<_>>();
    let view = read_view_for_table(bindings, read_views, plan.table_id)?;
    let summary = storage_for_table(bindings, storages, plan.table_id)?
        .scan_presence_counts_with_view(&column_ids, view)?;
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
    materialize_count_values(&plan.outputs, summary.live_rows, &summary.non_null_counts)
}

fn materialize_count_values(
    outputs: &[DirectCountOutput<'_>],
    live_rows: u128,
    non_null_counts: &[u128],
) -> Result<Vec<ScalarValue>, ExecutionError> {
    outputs
        .iter()
        .map(|output| {
            let count = match output.source {
                DirectCountSource::All => live_rows,
                DirectCountSource::Column(position) => *non_null_counts
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

fn try_execute_batch_aggregate(
    input: &PhysicalPlan,
    group_keys: &[ColumnRef],
    outputs: &[AggregateOutput],
    bindings: &[ExecutionStorageBinding],
    storages: &mut [ExecutionStorage<'_>],
    read_views: &[ExecutionReadView<'_>],
) -> Result<Option<ExecutionRows>, ExecutionError> {
    let mut pipeline = match build_batch_pipeline(input) {
        Ok(Some(pipeline)) => pipeline,
        Ok(None) | Err(_) => return Ok(None),
    };
    let mut accumulator = match AggregateAccumulator::new(&pipeline.fields, group_keys, outputs) {
        Ok(accumulator) => accumulator,
        Err(_) => return Ok(None),
    };
    let _ = visit_batch_pipeline(&mut pipeline, bindings, storages, read_views, |batch| {
        accumulator.consume_batch(batch)?;
        Ok(ControlFlow::Continue(()))
    })?;
    accumulator.finish().map(Some)
}

fn execute_aggregate(
    input: ExecutionRows,
    group_keys: &[ColumnRef],
    outputs: &[AggregateOutput],
) -> Result<ExecutionRows, ExecutionError> {
    let mut accumulator = AggregateAccumulator::new(&input.fields, group_keys, outputs)?;
    accumulator.consume_rows(&input.rows)?;
    accumulator.finish()
}

impl<'a> AggregateAccumulator<'a> {
    fn new(
        input_fields: &[OutputField],
        group_keys: &'a [ColumnRef],
        outputs: &'a [AggregateOutput],
    ) -> Result<Self, ExecutionError> {
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
            .map(|column| find_source_position(input_fields, column))
            .collect::<Result<Vec<_>, _>>()?;
        let mut group_key_targets = (0..input_fields.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for (key_target, source_position) in group_key_positions.iter().copied().enumerate() {
            group_key_targets
                .get_mut(source_position)
                .ok_or(ExecutionError::TypeMismatch)?
                .push(key_target);
        }
        let aggregate_positions = aggregates
            .iter()
            .map(|aggregate| match &aggregate.input {
                AggregateInput::All => Ok(None),
                AggregateInput::Column(column) => {
                    find_source_position(input_fields, column).map(Some)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut aggregate_index = 0;
        let output_positions = outputs
            .iter()
            .map(|output| match output {
                AggregateOutput::GroupKey(column) => group_keys
                    .iter()
                    .position(|group_key| same_source_column(group_key, column))
                    .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone())),
                AggregateOutput::Aggregate(_) => {
                    let position = group_keys.len() + aggregate_index;
                    aggregate_index += 1;
                    Ok(position)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output_projection =
            ProjectionPlan::from_positions(group_keys.len() + aggregates.len(), output_positions)?;
        let has_extremes = aggregates.iter().any(|aggregate| {
            matches!(
                aggregate.function,
                AggregateFunction::Min | AggregateFunction::Max
            )
        });

        let mut accumulator = Self {
            group_keys,
            aggregates,
            has_extremes,
            group_key_positions,
            group_key_targets,
            aggregate_positions,
            replacement_targets: (0..input_fields.len()).map(|_| Vec::new()).collect(),
            output_projection,
            output_fields: outputs.iter().map(AggregateOutput::output_field).collect(),
            group_lookup: GroupLookup::new(),
            groups: Vec::new(),
            #[cfg(test)]
            grouped_batch_stats: GroupedBatchStats::default(),
        };
        if group_keys.is_empty() {
            accumulator
                .groups
                .push(new_group_state(Vec::new(), &accumulator.aggregates)?);
        }
        Ok(accumulator)
    }

    fn consume_batch(&mut self, batch: &mut ExecutionBatch) -> Result<(), ExecutionError> {
        debug_assert!(batch.rows.len() <= EXECUTION_BATCH_CAPACITY);
        if self.group_keys.is_empty() {
            if !self.has_extremes {
                let result = self.consume_rows(&batch.rows);
                batch.rows.clear();
                return result;
            }
            for row in batch.rows.drain(..) {
                self.consume_owned_row(row)?;
            }
            return Ok(());
        }

        let result = self.consume_grouped_rows_mut(&mut batch.rows);
        batch.rows.clear();
        result
    }

    fn consume_grouped_rows_mut(
        &mut self,
        rows: &mut [ExecutionRow],
    ) -> Result<(), ExecutionError> {
        debug_assert!(!self.group_keys.is_empty());
        for row in rows {
            self.consume_grouped_row_mut(row)?;
        }
        Ok(())
    }

    fn consume_grouped_row_mut(&mut self, row: &mut ExecutionRow) -> Result<(), ExecutionError> {
        debug_assert!(!self.group_keys.is_empty());
        #[cfg(test)]
        {
            self.grouped_batch_stats.grouped_rows += 1;
        }
        let probe = self.probe_group(row)?;
        if self.has_extremes {
            for targets in &mut self.replacement_targets {
                targets.clear();
            }
        }

        if let Some(group_index) = probe.group_index {
            let group = self
                .groups
                .get_mut(group_index)
                .ok_or(ExecutionError::TypeMismatch)?;
            let has_actual_replacements = collect_owned_aggregate_updates(
                row,
                &self.aggregates,
                &self.aggregate_positions,
                &mut self.replacement_targets,
                group,
            )?;
            if has_actual_replacements {
                transfer_owned_row_values(
                    row,
                    false,
                    &self.group_key_targets,
                    &self.replacement_targets,
                    group,
                    &mut self.group_lookup,
                )?;
                #[cfg(test)]
                {
                    self.grouped_batch_stats.rows_with_scalar_transfer += 1;
                    self.grouped_batch_stats.extrema_replacement_rows += 1;
                }
            } else {
                #[cfg(test)]
                {
                    self.grouped_batch_stats.borrow_only_hits += 1;
                }
            }
            return Ok(());
        }

        validate_group_key(row, &self.group_key_positions, self.group_keys)?;
        let mut group = new_group_state(
            vec![ScalarValue::Null; self.group_keys.len()],
            &self.aggregates,
        )?;
        let _has_actual_replacements = collect_owned_aggregate_updates(
            row,
            &self.aggregates,
            &self.aggregate_positions,
            &mut self.replacement_targets,
            &mut group,
        )?;
        transfer_owned_row_values(
            row,
            true,
            &self.group_key_targets,
            &self.replacement_targets,
            &mut group,
            &mut self.group_lookup,
        )?;
        let group_index = self.groups.len();
        self.group_lookup.register_group(probe.hash, group_index)?;
        self.group_lookup.record_owned_key_materialization();
        self.groups.push(group);
        #[cfg(test)]
        {
            self.grouped_batch_stats.miss_rows += 1;
            self.grouped_batch_stats.rows_with_scalar_transfer += 1;
            if _has_actual_replacements {
                self.grouped_batch_stats.extrema_replacement_rows += 1;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    const fn grouped_batch_stats(&self) -> GroupedBatchStats {
        self.grouped_batch_stats
    }

    fn consume_rows(&mut self, rows: &[ExecutionRow]) -> Result<(), ExecutionError> {
        for row in rows {
            let group_index = self.borrowed_group_index(row)?;
            let group = self
                .groups
                .get_mut(group_index)
                .ok_or(ExecutionError::TypeMismatch)?;
            for ((aggregate, position), state) in self
                .aggregates
                .iter()
                .zip(&self.aggregate_positions)
                .zip(&mut group.aggregate_states)
            {
                let value = aggregate_input_value(row, aggregate, *position)?;
                update_aggregate_state(state, aggregate, value)?;
            }
        }
        Ok(())
    }

    fn consume_owned_row(&mut self, mut row: ExecutionRow) -> Result<(), ExecutionError> {
        let probe = self.probe_group(&row)?;
        if self.has_extremes {
            for targets in &mut self.replacement_targets {
                targets.clear();
            }
        }

        if let Some(group_index) = probe.group_index {
            let group = self
                .groups
                .get_mut(group_index)
                .ok_or(ExecutionError::TypeMismatch)?;
            collect_owned_aggregate_updates(
                &row,
                &self.aggregates,
                &self.aggregate_positions,
                &mut self.replacement_targets,
                group,
            )?;
            if self.has_extremes {
                transfer_owned_row_values(
                    &mut row,
                    false,
                    &self.group_key_targets,
                    &self.replacement_targets,
                    group,
                    &mut self.group_lookup,
                )?;
            }
            return Ok(());
        }

        validate_group_key(&row, &self.group_key_positions, self.group_keys)?;
        let mut group = new_group_state(
            vec![ScalarValue::Null; self.group_keys.len()],
            &self.aggregates,
        )?;
        collect_owned_aggregate_updates(
            &row,
            &self.aggregates,
            &self.aggregate_positions,
            &mut self.replacement_targets,
            &mut group,
        )?;
        transfer_owned_row_values(
            &mut row,
            true,
            &self.group_key_targets,
            &self.replacement_targets,
            &mut group,
            &mut self.group_lookup,
        )?;
        let group_index = self.groups.len();
        self.group_lookup.register_group(probe.hash, group_index)?;
        self.group_lookup.record_owned_key_materialization();
        self.groups.push(group);
        Ok(())
    }

    fn probe_group(&mut self, row: &ExecutionRow) -> Result<GroupLookupProbe, ExecutionError> {
        if self.group_keys.is_empty() {
            return Ok(GroupLookupProbe {
                hash: 0,
                group_index: Some(0),
            });
        }
        self.group_lookup.probe(
            row,
            &self.group_key_positions,
            self.group_keys,
            &self.groups,
        )
    }

    fn borrowed_group_index(&mut self, row: &ExecutionRow) -> Result<usize, ExecutionError> {
        let probe = self.probe_group(row)?;
        if let Some(index) = probe.group_index {
            return Ok(index);
        }

        let key_values = materialize_group_key(row, &self.group_key_positions, self.group_keys)?;
        let group = new_group_state(key_values, &self.aggregates)?;
        let index = self.groups.len();
        self.group_lookup.register_group(probe.hash, index)?;
        self.group_lookup.record_owned_key_materialization();
        self.groups.push(group);
        Ok(index)
    }

    fn finish(self) -> Result<ExecutionRows, ExecutionError> {
        let rows = self
            .groups
            .into_iter()
            .map(|group| {
                let mut values = group.key_values;
                values.extend(
                    group
                        .aggregate_states
                        .into_iter()
                        .map(finalize_aggregate_state),
                );
                project_execution_row(
                    ExecutionRow {
                        row_id: None,
                        values,
                    },
                    &self.output_projection,
                )
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;
        Ok(ExecutionRows {
            fields: self.output_fields,
            rows,
        })
    }
}

fn collect_owned_aggregate_updates(
    row: &ExecutionRow,
    aggregates: &[&AggregateExpr],
    aggregate_positions: &[Option<usize>],
    replacement_targets: &mut [Vec<usize>],
    group: &mut GroupState,
) -> Result<bool, ExecutionError> {
    if aggregates.len() != aggregate_positions.len()
        || aggregates.len() != group.aggregate_states.len()
    {
        return Err(ExecutionError::TypeMismatch);
    }
    let mut has_actual_replacements = false;
    for (aggregate_index, ((aggregate, position), state)) in aggregates
        .iter()
        .zip(aggregate_positions)
        .zip(&mut group.aggregate_states)
        .enumerate()
    {
        let value = aggregate_input_value(row, aggregate, *position)?;
        if matches!(
            aggregate.function,
            AggregateFunction::Min | AggregateFunction::Max
        ) {
            let Some(position) = position else {
                return Err(ExecutionError::InvalidAggregateInput {
                    function: aggregate.function,
                });
            };
            let value = value.ok_or(ExecutionError::TypeMismatch)?;
            if aggregate_extreme_replaces(state, value)? {
                has_actual_replacements = true;
                replacement_targets
                    .get_mut(*position)
                    .ok_or(ExecutionError::TypeMismatch)?
                    .push(aggregate_index);
            }
        } else {
            update_aggregate_state(state, aggregate, value)?;
        }
    }
    Ok(has_actual_replacements)
}

fn transfer_owned_row_values(
    row: &mut ExecutionRow,
    include_group_keys: bool,
    group_key_targets: &[Vec<usize>],
    replacement_targets: &[Vec<usize>],
    group: &mut GroupState,
    group_lookup: &mut GroupLookup,
) -> Result<(), ExecutionError> {
    if group_key_targets.len() != replacement_targets.len() {
        return Err(ExecutionError::TypeMismatch);
    }
    #[cfg(not(test))]
    let _ = group_lookup;
    for (position, (all_key_targets, aggregate_targets)) in group_key_targets
        .iter()
        .zip(replacement_targets)
        .enumerate()
    {
        let key_targets = if include_group_keys {
            all_key_targets.as_slice()
        } else {
            &[]
        };
        let durable_owners = key_targets.len() + aggregate_targets.len();
        if durable_owners == 0 {
            continue;
        }

        let candidate = std::mem::replace(
            row.values
                .get_mut(position)
                .ok_or(ExecutionError::TypeMismatch)?,
            ScalarValue::Null,
        );
        let mut candidate = Some(candidate);
        let mut remaining = durable_owners;
        for target in key_targets {
            let value = next_owned_value(&mut candidate, &mut remaining)?;
            *group
                .key_values
                .get_mut(*target)
                .ok_or(ExecutionError::TypeMismatch)? = value;
        }
        for target in aggregate_targets {
            let value = next_owned_value(&mut candidate, &mut remaining)?;
            replace_aggregate_extreme(
                group
                    .aggregate_states
                    .get_mut(*target)
                    .ok_or(ExecutionError::TypeMismatch)?,
                value,
            )?;
        }
        if remaining != 0 || candidate.is_some() {
            return Err(ExecutionError::TypeMismatch);
        }
        #[cfg(test)]
        group_lookup.record_owned_key_transfer(key_targets.len(), durable_owners);
    }
    Ok(())
}

fn next_owned_value(
    candidate: &mut Option<ScalarValue>,
    remaining: &mut usize,
) -> Result<ScalarValue, ExecutionError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(ExecutionError::TypeMismatch)?;
    if *remaining == 0 {
        candidate.take().ok_or(ExecutionError::TypeMismatch)
    } else {
        candidate
            .as_ref()
            .cloned()
            .ok_or(ExecutionError::TypeMismatch)
    }
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
        AggregateFunction::Min => Ok(AggregateState::Min(ExtremeState::empty(
            aggregate.output.data_type.physical,
        ))),
        AggregateFunction::Max => Ok(AggregateState::Max(ExtremeState::empty(
            aggregate.output.data_type.physical,
        ))),
    }
}

fn aggregate_input_value<'a>(
    row: &'a ExecutionRow,
    aggregate: &AggregateExpr,
    position: Option<usize>,
) -> Result<Option<&'a ScalarValue>, ExecutionError> {
    let Some(position) = position else {
        return Ok(None);
    };
    let value = row.values.get(position).ok_or_else(|| {
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
    Ok(Some(value))
}

fn aggregate_extreme_replaces(
    state: &AggregateState,
    value: &ScalarValue,
) -> Result<bool, ExecutionError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(false);
    }
    match state {
        AggregateState::Min(current) => Ok(current
            .compare_candidate(value)?
            .is_none_or(|ordering| ordering == Ordering::Less)),
        AggregateState::Max(current) => Ok(current
            .compare_candidate(value)?
            .is_none_or(|ordering| ordering == Ordering::Greater)),
        AggregateState::Count(_) | AggregateState::SumInt(_) | AggregateState::SumUInt(_) => {
            Err(ExecutionError::TypeMismatch)
        }
    }
}

fn replace_aggregate_extreme(
    state: &mut AggregateState,
    value: ScalarValue,
) -> Result<(), ExecutionError> {
    match state {
        AggregateState::Min(current) | AggregateState::Max(current) => current.replace(value),
        AggregateState::Count(_) | AggregateState::SumInt(_) | AggregateState::SumUInt(_) => {
            Err(ExecutionError::TypeMismatch)
        }
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
        AggregateState::Min(_) | AggregateState::Max(_) => {
            if let Some(value) = value {
                if aggregate_extreme_replaces(state, value)? {
                    replace_aggregate_extreme(state, value.clone())?;
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
        AggregateState::Min(value) | AggregateState::Max(value) => value.into_scalar(),
    }
}

fn storage_for_table<'a>(
    bindings: &[ExecutionStorageBinding],
    storages: &'a mut [ExecutionStorage<'_>],
    table_id: TableId,
) -> Result<&'a mut TableStorage, ExecutionError> {
    let storage_id = bindings
        .iter()
        .find(|binding| binding.table_id == table_id)
        .map(|binding| binding.storage_id)
        .ok_or(ExecutionError::MissingTableStorage(table_id))?;
    storages
        .iter_mut()
        .find(|storage| storage.storage_id == storage_id)
        .map(|storage| &mut *storage.storage)
        .ok_or(ExecutionError::MissingPhysicalStorage(storage_id))
}

fn storage_for_id<'a>(
    storages: &'a mut [ExecutionStorage<'_>],
    storage_id: StorageId,
) -> Result<&'a mut TableStorage, ExecutionError> {
    storages
        .iter_mut()
        .find(|storage| storage.storage_id == storage_id)
        .map(|storage| &mut *storage.storage)
        .ok_or(ExecutionError::MissingPhysicalStorage(storage_id))
}

fn read_view_for_table<'a>(
    bindings: &[ExecutionStorageBinding],
    read_views: &'a [ExecutionReadView<'_>],
    table_id: TableId,
) -> Result<&'a StorageReadView, ExecutionError> {
    let storage_id = bindings
        .iter()
        .find(|binding| binding.table_id == table_id)
        .map(|binding| binding.storage_id)
        .ok_or(ExecutionError::MissingTableStorage(table_id))?;
    read_views
        .iter()
        .find(|entry| entry.storage_id == storage_id)
        .map(|entry| entry.view)
        .ok_or(ExecutionError::MissingStorageReadView(storage_id))
}

fn read_view_for_storage<'a>(
    read_views: &'a [ExecutionReadView<'_>],
    storage_id: StorageId,
) -> Result<&'a StorageReadView, ExecutionError> {
    read_views
        .iter()
        .find(|entry| entry.storage_id == storage_id)
        .map(|entry| entry.view)
        .ok_or(ExecutionError::MissingStorageReadView(storage_id))
}

fn ensure_table(table_id: TableId, storage: &TableStorage) -> Result<(), ExecutionError> {
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
) -> Result<Vec<(StorageRowHandle, Vec<ScalarValue>)>, ExecutionError> {
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
    Borrowed(ScalarRef<'a>),
    Owned(ScalarValue),
}

impl EvaluatedScalar<'_> {
    fn as_scalar_ref(&self) -> ScalarRef<'_> {
        match self {
            Self::Borrowed(value) => *value,
            Self::Owned(value) => ScalarRef::from(value),
        }
    }
}

fn evaluate_bound_with<'a, G>(
    expression: &'a BoundExpr<'_>,
    value_at: &G,
) -> Result<EvaluatedScalar<'a>, ExecutionError>
where
    G: Fn(usize) -> Option<ScalarRef<'a>>,
{
    match &expression.kind {
        BoundExprKind::Column { position, name } => value_at(*position)
            .map(EvaluatedScalar::Borrowed)
            .ok_or_else(|| ExecutionError::MissingColumn((*name).to_owned())),
        BoundExprKind::Literal(value) => Ok(EvaluatedScalar::Borrowed(ScalarRef::from(*value))),
        BoundExprKind::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_bound_with(left, value_at)?;
            let right = evaluate_bound_with(right, value_at)?;
            evaluate_binary_scalar_refs(*operator, left.as_scalar_ref(), right.as_scalar_ref())
                .map(EvaluatedScalar::Owned)
        }
        BoundExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(EvaluatedScalar::Owned(
            TruthValue::from_scalar_view(
                evaluate_bound_with(expression, value_at)?.as_scalar_ref(),
            )?
            .not()
            .into_scalar(),
        )),
        BoundExprKind::IsNull {
            expression,
            negated,
        } => {
            let value = evaluate_bound_with(expression, value_at)?;
            let is_null = value.as_scalar_ref().is_null();
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(if *negated {
                !is_null
            } else {
                is_null
            })))
        }
    }
}

fn evaluate_bound_values<'a>(
    expression: &'a BoundExpr<'_>,
    values: EvaluationValues<'a>,
) -> Result<EvaluatedScalar<'a>, ExecutionError> {
    evaluate_bound_with(expression, &|position| {
        values.get(position).map(ScalarRef::from)
    })
}

fn evaluate_bound_truth<'a>(
    expression: &'a BoundExpr<'_>,
    values: EvaluationValues<'a>,
) -> Result<TruthValue, ExecutionError> {
    let value = evaluate_bound_values(expression, values)?;
    TruthValue::from_scalar_view(value.as_scalar_ref())
}

fn evaluate_bound_scalar_ref_truth<'a>(
    expression: &'a BoundExpr<'_>,
    values: &[ScalarRef<'a>],
) -> Result<TruthValue, ExecutionError> {
    let value = evaluate_bound_with(expression, &|position| values.get(position).copied())?;
    TruthValue::from_scalar_view(value.as_scalar_ref())
}

fn evaluate_dynamic_with<'a, G>(
    expression: &'a Expr,
    fields: &[OutputField],
    value_at: &G,
) -> Result<EvaluatedScalar<'a>, ExecutionError>
where
    G: Fn(usize) -> Option<ScalarRef<'a>>,
{
    match &expression.kind {
        ExprKind::Column(column) => {
            let position = find_source_position(fields, column)?;
            value_at(position)
                .map(EvaluatedScalar::Borrowed)
                .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))
        }
        ExprKind::Literal(value) => Ok(EvaluatedScalar::Borrowed(ScalarRef::from(value))),
        ExprKind::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate_dynamic_with(left, fields, value_at)?;
            let right = evaluate_dynamic_with(right, fields, value_at)?;
            evaluate_binary_scalar_refs(*operator, left.as_scalar_ref(), right.as_scalar_ref())
                .map(EvaluatedScalar::Owned)
        }
        ExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(EvaluatedScalar::Owned(
            TruthValue::from_scalar_view(
                evaluate_dynamic_with(expression, fields, value_at)?.as_scalar_ref(),
            )?
            .not()
            .into_scalar(),
        )),
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            let value = evaluate_dynamic_with(expression, fields, value_at)?;
            let is_null = value.as_scalar_ref().is_null();
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(if *negated {
                !is_null
            } else {
                is_null
            })))
        }
    }
}

fn evaluate_dynamic_borrowed_values<'a>(
    expression: &'a Expr,
    values: EvaluationValues<'a>,
    fields: &[OutputField],
) -> Result<EvaluatedScalar<'a>, ExecutionError> {
    evaluate_dynamic_with(expression, fields, &|position| {
        values.get(position).map(ScalarRef::from)
    })
}

fn evaluate_dynamic_borrowed_truth_values<'a>(
    expression: &'a Expr,
    values: EvaluationValues<'a>,
    fields: &[OutputField],
) -> Result<TruthValue, ExecutionError> {
    let value = evaluate_dynamic_borrowed_values(expression, values, fields)?;
    TruthValue::from_scalar_view(value.as_scalar_ref())
}

fn evaluate_dynamic_scalar_ref_truth<'a>(
    expression: &'a Expr,
    values: &[ScalarRef<'a>],
    fields: &[OutputField],
) -> Result<TruthValue, ExecutionError> {
    let value = evaluate_dynamic_with(expression, fields, &|position| {
        values.get(position).copied()
    })?;
    TruthValue::from_scalar_view(value.as_scalar_ref())
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

#[cfg(test)]
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
        Self::from_scalar_view(ScalarRef::from(&value))
    }

    #[cfg(test)]
    fn from_scalar_ref(value: &ScalarValue) -> Result<Self, ExecutionError> {
        Self::from_scalar_view(ScalarRef::from(value))
    }

    fn from_scalar_view(value: ScalarRef<'_>) -> Result<Self, ExecutionError> {
        match value {
            ScalarRef::Bool(true) => Ok(Self::True),
            ScalarRef::Bool(false) => Ok(Self::False),
            ScalarRef::Null => Ok(Self::Unknown),
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
    evaluate_binary_scalar_refs(operator, ScalarRef::from(left), ScalarRef::from(right))
}

fn evaluate_binary_scalar_refs(
    operator: BinaryOp,
    left: ScalarRef<'_>,
    right: ScalarRef<'_>,
) -> Result<ScalarValue, ExecutionError> {
    match operator {
        BinaryOp::And | BinaryOp::Or => {
            let left = TruthValue::from_scalar_view(left)?;
            let right = TruthValue::from_scalar_view(right)?;
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
            if left.is_null() || right.is_null() {
                return Ok(ScalarValue::Null);
            }
            let ordering = compare_scalar_refs(left, right)?;
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
    compare_scalar_refs(ScalarRef::from(left), ScalarRef::from(right))
}

fn compare_scalar_refs(
    left: ScalarRef<'_>,
    right: ScalarRef<'_>,
) -> Result<Ordering, ExecutionError> {
    match (left, right) {
        (ScalarRef::Bool(left), ScalarRef::Bool(right)) => Ok(left.cmp(&right)),
        (ScalarRef::Int64(left), ScalarRef::Int64(right)) => Ok(left.cmp(&right)),
        (ScalarRef::UInt64(left), ScalarRef::UInt64(right)) => Ok(left.cmp(&right)),
        (ScalarRef::Text(left), ScalarRef::Text(right)) => Ok(left.cmp(right)),
        _ => Err(ExecutionError::TypeMismatch),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    use std::ops::ControlFlow;

    use super::{
        AggregateAccumulator, BoundExpr, BoundExprKind, BoundInequality, EXECUTION_BATCH_CAPACITY,
        EvaluatedScalar, EvaluationValues, ExecutionBatch, ExecutionError, ExecutionReadView,
        ExecutionRow, ExecutionStorage, FilteredCountSummary, GroupLookup, GroupState,
        InequalityExecutionStrategy, PrehashedBuildHasher, PrehashedKey, ProjectionPlan,
        QueryResult, StreamingHashJoinStats, TopNState, TruthValue, bind_expression,
        build_batch_pipeline, build_top_n_plan, choose_inequality_strategy, collect_filter_columns,
        collect_streaming_filter_row, compatibility_bindings, count_to_sql_u64,
        direct_count_eligibility, evaluate, evaluate_binary, evaluate_binary_refs,
        evaluate_binary_scalar_refs, evaluate_bound_scalar_ref_truth, evaluate_bound_truth,
        evaluate_bound_values, evaluate_bound_with, evaluate_dynamic_borrowed_truth_values,
        evaluate_dynamic_borrowed_values, evaluate_dynamic_scalar_ref_truth, evaluate_dynamic_with,
        evaluate_truth, evaluate_truth_values, evaluate_values, exact_candidate_pair_count,
        execute, execute_inequality_sweep, execute_nested_loop_join, execute_rows,
        execute_rows_legacy, execute_with_storages, filtered_count_eligibility,
        find_required_inequality, hash_group_key, inequality_can_match, materialize_count_values,
        materialize_direct_count_values, potential_left_indices, project_execution_row,
        projected_streaming_seq_filter_eligibility, required_right_extreme,
        sorted_non_null_indices, streaming_seq_filter_eligibility,
        try_execute_streaming_hash_join_probe, update_filtered_count_summary, visit_batch_pipeline,
    };
    use netbadb_planner::{
        AccessPath, AccessPathCapabilities, PhysicalPlan, TableAccessStatistics, plan,
        plan_with_statistics,
    };
    use netbadb_rel::{
        AggregateExpr, AggregateFunction, AggregateInput, AggregateOutput, BinaryOp, ColumnRef,
        DerivedField, Expr, ExprKind, JoinKind, LogicalPlan, NullOrder, OutputField, SortDirection,
        SortKey, UnaryOp,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_storage::{
        IndexStatistics, PresenceCountSummary, StorageRowHandle, TableStatistics, TableStorage,
    };
    use netbadb_types::{
        ColumnId, ExprType, PhysicalType, RelationBindingId, ScalarRef, ScalarValue, SemanticType,
        TableId,
    };

    fn text_pointer(value: &ScalarValue) -> *const u8 {
        match value {
            ScalarValue::Text(value) => value.as_ptr(),
            _ => panic!("expected Text value"),
        }
    }

    fn scalar_ref_text_pointer(value: ScalarRef<'_>) -> *const u8 {
        match value {
            ScalarRef::Text(value) => value.as_ptr(),
            _ => panic!("expected Text scalar view"),
        }
    }

    fn test_storage_row_handle(case: &str) -> StorageRowHandle {
        let path = std::env::temp_dir().join(format!(
            "netbadb-executor-row-handle-{case}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(&wal);
        let _ = std::fs::remove_file(netbadb_storage::txn_status_path(&path));
        let _ = std::fs::remove_file(&path);
        let table = TableDef::new(
            TableId(999),
            "row_handle",
            vec![ColumnDef::new(
                ColumnId(1),
                "value",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        );
        let mut storage = TableStorage::create_heap(&path, table).expect("create row handle heap");
        let row = storage
            .insert(&[ScalarValue::Int64(1)])
            .expect("insert row handle");
        storage.close().expect("close row handle heap");
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(netbadb_storage::txn_status_path(&path));
        let _ = std::fs::remove_file(path);
        row
    }

    #[test]
    fn projection_identity_moves_the_complete_row_without_rebuilding_values() {
        let text = String::from("payload");
        let pointer = text.as_ptr();
        let row_id = test_storage_row_handle("identity");
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

        let empty = ProjectionPlan::from_positions(1, Vec::new()).expect("empty plan");
        let row_id = test_storage_row_handle("empty-projection");
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

    fn batch_table() -> TableDef {
        TableDef::new(
            TableId(55),
            "batch_items",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "unsigned_key",
                    TypeSpec::Physical(PhysicalType::UInt64),
                ),
                ColumnDef::new(
                    ColumnId(3),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
                ColumnDef::new(
                    ColumnId(4),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                ),
                ColumnDef::new(
                    ColumnId(5),
                    "nullable_key",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
            ],
        )
    }

    fn batch_columns() -> Vec<ColumnRef> {
        [
            (1, "id", PhysicalType::Int64, false),
            (2, "unsigned_key", PhysicalType::UInt64, false),
            (3, "active", PhysicalType::Bool, false),
            (4, "payload", PhysicalType::Text, false),
            (5, "nullable_key", PhysicalType::Int64, true),
        ]
        .into_iter()
        .map(|(id, name, physical, nullable)| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(55),
            column_id: ColumnId(id),
            relation_name: "batch_items".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable,
        })
        .collect()
    }

    fn batch_scan(columns: Vec<ColumnRef>) -> PhysicalPlan {
        PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(55),
            table_name: "batch_items".into(),
            columns,
        }
    }

    fn batch_column_expression(column: &ColumnRef) -> Expr {
        Expr {
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
            kind: ExprKind::Column(column.clone()),
        }
    }

    fn batch_literal(value: ScalarValue, physical: PhysicalType) -> Expr {
        Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(physical),
                nullable: matches!(value, ScalarValue::Null),
            },
            kind: ExprKind::Literal(value),
        }
    }

    fn batch_binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: left.expr_type.nullable || right.expr_type.nullable,
            },
            kind: ExprKind::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
        }
    }

    fn batch_filter(input: PhysicalPlan, predicate: Expr) -> PhysicalPlan {
        PhysicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    fn batch_project(input: PhysicalPlan, columns: Vec<ColumnRef>) -> PhysicalPlan {
        PhysicalPlan::Project {
            input: Box::new(input),
            columns,
        }
    }

    fn batch_limit(input: PhysicalPlan, limit: u64) -> PhysicalPlan {
        PhysicalPlan::Limit {
            input: Box::new(input),
            limit,
        }
    }

    fn batch_sort(input: PhysicalPlan, keys: Vec<SortKey>) -> PhysicalPlan {
        PhysicalPlan::Sort {
            input: Box::new(input),
            keys,
        }
    }

    fn batch_top_n(
        input: PhysicalPlan,
        keys: Vec<SortKey>,
        columns: Vec<ColumnRef>,
        limit: u64,
    ) -> PhysicalPlan {
        batch_limit(batch_project(batch_sort(input, keys), columns), limit)
    }

    fn batch_aggregate_expression(
        function: AggregateFunction,
        input: AggregateInput,
        name: &str,
        physical: PhysicalType,
        nullable: bool,
    ) -> AggregateOutput {
        AggregateOutput::Aggregate(AggregateExpr {
            function,
            input,
            output: DerivedField {
                name: name.into(),
                data_type: SemanticType::physical(physical),
                nullable,
            },
        })
    }

    fn batch_aggregate(
        input: PhysicalPlan,
        group_keys: Vec<ColumnRef>,
        outputs: Vec<AggregateOutput>,
    ) -> PhysicalPlan {
        PhysicalPlan::Aggregate {
            input: Box::new(input),
            group_keys,
            outputs,
        }
    }

    fn produced_batch_sizes(plan: &PhysicalPlan, storage: &mut TableStorage) -> Vec<usize> {
        let views = [storage.read_view().expect("create producer view")];
        let bindings = compatibility_bindings(std::slice::from_ref(storage))
            .expect("create producer bindings");
        let mut execution_storages = [ExecutionStorage {
            storage_id: bindings[0].storage_id,
            storage,
        }];
        let execution_views = [ExecutionReadView {
            storage_id: bindings[0].storage_id,
            view: &views[0],
        }];
        let mut pipeline = build_batch_pipeline(plan)
            .expect("build producer pipeline")
            .expect("eligible producer pipeline");
        let mut sizes = Vec::new();
        let _ = visit_batch_pipeline(
            &mut pipeline,
            &bindings,
            &mut execution_storages,
            &execution_views,
            |batch| {
                sizes.push(batch.rows.len());
                Ok(ControlFlow::Continue(()))
            },
        )
        .expect("visit producer batches");
        sizes
    }

    fn batch_test_path(case: &str, lsm: bool) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-executor-batch-{case}-{}-{:?}-{}",
            std::process::id(),
            std::thread::current().id(),
            if lsm { "lsm" } else { "heap" }
        ))
    }

    fn remove_batch_test_path(path: &std::path::Path, lsm: bool) {
        if lsm {
            let _ = std::fs::remove_dir_all(path);
        } else {
            let wal = netbadb_storage::wal_path(path);
            let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
            let _ = std::fs::remove_file(wal);
            let _ = std::fs::remove_file(netbadb_storage::txn_status_path(path));
            let _ = std::fs::remove_file(path);
        }
    }

    fn batch_storage(case: &str, lsm: bool, rows: usize) -> (TableStorage, std::path::PathBuf) {
        let path = batch_test_path(case, lsm);
        remove_batch_test_path(&path, lsm);
        let mut storage = if lsm {
            TableStorage::create_lsm(&path, batch_table(), ColumnId(1)).expect("create batch LSM")
        } else {
            TableStorage::create_heap(&path, batch_table()).expect("create batch Heap")
        };
        if rows != 0 {
            let mut transaction = storage.begin_transaction().expect("begin batch load");
            for index in 0..rows {
                let id = i64::try_from(index).expect("batch test row ID fits i64");
                storage
                    .insert_in(
                        &mut transaction,
                        &[
                            ScalarValue::Int64(id),
                            ScalarValue::UInt64((index % 7) as u64),
                            ScalarValue::Bool(index % 3 == 0),
                            ScalarValue::Text(format!("group-{}", index % 5)),
                            if index % 4 == 0 {
                                ScalarValue::Null
                            } else {
                                ScalarValue::Int64(id)
                            },
                        ],
                    )
                    .expect("insert batch row");
            }
            transaction.commit().expect("commit batch load");
        }
        (storage, path)
    }

    fn assert_batch_matches_legacy(plan: &PhysicalPlan, storage: &mut TableStorage) {
        let batch = execute_rows(plan, std::slice::from_mut(storage)).expect("execute batch path");
        let legacy =
            execute_rows_legacy(plan, std::slice::from_mut(storage)).expect("execute legacy path");
        assert_eq!(batch, legacy);
    }

    fn count_output() -> AggregateOutput {
        batch_aggregate_expression(
            AggregateFunction::Count,
            AggregateInput::All,
            "COUNT(*)",
            PhysicalType::UInt64,
            false,
        )
    }

    fn consume_generated_batches(
        accumulator: &mut AggregateAccumulator<'_>,
        rows: usize,
        mut values: impl FnMut(usize) -> Vec<ScalarValue>,
    ) {
        for start in (0..rows).step_by(EXECUTION_BATCH_CAPACITY) {
            let end = rows.min(start + EXECUTION_BATCH_CAPACITY);
            let mut batch = ExecutionBatch::with_capacity();
            batch.rows.extend((start..end).map(|index| ExecutionRow {
                row_id: None,
                values: values(index),
            }));
            accumulator
                .consume_batch(&mut batch)
                .expect("consume generated aggregate batch");
            assert!(batch.rows.is_empty());
        }
    }

    #[test]
    fn typed_extreme_state_binds_plan_physical_type_and_rejects_mismatches() {
        let aggregate = |physical| AggregateExpr {
            function: AggregateFunction::Min,
            input: AggregateInput::All,
            output: DerivedField {
                name: "typed MIN".into(),
                data_type: SemanticType::physical(physical),
                nullable: true,
            },
        };
        assert!(matches!(
            super::initial_aggregate_state(&aggregate(PhysicalType::Bool)),
            Ok(super::AggregateState::Min(super::ExtremeState::Bool(None)))
        ));
        assert!(matches!(
            super::initial_aggregate_state(&aggregate(PhysicalType::Int64)),
            Ok(super::AggregateState::Min(super::ExtremeState::Int64(None)))
        ));
        assert!(matches!(
            super::initial_aggregate_state(&aggregate(PhysicalType::UInt64)),
            Ok(super::AggregateState::Min(super::ExtremeState::UInt64(
                None
            )))
        ));
        assert!(matches!(
            super::initial_aggregate_state(&aggregate(PhysicalType::Text)),
            Ok(super::AggregateState::Min(super::ExtremeState::Text(None)))
        ));

        let mut text_min = super::AggregateState::Min(super::ExtremeState::Text(None));
        assert!(
            !super::aggregate_extreme_replaces(&text_min, &ScalarValue::Null)
                .expect("NULL candidate is ignored")
        );
        assert!(matches!(
            super::aggregate_extreme_replaces(&text_min, &ScalarValue::Int64(1)),
            Err(ExecutionError::TypeMismatch)
        ));
        assert!(matches!(
            super::replace_aggregate_extreme(&mut text_min, ScalarValue::Int64(1)),
            Err(ExecutionError::TypeMismatch)
        ));
        assert_eq!(super::finalize_aggregate_state(text_min), ScalarValue::Null);

        let invalid_output = [AggregateOutput::Aggregate(aggregate(PhysicalType::Text))];
        assert!(matches!(
            AggregateAccumulator::new(&[], &[], &invalid_output),
            Err(ExecutionError::InvalidAggregateInput {
                function: AggregateFunction::Min
            })
        ));
    }

    #[test]
    fn typed_extreme_replacement_matches_generic_comparison_for_all_physical_types() {
        fn assert_pair(physical: PhysicalType, candidate: ScalarValue, current: ScalarValue) {
            let ordering = super::compare_values(&candidate, &current)
                .expect("generic comparison accepts same physical types");
            for (function, expected) in [
                (AggregateFunction::Min, ordering == std::cmp::Ordering::Less),
                (
                    AggregateFunction::Max,
                    ordering == std::cmp::Ordering::Greater,
                ),
            ] {
                let extreme = super::ExtremeState::empty(physical);
                let mut state = if function == AggregateFunction::Min {
                    super::AggregateState::Min(extreme)
                } else {
                    super::AggregateState::Max(extreme)
                };
                super::replace_aggregate_extreme(&mut state, current.clone())
                    .expect("seed typed extreme");
                assert_eq!(
                    super::aggregate_extreme_replaces(&state, &candidate)
                        .expect("compare typed extreme"),
                    expected,
                    "typed {function:?} comparison disagreed for {candidate:?} and {current:?}"
                );
            }
        }

        for candidate in [false, true] {
            for current in [false, true] {
                assert_pair(
                    PhysicalType::Bool,
                    ScalarValue::Bool(candidate),
                    ScalarValue::Bool(current),
                );
            }
        }
        for candidate in [-9_i64, 0, 17] {
            for current in [-9_i64, 0, 17] {
                assert_pair(
                    PhysicalType::Int64,
                    ScalarValue::Int64(candidate),
                    ScalarValue::Int64(current),
                );
            }
        }
        for candidate in [0_u64, 1, u64::MAX] {
            for current in [0_u64, 1, u64::MAX] {
                assert_pair(
                    PhysicalType::UInt64,
                    ScalarValue::UInt64(candidate),
                    ScalarValue::UInt64(current),
                );
            }
        }

        let mut common_prefix_a = "p".repeat(63);
        common_prefix_a.push('a');
        let mut common_prefix_z = "p".repeat(63);
        common_prefix_z.push('z');
        let text_values = [
            String::new(),
            String::from("a"),
            String::from("aa"),
            String::from("ab"),
            String::from("b"),
            String::from("é"),
            String::from("中"),
            String::from("😀"),
            "x".repeat(64),
            common_prefix_a,
            common_prefix_z,
        ];
        for candidate in &text_values {
            for current in &text_values {
                assert_pair(
                    PhysicalType::Text,
                    ScalarValue::Text(candidate.clone()),
                    ScalarValue::Text(current.clone()),
                );
            }
        }
    }

    #[test]
    fn equal_text_extremes_replace_once_and_keep_the_first_allocation() {
        for function in [AggregateFunction::Min, AggregateFunction::Max] {
            let extreme = super::ExtremeState::empty(PhysicalType::Text);
            let mut state = if function == AggregateFunction::Min {
                super::AggregateState::Min(extreme)
            } else {
                super::AggregateState::Max(extreme)
            };
            let mut first = Some(String::from("equal-value-with-a-complete-shared-prefix"));
            let first_pointer = first.as_ref().expect("first Text candidate").as_ptr();
            let mut replacements = 0;
            let mut non_replacements = 0;
            for index in 0..513 {
                let candidate = ScalarValue::Text(if index == 0 {
                    first.take().expect("move first Text candidate")
                } else {
                    String::from("equal-value-with-a-complete-shared-prefix")
                });
                if super::aggregate_extreme_replaces(&state, &candidate)
                    .expect("compare equal Text candidate")
                {
                    replacements += 1;
                    super::replace_aggregate_extreme(&mut state, candidate)
                        .expect("replace initial Text extreme");
                } else {
                    non_replacements += 1;
                }
            }
            assert_eq!(replacements, 1);
            assert_eq!(non_replacements, 512);
            let result = super::finalize_aggregate_state(state);
            assert_eq!(
                result,
                ScalarValue::Text(String::from("equal-value-with-a-complete-shared-prefix"))
            );
            assert_eq!(text_pointer(&result), first_pointer);
        }
    }

    #[test]
    fn borrowed_group_lookup_materializes_only_distinct_int_and_text_keys() {
        let columns = batch_columns();
        let int_key = columns[1].clone();
        let int_fields = [OutputField::Source(int_key.clone())];
        let int_outputs = [AggregateOutput::GroupKey(int_key.clone()), count_output()];
        let mut accumulator =
            AggregateAccumulator::new(&int_fields, std::slice::from_ref(&int_key), &int_outputs)
                .expect("build hit-heavy integer accumulator");
        consume_generated_batches(&mut accumulator, 513, |index| {
            vec![ScalarValue::UInt64((index % 4) as u64)]
        });
        let stats = accumulator.group_lookup.stats();
        assert_eq!(stats.lookups, 513);
        assert_eq!(stats.hits, 509);
        assert_eq!(stats.misses, 4);
        assert_eq!(stats.owned_key_materializations, 4);
        assert_eq!(stats.owned_key_moves, 4);
        assert_eq!(stats.owned_key_clones, 0);
        assert!(stats.exact_collision_checks >= stats.hits);
        assert_eq!(
            accumulator.grouped_batch_stats(),
            super::GroupedBatchStats {
                grouped_rows: 513,
                borrow_only_hits: 509,
                miss_rows: 4,
                rows_with_scalar_transfer: 4,
                extrema_replacement_rows: 0,
            }
        );
        assert_eq!(
            accumulator.finish().expect("finish integer groups").rows,
            (0_u64..4)
                .map(|key| super::ExecutionRow {
                    row_id: None,
                    values: vec![
                        ScalarValue::UInt64(key),
                        ScalarValue::UInt64(if key == 0 { 129 } else { 128 }),
                    ],
                })
                .collect::<Vec<_>>()
        );

        let text_key = columns[3].clone();
        let text_fields = [OutputField::Source(text_key.clone())];
        let text_outputs = [AggregateOutput::GroupKey(text_key.clone()), count_output()];
        let mut accumulator =
            AggregateAccumulator::new(&text_fields, std::slice::from_ref(&text_key), &text_outputs)
                .expect("build hit-heavy Text accumulator");
        let first_text = String::from("repeated-key");
        let original_pointer = first_text.as_ptr();
        let mut first_batch = ExecutionBatch::with_capacity();
        first_batch.rows.push(ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Text(first_text)],
        });
        accumulator
            .consume_batch(&mut first_batch)
            .expect("materialize first Text group");
        let durable_pointer = text_pointer(&accumulator.groups[0].key_values[0]);
        assert_eq!(durable_pointer, original_pointer);
        consume_generated_batches(&mut accumulator, 512, |_| {
            vec![ScalarValue::Text("repeated-key".into())]
        });
        assert_eq!(
            accumulator.group_lookup.stats(),
            super::GroupLookupStats {
                lookups: 513,
                hits: 512,
                misses: 1,
                owned_key_materializations: 1,
                owned_key_moves: 1,
                owned_key_clones: 0,
                exact_collision_checks: 512,
            }
        );
        assert_eq!(
            accumulator.grouped_batch_stats(),
            super::GroupedBatchStats {
                grouped_rows: 513,
                borrow_only_hits: 512,
                miss_rows: 1,
                rows_with_scalar_transfer: 1,
                extrema_replacement_rows: 0,
            }
        );
        assert_eq!(
            text_pointer(&accumulator.groups[0].key_values[0]),
            durable_pointer
        );
        assert_eq!(
            accumulator.finish().expect("finish Text group").rows[0].values,
            vec![
                ScalarValue::Text("repeated-key".into()),
                ScalarValue::UInt64(513),
            ]
        );
    }

    #[test]
    fn owned_group_misses_move_every_unique_int_and_text_key_without_cloning() {
        let columns = batch_columns();
        for text_key in [false, true] {
            let key = columns[if text_key { 3 } else { 0 }].clone();
            let input_fields = [OutputField::Source(key.clone())];
            let outputs = [AggregateOutput::GroupKey(key.clone()), count_output()];
            let mut accumulator =
                AggregateAccumulator::new(&input_fields, std::slice::from_ref(&key), &outputs)
                    .expect("build unique-key accumulator");
            consume_generated_batches(&mut accumulator, 513, |index| {
                vec![if text_key {
                    ScalarValue::Text(format!("unique-{index:04}"))
                } else {
                    ScalarValue::Int64(index as i64)
                }]
            });
            let stats = accumulator.group_lookup.stats();
            assert_eq!(stats.lookups, 513);
            assert_eq!(stats.hits, 0);
            assert_eq!(stats.misses, 513);
            assert_eq!(stats.owned_key_materializations, 513);
            assert_eq!(stats.owned_key_moves, 513);
            assert_eq!(stats.owned_key_clones, 0);
            assert_eq!(accumulator.groups.len(), 513);
            assert_eq!(
                accumulator.grouped_batch_stats(),
                super::GroupedBatchStats {
                    grouped_rows: 513,
                    borrow_only_hits: 0,
                    miss_rows: 513,
                    rows_with_scalar_transfer: 513,
                    extrema_replacement_rows: 0,
                }
            );
        }
    }

    #[test]
    fn grouped_borrow_first_extrema_transfer_only_on_actual_replacement() {
        let columns = batch_columns();
        let team = columns[1].clone();
        let payload = columns[3].clone();
        let input_fields = [
            OutputField::Source(team.clone()),
            OutputField::Source(payload.clone()),
        ];
        let extreme = |function, name: &str| {
            batch_aggregate_expression(
                function,
                AggregateInput::Column(payload.clone()),
                name,
                PhysicalType::Text,
                false,
            )
        };

        for (function, expected) in [
            (
                AggregateFunction::Min,
                super::GroupedBatchStats {
                    grouped_rows: 513,
                    borrow_only_hits: 512,
                    miss_rows: 1,
                    rows_with_scalar_transfer: 1,
                    extrema_replacement_rows: 1,
                },
            ),
            (
                AggregateFunction::Max,
                super::GroupedBatchStats {
                    grouped_rows: 513,
                    borrow_only_hits: 0,
                    miss_rows: 1,
                    rows_with_scalar_transfer: 513,
                    extrema_replacement_rows: 513,
                },
            ),
        ] {
            let outputs = [
                AggregateOutput::GroupKey(team.clone()),
                extreme(function, function.as_str()),
            ];
            let mut accumulator =
                AggregateAccumulator::new(&input_fields, std::slice::from_ref(&team), &outputs)
                    .expect("build grouped Text extreme accumulator");
            consume_generated_batches(&mut accumulator, 513, |index| {
                vec![
                    ScalarValue::UInt64(0),
                    ScalarValue::Text(format!("value-{index:04}")),
                ]
            });
            assert_eq!(accumulator.grouped_batch_stats(), expected);
            assert_eq!(accumulator.group_lookup.stats().owned_key_moves, 1);
            assert_eq!(accumulator.group_lookup.stats().owned_key_clones, 0);
        }
    }

    #[test]
    fn grouped_duplicate_max_keeps_clone_one_move_one_on_replacement() {
        let columns = batch_columns();
        let team = columns[1].clone();
        let payload = columns[3].clone();
        let input_fields = [
            OutputField::Source(team.clone()),
            OutputField::Source(payload.clone()),
        ];
        let maximum = |name: &str| {
            batch_aggregate_expression(
                AggregateFunction::Max,
                AggregateInput::Column(payload.clone()),
                name,
                PhysicalType::Text,
                false,
            )
        };
        let outputs = [
            AggregateOutput::GroupKey(team.clone()),
            maximum("MAX(payload)#1"),
            maximum("MAX(payload)#2"),
        ];
        let final_value = String::from("z-final");
        let final_pointer = final_value.as_ptr();
        let mut accumulator =
            AggregateAccumulator::new(&input_fields, std::slice::from_ref(&team), &outputs)
                .expect("build grouped duplicate MAX accumulator");
        let mut batch = ExecutionBatch::with_capacity();
        batch.rows.extend([
            ExecutionRow {
                row_id: None,
                values: vec![
                    ScalarValue::UInt64(0),
                    ScalarValue::Text(String::from("a-first")),
                ],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::UInt64(0), ScalarValue::Text(final_value)],
            },
        ]);
        accumulator
            .consume_batch(&mut batch)
            .expect("consume grouped duplicate MAX batch");
        assert_eq!(
            accumulator.grouped_batch_stats(),
            super::GroupedBatchStats {
                grouped_rows: 2,
                borrow_only_hits: 0,
                miss_rows: 1,
                rows_with_scalar_transfer: 2,
                extrema_replacement_rows: 2,
            }
        );
        assert_eq!(
            accumulator.groups[0]
                .aggregate_states
                .iter()
                .filter(|state| {
                    matches!(
                        state,
                        super::AggregateState::Max(super::ExtremeState::Text(Some(value)))
                            if value.as_ptr() == final_pointer
                    )
                })
                .count(),
            1
        );
    }

    #[test]
    fn owned_group_miss_clones_only_additional_text_and_primitive_owners() {
        let columns = batch_columns();
        let payload = columns[3].clone();
        let payload_fields = [OutputField::Source(payload.clone())];
        let extreme = |function, name: &str| {
            batch_aggregate_expression(
                function,
                AggregateInput::Column(payload.clone()),
                name,
                PhysicalType::Text,
                false,
            )
        };
        for max_owners in [1, 2] {
            let mut outputs = vec![AggregateOutput::GroupKey(payload.clone())];
            outputs.extend(
                (0..max_owners)
                    .map(|index| extreme(AggregateFunction::Max, &format!("MAX#{index}"))),
            );
            let text = String::from("unique-owned-text");
            let original_pointer = text.as_ptr();
            let hit_text = String::from("unique-owned-text");
            let hit_pointer = hit_text.as_ptr();
            let mut accumulator = AggregateAccumulator::new(
                &payload_fields,
                std::slice::from_ref(&payload),
                &outputs,
            )
            .expect("build overlapping Text owners");
            let mut batch = ExecutionBatch {
                rows: vec![
                    ExecutionRow {
                        row_id: None,
                        values: vec![ScalarValue::Text(text)],
                    },
                    ExecutionRow {
                        row_id: None,
                        values: vec![ScalarValue::Text(hit_text)],
                    },
                ],
            };
            accumulator
                .consume_batch(&mut batch)
                .expect("consume overlapping Text owners");

            let stats = accumulator.group_lookup.stats();
            assert_eq!(stats.misses, 1);
            assert_eq!(stats.hits, 1);
            assert_eq!(stats.owned_key_moves, 1);
            assert_eq!(stats.owned_key_clones, max_owners);
            let group = &accumulator.groups[0];
            let original_owners =
                usize::from(text_pointer(&group.key_values[0]) == original_pointer)
                    + group
                        .aggregate_states
                        .iter()
                        .filter(|state| {
                            matches!(
                                state,
                                super::AggregateState::Max(super::ExtremeState::Text(Some(value)))
                                    if value.as_ptr() == original_pointer
                            )
                        })
                        .count();
            assert_eq!(original_owners, 1);
            assert!(
                text_pointer(&group.key_values[0]) != hit_pointer
                    && group.aggregate_states.iter().all(|state| matches!(
                        state,
                        super::AggregateState::Max(super::ExtremeState::Text(Some(value)))
                            if value.as_ptr() != hit_pointer
                    ))
            );

            let result = accumulator
                .finish()
                .expect("finish overlapping Text owners");
            assert_eq!(result.rows[0].values.len(), max_owners + 1);
            assert!(
                result.rows[0]
                    .values
                    .iter()
                    .all(|value| value == &ScalarValue::Text("unique-owned-text".into()))
            );
        }

        let id = columns[0].clone();
        let id_fields = [OutputField::Source(id.clone())];
        let id_extreme = |function, name: &str| {
            batch_aggregate_expression(
                function,
                AggregateInput::Column(id.clone()),
                name,
                PhysicalType::Int64,
                false,
            )
        };
        let outputs = [
            AggregateOutput::GroupKey(id.clone()),
            id_extreme(AggregateFunction::Min, "MIN(id)"),
            id_extreme(AggregateFunction::Max, "MAX(id)"),
        ];
        let mut accumulator =
            AggregateAccumulator::new(&id_fields, std::slice::from_ref(&id), &outputs)
                .expect("build primitive overlapping owners");
        let mut batch = ExecutionBatch {
            rows: vec![ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Int64(17)],
            }],
        };
        accumulator
            .consume_batch(&mut batch)
            .expect("consume primitive overlapping owners");
        let stats = accumulator.group_lookup.stats();
        assert_eq!(stats.owned_key_moves, 1);
        assert_eq!(stats.owned_key_clones, 2);
        assert_eq!(
            accumulator
                .finish()
                .expect("finish primitive overlapping owners")
                .rows[0]
                .values,
            vec![
                ScalarValue::Int64(17),
                ScalarValue::Int64(17),
                ScalarValue::Int64(17),
            ]
        );
    }

    #[test]
    fn duplicate_group_key_slots_share_one_move_and_only_required_clones() {
        let payload = batch_columns()[3].clone();
        let input_fields = [OutputField::Source(payload.clone())];
        let group_keys = [payload.clone(), payload.clone()];
        let outputs = [
            AggregateOutput::GroupKey(payload.clone()),
            AggregateOutput::GroupKey(payload),
        ];
        let text = String::from("duplicate-group-key");
        let original_pointer = text.as_ptr();
        let mut accumulator = AggregateAccumulator::new(&input_fields, &group_keys, &outputs)
            .expect("build duplicate group-key owners");
        let mut batch = ExecutionBatch {
            rows: vec![ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::Text(text)],
            }],
        };
        accumulator
            .consume_batch(&mut batch)
            .expect("consume duplicate group-key owners");
        let stats = accumulator.group_lookup.stats();
        assert_eq!(stats.owned_key_moves, 1);
        assert_eq!(stats.owned_key_clones, 1);
        assert_eq!(accumulator.groups[0].key_values.len(), 2);
        assert_eq!(
            accumulator.groups[0]
                .key_values
                .iter()
                .filter(|value| text_pointer(value) == original_pointer)
                .count(),
            1
        );
    }

    #[test]
    #[expect(
        clippy::manual_hash_one,
        reason = "this structural test verifies the Hasher::finish contract directly"
    )]
    fn prehashed_bucket_hasher_passes_through_u64_values() {
        for value in [0, 1, u64::MAX, 0x0123_4567_89ab_cdef, 0xa5a5_5a5a_f0f0_0f0f] {
            let mut hasher = PrehashedBuildHasher.build_hasher();
            PrehashedKey(value).hash(&mut hasher);
            assert_eq!(hasher.finish(), value);
        }
    }

    #[test]
    #[should_panic(expected = "accepts only one opaque u64 prehash")]
    fn prehashed_bucket_hasher_rejects_generic_bytes() {
        let mut hasher = PrehashedBuildHasher.build_hasher();
        hasher.write(b"not a prehash");
    }

    #[test]
    fn prehashed_bucket_map_looks_up_distinct_hashes() {
        let mut lookup = GroupLookup::new();
        lookup.register_group(11, 0).expect("register hash A");
        lookup.register_group(29, 1).expect("register hash B");

        assert_eq!(lookup.bucket_heads.get(&PrehashedKey(11)).copied(), Some(0));
        assert_eq!(lookup.bucket_heads.get(&PrehashedKey(29)).copied(), Some(1));
    }

    #[test]
    fn group_key_hash_remains_random_state_keyed() {
        let key = batch_columns()[0].clone();
        let row = ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Int64(17)],
        };
        let random_state = RandomState::new();

        let first = hash_group_key(&random_state, &row, &[0], std::slice::from_ref(&key))
            .expect("hash group key");
        let second = hash_group_key(&random_state, &row, &[0], std::slice::from_ref(&key))
            .expect("rehash group key");
        assert_eq!(first, second);
    }

    #[test]
    fn group_lookup_collision_chain_uses_exact_typed_key_equality() {
        let columns = batch_columns();
        let groups = vec![
            GroupState {
                key_values: vec![
                    ScalarValue::Int64(1),
                    ScalarValue::UInt64(2),
                    ScalarValue::Bool(false),
                    ScalarValue::Text("alpha".into()),
                    ScalarValue::Null,
                ],
                aggregate_states: Vec::new(),
            },
            GroupState {
                key_values: vec![
                    ScalarValue::Int64(1),
                    ScalarValue::UInt64(2),
                    ScalarValue::Bool(false),
                    ScalarValue::Text("beta".into()),
                    ScalarValue::Null,
                ],
                aggregate_states: Vec::new(),
            },
        ];
        let positions = [0, 1, 2, 3, 4];
        let row = |text: &str, signed: i64, unsigned: u64| ExecutionRow {
            row_id: None,
            values: vec![
                ScalarValue::Int64(signed),
                ScalarValue::UInt64(unsigned),
                ScalarValue::Bool(false),
                ScalarValue::Text(text.into()),
                ScalarValue::Null,
            ],
        };
        let mut lookup = GroupLookup::new();
        lookup.register_group(7, 0).expect("register collision A");
        lookup.register_group(7, 1).expect("register collision B");
        let head = lookup.bucket_heads.get(&PrehashedKey(7)).copied();
        assert_eq!(
            lookup
                .find_in_bucket(&row("alpha", 1, 2), &positions, &columns, &groups, head)
                .expect("lookup collision A"),
            Some(0)
        );
        assert_eq!(
            lookup
                .find_in_bucket(&row("beta", 1, 2), &positions, &columns, &groups, head)
                .expect("lookup collision B"),
            Some(1)
        );
        assert_eq!(
            lookup
                .find_in_bucket(&row("gamma", 2, 1), &positions, &columns, &groups, head)
                .expect("lookup collision miss"),
            None
        );
        assert_eq!(lookup.stats().exact_collision_checks, 5);
    }

    #[test]
    fn forced_collision_group_hit_keeps_mutable_batch_row_borrowed() {
        let key = batch_columns()[3].clone();
        let input_fields = [OutputField::Source(key.clone())];
        let outputs = [AggregateOutput::GroupKey(key.clone()), count_output()];
        let mut accumulator =
            AggregateAccumulator::new(&input_fields, std::slice::from_ref(&key), &outputs)
                .expect("build forced-collision accumulator");
        accumulator.groups.extend([
            super::new_group_state(
                vec![ScalarValue::Text(String::from("alpha"))],
                &accumulator.aggregates,
            )
            .expect("build collision group A"),
            super::new_group_state(
                vec![ScalarValue::Text(String::from("beta"))],
                &accumulator.aggregates,
            )
            .expect("build collision group B"),
        ]);
        let text = String::from("alpha");
        let original_pointer = text.as_ptr();
        let mut row = ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Text(text)],
        };
        let hash = hash_group_key(
            &accumulator.group_lookup.key_hasher,
            &row,
            &accumulator.group_key_positions,
            accumulator.group_keys,
        )
        .expect("hash forced-collision row");
        accumulator
            .group_lookup
            .register_group(hash, 0)
            .expect("register collision group A");
        accumulator
            .group_lookup
            .register_group(hash, 1)
            .expect("register collision group B");

        accumulator
            .consume_grouped_row_mut(&mut row)
            .expect("consume forced-collision hit");
        assert_eq!(text_pointer(&row.values[0]), original_pointer);
        assert_eq!(row.values[0], ScalarValue::Text(String::from("alpha")));
        assert_eq!(
            accumulator.grouped_batch_stats(),
            super::GroupedBatchStats {
                grouped_rows: 1,
                borrow_only_hits: 1,
                miss_rows: 0,
                rows_with_scalar_transfer: 0,
                extrema_replacement_rows: 0,
            }
        );
        assert_eq!(accumulator.group_lookup.stats().exact_collision_checks, 2);
    }

    #[test]
    fn borrowed_group_lookup_matches_legacy_across_cardinality_and_batch_boundaries() {
        let key = batch_columns()[0].clone();
        let input_fields = [OutputField::Source(key.clone())];
        let outputs = [AggregateOutput::GroupKey(key.clone()), count_output()];
        for rows in [
            0,
            1,
            EXECUTION_BATCH_CAPACITY - 1,
            EXECUTION_BATCH_CAPACITY,
            EXECUTION_BATCH_CAPACITY + 1,
            2 * EXECUTION_BATCH_CAPACITY,
            2 * EXECUTION_BATCH_CAPACITY + 1,
        ] {
            let cardinalities = if rows == 0 {
                vec![1]
            } else {
                vec![1, rows.min(4), (rows / 2).max(1), rows]
            };
            for cardinality in cardinalities {
                let mut batch =
                    AggregateAccumulator::new(&input_fields, std::slice::from_ref(&key), &outputs)
                        .expect("build batch group accumulator");
                consume_generated_batches(&mut batch, rows, |index| {
                    vec![ScalarValue::Int64((index % cardinality) as i64)]
                });

                let materialized_rows = (0..rows)
                    .map(|index| ExecutionRow {
                        row_id: None,
                        values: vec![ScalarValue::Int64((index % cardinality) as i64)],
                    })
                    .collect::<Vec<_>>();
                let mut legacy =
                    AggregateAccumulator::new(&input_fields, std::slice::from_ref(&key), &outputs)
                        .expect("build legacy group accumulator");
                legacy
                    .consume_rows(&materialized_rows)
                    .expect("consume legacy grouped rows");

                let expected_groups = rows.min(cardinality);
                let stats = batch.group_lookup.stats();
                assert_eq!(stats.lookups, rows);
                assert_eq!(stats.misses, expected_groups);
                assert_eq!(stats.hits, rows - expected_groups);
                assert_eq!(stats.owned_key_materializations, expected_groups);
                assert_eq!(stats.owned_key_moves, expected_groups);
                assert_eq!(stats.owned_key_clones, 0);
                assert_eq!(
                    batch.grouped_batch_stats(),
                    super::GroupedBatchStats {
                        grouped_rows: rows,
                        borrow_only_hits: rows - expected_groups,
                        miss_rows: expected_groups,
                        rows_with_scalar_transfer: expected_groups,
                        extrema_replacement_rows: 0,
                    }
                );
                let batch = batch.finish().expect("finish batch groups");
                let legacy = legacy.finish().expect("finish legacy groups");
                assert_eq!(batch, legacy);
                assert_eq!(batch.rows.len(), expected_groups);
                for (position, row) in batch.rows.iter().enumerate() {
                    assert_eq!(row.values[0], ScalarValue::Int64(position as i64));
                    assert_eq!(
                        row.values[1],
                        ScalarValue::UInt64(((rows - 1 - position) / cardinality + 1) as u64)
                    );
                }
            }
        }
    }

    #[test]
    fn borrowed_multi_key_lookup_preserves_order_null_text_and_first_seen_groups() {
        let columns = batch_columns();
        let selected = [
            columns[0].clone(),
            columns[2].clone(),
            columns[4].clone(),
            columns[3].clone(),
        ];
        let input_fields = selected
            .iter()
            .cloned()
            .map(OutputField::Source)
            .collect::<Vec<_>>();
        let rows = || {
            [
                (1, true, ScalarValue::Int64(2), "alpha"),
                (2, false, ScalarValue::Int64(1), "beta"),
                (1, true, ScalarValue::Int64(2), "alpha"),
                (1, false, ScalarValue::Null, "gamma"),
                (1, false, ScalarValue::Null, "gamma"),
            ]
            .into_iter()
            .map(|(id, active, nullable, text)| ExecutionRow {
                row_id: None,
                values: vec![
                    ScalarValue::Int64(id),
                    ScalarValue::Bool(active),
                    nullable,
                    ScalarValue::Text(text.into()),
                ],
            })
            .collect::<Vec<_>>()
        };
        for group_keys in [
            vec![selected[0].clone(), selected[1].clone()],
            vec![selected[0].clone(), selected[2].clone()],
            vec![selected[0].clone(), selected[3].clone()],
        ] {
            let mut outputs = group_keys
                .iter()
                .cloned()
                .map(AggregateOutput::GroupKey)
                .collect::<Vec<_>>();
            outputs.push(count_output());
            let mut batch = AggregateAccumulator::new(&input_fields, &group_keys, &outputs)
                .expect("build multi-key batch accumulator");
            let mut execution_batch = ExecutionBatch { rows: rows() };
            batch
                .consume_batch(&mut execution_batch)
                .expect("consume multi-key batch");
            let stats = batch.group_lookup.stats();
            assert_eq!(stats.owned_key_moves, stats.misses * group_keys.len());
            assert_eq!(stats.owned_key_clones, 0);
            let mut legacy = AggregateAccumulator::new(&input_fields, &group_keys, &outputs)
                .expect("build multi-key legacy accumulator");
            legacy
                .consume_rows(&rows())
                .expect("consume multi-key legacy rows");
            assert_eq!(
                batch.finish().expect("finish multi-key batch"),
                legacy.finish().expect("finish multi-key legacy")
            );
        }

        let group_keys = vec![selected[0].clone(), selected[2].clone()];
        let outputs = vec![
            AggregateOutput::GroupKey(selected[0].clone()),
            AggregateOutput::GroupKey(selected[2].clone()),
            count_output(),
        ];
        let mut accumulator = AggregateAccumulator::new(&input_fields, &group_keys, &outputs)
            .expect("build ordered nullable-key accumulator");
        accumulator
            .consume_rows(&rows())
            .expect("consume ordered nullable keys");
        assert_eq!(
            accumulator
                .finish()
                .expect("finish ordered nullable keys")
                .rows,
            vec![
                super::ExecutionRow {
                    row_id: None,
                    values: vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::UInt64(2),
                    ],
                },
                super::ExecutionRow {
                    row_id: None,
                    values: vec![
                        ScalarValue::Int64(2),
                        ScalarValue::Int64(1),
                        ScalarValue::UInt64(1),
                    ],
                },
                super::ExecutionRow {
                    row_id: None,
                    values: vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Null,
                        ScalarValue::UInt64(2),
                    ],
                },
            ]
        );
    }

    #[test]
    fn move_aware_aggregate_moves_one_actual_replacement_and_clones_additional_owners() {
        let payload = batch_columns()[3].clone();
        let input_fields = [OutputField::Source(payload.clone())];
        let extreme = |function, name: &str| {
            batch_aggregate_expression(
                function,
                AggregateInput::Column(payload.clone()),
                name,
                PhysicalType::Text,
                true,
            )
        };
        let row = |value: String| ExecutionRow {
            row_id: None,
            values: vec![ScalarValue::Text(value)],
        };

        let final_max = String::from("z-final-max");
        let final_max_pointer = final_max.as_ptr();
        let outputs = [extreme(AggregateFunction::Max, "MAX(payload)")];
        let mut accumulator = AggregateAccumulator::new(&input_fields, &[], &outputs)
            .expect("build single-MAX accumulator");
        let mut batch = ExecutionBatch::with_capacity();
        let capacity = batch.rows.capacity();
        batch
            .rows
            .extend([row(String::from("a-first")), row(final_max)]);
        accumulator
            .consume_batch(&mut batch)
            .expect("consume owned MAX batch");
        assert!(batch.rows.is_empty());
        assert_eq!(batch.rows.capacity(), capacity);
        let result = accumulator.finish().expect("finish single MAX");
        assert_eq!(text_pointer(&result.rows[0].values[0]), final_max_pointer);

        let duplicate_max = String::from("z-duplicate-max");
        let duplicate_max_pointer = duplicate_max.as_ptr();
        let outputs = [
            extreme(AggregateFunction::Max, "MAX(payload)#1"),
            extreme(AggregateFunction::Max, "MAX(payload)#2"),
        ];
        let mut accumulator = AggregateAccumulator::new(&input_fields, &[], &outputs)
            .expect("build duplicate-MAX accumulator");
        let mut batch = ExecutionBatch::with_capacity();
        batch.rows.push(row(duplicate_max));
        accumulator
            .consume_batch(&mut batch)
            .expect("consume duplicate MAX batch");
        let result = accumulator.finish().expect("finish duplicate MAX");
        assert_eq!(result.rows[0].values.len(), 2);
        assert_eq!(
            result.rows[0]
                .values
                .iter()
                .filter(|value| text_pointer(value) == duplicate_max_pointer)
                .count(),
            1
        );

        let group_value = String::from("owned-group-key");
        let group_outputs = [
            AggregateOutput::GroupKey(payload.clone()),
            AggregateOutput::GroupKey(payload.clone()),
        ];
        let mut accumulator = AggregateAccumulator::new(
            &input_fields,
            std::slice::from_ref(&payload),
            &group_outputs,
        )
        .expect("build duplicate group-output accumulator");
        let mut batch = ExecutionBatch::with_capacity();
        batch.rows.push(row(group_value));
        accumulator
            .consume_batch(&mut batch)
            .expect("consume duplicate group-output batch");
        let group_pointer = text_pointer(&accumulator.groups[0].key_values[0]);
        let result = accumulator.finish().expect("finish duplicate group output");
        assert_eq!(
            result.rows[0]
                .values
                .iter()
                .filter(|value| text_pointer(value) == group_pointer)
                .count(),
            1
        );

        let later_max = String::from("z-later-max");
        let later_max_pointer = later_max.as_ptr();
        let outputs = [
            extreme(AggregateFunction::Min, "MIN(payload)"),
            extreme(AggregateFunction::Max, "MAX(payload)"),
        ];
        let mut accumulator = AggregateAccumulator::new(&input_fields, &[], &outputs)
            .expect("build MIN/MAX accumulator");
        let mut batch = ExecutionBatch::with_capacity();
        batch
            .rows
            .extend([row(String::from("a-first-min")), row(later_max)]);
        accumulator
            .consume_batch(&mut batch)
            .expect("consume MIN/MAX batch");
        let result = accumulator.finish().expect("finish MIN/MAX");
        assert_eq!(
            result.rows[0].values,
            [
                ScalarValue::Text(String::from("a-first-min")),
                ScalarValue::Text(String::from("z-later-max")),
            ]
        );
        assert_eq!(text_pointer(&result.rows[0].values[1]), later_max_pointer);
    }

    #[test]
    fn move_aware_text_extremes_match_legacy_at_every_batch_boundary() {
        let columns = batch_columns();
        let extreme = |function, name: &str| {
            batch_aggregate_expression(
                function,
                AggregateInput::Column(columns[3].clone()),
                name,
                PhysicalType::Text,
                true,
            )
        };
        for rows in [
            0,
            1,
            EXECUTION_BATCH_CAPACITY - 1,
            EXECUTION_BATCH_CAPACITY,
            EXECUTION_BATCH_CAPACITY + 1,
            2 * EXECUTION_BATCH_CAPACITY,
            2 * EXECUTION_BATCH_CAPACITY + 1,
        ] {
            let (mut storage, path) =
                batch_storage(&format!("move-aware-extremes-{rows}"), false, rows);
            let plans = [
                vec![extreme(AggregateFunction::Min, "MIN(payload)")],
                vec![extreme(AggregateFunction::Max, "MAX(payload)")],
                vec![
                    extreme(AggregateFunction::Min, "MIN(payload)"),
                    extreme(AggregateFunction::Max, "MAX(payload)"),
                ],
                vec![
                    extreme(AggregateFunction::Min, "MIN(payload)#1"),
                    extreme(AggregateFunction::Min, "MIN(payload)#2"),
                ],
                vec![
                    extreme(AggregateFunction::Max, "MAX(payload)#1"),
                    extreme(AggregateFunction::Max, "MAX(payload)#2"),
                ],
            ];
            for outputs in plans {
                assert_batch_matches_legacy(
                    &batch_aggregate(batch_scan(columns.clone()), Vec::new(), outputs),
                    &mut storage,
                );
            }
            storage.close().expect("close move-aware boundary Heap");
            remove_batch_test_path(&path, false);
        }
    }

    #[test]
    fn move_aware_text_extremes_preserve_null_repetition_and_alternation() {
        let payload = batch_columns()[3].clone();
        let input_fields = [OutputField::Source(payload.clone())];
        let outputs = [
            batch_aggregate_expression(
                AggregateFunction::Min,
                AggregateInput::Column(payload.clone()),
                "MIN(payload)",
                PhysicalType::Text,
                true,
            ),
            batch_aggregate_expression(
                AggregateFunction::Max,
                AggregateInput::Column(payload),
                "MAX(payload)",
                PhysicalType::Text,
                true,
            ),
        ];
        for (values, expected) in [
            (
                vec![
                    ScalarValue::Text(String::from("same")),
                    ScalarValue::Text(String::from("same")),
                ],
                vec![
                    ScalarValue::Text(String::from("same")),
                    ScalarValue::Text(String::from("same")),
                ],
            ),
            (
                vec![
                    ScalarValue::Text(String::from("middle")),
                    ScalarValue::Null,
                    ScalarValue::Text(String::from("high")),
                    ScalarValue::Text(String::from("low")),
                    ScalarValue::Text(String::from("high")),
                ],
                vec![
                    ScalarValue::Text(String::from("high")),
                    ScalarValue::Text(String::from("middle")),
                ],
            ),
            (
                vec![ScalarValue::Null, ScalarValue::Null],
                vec![ScalarValue::Null, ScalarValue::Null],
            ),
        ] {
            let rows = values
                .into_iter()
                .map(|value| ExecutionRow {
                    row_id: None,
                    values: vec![value],
                })
                .collect::<Vec<_>>();
            let mut accumulator = AggregateAccumulator::new(&input_fields, &[], &outputs)
                .expect("build Text pattern accumulator");
            let mut batch = ExecutionBatch { rows };
            accumulator
                .consume_batch(&mut batch)
                .expect("consume Text pattern");
            assert_eq!(
                accumulator.finish().expect("finish Text pattern").rows[0].values,
                expected
            );
        }
    }

    #[test]
    fn batch_scan_cardinalities_cross_every_runtime_boundary() {
        let columns = batch_columns();
        for rows in [
            0,
            1,
            EXECUTION_BATCH_CAPACITY - 1,
            EXECUTION_BATCH_CAPACITY,
            EXECUTION_BATCH_CAPACITY + 1,
            2 * EXECUTION_BATCH_CAPACITY,
            2 * EXECUTION_BATCH_CAPACITY + 1,
        ] {
            let (mut storage, path) = batch_storage(&format!("cardinality-{rows}"), false, rows);
            let plan = batch_scan(vec![columns[0].clone(), columns[3].clone()]);
            assert_batch_matches_legacy(&plan, &mut storage);
            let result = execute(&plan, &mut storage).expect("materialize owned query result");
            assert_eq!(result.rows.len(), rows);
            assert!(result.rows.iter().all(|row| row.len() == 2));

            let zero_width = batch_scan(Vec::new());
            assert_batch_matches_legacy(&zero_width, &mut storage);
            assert!(
                execute(&zero_width, &mut storage)
                    .expect("zero-width scan")
                    .rows
                    .iter()
                    .all(Vec::is_empty)
            );
            storage.close().expect("close cardinality Heap");
            remove_batch_test_path(&path, false);
        }
    }

    #[test]
    fn batch_filter_project_and_limit_match_authoritative_semantics() {
        let columns = batch_columns();
        let (mut storage, path) =
            batch_storage("operators", false, 2 * EXECUTION_BATCH_CAPACITY + 1);
        let scan = || batch_scan(columns.clone());
        let true_literal = batch_literal(ScalarValue::Bool(true), PhysicalType::Bool);
        let false_literal = batch_literal(ScalarValue::Bool(false), PhysicalType::Bool);
        let active = batch_binary(
            BinaryOp::Eq,
            batch_column_expression(&columns[2]),
            batch_literal(ScalarValue::Bool(true), PhysicalType::Bool),
        );
        let text = batch_binary(
            BinaryOp::Eq,
            batch_column_expression(&columns[3]),
            batch_literal(ScalarValue::Text("group-2".into()), PhysicalType::Text),
        );
        let nullable = batch_binary(
            BinaryOp::Eq,
            batch_column_expression(&columns[4]),
            batch_literal(ScalarValue::Int64(1), PhysicalType::Int64),
        );
        let repeated_id = batch_binary(
            BinaryOp::And,
            batch_binary(
                BinaryOp::GtEq,
                batch_column_expression(&columns[0]),
                batch_literal(ScalarValue::Int64(10), PhysicalType::Int64),
            ),
            batch_binary(
                BinaryOp::LtEq,
                batch_column_expression(&columns[0]),
                batch_literal(ScalarValue::Int64(20), PhysicalType::Int64),
            ),
        );
        let multiple_columns = batch_binary(BinaryOp::And, active.clone(), text.clone());

        // Retaining the Text predicate column is intentionally not eligible
        // for the predicate-only borrowed specialization, so this exercises
        // Text comparison inside the position-bound owned batch Filter.
        assert_batch_matches_legacy(
            &batch_project(batch_filter(scan(), text.clone()), vec![columns[3].clone()]),
            &mut storage,
        );

        for predicate in [
            true_literal,
            false_literal,
            active.clone(),
            text,
            nullable,
            repeated_id,
            multiple_columns,
        ] {
            assert_batch_matches_legacy(&batch_filter(scan(), predicate), &mut storage);
        }

        for projected in [
            columns.clone(),
            vec![columns[0].clone()],
            vec![columns[3].clone(), columns[0].clone()],
            vec![columns[3].clone(), columns[3].clone()],
            Vec::new(),
        ] {
            assert_batch_matches_legacy(&batch_project(scan(), projected), &mut storage);
        }

        for limit in [
            0,
            1,
            (EXECUTION_BATCH_CAPACITY - 1) as u64,
            EXECUTION_BATCH_CAPACITY as u64,
            (EXECUTION_BATCH_CAPACITY + 1) as u64,
            (2 * EXECUTION_BATCH_CAPACITY) as u64,
            (2 * EXECUTION_BATCH_CAPACITY + 2) as u64,
        ] {
            assert_batch_matches_legacy(&batch_limit(scan(), limit), &mut storage);
        }

        let filtered_limit = batch_limit(batch_filter(scan(), active.clone()), 20);
        assert_batch_matches_legacy(&filtered_limit, &mut storage);
        let complete = batch_limit(
            batch_project(
                batch_filter(scan(), active),
                vec![columns[3].clone(), columns[0].clone(), columns[3].clone()],
            ),
            20,
        );
        assert_batch_matches_legacy(&complete, &mut storage);
        storage.close().expect("close operator Heap");
        remove_batch_test_path(&path, false);
    }

    #[test]
    fn top_n_state_bounds_retention_and_preserves_equal_key_input_order() {
        let key_column = batch_columns()[1].clone();
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            let keys = [SortKey {
                column: key_column.clone(),
                direction,
                null_order: NullOrder::Last,
            }];
            for limit in [0, 1, 20, 256, 257, 600] {
                let mut state = TopNState::new(limit, &keys, &[0]);
                for index in 0..513 {
                    state
                        .consider(ExecutionRow {
                            row_id: None,
                            values: vec![ScalarValue::UInt64(7), ScalarValue::Int64(index as i64)],
                        })
                        .expect("consume equal-key Top-N candidate");
                }
                assert_eq!(state.rows_seen, 513);
                assert_eq!(state.max_retained, limit.min(513));
                assert!(state.candidates.len() <= limit.min(513));
                assert!(state.candidates_inserted <= 513);
                let rows = state.into_sorted_rows().expect("finish equal-key Top-N");
                assert_eq!(rows.len(), limit.min(513));
                assert_eq!(
                    rows.into_iter()
                        .map(|row| row.values[1].clone())
                        .collect::<Vec<_>>(),
                    (0..limit.min(513))
                        .map(|index| ScalarValue::Int64(index as i64))
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn top_n_matches_legacy_at_every_batch_and_limit_boundary() {
        let columns = batch_columns();
        let key = SortKey {
            column: columns[1].clone(),
            direction: SortDirection::Asc,
            null_order: NullOrder::Last,
        };
        for rows in [
            0,
            1,
            EXECUTION_BATCH_CAPACITY - 1,
            EXECUTION_BATCH_CAPACITY,
            EXECUTION_BATCH_CAPACITY + 1,
            2 * EXECUTION_BATCH_CAPACITY,
            2 * EXECUTION_BATCH_CAPACITY + 1,
        ] {
            let (mut storage, path) = batch_storage(&format!("top-n-boundary-{rows}"), false, rows);
            for limit in [0, 1, 20, 255, 256, 257] {
                let plan = batch_top_n(
                    batch_scan(columns.clone()),
                    vec![key.clone()],
                    vec![columns[0].clone()],
                    limit,
                );
                assert!(build_top_n_plan(&plan).is_some());
                assert_batch_matches_legacy(&plan, &mut storage);
            }
            storage.close().expect("close Top-N boundary Heap");
            remove_batch_test_path(&path, false);
        }
    }

    #[test]
    fn top_n_matches_legacy_for_filters_types_nulls_and_duplicate_projection() {
        let columns = batch_columns();
        let rows = 2 * EXECUTION_BATCH_CAPACITY + 1;
        let (mut storage, path) = batch_storage("top-n-semantics", false, rows);
        let sort_key = |column: ColumnRef, direction, null_order| SortKey {
            column,
            direction,
            null_order,
        };
        let scan = || batch_scan(columns.clone());
        let active = || {
            batch_binary(
                BinaryOp::Eq,
                batch_column_expression(&columns[2]),
                batch_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            )
        };

        let mut plans = vec![
            batch_top_n(
                scan(),
                vec![sort_key(
                    columns[1].clone(),
                    SortDirection::Asc,
                    NullOrder::Last,
                )],
                vec![columns[0].clone()],
                20,
            ),
            batch_top_n(
                scan(),
                vec![sort_key(
                    columns[1].clone(),
                    SortDirection::Desc,
                    NullOrder::First,
                )],
                vec![columns[0].clone()],
                20,
            ),
            batch_top_n(
                scan(),
                vec![
                    sort_key(columns[1].clone(), SortDirection::Asc, NullOrder::Last),
                    sort_key(columns[0].clone(), SortDirection::Desc, NullOrder::First),
                ],
                vec![columns[0].clone()],
                20,
            ),
            batch_top_n(
                batch_filter(scan(), active()),
                vec![sort_key(
                    columns[1].clone(),
                    SortDirection::Asc,
                    NullOrder::Last,
                )],
                vec![columns[0].clone()],
                20,
            ),
            batch_top_n(
                scan(),
                vec![sort_key(
                    columns[3].clone(),
                    SortDirection::Desc,
                    NullOrder::First,
                )],
                vec![columns[0].clone()],
                20,
            ),
            batch_top_n(
                batch_project(
                    scan(),
                    vec![columns[3].clone(), columns[0].clone(), columns[4].clone()],
                ),
                vec![sort_key(
                    columns[4].clone(),
                    SortDirection::Asc,
                    NullOrder::First,
                )],
                vec![columns[3].clone(), columns[0].clone(), columns[3].clone()],
                20,
            ),
        ];
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            for null_order in [NullOrder::First, NullOrder::Last] {
                plans.push(batch_top_n(
                    scan(),
                    vec![sort_key(columns[4].clone(), direction, null_order)],
                    vec![columns[0].clone()],
                    20,
                ));
            }
        }
        for plan in plans {
            assert_batch_matches_legacy(&plan, &mut storage);
        }
        storage.close().expect("close Top-N semantics Heap");
        remove_batch_test_path(&path, false);
    }

    #[test]
    fn top_n_falls_back_for_ineligible_or_malformed_setup_and_keeps_runtime_errors() {
        let columns = batch_columns();
        let key = SortKey {
            column: columns[0].clone(),
            direction: SortDirection::Asc,
            null_order: NullOrder::Last,
        };
        let full_sort = batch_project(
            batch_sort(batch_scan(columns.clone()), vec![key.clone()]),
            vec![columns[0].clone()],
        );
        assert!(build_top_n_plan(&full_sort).is_none());

        let mut missing = columns[0].clone();
        missing.column_id = ColumnId(99);
        missing.name = "missing".into();
        let missing_plan = batch_top_n(
            batch_scan(columns.clone()),
            vec![SortKey {
                column: missing,
                direction: SortDirection::Asc,
                null_order: NullOrder::Last,
            }],
            vec![columns[0].clone()],
            1,
        );
        assert!(build_top_n_plan(&missing_plan).is_none());

        let (mut empty, empty_path) = batch_storage("top-n-malformed-empty", false, 0);
        assert!(matches!(
            execute_rows(&missing_plan, std::slice::from_mut(&mut empty)),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));
        assert!(matches!(
            execute_rows_legacy(&missing_plan, std::slice::from_mut(&mut empty)),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));
        empty.close().expect("close malformed empty Heap");
        remove_batch_test_path(&empty_path, false);

        let mut mismatched = columns[0].clone();
        mismatched.data_type = SemanticType::physical(PhysicalType::UInt64);
        let mismatched_plan = batch_top_n(
            batch_scan(columns),
            vec![SortKey {
                column: mismatched,
                direction: SortDirection::Asc,
                null_order: NullOrder::Last,
            }],
            vec![key.column],
            0,
        );
        assert!(build_top_n_plan(&mismatched_plan).is_some());
        let (mut storage, path) = batch_storage("top-n-runtime-error", false, 513);
        assert!(matches!(
            execute_rows(&mismatched_plan, std::slice::from_mut(&mut storage)),
            Err(ExecutionError::TypeMismatch)
        ));
        assert!(matches!(
            execute_rows_legacy(&mismatched_plan, std::slice::from_mut(&mut storage)),
            Err(ExecutionError::TypeMismatch)
        ));
        storage.close().expect("close Top-N runtime-error Heap");
        remove_batch_test_path(&path, false);
    }

    #[test]
    fn top_n_results_are_equivalent_across_heap_lsm_and_legacy() {
        let columns = batch_columns();
        let rows = 2 * EXECUTION_BATCH_CAPACITY + 1;
        let (mut heap, heap_path) = batch_storage("top-n-engine", false, rows);
        let (mut lsm, lsm_path) = batch_storage("top-n-engine", true, rows);
        let plan = batch_top_n(
            batch_scan(columns.clone()),
            vec![
                SortKey {
                    column: columns[1].clone(),
                    direction: SortDirection::Asc,
                    null_order: NullOrder::Last,
                },
                SortKey {
                    column: columns[0].clone(),
                    direction: SortDirection::Desc,
                    null_order: NullOrder::First,
                },
            ],
            vec![columns[3].clone(), columns[0].clone()],
            20,
        );
        let heap_result = execute(&plan, &mut heap).expect("execute Heap Top-N");
        let lsm_result = execute(&plan, &mut lsm).expect("execute LSM Top-N");
        assert_eq!(heap_result, lsm_result);
        assert_batch_matches_legacy(&plan, &mut heap);
        assert_batch_matches_legacy(&plan, &mut lsm);
        heap.close().expect("close Top-N Heap");
        lsm.close().expect("close Top-N LSM");
        remove_batch_test_path(&heap_path, false);
        remove_batch_test_path(&lsm_path, true);
    }

    #[test]
    fn batch_queries_are_equivalent_across_heap_and_lsm() {
        let columns = batch_columns();
        let rows = 2 * EXECUTION_BATCH_CAPACITY + 1;
        let (mut heap, heap_path) = batch_storage("engine-equivalence", false, rows);
        let (mut lsm, lsm_path) = batch_storage("engine-equivalence", true, rows);
        let scan = || batch_scan(columns.clone());
        let active = || {
            batch_binary(
                BinaryOp::Eq,
                batch_column_expression(&columns[2]),
                batch_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            )
        };
        let plans = [
            batch_project(scan(), vec![columns[3].clone(), columns[0].clone()]),
            batch_project(
                batch_filter(scan(), active()),
                vec![columns[0].clone(), columns[3].clone()],
            ),
            batch_limit(batch_project(scan(), vec![columns[0].clone()]), 20),
            batch_limit(
                batch_project(
                    batch_filter(scan(), active()),
                    vec![columns[0].clone(), columns[3].clone()],
                ),
                20,
            ),
        ];
        for plan in plans {
            let heap_result = execute(&plan, &mut heap).expect("execute Heap batch query");
            let lsm_result = execute(&plan, &mut lsm).expect("execute LSM batch query");
            assert_eq!(heap_result, lsm_result);
            assert_batch_matches_legacy(&plan, &mut heap);
            assert_batch_matches_legacy(&plan, &mut lsm);
        }
        heap.close().expect("close equivalence Heap");
        lsm.close().expect("close equivalence LSM");
        remove_batch_test_path(&heap_path, false);
        remove_batch_test_path(&lsm_path, true);
    }

    #[test]
    fn batch_aggregate_cardinalities_nulls_types_and_group_order_match_legacy() {
        let columns = batch_columns();
        for rows in [
            0,
            1,
            EXECUTION_BATCH_CAPACITY - 1,
            EXECUTION_BATCH_CAPACITY,
            EXECUTION_BATCH_CAPACITY + 1,
            2 * EXECUTION_BATCH_CAPACITY,
            2 * EXECUTION_BATCH_CAPACITY + 1,
        ] {
            let (mut storage, path) =
                batch_storage(&format!("aggregate-cardinality-{rows}"), false, rows);
            let global = batch_aggregate(
                batch_scan(columns.clone()),
                Vec::new(),
                vec![
                    batch_aggregate_expression(
                        AggregateFunction::Sum,
                        AggregateInput::Column(columns[0].clone()),
                        "SUM(id)",
                        PhysicalType::Int64,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Count,
                        AggregateInput::All,
                        "COUNT(*)",
                        PhysicalType::UInt64,
                        false,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Sum,
                        AggregateInput::Column(columns[1].clone()),
                        "SUM(unsigned_key)",
                        PhysicalType::UInt64,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Min,
                        AggregateInput::Column(columns[0].clone()),
                        "MIN(id)",
                        PhysicalType::Int64,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Max,
                        AggregateInput::Column(columns[0].clone()),
                        "MAX(id)",
                        PhysicalType::Int64,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Min,
                        AggregateInput::Column(columns[3].clone()),
                        "MIN(payload)",
                        PhysicalType::Text,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Max,
                        AggregateInput::Column(columns[3].clone()),
                        "MAX(payload)",
                        PhysicalType::Text,
                        true,
                    ),
                ],
            );
            assert_batch_matches_legacy(&global, &mut storage);
            let result = execute_rows(&global, std::slice::from_mut(&mut storage))
                .expect("execute global batch aggregate");
            let expected_sum =
                i64::try_from(rows * rows.saturating_sub(1) / 2).expect("test SUM fits i64");
            let expected_unsigned = (0..rows).map(|index| (index % 7) as u64).sum::<u64>();
            let expected = if rows == 0 {
                vec![
                    ScalarValue::Null,
                    ScalarValue::UInt64(0),
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Null,
                ]
            } else {
                vec![
                    ScalarValue::Int64(expected_sum),
                    ScalarValue::UInt64(rows as u64),
                    ScalarValue::UInt64(expected_unsigned),
                    ScalarValue::Int64(0),
                    ScalarValue::Int64(i64::try_from(rows - 1).expect("MAX fits i64")),
                    ScalarValue::Text("group-0".into()),
                    ScalarValue::Text(format!("group-{}", (rows - 1).min(4))),
                ]
            };
            assert_eq!(
                result.rows,
                vec![super::ExecutionRow {
                    row_id: None,
                    values: expected
                }]
            );

            let grouped = batch_aggregate(
                batch_scan(columns.clone()),
                vec![columns[1].clone()],
                vec![
                    batch_aggregate_expression(
                        AggregateFunction::Count,
                        AggregateInput::All,
                        "COUNT(*)",
                        PhysicalType::UInt64,
                        false,
                    ),
                    AggregateOutput::GroupKey(columns[1].clone()),
                    batch_aggregate_expression(
                        AggregateFunction::Sum,
                        AggregateInput::Column(columns[0].clone()),
                        "SUM(id)",
                        PhysicalType::Int64,
                        true,
                    ),
                ],
            );
            assert_batch_matches_legacy(&grouped, &mut storage);
            let grouped_result = execute_rows(&grouped, std::slice::from_mut(&mut storage))
                .expect("execute grouped batch aggregate");
            assert_eq!(grouped_result.rows.len(), rows.min(7));
            for (position, row) in grouped_result.rows.iter().enumerate() {
                assert_eq!(
                    row.values.get(1),
                    Some(&ScalarValue::UInt64(position as u64))
                );
            }

            if rows == 2 * EXECUTION_BATCH_CAPACITY + 1 {
                assert_eq!(
                    produced_batch_sizes(&batch_scan(columns.clone()), &mut storage),
                    vec![EXECUTION_BATCH_CAPACITY, EXECUTION_BATCH_CAPACITY, 1]
                );
                let high_cardinality = batch_aggregate(
                    batch_scan(columns.clone()),
                    vec![columns[0].clone()],
                    vec![
                        AggregateOutput::GroupKey(columns[0].clone()),
                        batch_aggregate_expression(
                            AggregateFunction::Count,
                            AggregateInput::All,
                            "COUNT(*)",
                            PhysicalType::UInt64,
                            false,
                        ),
                    ],
                );
                assert_batch_matches_legacy(&high_cardinality, &mut storage);
                let high_cardinality_result =
                    execute_rows(&high_cardinality, std::slice::from_mut(&mut storage))
                        .expect("execute high-cardinality aggregate");
                assert_eq!(high_cardinality_result.rows.len(), rows);
                assert_eq!(
                    high_cardinality_result
                        .rows
                        .last()
                        .expect("high-cardinality result has a final group")
                        .values,
                    vec![
                        ScalarValue::Int64((rows - 1) as i64),
                        ScalarValue::UInt64(1),
                    ]
                );
            }
            storage.close().expect("close aggregate cardinality Heap");
            remove_batch_test_path(&path, false);
        }
    }

    #[test]
    fn batch_aggregate_filtered_children_limit_and_all_null_match_legacy() {
        let columns = batch_columns();
        let rows = 2 * EXECUTION_BATCH_CAPACITY + 1;
        let (mut storage, path) = batch_storage("aggregate-filtered", false, rows);
        let grouped_outputs = || {
            vec![
                AggregateOutput::GroupKey(columns[1].clone()),
                batch_aggregate_expression(
                    AggregateFunction::Count,
                    AggregateInput::All,
                    "COUNT(*)",
                    PhysicalType::UInt64,
                    false,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Sum,
                    AggregateInput::Column(columns[0].clone()),
                    "SUM(id)",
                    PhysicalType::Int64,
                    true,
                ),
            ]
        };
        let predicates = [
            batch_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            batch_literal(ScalarValue::Bool(false), PhysicalType::Bool),
            batch_binary(
                BinaryOp::Eq,
                batch_column_expression(&columns[2]),
                batch_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            ),
            batch_binary(
                BinaryOp::Eq,
                batch_column_expression(&columns[4]),
                batch_literal(ScalarValue::Int64(1), PhysicalType::Int64),
            ),
            batch_binary(
                BinaryOp::Eq,
                batch_column_expression(&columns[3]),
                batch_literal(ScalarValue::Text("group-2".into()), PhysicalType::Text),
            ),
        ];
        for predicate in &predicates {
            let plan = batch_aggregate(
                batch_filter(batch_scan(columns.clone()), predicate.clone()),
                vec![columns[1].clone()],
                grouped_outputs(),
            );
            assert_batch_matches_legacy(&plan, &mut storage);
        }
        for predicate in predicates {
            let plan = batch_aggregate(
                batch_filter(batch_scan(columns.clone()), predicate),
                vec![columns[1].clone()],
                vec![
                    AggregateOutput::GroupKey(columns[1].clone()),
                    batch_aggregate_expression(
                        AggregateFunction::Min,
                        AggregateInput::Column(columns[3].clone()),
                        "MIN(payload)",
                        PhysicalType::Text,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Max,
                        AggregateInput::Column(columns[3].clone()),
                        "MAX(payload)",
                        PhysicalType::Text,
                        true,
                    ),
                ],
            );
            assert_batch_matches_legacy(&plan, &mut storage);
        }
        let projected_child = batch_aggregate(
            batch_project(
                batch_scan(columns.clone()),
                vec![columns[1].clone(), columns[0].clone()],
            ),
            vec![columns[1].clone()],
            grouped_outputs(),
        );
        assert_batch_matches_legacy(&projected_child, &mut storage);

        let multiple_group_columns = batch_aggregate(
            batch_scan(columns.clone()),
            vec![columns[1].clone(), columns[2].clone()],
            vec![
                AggregateOutput::GroupKey(columns[2].clone()),
                batch_aggregate_expression(
                    AggregateFunction::Count,
                    AggregateInput::All,
                    "COUNT(*)",
                    PhysicalType::UInt64,
                    false,
                ),
                AggregateOutput::GroupKey(columns[1].clone()),
            ],
        );
        assert_batch_matches_legacy(&multiple_group_columns, &mut storage);

        let nullable_group_extremes = batch_aggregate(
            batch_scan(columns.clone()),
            vec![columns[4].clone()],
            vec![
                AggregateOutput::GroupKey(columns[4].clone()),
                batch_aggregate_expression(
                    AggregateFunction::Min,
                    AggregateInput::Column(columns[3].clone()),
                    "MIN(payload)",
                    PhysicalType::Text,
                    true,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Max,
                    AggregateInput::Column(columns[3].clone()),
                    "MAX(payload)",
                    PhysicalType::Text,
                    true,
                ),
            ],
        );
        assert_batch_matches_legacy(&nullable_group_extremes, &mut storage);

        let primitive_extremes = batch_aggregate(
            batch_scan(columns.clone()),
            Vec::new(),
            vec![
                batch_aggregate_expression(
                    AggregateFunction::Min,
                    AggregateInput::Column(columns[0].clone()),
                    "MIN(id)",
                    PhysicalType::Int64,
                    true,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Max,
                    AggregateInput::Column(columns[1].clone()),
                    "MAX(unsigned_key)",
                    PhysicalType::UInt64,
                    true,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Min,
                    AggregateInput::Column(columns[2].clone()),
                    "MIN(active)",
                    PhysicalType::Bool,
                    true,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Max,
                    AggregateInput::Column(columns[2].clone()),
                    "MAX(active)",
                    PhysicalType::Bool,
                    true,
                ),
            ],
        );
        assert_batch_matches_legacy(&primitive_extremes, &mut storage);

        let groups_after_first_batch = batch_aggregate(
            batch_filter(
                batch_scan(columns.clone()),
                batch_binary(
                    BinaryOp::GtEq,
                    batch_column_expression(&columns[0]),
                    batch_literal(
                        ScalarValue::Int64(EXECUTION_BATCH_CAPACITY as i64),
                        PhysicalType::Int64,
                    ),
                ),
            ),
            vec![columns[2].clone()],
            vec![
                AggregateOutput::GroupKey(columns[2].clone()),
                batch_aggregate_expression(
                    AggregateFunction::Sum,
                    AggregateInput::Column(columns[0].clone()),
                    "SUM(id)",
                    PhysicalType::Int64,
                    true,
                ),
            ],
        );
        assert_batch_matches_legacy(&groups_after_first_batch, &mut storage);
        let limited = batch_limit(
            batch_aggregate(
                batch_scan(columns.clone()),
                vec![columns[1].clone()],
                grouped_outputs(),
            ),
            3,
        );
        assert_batch_matches_legacy(&limited, &mut storage);
        assert_eq!(
            execute_rows(&limited, std::slice::from_mut(&mut storage))
                .expect("execute Limit above Aggregate")
                .rows
                .len(),
            3
        );
        storage.close().expect("close filtered aggregate Heap");
        remove_batch_test_path(&path, false);

        let (mut all_null, null_path) = batch_storage("aggregate-all-null", false, 0);
        let mut transaction = all_null.begin_transaction().expect("begin all-NULL load");
        for index in 0..=EXECUTION_BATCH_CAPACITY {
            all_null
                .insert_in(
                    &mut transaction,
                    &[
                        ScalarValue::Int64(index as i64),
                        ScalarValue::UInt64(0),
                        ScalarValue::Bool(true),
                        ScalarValue::Text("same".into()),
                        ScalarValue::Null,
                    ],
                )
                .expect("insert all-NULL row");
        }
        transaction.commit().expect("commit all-NULL load");
        let all_null_plan = batch_aggregate(
            batch_scan(columns.clone()),
            Vec::new(),
            vec![
                batch_aggregate_expression(
                    AggregateFunction::Count,
                    AggregateInput::Column(columns[4].clone()),
                    "COUNT(nullable_key)",
                    PhysicalType::UInt64,
                    false,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Sum,
                    AggregateInput::Column(columns[4].clone()),
                    "SUM(nullable_key)",
                    PhysicalType::Int64,
                    true,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Min,
                    AggregateInput::Column(columns[4].clone()),
                    "MIN(nullable_key)",
                    PhysicalType::Int64,
                    true,
                ),
                batch_aggregate_expression(
                    AggregateFunction::Max,
                    AggregateInput::Column(columns[4].clone()),
                    "MAX(nullable_key)",
                    PhysicalType::Int64,
                    true,
                ),
            ],
        );
        assert_batch_matches_legacy(&all_null_plan, &mut all_null);
        assert_eq!(
            execute_rows(&all_null_plan, std::slice::from_mut(&mut all_null))
                .expect("execute all-NULL aggregate")
                .rows[0]
                .values,
            vec![
                ScalarValue::UInt64(0),
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
            ]
        );
        all_null.close().expect("close all-NULL Heap");
        remove_batch_test_path(&null_path, false);
    }

    #[test]
    fn batch_aggregates_are_equivalent_across_heap_lsm_and_legacy() {
        let columns = batch_columns();
        let rows = 2 * EXECUTION_BATCH_CAPACITY + 1;
        let (mut heap, heap_path) = batch_storage("aggregate-engines", false, rows);
        let (mut lsm, lsm_path) = batch_storage("aggregate-engines", true, rows);
        let sum = || {
            batch_aggregate_expression(
                AggregateFunction::Sum,
                AggregateInput::Column(columns[0].clone()),
                "SUM(id)",
                PhysicalType::Int64,
                true,
            )
        };
        let count = || {
            batch_aggregate_expression(
                AggregateFunction::Count,
                AggregateInput::All,
                "COUNT(*)",
                PhysicalType::UInt64,
                false,
            )
        };
        let active = || {
            batch_binary(
                BinaryOp::Eq,
                batch_column_expression(&columns[2]),
                batch_literal(ScalarValue::Bool(true), PhysicalType::Bool),
            )
        };
        let plans = [
            batch_aggregate(batch_scan(columns.clone()), Vec::new(), vec![sum()]),
            batch_aggregate(
                batch_scan(columns.clone()),
                Vec::new(),
                vec![
                    batch_aggregate_expression(
                        AggregateFunction::Min,
                        AggregateInput::Column(columns[0].clone()),
                        "MIN(id)",
                        PhysicalType::Int64,
                        true,
                    ),
                    batch_aggregate_expression(
                        AggregateFunction::Max,
                        AggregateInput::Column(columns[0].clone()),
                        "MAX(id)",
                        PhysicalType::Int64,
                        true,
                    ),
                ],
            ),
            batch_aggregate(
                batch_scan(columns.clone()),
                vec![columns[1].clone()],
                vec![AggregateOutput::GroupKey(columns[1].clone()), count()],
            ),
            batch_aggregate(
                batch_filter(batch_scan(columns.clone()), active()),
                vec![columns[1].clone()],
                vec![AggregateOutput::GroupKey(columns[1].clone()), sum()],
            ),
        ];
        for plan in plans {
            let heap_result = execute_rows(&plan, std::slice::from_mut(&mut heap))
                .expect("execute Heap batch aggregate");
            let lsm_result = execute_rows(&plan, std::slice::from_mut(&mut lsm))
                .expect("execute LSM batch aggregate");
            assert_eq!(heap_result, lsm_result);
            assert_batch_matches_legacy(&plan, &mut heap);
            assert_batch_matches_legacy(&plan, &mut lsm);
        }
        heap.close().expect("close aggregate Heap");
        lsm.close().expect("close aggregate LSM");
        remove_batch_test_path(&heap_path, false);
        remove_batch_test_path(&lsm_path, true);
    }

    #[test]
    fn batch_aggregate_fallback_and_incremental_errors_keep_authoritative_details() {
        let columns = batch_columns();
        let (mut storage, path) = batch_storage("aggregate-errors", false, 0);
        let mut missing = columns[0].clone();
        missing.column_id = ColumnId(99);
        missing.name = "missing".into();
        let malformed = batch_aggregate(
            batch_scan(vec![columns[0].clone()]),
            Vec::new(),
            vec![batch_aggregate_expression(
                AggregateFunction::Sum,
                AggregateInput::Column(missing),
                "SUM(missing)",
                PhysicalType::Int64,
                true,
            )],
        );
        for result in [
            execute_rows(&malformed, std::slice::from_mut(&mut storage)),
            execute_rows_legacy(&malformed, std::slice::from_mut(&mut storage)),
        ] {
            assert!(matches!(
                result,
                Err(ExecutionError::MissingColumn(name)) if name == "missing"
            ));
        }

        let overflow_output = [batch_aggregate_expression(
            AggregateFunction::Sum,
            AggregateInput::Column(columns[0].clone()),
            "precise_total",
            PhysicalType::Int64,
            true,
        )];
        let input_fields = [OutputField::Source(columns[0].clone())];
        let mut accumulator = AggregateAccumulator::new(&input_fields, &[], &overflow_output)
            .expect("build overflow accumulator");
        let error = accumulator
            .consume_rows(&[
                ExecutionRow {
                    row_id: None,
                    values: vec![ScalarValue::Int64(i64::MAX)],
                },
                ExecutionRow {
                    row_id: None,
                    values: vec![ScalarValue::Int64(1)],
                },
            ])
            .expect_err("SUM must overflow");
        assert!(matches!(
            error,
            ExecutionError::AggregateOverflow {
                function: AggregateFunction::Sum,
                output,
            } if output == "precise_total"
        ));

        let short_output = [batch_aggregate_expression(
            AggregateFunction::Min,
            AggregateInput::Column(columns[0].clone()),
            "MIN(id)",
            PhysicalType::Int64,
            true,
        )];
        let mut accumulator = AggregateAccumulator::new(&input_fields, &[], &short_output)
            .expect("build short-row accumulator");
        assert!(matches!(
            accumulator.consume_rows(&[ExecutionRow {
                row_id: None,
                values: Vec::new(),
            }]),
            Err(ExecutionError::MissingColumn(name)) if name == "id"
        ));
        storage.close().expect("close aggregate error Heap");
        remove_batch_test_path(&path, false);
    }

    #[test]
    fn grouped_batch_error_clears_rows_and_retains_capacity() {
        let columns = batch_columns();
        let team = columns[1].clone();
        let id = columns[0].clone();
        let input_fields = [
            OutputField::Source(team.clone()),
            OutputField::Source(id.clone()),
        ];
        let outputs = [
            AggregateOutput::GroupKey(team.clone()),
            batch_aggregate_expression(
                AggregateFunction::Sum,
                AggregateInput::Column(id),
                "SUM(id)",
                PhysicalType::Int64,
                true,
            ),
        ];
        let mut accumulator =
            AggregateAccumulator::new(&input_fields, std::slice::from_ref(&team), &outputs)
                .expect("build grouped overflow accumulator");
        let mut batch = ExecutionBatch::with_capacity();
        let capacity = batch.rows.capacity();
        batch.rows.extend([
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::UInt64(0), ScalarValue::Int64(i64::MAX)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::UInt64(0), ScalarValue::Int64(1)],
            },
            ExecutionRow {
                row_id: None,
                values: vec![ScalarValue::UInt64(0), ScalarValue::Int64(2)],
            },
        ]);

        let error = accumulator
            .consume_batch(&mut batch)
            .expect_err("grouped SUM must overflow");
        assert!(matches!(
            error,
            ExecutionError::AggregateOverflow {
                function: AggregateFunction::Sum,
                output,
            } if output == "SUM(id)"
        ));
        assert!(batch.rows.is_empty());
        assert_eq!(batch.rows.capacity(), capacity);
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
        let mut storage = TableStorage::create_heap(&path, table).expect("create heap");
        let first_row_id = storage
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert");
        let second_row_id = storage
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
        let direct_filter = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan {
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
                        kind: ExprKind::Column(id.clone()),
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
        };
        let direct = execute_rows(&direct_filter, std::slice::from_mut(&mut storage))
            .expect("execute direct streaming filter");
        assert_eq!(
            direct.fields,
            vec![
                OutputField::Source(id.clone()),
                OutputField::Source(name.clone())
            ]
        );
        assert_eq!(direct.rows.len(), 1);
        assert_eq!(direct.rows[0].row_id, Some(second_row_id));
        assert_eq!(
            direct.rows[0].values,
            vec![ScalarValue::Int64(2), ScalarValue::Text("Lin".into())]
        );
        assert_ne!(first_row_id, second_row_id);

        let projected_filter = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::SeqScan {
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
                    kind: ExprKind::IsNull {
                        expression: Box::new(Expr {
                            expr_type: ExprType {
                                data_type: name.data_type.clone(),
                                nullable: false,
                            },
                            kind: ExprKind::Column(name.clone()),
                        }),
                        negated: true,
                    },
                },
            }),
            columns: vec![id.clone()],
        };
        let projected = execute_rows(&projected_filter, std::slice::from_mut(&mut storage))
            .expect("execute retained-column-aware streaming filter");
        assert_eq!(projected.fields, [OutputField::Source(id.clone())]);
        assert_eq!(projected.rows.len(), 2);
        assert_eq!(projected.rows[0].row_id, Some(first_row_id));
        assert_eq!(projected.rows[0].values, [ScalarValue::Int64(1)]);
        assert_eq!(projected.rows[1].row_id, Some(second_row_id));
        assert_eq!(projected.rows[1].values, [ScalarValue::Int64(2)]);

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
    fn generic_filter_preserves_row_dependent_binding_and_evaluation_errors() {
        let table = TableDef::new(
            TableId(1),
            "items",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        );
        let path = std::env::temp_dir().join(format!(
            "netbadb-executor-empty-filter-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut storage = TableStorage::create_heap(&path, table).expect("create empty heap");
        let column = |column_id: u32, name: &str, physical: PhysicalType| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            column_id: ColumnId(column_id),
            relation_name: "items".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable: false,
        };
        let id = column(1, "id", PhysicalType::Int64);
        let missing = column(2, "missing", PhysicalType::Bool);
        let filter = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(0),
                table_id: TableId(1),
                table_name: "items".into(),
                columns: vec![id.clone()],
            }),
            predicate: Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
                kind: ExprKind::Column(missing.clone()),
            },
        };

        let result = execute(&filter, &mut storage).expect("empty filter skips predicate");
        assert!(result.rows.is_empty());
        storage
            .insert(&[ScalarValue::Int64(1)])
            .expect("insert malformed-plan input");
        assert!(matches!(
            execute(&filter, &mut storage),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));

        let scan = || PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(1),
            table_name: "items".into(),
            columns: vec![id.clone()],
        };
        let type_mismatch = PhysicalPlan::Filter {
            input: Box::new(scan()),
            predicate: Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
                kind: ExprKind::Binary {
                    operator: BinaryOp::Eq,
                    left: Box::new(Expr {
                        expr_type: ExprType {
                            data_type: id.data_type.clone(),
                            nullable: false,
                        },
                        kind: ExprKind::Column(id.clone()),
                    }),
                    right: Box::new(Expr {
                        expr_type: ExprType {
                            data_type: SemanticType::physical(PhysicalType::Text),
                            nullable: false,
                        },
                        kind: ExprKind::Literal(ScalarValue::Text("wrong".into())),
                    }),
                },
            },
        };
        assert!(matches!(
            execute(&type_mismatch, &mut storage),
            Err(ExecutionError::TypeMismatch)
        ));

        let non_boolean = PhysicalPlan::Filter {
            input: Box::new(scan()),
            predicate: Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Int64),
                    nullable: false,
                },
                kind: ExprKind::Literal(ScalarValue::Int64(1)),
            },
        };
        assert!(matches!(
            execute(&non_boolean, &mut storage),
            Err(ExecutionError::ExpectedBoolean)
        ));
        let malformed_project = PhysicalPlan::Project {
            input: Box::new(filter),
            columns: vec![missing],
        };
        assert!(matches!(
            execute(&malformed_project, &mut storage),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));
        storage.close().expect("close empty heap");
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
        let mut storage = TableStorage::create_heap(&path, table).expect("create indexed heap");
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
        storage
            .create_index(ColumnId(2))
            .expect("create team index");
        let access_path = storage.access_paths()[0].id;

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
            access_path,
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
        let point_filter = PhysicalPlan::Filter {
            input: Box::new(scan.clone()),
            predicate: bound(BinaryOp::Eq, 10),
        };
        assert!(
            filtered_count_eligibility(&point_filter, &[], std::slice::from_ref(&count_id))
                .is_none()
        );
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
            &[AccessPath {
                table_id: TableId(101),
                column_id: ColumnId(2),
                id: access_path,
                capabilities: AccessPathCapabilities {
                    point_lookup: true,
                    range_lookup: true,
                    ordered: true,
                },
                statistics: Some(IndexStatistics {
                    distinct_non_null_keys: 10_000,
                    null_count: 0,
                    tree_height: 2,
                }),
                cost_hints: None,
            }],
        );
        let PhysicalPlan::Filter { input, .. } = &range_plan else {
            panic!("bounded range must retain its residual filter");
        };
        assert!(matches!(
            input.as_ref(),
            PhysicalPlan::RangeIndexScan { .. }
        ));
        assert!(
            filtered_count_eligibility(&range_plan, &[], std::slice::from_ref(&count_id)).is_none()
        );
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
        assert_eq!(
            execute(&scan, &mut storage)
                .expect("skip expired candidate")
                .rows,
            vec![vec![
                ScalarValue::Int64(2),
                ScalarValue::UInt64(10),
                ScalarValue::Bool(false),
            ]]
        );

        storage.close().expect("close indexed heap");
        let _ = std::fs::remove_file(netbadb_storage::txn_status_path(&path));
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
                assert_eq!(
                    evaluate_binary_refs(operator, left, right).expect("reference comparison"),
                    evaluate_binary_scalar_refs(
                        operator,
                        ScalarRef::from(left),
                        ScalarRef::from(right),
                    )
                    .expect("scalar-view comparison")
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
                    assert_eq!(
                        evaluate_binary_refs(operator, left, right)
                            .expect("reference truth operation"),
                        evaluate_binary_scalar_refs(
                            operator,
                            ScalarRef::from(left),
                            ScalarRef::from(right),
                        )
                        .expect("scalar-view truth operation")
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
        let scalar_refs = contiguous.iter().map(ScalarRef::from).collect::<Vec<_>>();

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
                    BinaryOp::GtEq,
                    column_expr(&left_columns[3]),
                    literal(ScalarValue::Text("shared".into()), PhysicalType::Text),
                ),
                binary(
                    BinaryOp::LtEq,
                    column_expr(&left_columns[3]),
                    literal(ScalarValue::Text("shared".into()), PhysicalType::Text),
                ),
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
            let dynamic_borrowed = evaluate_dynamic_borrowed_values(&expression, joined, &fields)
                .expect("borrowed dynamic evaluation");
            assert_eq!(dynamic, dynamic_borrowed.as_scalar_ref().to_owned());
            let evaluated = evaluate_bound_values(&bound, joined).expect("bound evaluation");
            assert_eq!(dynamic, evaluated.as_scalar_ref().to_owned());
            assert_eq!(
                evaluate_truth(&expression, &contiguous, &fields).expect("contiguous truth"),
                evaluate_truth_values(&expression, joined, &fields).expect("joined truth")
            );
            assert_eq!(
                evaluate_truth_values(&expression, joined, &fields).expect("dynamic truth"),
                evaluate_bound_truth(&bound, joined).expect("bound truth")
            );
            assert_eq!(
                evaluate_truth_values(&expression, joined, &fields).expect("dynamic truth"),
                evaluate_dynamic_borrowed_truth_values(&expression, joined, &fields)
                    .expect("borrowed dynamic truth")
            );
            assert_eq!(
                evaluate_truth_values(&expression, joined, &fields).expect("dynamic truth"),
                evaluate_dynamic_scalar_ref_truth(&expression, &scalar_refs, &fields)
                    .expect("scalar-view dynamic truth")
            );
            assert_eq!(
                evaluate_dynamic_scalar_ref_truth(&expression, &scalar_refs, &fields)
                    .expect("scalar-view dynamic truth"),
                evaluate_bound_scalar_ref_truth(&bound, &scalar_refs)
                    .expect("bound scalar-view truth")
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
        assert!(matches!(
            evaluate_dynamic_borrowed_values(
                &column_expr(&missing),
                EvaluationValues::Joined {
                    left: &left,
                    right: &right,
                },
                &fields
            ),
            Err(ExecutionError::MissingColumn(name)) if name == "missing"
        ));
        let missing_expression = column_expr(&missing);
        assert!(matches!(
            evaluate_dynamic_scalar_ref_truth(&missing_expression, &scalar_refs, &fields),
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
        assert!(matches!(
            evaluate_dynamic_borrowed_values(
                &right_signed,
                EvaluationValues::Contiguous(&left[..1]),
                &fields
            ),
            Err(ExecutionError::MissingColumn(name)) if name == "signed"
        ));
        let short_scalar_refs = left[..1].iter().map(ScalarRef::from).collect::<Vec<_>>();
        assert!(matches!(
            evaluate_dynamic_scalar_ref_truth(&right_signed, &short_scalar_refs, &fields),
            Err(ExecutionError::MissingColumn(name)) if name == "signed"
        ));
        assert!(matches!(
            evaluate_bound_scalar_ref_truth(&bound_right_signed, &short_scalar_refs),
            Err(ExecutionError::MissingColumn(name)) if name == "signed"
        ));
    }

    #[test]
    fn binding_reuses_source_order_positions_for_repeated_and_self_bound_columns() {
        fn column(binding_id: u32, column_id: u32, name: &str) -> ColumnRef {
            ColumnRef {
                binding_id: RelationBindingId(binding_id),
                table_id: TableId(7),
                column_id: ColumnId(column_id),
                relation_name: format!("side_{binding_id}"),
                name: name.into(),
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable: false,
            }
        }

        fn column_expr(column: &ColumnRef) -> Expr {
            Expr {
                kind: ExprKind::Column(column.clone()),
                expr_type: ExprType {
                    data_type: column.data_type.clone(),
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
                    nullable: false,
                },
            }
        }

        fn collect_columns<'a>(expression: &'a BoundExpr<'_>, output: &mut Vec<(usize, &'a str)>) {
            match &expression.kind {
                BoundExprKind::Column { position, name } => output.push((*position, name)),
                BoundExprKind::Literal(_) => {}
                BoundExprKind::Binary { left, right, .. } => {
                    collect_columns(left, output);
                    collect_columns(right, output);
                }
                BoundExprKind::Unary { expression, .. }
                | BoundExprKind::IsNull { expression, .. } => collect_columns(expression, output),
            }
        }

        let left_id = column(1, 1, "left_id");
        let right_id = column(2, 1, "right_id");
        let team_id = column(1, 2, "team_id");
        let fields = vec![
            OutputField::Source(right_id.clone()),
            OutputField::Source(team_id.clone()),
            OutputField::Source(left_id.clone()),
        ];
        let expression = binary(
            BinaryOp::And,
            binary(BinaryOp::Eq, column_expr(&left_id), column_expr(&left_id)),
            binary(
                BinaryOp::And,
                binary(BinaryOp::Eq, column_expr(&team_id), column_expr(&left_id)),
                binary(BinaryOp::Eq, column_expr(&right_id), column_expr(&left_id)),
            ),
        );
        let bound = bind_expression(&expression, &fields).expect("bind source-order expression");
        let mut columns = Vec::new();
        collect_columns(&bound, &mut columns);
        assert_eq!(
            columns,
            [
                (2, "left_id"),
                (2, "left_id"),
                (1, "team_id"),
                (2, "left_id"),
                (0, "right_id"),
                (2, "left_id"),
            ]
        );

        let missing = column(3, 1, "missing_id");
        assert!(matches!(
            bind_expression(&column_expr(&missing), &fields),
            Err(ExecutionError::MissingColumn(name)) if name == "missing_id"
        ));
    }

    #[test]
    fn generic_filter_binding_resolves_wide_positions_once_and_matches_dynamic_rows() {
        fn column(column_id: u32) -> ColumnRef {
            ColumnRef {
                binding_id: RelationBindingId(0),
                table_id: TableId(7),
                column_id: ColumnId(column_id),
                relation_name: "wide".into(),
                name: format!("c{column_id}"),
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable: true,
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

        fn literal(value: i64) -> Expr {
            Expr {
                kind: ExprKind::Literal(ScalarValue::Int64(value)),
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Int64),
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

        fn collect_positions(expression: &BoundExpr<'_>, positions: &mut Vec<usize>) {
            match &expression.kind {
                BoundExprKind::Column { position, .. } => positions.push(*position),
                BoundExprKind::Literal(_) => {}
                BoundExprKind::Binary { left, right, .. } => {
                    collect_positions(left, positions);
                    collect_positions(right, positions);
                }
                BoundExprKind::Unary { expression, .. }
                | BoundExprKind::IsNull { expression, .. } => {
                    collect_positions(expression, positions);
                }
            }
        }

        let columns = (1..=7).map(column).collect::<Vec<_>>();
        let fields = columns
            .iter()
            .cloned()
            .map(OutputField::Source)
            .collect::<Vec<_>>();
        let expression = binary(
            BinaryOp::And,
            binary(BinaryOp::Eq, column_expr(&columns[0]), literal(1)),
            binary(
                BinaryOp::And,
                binary(
                    BinaryOp::Eq,
                    column_expr(&columns[3]),
                    column_expr(&columns[3]),
                ),
                binary(
                    BinaryOp::And,
                    binary(BinaryOp::Eq, column_expr(&columns[6]), literal(7)),
                    binary(
                        BinaryOp::Eq,
                        column_expr(&columns[6]),
                        column_expr(&columns[6]),
                    ),
                ),
            ),
        );
        let bound = bind_expression(&expression, &fields).expect("bind wide generic filter");
        let mut positions = Vec::new();
        collect_positions(&bound, &mut positions);
        assert_eq!(positions, [0, 3, 3, 6, 6, 6]);

        for values in [
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Int64(3),
                ScalarValue::Int64(4),
                ScalarValue::Int64(5),
                ScalarValue::Int64(6),
                ScalarValue::Int64(7),
            ],
            vec![
                ScalarValue::Int64(0),
                ScalarValue::Int64(2),
                ScalarValue::Int64(3),
                ScalarValue::Int64(4),
                ScalarValue::Int64(5),
                ScalarValue::Int64(6),
                ScalarValue::Int64(7),
            ],
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Int64(3),
                ScalarValue::Int64(4),
                ScalarValue::Int64(5),
                ScalarValue::Int64(6),
                ScalarValue::Null,
            ],
        ] {
            let dynamic = evaluate_dynamic_borrowed_truth_values(
                &expression,
                EvaluationValues::Contiguous(&values),
                &fields,
            )
            .expect("evaluate dynamic wide row");
            assert_eq!(
                evaluate_bound_truth(&bound, EvaluationValues::Contiguous(&values))
                    .expect("evaluate bound wide row"),
                dynamic
            );
            let scalar_refs = values.iter().map(ScalarRef::from).collect::<Vec<_>>();
            assert_eq!(
                evaluate_bound_scalar_ref_truth(&bound, &scalar_refs)
                    .expect("evaluate bound wide scalar refs"),
                evaluate_dynamic_scalar_ref_truth(&expression, &scalar_refs, &fields)
                    .expect("evaluate dynamic wide scalar refs")
            );
        }
    }

    #[test]
    fn dynamic_borrowed_leaf_values_preserve_identity_and_computed_values_are_owned() {
        let column = |binding_id: u32, name: &str, physical: PhysicalType| ColumnRef {
            binding_id: RelationBindingId(binding_id),
            table_id: TableId(7),
            column_id: ColumnId(1),
            relation_name: format!("side_{binding_id}"),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable: false,
        };
        let left = column(1, "left_number", PhysicalType::Int64);
        let right = column(2, "right_text", PhysicalType::Text);
        let fields = vec![
            OutputField::Source(left.clone()),
            OutputField::Source(right.clone()),
        ];
        let values = vec![ScalarValue::Int64(7), ScalarValue::Text("row-text".into())];
        let scalar_refs = values.iter().map(ScalarRef::from).collect::<Vec<_>>();
        let expression = |column: ColumnRef| Expr {
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
            kind: ExprKind::Column(column),
        };

        for (column, position) in [(left.clone(), 0), (right.clone(), 1)] {
            let expression = expression(column);
            match evaluate_dynamic_borrowed_values(
                &expression,
                EvaluationValues::Contiguous(&values),
                &fields,
            )
            .expect("evaluate borrowed column")
            {
                EvaluatedScalar::Borrowed(value) => {
                    assert_eq!(value, ScalarRef::from(&values[position]));
                    if position == 1 {
                        assert_eq!(
                            scalar_ref_text_pointer(value),
                            text_pointer(&values[position])
                        );
                    }
                }
                EvaluatedScalar::Owned(_) => panic!("dynamic column must remain borrowed"),
            }
            match evaluate_dynamic_with(&expression, &fields, &|index| {
                scalar_refs.get(index).copied()
            })
            .expect("evaluate scalar-view column")
            {
                EvaluatedScalar::Borrowed(value) => {
                    assert_eq!(value, scalar_refs[position]);
                    if position == 1 {
                        assert_eq!(
                            scalar_ref_text_pointer(value),
                            scalar_ref_text_pointer(scalar_refs[position])
                        );
                    }
                }
                EvaluatedScalar::Owned(_) => panic!("scalar-view column must remain borrowed"),
            }
        }

        let literal = Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Text),
                nullable: false,
            },
            kind: ExprKind::Literal(ScalarValue::Text("literal-text".into())),
        };
        let literal_pointer = match &literal.kind {
            ExprKind::Literal(value) => text_pointer(value),
            _ => panic!("expected literal"),
        };
        match evaluate_dynamic_borrowed_values(
            &literal,
            EvaluationValues::Contiguous(&values),
            &fields,
        )
        .expect("evaluate borrowed literal")
        {
            EvaluatedScalar::Borrowed(value) => {
                assert_eq!(scalar_ref_text_pointer(value), literal_pointer);
            }
            EvaluatedScalar::Owned(_) => panic!("dynamic literal must remain borrowed"),
        }
        match evaluate_dynamic_with(&literal, &fields, &|index| scalar_refs.get(index).copied())
            .expect("evaluate scalar-view literal")
        {
            EvaluatedScalar::Borrowed(value) => {
                assert_eq!(scalar_ref_text_pointer(value), literal_pointer);
            }
            EvaluatedScalar::Owned(_) => panic!("scalar-view literal must remain borrowed"),
        }

        let comparison = Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
            kind: ExprKind::Binary {
                operator: BinaryOp::Eq,
                left: Box::new(expression(right)),
                right: Box::new(literal),
            },
        };
        assert!(matches!(
            evaluate_dynamic_borrowed_values(
                &comparison,
                EvaluationValues::Contiguous(&values),
                &fields
            ),
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(false)))
        ));

        let null_literal = || Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable: true,
            },
            kind: ExprKind::Literal(ScalarValue::Null),
        };
        let null_comparison = Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: true,
            },
            kind: ExprKind::Binary {
                operator: BinaryOp::Eq,
                left: Box::new(expression(left)),
                right: Box::new(null_literal()),
            },
        };
        assert!(matches!(
            evaluate_dynamic_borrowed_values(
                &null_comparison,
                EvaluationValues::Contiguous(&values),
                &fields
            ),
            Ok(EvaluatedScalar::Owned(ScalarValue::Null))
        ));

        let is_null = Expr {
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
            kind: ExprKind::IsNull {
                expression: Box::new(null_literal()),
                negated: false,
            },
        };
        assert!(matches!(
            evaluate_dynamic_borrowed_values(
                &is_null,
                EvaluationValues::Contiguous(&values),
                &fields
            ),
            Ok(EvaluatedScalar::Owned(ScalarValue::Bool(true)))
        ));

        for value in [ScalarValue::Bool(true), ScalarValue::Null] {
            let expected = match &value {
                ScalarValue::Bool(true) => ScalarValue::Bool(false),
                ScalarValue::Null => ScalarValue::Null,
                _ => unreachable!("test input is Bool or NULL"),
            };
            let not = Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: true,
                },
                kind: ExprKind::Unary {
                    operator: UnaryOp::Not,
                    expression: Box::new(Expr {
                        expr_type: ExprType {
                            data_type: SemanticType::physical(PhysicalType::Bool),
                            nullable: matches!(value, ScalarValue::Null),
                        },
                        kind: ExprKind::Literal(value),
                    }),
                },
            };
            assert!(matches!(
                evaluate_dynamic_borrowed_values(
                    &not,
                    EvaluationValues::Contiguous(&values),
                    &fields
                ),
                Ok(EvaluatedScalar::Owned(value)) if value == expected
            ));
        }

        for operator in [BinaryOp::And, BinaryOp::Or] {
            let left_value = ScalarValue::Bool(matches!(operator, BinaryOp::And));
            let boolean_with_null = Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: true,
                },
                kind: ExprKind::Binary {
                    operator,
                    left: Box::new(Expr {
                        expr_type: ExprType {
                            data_type: SemanticType::physical(PhysicalType::Bool),
                            nullable: false,
                        },
                        kind: ExprKind::Literal(left_value),
                    }),
                    right: Box::new(Expr {
                        expr_type: ExprType {
                            data_type: SemanticType::physical(PhysicalType::Bool),
                            nullable: true,
                        },
                        kind: ExprKind::Literal(ScalarValue::Null),
                    }),
                },
            };
            assert!(matches!(
                evaluate_dynamic_borrowed_values(
                    &boolean_with_null,
                    EvaluationValues::Contiguous(&values),
                    &fields
                ),
                Ok(EvaluatedScalar::Owned(ScalarValue::Null))
            ));
        }

        let missing = column(3, "missing", PhysicalType::Bool);
        for operator in [BinaryOp::And, BinaryOp::Or] {
            let left_value = matches!(operator, BinaryOp::Or);
            let expression = Expr {
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
                kind: ExprKind::Binary {
                    operator,
                    left: Box::new(Expr {
                        expr_type: ExprType {
                            data_type: SemanticType::physical(PhysicalType::Bool),
                            nullable: false,
                        },
                        kind: ExprKind::Literal(ScalarValue::Bool(left_value)),
                    }),
                    right: Box::new(expression(missing.clone())),
                },
            };
            assert!(matches!(
                evaluate_dynamic_borrowed_values(
                    &expression,
                    EvaluationValues::Contiguous(&values),
                    &fields
                ),
                Err(ExecutionError::MissingColumn(name)) if name == "missing"
            ));
        }
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
        let scalar_refs = values.iter().map(ScalarRef::from).collect::<Vec<_>>();

        for (position, name) in [(0, "number"), (1, "text")] {
            let expression = BoundExpr {
                kind: BoundExprKind::Column { position, name },
            };
            let evaluated =
                evaluate_bound_values(&expression, evaluation_values).expect("evaluate column");
            match evaluated {
                EvaluatedScalar::Borrowed(value) => {
                    assert_eq!(value, ScalarRef::from(&values[position]));
                    if position == 1 {
                        assert_eq!(
                            scalar_ref_text_pointer(value),
                            text_pointer(&values[position])
                        );
                    }
                }
                EvaluatedScalar::Owned(_) => panic!("bound column must remain borrowed"),
            }
            let scalar_view =
                evaluate_bound_with(&expression, &|index| scalar_refs.get(index).copied())
                    .expect("evaluate bound scalar-view column");
            match scalar_view {
                EvaluatedScalar::Borrowed(value) => {
                    assert_eq!(value, scalar_refs[position]);
                    if position == 1 {
                        assert_eq!(
                            scalar_ref_text_pointer(value),
                            scalar_ref_text_pointer(scalar_refs[position])
                        );
                    }
                }
                EvaluatedScalar::Owned(_) => {
                    panic!("bound scalar-view column must remain borrowed")
                }
            }
        }

        let literal_value = ScalarValue::Text("constant-value".into());
        let literal = BoundExpr {
            kind: BoundExprKind::Literal(&literal_value),
        };
        let evaluated =
            evaluate_bound_values(&literal, evaluation_values).expect("evaluate literal");
        match evaluated {
            EvaluatedScalar::Borrowed(value) => {
                assert_eq!(value, ScalarRef::from(&literal_value));
                assert_eq!(scalar_ref_text_pointer(value), text_pointer(&literal_value));
            }
            EvaluatedScalar::Owned(_) => panic!("bound literal must remain borrowed"),
        }
        let scalar_view = evaluate_bound_with(&literal, &|index| scalar_refs.get(index).copied())
            .expect("evaluate bound scalar-view literal");
        match scalar_view {
            EvaluatedScalar::Borrowed(value) => {
                assert_eq!(scalar_ref_text_pointer(value), text_pointer(&literal_value));
            }
            EvaluatedScalar::Owned(_) => panic!("bound scalar-view literal must remain borrowed"),
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

        for (operator, left) in [(BinaryOp::And, false), (BinaryOp::Or, true)] {
            let left = ScalarValue::Bool(left);
            let expression = BoundExpr {
                kind: BoundExprKind::Binary {
                    operator,
                    left: Box::new(BoundExpr {
                        kind: BoundExprKind::Literal(&left),
                    }),
                    right: Box::new(BoundExpr {
                        kind: BoundExprKind::Column {
                            position: values.len(),
                            name: "missing_rhs",
                        },
                    }),
                },
            };
            assert!(matches!(
                evaluate_bound_scalar_ref_truth(&expression, &scalar_refs),
                Err(ExecutionError::MissingColumn(name)) if name == "missing_rhs"
            ));
        }
    }

    fn streaming_join_table(table_id: TableId, name: &str) -> TableDef {
        TableDef::new(
            table_id,
            name,
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "join_key",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
                ColumnDef::new(
                    ColumnId(3),
                    "payload",
                    TypeSpec::Physical(PhysicalType::Text),
                ),
            ],
        )
    }

    fn streaming_join_columns(
        binding_id: u32,
        table_id: TableId,
        relation_name: &str,
    ) -> Vec<ColumnRef> {
        [
            (1, "id", PhysicalType::Int64, false),
            (2, "join_key", PhysicalType::Int64, true),
            (3, "payload", PhysicalType::Text, false),
        ]
        .into_iter()
        .map(|(column_id, name, physical, nullable)| ColumnRef {
            binding_id: RelationBindingId(binding_id),
            table_id,
            column_id: ColumnId(column_id),
            relation_name: relation_name.into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable,
        })
        .collect()
    }

    fn streaming_join_expression(column: &ColumnRef) -> Expr {
        Expr {
            kind: ExprKind::Column(column.clone()),
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
        }
    }

    fn streaming_join_binary(operator: BinaryOp, left: Expr, right: Expr) -> Expr {
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

    fn streaming_join_plan(
        left_columns: &[ColumnRef],
        right_columns: &[ColumnRef],
        predicate: Expr,
        columns: Vec<ColumnRef>,
    ) -> PhysicalPlan {
        PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                binding_id: left_columns[0].binding_id,
                table_id: left_columns[0].table_id,
                table_name: "left_rows".into(),
                columns: left_columns.to_vec(),
            }),
            right: Box::new(PhysicalPlan::SeqScan {
                binding_id: right_columns[0].binding_id,
                table_id: right_columns[0].table_id,
                table_name: "right_rows".into(),
                columns: right_columns.to_vec(),
            }),
            kind: JoinKind::Inner,
            left_key: left_columns[1].clone(),
            right_key: right_columns[1].clone(),
            predicate,
            columns,
        }
    }

    fn streaming_join_storage(
        case: &str,
        side: &str,
        table_id: TableId,
        rows: usize,
        lsm: bool,
        mut key: impl FnMut(usize) -> ScalarValue,
    ) -> (TableStorage, std::path::PathBuf) {
        let path = batch_test_path(&format!("hash-{case}-{side}"), lsm);
        remove_batch_test_path(&path, lsm);
        let table = streaming_join_table(table_id, &format!("{side}_rows"));
        let mut storage = if lsm {
            TableStorage::create_lsm(&path, table, ColumnId(1)).expect("create streaming join LSM")
        } else {
            TableStorage::create_heap(&path, table).expect("create streaming join Heap")
        };
        if rows != 0 {
            let mut transaction = storage
                .begin_transaction()
                .expect("begin streaming join load");
            for index in 0..rows {
                storage
                    .insert_in(
                        &mut transaction,
                        &[
                            ScalarValue::Int64(
                                i64::try_from(index).expect("streaming join ID fits i64"),
                            ),
                            key(index),
                            ScalarValue::Text(format!("{side}-{index}")),
                        ],
                    )
                    .expect("insert streaming join row");
            }
            transaction.commit().expect("commit streaming join load");
        }
        (storage, path)
    }

    fn execute_streaming_hash_join_with_stats(
        plan: &PhysicalPlan,
        storages: &mut [TableStorage],
        stats: &mut StreamingHashJoinStats,
    ) -> Result<Option<super::ExecutionRows>, ExecutionError> {
        let views = storages
            .iter()
            .map(TableStorage::read_view)
            .collect::<Result<Vec<_>, _>>()?;
        let bindings = compatibility_bindings(storages)?;
        let mut execution_storages = storages
            .iter_mut()
            .zip(&bindings)
            .map(|(storage, binding)| ExecutionStorage {
                storage_id: binding.storage_id,
                storage,
            })
            .collect::<Vec<_>>();
        let execution_views = views
            .iter()
            .zip(&bindings)
            .map(|(view, binding)| ExecutionReadView {
                storage_id: binding.storage_id,
                view,
            })
            .collect::<Vec<_>>();
        let PhysicalPlan::HashJoin {
            left,
            right,
            kind,
            left_key,
            right_key,
            predicate,
            columns,
        } = plan
        else {
            panic!("expected HashJoin")
        };
        try_execute_streaming_hash_join_probe(
            left,
            right,
            *kind,
            left_key,
            right_key,
            predicate,
            columns,
            &bindings,
            &mut execution_storages,
            &execution_views,
            Some(stats),
        )
    }

    #[test]
    fn streaming_hash_join_probe_batches_bound_left_input_and_materialize_right() {
        for probe_rows in [
            0,
            1,
            EXECUTION_BATCH_CAPACITY - 1,
            EXECUTION_BATCH_CAPACITY,
            EXECUTION_BATCH_CAPACITY + 1,
            2 * EXECUTION_BATCH_CAPACITY,
            2 * EXECUTION_BATCH_CAPACITY + 1,
        ] {
            let case = format!("boundary-{probe_rows}");
            let left_table_id = TableId(700);
            let right_table_id = TableId(701);
            let (left_storage, left_path) =
                streaming_join_storage(&case, "left", left_table_id, probe_rows, false, |index| {
                    ScalarValue::Int64(i64::try_from(index).expect("left key fits i64"))
                });
            let (right_storage, right_path) =
                streaming_join_storage(&case, "right", right_table_id, 17, false, |index| {
                    ScalarValue::Int64(i64::try_from(index + 10_000).expect("right key fits i64"))
                });
            let left_columns = streaming_join_columns(10, left_table_id, "left_rows");
            let right_columns = streaming_join_columns(20, right_table_id, "right_rows");
            let predicate = streaming_join_binary(
                BinaryOp::Eq,
                streaming_join_expression(&left_columns[1]),
                streaming_join_expression(&right_columns[1]),
            );
            let plan = streaming_join_plan(
                &left_columns,
                &right_columns,
                predicate,
                vec![left_columns[0].clone()],
            );
            let mut storages = [left_storage, right_storage];
            let mut stats = StreamingHashJoinStats::default();
            let streaming =
                execute_streaming_hash_join_with_stats(&plan, &mut storages, &mut stats)
                    .expect("execute streaming HashJoin")
                    .expect("eligible streaming HashJoin");
            let materialized =
                execute_rows_legacy(&plan, &mut storages).expect("execute materialized HashJoin");
            assert_eq!(streaming, materialized);
            assert_eq!(stats.probe_rows_seen, probe_rows);
            assert_eq!(
                stats.probe_batches_seen,
                probe_rows.div_ceil(EXECUTION_BATCH_CAPACITY)
            );
            assert_eq!(
                stats.max_probe_batch_rows,
                probe_rows.min(EXECUTION_BATCH_CAPACITY)
            );
            assert_eq!(stats.build_rows_materialized, 17);
            assert_eq!(stats.candidate_pairs_checked, 0);
            assert_eq!(stats.output_rows, 0);
            for storage in storages {
                storage.close().expect("close streaming join storage");
            }
            remove_batch_test_path(&left_path, false);
            remove_batch_test_path(&right_path, false);
        }
    }

    #[test]
    fn streaming_hash_join_preserves_cross_batch_left_major_right_minor_projection() {
        let probe_rows = 2 * EXECUTION_BATCH_CAPACITY + 1;
        let left_table_id = TableId(710);
        let right_table_id = TableId(711);
        let (left_storage, left_path) =
            streaming_join_storage("ordering", "left", left_table_id, probe_rows, false, |_| {
                ScalarValue::Int64(7)
            });
        let (right_storage, right_path) =
            streaming_join_storage("ordering", "right", right_table_id, 3, false, |_| {
                ScalarValue::Int64(7)
            });
        let left_columns = streaming_join_columns(10, left_table_id, "left_rows");
        let right_columns = streaming_join_columns(20, right_table_id, "right_rows");
        let predicate = streaming_join_binary(
            BinaryOp::Eq,
            streaming_join_expression(&left_columns[1]),
            streaming_join_expression(&right_columns[1]),
        );
        let plan = streaming_join_plan(
            &left_columns,
            &right_columns,
            predicate,
            vec![
                right_columns[2].clone(),
                left_columns[0].clone(),
                right_columns[2].clone(),
            ],
        );
        let mut storages = [left_storage, right_storage];
        let mut stats = StreamingHashJoinStats::default();
        let streaming = execute_streaming_hash_join_with_stats(&plan, &mut storages, &mut stats)
            .expect("execute ordered streaming HashJoin")
            .expect("eligible ordered streaming HashJoin");
        let materialized = execute_rows_legacy(&plan, &mut storages)
            .expect("execute ordered materialized HashJoin");
        assert_eq!(streaming, materialized);
        assert_eq!(stats.probe_rows_seen, probe_rows);
        assert_eq!(stats.probe_batches_seen, 3);
        assert_eq!(stats.max_probe_batch_rows, EXECUTION_BATCH_CAPACITY);
        assert_eq!(stats.build_rows_materialized, 3);
        assert_eq!(stats.candidate_pairs_checked, probe_rows * 3);
        assert_eq!(stats.output_rows, probe_rows * 3);
        let expected = (0..probe_rows)
            .flat_map(|left_id| {
                (0..3).map(move |right_id| super::ExecutionRow {
                    row_id: None,
                    values: vec![
                        ScalarValue::Text(format!("right-{right_id}")),
                        ScalarValue::Int64(i64::try_from(left_id).expect("left ID fits i64")),
                        ScalarValue::Text(format!("right-{right_id}")),
                    ],
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(streaming.rows, expected);
        for storage in storages {
            storage.close().expect("close ordered join storage");
        }
        remove_batch_test_path(&left_path, false);
        remove_batch_test_path(&right_path, false);
    }

    #[test]
    fn streaming_hash_join_matches_materialized_on_heap_and_lsm() {
        for lsm in [false, true] {
            let backend = if lsm { "lsm" } else { "heap" };
            let left_table_id = TableId(720);
            let right_table_id = TableId(721);
            let (left_storage, left_path) = streaming_join_storage(
                backend,
                "left",
                left_table_id,
                EXECUTION_BATCH_CAPACITY + 1,
                lsm,
                |index| ScalarValue::Int64(i64::try_from(index % 17).expect("left key fits")),
            );
            let (right_storage, right_path) =
                streaming_join_storage(backend, "right", right_table_id, 17, lsm, |index| {
                    ScalarValue::Int64(i64::try_from(index).expect("right key fits"))
                });
            let left_columns = streaming_join_columns(10, left_table_id, "left_rows");
            let right_columns = streaming_join_columns(20, right_table_id, "right_rows");
            let predicate = streaming_join_binary(
                BinaryOp::Eq,
                streaming_join_expression(&left_columns[1]),
                streaming_join_expression(&right_columns[1]),
            );
            let plan = streaming_join_plan(
                &left_columns,
                &right_columns,
                predicate,
                vec![left_columns[0].clone(), right_columns[2].clone()],
            );
            let mut storages = [left_storage, right_storage];
            let mut stats = StreamingHashJoinStats::default();
            let streaming =
                execute_streaming_hash_join_with_stats(&plan, &mut storages, &mut stats)
                    .expect("execute backend streaming HashJoin")
                    .expect("eligible backend streaming HashJoin");
            let materialized = execute_rows_legacy(&plan, &mut storages)
                .expect("execute backend materialized HashJoin");
            assert_eq!(streaming, materialized);
            assert_eq!(stats.probe_rows_seen, EXECUTION_BATCH_CAPACITY + 1);
            assert_eq!(stats.probe_batches_seen, 2);
            assert_eq!(stats.max_probe_batch_rows, EXECUTION_BATCH_CAPACITY);
            assert_eq!(stats.build_rows_materialized, 17);
            assert_eq!(stats.candidate_pairs_checked, EXECUTION_BATCH_CAPACITY + 1);
            assert_eq!(stats.output_rows, EXECUTION_BATCH_CAPACITY + 1);
            for storage in storages {
                storage.close().expect("close backend join storage");
            }
            remove_batch_test_path(&left_path, lsm);
            remove_batch_test_path(&right_path, lsm);
        }
    }

    #[test]
    fn streaming_hash_join_self_join_reuses_one_read_view_and_matches_materialized() {
        let table_id = TableId(725);
        let (storage, path) =
            streaming_join_storage("self", "employees", table_id, 3, false, |index| {
                if index == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(1)
                }
            });
        let left_columns = streaming_join_columns(10, table_id, "employees");
        let right_columns = streaming_join_columns(20, table_id, "employees");
        let predicate = streaming_join_binary(
            BinaryOp::Eq,
            streaming_join_expression(&left_columns[1]),
            streaming_join_expression(&right_columns[1]),
        );
        let plan = streaming_join_plan(
            &left_columns,
            &right_columns,
            predicate,
            vec![left_columns[0].clone(), right_columns[0].clone()],
        );
        let mut storages = [storage];
        let mut stats = StreamingHashJoinStats::default();
        let streaming = execute_streaming_hash_join_with_stats(&plan, &mut storages, &mut stats)
            .expect("execute streaming self HashJoin")
            .expect("eligible streaming self HashJoin");
        let materialized =
            execute_rows_legacy(&plan, &mut storages).expect("execute materialized self HashJoin");
        assert_eq!(streaming, materialized);
        assert_eq!(stats.probe_rows_seen, 3);
        assert_eq!(stats.build_rows_materialized, 3);
        assert_eq!(stats.candidate_pairs_checked, 4);
        assert_eq!(stats.output_rows, 4);
        assert_eq!(
            streaming
                .rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Int64(1)],
                vec![ScalarValue::Int64(1), ScalarValue::Int64(2)],
                vec![ScalarValue::Int64(2), ScalarValue::Int64(1)],
                vec![ScalarValue::Int64(2), ScalarValue::Int64(2)],
            ]
        );
        let [storage] = storages;
        storage.close().expect("close self join storage");
        remove_batch_test_path(&path, false);
    }

    #[test]
    fn streaming_hash_join_setup_rejects_non_scan_probe_and_preserves_fallback_errors() {
        let left_table_id = TableId(730);
        let right_table_id = TableId(731);
        let (left_storage, left_path) =
            streaming_join_storage("fallback", "left", left_table_id, 3, false, |index| {
                ScalarValue::Int64(i64::try_from(index).expect("left key fits"))
            });
        let (right_storage, right_path) =
            streaming_join_storage("fallback", "right", right_table_id, 3, false, |index| {
                ScalarValue::Int64(i64::try_from(index).expect("right key fits"))
            });
        let left_columns = streaming_join_columns(10, left_table_id, "left_rows");
        let right_columns = streaming_join_columns(20, right_table_id, "right_rows");
        let equality = streaming_join_binary(
            BinaryOp::Eq,
            streaming_join_expression(&left_columns[1]),
            streaming_join_expression(&right_columns[1]),
        );
        let mut filtered_left = streaming_join_plan(
            &left_columns,
            &right_columns,
            equality.clone(),
            vec![left_columns[0].clone()],
        );
        let PhysicalPlan::HashJoin { left, .. } = &mut filtered_left else {
            panic!("expected HashJoin")
        };
        let scan = left.clone();
        **left = PhysicalPlan::Filter {
            input: scan,
            predicate: Expr {
                kind: ExprKind::Literal(ScalarValue::Bool(true)),
                expr_type: ExprType {
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                },
            },
        };
        let mut storages = [left_storage, right_storage];
        let mut stats = StreamingHashJoinStats::default();
        assert!(
            execute_streaming_hash_join_with_stats(&filtered_left, &mut storages, &mut stats)
                .expect("reject unsupported probe")
                .is_none()
        );
        assert_eq!(stats, StreamingHashJoinStats::default());
        assert_eq!(
            execute_rows(&filtered_left, &mut storages).expect("fallback filtered HashJoin"),
            execute_rows_legacy(&filtered_left, &mut storages)
                .expect("materialized filtered HashJoin")
        );

        let mut missing_key = streaming_join_plan(
            &left_columns,
            &right_columns,
            equality.clone(),
            vec![left_columns[0].clone()],
        );
        let PhysicalPlan::HashJoin { left_key, .. } = &mut missing_key else {
            panic!("expected HashJoin")
        };
        left_key.column_id = ColumnId(99);
        left_key.name = "missing_key".into();
        for result in [
            execute_rows(&missing_key, &mut storages),
            execute_rows_legacy(&missing_key, &mut storages),
        ] {
            assert!(matches!(
                result,
                Err(ExecutionError::MissingColumn(name)) if name == "missing_key"
            ));
        }

        let mut incompatible = streaming_join_plan(
            &left_columns,
            &right_columns,
            equality.clone(),
            vec![left_columns[0].clone()],
        );
        let PhysicalPlan::HashJoin { right_key, .. } = &mut incompatible else {
            panic!("expected HashJoin")
        };
        right_key.data_type = SemanticType::physical(PhysicalType::UInt64);
        assert!(matches!(
            execute_rows(&incompatible, &mut storages),
            Err(ExecutionError::TypeMismatch)
        ));
        assert!(matches!(
            execute_rows_legacy(&incompatible, &mut storages),
            Err(ExecutionError::TypeMismatch)
        ));

        let mut runtime_key_mismatch = streaming_join_plan(
            &left_columns,
            &right_columns,
            equality.clone(),
            vec![left_columns[0].clone()],
        );
        let PhysicalPlan::HashJoin {
            left_key,
            right_key,
            ..
        } = &mut runtime_key_mismatch
        else {
            panic!("expected HashJoin")
        };
        left_key.data_type = SemanticType::physical(PhysicalType::UInt64);
        right_key.data_type = SemanticType::physical(PhysicalType::UInt64);
        let mut stats = StreamingHashJoinStats::default();
        assert!(
            execute_streaming_hash_join_with_stats(
                &runtime_key_mismatch,
                &mut storages,
                &mut stats,
            )
            .expect("reject runtime key mismatch during setup")
            .is_none()
        );
        assert_eq!(stats, StreamingHashJoinStats::default());
        assert!(matches!(
            execute_rows(&runtime_key_mismatch, &mut storages),
            Err(ExecutionError::TypeMismatch)
        ));
        assert!(matches!(
            execute_rows_legacy(&runtime_key_mismatch, &mut storages),
            Err(ExecutionError::TypeMismatch)
        ));

        let mut malformed_predicate = streaming_join_plan(
            &left_columns,
            &right_columns,
            equality,
            vec![left_columns[0].clone()],
        );
        let missing_predicate_column = ColumnRef {
            binding_id: RelationBindingId(10),
            table_id: left_table_id,
            column_id: ColumnId(98),
            relation_name: "left_rows".into(),
            name: "missing_predicate".into(),
            data_type: SemanticType::physical(PhysicalType::Bool),
            nullable: false,
        };
        let PhysicalPlan::HashJoin { predicate, .. } = &mut malformed_predicate else {
            panic!("expected HashJoin")
        };
        *predicate = streaming_join_expression(&missing_predicate_column);
        for result in [
            execute_rows(&malformed_predicate, &mut storages),
            execute_rows_legacy(&malformed_predicate, &mut storages),
        ] {
            assert!(matches!(
                result,
                Err(ExecutionError::MissingColumn(name)) if name == "missing_predicate"
            ));
        }
        for storage in storages {
            storage.close().expect("close fallback join storage");
        }
        remove_batch_test_path(&left_path, false);
        remove_batch_test_path(&right_path, false);
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
                TableStorage::create_heap(&left_path, table(left_table_id, "left_rows"))
                    .expect("create left heap");
            let mut right_storage =
                TableStorage::create_heap(&right_path, table(right_table_id, "right_rows"))
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
            assert_eq!(
                execute_rows(&hash, &mut storages).expect("streaming HashJoin rows"),
                execute_rows_legacy(&hash, &mut storages).expect("materialized HashJoin rows")
            );
            let zero_width_hash = PhysicalPlan::HashJoin {
                left: Box::new(left_scan.clone()),
                right: Box::new(right_scan.clone()),
                kind: JoinKind::Inner,
                left_key: left_columns[1].clone(),
                right_key: right_columns[1].clone(),
                predicate: equality.clone(),
                columns: Vec::new(),
            };
            assert_eq!(
                execute_rows(&zero_width_hash, &mut storages)
                    .expect("streaming zero-width HashJoin"),
                execute_rows_legacy(&zero_width_hash, &mut storages)
                    .expect("materialized zero-width HashJoin")
            );
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
                assert!(matches!(
                    execute_rows_legacy(&invalid_runtime_key, &mut storages),
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
                    execute_rows(&hash_residual, &mut storages)
                        .expect("streaming hash residual rows"),
                    execute_rows_legacy(&hash_residual, &mut storages)
                        .expect("materialized hash residual rows")
                );
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
        let mut storage = TableStorage::create_heap(&path, table).expect("create heap");
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
        let mut storage = TableStorage::create_heap(&path, table).expect("create grouped heap");
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
        let physical = plan(&logical);
        assert_batch_matches_legacy(&physical, &mut storage);
        let result = execute(&physical, &mut storage).expect("execute grouped aggregate");
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
    fn streaming_seq_filter_eligibility_is_exact_and_identity_aware() {
        let column =
            |binding_id: u32, table_id: u64, column_id: u32, name: &str, physical| ColumnRef {
                binding_id: RelationBindingId(binding_id),
                table_id: TableId(table_id),
                column_id: ColumnId(column_id),
                relation_name: format!("t{table_id}"),
                name: name.into(),
                data_type: SemanticType::physical(physical),
                nullable: false,
            };
        let id = column(0, 7, 1, "id", PhysicalType::Int64);
        let active = column(0, 7, 2, "active", PhysicalType::Bool);
        let true_literal = Expr {
            kind: ExprKind::Literal(ScalarValue::Bool(true)),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let active_predicate = Expr {
            kind: ExprKind::Column(active.clone()),
            expr_type: ExprType {
                data_type: active.data_type.clone(),
                nullable: false,
            },
        };
        let scan = |columns| PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns,
        };

        let eligible_scan = scan(vec![id.clone(), active.clone()]);
        let eligible = streaming_seq_filter_eligibility(&eligible_scan, &active_predicate)
            .expect("direct valid SeqScan is eligible");
        assert_eq!(eligible.table_id, TableId(7));
        assert_eq!(eligible.columns, [id.clone(), active.clone()]);
        assert!(streaming_seq_filter_eligibility(&eligible_scan, &true_literal).is_some());

        let missing = column(0, 7, 3, "missing", PhysicalType::Bool);
        let missing_predicate = Expr {
            kind: ExprKind::Column(missing),
            expr_type: active_predicate.expr_type.clone(),
        };
        assert!(streaming_seq_filter_eligibility(&eligible_scan, &missing_predicate).is_none());
        assert!(
            streaming_seq_filter_eligibility(&scan(vec![id.clone(), id.clone()]), &true_literal)
                .is_none()
        );
        assert!(
            streaming_seq_filter_eligibility(
                &PhysicalPlan::SeqScan {
                    binding_id: RelationBindingId(1),
                    table_id: TableId(7),
                    table_name: "t7".into(),
                    columns: vec![id.clone()],
                },
                &true_literal
            )
            .is_none()
        );
        assert!(
            streaming_seq_filter_eligibility(
                &PhysicalPlan::SeqScan {
                    binding_id: RelationBindingId(0),
                    table_id: TableId(8),
                    table_name: "t8".into(),
                    columns: vec![id.clone()],
                },
                &true_literal
            )
            .is_none()
        );

        let sorted = PhysicalPlan::Sort {
            input: Box::new(eligible_scan.clone()),
            keys: vec![SortKey {
                column: id.clone(),
                direction: SortDirection::Asc,
                null_order: NullOrder::First,
            }],
        };
        let nested_filter = PhysicalPlan::Filter {
            input: Box::new(eligible_scan.clone()),
            predicate: true_literal.clone(),
        };
        let right_id = column(1, 8, 1, "right_id", PhysicalType::Int64);
        let joined = PhysicalPlan::NestedLoopJoin {
            left: Box::new(eligible_scan),
            right: Box::new(PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(1),
                table_id: TableId(8),
                table_name: "t8".into(),
                columns: vec![right_id.clone()],
            }),
            kind: JoinKind::Inner,
            predicate: true_literal.clone(),
            columns: vec![id, active, right_id],
        };
        for input in [sorted, nested_filter, joined] {
            assert!(streaming_seq_filter_eligibility(&input, &true_literal).is_none());
        }
    }

    #[test]
    fn streaming_filter_materializes_only_true_rows_and_preserves_first_error() {
        let column = |column_id: u32, name: &str, physical| ColumnRef {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            column_id: ColumnId(column_id),
            relation_name: "items".into(),
            name: name.into(),
            data_type: SemanticType::physical(physical),
            nullable: false,
        };
        let id = column(1, "id", PhysicalType::Int64);
        let payload = column(2, "payload", PhysicalType::Text);
        let fields = vec![
            OutputField::Source(id),
            OutputField::Source(payload.clone()),
        ];
        let predicate = |value, nullable| Expr {
            kind: ExprKind::Literal(value),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable,
            },
        };
        let false_predicate = predicate(ScalarValue::Bool(false), false);
        let unknown_predicate = predicate(ScalarValue::Null, true);
        let true_predicate = predicate(ScalarValue::Bool(true), false);
        let invalid_predicate = Expr {
            kind: ExprKind::Literal(ScalarValue::Int64(1)),
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Int64),
                nullable: false,
            },
        };
        let false_predicate = bind_expression(&false_predicate, &fields).expect("bind false");
        let unknown_predicate = bind_expression(&unknown_predicate, &fields).expect("bind unknown");
        let true_predicate = bind_expression(&true_predicate, &fields).expect("bind true");
        let invalid_predicate =
            bind_expression(&invalid_predicate, &fields).expect("bind invalid scalar");
        let text = String::from("qualified");
        let original_pointer = text.as_ptr();
        let values = [ScalarRef::Int64(7), ScalarRef::Text(&text)];
        let row_id = test_storage_row_handle("streaming-filter");
        let mut rows = Vec::new();
        let mut pending = None;

        let rejected_text_predicate = Expr {
            kind: ExprKind::Binary {
                operator: BinaryOp::Eq,
                left: Box::new(Expr {
                    kind: ExprKind::Column(payload),
                    expr_type: ExprType {
                        data_type: SemanticType::physical(PhysicalType::Text),
                        nullable: false,
                    },
                }),
                right: Box::new(Expr {
                    kind: ExprKind::Literal(ScalarValue::Text("rejected".into())),
                    expr_type: ExprType {
                        data_type: SemanticType::physical(PhysicalType::Text),
                        nullable: false,
                    },
                }),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let rejected_text_predicate =
            bind_expression(&rejected_text_predicate, &fields).expect("bind rejected Text");
        collect_streaming_filter_row(
            &rejected_text_predicate,
            &[1],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert!(rows.is_empty());
        assert!(pending.is_none());
        assert_eq!(text.as_ptr(), original_pointer);

        collect_streaming_filter_row(
            &false_predicate,
            &[0, 1],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        collect_streaming_filter_row(
            &unknown_predicate,
            &[0, 1],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert!(rows.is_empty());
        assert!(pending.is_none());

        collect_streaming_filter_row(
            &true_predicate,
            &[0, 1],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_id, Some(row_id));
        assert_eq!(
            rows[0].values,
            [ScalarValue::Int64(7), ScalarValue::Text("qualified".into())]
        );
        assert_ne!(text_pointer(&rows[0].values[1]), original_pointer);

        rows.clear();
        collect_streaming_filter_row(
            &true_predicate,
            &[0],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert_eq!(rows[0].row_id, Some(row_id));
        assert_eq!(rows[0].values, [ScalarValue::Int64(7)]);

        rows.clear();
        collect_streaming_filter_row(
            &true_predicate,
            &[1, 0, 1],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert_eq!(
            rows[0].values,
            [
                ScalarValue::Text("qualified".into()),
                ScalarValue::Int64(7),
                ScalarValue::Text("qualified".into())
            ]
        );
        assert_ne!(text_pointer(&rows[0].values[0]), original_pointer);
        assert_ne!(text_pointer(&rows[0].values[2]), original_pointer);
        assert_ne!(
            text_pointer(&rows[0].values[0]),
            text_pointer(&rows[0].values[2])
        );

        rows.clear();
        collect_streaming_filter_row(
            &true_predicate,
            &[],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert_eq!(rows[0].row_id, Some(row_id));
        assert!(rows[0].values.is_empty());

        rows.clear();
        collect_streaming_filter_row(
            &invalid_predicate,
            &[0, 1],
            Some(row_id),
            &values,
            &mut rows,
            &mut pending,
        );
        assert!(matches!(pending, Some(ExecutionError::ExpectedBoolean)));
        collect_streaming_filter_row(
            &true_predicate,
            &[],
            Some(row_id),
            &[],
            &mut rows,
            &mut pending,
        );
        assert!(rows.is_empty());
        assert!(matches!(pending, Some(ExecutionError::ExpectedBoolean)));
    }

    #[test]
    fn projected_streaming_filter_eligibility_requires_predicate_only_columns() {
        let column =
            |binding_id: u32, table_id: u64, column_id: u32, name: &str, physical| ColumnRef {
                binding_id: RelationBindingId(binding_id),
                table_id: TableId(table_id),
                column_id: ColumnId(column_id),
                relation_name: format!("t{table_id}"),
                name: name.into(),
                data_type: SemanticType::physical(physical),
                nullable: false,
            };
        let id = column(0, 7, 1, "id", PhysicalType::Int64);
        let payload = column(0, 7, 2, "payload", PhysicalType::Text);
        let marker = column(0, 7, 3, "marker", PhysicalType::Bool);
        let predicate = |column: &ColumnRef| Expr {
            kind: ExprKind::IsNull {
                expression: Box::new(Expr {
                    kind: ExprKind::Column(column.clone()),
                    expr_type: ExprType {
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                    },
                }),
                negated: true,
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: false,
            },
        };
        let filter = |columns: Vec<ColumnRef>, predicate: Expr| PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan {
                binding_id: RelationBindingId(0),
                table_id: TableId(7),
                table_name: "t7".into(),
                columns,
            }),
            predicate,
        };

        let eligible = filter(vec![id.clone(), payload.clone()], predicate(&payload));
        let plan = projected_streaming_seq_filter_eligibility(&eligible, std::slice::from_ref(&id))
            .expect("predicate-only payload is eligible");
        assert_eq!(plan.output_positions, [0]);
        assert_eq!(plan.output_fields, [OutputField::Source(id.clone())]);

        let duplicate =
            projected_streaming_seq_filter_eligibility(&eligible, &[id.clone(), id.clone()])
                .expect("duplicate projected source is eligible");
        assert_eq!(duplicate.output_positions, [0, 0]);

        let reorder = filter(
            vec![id.clone(), payload.clone(), marker.clone()],
            predicate(&marker),
        );
        let reordered = projected_streaming_seq_filter_eligibility(
            &reorder,
            &[payload.clone(), id.clone(), payload.clone()],
        )
        .expect("reordered duplicate projection is eligible");
        assert_eq!(reordered.output_positions, [1, 0, 1]);

        let zero_width = filter(vec![payload.clone()], predicate(&payload));
        let zero = projected_streaming_seq_filter_eligibility(&zero_width, &[])
            .expect("zero-width output with a predicate-only source is eligible");
        assert!(zero.output_positions.is_empty());
        assert!(zero.output_fields.is_empty());

        assert!(
            projected_streaming_seq_filter_eligibility(
                &filter(vec![id.clone()], predicate(&id)),
                std::slice::from_ref(&id)
            )
            .is_none()
        );
        assert!(
            projected_streaming_seq_filter_eligibility(
                &filter(vec![payload.clone()], predicate(&payload)),
                std::slice::from_ref(&payload)
            )
            .is_none()
        );
        assert!(
            projected_streaming_seq_filter_eligibility(
                &filter(
                    vec![id.clone(), payload.clone(), marker.clone()],
                    predicate(&payload)
                ),
                std::slice::from_ref(&id)
            )
            .is_none()
        );
        let missing = column(0, 7, 4, "missing", PhysicalType::Int64);
        assert!(
            projected_streaming_seq_filter_eligibility(&eligible, std::slice::from_ref(&missing))
                .is_none()
        );
        assert!(
            projected_streaming_seq_filter_eligibility(
                &filter(vec![id.clone(), payload.clone()], predicate(&missing)),
                std::slice::from_ref(&id)
            )
            .is_none()
        );
        assert!(
            projected_streaming_seq_filter_eligibility(
                &filter(
                    vec![id.clone(), id.clone(), payload.clone()],
                    predicate(&payload)
                ),
                std::slice::from_ref(&id)
            )
            .is_none()
        );

        let sorted = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::Sort {
                input: Box::new(PhysicalPlan::SeqScan {
                    binding_id: RelationBindingId(0),
                    table_id: TableId(7),
                    table_name: "t7".into(),
                    columns: vec![id.clone(), payload.clone()],
                }),
                keys: vec![SortKey {
                    column: id.clone(),
                    direction: SortDirection::Asc,
                    null_order: NullOrder::First,
                }],
            }),
            predicate: predicate(&payload),
        };
        let nested = PhysicalPlan::Filter {
            input: Box::new(eligible.clone()),
            predicate: predicate(&payload),
        };
        for input in [sorted, nested] {
            assert!(
                projected_streaming_seq_filter_eligibility(&input, std::slice::from_ref(&id))
                    .is_none()
            );
        }
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
        let zero_scan = PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns: vec![],
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
        for output_count in 1..=3 {
            let all_star_outputs = vec![count_all.clone(); output_count];
            let all_star = direct_count_eligibility(&zero_scan, &[], &all_star_outputs)
                .expect("zero-column all-star COUNT is eligible");
            assert!(all_star.scan_columns.is_empty());
            assert_eq!(all_star.outputs.len(), output_count);
            assert!(
                all_star
                    .outputs
                    .iter()
                    .all(|output| output.source == super::DirectCountSource::All)
            );
        }
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
        let named_star_outputs = [
            named_count("first_star", AggregateInput::All),
            named_count("second_star", AggregateInput::All),
            named_count("third_star", AggregateInput::All),
        ];
        let named_star_plan = direct_count_eligibility(&zero_scan, &[], &named_star_outputs)
            .expect("named triple-star COUNT is eligible");
        assert_eq!(
            materialize_direct_count_values(
                &named_star_plan,
                &PresenceCountSummary {
                    live_rows: 9,
                    non_null_counts: vec![],
                }
            )
            .expect("materialize triple-star counts"),
            vec![
                ScalarValue::UInt64(9),
                ScalarValue::UInt64(9),
                ScalarValue::UInt64(9),
            ]
        );
        for (start, expected) in [(0, "first_star"), (1, "second_star"), (2, "third_star")] {
            assert!(matches!(
                materialize_count_values(
                    &named_star_plan.outputs[start..],
                    u128::from(u64::MAX) + 1,
                    &[]
                ),
                Err(ExecutionError::AggregateOverflow {
                    function: AggregateFunction::Count,
                    output,
                }) if output == expected
            ));
        }
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
    fn filtered_count_eligibility_maps_source_order_and_rejects_other_shapes() {
        let column =
            |binding_id: u32, table_id: u64, column_id: u32, name: &str, physical| ColumnRef {
                binding_id: RelationBindingId(binding_id),
                table_id: TableId(table_id),
                column_id: ColumnId(column_id),
                relation_name: format!("t{table_id}"),
                name: name.into(),
                data_type: SemanticType::physical(physical),
                nullable: false,
            };
        let id = column(0, 7, 1, "id", PhysicalType::Int64);
        let note = column(0, 7, 2, "note", PhysicalType::Text);
        let active = column(0, 7, 3, "active", PhysicalType::Bool);
        let extra = column(0, 7, 4, "extra", PhysicalType::Int64);
        let column_expr = |column: ColumnRef| Expr {
            expr_type: ExprType {
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
            kind: ExprKind::Column(column),
        };
        let bool_type = ExprType {
            data_type: SemanticType::physical(PhysicalType::Bool),
            nullable: false,
        };
        let true_literal = Expr {
            kind: ExprKind::Literal(ScalarValue::Bool(true)),
            expr_type: bool_type.clone(),
        };
        let predicate = Expr {
            kind: ExprKind::Binary {
                operator: BinaryOp::Eq,
                left: Box::new(column_expr(active.clone())),
                right: Box::new(true_literal.clone()),
            },
            expr_type: bool_type.clone(),
        };
        let scan = |columns| PhysicalPlan::SeqScan {
            binding_id: RelationBindingId(0),
            table_id: TableId(7),
            table_name: "t7".into(),
            columns,
        };
        let filtered = |input, predicate: Expr| PhysicalPlan::Filter {
            input: Box::new(input),
            predicate,
        };
        let aggregate = |function, input, name: &str| {
            AggregateOutput::Aggregate(AggregateExpr {
                function,
                input,
                output: DerivedField {
                    name: name.into(),
                    data_type: SemanticType::physical(PhysicalType::UInt64),
                    nullable: false,
                },
            })
        };
        let count_note = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(note.clone()),
            "COUNT(note)",
        );
        let count_id = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(id.clone()),
            "COUNT(id)",
        );
        let count_all = aggregate(AggregateFunction::Count, AggregateInput::All, "COUNT(*)");
        let outputs = [
            count_note.clone(),
            count_all.clone(),
            count_note.clone(),
            count_id.clone(),
        ];
        let eligible_input = filtered(
            scan(vec![id.clone(), note.clone(), active.clone()]),
            predicate.clone(),
        );
        let eligible = filtered_count_eligibility(&eligible_input, &[], &outputs)
            .expect("filtered mixed COUNT is eligible");
        assert_eq!(eligible.table_id, TableId(7));
        assert_eq!(eligible.predicate_columns, [&active]);
        assert_eq!(eligible.presence_columns, [&id, &note]);
        assert_eq!(
            eligible
                .outputs
                .iter()
                .map(|output| output.source)
                .collect::<Vec<_>>(),
            [
                super::DirectCountSource::Column(1),
                super::DirectCountSource::All,
                super::DirectCountSource::Column(1),
                super::DirectCountSource::Column(0),
            ]
        );
        assert_eq!(
            eligible
                .star_aggregate
                .map(|item| item.output.name.as_str()),
            Some("COUNT(*)")
        );
        assert_eq!(
            eligible
                .presence_aggregates
                .iter()
                .map(|item| item.output.name.as_str())
                .collect::<Vec<_>>(),
            ["COUNT(id)", "COUNT(note)"]
        );
        let mut star_overflow = FilteredCountSummary {
            qualified_rows: u128::MAX,
            non_null_counts: vec![0, 0],
        };
        assert!(matches!(
            update_filtered_count_summary(&eligible, &mut star_overflow, &[false, false]),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Count,
                output,
            }) if output == "COUNT(*)"
        ));
        let mut column_overflow = FilteredCountSummary {
            qualified_rows: 0,
            non_null_counts: vec![u128::MAX, 0],
        };
        assert!(matches!(
            update_filtered_count_summary(&eligible, &mut column_overflow, &[true, false]),
            Err(ExecutionError::AggregateOverflow {
                function: AggregateFunction::Count,
                output,
            }) if output == "COUNT(id)"
        ));

        assert!(filtered_count_eligibility(&eligible_input, &[], &[]).is_none());
        assert!(
            filtered_count_eligibility(&eligible_input, std::slice::from_ref(&id), &outputs)
                .is_none()
        );
        assert!(
            filtered_count_eligibility(
                &eligible_input,
                &[],
                &[count_all.clone(), count_all.clone()]
            )
            .is_none()
        );
        assert!(
            filtered_count_eligibility(
                &eligible_input,
                &[],
                &[
                    count_note.clone(),
                    aggregate(
                        AggregateFunction::Sum,
                        AggregateInput::Column(id.clone()),
                        "SUM(id)"
                    )
                ]
            )
            .is_none()
        );
        assert!(
            filtered_count_eligibility(
                &eligible_input,
                &[],
                &[AggregateOutput::GroupKey(id.clone())]
            )
            .is_none()
        );

        let unused_scan_column = filtered(
            scan(vec![
                id.clone(),
                note.clone(),
                active.clone(),
                extra.clone(),
            ]),
            predicate.clone(),
        );
        assert!(filtered_count_eligibility(&unused_scan_column, &[], &outputs).is_none());
        let missing_predicate_column =
            filtered(scan(vec![id.clone(), note.clone()]), predicate.clone());
        assert!(filtered_count_eligibility(&missing_predicate_column, &[], &outputs).is_none());
        let missing_count_column =
            filtered(scan(vec![id.clone(), active.clone()]), predicate.clone());
        assert!(filtered_count_eligibility(&missing_count_column, &[], &outputs).is_none());

        let mut mismatched_predicate = predicate.clone();
        let ExprKind::Binary { left, .. } = &mut mismatched_predicate.kind else {
            panic!("expected binary predicate");
        };
        **left = column_expr(column(1, 7, 3, "active", PhysicalType::Bool));
        assert!(
            filtered_count_eligibility(
                &filtered(
                    scan(vec![id.clone(), note.clone(), active.clone()]),
                    mismatched_predicate
                ),
                &[],
                &outputs
            )
            .is_none()
        );
        let mismatched_count = aggregate(
            AggregateFunction::Count,
            AggregateInput::Column(column(0, 8, 2, "note", PhysicalType::Text)),
            "COUNT(other.note)",
        );
        assert!(
            filtered_count_eligibility(
                &eligible_input,
                &[],
                std::slice::from_ref(&mismatched_count)
            )
            .is_none()
        );

        let nested_filter = filtered(
            filtered(
                scan(vec![id.clone(), note.clone(), active.clone()]),
                predicate.clone(),
            ),
            predicate.clone(),
        );
        assert!(filtered_count_eligibility(&nested_filter, &[], &outputs).is_none());
        let sorted = filtered(
            PhysicalPlan::Sort {
                input: Box::new(scan(vec![id.clone(), note.clone(), active.clone()])),
                keys: vec![SortKey {
                    column: id.clone(),
                    direction: SortDirection::Asc,
                    null_order: NullOrder::First,
                }],
            },
            predicate.clone(),
        );
        assert!(filtered_count_eligibility(&sorted, &[], &outputs).is_none());
        let right = column(1, 8, 1, "id", PhysicalType::Int64);
        let joined = filtered(
            PhysicalPlan::NestedLoopJoin {
                left: Box::new(scan(vec![id.clone(), note.clone(), active.clone()])),
                right: Box::new(PhysicalPlan::SeqScan {
                    binding_id: RelationBindingId(1),
                    table_id: TableId(8),
                    table_name: "t8".into(),
                    columns: vec![right],
                }),
                kind: JoinKind::Inner,
                predicate: true_literal,
                columns: vec![id.clone(), note.clone(), active.clone()],
            },
            predicate,
        );
        assert!(filtered_count_eligibility(&joined, &[], &outputs).is_none());
    }

    #[test]
    fn filtered_column_collection_covers_every_expression_shape() {
        let column = |id: u32, name: &str| ColumnRef {
            binding_id: RelationBindingId(2),
            table_id: TableId(9),
            column_id: ColumnId(id),
            relation_name: "items".into(),
            name: name.into(),
            data_type: SemanticType::physical(PhysicalType::Bool),
            nullable: true,
        };
        let bool_type = ExprType {
            data_type: SemanticType::physical(PhysicalType::Bool),
            nullable: true,
        };
        let leaf = |column| Expr {
            kind: ExprKind::Column(column),
            expr_type: bool_type.clone(),
        };
        let predicate = Expr {
            kind: ExprKind::Binary {
                operator: BinaryOp::Or,
                left: Box::new(Expr {
                    kind: ExprKind::Unary {
                        operator: UnaryOp::Not,
                        expression: Box::new(leaf(column(1, "active"))),
                    },
                    expr_type: bool_type.clone(),
                }),
                right: Box::new(Expr {
                    kind: ExprKind::IsNull {
                        expression: Box::new(leaf(column(2, "flag"))),
                        negated: false,
                    },
                    expr_type: bool_type,
                }),
            },
            expr_type: ExprType {
                data_type: SemanticType::physical(PhysicalType::Bool),
                nullable: true,
            },
        };
        assert_eq!(
            collect_filter_columns(&predicate),
            [
                (RelationBindingId(2), TableId(9), ColumnId(1)),
                (RelationBindingId(2), TableId(9), ColumnId(2)),
            ]
            .into_iter()
            .collect()
        );
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
        let mut storage = TableStorage::create_heap(&path, table).expect("create heap");
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
