//! Transaction-local deferred value population for adopted Heap rewrites.
//!
//! The program records typed, bound UPDATE actions after an adopted source has
//! entered schema refinement. Execute observes the exact S1 transaction view;
//! finalization replays the program while projecting S1 rows into S2 and
//! verifies that the replay produced the same ordered observation.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use netbadb_executor::{evaluate_typed_row_expression, typed_row_predicate_matches};
use netbadb_rel::{
    Assignment, BinaryOp, Expr, ExprKind, LogicalPlan, LogicalStatement, OutputField,
};
use netbadb_schema::TableDef;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId, TableSchemaVersion};
use sha2::{Digest, Sha256};

use crate::schema_composition::{MAX_SCHEMA_ACTIONS, RowProjection, SchemaCompositionState};
use crate::schema_mutation::{self, SchemaMutationError};
use crate::{Database, DatabaseError, Transaction};

const MAX_DEFERRED_ACTIONS: usize = 32;
const MAX_ASSIGNMENTS_PER_ACTION: usize = 32;
const MAX_EXPRESSION_NODES: usize = 256;
const MAX_EXPRESSION_DEPTH: usize = 32;

type EvaluatedAssignments = Vec<(ColumnId, usize, ScalarValue)>;

#[derive(Debug, Clone)]
struct EvaluationLayout {
    fields: Vec<OutputField>,
    target_positions: Vec<usize>,
}

/// Immutable row layout captured by the first accepted deferred action.
///
/// Names and nullability are retained because `TableDef` is the existing
/// checked schema primitive, but only TableId, ordered ColumnIds, and semantic
/// types define compatibility with later action layouts. Final constraints
/// always come from the final table, not this snapshot.
#[derive(Debug, Clone)]
struct FrozenEvaluationSchema {
    table: TableDef,
    version: TableSchemaVersion,
}

impl FrozenEvaluationSchema {
    fn capture(table: &TableDef, version: TableSchemaVersion) -> Self {
        Self {
            table: table.clone(),
            version,
        }
    }

    fn validate_layout(&self, table: &TableDef) -> Result<(), DatabaseError> {
        if self.table.id != table.id
            || self.table.columns.len() != table.columns.len()
            || self
                .table
                .columns
                .iter()
                .zip(&table.columns)
                .any(|(frozen, current)| {
                    frozen.id != current.id || frozen.semantic_type() != current.semantic_type()
                })
        {
            return Err(SchemaMutationError::Corrupt(
                "deferred evaluation layout changed after freeze",
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FinalOutputEntry {
    column_id: ColumnId,
    evaluation_position: usize,
    target_nullable: bool,
}

/// Checked E-to-F projection. Output order and constraints come exclusively
/// from F; cached ordinals are derived only after exact ColumnId/type matches.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FinalOutputProjection {
    evaluation_width: usize,
    entries: Vec<FinalOutputEntry>,
    identity: bool,
}

impl FinalOutputProjection {
    fn build(evaluation: &TableDef, final_table: &TableDef) -> Result<Self, DatabaseError> {
        evaluation.validate()?;
        final_table.validate()?;
        if evaluation.id != final_table.id {
            return Err(SchemaMutationError::InvalidSchemaEvolution(
                "final output projection changes TableId",
            )
            .into());
        }
        let evaluation_by_id = evaluation
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| (column.id, (position, column)))
            .collect::<HashMap<_, _>>();
        let entries = final_table
            .columns
            .iter()
            .map(|final_column| {
                let (evaluation_position, evaluation_column) = evaluation_by_id
                    .get(&final_column.id)
                    .ok_or(SchemaMutationError::InvalidSchemaEvolution(
                        "final column is absent from frozen evaluation schema",
                    ))?;
                if evaluation_column.semantic_type() != final_column.semantic_type() {
                    return Err(SchemaMutationError::UnsupportedSchemaEvolution);
                }
                Ok(FinalOutputEntry {
                    column_id: final_column.id,
                    evaluation_position: *evaluation_position,
                    target_nullable: final_column.nullable,
                })
            })
            .collect::<Result<Vec<_>, SchemaMutationError>>()?;
        let identity = entries.len() == evaluation.columns.len()
            && entries
                .iter()
                .enumerate()
                .all(|(position, entry)| entry.evaluation_position == position);
        Ok(Self {
            evaluation_width: evaluation.columns.len(),
            entries,
            identity,
        })
    }

    fn project(
        &self,
        evaluation_values: Vec<ScalarValue>,
    ) -> Result<Vec<ScalarValue>, DatabaseError> {
        if evaluation_values.len() != self.evaluation_width {
            return Err(SchemaMutationError::Corrupt(
                "final output projection source width mismatch",
            )
            .into());
        }
        let final_values = if self.identity {
            evaluation_values
        } else {
            self.entries
                .iter()
                .map(|entry| {
                    evaluation_values
                        .get(entry.evaluation_position)
                        .cloned()
                        .ok_or_else(|| {
                            SchemaMutationError::Corrupt(
                                "final output projection ordinal out of bounds",
                            )
                            .into()
                        })
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?
        };
        for (entry, value) in self.entries.iter().zip(&final_values) {
            if !entry.target_nullable && matches!(value, ScalarValue::Null) {
                return Err(SchemaMutationError::NotNullViolation(entry.column_id).into());
            }
        }
        Ok(final_values)
    }
}

pub(crate) struct DeferredFinalizationProjection {
    base_to_evaluation: RowProjection,
    evaluation_to_final: FinalOutputProjection,
}

impl EvaluationLayout {
    fn values(&self, target_values: &[ScalarValue]) -> Result<Vec<ScalarValue>, DatabaseError> {
        self.target_positions
            .iter()
            .map(|position| {
                target_values.get(*position).cloned().ok_or_else(|| {
                    SchemaMutationError::Corrupt("deferred backfill target ordinal out of bounds")
                        .into()
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct DeferredAssignment {
    target: ColumnId,
    target_position: usize,
    target_type: netbadb_types::SemanticType,
    target_nullable: bool,
    value: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionObservation {
    affected_rows: u64,
    result_digest: [u8; 32],
}

pub(crate) struct ActionAccumulator {
    affected_rows: u64,
    result: Sha256,
}

impl ActionAccumulator {
    fn new() -> Self {
        let mut result = Sha256::new();
        result.update(b"NetbaDB deferred backfill result v1\0");
        Self {
            affected_rows: 0,
            result,
        }
    }

    fn observe(
        &mut self,
        source_values: &[ScalarValue],
        assignments: &[(ColumnId, ScalarValue)],
    ) -> Result<(), DatabaseError> {
        self.affected_rows =
            self.affected_rows
                .checked_add(1)
                .ok_or(SchemaMutationError::Corrupt(
                    "deferred backfill affected-row count overflow",
                ))?;
        put_values(&mut self.result, source_values);
        put_u32(&mut self.result, assignments.len())?;
        for (column, value) in assignments {
            self.result.update(column.0.to_le_bytes());
            put_scalar(&mut self.result, value);
        }
        Ok(())
    }

    fn finish(self) -> ActionObservation {
        ActionObservation {
            affected_rows: self.affected_rows,
            result_digest: self.result.finalize().into(),
        }
    }
}

#[derive(Debug, Clone)]
struct DeferredBackfillAction {
    layout: EvaluationLayout,
    predicate: Option<Expr>,
    assignments: Vec<DeferredAssignment>,
    expected: ActionObservation,
    semantic_digest: [u8; 32],
}

/// Narrow source authority used while observing one deferred action. The
/// distinction is about which existing read view is authoritative; it does not
/// change row evaluation or deferred-program semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeferredSourceAuthority {
    #[cfg(test)]
    CommittedStorageView {
        storage: StorageId,
    },
    TransactionStorageView {
        storage: StorageId,
    },
}

/// An observed action that is not installed in a transaction-local program
/// until all statement validation and durable identity reservations succeed.
#[cfg(test)]
pub(crate) struct PendingDeferredAction {
    action: DeferredBackfillAction,
}

#[cfg(test)]
impl PendingDeferredAction {
    pub(crate) fn semantic_digest(&self) -> [u8; 32] {
        self.action.semantic_digest
    }
}

impl DeferredBackfillAction {
    fn evaluate(
        &self,
        target_values: &[ScalarValue],
    ) -> Result<Option<EvaluatedAssignments>, DatabaseError> {
        let values = self.layout.values(target_values)?;
        if let Some(predicate) = &self.predicate {
            if !typed_row_predicate_matches(predicate, &self.layout.fields, &values)? {
                return Ok(None);
            }
        }
        let evaluated = self
            .assignments
            .iter()
            .map(|assignment| {
                let value =
                    evaluate_typed_row_expression(&assignment.value, &self.layout.fields, &values)?;
                if matches!(value, ScalarValue::Null) {
                    if !assignment.target_nullable {
                        return Err(SchemaMutationError::NotNullViolation(assignment.target).into());
                    }
                } else if !value.matches_type(&assignment.target_type) {
                    return Err(netbadb_executor::ExecutionError::TypeMismatch.into());
                }
                Ok((assignment.target, assignment.target_position, value))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        Ok(Some(evaluated))
    }
}

/// Ordered typed actions retained only by the active schema transaction.
#[derive(Debug, Clone, Default)]
pub(crate) struct DeferredBackfillProgram {
    actions: Vec<DeferredBackfillAction>,
    evaluation_schema: Option<FrozenEvaluationSchema>,
}

impl DeferredBackfillProgram {
    pub(crate) fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub(crate) fn has_evaluation_schema(&self) -> bool {
        self.evaluation_schema.is_some()
    }

    fn validate_evaluation_layout(&self, target: &TableDef) -> Result<(), DatabaseError> {
        if let Some(evaluation) = &self.evaluation_schema {
            evaluation.validate_layout(target)?;
        }
        Ok(())
    }

    fn accept_action(
        &mut self,
        action: DeferredBackfillAction,
        target: &TableDef,
        target_version: TableSchemaVersion,
    ) {
        if self.evaluation_schema.is_none() {
            self.evaluation_schema = Some(FrozenEvaluationSchema::capture(target, target_version));
        }
        self.actions.push(action);
    }

    #[cfg(test)]
    pub(crate) fn accept_pending_action(
        &mut self,
        pending: PendingDeferredAction,
        target: &TableDef,
        target_version: TableSchemaVersion,
    ) {
        self.accept_action(pending.action, target, target_version);
    }

    pub(crate) fn begin_finalization(&self) -> Vec<ActionAccumulator> {
        self.actions
            .iter()
            .map(|_| ActionAccumulator::new())
            .collect()
    }

    pub(crate) fn apply_row(
        &self,
        source_values: &[ScalarValue],
        target_values: &mut [ScalarValue],
        observations: &mut [ActionAccumulator],
    ) -> Result<(), DatabaseError> {
        if observations.len() != self.actions.len() {
            return Err(SchemaMutationError::Corrupt(
                "deferred backfill observation width mismatch",
            )
            .into());
        }
        for (action, observation) in self.actions.iter().zip(observations) {
            // WHERE and every RHS observe one immutable pre-statement row. The
            // assignments are applied only after all expressions have been
            // evaluated, preserving SQL's simultaneous assignment semantics.
            if let Some(assignments) = action.evaluate(target_values)? {
                let digest_values = assignments
                    .iter()
                    .map(|(column, _, value)| (*column, value.clone()))
                    .collect::<Vec<_>>();
                observation.observe(source_values, &digest_values)?;
                for (_, position, value) in assignments {
                    *target_values
                        .get_mut(position)
                        .ok_or(SchemaMutationError::Corrupt(
                            "deferred backfill target ordinal out of bounds",
                        ))? = value;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn build_finalization_projection(
        &self,
        source: &TableDef,
        source_version: TableSchemaVersion,
        final_table: &TableDef,
        reserved_new_columns: &BTreeSet<ColumnId>,
    ) -> Result<DeferredFinalizationProjection, DatabaseError> {
        let evaluation = self
            .evaluation_schema
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt(
                "deferred evaluation schema is absent",
            ))?;
        Ok(DeferredFinalizationProjection {
            base_to_evaluation: RowProjection::build(
                source,
                source_version,
                &evaluation.table,
                evaluation.version,
                reserved_new_columns,
            )?,
            evaluation_to_final: FinalOutputProjection::build(&evaluation.table, final_table)?,
        })
    }

    pub(crate) fn project_final_row(
        &self,
        projection: &DeferredFinalizationProjection,
        source_values: &[ScalarValue],
        observations: &mut [ActionAccumulator],
    ) -> Result<Vec<ScalarValue>, DatabaseError> {
        let mut evaluation_values = projection
            .base_to_evaluation
            .project_without_target_constraints(source_values)?;
        self.apply_row(source_values, &mut evaluation_values, observations)?;
        projection.evaluation_to_final.project(evaluation_values)
    }

    pub(crate) fn verify_finalization(
        &self,
        observations: Vec<ActionAccumulator>,
    ) -> Result<(), DatabaseError> {
        if observations.len() != self.actions.len()
            || observations
                .into_iter()
                .zip(&self.actions)
                .any(|(observed, action)| observed.finish() != action.expected)
        {
            return Err(SchemaMutationError::Corrupt(
                "deferred backfill Execute/finalization mismatch",
            )
            .into());
        }
        Ok(())
    }

    fn project_row(
        &self,
        projection: &RowProjection,
        source_values: &[ScalarValue],
    ) -> Result<Vec<ScalarValue>, DatabaseError> {
        let mut values = projection.project_without_target_constraints(source_values)?;
        let mut observations = self.begin_finalization();
        self.apply_row(source_values, &mut values, &mut observations)?;
        projection.validate_target_constraints(&values)?;
        Ok(values)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.actions.len()
    }

    #[cfg(test)]
    pub(crate) fn semantic_digest(&self, index: usize) -> Option<[u8; 32]> {
        self.actions.get(index).map(|action| action.semantic_digest)
    }

    #[cfg(test)]
    pub(crate) fn corrupt_expected_digest(&mut self, index: usize) {
        if let Some(action) = self.actions.get_mut(index) {
            action.expected.result_digest[0] ^= 0xff;
        }
    }

    #[cfg(test)]
    pub(crate) fn resident_metadata_bytes_estimate(&self) -> usize {
        use std::mem::size_of;

        size_of::<Self>()
            + self.evaluation_schema.as_ref().map_or(0, |evaluation| {
                evaluation.table.columns.capacity() * size_of::<netbadb_schema::ColumnDef>()
            })
            + self.actions.capacity() * size_of::<DeferredBackfillAction>()
            + self
                .actions
                .iter()
                .map(|action| {
                    action.layout.fields.capacity() * size_of::<OutputField>()
                        + action.layout.target_positions.capacity() * size_of::<usize>()
                        + action.assignments.capacity() * size_of::<DeferredAssignment>()
                })
                .sum::<usize>()
    }

    #[cfg(test)]
    pub(crate) fn evaluation_column_ids(&self) -> Option<Vec<ColumnId>> {
        self.evaluation_schema.as_ref().map(|evaluation| {
            evaluation
                .table
                .columns
                .iter()
                .map(|column| column.id)
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn action_cached_positions(&self, index: usize) -> Option<(Vec<usize>, Vec<usize>)> {
        self.actions.get(index).map(|action| {
            (
                action.layout.target_positions.clone(),
                action
                    .assignments
                    .iter()
                    .map(|assignment| assignment.target_position)
                    .collect(),
            )
        })
    }
}

fn put_u32(hash: &mut Sha256, value: usize) -> Result<(), DatabaseError> {
    hash.update(
        u32::try_from(value)
            .map_err(|_| SchemaMutationError::Corrupt("deferred digest length overflow"))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_bytes(hash: &mut Sha256, bytes: &[u8]) -> Result<(), DatabaseError> {
    put_u32(hash, bytes.len())?;
    hash.update(bytes);
    Ok(())
}

fn put_scalar(hash: &mut Sha256, value: &ScalarValue) {
    match value {
        ScalarValue::Null => hash.update([0]),
        ScalarValue::Bool(value) => hash.update([1, u8::from(*value)]),
        ScalarValue::Int64(value) => {
            hash.update([2]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::UInt64(value) => {
            hash.update([3]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::Text(value) => {
            hash.update([4]);
            hash.update((value.len() as u64).to_le_bytes());
            hash.update(value.as_bytes());
        }
        ScalarValue::Int8(value) => {
            hash.update([5]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::Int16(value) => {
            hash.update([6]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::Int32(value) => {
            hash.update([7]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::Int128(value) => {
            hash.update([8]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::UInt8(value) => {
            hash.update([9]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::UInt16(value) => {
            hash.update([10]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::UInt32(value) => {
            hash.update([11]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::UInt128(value) => {
            hash.update([12]);
            hash.update(value.to_le_bytes());
        }
        ScalarValue::Float32(value) => {
            hash.update([13]);
            hash.update(value.to_bits().to_le_bytes());
        }
        ScalarValue::Float64(value) => {
            hash.update([14]);
            hash.update(value.to_bits().to_le_bytes());
        }
        ScalarValue::Bytes(value) => {
            hash.update([15]);
            hash.update((value.len() as u64).to_le_bytes());
            hash.update(value);
        }
    }
}

fn put_values(hash: &mut Sha256, values: &[ScalarValue]) {
    hash.update((values.len() as u64).to_le_bytes());
    for value in values {
        put_scalar(hash, value);
    }
}

fn put_physical(hash: &mut Sha256, physical: PhysicalType) {
    hash.update([match physical {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
        PhysicalType::Int8 => 5,
        PhysicalType::Int16 => 6,
        PhysicalType::Int32 => 7,
        PhysicalType::Int128 => 8,
        PhysicalType::UInt8 => 9,
        PhysicalType::UInt16 => 10,
        PhysicalType::UInt32 => 11,
        PhysicalType::UInt128 => 12,
        PhysicalType::Float32 => 13,
        PhysicalType::Float64 => 14,
        PhysicalType::Bytes => 15,
    }]);
}

fn put_expr(hash: &mut Sha256, expression: &Expr) -> Result<(), DatabaseError> {
    put_physical(hash, expression.expr_type.data_type.physical);
    hash.update([u8::from(expression.expr_type.nullable)]);
    match &expression.expr_type.data_type.name {
        Some(name) => {
            hash.update([1]);
            put_bytes(hash, name.as_bytes())?;
        }
        None => hash.update([0]),
    }
    match &expression.kind {
        ExprKind::Column(column) => {
            hash.update([1]);
            hash.update(column.table_id.0.to_le_bytes());
            hash.update(column.column_id.0.to_le_bytes());
        }
        ExprKind::Literal(value) => {
            hash.update([2]);
            put_scalar(hash, value);
        }
        ExprKind::Parameter(_) => {
            return Err(SchemaMutationError::Corrupt(
                "deferred program retained an unbound parameter",
            )
            .into());
        }
        ExprKind::Cast { expression } => {
            hash.update([3]);
            put_expr(hash, expression)?;
        }
        ExprKind::Binary {
            operator,
            left,
            right,
        } => {
            hash.update([
                4,
                match operator {
                    BinaryOp::Eq => 1,
                    BinaryOp::NotEq => 2,
                    BinaryOp::Lt => 3,
                    BinaryOp::LtEq => 4,
                    BinaryOp::Gt => 5,
                    BinaryOp::GtEq => 6,
                    BinaryOp::And => 7,
                    BinaryOp::Or => 8,
                },
            ]);
            put_expr(hash, left)?;
            put_expr(hash, right)?;
        }
        ExprKind::Unary { expression, .. } => {
            hash.update([5, 1]);
            put_expr(hash, expression)?;
        }
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            hash.update([6, u8::from(*negated)]);
            put_expr(hash, expression)?;
        }
    }
    Ok(())
}

fn expression_is_eligible(
    expression: &Expr,
    table: TableId,
    surviving: &BTreeSet<ColumnId>,
    depth: usize,
    nodes: &mut usize,
) -> Result<bool, DatabaseError> {
    *nodes = nodes
        .checked_add(1)
        .ok_or(SchemaMutationError::CompositionLimitExceeded(
            "deferred expression nodes",
        ))?;
    if depth > MAX_EXPRESSION_DEPTH {
        return Err(
            SchemaMutationError::CompositionLimitExceeded("deferred expression depth").into(),
        );
    }
    if *nodes > MAX_EXPRESSION_NODES {
        return Err(
            SchemaMutationError::CompositionLimitExceeded("deferred expression nodes").into(),
        );
    }
    match &expression.kind {
        ExprKind::Column(column) => {
            Ok(column.table_id == table && surviving.contains(&column.column_id))
        }
        ExprKind::Literal(_) => Ok(true),
        ExprKind::Parameter(_) => Ok(false),
        ExprKind::Cast { expression }
        | ExprKind::Unary { expression, .. }
        | ExprKind::IsNull { expression, .. } => {
            expression_is_eligible(expression, table, surviving, depth + 1, nodes)
        }
        ExprKind::Binary { left, right, .. } => {
            Ok(
                expression_is_eligible(left, table, surviving, depth + 1, nodes)?
                    && expression_is_eligible(right, table, surviving, depth + 1, nodes)?,
            )
        }
    }
}

#[cfg(test)]
pub(crate) fn validate_expression_limits_for_test(expression: &Expr) -> Result<(), DatabaseError> {
    let mut nodes = 0;
    expression_is_eligible(expression, TableId(1), &BTreeSet::new(), 1, &mut nodes)?;
    Ok(())
}

fn scan_and_predicate(
    input: &LogicalPlan,
) -> Option<(TableId, &[netbadb_rel::ColumnRef], Option<&Expr>)> {
    match input {
        LogicalPlan::Scan {
            table_id, columns, ..
        } => Some((*table_id, columns, None)),
        LogicalPlan::Filter { input, predicate } => match input.as_ref() {
            LogicalPlan::Scan {
                table_id, columns, ..
            } => Some((*table_id, columns, Some(predicate))),
            _ => None,
        },
        _ => None,
    }
}

fn build_action_parts(
    table_id: TableId,
    scan_columns: &[netbadb_rel::ColumnRef],
    predicate: Option<&Expr>,
    assignments: &[Assignment],
    base: &TableDef,
    target: &TableDef,
    reserved: &BTreeSet<ColumnId>,
) -> Result<Option<DeferredBackfillAction>, DatabaseError> {
    if table_id != base.id || target.id != base.id || assignments.is_empty() {
        return Ok(None);
    }
    if assignments.len() > MAX_ASSIGNMENTS_PER_ACTION {
        return Err(SchemaMutationError::CompositionLimitExceeded("deferred assignments").into());
    }
    let surviving = target
        .columns
        .iter()
        .filter(|column| base.column_by_id(column.id).is_some())
        .map(|column| column.id)
        .collect::<BTreeSet<_>>();
    // A durable reservation is allocation history, not schema visibility.
    // Only reserved identities that are still present in the current target
    // and absent from the captured base become synthesized/readable late
    // columns. A previously added-then-dropped identity remains burned but
    // cannot re-enter the VirtualRow authority.
    let visible_reserved_late = target
        .columns
        .iter()
        .filter(|column| base.column_by_id(column.id).is_none() && reserved.contains(&column.id))
        .map(|column| column.id)
        .collect::<BTreeSet<_>>();
    let readable = surviving
        .union(&visible_reserved_late)
        .copied()
        .collect::<BTreeSet<_>>();
    let target_positions = scan_columns
        .iter()
        .filter(|column| column.table_id == table_id && readable.contains(&column.column_id))
        .map(|column| {
            target
                .columns
                .iter()
                .position(|target_column| target_column.id == column.column_id)
                .map(|position| (OutputField::Source(column.clone()), position))
                .ok_or(SchemaMutationError::Corrupt(
                    "deferred readable target column disappeared",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let layout = EvaluationLayout {
        fields: target_positions
            .iter()
            .map(|(field, _)| field.clone())
            .collect(),
        target_positions: target_positions
            .iter()
            .map(|(_, position)| *position)
            .collect(),
    };
    let mut node_count = 0;
    if let Some(expression) = predicate {
        if !expression_is_eligible(expression, table_id, &readable, 1, &mut node_count)? {
            return Ok(None);
        }
    }
    let mut deferred_assignments = Vec::with_capacity(assignments.len());
    for Assignment { column, value } in assignments {
        let Some(target_column) = target.column_by_id(column.column_id) else {
            return Ok(None);
        };
        if base.column_by_id(column.column_id).is_some()
            || !reserved.contains(&column.column_id)
            || column.table_id != table_id
        {
            return Ok(None);
        }
        if !expression_is_eligible(value, table_id, &readable, 1, &mut node_count)? {
            return Ok(None);
        }
        let target_position = target
            .columns
            .iter()
            .position(|candidate| candidate.id == column.column_id)
            .ok_or(SchemaMutationError::Corrupt(
                "deferred target ordinal disappeared",
            ))?;
        deferred_assignments.push(DeferredAssignment {
            target: column.column_id,
            target_position,
            target_type: target_column.semantic_type(),
            target_nullable: target_column.nullable,
            value: value.clone(),
        });
    }
    let mut semantic = Sha256::new();
    semantic.update(b"NetbaDB deferred backfill action v1\0");
    semantic.update(table_id.0.to_le_bytes());
    put_u32(&mut semantic, deferred_assignments.len())?;
    for assignment in &deferred_assignments {
        semantic.update(assignment.target.0.to_le_bytes());
        put_expr(&mut semantic, &assignment.value)?;
    }
    match predicate {
        Some(predicate) => {
            semantic.update([1]);
            put_expr(&mut semantic, predicate)?;
        }
        None => semantic.update([0]),
    }
    Ok(Some(DeferredBackfillAction {
        layout,
        predicate: predicate.cloned(),
        assignments: deferred_assignments,
        expected: ActionObservation {
            affected_rows: 0,
            result_digest: [0; 32],
        },
        semantic_digest: semantic.finalize().into(),
    }))
}

fn build_action(
    statement: &LogicalStatement,
    base: &TableDef,
    target: &TableDef,
    reserved: &BTreeSet<ColumnId>,
) -> Result<Option<DeferredBackfillAction>, DatabaseError> {
    let LogicalStatement::Update {
        input,
        table_id,
        assignments,
    } = statement
    else {
        return Ok(None);
    };
    let Some((scan_table, scan_columns, predicate)) = scan_and_predicate(input) else {
        return Ok(None);
    };
    if scan_table != *table_id {
        return Ok(None);
    }
    build_action_parts(
        *table_id,
        scan_columns,
        predicate,
        assignments,
        base,
        target,
        reserved,
    )
}

#[cfg(test)]
fn collect_expression_columns(expression: &Expr, columns: &mut Vec<netbadb_rel::ColumnRef>) {
    match &expression.kind {
        ExprKind::Column(column) => {
            if !columns.iter().any(|candidate| {
                candidate.binding_id == column.binding_id && candidate.column_id == column.column_id
            }) {
                columns.push(column.clone());
            }
        }
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
        ExprKind::Cast { expression }
        | ExprKind::Unary { expression, .. }
        | ExprKind::IsNull { expression, .. } => collect_expression_columns(expression, columns),
        ExprKind::Binary { left, right, .. } => {
            collect_expression_columns(left, columns);
            collect_expression_columns(right, columns);
        }
    }
}

/// Builds the one assignment synthesized by the Round 61 test carrier. The
/// input expression is already typed and bound against the pre-change schema;
/// this function neither parses source text nor selects a conversion.
#[cfg(test)]
pub(crate) fn build_synthetic_assignment(
    using: Expr,
    base: &TableDef,
    evaluation: &TableDef,
    target_column: ColumnId,
) -> Result<PendingDeferredAction, DatabaseError> {
    let target = evaluation
        .column_by_id(target_column)
        .ok_or(SchemaMutationError::Corrupt(
            "synthetic deferred target column absent",
        ))?;
    let mut scan_columns = Vec::new();
    collect_expression_columns(&using, &mut scan_columns);
    let assignment = Assignment {
        column: netbadb_rel::ColumnRef {
            binding_id: netbadb_types::RelationBindingId(0),
            table_id: base.id,
            column_id: target_column,
            relation_name: evaluation.name.clone(),
            name: target.name.clone(),
            data_type: target.semantic_type(),
            nullable: target.nullable,
        },
        value: using,
    };
    let reserved = BTreeSet::from([target_column]);
    let action = build_action_parts(
        base.id,
        &scan_columns,
        None,
        &[assignment],
        base,
        evaluation,
        &reserved,
    )?
    .ok_or(SchemaMutationError::InvalidSchemaEvolution(
        "USING expression is not a same-table scalar expression",
    ))?;
    Ok(PendingDeferredAction { action })
}

struct AdoptedParts {
    table: TableId,
    base: TableDef,
    target: TableDef,
    storage: StorageId,
    reserved: BTreeSet<ColumnId>,
    projection: RowProjection,
}

fn adopted_parts(transaction: &Transaction) -> Result<AdoptedParts, DatabaseError> {
    let adopted = transaction.schema_composition.adopted_source().ok_or(
        SchemaMutationError::InvalidSchemaEvolution("deferred backfill requires adopted source"),
    )?;
    let (&table, touched) = adopted
        .logical
        .touched
        .iter()
        .next()
        .ok_or(SchemaMutationError::Corrupt("adopted source table absent"))?;
    let target = adopted
        .logical
        .overlay
        .schema
        .tables()
        .iter()
        .find(|candidate| candidate.id == table)
        .ok_or(SchemaMutationError::Corrupt("adopted target absent"))?
        .clone();
    let reserved = adopted
        .logical
        .journal
        .borrow()
        .compositions
        .get(&transaction.id())
        .into_iter()
        .flat_map(|record| record.reservations.iter())
        .filter(|reservation| reservation.table == table)
        .map(|reservation| reservation.column)
        .collect::<BTreeSet<_>>();
    let projection = RowProjection::build(
        &touched.base_table,
        touched.base_lineage.version,
        &target,
        adopted.logical.dependency(table)?.table_version,
        &reserved,
    )?;
    Ok(AdoptedParts {
        table,
        base: touched.base_table.clone(),
        target,
        storage: adopted.source_storage,
        reserved,
        projection,
    })
}

#[allow(clippy::too_many_arguments)]
fn observe_action(
    database: &mut Database,
    transaction: &mut Transaction,
    authority: DeferredSourceAuthority,
    base: &TableDef,
    projection: &RowProjection,
    prefix: &DeferredBackfillProgram,
    action: &DeferredBackfillAction,
    final_table: Option<&TableDef>,
    evaluation: &TableDef,
) -> Result<ActionObservation, DatabaseError> {
    let storage = match authority {
        #[cfg(test)]
        DeferredSourceAuthority::CommittedStorageView { storage }
        | DeferredSourceAuthority::TransactionStorageView { storage } => storage,
        #[cfg(not(test))]
        DeferredSourceAuthority::TransactionStorageView { storage } => storage,
    };
    let transaction_view = match authority {
        #[cfg(test)]
        DeferredSourceAuthority::CommittedStorageView { .. } => None,
        DeferredSourceAuthority::TransactionStorageView { .. } => {
            Some(transaction.begin_read_view(&[storage], &mut database.registry)?)
        }
    };
    let committed_view = match authority {
        #[cfg(test)]
        DeferredSourceAuthority::CommittedStorageView { .. } => Some(
            database
                .registry
                .get(storage)
                .ok_or(SchemaMutationError::Corrupt("deferred source Heap absent"))?
                .read_view()?,
        ),
        DeferredSourceAuthority::TransactionStorageView { .. } => None,
    };
    let source_view = match (&transaction_view, &committed_view) {
        (Some(view), None) => view
            .iter()
            .find_map(|(candidate, view)| (candidate == storage).then_some(view))
            .ok_or(SchemaMutationError::Corrupt(
                "deferred transaction source read view absent",
            ))?,
        (None, Some(view)) => view,
        _ => {
            return Err(SchemaMutationError::Corrupt("deferred source view state invalid").into());
        }
    };
    let columns = base
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let final_projection = final_table
        .map(|final_table| FinalOutputProjection::build(evaluation, final_table))
        .transpose()?;
    let mut observation = ActionAccumulator::new();
    let mut prefix_observations = prefix.begin_finalization();
    let flow = database
        .registry
        .get_mut(storage)
        .ok_or(SchemaMutationError::Corrupt("deferred source Heap absent"))?
        .visit_rows_with_view_control::<DatabaseError, _>(
            &columns,
            source_view,
            |_row, source_values| {
                let mut virtual_values =
                    projection.project_without_target_constraints(&source_values)?;
                prefix.apply_row(
                    &source_values,
                    &mut virtual_values,
                    &mut prefix_observations,
                )?;
                if final_projection.is_none() {
                    projection.validate_target_constraints(&virtual_values)?;
                }
                if let Some(assignments) = action.evaluate(&virtual_values)? {
                    let digest_values = assignments
                        .iter()
                        .map(|(column, _, value)| (*column, value.clone()))
                        .collect::<Vec<_>>();
                    observation.observe(&source_values, &digest_values)?;
                    if let Some(final_projection) = &final_projection {
                        for (_, position, value) in assignments {
                            *virtual_values.get_mut(position).ok_or(
                                SchemaMutationError::Corrupt(
                                    "synthetic deferred target ordinal out of bounds",
                                ),
                            )? = value;
                        }
                        final_projection.project(virtual_values)?;
                    }
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
    if flow.is_break() {
        return Err(SchemaMutationError::Corrupt(
            "deferred source row visitor stopped unexpectedly",
        )
        .into());
    }
    prefix.verify_finalization(prefix_observations)?;
    Ok(observation.finish())
}

/// Executes the complete Round 61 acceptance scan without retaining converted
/// rows. The same pending action and frozen evaluation schema are installed
/// only after this function succeeds.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn observe_synthetic_assignment(
    database: &mut Database,
    transaction: &mut Transaction,
    authority: DeferredSourceAuthority,
    pending: &mut PendingDeferredAction,
    base: &TableDef,
    base_version: TableSchemaVersion,
    evaluation: &TableDef,
    evaluation_version: TableSchemaVersion,
    final_table: &TableDef,
    target_column: ColumnId,
) -> Result<u64, DatabaseError> {
    let projection = RowProjection::build(
        base,
        base_version,
        evaluation,
        evaluation_version,
        &BTreeSet::from([target_column]),
    )?;
    let prefix = DeferredBackfillProgram::default();
    let expected = observe_action(
        database,
        transaction,
        authority,
        base,
        &projection,
        &prefix,
        &pending.action,
        Some(final_table),
        evaluation,
    )?;
    let affected_rows = expected.affected_rows;
    pending.action.expected = expected;
    Ok(affected_rows)
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct SyntheticDeferredCost {
    pub(crate) validation: std::time::Duration,
    pub(crate) finalization: std::time::Duration,
    pub(crate) transient_vector_allocations: usize,
    pub(crate) resident_metadata_bytes: usize,
}

/// In-memory cost observation for the exact synthetic action and E-to-F row
/// path. Heap I/O and fixture construction are deliberately excluded.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn audit_synthetic_deferred_cost<F>(
    using: Expr,
    base: &TableDef,
    base_version: TableSchemaVersion,
    evaluation: &TableDef,
    evaluation_version: TableSchemaVersion,
    final_table: &TableDef,
    target_column: ColumnId,
    rows: usize,
    mut source_row: F,
) -> Result<SyntheticDeferredCost, DatabaseError>
where
    F: FnMut(usize) -> Vec<ScalarValue>,
{
    let mut pending = build_synthetic_assignment(using, base, evaluation, target_column)?;
    let reserved = BTreeSet::from([target_column]);
    let base_to_evaluation = RowProjection::build(
        base,
        base_version,
        evaluation,
        evaluation_version,
        &reserved,
    )?;
    let evaluation_to_final = FinalOutputProjection::build(evaluation, final_table)?;
    let validation_started = std::time::Instant::now();
    let mut observed = ActionAccumulator::new();
    for row in 0..rows {
        let source_values = source_row(row);
        let mut values = base_to_evaluation.project_without_target_constraints(&source_values)?;
        let assignments = pending
            .action
            .evaluate(&values)?
            .ok_or(SchemaMutationError::Corrupt(
                "synthetic cost action unexpectedly skipped a row",
            ))?;
        let digest_values = assignments
            .iter()
            .map(|(column, _, value)| (*column, value.clone()))
            .collect::<Vec<_>>();
        observed.observe(&source_values, &digest_values)?;
        for (_, position, value) in assignments {
            values[position] = value;
        }
        std::hint::black_box(evaluation_to_final.project(values)?);
    }
    let validation = validation_started.elapsed();
    pending.action.expected = observed.finish();

    let mut program = DeferredBackfillProgram::default();
    program.accept_pending_action(pending, evaluation, evaluation_version);
    let projection =
        program.build_finalization_projection(base, base_version, final_table, &reserved)?;
    let finalization_started = std::time::Instant::now();
    let mut observations = program.begin_finalization();
    for row in 0..rows {
        let source_values = source_row(row);
        std::hint::black_box(program.project_final_row(
            &projection,
            &source_values,
            &mut observations,
        )?);
    }
    program.verify_finalization(observations)?;
    let finalization = finalization_started.elapsed();
    Ok(SyntheticDeferredCost {
        validation,
        finalization,
        transient_vector_allocations: rows.checked_mul(4).ok_or(SchemaMutationError::Corrupt(
            "synthetic cost allocation count overflow",
        ))?,
        resident_metadata_bytes: program.resident_metadata_bytes_estimate(),
    })
}

pub(crate) fn try_execute_adopted_update(
    database: &mut Database,
    transaction: &mut Transaction,
    statement: &LogicalStatement,
) -> Result<Option<u64>, DatabaseError> {
    try_execute_adopted_update_inner(database, transaction, statement)
        .map(|accepted| accepted.map(|accepted| accepted.affected_rows))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AcceptedAction {
    affected_rows: u64,
}

fn try_execute_adopted_update_inner(
    database: &mut Database,
    transaction: &mut Transaction,
    statement: &LogicalStatement,
) -> Result<Option<AcceptedAction>, DatabaseError> {
    if !matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceRefining(_)
            | SchemaCompositionState::AdoptedSourceBackfilling(_)
    ) {
        return Ok(None);
    }
    let parts = adopted_parts(transaction)?;
    let Some(mut action) = build_action(statement, &parts.base, &parts.target, &parts.reserved)?
    else {
        return Ok(None);
    };
    let plan = transaction
        .schema_composition
        .plan()
        .ok_or(SchemaMutationError::Corrupt("deferred plan absent"))?;
    plan.deferred_backfill
        .validate_evaluation_layout(&parts.target)?;
    let target_version = plan.dependency(parts.table)?.table_version;
    if plan.deferred_backfill.actions.len() >= MAX_DEFERRED_ACTIONS
        || plan.action_count() >= MAX_SCHEMA_ACTIONS
    {
        return Err(
            SchemaMutationError::CompositionLimitExceeded("deferred backfill actions").into(),
        );
    }

    let prefix = plan.deferred_backfill.clone();
    action.expected = observe_action(
        database,
        transaction,
        DeferredSourceAuthority::TransactionStorageView {
            storage: parts.storage,
        },
        &parts.base,
        &parts.projection,
        &prefix,
        &action,
        None,
        &parts.target,
    )?;
    let affected_rows = action.expected.affected_rows;
    let semantic_digest = action.semantic_digest;
    let adopted =
        transaction
            .schema_composition
            .adopted_source_mut()
            .ok_or(SchemaMutationError::Corrupt(
                "deferred adopted state disappeared",
            ))?;
    adopted.logical.action_evidence.push(semantic_digest);
    adopted
        .logical
        .deferred_backfill
        .accept_action(action, &parts.target, target_version);

    let previous = std::mem::replace(
        &mut transaction.schema_composition,
        SchemaCompositionState::None,
    );
    transaction.schema_composition = match previous {
        SchemaCompositionState::AdoptedSourceRefining(adopted)
        | SchemaCompositionState::AdoptedSourceBackfilling(adopted) => {
            SchemaCompositionState::AdoptedSourceBackfilling(adopted)
        }
        other => {
            transaction.schema_composition = other;
            return Err(SchemaMutationError::Corrupt(
                "deferred adopted state changed during acceptance",
            )
            .into());
        }
    };
    schema_mutation::crash("deferred-backfill-accepted");
    Ok(Some(AcceptedAction { affected_rows }))
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VirtualAuditReplayCost {
    pub(crate) rows: usize,
    pub(crate) actions: usize,
    pub(crate) action_evaluations: u64,
    pub(crate) resident_metadata_bytes_estimate: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FinalOutputAuditCost {
    pub(crate) rows: usize,
    pub(crate) evaluation_width: usize,
    pub(crate) final_width: usize,
    pub(crate) dropped_columns: usize,
    pub(crate) output_allocations: usize,
    pub(crate) copied_values: usize,
}

/// Deterministic Round 55 cost probe for the additional E-to-F projection.
#[cfg(test)]
pub(crate) fn audit_final_output_projection(
    evaluation: &TableDef,
    final_table: &TableDef,
    row: &[ScalarValue],
    rows: usize,
) -> Result<FinalOutputAuditCost, DatabaseError> {
    let projection = FinalOutputProjection::build(evaluation, final_table)?;
    if row.len() != evaluation.columns.len() {
        return Err(
            SchemaMutationError::Corrupt("terminal projection audit row width mismatch").into(),
        );
    }
    for _ in 0..rows {
        let projected = projection.project(row.to_vec())?;
        std::hint::black_box(projected);
    }
    Ok(FinalOutputAuditCost {
        rows,
        evaluation_width: evaluation.columns.len(),
        final_width: final_table.columns.len(),
        dropped_columns: evaluation
            .columns
            .len()
            .checked_sub(final_table.columns.len())
            .ok_or(SchemaMutationError::Corrupt(
                "terminal projection audit expands final schema",
            ))?,
        output_allocations: usize::from(!projection.identity).checked_mul(rows).ok_or(
            SchemaMutationError::Corrupt("terminal projection allocation count overflow"),
        )?,
        copied_values: final_table.columns.len().checked_mul(rows).ok_or(
            SchemaMutationError::Corrupt("terminal projection copy count overflow"),
        )?,
    })
}

/// Deterministic in-memory replay probe retained for the bounded cost test.
#[cfg(test)]
pub(crate) fn audit_replay_virtual_rows(
    transaction: &Transaction,
    source_values: &[ScalarValue],
    rows: usize,
    actions: usize,
) -> Result<VirtualAuditReplayCost, DatabaseError> {
    let parts = adopted_parts(transaction)?;
    let mut program = transaction
        .schema_composition
        .plan()
        .ok_or(SchemaMutationError::Corrupt("deferred plan absent"))?
        .deferred_backfill
        .clone();
    if actions > program.actions.len() {
        return Err(
            SchemaMutationError::Corrupt("virtual replay action count exceeds program").into(),
        );
    }
    program.actions.truncate(actions);
    program.actions.shrink_to_fit();
    let mut observations = program.begin_finalization();
    for _ in 0..rows {
        let mut virtual_values = parts
            .projection
            .project_without_target_constraints(source_values)?;
        program.apply_row(source_values, &mut virtual_values, &mut observations)?;
    }
    Ok(VirtualAuditReplayCost {
        rows,
        actions,
        action_evaluations: u64::try_from(rows)
            .ok()
            .and_then(|rows| {
                u64::try_from(actions)
                    .ok()
                    .and_then(|actions| rows.checked_mul(actions))
            })
            .ok_or(SchemaMutationError::Corrupt(
                "virtual replay evaluation count overflow",
            ))?,
        resident_metadata_bytes_estimate: program.resident_metadata_bytes_estimate(),
    })
}

pub(crate) fn validate_projected_new_not_null(
    database: &mut Database,
    transaction: &mut Transaction,
    table: TableId,
    column: ColumnId,
) -> Result<(), DatabaseError> {
    if !matches!(
        transaction.schema_composition,
        SchemaCompositionState::AdoptedSourceBackfilling(_)
    ) {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "new-column SET NOT NULL requires deferred backfilling",
        )
        .into());
    }
    let parts = adopted_parts(transaction)?;
    if parts.table != table
        || parts.base.column_by_id(column).is_some()
        || !parts.reserved.contains(&column)
        || !parts
            .target
            .column_by_id(column)
            .is_some_and(|candidate| candidate.nullable)
    {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "SET NOT NULL target is not a nullable late column",
        )
        .into());
    }
    let target_position = parts
        .target
        .columns
        .iter()
        .position(|candidate| candidate.id == column)
        .ok_or(SchemaMutationError::Corrupt(
            "projected SET NOT NULL target disappeared",
        ))?;
    let program = transaction
        .schema_composition
        .plan()
        .ok_or(SchemaMutationError::Corrupt("deferred plan absent"))?
        .deferred_backfill
        .clone();
    let view = transaction.begin_read_view(&[parts.storage], &mut database.registry)?;
    let source_view = view
        .iter()
        .find_map(|(storage, view)| (storage == parts.storage).then_some(view))
        .ok_or(SchemaMutationError::Corrupt(
            "projected SET NOT NULL source view absent",
        ))?;
    let columns = parts
        .base
        .columns
        .iter()
        .map(|candidate| candidate.id)
        .collect::<Vec<_>>();
    let flow = database
        .registry
        .get_mut(parts.storage)
        .ok_or(SchemaMutationError::Corrupt("deferred source Heap absent"))?
        .visit_rows_with_view_control::<DatabaseError, _>(
            &columns,
            source_view,
            |_row, source_values| {
                let projected = program.project_row(&parts.projection, &source_values)?;
                if matches!(projected[target_position], ScalarValue::Null) {
                    return Err(SchemaMutationError::NotNullViolation(column).into());
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
    if flow.is_break() {
        return Err(SchemaMutationError::Corrupt(
            "projected SET NOT NULL row visitor stopped unexpectedly",
        )
        .into());
    }
    Ok(())
}
