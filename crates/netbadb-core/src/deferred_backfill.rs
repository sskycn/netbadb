//! Transaction-local deferred value population for adopted Heap rewrites.
//!
//! The program records typed, bound UPDATE actions after an adopted source has
//! entered schema refinement. Execute observes the exact S1 transaction view;
//! finalization replays the program while projecting S1 rows into S2 and
//! verifies that the replay produced the same ordered observation.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use netbadb_executor::{evaluate_typed_row_expression, typed_row_predicate_matches};
use netbadb_rel::{
    Assignment, BinaryOp, Expr, ExprKind, LogicalPlan, LogicalStatement, OutputField,
};
use netbadb_schema::TableDef;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};
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
    source_positions: Vec<usize>,
}

impl EvaluationLayout {
    fn values(&self, source_values: &[ScalarValue]) -> Result<Vec<ScalarValue>, DatabaseError> {
        self.source_positions
            .iter()
            .map(|position| {
                source_values.get(*position).cloned().ok_or_else(|| {
                    SchemaMutationError::Corrupt("deferred backfill source ordinal out of bounds")
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

impl DeferredBackfillAction {
    fn evaluate(
        &self,
        source_values: &[ScalarValue],
    ) -> Result<Option<EvaluatedAssignments>, DatabaseError> {
        let values = self.layout.values(source_values)?;
        if let Some(predicate) = &self.predicate
            && !typed_row_predicate_matches(predicate, &self.layout.fields, &values)?
        {
            return Ok(None);
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
}

impl DeferredBackfillProgram {
    pub(crate) fn is_empty(&self) -> bool {
        self.actions.is_empty()
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
            if let Some(assignments) = action.evaluate(source_values)? {
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
    if *table_id != base.id || target.id != base.id || assignments.is_empty() {
        return Ok(None);
    }
    if assignments.len() > MAX_ASSIGNMENTS_PER_ACTION {
        return Err(SchemaMutationError::CompositionLimitExceeded("deferred assignments").into());
    }
    let Some((scan_table, scan_columns, predicate)) = scan_and_predicate(input) else {
        return Ok(None);
    };
    if scan_table != *table_id {
        return Ok(None);
    }
    let surviving = target
        .columns
        .iter()
        .filter(|column| base.column_by_id(column.id).is_some())
        .map(|column| column.id)
        .collect::<BTreeSet<_>>();
    let source_positions = scan_columns
        .iter()
        .filter(|column| column.table_id == *table_id && surviving.contains(&column.column_id))
        .map(|column| {
            base.columns
                .iter()
                .position(|base_column| base_column.id == column.column_id)
                .map(|position| (OutputField::Source(column.clone()), position))
                .ok_or(SchemaMutationError::Corrupt(
                    "deferred surviving source column disappeared",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let layout = EvaluationLayout {
        fields: source_positions
            .iter()
            .map(|(field, _)| field.clone())
            .collect(),
        source_positions: source_positions
            .iter()
            .map(|(_, position)| *position)
            .collect(),
    };
    let mut node_count = 0;
    if let Some(expression) = predicate
        && !expression_is_eligible(expression, *table_id, &surviving, 1, &mut node_count)?
    {
        return Ok(None);
    }
    let mut deferred_assignments = Vec::with_capacity(assignments.len());
    for Assignment { column, value } in assignments {
        let Some(target_column) = target.column_by_id(column.column_id) else {
            return Ok(None);
        };
        if base.column_by_id(column.column_id).is_some()
            || !reserved.contains(&column.column_id)
            || column.table_id != *table_id
        {
            return Ok(None);
        }
        if !expression_is_eligible(value, *table_id, &surviving, 1, &mut node_count)? {
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

pub(crate) fn try_execute_adopted_update(
    database: &mut Database,
    transaction: &mut Transaction,
    statement: &LogicalStatement,
) -> Result<Option<u64>, DatabaseError> {
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
    if plan.deferred_backfill.actions.len() >= MAX_DEFERRED_ACTIONS
        || plan.action_count() >= MAX_SCHEMA_ACTIONS
    {
        return Err(
            SchemaMutationError::CompositionLimitExceeded("deferred backfill actions").into(),
        );
    }

    let view = transaction.begin_read_view(&[parts.storage], &mut database.registry)?;
    let source_view = view
        .iter()
        .find_map(|(storage, view)| (storage == parts.storage).then_some(view))
        .ok_or(SchemaMutationError::Corrupt(
            "deferred source read view absent",
        ))?;
    let columns = parts
        .base
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    let mut observation = ActionAccumulator::new();
    let flow = database
        .registry
        .get_mut(parts.storage)
        .ok_or(SchemaMutationError::Corrupt("deferred source Heap absent"))?
        .visit_rows_with_view_control::<DatabaseError, _>(
            &columns,
            source_view,
            |_row, source_values| {
                if let Some(assignments) = action.evaluate(&source_values)? {
                    let digest_values = assignments
                        .into_iter()
                        .map(|(column, _, value)| (column, value))
                        .collect::<Vec<_>>();
                    observation.observe(&source_values, &digest_values)?;
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
    action.expected = observation.finish();
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
    adopted.logical.deferred_backfill.actions.push(action);

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
    Ok(Some(affected_rows))
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
