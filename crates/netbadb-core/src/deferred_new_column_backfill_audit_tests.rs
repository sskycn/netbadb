//! Round 49 test-only executable architecture for deferred new-column values.
//!
//! The carrier is reachable only from Core unit tests. Production SQL continues
//! to reject every relational statement after adopted-source refinement.

use super::*;
use crate::schema_composition::{MAX_SCHEMA_ACTIONS, RowProjection};
use crate::schema_mutation::SchemaMutationError;
use netbadb_executor::{evaluate_typed_row_expression, typed_row_predicate_matches};
use netbadb_rel::{
    Assignment, BinaryOp, Expr, ExprKind, LogicalPlan, LogicalStatement, OutputField,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

const MAX_DEFERRED_ACTIONS: usize = 32;
const MAX_ASSIGNMENTS_PER_ACTION: usize = 32;
const MAX_EXPRESSION_NODES: usize = 256;
const MAX_EXPRESSION_DEPTH: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditPhase {
    Refining,
    Backfilling,
    IndexFinalizing,
}

impl AuditPhase {
    const fn structural_alter_allowed(self) -> bool {
        matches!(self, Self::Refining)
    }

    const fn deferred_update_allowed(self) -> bool {
        matches!(self, Self::Refining | Self::Backfilling)
    }

    const fn projected_nullability_allowed(self) -> bool {
        matches!(self, Self::Backfilling)
    }

    const fn final_index_allowed(self) -> bool {
        matches!(self, Self::Refining | Self::Backfilling)
    }
}

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

    fn observe_rows(&self, rows: &[Vec<ScalarValue>]) -> Result<ActionObservation, DatabaseError> {
        let mut observation = ActionAccumulator::new();
        for source_values in rows {
            if let Some(assignments) = self.evaluate(source_values)? {
                let digest_values = assignments
                    .iter()
                    .map(|(column, _, value)| (*column, value.clone()))
                    .collect::<Vec<_>>();
                observation.observe(source_values, &digest_values)?;
            }
        }
        Ok(observation.finish())
    }
}

/// Ordered typed programs are the selected Round 49 value-population authority.
/// The type is test-only; production dispatch cannot construct or route one.
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
) -> bool {
    *nodes += 1;
    if depth > MAX_EXPRESSION_DEPTH || *nodes > MAX_EXPRESSION_NODES {
        return false;
    }
    match &expression.kind {
        ExprKind::Column(column) => {
            column.table_id == table && surviving.contains(&column.column_id)
        }
        ExprKind::Literal(_) => true,
        ExprKind::Parameter(_) => false,
        ExprKind::Cast { expression }
        | ExprKind::Unary { expression, .. }
        | ExprKind::IsNull { expression, .. } => {
            expression_is_eligible(expression, table, surviving, depth + 1, nodes)
        }
        ExprKind::Binary { left, right, .. } => {
            expression_is_eligible(left, table, surviving, depth + 1, nodes)
                && expression_is_eligible(right, table, surviving, depth + 1, nodes)
        }
    }
}

fn scan_and_predicate(input: &LogicalPlan) -> Option<(&[netbadb_rel::ColumnRef], Option<&Expr>)> {
    match input {
        LogicalPlan::Scan { columns, .. } => Some((columns, None)),
        LogicalPlan::Filter { input, predicate } => match input.as_ref() {
            LogicalPlan::Scan { columns, .. } => Some((columns, Some(predicate))),
            _ => None,
        },
        _ => None,
    }
}

fn build_action(
    statement: LogicalStatement,
    base: &TableDef,
    target: &TableDef,
    reserved: &BTreeSet<ColumnId>,
) -> Result<DeferredBackfillAction, DatabaseError> {
    let LogicalStatement::Update {
        input,
        table_id,
        assignments,
    } = statement
    else {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "deferred backfill accepts UPDATE only",
        )
        .into());
    };
    if table_id != base.id || target.id != base.id || assignments.is_empty() {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "deferred backfill must target the adopted table",
        )
        .into());
    }
    if assignments.len() > MAX_ASSIGNMENTS_PER_ACTION {
        return Err(SchemaMutationError::CompositionLimitExceeded("deferred assignments").into());
    }
    let (scan_columns, predicate) = scan_and_predicate(&input).ok_or(
        SchemaMutationError::InvalidSchemaEvolution("deferred UPDATE input is not scan/filter"),
    )?;
    let surviving = target
        .columns
        .iter()
        .filter(|column| base.column_by_id(column.id).is_some())
        .map(|column| column.id)
        .collect::<BTreeSet<_>>();
    let source_positions = scan_columns
        .iter()
        .filter_map(|column| {
            base.columns
                .iter()
                .position(|base_column| base_column.id == column.column_id)
                .map(|position| (OutputField::Source(column.clone()), position))
        })
        .collect::<Vec<_>>();
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
    if predicate.is_some_and(|expression| {
        !expression_is_eligible(expression, table_id, &surviving, 1, &mut node_count)
    }) {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "deferred predicate reads a late or foreign column",
        )
        .into());
    }
    let assignments = assignments
        .into_iter()
        .map(|Assignment { column, value }| {
            let target_column = target.column_by_id(column.column_id).ok_or(
                SchemaMutationError::InvalidSchemaEvolution("deferred target disappeared"),
            )?;
            if base.column_by_id(column.column_id).is_some()
                || !reserved.contains(&column.column_id)
                || column.table_id != table_id
            {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "deferred assignment target is not a durably reserved late column",
                )
                .into());
            }
            if !expression_is_eligible(&value, table_id, &surviving, 1, &mut node_count) {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "deferred value reads a late or foreign column",
                )
                .into());
            }
            let target_position = target
                .columns
                .iter()
                .position(|candidate| candidate.id == column.column_id)
                .ok_or(SchemaMutationError::Corrupt(
                    "deferred target ordinal disappeared",
                ))?;
            Ok(DeferredAssignment {
                target: column.column_id,
                target_position,
                target_type: target_column.semantic_type(),
                target_nullable: target_column.nullable,
                value,
            })
        })
        .collect::<Result<Vec<_>, DatabaseError>>()?;
    let mut semantic = Sha256::new();
    semantic.update(b"NetbaDB deferred backfill action v1\0");
    semantic.update(table_id.0.to_le_bytes());
    put_u32(&mut semantic, assignments.len())?;
    for assignment in &assignments {
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
    Ok(DeferredBackfillAction {
        layout,
        predicate: predicate.cloned(),
        assignments,
        expected: ActionObservation {
            affected_rows: 0,
            result_digest: [0; 32],
        },
        semantic_digest: semantic.finalize().into(),
    })
}

fn adopted_parts(
    transaction: &Transaction,
) -> Result<(TableId, TableDef, TableDef, StorageId, RowProjection), DatabaseError> {
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
    Ok((
        table,
        touched.base_table.clone(),
        target,
        adopted.source_storage,
        projection,
    ))
}

fn source_rows(
    db: &mut Database,
    transaction: &mut Transaction,
    storage: StorageId,
    base: &TableDef,
) -> Result<Vec<Vec<ScalarValue>>, DatabaseError> {
    let view = transaction.begin_read_view(&[storage], &mut db.registry)?;
    let source_view = view
        .iter()
        .find_map(|(candidate, view)| (candidate == storage).then_some(view))
        .ok_or(SchemaMutationError::Corrupt(
            "deferred source read view absent",
        ))?;
    let columns = base
        .columns
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    Ok(db
        .registry
        .get_mut(storage)
        .ok_or(SchemaMutationError::Corrupt("deferred source Heap absent"))?
        .scan_columns_with_view(&columns, source_view)?
        .into_iter()
        .map(|(_, values)| values)
        .collect())
}

fn source_rows_with_handles(
    db: &mut Database,
    transaction: &mut Transaction,
    storage: StorageId,
    columns: &[ColumnId],
) -> Result<Vec<(netbadb_storage::StorageRowHandle, Vec<ScalarValue>)>, DatabaseError> {
    let view = transaction.begin_read_view(&[storage], &mut db.registry)?;
    let source_view = view
        .iter()
        .find_map(|(candidate, view)| (candidate == storage).then_some(view))
        .ok_or(SchemaMutationError::Corrupt(
            "deferred source read view absent",
        ))?;
    Ok(db
        .registry
        .get_mut(storage)
        .ok_or(SchemaMutationError::Corrupt("deferred source Heap absent"))?
        .scan_columns_with_view(columns, source_view)?)
}

fn accept_deferred_update(
    db: &mut Database,
    transaction: &mut Transaction,
    prepared: &PreparedStatement,
    parameters: &[ScalarValue],
    phase: &mut AuditPhase,
) -> Result<u64, DatabaseError> {
    if !phase.deferred_update_allowed() {
        return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
    }
    db.validate_transaction(transaction)?;
    db.validate_prepared_dependencies(prepared, Some(transaction))?;
    let statement = netbadb_compiler::bind_statement(&prepared.compiled, parameters)?;
    let (table, base, target, storage, _) = adopted_parts(transaction)?;
    let reserved = transaction
        .schema_composition
        .plan()
        .ok_or(SchemaMutationError::Corrupt("deferred plan absent"))?
        .journal
        .borrow()
        .compositions
        .get(&transaction.id())
        .into_iter()
        .flat_map(|record| record.reservations.iter())
        .filter(|reservation| reservation.table == table)
        .map(|reservation| reservation.column)
        .collect::<BTreeSet<_>>();
    let mut action = build_action(statement, &base, &target, &reserved)?;
    let rows = source_rows(db, transaction, storage, &base)?;
    action.expected = action.observe_rows(&rows)?;
    let affected = action.expected.affected_rows;
    let plan = &mut transaction
        .schema_composition
        .adopted_source_mut()
        .ok_or(SchemaMutationError::Corrupt(
            "deferred adopted state disappeared",
        ))?
        .logical;
    if plan.deferred_backfill.actions.len() >= MAX_DEFERRED_ACTIONS
        || plan.action_count() >= MAX_SCHEMA_ACTIONS
    {
        return Err(
            SchemaMutationError::CompositionLimitExceeded("deferred backfill actions").into(),
        );
    }
    plan.action_evidence.push(action.semantic_digest);
    plan.deferred_backfill.actions.push(action);
    *phase = AuditPhase::Backfilling;
    crate::schema_mutation::crash("deferred-backfill-accepted");
    Ok(affected)
}

fn validate_and_set_new_not_null(
    db: &mut Database,
    transaction: &mut Transaction,
    column: ColumnId,
    phase: AuditPhase,
) -> Result<(), DatabaseError> {
    if phase != AuditPhase::Backfilling {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "new-column SET NOT NULL requires backfilling phase",
        )
        .into());
    }
    let (table, base, target, storage, projection) = adopted_parts(transaction)?;
    if base.column_by_id(column).is_some()
        || target.column_by_id(column).is_none()
        || !target
            .column_by_id(column)
            .is_some_and(|value| value.nullable)
    {
        return Err(SchemaMutationError::InvalidSchemaEvolution(
            "SET target is not a nullable late column",
        )
        .into());
    }
    let program = transaction
        .schema_composition
        .plan()
        .ok_or(SchemaMutationError::Corrupt("deferred plan absent"))?
        .deferred_backfill
        .clone();
    let target_position = target
        .columns
        .iter()
        .position(|candidate| candidate.id == column)
        .ok_or(SchemaMutationError::Corrupt("SET target ordinal absent"))?;
    for source in source_rows(db, transaction, storage, &base)? {
        if matches!(
            program.project_row(&projection, &source)?[target_position],
            ScalarValue::Null
        ) {
            return Err(SchemaMutationError::NotNullViolation(column).into());
        }
    }
    let plan = &mut transaction
        .schema_composition
        .adopted_source_mut()
        .ok_or(SchemaMutationError::Corrupt(
            "deferred adopted state disappeared",
        ))?
        .logical;
    let mut tables = plan.overlay.schema.tables().to_vec();
    tables
        .iter_mut()
        .find(|candidate| candidate.id == table)
        .and_then(|table| {
            table
                .columns
                .iter_mut()
                .find(|candidate| candidate.id == column)
        })
        .ok_or(SchemaMutationError::Corrupt("SET target disappeared"))?
        .nullable = false;
    plan.overlay.schema = netbadb_schema::Schema::new(tables)?;
    let mut evidence = Sha256::new();
    evidence.update(b"NetbaDB deferred projected SET NOT NULL v1\0");
    evidence.update(table.0.to_le_bytes());
    evidence.update(column.0.to_le_bytes());
    plan.action_evidence.push(evidence.finalize().into());
    Ok(())
}

fn prepared_update(
    db: &Database,
    transaction: &Transaction,
    sql: &str,
    declared: &[Option<PhysicalType>],
) -> PreparedStatement {
    let PreparedSqlStatement::Relational(prepared) = db
        .prepare_sql_statement_in(transaction, sql, declared)
        .unwrap()
    else {
        panic!("expected relational UPDATE")
    };
    *prepared
}

fn root(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round49-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn seed(path: &std::path::Path) -> Database {
    let mut db = Database::create_catalog(
        path.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            path.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(path.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE users (id BIGINT NOT NULL, legacy TEXT, flag BOOLEAN, score BIGINT)")
        .unwrap();
    for sql in [
        "INSERT INTO users VALUES (1, 'old1', true, 10)",
        "INSERT INTO users VALUES (2, 'old2', false, 20)",
        "INSERT INTO users VALUES (3, NULL, NULL, 30)",
    ] {
        db.execute(sql).unwrap();
    }
    db
}

fn adopt(
    db: &mut Database,
    path: &std::path::Path,
    additions: &str,
) -> (Transaction, AuditPhase, StorageId, [u8; 32]) {
    let mut transaction = db.begin_transaction().unwrap();
    for sql in [
        "UPDATE users SET legacy = 'updated1' WHERE id = 1",
        "INSERT INTO users VALUES (4, 'inserted4', true, 40)",
        "DELETE FROM users WHERE id = 2",
    ] {
        db.execute_in(&mut transaction, sql).unwrap();
    }
    for sql in additions.split(';').filter(|sql| !sql.trim().is_empty()) {
        db.execute_in(&mut transaction, sql).unwrap();
    }
    let source = transaction
        .schema_composition
        .adopted_source()
        .unwrap()
        .source_storage;
    let digest = crate::schema_mutation_journal::heap_rewrite_indexes_digest(
        &db.registry
            .get_mut(source)
            .unwrap()
            .heap_rewrite_indexes()
            .unwrap(),
    )
    .unwrap();
    let _ = path;
    (transaction, AuditPhase::Refining, source, digest)
}

fn assert_physical_keys(
    db: &mut Database,
    storage_id: StorageId,
    column: ColumnId,
    key: &ScalarValue,
    expected: &[i64],
) {
    let storage = db.registry.get_mut(storage_id).unwrap();
    let path = storage
        .access_paths()
        .into_iter()
        .find(|path| path.column_id == column)
        .unwrap()
        .id;
    let view = storage.read_view().unwrap();
    let mut ids = storage
        .point_lookup_columns_with_view(path, key, &[ColumnId(1)], &view)
        .unwrap()
        .into_iter()
        .map(|(_, values)| {
            let ScalarValue::Int64(id) = values[0] else {
                panic!("id")
            };
            id
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, expected);
}

#[test]
fn ordered_typed_program_populates_one_s2_and_builds_final_btree() {
    let root = root("full-sequence");
    let mut db = seed(&root);
    let (mut transaction, mut phase, source, source_digest) = adopt(
        &mut db,
        &root,
        "ALTER TABLE users ADD COLUMN marker TEXT; ALTER TABLE users ADD COLUMN copied_score BIGINT; ALTER TABLE users ADD COLUMN copied_flag BOOLEAN",
    );
    let next_storage_before = db.next_storage_id().unwrap();

    let first = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET marker = legacy, copied_score = score, copied_flag = flag WHERE legacy IS NOT NULL",
        &[],
    );
    assert_eq!(
        accept_deferred_update(&mut db, &mut transaction, &first, &[], &mut phase).unwrap(),
        2
    );
    assert_eq!(phase, AuditPhase::Backfilling);
    assert_eq!(db.next_storage_id().unwrap(), next_storage_before);
    assert_eq!(
        crate::schema_mutation_journal::heap_rewrite_indexes_digest(
            &db.registry
                .get_mut(source)
                .unwrap()
                .heap_rewrite_indexes()
                .unwrap(),
        )
        .unwrap(),
        source_digest
    );

    let repair = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET marker = $1, copied_score = $2, copied_flag = $3 WHERE legacy IS NULL",
        &[
            Some(PhysicalType::Text),
            Some(PhysicalType::Int64),
            Some(PhysicalType::Bool),
        ],
    );
    assert_eq!(
        accept_deferred_update(
            &mut db,
            &mut transaction,
            &repair,
            &[
                ScalarValue::Text("missing".into()),
                ScalarValue::Int64(30),
                ScalarValue::Bool(false),
            ],
            &mut phase,
        )
        .unwrap(),
        1
    );

    for column in [ColumnId(5), ColumnId(6), ColumnId(7)] {
        validate_and_set_new_not_null(&mut db, &mut transaction, column, phase).unwrap();
    }
    assert_eq!(
        db.execute_in(
            &mut transaction,
            "CREATE INDEX users_marker_idx ON users(marker)",
        )
        .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    phase = AuditPhase::IndexFinalizing;
    let expected_action_digest = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_digest();
    let blocked = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET marker = 'too-late'",
        &[],
    );
    assert!(accept_deferred_update(&mut db, &mut transaction, &blocked, &[], &mut phase).is_err());

    db.finalize_adopted_source(&mut transaction).unwrap();
    let materialized = transaction.schema_composition.materialized_index().unwrap();
    assert_eq!(materialized.intent.action_digest, expected_action_digest);
    assert_eq!(materialized.source_copy_passes, 1);
    assert_eq!(materialized.source_rows_copied, 3);
    let crate::schema_mutation_journal::SchemaIndexTablePlan::RewriteHeap { replacement, .. } =
        &materialized.intent.tables[0]
    else {
        panic!("expected one final rewrite")
    };
    let target = replacement.new_storage();
    assert_ne!(source, target);
    assert_eq!(target, next_storage_before);
    let source_intent = db
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .source_backfill_intents
        .get(&transaction.id())
        .unwrap()
        .clone();
    let mut clone_plan = Sha256::new();
    clone_plan.update(materialized.intent.transaction.0.to_le_bytes());
    clone_plan.update(source.0.to_le_bytes());
    clone_plan.update(target.0.to_le_bytes());
    clone_plan.update(materialized.intent.action_digest);
    clone_plan.update(materialized.intent.snapshot_digest);
    assert_eq!(
        source_intent.clone_plan_digest,
        <[u8; 32]>::from(clone_plan.finalize())
    );
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(
        db.query("SELECT id, marker, copied_score, copied_flag FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("updated1".into()),
                ScalarValue::Int64(10),
                ScalarValue::Bool(true),
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Text("missing".into()),
                ScalarValue::Int64(30),
                ScalarValue::Bool(false),
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Text("inserted4".into()),
                ScalarValue::Int64(40),
                ScalarValue::Bool(true),
            ],
        ]
    );
    assert_physical_keys(
        &mut db,
        target,
        ColumnId(5),
        &ScalarValue::Text("updated1".into()),
        &[1],
    );
    assert_physical_keys(
        &mut db,
        target,
        ColumnId(5),
        &ScalarValue::Text("missing".into()),
        &[3],
    );
    for _ in 0..3 {
        db.close().unwrap();
        db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            db.query("SELECT marker FROM users ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Text("updated1".into())],
                vec![ScalarValue::Text("missing".into())],
                vec![ScalarValue::Text("inserted4".into())],
            ]
        );
    }
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn production_gate_prepare_purity_stale_identity_and_eligibility_are_bounded() {
    let root = root("eligibility");
    let mut db = seed(&root);
    let (mut transaction, mut phase, _source, _) =
        adopt(&mut db, &root, "ALTER TABLE users ADD COLUMN marker TEXT");
    let journal_before = db
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .encode()
        .unwrap();
    let storage_before = db.next_storage_id();
    let prepared = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET marker = $1 WHERE id = $2",
        &[Some(PhysicalType::Text), Some(PhysicalType::Int64)],
    );
    assert_eq!(
        db.mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .encode()
            .unwrap(),
        journal_before
    );
    assert_eq!(db.next_storage_id(), storage_before);
    assert!(matches!(
        db.execute_prepared_in(
            &mut transaction,
            &prepared,
            &[ScalarValue::Text("x".into()), ScalarValue::Int64(1)],
        ),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::MigrationDataAccessAfterRefinement
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );

    let base_write = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET legacy = 'forbidden'",
        &[],
    );
    assert!(
        accept_deferred_update(&mut db, &mut transaction, &base_write, &[], &mut phase).is_err()
    );
    let late_read = prepared_update(&db, &transaction, "UPDATE users SET marker = marker", &[]);
    assert!(
        accept_deferred_update(&mut db, &mut transaction, &late_read, &[], &mut phase).is_err()
    );

    let stale = prepared_update(&db, &transaction, "UPDATE users SET marker = legacy", &[]);
    db.execute_in(
        &mut transaction,
        "ALTER TABLE users RENAME COLUMN legacy TO contact",
    )
    .unwrap();
    let action_count = transaction
        .schema_composition
        .plan()
        .unwrap()
        .action_count();
    let stale_result = accept_deferred_update(&mut db, &mut transaction, &stale, &[], &mut phase);
    assert!(
        matches!(
            stale_result,
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::StalePreparedStatement
            ))
        ),
        "{stale_result:?}"
    );
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .action_count(),
        action_count
    );
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .is_empty()
    );
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn partial_not_null_failure_can_be_repaired_and_ordered_updates_are_last_wins() {
    let root = root("repair");
    let mut db = seed(&root);
    let (mut transaction, mut phase, _, _) =
        adopt(&mut db, &root, "ALTER TABLE users ADD COLUMN marker TEXT");
    let partial = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET marker = 'a' WHERE id = 1",
        &[],
    );
    assert_eq!(
        accept_deferred_update(&mut db, &mut transaction, &partial, &[], &mut phase).unwrap(),
        1
    );
    assert!(matches!(
        validate_and_set_new_not_null(&mut db, &mut transaction, ColumnId(5), phase),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(5))
        ))
    ));
    assert!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .overlay
            .schema
            .table("users")
            .unwrap()
            .column("marker")
            .unwrap()
            .nullable
    );
    for (sql, affected) in [
        ("UPDATE users SET marker = 'b' WHERE id = 1", 1),
        ("UPDATE users SET marker = 'rest' WHERE NOT(id = 1)", 2),
    ] {
        let prepared = prepared_update(&db, &transaction, sql, &[]);
        assert_eq!(
            accept_deferred_update(&mut db, &mut transaction, &prepared, &[], &mut phase).unwrap(),
            affected
        );
    }
    validate_and_set_new_not_null(&mut db, &mut transaction, ColumnId(5), phase).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        db.query("SELECT id, marker FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("b".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("rest".into())],
            vec![ScalarValue::Int64(4), ScalarValue::Text("rest".into())],
        ]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_table_set_not_null_is_vacuous_and_null_predicates_match_only_true() {
    let empty_root = root("empty-and-three-valued");
    let mut db = seed(&empty_root);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "DELETE FROM users")
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE users ADD COLUMN marker TEXT")
        .unwrap();
    let mut phase = AuditPhase::Refining;
    let prepared = prepared_update(
        &db,
        &transaction,
        "UPDATE users SET marker = 'unused' WHERE flag = true",
        &[],
    );
    assert_eq!(
        accept_deferred_update(&mut db, &mut transaction, &prepared, &[], &mut phase).unwrap(),
        0
    );
    validate_and_set_new_not_null(&mut db, &mut transaction, ColumnId(5), phase).unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    assert!(
        db.query("SELECT marker FROM users")
            .unwrap()
            .rows
            .is_empty()
    );
    db.close().unwrap();

    let second = root("three-valued");
    let mut db = seed(&second);
    let (mut transaction, mut phase, _, _) =
        adopt(&mut db, &second, "ALTER TABLE users ADD COLUMN marker TEXT");
    for (sql, expected) in [
        ("UPDATE users SET marker = 't' WHERE flag = true", 2),
        ("UPDATE users SET marker = 'f' WHERE flag = false", 0),
        ("UPDATE users SET marker = 'n' WHERE flag IS NULL", 1),
    ] {
        let prepared = prepared_update(&db, &transaction, sql, &[]);
        assert_eq!(
            accept_deferred_update(&mut db, &mut transaction, &prepared, &[], &mut phase).unwrap(),
            expected
        );
    }
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(
        db.query("SELECT id, marker FROM users ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("t".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("n".into())],
            vec![ScalarValue::Int64(4), ScalarValue::Text("t".into())],
        ]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(empty_root).unwrap();
    std::fs::remove_dir_all(second).unwrap();
}

#[test]
fn execute_finalization_mismatch_is_corrupt_and_program_digest_is_canonical() {
    let first_root = root("digest-a");
    let mut first_db = seed(&first_root);
    let (mut first_transaction, mut first_phase, _, _) = adopt(
        &mut first_db,
        &first_root,
        "ALTER TABLE users ADD COLUMN marker TEXT",
    );
    let first = prepared_update(
        &first_db,
        &first_transaction,
        "UPDATE users SET marker = $1 WHERE id = 1",
        &[Some(PhysicalType::Text)],
    );
    accept_deferred_update(
        &mut first_db,
        &mut first_transaction,
        &first,
        &[ScalarValue::Text("bound".into())],
        &mut first_phase,
    )
    .unwrap();
    let digest_a = first_transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill
        .actions[0]
        .semantic_digest;
    let different = prepared_update(
        &first_db,
        &first_transaction,
        "UPDATE users SET marker = $1 WHERE id = 1",
        &[Some(PhysicalType::Text)],
    );
    let mut other_phase = first_phase;
    accept_deferred_update(
        &mut first_db,
        &mut first_transaction,
        &different,
        &[ScalarValue::Text("other".into())],
        &mut other_phase,
    )
    .unwrap();
    let digest_b = first_transaction
        .schema_composition
        .plan()
        .unwrap()
        .deferred_backfill
        .actions[1]
        .semantic_digest;
    assert_ne!(digest_a, digest_b);

    first_transaction
        .schema_composition
        .adopted_source_mut()
        .unwrap()
        .logical
        .deferred_backfill
        .actions[0]
        .expected
        .result_digest[0] ^= 1;
    assert!(matches!(
        first_db.finalize_adopted_source(&mut first_transaction),
        Err(DatabaseError::SchemaMutation(SchemaMutationError::Corrupt(
            "deferred backfill Execute/finalization mismatch"
        )))
    ));
    first_transaction.rollback().unwrap();
    first_db.close().unwrap();
    std::fs::remove_dir_all(first_root).unwrap();
}

#[test]
fn alternative_candidates_have_larger_or_weaker_authority_costs() {
    #[derive(Clone)]
    struct HypotheticalDefault {
        column: ColumnDef,
        value: ScalarValue,
    }
    fn hypothetical_default_digest(value: &HypotheticalDefault) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"hypothetical canonical ColumnDef v2\0");
        hash.update(value.column.id.0.to_le_bytes());
        put_bytes(&mut hash, value.column.name.as_bytes()).unwrap();
        put_physical(&mut hash, value.column.semantic_type().physical);
        hash.update([u8::from(value.column.nullable)]);
        put_scalar(&mut hash, &value.value);
        hash.finalize().into()
    }

    let column = ColumnDef::new(
        ColumnId(5),
        "marker",
        TypeSpec::Physical(PhysicalType::Text),
    )
    .nullable(true);
    let null_default = HypotheticalDefault {
        column: column.clone(),
        value: ScalarValue::Null,
    };
    let text_default = HypotheticalDefault {
        column,
        value: ScalarValue::Text("value".into()),
    };
    assert_ne!(
        hypothetical_default_digest(&null_default),
        hypothetical_default_digest(&text_default)
    );

    let sidecar_entry_floor = std::mem::size_of::<netbadb_storage::StorageRowHandle>()
        + std::mem::size_of::<ScalarValue>();
    let estimated_rows = 10_000_usize;
    let estimated_sidecar_floor = estimated_rows.checked_mul(sidecar_entry_floor).unwrap();
    assert!(estimated_sidecar_floor > estimated_rows);

    let root = root("candidate-cost");
    let mut db = seed(&root);
    let (mut transaction, mut phase, source, _) =
        adopt(&mut db, &root, "ALTER TABLE users ADD COLUMN marker TEXT");
    let sidecar_rows = source_rows_with_handles(
        &mut db,
        &mut transaction,
        source,
        &[ColumnId(1), ColumnId(2)],
    )
    .unwrap();
    let sidecar = sidecar_rows
        .iter()
        .map(|(handle, values)| (*handle, values[1].clone()))
        .collect::<HashMap<_, _>>();
    assert_eq!(sidecar.len(), 3);
    let mut visible_ids = sidecar_rows
        .iter()
        .map(|(_, values)| values[0].clone())
        .collect::<Vec<_>>();
    visible_ids.sort_by_key(|value| match value {
        ScalarValue::Int64(value) => *value,
        _ => i64::MAX,
    });
    assert_eq!(
        visible_ids,
        vec![
            ScalarValue::Int64(1),
            ScalarValue::Int64(3),
            ScalarValue::Int64(4),
        ]
    );
    assert!(sidecar.keys().all(|handle| handle.storage_id() == source));
    let rescanned = source_rows_with_handles(
        &mut db,
        &mut transaction,
        source,
        &[ColumnId(1), ColumnId(2)],
    )
    .unwrap();
    assert!(
        rescanned.iter().all(|(handle, values)| {
            sidecar.get(handle).is_some_and(|value| value == &values[1])
        })
    );
    let observed_sidecar_floor = sidecar.len().checked_mul(sidecar_entry_floor).unwrap();
    assert_eq!(observed_sidecar_floor, 3 * sidecar_entry_floor);
    let before = db.next_storage_id().unwrap();
    let fill = prepared_update(&db, &transaction, "UPDATE users SET marker = legacy", &[]);
    assert_eq!(
        accept_deferred_update(&mut db, &mut transaction, &fill, &[], &mut phase).unwrap(),
        3
    );
    assert_eq!(db.next_storage_id(), Some(before));
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .deferred_backfill
            .actions
            .len(),
        1
    );
    let (_, base, _, _, projection) = adopted_parts(&transaction).unwrap();
    let rows = source_rows(&mut db, &mut transaction, source, &base).unwrap();
    assert!(matches!(
        projection.project(&rows[0]).unwrap().last(),
        Some(ScalarValue::Null)
    ));
    assert!(matches!(
        validate_and_set_new_not_null(&mut db, &mut transaction, ColumnId(5), phase),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(5))
        ))
    ));
    transaction.rollback().unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn proposed_phase_policy_freezes_structure_but_allows_repair_before_indexes() {
    assert!(AuditPhase::Refining.structural_alter_allowed());
    assert!(AuditPhase::Refining.deferred_update_allowed());
    assert!(!AuditPhase::Refining.projected_nullability_allowed());
    assert!(AuditPhase::Refining.final_index_allowed());

    assert!(!AuditPhase::Backfilling.structural_alter_allowed());
    assert!(AuditPhase::Backfilling.deferred_update_allowed());
    assert!(AuditPhase::Backfilling.projected_nullability_allowed());
    assert!(AuditPhase::Backfilling.final_index_allowed());

    assert!(!AuditPhase::IndexFinalizing.structural_alter_allowed());
    assert!(!AuditPhase::IndexFinalizing.deferred_update_allowed());
    assert!(!AuditPhase::IndexFinalizing.projected_nullability_allowed());
    assert!(!AuditPhase::IndexFinalizing.final_index_allowed());
    assert_eq!(MAX_DEFERRED_ACTIONS, 32);
    assert_eq!(MAX_ASSIGNMENTS_PER_ACTION, 32);
    assert_eq!(MAX_EXPRESSION_NODES, 256);
    assert_eq!(MAX_EXPRESSION_DEPTH, 32);
}

#[test]
fn round49_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND49_CRASH_ROOT") else {
        return;
    };
    let path = std::path::Path::new(&root);
    let mut db = Database::open_catalog(path.join("catalog")).unwrap();
    let (mut transaction, mut phase, _, _) =
        adopt(&mut db, path, "ALTER TABLE users ADD COLUMN marker TEXT");
    let fill = prepared_update(&db, &transaction, "UPDATE users SET marker = 'filled'", &[]);
    accept_deferred_update(&mut db, &mut transaction, &fill, &[], &mut phase).unwrap();
    validate_and_set_new_not_null(&mut db, &mut transaction, ColumnId(5), phase).unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE INDEX users_marker_idx ON users(marker)",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    panic!("configured Round 49 crash hook was not reached");
}

fn assert_no_stage(path: &std::path::Path) {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        assert!(
            !entry.file_name().to_string_lossy().contains(".stage"),
            "{:?}",
            entry.path()
        );
        if entry.file_type().unwrap().is_dir() {
            assert_no_stage(&entry.path());
        }
    }
}

#[test]
fn crash_matrix_loses_before_cord_and_recovers_projected_winners_without_program_replay() {
    let cases = [
        ("deferred-backfill-accepted", false, false, false),
        ("composition-intent-durable", false, false, false),
        ("source-backfill-intent-durable", false, false, false),
        ("source-backfill-stage-intent-durable", false, false, false),
        ("source-backfill-mid-copy", false, false, false),
        ("source-backfill-final-indexes-built", false, false, false),
        ("after-prepare-1", true, false, false),
        ("after-all-prepares", true, false, false),
        ("after-durable-decision", true, true, false),
        ("after-commit-1", true, true, false),
        ("after-commit-1", true, true, true),
        ("after-all-commits", true, true, false),
    ];
    for (point, coordinator, winner, reverse) in cases {
        let root = root(&format!("crash-{point}-{reverse}"));
        let db = seed(&root);
        let source = db.bindings.resolve_single(TableId(2)).unwrap();
        db.close().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "deferred_new_column_backfill_audit_tests::round49_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_ROUND49_CRASH_ROOT", &root);
        if reverse {
            command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
        }
        if coordinator {
            crate::coordinator_crash::configure_child(&mut command, point, &root, point);
        } else {
            command.env("NETBADB_BACKFILL_CRASH_POINT", point);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if coordinator { 87 } else { 90 }),
            "{point} {reverse}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            if winner {
                let table = reopened.schema().table("users").unwrap();
                let marker = table.column("marker").unwrap();
                assert!(!marker.nullable);
                assert_eq!(marker.id, ColumnId(5));
                assert_eq!(
                    reopened
                        .query("SELECT id, marker FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![ScalarValue::Int64(1), ScalarValue::Text("filled".into())],
                        vec![ScalarValue::Int64(3), ScalarValue::Text("filled".into())],
                        vec![ScalarValue::Int64(4), ScalarValue::Text("filled".into())],
                    ]
                );
                let target = reopened.bindings.resolve_single(TableId(2)).unwrap();
                assert_ne!(target, source);
                assert_physical_keys(
                    &mut reopened,
                    target,
                    ColumnId(5),
                    &ScalarValue::Text("filled".into()),
                    &[1, 3, 4],
                );
            } else {
                assert_eq!(reopened.bindings.resolve_single(TableId(2)), Ok(source));
                assert!(
                    reopened
                        .schema()
                        .table("users")
                        .unwrap()
                        .column("marker")
                        .is_none()
                );
                assert_eq!(
                    reopened
                        .query("SELECT id FROM users ORDER BY id")
                        .unwrap()
                        .rows,
                    vec![
                        vec![ScalarValue::Int64(1)],
                        vec![ScalarValue::Int64(2)],
                        vec![ScalarValue::Int64(3)],
                    ]
                );
            }
            reopened.close().unwrap();
        }
        assert_no_stage(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}
