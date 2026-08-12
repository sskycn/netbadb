//! Synchronous execution of typed query and DML physical statements.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use netbadb_planner::{PhysicalPlan, PhysicalStatement};
use netbadb_rel::{Assignment, BinaryOp, ColumnRef, Expr, ExprKind, UnaryOp};
use netbadb_storage::{HeapStorage, StorageError, Transaction};
use netbadb_types::{RowId, ScalarValue, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<ColumnRef>,
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
    TableMismatch { planned: TableId, storage: TableId },
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
    let result = execute_rows(plan, storage)?;
    Ok(QueryResult {
        columns: result.columns,
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
            let input = execute_rows(input, storage)?;
            let replacements = build_replacements(&input, assignments)?;
            let affected = u64::try_from(replacements.len())
                .map_err(|_| ExecutionError::AffectedRowsOverflow)?;
            for (row_id, values) in replacements {
                storage.update_in(transaction, row_id, &values)?;
            }
            Ok(ExecutionResult::AffectedRows(affected))
        }
        PhysicalStatement::Delete { input, table_id } => {
            let transaction = transaction.ok_or(ExecutionError::TransactionRequired)?;
            storage.validate_transaction(transaction)?;
            ensure_table(*table_id, storage)?;
            let input = execute_rows(input, storage)?;
            let affected = u64::try_from(input.rows.len())
                .map_err(|_| ExecutionError::AffectedRowsOverflow)?;
            for row in input.rows {
                storage.delete_in(transaction, row.row_id)?;
            }
            Ok(ExecutionResult::AffectedRows(affected))
        }
    }
}

#[derive(Debug)]
struct ExecutionRow {
    row_id: RowId,
    values: Vec<ScalarValue>,
}

#[derive(Debug)]
struct ExecutionRows {
    columns: Vec<ColumnRef>,
    rows: Vec<ExecutionRow>,
}

fn execute_rows(
    plan: &PhysicalPlan,
    storage: &mut HeapStorage,
) -> Result<ExecutionRows, ExecutionError> {
    match plan {
        PhysicalPlan::SeqScan {
            table_id, columns, ..
        } => {
            ensure_table(*table_id, storage)?;
            let rows = storage
                .scan()?
                .into_iter()
                .map(|(row_id, values)| ExecutionRow { row_id, values })
                .collect();
            Ok(ExecutionRows {
                columns: columns.clone(),
                rows,
            })
        }
        PhysicalPlan::Filter { input, predicate } => {
            let mut result = execute_rows(input, storage)?;
            let columns = result.columns.clone();
            result.rows = result
                .rows
                .into_iter()
                .filter_map(
                    |row| match evaluate_truth(predicate, &row.values, &columns) {
                        Ok(TruthValue::True) => Some(Ok(row)),
                        Ok(TruthValue::False | TruthValue::Unknown) => None,
                        Err(error) => Some(Err(error)),
                    },
                )
                .collect::<Result<Vec<_>, _>>()?;
            Ok(result)
        }
        PhysicalPlan::Project { input, columns } => {
            let input_result = execute_rows(input, storage)?;
            let positions = columns
                .iter()
                .map(|column| {
                    input_result
                        .columns
                        .iter()
                        .position(|candidate| {
                            candidate.table_id == column.table_id
                                && candidate.column_id == column.column_id
                        })
                        .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))
                })
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
                columns: columns.clone(),
                rows,
            })
        }
        PhysicalPlan::Limit { input, limit } => {
            let mut result = execute_rows(input, storage)?;
            let limit = usize::try_from(*limit).unwrap_or(usize::MAX);
            result.rows.truncate(limit);
            Ok(result)
        }
    }
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
                    let position = input
                        .columns
                        .iter()
                        .position(|column| {
                            column.table_id == assignment.column.table_id
                                && column.column_id == assignment.column.column_id
                        })
                        .ok_or_else(|| {
                            ExecutionError::MissingColumn(assignment.column.name.clone())
                        })?;
                    let value = evaluate(&assignment.value, &row.values, &input.columns)?;
                    Ok((position, value))
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?;
            let mut replacement = row.values.clone();
            for (position, value) in evaluated {
                replacement[position] = value;
            }
            Ok((row.row_id, replacement))
        })
        .collect()
}

fn evaluate(
    expression: &Expr,
    row: &[ScalarValue],
    columns: &[ColumnRef],
) -> Result<ScalarValue, ExecutionError> {
    match &expression.kind {
        ExprKind::Column(column) => {
            let position = columns
                .iter()
                .position(|candidate| {
                    candidate.table_id == column.table_id && candidate.column_id == column.column_id
                })
                .ok_or_else(|| ExecutionError::MissingColumn(column.name.clone()))?;
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
            let left = evaluate(left, row, columns)?;
            let right = evaluate(right, row, columns)?;
            evaluate_binary(*operator, left, right)
        }
        ExprKind::Unary {
            operator: UnaryOp::Not,
            expression,
        } => Ok(evaluate_truth(expression, row, columns)?
            .not()
            .into_scalar()),
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            let is_null = matches!(evaluate(expression, row, columns)?, ScalarValue::Null);
            Ok(ScalarValue::Bool(if *negated { !is_null } else { is_null }))
        }
    }
}

fn evaluate_truth(
    expression: &Expr,
    row: &[ScalarValue],
    columns: &[ColumnRef],
) -> Result<TruthValue, ExecutionError> {
    TruthValue::from_scalar(evaluate(expression, row, columns)?)
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
    use super::{QueryResult, TruthValue, evaluate_binary, execute};
    use netbadb_planner::plan;
    use netbadb_rel::{BinaryOp, ColumnRef, Expr, ExprKind, LogicalPlan};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_storage::HeapStorage;
    use netbadb_types::{ColumnId, ExprType, PhysicalType, ScalarValue, SemanticType, TableId};

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
            table_id: TableId(1),
            column_id: ColumnId(1),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let name = ColumnRef {
            table_id: TableId(1),
            column_id: ColumnId(2),
            name: "name".into(),
            data_type: SemanticType::physical(PhysicalType::Text),
            nullable: false,
        };
        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Scan {
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
}
