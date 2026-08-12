//! Synchronous execution of the initial physical plan subset.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use netbadb_planner::PhysicalPlan;
use netbadb_rel::{BinaryOp, ColumnRef, Expr};
use netbadb_storage::{HeapStorage, StorageError};
use netbadb_types::ScalarValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<ColumnRef>,
    pub rows: Vec<Vec<ScalarValue>>,
}

#[derive(Debug)]
pub enum ExecutionError {
    Storage(StorageError),
    MissingColumn(String),
    ExpectedBoolean,
    TypeMismatch,
    UnsupportedNull,
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
            Self::UnsupportedNull => {
                formatter.write_str("NULL evaluation is not implemented in the initial slice")
            }
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
    match plan {
        PhysicalPlan::SeqScan { columns, .. } => {
            let rows = storage.scan()?.into_iter().map(|(_, row)| row).collect();
            Ok(QueryResult {
                columns: columns.clone(),
                rows,
            })
        }
        PhysicalPlan::Filter { input, predicate } => {
            let mut result = execute(input, storage)?;
            let columns = result.columns.clone();
            result.rows = result
                .rows
                .into_iter()
                .filter_map(|row| match evaluate(predicate, &row, &columns) {
                    Ok(ScalarValue::Bool(true)) => Some(Ok(row)),
                    Ok(ScalarValue::Bool(false)) => None,
                    Ok(_) => Some(Err(ExecutionError::ExpectedBoolean)),
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(result)
        }
        PhysicalPlan::Project { input, columns } => {
            let input_result = execute(input, storage)?;
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
                    positions
                        .iter()
                        .map(|position| row[*position].clone())
                        .collect()
                })
                .collect();
            Ok(QueryResult {
                columns: columns.clone(),
                rows,
            })
        }
        PhysicalPlan::Limit { input, limit } => {
            let mut result = execute(input, storage)?;
            let limit = usize::try_from(*limit).unwrap_or(usize::MAX);
            result.rows.truncate(limit);
            Ok(result)
        }
    }
}

fn evaluate(
    expression: &Expr,
    row: &[ScalarValue],
    columns: &[ColumnRef],
) -> Result<ScalarValue, ExecutionError> {
    match expression {
        Expr::Column(column) => {
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
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Binary {
            operator,
            left,
            right,
        } => {
            let left = evaluate(left, row, columns)?;
            let right = evaluate(right, row, columns)?;
            evaluate_binary(*operator, left, right)
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
            let (ScalarValue::Bool(left), ScalarValue::Bool(right)) = (left, right) else {
                return Err(ExecutionError::ExpectedBoolean);
            };
            let value = if operator == BinaryOp::And {
                left && right
            } else {
                left || right
            };
            Ok(ScalarValue::Bool(value))
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => {
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
        (ScalarValue::Null, _) | (_, ScalarValue::Null) => Err(ExecutionError::UnsupportedNull),
        _ => Err(ExecutionError::TypeMismatch),
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryResult, execute};
    use netbadb_planner::plan;
    use netbadb_rel::{BinaryOp, ColumnRef, Expr, LogicalPlan};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_storage::HeapStorage;
    use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};

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
        };
        let name = ColumnRef {
            table_id: TableId(1),
            column_id: ColumnId(2),
            name: "name".into(),
            data_type: SemanticType::physical(PhysicalType::Text),
        };
        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Scan {
                        table_id: TableId(1),
                        table_name: "users".into(),
                        columns: vec![id.clone(), name.clone()],
                    }),
                    predicate: Expr::Binary {
                        operator: BinaryOp::Gt,
                        left: Box::new(Expr::Column(id)),
                        right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
                    },
                }),
                columns: vec![name],
            }),
            limit: 1,
        };
        let result: QueryResult = execute(&plan(&logical), &mut storage).expect("execute");
        assert_eq!(result.rows, vec![vec![ScalarValue::Text("Lin".into())]]);
        let _ = std::fs::remove_file(path);
    }
}
