use std::error::Error;
use std::fmt;

use netbadb_types::{ColumnId, ExprType, ParameterId, RelationBindingId, SemanticType, TableId};

use crate::{
    AggregateFunction, AggregateInput, AggregateOutput, BinaryOp, ColumnRef, Expr, ExprKind,
    JoinKind, LogicalPlan, NullOrder, ProjectedExpr, SortDirection, SortKey, UnaryOp,
};

/// Query-local relation identity canonicalized by first logical scan occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CanonicalBindingOrdinal(pub u32);

/// Stable resolved column meaning without source/display names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryColumnShape {
    pub binding: CanonicalBindingOrdinal,
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub data_type: SemanticType,
    pub nullable: bool,
}

/// A normalized literal retains type and NULL meaning, never scalar payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiteralShape {
    pub expr_type: ExprType,
    pub is_null: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryExpressionShape {
    pub expr_type: ExprType,
    pub kind: QueryExpressionShapeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryExpressionShapeKind {
    Column(QueryColumnShape),
    Literal(LiteralShape),
    Parameter(ParameterId),
    Cast {
        expression: Box<QueryExpressionShape>,
    },
    Binary {
        operator: BinaryOp,
        left: Box<QueryExpressionShape>,
        right: Box<QueryExpressionShape>,
    },
    Unary {
        operator: UnaryOp,
        expression: Box<QueryExpressionShape>,
    },
    IsNull {
        expression: Box<QueryExpressionShape>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuerySortKeyShape {
    pub column: QueryColumnShape,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryProjectedExpressionShape {
    pub expression: QueryExpressionShape,
    pub output_type: ExprType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateInputShape {
    All,
    Column(QueryColumnShape),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateOutputShape {
    GroupKey(QueryColumnShape),
    Aggregate {
        function: AggregateFunction,
        input: AggregateInputShape,
        output_type: ExprType,
    },
}

/// Collision-safe typed logical workload identity.
///
/// Names, aliases, SQL text, and ordinary literal payloads are absent. `Hash`
/// is only an indexing aid; structural equality remains semantic authority.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalQueryShape {
    OneRow,
    Scan {
        binding: CanonicalBindingOrdinal,
        table_id: TableId,
        columns: Vec<QueryColumnShape>,
    },
    Join {
        left: Box<LogicalQueryShape>,
        right: Box<LogicalQueryShape>,
        kind: JoinKind,
        predicate: QueryExpressionShape,
        columns: Vec<QueryColumnShape>,
    },
    Filter {
        input: Box<LogicalQueryShape>,
        predicate: QueryExpressionShape,
    },
    Sort {
        input: Box<LogicalQueryShape>,
        keys: Vec<QuerySortKeyShape>,
    },
    Project {
        input: Box<LogicalQueryShape>,
        columns: Vec<QueryColumnShape>,
    },
    ScalarProject {
        input: Box<LogicalQueryShape>,
        expressions: Vec<QueryProjectedExpressionShape>,
    },
    Aggregate {
        input: Box<LogicalQueryShape>,
        group_keys: Vec<QueryColumnShape>,
        outputs: Vec<AggregateOutputShape>,
    },
    Limit {
        input: Box<LogicalQueryShape>,
        limit: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryShapeError {
    TooManyRelationBindings,
    MissingRelationBinding(RelationBindingId),
}

impl fmt::Display for QueryShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyRelationBindings => {
                formatter.write_str("logical query shape exceeds the relation binding limit")
            }
            Self::MissingRelationBinding(binding) => write!(
                formatter,
                "logical query shape references unregistered relation binding {}",
                binding.0
            ),
        }
    }
}

impl Error for QueryShapeError {}

impl LogicalQueryShape {
    /// Derives a structural shape directly from typed logical IR.
    pub fn from_plan(plan: &LogicalPlan) -> Result<Self, QueryShapeError> {
        QueryShapeCanonicalizer::default().plan(plan)
    }
}

/// Shared first-occurrence binding canonicalizer used by logical and physical
/// structural identities.
#[derive(Default)]
pub struct QueryShapeCanonicalizer {
    bindings: Vec<(RelationBindingId, CanonicalBindingOrdinal)>,
}

impl QueryShapeCanonicalizer {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            bindings: Vec::new(),
        }
    }

    fn plan(&mut self, plan: &LogicalPlan) -> Result<LogicalQueryShape, QueryShapeError> {
        Ok(match plan {
            LogicalPlan::OneRow => LogicalQueryShape::OneRow,
            LogicalPlan::Scan {
                binding_id,
                table_id,
                columns,
                ..
            } => {
                let binding = self.register_binding(*binding_id)?;
                LogicalQueryShape::Scan {
                    binding,
                    table_id: *table_id,
                    columns: self.columns(columns)?,
                }
            }
            LogicalPlan::Join {
                left,
                right,
                kind,
                predicate,
                columns,
            } => {
                let left = self.plan(left)?;
                let right = self.plan(right)?;
                LogicalQueryShape::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    kind: *kind,
                    predicate: self.expression(predicate)?,
                    columns: self.columns(columns)?,
                }
            }
            LogicalPlan::Filter { input, predicate } => LogicalQueryShape::Filter {
                input: Box::new(self.plan(input)?),
                predicate: self.expression(predicate)?,
            },
            LogicalPlan::Sort { input, keys } => LogicalQueryShape::Sort {
                input: Box::new(self.plan(input)?),
                keys: keys
                    .iter()
                    .map(|key| self.sort_key(key))
                    .collect::<Result<_, _>>()?,
            },
            LogicalPlan::Project { input, columns } => LogicalQueryShape::Project {
                input: Box::new(self.plan(input)?),
                columns: self.columns(columns)?,
            },
            LogicalPlan::ScalarProject { input, expressions } => LogicalQueryShape::ScalarProject {
                input: Box::new(self.plan(input)?),
                expressions: expressions
                    .iter()
                    .map(|expression| self.projected_expression(expression))
                    .collect::<Result<_, _>>()?,
            },
            LogicalPlan::Aggregate {
                input,
                group_keys,
                outputs,
            } => LogicalQueryShape::Aggregate {
                input: Box::new(self.plan(input)?),
                group_keys: self.columns(group_keys)?,
                outputs: outputs
                    .iter()
                    .map(|output| self.aggregate_output(output))
                    .collect::<Result<_, _>>()?,
            },
            LogicalPlan::Limit { input, limit } => LogicalQueryShape::Limit {
                input: Box::new(self.plan(input)?),
                limit: *limit,
            },
        })
    }

    pub fn register_binding(
        &mut self,
        binding_id: RelationBindingId,
    ) -> Result<CanonicalBindingOrdinal, QueryShapeError> {
        if let Some((_, ordinal)) = self
            .bindings
            .iter()
            .find(|(candidate, _)| *candidate == binding_id)
        {
            return Ok(*ordinal);
        }
        let ordinal = u32::try_from(self.bindings.len())
            .map(CanonicalBindingOrdinal)
            .map_err(|_| QueryShapeError::TooManyRelationBindings)?;
        self.bindings.push((binding_id, ordinal));
        Ok(ordinal)
    }

    pub fn binding(
        &self,
        binding_id: RelationBindingId,
    ) -> Result<CanonicalBindingOrdinal, QueryShapeError> {
        self.bindings
            .iter()
            .find(|(candidate, _)| *candidate == binding_id)
            .map(|(_, ordinal)| *ordinal)
            .ok_or(QueryShapeError::MissingRelationBinding(binding_id))
    }

    pub fn column(&self, column: &ColumnRef) -> Result<QueryColumnShape, QueryShapeError> {
        Ok(QueryColumnShape {
            binding: self.binding(column.binding_id)?,
            table_id: column.table_id,
            column_id: column.column_id,
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        })
    }

    pub fn columns(&self, columns: &[ColumnRef]) -> Result<Vec<QueryColumnShape>, QueryShapeError> {
        columns.iter().map(|column| self.column(column)).collect()
    }

    pub fn expression(&self, expression: &Expr) -> Result<QueryExpressionShape, QueryShapeError> {
        let kind = match &expression.kind {
            ExprKind::Column(column) => QueryExpressionShapeKind::Column(self.column(column)?),
            ExprKind::Literal(value) => QueryExpressionShapeKind::Literal(LiteralShape {
                expr_type: expression.expr_type.clone(),
                is_null: matches!(value, netbadb_types::ScalarValue::Null),
            }),
            ExprKind::Parameter(parameter) => QueryExpressionShapeKind::Parameter(*parameter),
            ExprKind::Cast { expression } => QueryExpressionShapeKind::Cast {
                expression: Box::new(self.expression(expression)?),
            },
            ExprKind::Binary {
                operator,
                left,
                right,
            } => QueryExpressionShapeKind::Binary {
                operator: *operator,
                left: Box::new(self.expression(left)?),
                right: Box::new(self.expression(right)?),
            },
            ExprKind::Unary {
                operator,
                expression,
            } => QueryExpressionShapeKind::Unary {
                operator: *operator,
                expression: Box::new(self.expression(expression)?),
            },
            ExprKind::IsNull {
                expression,
                negated,
            } => QueryExpressionShapeKind::IsNull {
                expression: Box::new(self.expression(expression)?),
                negated: *negated,
            },
        };
        Ok(QueryExpressionShape {
            expr_type: expression.expr_type.clone(),
            kind,
        })
    }

    pub fn sort_key(&self, key: &SortKey) -> Result<QuerySortKeyShape, QueryShapeError> {
        Ok(QuerySortKeyShape {
            column: self.column(&key.column)?,
            direction: key.direction,
            null_order: key.null_order,
        })
    }

    pub fn projected_expression(
        &self,
        expression: &ProjectedExpr,
    ) -> Result<QueryProjectedExpressionShape, QueryShapeError> {
        Ok(QueryProjectedExpressionShape {
            expression: self.expression(&expression.expression)?,
            output_type: ExprType {
                data_type: expression.output.data_type.clone(),
                nullable: expression.output.nullable,
            },
        })
    }

    pub fn aggregate_input(
        &self,
        input: &AggregateInput,
    ) -> Result<AggregateInputShape, QueryShapeError> {
        Ok(match input {
            AggregateInput::All => AggregateInputShape::All,
            AggregateInput::Column(column) => AggregateInputShape::Column(self.column(column)?),
        })
    }

    pub fn aggregate_output(
        &self,
        output: &AggregateOutput,
    ) -> Result<AggregateOutputShape, QueryShapeError> {
        Ok(match output {
            AggregateOutput::GroupKey(column) => {
                AggregateOutputShape::GroupKey(self.column(column)?)
            }
            AggregateOutput::Aggregate(aggregate) => AggregateOutputShape::Aggregate {
                function: aggregate.function,
                input: self.aggregate_input(&aggregate.input)?,
                output_type: ExprType {
                    data_type: aggregate.output.data_type.clone(),
                    nullable: aggregate.output.nullable,
                },
            },
        })
    }
}
