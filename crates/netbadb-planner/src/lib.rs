//! Physical planning kept separate from logical relational meaning.

use netbadb_rel::{ColumnRef, Expr};
use netbadb_types::TableId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    SeqScan {
        table_id: TableId,
        table_name: String,
        columns: Vec<ColumnRef>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ColumnRef>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: u64,
    },
}

impl PhysicalPlan {
    #[must_use]
    pub fn output_columns(&self) -> &[ColumnRef] {
        match self {
            Self::SeqScan { columns, .. } | Self::Project { columns, .. } => columns,
            Self::Filter { input, .. } | Self::Limit { input, .. } => input.output_columns(),
        }
    }
}

#[must_use]
pub fn plan(logical: &netbadb_rel::LogicalPlan) -> PhysicalPlan {
    match logical {
        netbadb_rel::LogicalPlan::Scan {
            table_id,
            table_name,
            columns,
        } => PhysicalPlan::SeqScan {
            table_id: *table_id,
            table_name: table_name.clone(),
            columns: columns.clone(),
        },
        netbadb_rel::LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(plan(input)),
            predicate: predicate.clone(),
        },
        netbadb_rel::LogicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(plan(input)),
            columns: columns.clone(),
        },
        netbadb_rel::LogicalPlan::Limit { input, limit } => PhysicalPlan::Limit {
            input: Box::new(plan(input)),
            limit: *limit,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalPlan, plan};
    use netbadb_rel::ColumnRef;
    use netbadb_rel::LogicalPlan;
    use netbadb_types::{ColumnId, PhysicalType, SemanticType, TableId};

    #[test]
    fn creates_a_sequence_scan_physical_plan() {
        let column = ColumnRef {
            table_id: TableId(1),
            column_id: ColumnId(1),
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::Int64),
            nullable: false,
        };
        let logical = LogicalPlan::Scan {
            table_id: TableId(1),
            table_name: "users".into(),
            columns: vec![column],
        };
        assert!(matches!(plan(&logical), PhysicalPlan::SeqScan { .. }));
    }
}
