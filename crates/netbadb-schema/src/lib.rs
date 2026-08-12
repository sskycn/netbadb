//! Canonical Schema IR. It deliberately contains no Rust- or Go-specific data.

use std::error::Error;
use std::fmt;

use netbadb_types::{ColumnId, PhysicalType, SemanticType, TableId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeSpec {
    Physical(PhysicalType),
    Semantic {
        name: String,
        physical: PhysicalType,
    },
}

impl TypeSpec {
    #[must_use]
    pub fn semantic_type(&self) -> SemanticType {
        match self {
            Self::Physical(physical) => SemanticType::physical(*physical),
            Self::Semantic { name, physical } => SemanticType::named(name, *physical),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub type_spec: TypeSpec,
    pub nullable: bool,
    pub primary_key: bool,
}

impl ColumnDef {
    #[must_use]
    pub fn new(id: ColumnId, name: impl Into<String>, type_spec: TypeSpec) -> Self {
        Self {
            id,
            name: name.into(),
            type_spec,
            nullable: false,
            primary_key: false,
        }
    }

    #[must_use]
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    #[must_use]
    pub fn primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        self
    }

    #[must_use]
    pub fn semantic_type(&self) -> SemanticType {
        self.type_spec.semantic_type()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl TableDef {
    #[must_use]
    pub fn new(id: TableId, name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        Self {
            id,
            name: name.into(),
            columns,
        }
    }

    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|column| column.name == name)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    tables: Vec<TableDef>,
}

impl Schema {
    #[must_use]
    pub fn new(tables: Vec<TableDef>) -> Self {
        Self { tables }
    }

    pub fn add_table(&mut self, table: TableDef) -> Result<(), SchemaError> {
        if self
            .tables
            .iter()
            .any(|existing| existing.name == table.name)
        {
            return Err(SchemaError::DuplicateTable(table.name));
        }
        self.tables.push(table);
        Ok(())
    }

    #[must_use]
    pub fn table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|table| table.name == name)
    }

    #[must_use]
    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    DuplicateTable(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTable(name) => write!(formatter, "table `{name}` is already defined"),
        }
    }
}

impl Error for SchemaError {}

#[cfg(test)]
mod tests {
    use super::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    #[test]
    fn schema_preserves_nominal_column_types() {
        let users = TableDef::new(
            TableId(1),
            "users",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Semantic {
                    name: "UserId".into(),
                    physical: PhysicalType::UInt64,
                },
            )],
        );
        let schema = Schema::new(vec![users]);

        assert_eq!(
            schema.table("users").expect("table exists").columns[0]
                .semantic_type()
                .name
                .as_deref(),
            Some("UserId")
        );
    }
}
