//! Canonical Schema IR. It deliberately contains no Rust- or Go-specific data.

use std::error::Error;
use std::fmt;

use netbadb_types::{ColumnId, PhysicalType, SemanticType, TableId};
use sha2::{Digest, Sha256};

/// Version of the explicit canonical table-schema encoding.
pub const CANONICAL_TABLE_SCHEMA_VERSION: u16 = 1;
const CANONICAL_TABLE_SCHEMA_MAGIC: &[u8; 4] = b"NBTS";

/// Stable SHA-256 identity of one validated canonical table definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaFingerprint([u8; 32]);

impl SchemaFingerprint {
    pub const LENGTH: usize = 32;

    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::LENGTH]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

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

    #[must_use]
    pub fn column_by_id(&self, id: ColumnId) -> Option<&ColumnDef> {
        self.columns.iter().find(|column| column.id == id)
    }

    /// Validates all invariants local to one canonical table definition.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.name.is_empty() {
            return Err(SchemaError::EmptyTableName);
        }
        canonical_string_length(&self.name, "table name")?;
        if self.columns.len() > u32::MAX as usize {
            return Err(SchemaError::TooManyColumns {
                table: self.name.clone(),
                count: self.columns.len(),
            });
        }
        for (index, column) in self.columns.iter().enumerate() {
            if column.name.is_empty() {
                return Err(SchemaError::EmptyColumnName {
                    table: self.name.clone(),
                });
            }
            canonical_string_length(&column.name, "column name")?;
            if let TypeSpec::Semantic { name, .. } = &column.type_spec {
                if name.is_empty() {
                    return Err(SchemaError::InvalidSemanticType {
                        table: self.name.clone(),
                        column: column.name.clone(),
                        name: name.clone(),
                    });
                }
                canonical_string_length(name, "semantic type name")?;
            }
            for other in &self.columns[..index] {
                if column.id == other.id {
                    return Err(SchemaError::DuplicateColumnId {
                        table: self.name.clone(),
                        column_id: column.id,
                    });
                }
                if column.name == other.name {
                    return Err(SchemaError::DuplicateColumnName {
                        table: self.name.clone(),
                        name: column.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Encodes this table using the versioned, language-independent schema
    /// identity format. All integers are little-endian and all strings are
    /// UTF-8 with an explicit `u32` byte length.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, SchemaError> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CANONICAL_TABLE_SCHEMA_MAGIC);
        bytes.extend_from_slice(&CANONICAL_TABLE_SCHEMA_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&self.id.0.to_le_bytes());
        push_string(&mut bytes, &self.name, "table name")?;
        bytes.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for column in &self.columns {
            bytes.extend_from_slice(&column.id.0.to_le_bytes());
            push_string(&mut bytes, &column.name, "column name")?;
            let (physical, semantic_name) = match &column.type_spec {
                TypeSpec::Physical(physical) => (*physical, None),
                TypeSpec::Semantic { name, physical } => (*physical, Some(name.as_str())),
            };
            bytes.push(physical_type_tag(physical));
            match semantic_name {
                None => bytes.push(0),
                Some(name) => {
                    bytes.push(1);
                    push_string(&mut bytes, name, "semantic type name")?;
                }
            }
            bytes.push(u8::from(column.nullable));
            bytes.push(u8::from(column.primary_key));
        }
        Ok(bytes)
    }

    /// Computes the stable SHA-256 identity of [`Self::canonical_bytes`].
    pub fn fingerprint(&self) -> Result<SchemaFingerprint, SchemaError> {
        let digest = Sha256::digest(self.canonical_bytes()?);
        Ok(SchemaFingerprint::from_bytes(digest.into()))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    tables: Vec<TableDef>,
}

impl Schema {
    /// Constructs a schema after validating every canonical invariant.
    pub fn new(tables: Vec<TableDef>) -> Result<Self, SchemaError> {
        let schema = Self { tables };
        schema.validate()?;
        Ok(schema)
    }

    /// Validates table-local invariants and schema-wide table identity.
    pub fn validate(&self) -> Result<(), SchemaError> {
        for (index, table) in self.tables.iter().enumerate() {
            table.validate()?;
            for other in &self.tables[..index] {
                if table.id == other.id {
                    return Err(SchemaError::DuplicateTableId { table_id: table.id });
                }
                if table.name == other.name {
                    return Err(SchemaError::DuplicateTableName {
                        name: table.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn add_table(&mut self, table: TableDef) -> Result<(), SchemaError> {
        self.validate()?;
        table.validate()?;
        if self.tables.iter().any(|existing| existing.id == table.id) {
            return Err(SchemaError::DuplicateTableId { table_id: table.id });
        }
        if self
            .tables
            .iter()
            .any(|existing| existing.name == table.name)
        {
            return Err(SchemaError::DuplicateTableName { name: table.name });
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
    DuplicateTableId {
        table_id: TableId,
    },
    DuplicateTableName {
        name: String,
    },
    DuplicateColumnId {
        table: String,
        column_id: ColumnId,
    },
    DuplicateColumnName {
        table: String,
        name: String,
    },
    EmptyTableName,
    EmptyColumnName {
        table: String,
    },
    InvalidSemanticType {
        table: String,
        column: String,
        name: String,
    },
    TooManyColumns {
        table: String,
        count: usize,
    },
    CanonicalStringTooLong {
        field: &'static str,
        length: usize,
    },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateTableId { table_id } => {
                write!(
                    formatter,
                    "table ID {} is defined more than once",
                    table_id.0
                )
            }
            Self::DuplicateTableName { name } => {
                write!(formatter, "table name `{name}` is defined more than once")
            }
            Self::DuplicateColumnId { table, column_id } => write!(
                formatter,
                "column ID {} is defined more than once in table `{table}`",
                column_id.0
            ),
            Self::DuplicateColumnName { table, name } => write!(
                formatter,
                "column name `{name}` is defined more than once in table `{table}`"
            ),
            Self::EmptyTableName => formatter.write_str("table name must not be empty"),
            Self::EmptyColumnName { table } => {
                write!(
                    formatter,
                    "column name in table `{table}` must not be empty"
                )
            }
            Self::InvalidSemanticType {
                table,
                column,
                name,
            } => write!(
                formatter,
                "semantic type name `{name}` for `{table}.{column}` must not be empty"
            ),
            Self::TooManyColumns { table, count } => {
                write!(
                    formatter,
                    "table `{table}` has {count} columns; maximum is {}",
                    u32::MAX
                )
            }
            Self::CanonicalStringTooLong { field, length } => write!(
                formatter,
                "{field} is {length} bytes; canonical encoding supports at most {} bytes",
                u32::MAX
            ),
        }
    }
}

impl Error for SchemaError {}

fn push_string(output: &mut Vec<u8>, value: &str, field: &'static str) -> Result<(), SchemaError> {
    let length = canonical_string_length(value, field)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn canonical_string_length(value: &str, field: &'static str) -> Result<u32, SchemaError> {
    u32::try_from(value.len()).map_err(|_| SchemaError::CanonicalStringTooLong {
        field,
        length: value.len(),
    })
}

const fn physical_type_tag(physical: PhysicalType) -> u8 {
    match physical {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnDef, Schema, SchemaError, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    fn user_table() -> TableDef {
        TableDef::new(
            TableId(7),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::UInt64,
                    },
                )
                .primary_key(true),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
            ],
        )
    }

    #[test]
    fn validates_schema_and_rejects_duplicate_table_identity() {
        Schema::new(vec![user_table()]).expect("valid schema");
        let mut same_id = user_table();
        same_id.name = "other".into();
        assert!(matches!(
            Schema::new(vec![user_table(), same_id]),
            Err(SchemaError::DuplicateTableId {
                table_id: TableId(7)
            })
        ));
        let mut same_name = user_table();
        same_name.id = TableId(8);
        assert!(matches!(
            Schema::new(vec![user_table(), same_name]),
            Err(SchemaError::DuplicateTableName { name }) if name == "users"
        ));
    }

    #[test]
    fn rejects_duplicate_column_identity_and_empty_names() {
        let mut duplicate_id = user_table();
        duplicate_id.columns[1].id = ColumnId(1);
        assert!(matches!(
            duplicate_id.validate(),
            Err(SchemaError::DuplicateColumnId {
                column_id: ColumnId(1),
                ..
            })
        ));
        let mut duplicate_name = user_table();
        duplicate_name.columns[1].name = "id".into();
        assert!(matches!(
            duplicate_name.validate(),
            Err(SchemaError::DuplicateColumnName { name, .. }) if name == "id"
        ));
        let mut empty_table_name = user_table();
        empty_table_name.name.clear();
        assert!(matches!(
            empty_table_name.validate(),
            Err(SchemaError::EmptyTableName)
        ));
        let mut empty_column_name = user_table();
        empty_column_name.columns[0].name.clear();
        assert!(matches!(
            empty_column_name.validate(),
            Err(SchemaError::EmptyColumnName { table }) if table == "users"
        ));
        let mut empty_semantic_name = user_table();
        if let TypeSpec::Semantic { name, .. } = &mut empty_semantic_name.columns[0].type_spec {
            name.clear();
        }
        assert!(matches!(
            empty_semantic_name.validate(),
            Err(SchemaError::InvalidSemanticType { .. })
        ));

        let mut nullable_primary_key = user_table();
        nullable_primary_key.columns[0].nullable = true;
        nullable_primary_key
            .validate()
            .expect("primary-key enforcement is not yet a schema invariant");
    }

    #[test]
    fn accepts_frontend_independent_names_and_preserves_case() {
        for name in ["order", "group", "team-members", "用户"] {
            TableDef::new(
                TableId(20),
                name,
                vec![ColumnDef::new(
                    ColumnId(1),
                    name,
                    TypeSpec::Physical(PhysicalType::UInt64),
                )],
            )
            .validate()
            .expect("canonical names are independent of SQL spelling rules");
        }

        let schema = Schema::new(vec![
            TableDef::new(
                TableId(21),
                "users",
                vec![
                    ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::UInt64)),
                    ColumnDef::new(ColumnId(2), "Id", TypeSpec::Physical(PhysicalType::UInt64)),
                ],
            ),
            TableDef::new(TableId(22), "Users", Vec::new()),
        ])
        .expect("canonical name identity is case-sensitive");
        assert!(schema.table("users").is_some());
        assert!(schema.table("Users").is_some());
    }

    #[test]
    fn zero_column_tables_are_valid_and_have_a_stable_identity() {
        let table = TableDef::new(TableId(9), "events", Vec::new());
        table.validate().expect("zero-column table is supported");
        assert_eq!(table.fingerprint(), table.fingerprint());
    }

    #[test]
    fn canonical_encoding_is_explicit_and_versioned() {
        let table = user_table();
        let expected = [
            b"NBTS".as_slice(),
            &1_u16.to_le_bytes(),
            &0_u16.to_le_bytes(),
            &7_u64.to_le_bytes(),
            &5_u32.to_le_bytes(),
            b"users".as_slice(),
            &2_u32.to_le_bytes(),
            &1_u32.to_le_bytes(),
            &2_u32.to_le_bytes(),
            b"id".as_slice(),
            &[3, 1],
            &6_u32.to_le_bytes(),
            b"UserId".as_slice(),
            &[0, 1],
            &2_u32.to_le_bytes(),
            &4_u32.to_le_bytes(),
            b"name".as_slice(),
            &[4, 0, 1, 0],
        ]
        .concat();
        assert_eq!(table.canonical_bytes().expect("canonical bytes"), expected);
        assert_eq!(
            table.fingerprint().expect("fingerprint").to_string(),
            "823e72558862af9f9520020c872b4ebbdc5f63b9a93a4c460f849b935493f7c4"
        );
    }

    #[test]
    fn frontend_independent_names_have_deterministic_identity() {
        let table = TableDef::new(
            TableId(23),
            "用户",
            vec![ColumnDef::new(
                ColumnId(1),
                "用户-id",
                TypeSpec::Physical(PhysicalType::UInt64),
            )],
        );

        assert_eq!(
            table.canonical_bytes().expect("first canonical encoding"),
            table.canonical_bytes().expect("second canonical encoding")
        );
        assert_eq!(
            table.fingerprint().expect("first fingerprint"),
            table.fingerprint().expect("second fingerprint")
        );
    }

    #[test]
    fn fingerprint_changes_for_every_schema_identity_field() {
        let baseline = user_table();
        let baseline_fingerprint = baseline.fingerprint().expect("baseline fingerprint");
        let mut variants = Vec::new();

        let mut table_id = baseline.clone();
        table_id.id = TableId(8);
        variants.push(table_id);
        let mut table_name = baseline.clone();
        table_name.name = "members".into();
        variants.push(table_name);
        let mut column_order = baseline.clone();
        column_order.columns.swap(0, 1);
        variants.push(column_order);
        let mut column_id = baseline.clone();
        column_id.columns[0].id = ColumnId(3);
        variants.push(column_id);
        let mut column_name = baseline.clone();
        column_name.columns[0].name = "user_id".into();
        variants.push(column_name);
        let mut physical_type = baseline.clone();
        physical_type.columns[0].type_spec = TypeSpec::Semantic {
            name: "UserId".into(),
            physical: PhysicalType::Int64,
        };
        variants.push(physical_type);
        let mut semantic_type = baseline.clone();
        semantic_type.columns[0].type_spec = TypeSpec::Semantic {
            name: "TeamId".into(),
            physical: PhysicalType::UInt64,
        };
        variants.push(semantic_type);
        let mut nullable = baseline.clone();
        nullable.columns[0].nullable = true;
        variants.push(nullable);
        let mut primary_key = baseline.clone();
        primary_key.columns[0].primary_key = false;
        variants.push(primary_key);

        for variant in variants {
            assert_ne!(
                variant.fingerprint().expect("variant fingerprint"),
                baseline_fingerprint
            );
        }
    }

    #[test]
    fn schema_preserves_nominal_column_types() {
        let schema = Schema::new(vec![user_table()]).expect("valid schema");
        assert_eq!(
            schema.table("users").expect("table exists").columns[0]
                .semantic_type()
                .name
                .as_deref(),
            Some("UserId")
        );
    }
}
