//! Strict parsing of the language-neutral SDK Schema Spec v1 and v2 contracts.

use std::error::Error;
use std::fmt;

use netbadb_schema::{ColumnDef, SchemaError, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde::Deserialize;

pub use netbadb_schema::Schema;

/// Version of the language-neutral SDK schema input.
pub const SDK_SCHEMA_SPEC_VERSION: u32 = 2;
const SDK_SCHEMA_SPEC_VERSION_V1: u32 = 1;

#[derive(Debug, Deserialize)]
struct SpecVersion {
    version: u32,
}

/// Strict JSON input for SDK generation and schema-driven tooling.
///
/// This is not Canonical Schema encoding or a deployment manifest. Its fields
/// remain private so callers construct the validated [`Schema`] returned by
/// [`parse_schema_spec`] instead of depending on the JSON representation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSpec {
    version: u32,
    tables: Vec<TableSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSpec {
    id: u64,
    name: String,
    columns: Vec<ColumnSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnSpec {
    id: u32,
    name: String,
    physical_type: PhysicalTypeSpec,
    semantic_type: Option<String>,
    nullable: bool,
    primary_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PhysicalTypeSpec {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Float32,
    Float64,
    Text,
    Bytes,
}

impl From<PhysicalTypeSpec> for PhysicalType {
    fn from(value: PhysicalTypeSpec) -> Self {
        match value {
            PhysicalTypeSpec::Bool => Self::Bool,
            PhysicalTypeSpec::Int8 => Self::Int8,
            PhysicalTypeSpec::Int16 => Self::Int16,
            PhysicalTypeSpec::Int32 => Self::Int32,
            PhysicalTypeSpec::Int64 => Self::Int64,
            PhysicalTypeSpec::Int128 => Self::Int128,
            PhysicalTypeSpec::Uint8 => Self::UInt8,
            PhysicalTypeSpec::Uint16 => Self::UInt16,
            PhysicalTypeSpec::Uint32 => Self::UInt32,
            PhysicalTypeSpec::Uint64 => Self::UInt64,
            PhysicalTypeSpec::Uint128 => Self::UInt128,
            PhysicalTypeSpec::Float32 => Self::Float32,
            PhysicalTypeSpec::Float64 => Self::Float64,
            PhysicalTypeSpec::Text => Self::Text,
            PhysicalTypeSpec::Bytes => Self::Bytes,
        }
    }
}

impl SchemaSpec {
    fn into_schema(self) -> Result<Schema, SchemaSpecError> {
        let tables = self
            .tables
            .into_iter()
            .map(TableSpec::into_table)
            .collect::<Vec<_>>();
        Schema::new(tables).map_err(SchemaSpecError::Schema)
    }
}

impl PhysicalTypeSpec {
    const fn is_v1(self) -> bool {
        matches!(self, Self::Bool | Self::Int64 | Self::Uint64 | Self::Text)
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Int128 => "int128",
            Self::Uint8 => "uint8",
            Self::Uint16 => "uint16",
            Self::Uint32 => "uint32",
            Self::Uint64 => "uint64",
            Self::Uint128 => "uint128",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Text => "text",
            Self::Bytes => "bytes",
        }
    }
}

impl TableSpec {
    fn into_table(self) -> TableDef {
        let columns = self
            .columns
            .into_iter()
            .map(ColumnSpec::into_column)
            .collect();
        TableDef::new(TableId(self.id), self.name, columns)
    }
}

impl ColumnSpec {
    fn into_column(self) -> ColumnDef {
        let physical = self.physical_type.into();
        let type_spec = self
            .semantic_type
            .map_or(TypeSpec::Physical(physical), |name| TypeSpec::Semantic {
                name,
                physical,
            });
        ColumnDef::new(ColumnId(self.id), self.name, type_spec)
            .nullable(self.nullable)
            .primary_key(self.primary_key)
    }
}

/// Parses SDK Schema Spec v1 or v2 and validates it through the canonical schema API.
pub fn parse_schema_spec(source: &str) -> Result<Schema, SchemaSpecError> {
    let version: SpecVersion = serde_json::from_str(source).map_err(SchemaSpecError::Json)?;
    if !matches!(
        version.version,
        SDK_SCHEMA_SPEC_VERSION_V1 | SDK_SCHEMA_SPEC_VERSION
    ) {
        return Err(SchemaSpecError::UnsupportedVersion(version.version));
    }
    let spec: SchemaSpec = serde_json::from_str(source).map_err(SchemaSpecError::Json)?;
    if spec.version == SDK_SCHEMA_SPEC_VERSION_V1 {
        if let Some(physical) = spec
            .tables
            .iter()
            .flat_map(|table| &table.columns)
            .map(|column| column.physical_type)
            .find(|physical| !physical.is_v1())
        {
            return Err(SchemaSpecError::PhysicalTypeNotSupported {
                version: spec.version,
                physical: physical.name(),
            });
        }
    }
    spec.into_schema()
}

/// A strict Schema Spec decoding or canonical validation failure.
#[derive(Debug)]
pub enum SchemaSpecError {
    /// The JSON shape, required fields, or physical-type spelling is invalid.
    Json(serde_json::Error),
    /// The input declares a version other than [`SDK_SCHEMA_SPEC_VERSION`].
    UnsupportedVersion(u32),
    /// A known historical version used a type introduced by a later version.
    PhysicalTypeNotSupported {
        version: u32,
        physical: &'static str,
    },
    /// The decoded tables violate canonical schema invariants.
    Schema(SchemaError),
}

impl fmt::Display for SchemaSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "invalid SDK Schema Spec JSON: {error}"),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported SDK Schema Spec version {version}; supported versions are 1 and {SDK_SCHEMA_SPEC_VERSION}"
            ),
            Self::PhysicalTypeNotSupported { version, physical } => write!(
                formatter,
                "physical type `{physical}` is not supported by SDK Schema Spec v{version}"
            ),
            Self::Schema(error) => write!(formatter, "invalid canonical schema: {error}"),
        }
    }
}

impl Error for SchemaSpecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::UnsupportedVersion(_) | Self::PhysicalTypeNotSupported { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use netbadb_schema::SchemaError;

    use super::*;

    const SPEC: &str = r#"{
      "version": 1,
      "tables": [
        {"id": 1, "name": "users", "columns": [
          {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": "UserId", "nullable": false, "primary_key": true},
          {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
        ]},
        {"id": 2, "name": "teams", "columns": [
          {"id": 1, "name": "id", "physical_type": "uint64", "semantic_type": "TeamId", "nullable": false, "primary_key": true}
        ]}
      ]
    }"#;

    #[test]
    fn parses_strict_spec_through_canonical_schema() {
        let schema = parse_schema_spec(SPEC).expect("valid spec");
        assert_eq!(schema.tables().len(), 2);
        assert_eq!(
            schema.tables()[0].columns[0]
                .semantic_type()
                .name
                .as_deref(),
            Some("UserId")
        );
        assert!(schema.tables()[0].columns[1].nullable);

        let unknown = SPEC.replacen("\"version\": 1", "\"version\": 1, \"listen\": \"x\"", 1);
        assert!(matches!(
            parse_schema_spec(&unknown),
            Err(SchemaSpecError::Json(_))
        ));
        let unsupported = SPEC.replacen("\"version\": 1", "\"version\": 3", 1);
        assert!(matches!(
            parse_schema_spec(&unsupported),
            Err(SchemaSpecError::UnsupportedVersion(3))
        ));
        let missing_identity_field = r#"{"version":1,"tables":[{"id":1,"name":"users","columns":[{"id":1,"name":"id","physical_type":"int64","semantic_type":null,"nullable":false}]}]}"#;
        assert!(matches!(
            parse_schema_spec(missing_identity_field),
            Err(SchemaSpecError::Json(_))
        ));
    }

    #[test]
    fn propagates_canonical_errors_and_rejects_type_aliases() {
        let duplicate = SPEC.replacen(
            "\"id\": 2, \"name\": \"teams\"",
            "\"id\": 1, \"name\": \"teams\"",
            1,
        );
        assert!(matches!(
            parse_schema_spec(&duplicate),
            Err(SchemaSpecError::Schema(
                SchemaError::DuplicateTableId { .. }
            ))
        ));
        let alias = SPEC.replacen(
            "\"physical_type\": \"int64\"",
            "\"physical_type\": \"i64\"",
            1,
        );
        assert!(matches!(
            parse_schema_spec(&alias),
            Err(SchemaSpecError::Json(_))
        ));
    }

    #[test]
    fn versions_gate_physical_type_grammar() {
        let names = [
            "bool", "int8", "int16", "int32", "int64", "int128", "uint8", "uint16", "uint32",
            "uint64", "uint128", "float32", "float64", "text", "bytes",
        ];
        let columns = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                format!(
                    r#"{{"id":{},"name":"c{}","physical_type":"{}","semantic_type":null,"nullable":false,"primary_key":false}}"#,
                    index + 1,
                    index + 1,
                    name
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let source = format!(
            r#"{{"version":2,"tables":[{{"id":1,"name":"values","columns":[{columns}]}}]}}"#
        );
        let schema = parse_schema_spec(&source).expect("all physical spellings");
        assert_eq!(schema.tables()[0].columns.len(), names.len());

        let v1_new_type = source.replacen("\"version\":2", "\"version\":1", 1);
        assert!(matches!(
            parse_schema_spec(&v1_new_type),
            Err(SchemaSpecError::PhysicalTypeNotSupported {
                version: 1,
                physical: "int8"
            })
        ));

        let v2_unknown = source.replacen("\"tables\"", "\"unknown\":true,\"tables\"", 1);
        assert!(matches!(
            parse_schema_spec(&v2_unknown),
            Err(SchemaSpecError::Json(_))
        ));
    }
}
