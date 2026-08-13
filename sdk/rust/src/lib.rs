//! Public Rust embedded SDK. The core crate owns implementation boundaries;
//! this crate is the stable application-facing re-export surface.

pub use netbadb_core::{
    Database, DatabaseError, ExecutionResult, QueryResult, ResultColumn, Transaction,
    TransactionState,
};
pub use netbadb_schema::{
    CANONICAL_TABLE_SCHEMA_VERSION, ColumnDef, IdentifierError, Schema, SchemaError,
    SchemaFingerprint, TableDef, TypeSpec,
};
pub use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};
