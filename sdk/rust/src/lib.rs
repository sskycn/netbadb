//! Public Rust SDK façade for embedded and synchronous remote applications.

#[cfg(feature = "embedded")]
pub use netbadb_core::{
    Database, DatabaseError, ExecutionResult, QueryResult, ResultColumn, Transaction,
    TransactionState,
};
pub use netbadb_schema::{
    CANONICAL_TABLE_SCHEMA_VERSION, ColumnDef, Schema, SchemaError, SchemaFingerprint, TableDef,
    TypeSpec,
};
pub use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};

/// Synchronous Protocol v1 remote client APIs.
#[cfg(feature = "remote")]
pub mod remote {
    pub use netbadb_client::{
        CAPABILITY_ANALYZE, CAPABILITY_EXPLICIT_TRANSACTIONS, CAPABILITY_STREAMED_QUERY_RESULTS,
        Client, ClientError, Config, ProtocolErrorCode, ResultColumn, Rows, ServerError,
        ServerInfo, TableIdentity, TlsConfig, TlsConfigError, TlsHandshakeError, Transaction,
        WireTransactionState,
    };
}
