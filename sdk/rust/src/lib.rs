//! Public Rust SDK façade for embedded and synchronous remote applications.

#[cfg(feature = "embedded")]
pub use netbadb_core::{
    AlterTableOperation, AlterTableSpec, ColumnarExecutionStatistics,
    ColumnarProjectionCatalogInspection, ColumnarProjectionHealth, ColumnarProjectionInspection,
    ColumnarProjectionSpec, CompleteLegacyInventory, CreateColumnSpec, CreateTableSpec, Database,
    DatabaseCoordinatorConfig, DatabaseError, DatabaseSnapshot, DatabaseVisibilityInspection,
    DatabaseVisibilityMode, ExecutionResult, LegacyStorageLocation, LsmMaintenanceReport,
    MaintenanceAction, MaintenanceActionReport, MaintenanceBlocker, MaintenanceBound,
    MaintenanceBudget, MaintenanceCandidate, MaintenanceConsumption, MaintenanceDecision,
    MaintenanceEstimate, MaintenanceInspection, MaintenanceOutcome, MaintenanceReason,
    MaintenanceStepReport, PartitionCatalogConfig, ProjectionCatalogError, QueryResult,
    RangePartitionSpec, ReplacementRetiredHeap, ResultColumn, SchemaCatalogError, SchemaDependency,
    SchemaGeneration, SchemaMutationError, TablePlacementSpec, TableSchemaVersion,
    TableStorageCreateSpec, Transaction, TransactionState, VisibilityBoundaryInspection,
};
pub use netbadb_schema::{
    CANONICAL_TABLE_SCHEMA_VERSION, ColumnDef, Schema, SchemaError, SchemaFingerprint, TableDef,
    TypeSpec,
};
pub use netbadb_types::{
    AccessPathId, ColumnId, ColumnarGeneration, ColumnarProjectionId, ColumnarSegmentId,
    DatabaseCommitSeq, Float32Value, Float64Value, PartitionId, PhysicalType, RelationBindingId,
    ScalarValue, SemanticType, StorageId, TableId,
};

/// Stable embedded catalog and physical-plan inspection values and renderers.
#[cfg(feature = "embedded")]
pub mod inspection {
    pub use netbadb_inspect::{
        AggregateFunctionInspection, AggregateInputInspection, AggregateOutputInspection,
        AssignmentInspection, BinaryOpInspection, CatalogInspection, ColumnInspection,
        ColumnReferenceInspection, ExpressionInspection, ExpressionKindInspection, IndexInspection,
        IndexKindInspection, IndexRangeInspection, IndexStatisticsInspection, JoinKindInspection,
        NullOrderInspection, PartitionAccessInspection, PartitionScanInspection,
        PlanNodeInspection, RangeBoundInspection, RangePartitionInspection, ResultFieldInspection,
        SortDirectionInspection, SortKeyInspection, SourceColumnInspection,
        StatementAccessInspection, StatementInspection, StatementKind, StatementPlanInspection,
        StatementResultInspection, TableInspection, TablePlacementInspection,
        TableStatisticsInspection, UnaryOpInspection, render_catalog, render_statement,
    };
}

/// Synchronous Protocol v2 remote client APIs.
#[cfg(feature = "remote")]
pub mod remote {
    pub use netbadb_client::{
        CAPABILITY_ANALYZE, CAPABILITY_EXPLICIT_TRANSACTIONS, CAPABILITY_STREAMED_QUERY_RESULTS,
        Client, ClientError, Config, ProtocolErrorCode, ResultColumn, Rows, ServerError,
        ServerInfo, TableIdentity, TlsConfig, TlsConfigError, TlsHandshakeError, Transaction,
        WireTransactionState,
    };
}
