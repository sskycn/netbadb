//! Native synchronous embedded API for NetbaDB.

mod columnar;
#[cfg(test)]
mod columnar_tests;
#[cfg(test)]
mod coordinator_crash;
mod coordinator_log;
mod deferred_backfill;
mod inspection;
mod maintenance;
#[cfg(test)]
mod maintenance_tests;
mod partition_catalog;
mod projection_catalog;
mod registry;
mod schema_catalog;
mod schema_catalog_api;
mod schema_catalog_file;
#[cfg(test)]
mod schema_catalog_tests;
mod schema_composition;
mod schema_mutation;
mod schema_mutation_journal;
#[cfg(test)]
mod schema_mutation_tests;
mod schema_view;
#[cfg(test)]
mod staged_index_evacuation_tests;
mod transaction;

use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_compiler::{
    BindError, CompileError, CompileErrorKind, CompiledDdlStatement, CompiledStatement,
    DropIndexTarget, IndexNameBinding, PreparedParameter, TableIdentityBinding, TypedCreateIndex,
    TypedDropIndex, bind_statement, compile_ddl_statement, compile_statement,
    compile_statement_with_parameters,
};
use netbadb_executor::{
    ExecutionColumnarProjection, ExecutionError, ExecutionReadView, ExecutionStorage,
    ExecutionStorageBinding, PreparedMutation, execute_with_columnar_context,
    prepare_mutation_with_storage_context,
};
use netbadb_inspect::StatementInspection;
use netbadb_planner::{
    AccessCostHints, AccessPath, AccessPathCapabilities, ColumnarProjectionPlanningSnapshot,
    ColumnarRowGroupPlanningSnapshot, ColumnarZoneMapPlanningSnapshot, PartitionPlanningSnapshot,
    PhysicalStatement, RangeTablePlanningSnapshot, TableAccessStatistics,
    plan_statement_with_columnar_snapshots, plan_statement_with_partition_snapshots,
};
use netbadb_schema::{Schema, SchemaError, TableDef};
use netbadb_storage::{
    ColumnarProjection, HeapRecoveryInspection, PreparedDecision, PreparedTransaction,
    PreparedTransactionState, PreparedTxnResolution, StorageVisibilityBoundary, TableStorage,
};
use netbadb_types::{
    AccessPathId, ColumnId, ColumnarGeneration, ColumnarProjectionId, DatabaseCommitSeq,
    DatabaseTxnId, IndexName, PhysicalType, ScalarValue, StorageId, TableId, TxnId,
};

use columnar::{ProjectionRegistry, ProjectionRegistryEntry};
use coordinator_log::{CoordinatorDecision, CoordinatorLog};
use partition_catalog::{CatalogTable, PartitionCatalog, canonicalize_partitions, route_partition};
use registry::{
    PhysicalBindings, RangePartitionBinding, StorageRegistry, StorageRegistryEntry, TablePlacement,
};
use schema_catalog::CommittedCatalogState;
use transaction::{PublishedVisibility, SharedCoordinatorLog, SharedPublishedVisibility};

pub use columnar::{
    ChangeStreamGcReport, ColumnarAdvanceBudget, ColumnarAdvanceReport, ColumnarCompactionReport,
    ColumnarProjectionCatalogInspection, ColumnarProjectionHealth, ColumnarProjectionInspection,
    ColumnarProjectionSpec,
};
pub use coordinator_log::CoordinatorLogError;
pub use maintenance::{
    LsmMaintenanceReport, MaintenanceAction, MaintenanceActionReport, MaintenanceBlocker,
    MaintenanceBound, MaintenanceBudget, MaintenanceCandidate, MaintenanceConsumption,
    MaintenanceDecision, MaintenanceEstimate, MaintenanceInspection, MaintenanceOutcome,
    MaintenanceReason, MaintenanceStepReport,
};
pub use netbadb_executor::{
    ColumnarExecutionStatistics, ExecutionResult, QueryResult, ResultColumn,
};
pub use netbadb_inspect::{CatalogInspection, IndexKindInspection, TablePlacementInspection};
pub use netbadb_schema::DropTableTarget;
pub use netbadb_storage::{
    ChangeBatch, ChangeBatchInspection, ChangeReadResult, ChangeStreamCursor, ChangeStreamError,
    ChangeStreamInspection, CommittedReadAnchor, HistoricalOrphanAdoptionReport, IndexDefinition,
    IndexMaintenanceReport, IndexReclaimReport, IndexStatistics, IndexTailReclaimReport,
    IsolationLevel, LsmInspection, LsmLevelInspection, LsmReadAmplification, LsmWriteAmplification,
    PageReuseClass, PageReuseInspection, ReusablePageInspection, StorageChange, StorageError,
    StorageKind, StorageVersionKey, TableStatistics,
};
pub use netbadb_types::{
    ChangeStreamGeneration, SchemaGeneration, StorageDataVersion, TableSchemaVersion,
};
pub use partition_catalog::{
    PartitionCatalogConfig, PartitionError, RangePartitionSpec, TablePlacementSpec,
};
pub use projection_catalog::ProjectionCatalogError;
pub use registry::StorageRegistryError;
pub use schema_catalog::SchemaCatalogError;
pub use schema_catalog_api::{CompleteLegacyInventory, LegacyStorageLocation};
pub use schema_mutation::{
    AlterTableOperation, AlterTableSpec, CreateColumnSpec, CreateTableSpec, ReplacementRetiredHeap,
    RetiredHeapGcBlocker, RetiredHeapGcComponent, RetiredHeapGcComponentKind,
    RetiredHeapGcInspection, RetiredHeapGcReport, RetiredHeapGcState, RetiredHeapGcTarget,
    RetiredTableResource, SchemaDependency, SchemaMutationError,
};

impl From<SchemaCatalogError> for DatabaseError {
    fn from(error: SchemaCatalogError) -> Self {
        Self::SchemaCatalog(error)
    }
}

/// Strict bounded decoder entry point for the standalone catalog fuzzer.
#[doc(hidden)]
pub fn fuzz_schema_catalog_bytes(bytes: &[u8]) {
    if let Ok(snapshot) = schema_catalog::SchemaCatalogSnapshot::decode(bytes) {
        if let Ok(encoded) = snapshot.encode() {
            assert_eq!(bytes, encoded);
        }
    }
}
/// Strict bounded mutation journal decoder entry point for fuzzing.
#[doc(hidden)]
pub fn fuzz_schema_mutation_bytes(bytes: &[u8]) {
    if let Ok(journal) = schema_mutation_journal::SchemaMutationJournal::decode(bytes) {
        assert_eq!(
            journal.encode().expect("decoded journal must encode"),
            bytes
        );
    }
}

pub use transaction::{
    CoordinatorError, DatabaseReadView, DatabaseSnapshot, DatabaseTransaction,
    DatabaseVisibilityMode, ParticipantMode, TransactionState,
};
pub type Transaction = DatabaseTransaction;

/// Exercises the strict coordinator decoder for the standalone fuzz target.
///
/// This is not a database recovery API. Normal callers must use
/// [`Database::open_tables_with_coordinator`].
#[doc(hidden)]
pub fn fuzz_coordinator_log_file(path: &Path) {
    let _ = CoordinatorLog::open(path);
}

/// Exercises the strict bounded PartitionCatalog v1 decoder for fuzzing.
#[doc(hidden)]
pub fn fuzz_partition_catalog_bytes(bytes: &[u8]) {
    let _ = PartitionCatalog::decode(bytes);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseCoordinatorConfig {
    log_path: PathBuf,
    retired_storage_ids: Vec<StorageId>,
    global_visibility: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityBoundaryInspection {
    pub storage_id: StorageId,
    pub storage_kind: netbadb_storage::StorageKind,
    pub boundary: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseVisibilityInspection {
    pub mode: DatabaseVisibilityMode,
    pub published_commit_seq: Option<DatabaseCommitSeq>,
    pub next_commit_seq: Option<DatabaseCommitSeq>,
    pub boundaries: Vec<VisibilityBoundaryInspection>,
}

impl DatabaseCoordinatorConfig {
    #[must_use]
    pub fn new(log_path: impl Into<PathBuf>) -> Self {
        Self {
            log_path: log_path.into(),
            retired_storage_ids: Vec::new(),
            global_visibility: false,
        }
    }

    #[must_use]
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// Requests durable database-global snapshot publication for a newly
    /// created coordinator. Open always follows the mode persisted in NBCO.
    #[must_use]
    pub fn with_global_visibility(mut self) -> Self {
        self.global_visibility = true;
        self
    }

    pub(crate) fn with_retired_storage_ids(mut self, ids: Vec<StorageId>) -> Self {
        self.retired_storage_ids = ids;
        self
    }

    pub(crate) fn retired_storage_ids(&self) -> &[StorageId] {
        &self.retired_storage_ids
    }
}

/// Explicit physical layout used when creating a single-table storage.
/// Paths never imply an engine kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableStorageCreateSpec {
    Heap {
        path: PathBuf,
        table: TableDef,
    },
    Lsm {
        directory: PathBuf,
        table: TableDef,
        clustering_column: ColumnId,
    },
}

impl TableStorageCreateSpec {
    #[must_use]
    pub fn heap(path: impl Into<PathBuf>, table: TableDef) -> Self {
        Self::Heap {
            path: path.into(),
            table,
        }
    }

    #[must_use]
    pub fn lsm(
        directory: impl Into<PathBuf>,
        table: TableDef,
        clustering_column: ColumnId,
    ) -> Self {
        Self::Lsm {
            directory: directory.into(),
            table,
            clustering_column,
        }
    }

    fn path(&self) -> &Path {
        match self {
            Self::Heap { path, .. } => path,
            Self::Lsm { directory, .. } => directory,
        }
    }

    fn table(&self) -> &TableDef {
        match self {
            Self::Heap { table, .. } | Self::Lsm { table, .. } => table,
        }
    }
}

/// Explicit physical layout used when opening an existing storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableStorageOpenSpec {
    Heap { path: PathBuf, table: TableDef },
    Lsm { directory: PathBuf, table: TableDef },
}

impl TableStorageOpenSpec {
    #[must_use]
    pub fn heap(path: impl Into<PathBuf>, table: TableDef) -> Self {
        Self::Heap {
            path: path.into(),
            table,
        }
    }

    #[must_use]
    pub fn lsm(directory: impl Into<PathBuf>, table: TableDef) -> Self {
        Self::Lsm {
            directory: directory.into(),
            table,
        }
    }

    fn path(&self) -> &Path {
        match self {
            Self::Heap { path, .. } => path,
            Self::Lsm { directory, .. } => directory,
        }
    }

    fn table(&self) -> &TableDef {
        match self {
            Self::Heap { table, .. } | Self::Lsm { table, .. } => table,
        }
    }
}

/// Canonical table identities read or written by one successfully compiled SQL
/// statement. This exposes no syntax, compiler IR, plan, or storage details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementAccess {
    read_tables: Vec<TableId>,
    write_tables: Vec<TableId>,
    schema_tables: Vec<TableId>,
    schema_write: bool,
}

/// Typed output metadata obtained without executing a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementDescription {
    pub columns: Vec<ResultColumn>,
    pub is_query: bool,
}

/// A statement parsed, resolved, and type-checked once with frontend-neutral
/// parameter metadata. Binding produces a temporary logical statement so the
/// planner sees concrete scalar values.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    compiled: CompiledStatement,
    dependencies: Vec<SchemaDependency>,
    scope: Option<std::rc::Weak<()>>,
}

/// Prepared SQL retains separate relational and DDL boundaries.
#[derive(Debug, Clone)]
pub enum PreparedSqlStatement {
    Relational(Box<PreparedStatement>),
    Ddl(PreparedDdlStatement),
}
impl PreparedSqlStatement {
    #[must_use]
    pub fn access(&self) -> StatementAccess {
        match self {
            Self::Relational(s) => s.access(),
            Self::Ddl(s) => s.access(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedDdlStatement {
    compiled: CompiledDdlStatement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlOutcome {
    Created,
    Dropped,
    Altered,
    Unchanged,
}

impl PreparedDdlStatement {
    #[must_use]
    pub fn access(&self) -> StatementAccess {
        match &self.compiled {
            CompiledDdlStatement::CreateTable(_) => StatementAccess {
                read_tables: Vec::new(),
                write_tables: Vec::new(),
                schema_tables: Vec::new(),
                schema_write: true,
            },
            CompiledDdlStatement::DropTable(statement) => StatementAccess {
                read_tables: Vec::new(),
                write_tables: Vec::new(),
                schema_tables: vec![statement.target.table_id],
                schema_write: true,
            },
            CompiledDdlStatement::AlterTable(statement) => StatementAccess {
                read_tables: Vec::new(),
                write_tables: Vec::new(),
                schema_tables: vec![statement.target.table_id],
                schema_write: true,
            },
            CompiledDdlStatement::DropIndex(statement) => StatementAccess {
                schema_write: true,
                read_tables: Vec::new(),
                schema_tables: statement
                    .target
                    .map(|target| target.table_id)
                    .into_iter()
                    .collect(),
                write_tables: statement
                    .target
                    .map(|target| target.table_id)
                    .into_iter()
                    .collect(),
            },
            CompiledDdlStatement::CreateIndex(statement) => StatementAccess {
                schema_write: true,
                read_tables: Vec::new(),
                write_tables: vec![statement.target.table_id],
                schema_tables: vec![statement.target.table_id],
            },
        }
    }
    #[must_use]
    pub fn is_table_create(&self) -> bool {
        matches!(self.compiled, CompiledDdlStatement::CreateTable(_))
    }

    /// Declaration types for frontend compatibility preflight, without executing.
    pub fn created_column_types(&self) -> impl Iterator<Item = &netbadb_types::SemanticType> {
        let columns = match &self.compiled {
            CompiledDdlStatement::CreateTable(s) => s.columns.as_slice(),
            _ => &[],
        };
        let added = match &self.compiled {
            CompiledDdlStatement::AlterTable(statement) => match &statement.operation {
                netbadb_compiler::TypedAlterTableOperation::AddNullableColumn {
                    data_type, ..
                } => Some(data_type),
                _ => None,
            },
            _ => None,
        };
        columns.iter().map(|c| &c.data_type).chain(added)
    }

    /// Frontend command kind, with no protocol-specific completion tag.
    #[must_use]
    pub fn is_index_drop(&self) -> bool {
        matches!(self.compiled, CompiledDdlStatement::DropIndex(_))
    }

    /// Whether this statement is a generic SQL DROP TABLE.
    #[must_use]
    pub fn is_table_drop(&self) -> bool {
        matches!(self.compiled, CompiledDdlStatement::DropTable(_))
    }

    /// Whether this statement is a generic SQL ALTER TABLE.
    #[must_use]
    pub fn is_table_alter(&self) -> bool {
        matches!(self.compiled, CompiledDdlStatement::AlterTable(_))
    }

    /// Exact prepared ALTER target, when applicable.
    #[must_use]
    pub fn alter_table_target(&self) -> Option<DropTableTarget> {
        match &self.compiled {
            CompiledDdlStatement::AlterTable(statement) => Some(statement.target),
            _ => None,
        }
    }

    /// Exact prepared target for generic DROP TABLE, when applicable.
    #[must_use]
    pub fn drop_table_target(&self) -> Option<DropTableTarget> {
        match &self.compiled {
            CompiledDdlStatement::DropTable(statement) => Some(statement.target),
            _ => None,
        }
    }

    #[must_use]
    pub fn index_drop_if_exists(&self) -> bool {
        matches!(&self.compiled, CompiledDdlStatement::DropIndex(statement) if statement.if_exists)
    }

    /// Allows an adapter to resolve its own legacy alias before authorization.
    #[must_use]
    pub fn unresolved_index_name(&self) -> Option<&IndexName> {
        match &self.compiled {
            CompiledDdlStatement::DropIndex(statement) if statement.target.is_none() => {
                statement.name.as_ref()
            }
            _ => None,
        }
    }
}

impl PreparedStatement {
    #[must_use]
    pub fn schema_dependencies(&self) -> &[SchemaDependency] {
        &self.dependencies
    }
    #[must_use]
    pub fn parameters(&self) -> &[PreparedParameter] {
        &self.compiled.parameters
    }

    #[must_use]
    pub fn access(&self) -> StatementAccess {
        StatementAccess {
            schema_write: false,
            read_tables: self.compiled.logical_statement.read_tables(),
            write_tables: self.compiled.logical_statement.write_tables(),
            schema_tables: Vec::new(),
        }
    }

    #[must_use]
    pub fn description(&self) -> StatementDescription {
        statement_description(&self.compiled.logical_statement)
    }
}

impl StatementAccess {
    /// Schema mutation is a generic capability, independent of any table identity.
    #[must_use]
    pub fn schema_write(&self) -> bool {
        self.schema_write
    }

    #[must_use]
    pub fn read_tables(&self) -> &[TableId] {
        &self.read_tables
    }

    #[must_use]
    pub fn write_tables(&self) -> &[TableId] {
        &self.write_tables
    }

    /// Exact logical targets of schema DDL, separate from row-write grants.
    #[must_use]
    pub fn schema_tables(&self) -> &[TableId] {
        &self.schema_tables
    }
}

fn statement_description(statement: &netbadb_rel::LogicalStatement) -> StatementDescription {
    let netbadb_rel::LogicalStatement::Query(plan) = statement else {
        return StatementDescription {
            columns: Vec::new(),
            is_query: false,
        };
    };
    StatementDescription {
        columns: plan
            .output_fields()
            .into_iter()
            .map(|field| ResultColumn {
                name: field.name().to_owned(),
                data_type: field.data_type().clone(),
                nullable: field.nullable(),
            })
            .collect(),
        is_query: true,
    }
}

#[derive(Debug)]
pub enum DatabaseError {
    Compile(CompileError),
    Bind(BindError),
    Schema(SchemaError),
    SchemaCatalog(SchemaCatalogError),
    ProjectionCatalog(ProjectionCatalogError),
    SchemaMutation(SchemaMutationError),
    Storage(StorageError),
    Execution(ExecutionError),
    Registry(StorageRegistryError),
    Transaction(CoordinatorError),
    CoordinatorLog(CoordinatorLogError),
    Partition(PartitionError),
    ExpectedQuery,
    EmptyCatalog,
    TableSelectionRequired,
    DuplicateStoragePath(PathBuf),
    CoordinatorPathConflictsWithStorage(PathBuf),
    MissingCommitParticipant {
        database_txn_id: DatabaseTxnId,
        storage_id: StorageId,
        physical_txn_id: TxnId,
    },
    PreparedParticipantMismatch {
        database_txn_id: DatabaseTxnId,
        storage_id: StorageId,
        physical_txn_id: TxnId,
    },
    InspectionStorageMissing {
        table_id: TableId,
    },
    InspectionIndexColumnMissing {
        table_id: TableId,
        column_id: ColumnId,
    },
    InspectionRegistrationOrderOverflow {
        table_id: TableId,
        position: usize,
    },
    DuplicateIndexName(IndexName),
    UndefinedIndex,
    UnsupportedDdlCombination,
    ColumnarProjectionNotFound(ColumnarProjectionId),
    ColumnarProjectionNotIncremental(ColumnarProjectionId),
    ColumnarProjectionRebuildRequired(ColumnarProjectionId),
    ChangeStreamGcNoRetentionConsumer(StorageId),
    ChangeStreamGcUnsafe {
        storage_id: StorageId,
        reason: String,
    },
    ColumnarProjectionIdExhausted,
    ColumnarBuildSourceChanged {
        storage_id: StorageId,
    },
    ColumnarProjectionRequiresSingleStorage(TableId),
    CreateTablesRollback {
        creation: StorageError,
        cleanup_path: PathBuf,
        cleanup: std::io::Error,
    },
}

/// Transport-neutral diagnostic categories exposed at the database boundary.
///
/// Frontends use these stable semantic categories to map errors into their own
/// protocol without inspecting compiler enums or parsing human messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseErrorKind {
    Syntax,
    DuplicateColumn,
    DependentObjects,
    SchemaBusy,
    UndefinedTable,
    UndefinedColumn,
    AmbiguousColumn,
    DatatypeMismatch,
    IndeterminateDatatype,
    ParameterCount,
    NotNullViolation,
    FeatureNotSupported,
    DuplicateObject,
    UndefinedObject,
    TransactionState,
    Operational,
    Internal,
}

impl DatabaseError {
    #[must_use]
    pub const fn kind(&self) -> DatabaseErrorKind {
        match self {
            Self::Compile(error) => match error.kind() {
                CompileErrorKind::UndefinedType => DatabaseErrorKind::UndefinedObject,
                CompileErrorKind::DuplicateColumn => DatabaseErrorKind::DuplicateColumn,
                CompileErrorKind::DuplicateTable => DatabaseErrorKind::DuplicateObject,
                CompileErrorKind::Syntax => DatabaseErrorKind::Syntax,
                CompileErrorKind::UndefinedTable => DatabaseErrorKind::UndefinedTable,
                CompileErrorKind::UndefinedColumn => DatabaseErrorKind::UndefinedColumn,
                CompileErrorKind::AmbiguousColumn => DatabaseErrorKind::AmbiguousColumn,
                CompileErrorKind::DatatypeMismatch => DatabaseErrorKind::DatatypeMismatch,
                CompileErrorKind::IndeterminateDatatype => DatabaseErrorKind::IndeterminateDatatype,
                CompileErrorKind::NotNullViolation => DatabaseErrorKind::NotNullViolation,
                CompileErrorKind::FeatureNotSupported => DatabaseErrorKind::FeatureNotSupported,
            },
            Self::Bind(BindError::ParameterCount { .. }) => DatabaseErrorKind::ParameterCount,
            Self::Bind(BindError::ParameterType { .. }) => DatabaseErrorKind::DatatypeMismatch,
            Self::Storage(StorageError::NullNotAllowed { .. })
            | Self::Execution(ExecutionError::Storage(StorageError::NullNotAllowed { .. })) => {
                DatabaseErrorKind::NotNullViolation
            }
            Self::Storage(StorageError::TypeMismatch { .. })
            | Self::Execution(ExecutionError::Storage(StorageError::TypeMismatch { .. })) => {
                DatabaseErrorKind::DatatypeMismatch
            }
            Self::Storage(StorageError::Index(
                netbadb_index::IndexError::IndexAlreadyExists { .. }
                | netbadb_index::IndexError::IndexNameAlreadyExists { .. },
            )) => DatabaseErrorKind::DuplicateObject,
            Self::Storage(StorageError::UnsupportedOperation { .. })
            | Self::Partition(PartitionError::PartitionedIndexCreationNotSupported(_)) => {
                DatabaseErrorKind::FeatureNotSupported
            }
            Self::SchemaMutation(
                SchemaMutationError::UnsupportedConstraint
                | SchemaMutationError::UnsupportedPlacement
                | SchemaMutationError::MultipleCreatesUnsupported
                | SchemaMutationError::UnsupportedSchemaEvolution
                | SchemaMutationError::UnsupportedBackfillRefinement(_)
                | SchemaMutationError::ActiveChangeStreamBlocksReplacement { .. },
            ) => DatabaseErrorKind::FeatureNotSupported,
            Self::SchemaMutation(
                SchemaMutationError::TableNotFound(_) | SchemaMutationError::UndefinedTable(_),
            ) => DatabaseErrorKind::UndefinedTable,
            Self::SchemaMutation(SchemaMutationError::ColumnNotFound(_)) => {
                DatabaseErrorKind::UndefinedColumn
            }
            Self::SchemaMutation(SchemaMutationError::NotNullViolation(_)) => {
                DatabaseErrorKind::NotNullViolation
            }
            Self::SchemaMutation(
                SchemaMutationError::IndexedColumn(_) | SchemaMutationError::PrimaryKeyColumn(_),
            ) => DatabaseErrorKind::DependentObjects,
            Self::Schema(SchemaError::DuplicateTableName { .. }) => {
                DatabaseErrorKind::DuplicateObject
            }
            Self::Schema(SchemaError::DuplicateColumnName { .. }) => {
                DatabaseErrorKind::DuplicateColumn
            }
            Self::SchemaMutation(SchemaMutationError::SchemaBusy) => DatabaseErrorKind::SchemaBusy,
            Self::SchemaMutation(
                SchemaMutationError::StalePreparedStatement
                | SchemaMutationError::StaleSchemaDependency
                | SchemaMutationError::RecoveryRequired
                | SchemaMutationError::TransactionNotPristine
                | SchemaMutationError::SchemaMutationAfterMaterialization
                | SchemaMutationError::MigrationDataAccessAfterRefinement
                | SchemaMutationError::EvacuationRequiresRefinement,
            ) => DatabaseErrorKind::TransactionState,
            Self::SchemaMutation(_) => DatabaseErrorKind::Operational,
            Self::Transaction(_) => DatabaseErrorKind::TransactionState,
            Self::UndefinedIndex => DatabaseErrorKind::UndefinedObject,
            Self::UnsupportedDdlCombination => DatabaseErrorKind::FeatureNotSupported,
            Self::ColumnarProjectionRequiresSingleStorage(_) => {
                DatabaseErrorKind::FeatureNotSupported
            }
            Self::Storage(StorageError::Index(
                netbadb_index::IndexError::UnknownIndexId(_)
                | netbadb_index::IndexError::IndexAlreadyRetired(_),
            )) => DatabaseErrorKind::UndefinedObject,
            Self::DuplicateIndexName(_) => DatabaseErrorKind::DuplicateObject,
            Self::Registry(StorageRegistryError::DuplicateIndexName { .. }) => {
                DatabaseErrorKind::DuplicateObject
            }
            Self::SchemaCatalog(_)
            | Self::ProjectionCatalog(_)
            | Self::Schema(_)
            | Self::Storage(_)
            | Self::Registry(_)
            | Self::CoordinatorLog(_)
            | Self::Partition(_)
            | Self::DuplicateStoragePath(_)
            | Self::CoordinatorPathConflictsWithStorage(_)
            | Self::CreateTablesRollback { .. } => DatabaseErrorKind::Operational,
            Self::ExpectedQuery | Self::TableSelectionRequired => {
                DatabaseErrorKind::FeatureNotSupported
            }
            Self::Execution(_)
            | Self::EmptyCatalog
            | Self::MissingCommitParticipant { .. }
            | Self::PreparedParticipantMismatch { .. }
            | Self::InspectionStorageMissing { .. }
            | Self::InspectionIndexColumnMissing { .. }
            | Self::InspectionRegistrationOrderOverflow { .. }
            | Self::ColumnarProjectionIdExhausted => DatabaseErrorKind::Internal,
            Self::ColumnarProjectionNotFound(_)
            | Self::ColumnarProjectionNotIncremental(_)
            | Self::ColumnarProjectionRebuildRequired(_)
            | Self::ChangeStreamGcNoRetentionConsumer(_)
            | Self::ChangeStreamGcUnsafe { .. }
            | Self::ColumnarBuildSourceChanged { .. } => DatabaseErrorKind::Operational,
        }
    }

    /// Returns a one-based source byte position when the compiler identified
    /// an exact failing syntax or semantic node.
    #[must_use]
    pub fn source_position(&self) -> Option<u32> {
        let zero_based = match self {
            Self::Compile(error) => error.span().start,
            _ => return None,
        };
        u32::try_from(zero_based).ok()?.checked_add(1)
    }
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(error) => error.fmt(formatter),
            Self::Bind(error) => error.fmt(formatter),
            Self::Schema(error) => error.fmt(formatter),
            Self::SchemaCatalog(error) => error.fmt(formatter),
            Self::ProjectionCatalog(error) => error.fmt(formatter),
            Self::SchemaMutation(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
            Self::Registry(error) => error.fmt(formatter),
            Self::Transaction(error) => error.fmt(formatter),
            Self::CoordinatorLog(error) => error.fmt(formatter),
            Self::Partition(error) => error.fmt(formatter),
            Self::ExpectedQuery => formatter.write_str("statement does not return query rows"),
            Self::EmptyCatalog => formatter.write_str("database requires at least one table"),
            Self::TableSelectionRequired => formatter
                .write_str("this catalog has multiple tables; use a table-specific embedded API"),
            Self::DuplicateStoragePath(path) => {
                write!(
                    formatter,
                    "storage path `{}` is registered more than once",
                    path.display()
                )
            }
            Self::CoordinatorPathConflictsWithStorage(path) => write!(
                formatter,
                "coordinator log path `{}` conflicts with a table storage path",
                path.display()
            ),
            Self::MissingCommitParticipant {
                database_txn_id,
                storage_id,
                physical_txn_id,
            } => write!(
                formatter,
                "database transaction {} commit decision requires missing storage {} physical transaction {}",
                database_txn_id.0, storage_id.0, physical_txn_id.0
            ),
            Self::PreparedParticipantMismatch {
                database_txn_id,
                storage_id,
                physical_txn_id,
            } => write!(
                formatter,
                "storage {} physical transaction {} is inconsistent with database transaction {} commit decision",
                storage_id.0, physical_txn_id.0, database_txn_id.0
            ),
            Self::InspectionStorageMissing { table_id } => write!(
                formatter,
                "inspection found no storage for catalog table {}",
                table_id.0
            ),
            Self::InspectionIndexColumnMissing {
                table_id,
                column_id,
            } => write!(
                formatter,
                "inspection found index column {} missing from table {}",
                column_id.0, table_id.0
            ),
            Self::InspectionRegistrationOrderOverflow { table_id, position } => write!(
                formatter,
                "inspection index registration position {position} for table {} exceeds u32",
                table_id.0
            ),
            Self::UndefinedIndex => formatter.write_str("index does not exist"),
            Self::UnsupportedDdlCombination => formatter
                .write_str("this schema/index DDL combination is not supported in one transaction"),
            Self::ColumnarProjectionNotFound(id) => {
                write!(formatter, "columnar projection {} does not exist", id.0)
            }
            Self::ColumnarProjectionNotIncremental(id) => write!(
                formatter,
                "columnar projection {} is a snapshot projection and cannot advance",
                id.0
            ),
            Self::ColumnarProjectionRebuildRequired(id) => write!(
                formatter,
                "columnar projection {} no longer matches the active change stream and must be rebuilt",
                id.0
            ),
            Self::ChangeStreamGcNoRetentionConsumer(storage_id) => write!(
                formatter,
                "change stream for storage {} has no managed incremental retention consumer",
                storage_id.0
            ),
            Self::ChangeStreamGcUnsafe { storage_id, reason } => write!(
                formatter,
                "change-stream retention for storage {} is unsafe: {reason}",
                storage_id.0
            ),
            Self::ColumnarProjectionIdExhausted => {
                formatter.write_str("columnar projection identity space is exhausted")
            }
            Self::ColumnarBuildSourceChanged { storage_id } => write!(
                formatter,
                "source storage {} changed while the columnar projection was built",
                storage_id.0
            ),
            Self::ColumnarProjectionRequiresSingleStorage(table_id) => write!(
                formatter,
                "columnar phase 1 requires table {} to have one physical storage",
                table_id.0
            ),
            Self::DuplicateIndexName(name) => write!(formatter, "index `{name}` already exists"),
            Self::CreateTablesRollback {
                creation,
                cleanup_path,
                cleanup,
            } => write!(
                formatter,
                "failed to create catalog: {creation}; also failed to remove newly created file `{}`: {cleanup}",
                cleanup_path.display()
            ),
        }
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compile(error) => Some(error),
            Self::Bind(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::SchemaCatalog(error) => Some(error),
            Self::ProjectionCatalog(error) => Some(error),
            Self::SchemaMutation(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Execution(error) => Some(error),
            Self::Registry(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::CoordinatorLog(error) => Some(error),
            Self::Partition(error) => Some(error),
            Self::CreateTablesRollback { creation, .. } => Some(creation),
            Self::ExpectedQuery
            | Self::EmptyCatalog
            | Self::TableSelectionRequired
            | Self::DuplicateStoragePath(_)
            | Self::CoordinatorPathConflictsWithStorage(_)
            | Self::MissingCommitParticipant { .. }
            | Self::PreparedParticipantMismatch { .. }
            | Self::InspectionStorageMissing { .. }
            | Self::InspectionIndexColumnMissing { .. }
            | Self::InspectionRegistrationOrderOverflow { .. } => None,
            Self::DuplicateIndexName(_)
            | Self::UndefinedIndex
            | Self::UnsupportedDdlCombination
            | Self::ColumnarProjectionNotFound(_)
            | Self::ColumnarProjectionNotIncremental(_)
            | Self::ColumnarProjectionRebuildRequired(_)
            | Self::ChangeStreamGcNoRetentionConsumer(_)
            | Self::ChangeStreamGcUnsafe { .. }
            | Self::ColumnarProjectionIdExhausted
            | Self::ColumnarBuildSourceChanged { .. }
            | Self::ColumnarProjectionRequiresSingleStorage(_) => None,
        }
    }
}

impl From<CompileError> for DatabaseError {
    fn from(error: CompileError) -> Self {
        Self::Compile(error)
    }
}

impl From<BindError> for DatabaseError {
    fn from(error: BindError) -> Self {
        Self::Bind(error)
    }
}

impl From<SchemaError> for DatabaseError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<ProjectionCatalogError> for DatabaseError {
    fn from(error: ProjectionCatalogError) -> Self {
        Self::ProjectionCatalog(error)
    }
}

impl From<StorageError> for DatabaseError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<netbadb_storage::ColumnarError> for DatabaseError {
    fn from(error: netbadb_storage::ColumnarError) -> Self {
        Self::Storage(StorageError::from(error))
    }
}

impl From<ExecutionError> for DatabaseError {
    fn from(error: ExecutionError) -> Self {
        Self::Execution(error)
    }
}

impl From<StorageRegistryError> for DatabaseError {
    fn from(error: StorageRegistryError) -> Self {
        Self::Registry(error)
    }
}

impl From<CoordinatorError> for DatabaseError {
    fn from(error: CoordinatorError) -> Self {
        match error {
            CoordinatorError::Storage(storage) => Self::Storage(storage),
            other => Self::Transaction(other),
        }
    }
}

impl From<CoordinatorLogError> for DatabaseError {
    fn from(error: CoordinatorLogError) -> Self {
        Self::CoordinatorLog(error)
    }
}

impl From<PartitionError> for DatabaseError {
    fn from(error: PartitionError) -> Self {
        Self::Partition(error)
    }
}

struct CapturedColumnarSource {
    storage_id: StorageId,
    table: TableDef,
    token: netbadb_storage::StorageSnapshotToken,
    rows: Vec<Vec<ScalarValue>>,
}

struct CapturedIncrementalColumnarSource {
    storage_id: StorageId,
    table: TableDef,
    token: netbadb_storage::StorageSnapshotToken,
    cursor: ChangeStreamCursor,
    rows: Vec<(StorageVersionKey, Vec<ScalarValue>)>,
}

pub struct Database {
    committed: schema_catalog::CommittedCatalogState,
    bindings: PhysicalBindings,
    registry: StorageRegistry,
    projections: ProjectionRegistry,
    transaction_owner: Rc<()>,
    next_transaction_id: DatabaseTxnId,
    coordinator: Option<SharedCoordinatorLog>,
    published_visibility: Option<SharedPublishedVisibility>,
    catalog_generation: u64,
    catalog_path: Option<PathBuf>,
    mutation_journal: Option<schema_mutation::SharedMutationJournal>,
    schema_writer: schema_mutation::SchemaWriter,
    maintenance_cursor: Option<maintenance::MaintenanceCursor>,
}

fn current_visibility_boundaries(
    registry: &StorageRegistry,
) -> Result<Vec<StorageVisibilityBoundary>, DatabaseError> {
    registry
        .iter()
        .map(|entry| {
            entry
                .storage
                .current_visibility_boundary()
                .map_err(Into::into)
        })
        .collect()
}

fn initial_published_visibility(
    coordinator: &CoordinatorLog,
    registry: &StorageRegistry,
) -> Result<Option<SharedPublishedVisibility>, DatabaseError> {
    if !coordinator.global_visibility_enabled() {
        return Ok(None);
    }
    let snapshot = DatabaseSnapshot::new(
        coordinator.published_commit_seq(),
        current_visibility_boundaries(registry)?,
    )?;
    Ok(Some(Rc::new(RefCell::new(PublishedVisibility {
        snapshot,
    }))))
}

impl Database {
    #[must_use]
    pub fn visibility_mode(&self) -> DatabaseVisibilityMode {
        if self.published_visibility.is_some() {
            DatabaseVisibilityMode::Global
        } else {
            DatabaseVisibilityMode::LegacyLocal
        }
    }

    pub fn current_database_snapshot(&self) -> Result<Option<DatabaseSnapshot>, DatabaseError> {
        self.published_visibility
            .as_ref()
            .map(|published| {
                published
                    .try_borrow()
                    .map(|state| state.snapshot.clone())
                    .map_err(|_| CoordinatorError::PublishedVisibilityBusy.into())
            })
            .transpose()
    }

    pub fn inspect_global_visibility(&self) -> Result<DatabaseVisibilityInspection, DatabaseError> {
        let Some(snapshot) = self.current_database_snapshot()? else {
            return Ok(DatabaseVisibilityInspection {
                mode: DatabaseVisibilityMode::LegacyLocal,
                published_commit_seq: None,
                next_commit_seq: None,
                boundaries: Vec::new(),
            });
        };
        let next_commit_seq = self
            .coordinator
            .as_ref()
            .ok_or(CoordinatorError::DurableCoordinatorRequired)?
            .try_borrow()
            .map_err(|_| CoordinatorError::CoordinatorBusy)?
            .next_commit_seq()?;
        Ok(DatabaseVisibilityInspection {
            mode: DatabaseVisibilityMode::Global,
            published_commit_seq: Some(snapshot.commit_seq()),
            next_commit_seq: Some(next_commit_seq),
            boundaries: snapshot
                .boundaries()
                .iter()
                .map(|boundary| VisibilityBoundaryInspection {
                    storage_id: boundary.storage_id(),
                    storage_kind: boundary.storage_kind(),
                    boundary: boundary.value(),
                })
                .collect(),
        })
    }

    /// Irreversibly enables durable database-global snapshot publication.
    /// The transition requires no outstanding database transaction handles.
    pub fn enable_global_visibility(&mut self) -> Result<(), DatabaseError> {
        if self.published_visibility.is_some() {
            return Ok(());
        }
        let outstanding = Rc::strong_count(&self.transaction_owner).saturating_sub(1);
        if outstanding != 0 {
            return Err(CoordinatorError::GlobalEnableRequiresQuiescence { outstanding }.into());
        }
        let boundaries = current_visibility_boundaries(&self.registry)?;
        let coordinator = self
            .coordinator
            .as_ref()
            .ok_or(CoordinatorError::DurableCoordinatorRequired)?;
        coordinator
            .try_borrow_mut()
            .map_err(|_| CoordinatorError::CoordinatorBusy)?
            .enable_global_visibility()?;
        self.published_visibility = Some(Rc::new(RefCell::new(PublishedVisibility {
            snapshot: DatabaseSnapshot::new(DatabaseCommitSeq(0), boundaries)?,
        })));
        Ok(())
    }

    fn configure_managed_projection_catalog(&mut self, incarnation: [u8; 16]) {
        let Some(schema_catalog_path) = self.catalog_path.as_deref() else {
            return;
        };
        let catalog_path = projection_catalog::catalog_path(schema_catalog_path);
        let mut catalog = match projection_catalog::ProjectionCatalog::open_or_initialize(
            schema_catalog_path,
            incarnation,
        ) {
            Ok(catalog) => catalog,
            Err(error) => {
                self.projections = ProjectionRegistry::degraded(catalog_path, error.to_string());
                return;
            }
        };
        let mut entries = Vec::with_capacity(catalog.entries().len());
        for mut identity in catalog.entries().to_vec() {
            let directory = catalog.resolve(&identity);
            let opened = (|| -> Result<ColumnarProjection, String> {
                let table = self
                    .committed
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == identity.table_id)
                    .ok_or_else(|| {
                        "projection table is absent from the authoritative schema".to_owned()
                    })?;
                if table.fingerprint().map_err(|error| error.to_string())?
                    != identity.schema_fingerprint
                {
                    return Err("projection schema fingerprint is stale".to_owned());
                }
                let expected_storage = match self
                    .bindings
                    .placement(identity.table_id)
                    .map_err(|error| error.to_string())?
                {
                    TablePlacement::Single { storage_id, .. } => *storage_id,
                    TablePlacement::RangePartitioned { .. } => {
                        return Err("partitioned projections are unsupported".to_owned());
                    }
                };
                if expected_storage != identity.source_storage_id
                    || self.registry.get(expected_storage).is_none()
                {
                    return Err("projection source storage identity is unavailable".to_owned());
                }
                let projection = ColumnarProjection::open(&directory, table)
                    .map_err(|error| error.to_string())?;
                let metadata = projection.metadata();
                if metadata.id != identity.id
                    || metadata.table_id != identity.table_id
                    || metadata.source_storage_id != identity.source_storage_id
                    || metadata.schema_fingerprint != identity.schema_fingerprint
                {
                    return Err("projection manifest differs from its catalog identity".to_owned());
                }
                if metadata.generation.0 < identity.generation.0 {
                    return Err("projection manifest generation moved backwards".to_owned());
                }
                Ok(projection)
            })();
            let (projection, detail) = match opened {
                Ok(projection) => {
                    if projection.metadata().generation.0 > identity.generation.0 {
                        let generation = projection.metadata().generation;
                        let _ = catalog.update_generation(identity.id, generation);
                        identity.generation = generation;
                    }
                    (Some(projection), None)
                }
                Err(detail) => (None, Some(detail)),
            };
            entries.push(ProjectionRegistryEntry {
                identity,
                directory,
                projection,
                detail,
                managed: true,
            });
        }
        self.projections = ProjectionRegistry::managed(catalog, entries);
    }

    /// Creates an explicit mixed Heap/LSM catalog without a durable database
    /// coordinator. As with the legacy API, at most one storage may be written
    /// by a transaction.
    fn physical_create_storages(specs: Vec<TableStorageCreateSpec>) -> Result<Self, DatabaseError> {
        let (schema, storages) = create_explicit_storages(specs)?;
        Self::compose(schema, storages)
    }

    /// Creates an explicit mixed Heap/LSM catalog whose multi-storage writes
    /// use the shared durable coordinator.
    fn physical_create_storages_with_coordinator(
        specs: Vec<TableStorageCreateSpec>,
        config: DatabaseCoordinatorConfig,
    ) -> Result<Self, DatabaseError> {
        validate_explicit_coordinator_path_create(&specs, &config)?;
        let cleanup_specs = specs.clone();
        let (schema, storages) = create_explicit_storages(specs)?;
        let mut coordinator = match CoordinatorLog::create(config.log_path()) {
            Ok(coordinator) => coordinator,
            Err(error) => {
                drop(storages);
                cleanup_explicit_specs(&cleanup_specs);
                return Err(error.into());
            }
        };
        if config.global_visibility {
            coordinator.enable_global_visibility()?;
        }
        Self::compose_with_coordinator(schema, storages, coordinator, DatabaseTxnId(1))
    }

    /// Opens an explicit mixed Heap/LSM catalog without coordinator recovery.
    /// Any prepared participant is therefore a typed in-doubt error.
    fn physical_open_storages(
        specs: Vec<TableStorageOpenSpec>,
        committed: CommittedCatalogState,
    ) -> Result<Self, DatabaseError> {
        validate_open_specs(&specs)?;
        let mut storages = Vec::with_capacity(specs.len());
        for spec in specs {
            storages.push(match spec {
                TableStorageOpenSpec::Heap { path, table } => TableStorage::open_heap(path, table)?,
                TableStorageOpenSpec::Lsm { directory, table } => {
                    TableStorage::open_lsm(directory, table)?
                }
            });
        }
        Self::compose_recovered(committed, storages, None, None)
    }

    /// Opens an explicit mixed Heap/LSM catalog, validates every durable
    /// participant identity before mutation, and resolves prepared work from
    /// the coordinator decision log.
    fn physical_open_storages_with_coordinator(
        specs: Vec<TableStorageOpenSpec>,
        config: DatabaseCoordinatorConfig,
        committed: CommittedCatalogState,
        placements: Option<PartitionCatalog>,
    ) -> Result<Self, DatabaseError> {
        validate_open_specs(&specs)?;
        if specs.iter().any(|spec| spec.path() == config.log_path()) {
            return Err(DatabaseError::CoordinatorPathConflictsWithStorage(
                config.log_path().to_owned(),
            ));
        }
        let mut coordinator = CoordinatorLog::open(config.log_path())?;
        let mut decisions = coordinator.decisions().cloned().collect::<Vec<_>>();
        decisions.sort_by_key(|decision| {
            decision
                .commit_seq
                .map_or((0_u8, 0_u64), |sequence| (1, sequence.0))
        });
        let mut inspected = Vec::with_capacity(specs.len());
        for spec in &specs {
            let recovery = match spec {
                TableStorageOpenSpec::Heap { path, table } => {
                    let recovery = TableStorage::inspect_heap_recovery(path, table)?;
                    GenericRecoveryInspection {
                        storage_id: recovery.storage_id,
                        prepared_transactions: recovery.prepared_transactions,
                    }
                }
                TableStorageOpenSpec::Lsm { directory, table } => {
                    let recovery = TableStorage::inspect_lsm_recovery(directory, table)?;
                    GenericRecoveryInspection {
                        storage_id: recovery.storage_id,
                        prepared_transactions: recovery.prepared_transactions,
                    }
                }
            };
            inspected.push(GenericInspectedStorage { recovery });
        }
        validate_generic_coordinator_recovery(&decisions, &inspected, &config.retired_storage_ids)?;

        let mut maximum_database_txn_id = decisions
            .iter()
            .map(|decision| decision.database_txn_id.0)
            .max()
            .unwrap_or(0);
        let mut storages = Vec::with_capacity(specs.len());
        for (spec, inspected_storage) in specs.into_iter().zip(&inspected) {
            let mut resolutions = Vec::new();
            for prepared in &inspected_storage.recovery.prepared_transactions {
                maximum_database_txn_id = maximum_database_txn_id.max(prepared.database_txn_id.0);
                resolutions.push(resolution_for_prepared(
                    prepared,
                    inspected_storage.recovery.storage_id,
                    &decisions,
                )?);
            }
            storages.push(match spec {
                TableStorageOpenSpec::Heap { path, table } => {
                    TableStorage::open_heap_with_prepared_resolutions(path, table, &resolutions)?
                }
                TableStorageOpenSpec::Lsm { directory, table } => {
                    TableStorage::open_lsm_with_prepared_resolutions(
                        directory,
                        table,
                        &resolutions,
                    )?
                }
            });
        }
        for decision in &decisions {
            if !decision.complete && decision.schema.is_none() {
                if let Some(commit_seq) = decision.commit_seq {
                    coordinator.complete_sequenced(decision.database_txn_id, commit_seq)?;
                } else {
                    coordinator.complete(decision.database_txn_id)?;
                }
            }
        }
        let next_transaction_id = DatabaseTxnId(
            maximum_database_txn_id
                .checked_add(1)
                .ok_or(CoordinatorError::TransactionIdExhausted)?,
        );
        Self::compose_recovered(
            committed,
            storages,
            Some((coordinator, next_transaction_id)),
            placements,
        )
    }

    /// Creates one heap file per validated table and composes them into one
    /// query catalog. Each heap persists the table's schema fingerprint.
    fn physical_create_tables(tables: Vec<(PathBuf, TableDef)>) -> Result<Self, DatabaseError> {
        validate_catalog_paths(&tables)?;
        let schema = Schema::new(tables.iter().map(|(_, table)| table.clone()).collect())?;
        let mut storages = Vec::with_capacity(tables.len());
        let mut created_paths = Vec::with_capacity(tables.len());
        for (position, (path, table)) in tables.into_iter().enumerate() {
            let storage_id = storage_id_for_position(position)?;
            match TableStorage::create_heap_with_storage_id(&path, table, storage_id) {
                Ok(storage) => {
                    storages.push(storage);
                    created_paths.push(path);
                }
                Err(creation) => {
                    // All paths in `created_paths` were created successfully by
                    // this invocation. Release their handles before removing
                    // only those exact database and WAL files.
                    drop(storages);
                    if let Some((cleanup_path, cleanup)) =
                        cleanup_created_table_files(&created_paths)
                    {
                        return Err(DatabaseError::CreateTablesRollback {
                            creation,
                            cleanup_path,
                            cleanup,
                        });
                    }
                    return Err(creation.into());
                }
            }
        }
        Self::compose(schema, storages)
    }

    /// Creates a catalog with an explicit durable database coordinator log.
    /// Only databases opened through this API permit atomic multi-storage writes.
    fn physical_create_tables_with_coordinator(
        tables: Vec<(PathBuf, TableDef)>,
        config: DatabaseCoordinatorConfig,
    ) -> Result<Self, DatabaseError> {
        validate_catalog_paths(&tables)?;
        validate_coordinator_path(&tables, &config)?;
        let schema = Schema::new(tables.iter().map(|(_, table)| table.clone()).collect())?;
        let mut storages = Vec::with_capacity(tables.len());
        let mut created_paths = Vec::with_capacity(tables.len());
        for (position, (path, table)) in tables.into_iter().enumerate() {
            let storage_id = storage_id_for_position(position)?;
            match TableStorage::create_heap_with_storage_id(&path, table, storage_id) {
                Ok(storage) => {
                    storages.push(storage);
                    created_paths.push(path);
                }
                Err(creation) => {
                    drop(storages);
                    if let Some((cleanup_path, cleanup)) =
                        cleanup_created_table_files(&created_paths)
                    {
                        return Err(DatabaseError::CreateTablesRollback {
                            creation,
                            cleanup_path,
                            cleanup,
                        });
                    }
                    return Err(creation.into());
                }
            }
        }
        let mut coordinator = match CoordinatorLog::create(config.log_path()) {
            Ok(coordinator) => coordinator,
            Err(error) => {
                drop(storages);
                let _ = cleanup_created_table_files(&created_paths);
                return Err(error.into());
            }
        };
        if config.global_visibility {
            coordinator.enable_global_visibility()?;
        }
        Self::compose_with_coordinator(schema, storages, coordinator, DatabaseTxnId(1))
    }

    /// Opens one existing heap-format file per table as a single query catalog.
    fn physical_open_tables(
        tables: Vec<(PathBuf, TableDef)>,
        committed: CommittedCatalogState,
    ) -> Result<Self, DatabaseError> {
        validate_catalog_paths(&tables)?;
        let mut storages = Vec::with_capacity(tables.len());
        for (path, table) in tables {
            storages.push(TableStorage::open_heap(path, table)?);
        }
        Self::compose_recovered(committed, storages, None, None)
    }

    /// Opens a coordinator-enabled database after resolving every prepared
    /// participant from the durable database decision log.
    fn physical_open_tables_with_coordinator(
        tables: Vec<(PathBuf, TableDef)>,
        config: DatabaseCoordinatorConfig,
        committed: CommittedCatalogState,
    ) -> Result<Self, DatabaseError> {
        validate_catalog_paths(&tables)?;
        validate_coordinator_path(&tables, &config)?;
        let mut coordinator = CoordinatorLog::open(config.log_path())?;
        let mut decisions = coordinator.decisions().cloned().collect::<Vec<_>>();
        decisions.sort_by_key(|decision| {
            decision
                .commit_seq
                .map_or((0_u8, 0_u64), |sequence| (1, sequence.0))
        });
        let mut inspected = Vec::with_capacity(tables.len());
        for (path, table) in &tables {
            inspected.push(InspectedStorage {
                path: path.clone(),
                table: table.clone(),
                recovery: TableStorage::inspect_heap_recovery(path, table)?,
            });
        }
        validate_coordinator_recovery(&decisions, &inspected, &config.retired_storage_ids)?;

        let mut maximum_database_txn_id = decisions
            .iter()
            .map(|decision| decision.database_txn_id.0)
            .max()
            .unwrap_or(0);
        let mut storages = Vec::with_capacity(inspected.len());
        for storage in inspected {
            let mut resolutions = Vec::new();
            for prepared in &storage.recovery.prepared_transactions {
                maximum_database_txn_id = maximum_database_txn_id.max(prepared.database_txn_id.0);
                let decision = decisions
                    .iter()
                    .find(|decision| decision.database_txn_id == prepared.database_txn_id);
                let resolution = if let Some(decision) = decision {
                    let participant_matches = decision.participants.iter().any(|participant| {
                        participant.storage_id == storage.recovery.storage_id
                            && participant.physical_txn_id == prepared.physical_txn_id
                    });
                    if !participant_matches {
                        return Err(DatabaseError::PreparedParticipantMismatch {
                            database_txn_id: prepared.database_txn_id,
                            storage_id: storage.recovery.storage_id,
                            physical_txn_id: prepared.physical_txn_id,
                        });
                    }
                    if prepared.state == PreparedTransactionState::RolledBack {
                        return Err(DatabaseError::PreparedParticipantMismatch {
                            database_txn_id: prepared.database_txn_id,
                            storage_id: storage.recovery.storage_id,
                            physical_txn_id: prepared.physical_txn_id,
                        });
                    }
                    PreparedDecision::Commit
                } else {
                    PreparedDecision::Abort
                };
                resolutions.push(PreparedTxnResolution {
                    database_txn_id: prepared.database_txn_id,
                    physical_txn_id: prepared.physical_txn_id,
                    decision: resolution,
                });
            }
            storages.push(TableStorage::open_heap_with_prepared_resolutions(
                storage.path,
                storage.table,
                &resolutions,
            )?);
        }
        for decision in &decisions {
            if !decision.complete && decision.schema.is_none() {
                if let Some(commit_seq) = decision.commit_seq {
                    coordinator.complete_sequenced(decision.database_txn_id, commit_seq)?;
                } else {
                    coordinator.complete(decision.database_txn_id)?;
                }
            }
        }
        let next_transaction_id = DatabaseTxnId(
            maximum_database_txn_id
                .checked_add(1)
                .ok_or(CoordinatorError::TransactionIdExhausted)?,
        );
        Self::compose_recovered(
            committed,
            storages,
            Some((coordinator, next_transaction_id)),
            None,
        )
    }

    /// Creates a mixed catalog of single and RANGE-partitioned logical tables.
    /// The explicit catalog and coordinator paths are never inferred from heap
    /// locations. All partitioned writes therefore use the durable database
    /// coordinator from their first release.
    fn physical_create_with_placements(
        specs: Vec<TablePlacementSpec>,
        config: PartitionCatalogConfig,
    ) -> Result<Self, DatabaseError> {
        if specs.is_empty() {
            return Err(DatabaseError::EmptyCatalog);
        }
        let schema = Schema::new(specs.iter().map(|spec| spec.table().clone()).collect())?;
        let mut paths = Vec::new();
        for spec in &specs {
            match spec {
                TablePlacementSpec::Single { path, .. } => paths.push(path.clone()),
                TablePlacementSpec::RangePartitioned { partitions, .. } => {
                    paths.extend(partitions.iter().map(|partition| partition.path.clone()));
                }
            }
        }
        validate_physical_paths(&paths, &config)?;
        prevalidate_placement_specs(&specs)?;

        let mut storages = Vec::with_capacity(paths.len());
        let mut created_paths = Vec::with_capacity(paths.len());
        let mut catalog_tables = Vec::with_capacity(specs.len());
        let mut next_storage_ordinal = 1_u64;
        for spec in specs {
            let table = spec.table().clone();
            let fingerprint = table.fingerprint()?;
            let placement = match spec {
                TablePlacementSpec::Single { path, table } => {
                    let storage_id = StorageId(next_storage_ordinal);
                    next_storage_ordinal = next_storage_ordinal
                        .checked_add(1)
                        .ok_or(StorageRegistryError::StorageIdExhausted)?;
                    create_partition_storage(
                        &path,
                        table.clone(),
                        storage_id,
                        &mut storages,
                        &mut created_paths,
                    )?;
                    TablePlacement::Single {
                        table_id: table.id,
                        storage_id,
                    }
                }
                TablePlacementSpec::RangePartitioned {
                    table,
                    partition_key,
                    partitions,
                } => {
                    let key_type = validate_partition_key(&table, partition_key)?;
                    let mut bindings = Vec::with_capacity(partitions.len());
                    for partition in partitions {
                        let storage_id = StorageId(next_storage_ordinal);
                        next_storage_ordinal = next_storage_ordinal
                            .checked_add(1)
                            .ok_or(StorageRegistryError::StorageIdExhausted)?;
                        create_partition_storage(
                            &partition.path,
                            table.clone(),
                            storage_id,
                            &mut storages,
                            &mut created_paths,
                        )?;
                        bindings.push(RangePartitionBinding {
                            partition_id: partition.partition_id,
                            storage_id,
                            lower: partition.lower,
                            upper: partition.upper,
                        });
                    }
                    let partitions = canonicalize_partitions(key_type, bindings)?;
                    TablePlacement::RangePartitioned {
                        table_id: table.id,
                        partition_key,
                        key_type,
                        partitions,
                    }
                }
            };
            catalog_tables.push(CatalogTable {
                table_id: table.id,
                schema_fingerprint: fingerprint,
                placement,
            });
        }

        let catalog_path_existed = config.catalog_path().exists();
        let catalog = match PartitionCatalog::create(config.catalog_path(), catalog_tables) {
            Ok(catalog) => catalog,
            Err(error) => {
                drop(storages);
                if !catalog_path_existed {
                    let _ = std::fs::remove_file(config.catalog_path());
                }
                let _ = cleanup_created_table_files(&created_paths);
                return Err(error.into());
            }
        };
        let coordinator = match CoordinatorLog::create(config.coordinator_log_path()) {
            Ok(coordinator) => coordinator,
            Err(error) => {
                drop(storages);
                let _ = std::fs::remove_file(config.catalog_path());
                let _ = cleanup_created_table_files(&created_paths);
                return Err(error.into());
            }
        };
        Self::compose_with_catalog(schema, storages, catalog, coordinator, DatabaseTxnId(1))
    }

    /// Opens a durable placement catalog from schemas plus an unordered set of
    /// physical heap paths. Heap identity, not caller order or path, resolves
    /// every partition before prepared transactions are recovered.
    fn physical_open_with_placements(
        tables: Vec<TableDef>,
        storage_paths: Vec<PathBuf>,
        config: PartitionCatalogConfig,
        committed: CommittedCatalogState,
    ) -> Result<Self, DatabaseError> {
        validate_physical_paths(&storage_paths, &config)?;
        let catalog = PartitionCatalog::open(config.catalog_path())?;
        validate_catalog_schemas(&catalog, &tables)?;
        let mut coordinator = CoordinatorLog::open(config.coordinator_log_path())?;
        let mut decisions = coordinator.decisions().cloned().collect::<Vec<_>>();
        decisions.sort_by_key(|decision| {
            decision
                .commit_seq
                .map_or((0_u8, 0_u64), |sequence| (1, sequence.0))
        });

        let mut inspected = Vec::with_capacity(storage_paths.len());
        for path in storage_paths {
            let identity = TableStorage::inspect_heap_identity(&path)?;
            let table = tables
                .iter()
                .find(|table| table.id == identity.table_id)
                .ok_or(PartitionError::PartitionStorageMismatch(
                    identity.storage_id,
                ))?;
            if table.fingerprint()? != identity.schema_fingerprint {
                return Err(PartitionError::PartitionStorageMismatch(identity.storage_id).into());
            }
            let expected = catalog.tables.iter().any(|entry| {
                entry.table_id == identity.table_id
                    && entry
                        .placement
                        .storage_ids()
                        .any(|storage_id| storage_id == identity.storage_id)
            });
            if !expected {
                return Err(PartitionError::PartitionStorageMismatch(identity.storage_id).into());
            }
            let recovery = TableStorage::inspect_heap_recovery(&path, table)?;
            inspected.push(InspectedStorage {
                path,
                table: table.clone(),
                recovery,
            });
        }
        validate_catalog_storage_set(&catalog, &inspected)?;
        validate_coordinator_recovery(&decisions, &inspected, &[])?;

        let mut maximum_database_txn_id = decisions
            .iter()
            .map(|decision| decision.database_txn_id.0)
            .max()
            .unwrap_or(0);
        let mut storages = Vec::with_capacity(inspected.len());
        for storage in inspected {
            let mut resolutions = Vec::new();
            for prepared in &storage.recovery.prepared_transactions {
                maximum_database_txn_id = maximum_database_txn_id.max(prepared.database_txn_id.0);
                let decision = decisions
                    .iter()
                    .find(|decision| decision.database_txn_id == prepared.database_txn_id);
                let resolution = if let Some(decision) = decision {
                    if !decision.participants.iter().any(|participant| {
                        participant.storage_id == storage.recovery.storage_id
                            && participant.physical_txn_id == prepared.physical_txn_id
                    }) || prepared.state == PreparedTransactionState::RolledBack
                    {
                        return Err(DatabaseError::PreparedParticipantMismatch {
                            database_txn_id: prepared.database_txn_id,
                            storage_id: storage.recovery.storage_id,
                            physical_txn_id: prepared.physical_txn_id,
                        });
                    }
                    PreparedDecision::Commit
                } else {
                    PreparedDecision::Abort
                };
                resolutions.push(PreparedTxnResolution {
                    database_txn_id: prepared.database_txn_id,
                    physical_txn_id: prepared.physical_txn_id,
                    decision: resolution,
                });
            }
            storages.push(TableStorage::open_heap_with_prepared_resolutions(
                storage.path,
                storage.table,
                &resolutions,
            )?);
        }
        for decision in &decisions {
            if !decision.complete && decision.schema.is_none() {
                if let Some(commit_seq) = decision.commit_seq {
                    coordinator.complete_sequenced(decision.database_txn_id, commit_seq)?;
                } else {
                    coordinator.complete(decision.database_txn_id)?;
                }
            }
        }
        let next_transaction_id = DatabaseTxnId(
            maximum_database_txn_id
                .checked_add(1)
                .ok_or(CoordinatorError::TransactionIdExhausted)?,
        );
        Self::compose_recovered(
            committed,
            storages,
            Some((coordinator, next_transaction_id)),
            Some(catalog),
        )
    }

    fn compose(schema: Schema, storages: Vec<TableStorage>) -> Result<Self, DatabaseError> {
        let (registry, bindings) = StorageRegistry::from_catalog_order(storages)?;
        Ok(Self {
            committed: crate::schema_catalog::CommittedCatalogState::initial(
                schema,
                bindings.iter().cloned(),
            ),
            bindings,
            registry,
            projections: ProjectionRegistry::new(),
            transaction_owner: Rc::new(()),
            next_transaction_id: DatabaseTxnId(1),
            coordinator: None,
            published_visibility: None,
            catalog_generation: 0,
            catalog_path: None,
            mutation_journal: None,
            schema_writer: Rc::new(std::cell::Cell::new(None)),
            maintenance_cursor: None,
        })
    }

    fn compose_with_coordinator(
        schema: Schema,
        storages: Vec<TableStorage>,
        coordinator: CoordinatorLog,
        next_transaction_id: DatabaseTxnId,
    ) -> Result<Self, DatabaseError> {
        let (registry, bindings) = StorageRegistry::from_catalog_order(storages)?;
        let published_visibility = initial_published_visibility(&coordinator, &registry)?;
        Ok(Self {
            committed: crate::schema_catalog::CommittedCatalogState::initial(
                schema,
                bindings.iter().cloned(),
            ),
            bindings,
            registry,
            projections: ProjectionRegistry::new(),
            transaction_owner: Rc::new(()),
            next_transaction_id,
            coordinator: Some(Rc::new(RefCell::new(coordinator))),
            published_visibility,
            catalog_generation: 0,
            catalog_path: None,
            mutation_journal: None,
            schema_writer: Rc::new(std::cell::Cell::new(None)),
            maintenance_cursor: None,
        })
    }

    fn compose_with_catalog(
        schema: Schema,
        storages: Vec<TableStorage>,
        catalog: PartitionCatalog,
        coordinator: CoordinatorLog,
        next_transaction_id: DatabaseTxnId,
    ) -> Result<Self, DatabaseError> {
        let registry = StorageRegistry::new(
            storages
                .into_iter()
                .map(|storage| StorageRegistryEntry {
                    id: storage.storage_id(),
                    storage,
                })
                .collect(),
        )?;
        let bindings = PhysicalBindings::new(
            catalog
                .tables
                .into_iter()
                .map(|entry| entry.placement)
                .collect(),
            &registry,
        )?;
        let published_visibility = initial_published_visibility(&coordinator, &registry)?;
        Ok(Self {
            committed: crate::schema_catalog::CommittedCatalogState::initial(
                schema,
                bindings.iter().cloned(),
            ),
            bindings,
            registry,
            projections: ProjectionRegistry::new(),
            transaction_owner: Rc::new(()),
            next_transaction_id,
            coordinator: Some(Rc::new(RefCell::new(coordinator))),
            published_visibility,
            catalog_generation: 0,
            catalog_path: None,
            mutation_journal: None,
            schema_writer: Rc::new(std::cell::Cell::new(None)),
            maintenance_cursor: None,
        })
    }

    // Recovery must carry stored lineage, including historical high-waters.
    // Only fresh bootstrap composition may compute initial successors.
    fn compose_recovered(
        committed: CommittedCatalogState,
        storages: Vec<TableStorage>,
        coordinator: Option<(CoordinatorLog, DatabaseTxnId)>,
        catalog: Option<PartitionCatalog>,
    ) -> Result<Self, DatabaseError> {
        let registry = StorageRegistry::new(
            storages
                .into_iter()
                .map(|storage| StorageRegistryEntry {
                    id: storage.storage_id(),
                    storage,
                })
                .collect(),
        )?;
        let bindings = match catalog {
            Some(catalog) => PhysicalBindings::new(
                catalog
                    .tables
                    .into_iter()
                    .map(|entry| entry.placement)
                    .collect(),
                &registry,
            )?,
            None => PhysicalBindings::from_single_storages(&registry)?,
        };
        if bindings.iter().count() != committed.schema.tables().len() {
            return Err(
                SchemaCatalogError::InventoryMismatch("committed table binding count").into(),
            );
        }
        for table in committed.schema.tables() {
            for storage_id in bindings.placement(table.id)?.storage_ids() {
                let storage = registry
                    .get(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                if storage.table() != table {
                    return Err(SchemaCatalogError::InventoryMismatch(
                        "recovered logical schema differs from catalog",
                    )
                    .into());
                }
            }
        }
        let (coordinator, published_visibility, next_transaction_id) = match coordinator {
            Some((log, next)) => {
                let visibility = initial_published_visibility(&log, &registry)?;
                (Some(Rc::new(RefCell::new(log))), visibility, next)
            }
            None => (None, None, DatabaseTxnId(1)),
        };
        Ok(Self {
            committed,
            bindings,
            registry,
            projections: ProjectionRegistry::new(),
            transaction_owner: Rc::new(()),
            next_transaction_id,
            coordinator,
            published_visibility,
            catalog_generation: 0,
            catalog_path: None,
            mutation_journal: None,
            schema_writer: Rc::new(std::cell::Cell::new(None)),
            maintenance_cursor: None,
        })
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<(), DatabaseError> {
        if self.published_visibility.is_some() {
            let mut transaction = self.begin_transaction()?;
            self.insert_in(&mut transaction, values)?;
            self.commit_transaction(&mut transaction)?;
            return Ok(());
        }
        self.ensure_schema_available(None)?;
        let storage_id = self.primary_storage_id()?;
        let table_id = self
            .registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .table()
            .id;
        let routed = self.route_storage_for_values(table_id, values)?;
        self.registry
            .get_mut(routed)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id: routed })?
            .insert(values)?;
        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction, DatabaseError> {
        self.begin_database_transaction(IsolationLevel::ReadCommitted)
    }

    pub fn begin_transaction_with_isolation(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction, DatabaseError> {
        self.begin_database_transaction(isolation_level)
    }

    /// Begins a database transaction after validating `table_id`'s current
    /// physical binding. Participant registration remains lazy.
    ///
    /// The table argument is retained for embedded/protocol compatibility; it
    /// does not bind the transaction to one physical storage.
    pub fn begin_transaction_for(
        &mut self,
        table_id: TableId,
    ) -> Result<Transaction, DatabaseError> {
        let _ = self.bindings.placement(table_id)?;
        self.begin_database_transaction(IsolationLevel::ReadCommitted)
    }

    pub fn begin_transaction_for_with_isolation(
        &mut self,
        table_id: TableId,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction, DatabaseError> {
        let _ = self.bindings.placement(table_id)?;
        self.begin_database_transaction(isolation_level)
    }

    pub fn insert_in(
        &mut self,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<(), DatabaseError> {
        let primary = self.primary_storage_id()?;
        let table_id = self
            .registry
            .get(primary)
            .ok_or(StorageRegistryError::UnknownStorageId {
                storage_id: primary,
            })?
            .table()
            .id;
        let storage_id = self.route_storage_for_values(table_id, values)?;
        self.validate_transaction(transaction)?;
        let context = transaction.write_context(storage_id, &mut self.registry)?;
        self.registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .insert_in(context, values)?;
        Ok(())
    }

    /// Inserts one row into a specific table using that heap's implicit
    /// transaction. This is the direct embedded data-loading counterpart to
    /// multi-table query catalogs.
    pub fn insert_into(
        &mut self,
        table_id: TableId,
        values: &[ScalarValue],
    ) -> Result<(), DatabaseError> {
        if self.published_visibility.is_some() {
            let mut transaction = self.begin_transaction_for(table_id)?;
            self.insert_into_in(table_id, &mut transaction, values)?;
            self.commit_transaction(&mut transaction)?;
            return Ok(());
        }
        self.ensure_schema_available(None)?;
        let storage_id = self.route_storage_for_values(table_id, values)?;
        self.registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .insert(values)?;
        Ok(())
    }

    /// Inserts into `table_id` through a database transaction. A participant
    /// for this storage is registered lazily, and a second physical writer is
    /// rejected before this method reaches storage mutation.
    pub fn insert_into_in(
        &mut self,
        table_id: TableId,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        let storage_id = self.route_storage_for_values_in(table_id, values, transaction)?;
        transaction.with_write_storage(storage_id, &mut self.registry, |storage, context| {
            storage.insert_in(context, values)
        })?;
        Ok(())
    }

    /// Flushes dirty pages and reports any write or sync failure.
    pub fn flush(&self) -> Result<(), DatabaseError> {
        self.ensure_schema_available(None)?;
        for entry in self.registry.iter() {
            entry.storage.flush()?;
        }
        Ok(())
    }

    /// Creates a quiescent checkpoint and recycles the previous WAL history.
    pub fn checkpoint(&mut self) -> Result<(), DatabaseError> {
        self.ensure_schema_available(None)?;
        for entry in self.registry.iter_mut() {
            entry.storage.checkpoint()?;
        }
        Ok(())
    }

    fn ensure_index_maintenance_quiescent(&self) -> Result<(), DatabaseError> {
        let handles = Rc::strong_count(&self.transaction_owner) - 1;
        if handles != 0 {
            return Err(StorageError::from(
                netbadb_storage::CheckpointError::OutstandingTransactions {
                    count: handles as u64,
                },
            )
            .into());
        }
        Ok(())
    }

    /// Explicit quiescent maintenance of one single-storage Heap index catalog.
    /// Drop all DatabaseTransaction handles first (including lazy participants);
    /// the Heap also enforces its existing checkpoint admission gate. Active
    /// indexes, statistics, reflection and catalog_generation remain unchanged.
    /// Owned retirements remain pending; legacy pages may be permanently abandoned.
    /// No physical page is truncated or reused.
    pub fn compact_index_catalog(
        &mut self,
        table_id: TableId,
    ) -> Result<IndexMaintenanceReport, DatabaseError> {
        self.ensure_index_maintenance_quiescent()?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .compact_index_catalog()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => Err(StorageError::UnsupportedOperation {
                operation: "index catalog compaction",
                storage_kind: "range-partitioned table",
            }
            .into()),
        }
    }

    /// Quiescent admin ownership inventory. Performs no truncate or PageId reuse;
    /// ordinary catalog inspection and PostgreSQL metadata remain active-only.
    pub fn inspect_index_reclaim(
        &mut self,
        table_id: TableId,
    ) -> Result<IndexReclaimReport, DatabaseError> {
        self.ensure_index_maintenance_quiescent()?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .inspect_index_reclaim()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => Err(StorageError::UnsupportedOperation {
                operation: "index reclaim inspection",
                storage_kind: "range-partitioned table",
            }
            .into()),
        }
    }

    /// Quiescent retired-owner candidate inspection; no allocation permission,
    /// SQL result or ordinary catalog metadata is exposed by this DTO.
    pub fn inspect_reusable_pages(
        &mut self,
        table_id: TableId,
    ) -> Result<PageReuseInspection, DatabaseError> {
        self.ensure_index_maintenance_quiescent()?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .inspect_reusable_pages()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => Err(StorageError::UnsupportedOperation {
                operation: "reusable page inspection",
                storage_kind: "range-partitioned table",
            }
            .into()),
        }
    }

    /// Explicitly adopts historical ordinary v3 orphans of all active indexes
    /// on one single-storage Heap table. Drop every DatabaseTransaction handle
    /// first, including resolved and lazy handles. Storage internally checkpoints,
    /// proves full reachability/ownership and commits same-allocation NBTR writes.
    /// No schema, planner/catalog generation, SQL surface or file length changes.
    /// An unresolved persistence failure requires reopen; it may have committed.
    pub fn adopt_historical_btree_orphans(
        &mut self,
        table_id: TableId,
    ) -> Result<HistoricalOrphanAdoptionReport, DatabaseError> {
        self.ensure_index_maintenance_quiescent()?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .adopt_historical_btree_orphans()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => Err(StorageError::UnsupportedOperation {
                operation: "historical BTree orphan adoption",
                storage_kind: "range-partitioned table",
            }
            .into()),
        }
    }

    /// Reclaims a validated complete-retired-v3 suffix through an internal
    /// checkpoint and durable intent. All database handles must first be dropped.
    /// On a persistence failure reopen the database before any further mutation.
    pub fn reclaim_retired_index_tail(
        &mut self,
        table_id: TableId,
    ) -> Result<IndexTailReclaimReport, DatabaseError> {
        self.ensure_index_maintenance_quiescent()?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .reclaim_retired_index_tail()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => Err(StorageError::UnsupportedOperation {
                operation: "retired index tail reclamation",
                storage_kind: "range-partitioned table",
            }
            .into()),
        }
    }

    /// Drives synchronous history-preserving leveled compaction for one LSM.
    /// Heap and partitioned layouts return a typed unsupported error.
    pub fn compact(&mut self, table_id: TableId) -> Result<(), DatabaseError> {
        self.ensure_schema_available(None)?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .compact()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => {
                Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into())
            }
        }
    }

    /// Runs synchronous quiescent full-history LSM compaction and safe GC.
    pub fn compact_full(&mut self, table_id: TableId) -> Result<(), DatabaseError> {
        self.ensure_schema_available(None)?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => self
                .registry
                .get_mut(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .compact_full()
                .map_err(Into::into),
            TablePlacement::RangePartitioned { .. } => {
                Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into())
            }
        }
    }

    /// Atomically backfills and registers one non-unique single-column index.
    /// Subsequent heap and SQL DML maintain the index in the same transaction.
    pub fn create_index(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, DatabaseError> {
        if self.published_visibility.is_some() {
            return self.create_index_globally(None, table_id, column_id);
        }
        self.ensure_schema_available(None)?;
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => {
                let storage_id = *storage_id;
                let floor = self.index_allocation_floor(table_id, storage_id)?;
                Ok(self
                    .registry
                    .get_mut(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                    .create_index_from_floor(None, column_id, floor)?)
            }
            TablePlacement::RangePartitioned { .. } => {
                Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into())
            }
        }
    }

    /// Creates a durable logically named non-unique single-column B+Tree.
    pub fn create_named_index(
        &mut self,
        name: IndexName,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, DatabaseError> {
        if self.published_visibility.is_some() {
            return self.create_index_globally(Some(name), table_id, column_id);
        }
        self.ensure_schema_available(None)?;
        if self.registry.iter().any(|entry| {
            entry
                .storage
                .indexes()
                .iter()
                .any(|definition| definition.name.as_ref() == Some(&name))
        }) {
            return Err(DatabaseError::DuplicateIndexName(name));
        }
        let definition = match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => {
                let storage_id = *storage_id;
                let floor = self.index_allocation_floor(table_id, storage_id)?;
                self.registry
                    .get_mut(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                    .create_index_from_floor(Some(name), column_id, floor)?
            }
            TablePlacement::RangePartitioned { .. } => {
                return Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into());
            }
        };
        self.catalog_generation = self.catalog_generation.saturating_add(1);
        Ok(definition)
    }

    fn create_index_globally(
        &mut self,
        name: Option<IndexName>,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, DatabaseError> {
        self.ensure_schema_available(None)?;
        if let Some(name) = &name
            && self.registry.iter().any(|entry| {
                entry
                    .storage
                    .indexes()
                    .iter()
                    .any(|definition| definition.name.as_ref() == Some(name))
            })
        {
            return Err(DatabaseError::DuplicateIndexName(name.clone()));
        }
        let storage_id = match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into());
            }
        };
        let floor = self.index_allocation_floor(table_id, storage_id)?;
        let mut transaction = self.begin_transaction_for(table_id)?;
        let context = transaction.write_context(storage_id, &mut self.registry)?;
        let definition = {
            let storage = self
                .registry
                .get_mut(storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
            storage.advance_index_id_floor_in(context, floor)?;
            match name {
                Some(name) => storage.create_named_index_in(context, name, column_id)?,
                None => storage.create_index_in(context, column_id)?,
            }
        };
        transaction.stage_index(storage_id, definition.clone());
        self.commit_transaction(&mut transaction)?;
        Ok(definition)
    }

    fn index_allocation_floor(
        &mut self,
        table_id: TableId,
        storage_id: StorageId,
    ) -> Result<netbadb_types::IndexId, DatabaseError> {
        let committed = self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .heap_rewrite_indexes()?
            .next_index_id;
        self.mutation_journal
            .as_ref()
            .map_or(Ok(committed), |journal| {
                journal
                    .borrow()
                    .effective_index(table_id, committed)
                    .ok_or_else(|| SchemaMutationError::IdentityExhausted("IndexId").into())
            })
    }

    /// Durably retires one table-scoped logical index. Physical tree space is
    /// retained; ordinary inspection, planning, and DML expose only active trees.
    pub fn drop_index(
        &mut self,
        table_id: TableId,
        id: netbadb_types::IndexId,
    ) -> Result<(), DatabaseError> {
        if self.published_visibility.is_some() {
            let mut transaction = self.begin_transaction_for(table_id)?;
            self.drop_index_in(&mut transaction, table_id, id)?;
            self.commit_transaction(&mut transaction)?;
            return Ok(());
        }
        let storage_id = self.index_ddl_storage(table_id)?;
        self.registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .drop_index(id)?;
        self.catalog_generation = self.catalog_generation.saturating_add(1);
        Ok(())
    }

    /// Stages retirement without changing published paths. The caller must use
    /// Database::commit_transaction to publish, or roll back the transaction.
    pub fn drop_index_in(
        &mut self,
        transaction: &mut Transaction,
        table_id: TableId,
        id: netbadb_types::IndexId,
    ) -> Result<(), DatabaseError> {
        if transaction.schema_composition.is_started() {
            return if transaction.schema_composition.is_sealed() {
                Err(SchemaMutationError::SchemaMutationAfterMaterialization.into())
            } else {
                Err(DatabaseError::UnsupportedDdlCombination)
            };
        }
        if transaction.schema_mutation.is_some() {
            return Err(DatabaseError::UnsupportedDdlCombination);
        }
        self.validate_transaction(transaction)?;
        if transaction.has_pending_index_creations() {
            return Err(DatabaseError::UnsupportedDdlCombination);
        }
        let storage_id = self.index_ddl_storage(table_id)?;
        let context = transaction.write_context(storage_id, &mut self.registry)?;
        self.registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .drop_index_in(context, id)?;
        transaction.stage_index_drop(storage_id, id);
        Ok(())
    }

    fn index_ddl_storage(&self, table_id: TableId) -> Result<StorageId, DatabaseError> {
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => Ok(*storage_id),
            TablePlacement::RangePartitioned { .. } => {
                Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into())
            }
        }
    }

    #[must_use]
    pub const fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }

    /// Creates one explicitly partition-local index. This API cannot create a
    /// global index and validates that the partition belongs to `table_id`.
    pub fn create_partition_index(
        &mut self,
        table_id: TableId,
        partition_id: netbadb_types::PartitionId,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, DatabaseError> {
        self.ensure_schema_available(None)?;
        let storage_id = match self.bindings.placement(table_id)? {
            TablePlacement::RangePartitioned { partitions, .. } => partitions
                .iter()
                .find(|partition| partition.partition_id == partition_id)
                .map(|partition| partition.storage_id)
                .ok_or(PartitionError::UnknownPartitionId(partition_id))?,
            TablePlacement::Single { .. } => {
                return Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into());
            }
        };
        Ok(self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .create_index(column_id)?)
    }

    /// Returns this table's registered indexes in persistent creation order.
    pub fn indexes(&self, table_id: TableId) -> Result<&[IndexDefinition], DatabaseError> {
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => Ok(self
                .registry
                .get(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .indexes()),
            TablePlacement::RangePartitioned { .. } => {
                Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into())
            }
        }
    }

    /// Persists a fresh optimizer snapshot for one table and all of its
    /// registered indexes. DML does not maintain this snapshot automatically.
    pub fn analyze(&mut self, table_id: TableId) -> Result<(), DatabaseError> {
        self.ensure_schema_available(None)?;
        let storage_ids = self
            .bindings
            .placement(table_id)?
            .storage_ids()
            .collect::<Vec<_>>();
        for storage_id in storage_ids {
            self.registry
                .get_mut(storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                .analyze()?;
        }
        Ok(())
    }

    /// Builds and publishes an immutable projection from one fixed committed view.
    pub fn build_columnar_projection(
        &mut self,
        spec: ColumnarProjectionSpec,
    ) -> Result<ColumnarProjectionId, DatabaseError> {
        self.build_columnar_projection_with(spec, |_| Ok(()))
    }

    /// Builds a versioned NBCS v2 base at an atomic committed-read anchor.
    /// The source change stream must already be explicitly enabled.
    pub fn build_incremental_columnar_projection(
        &mut self,
        spec: ColumnarProjectionSpec,
    ) -> Result<ColumnarProjectionId, DatabaseError> {
        self.build_incremental_columnar_projection_with(spec, |_| Ok(()))
    }

    fn build_incremental_columnar_projection_with<F>(
        &mut self,
        mut spec: ColumnarProjectionSpec,
        after_scan: F,
    ) -> Result<ColumnarProjectionId, DatabaseError>
    where
        F: FnOnce(&mut Self) -> Result<(), DatabaseError>,
    {
        self.ensure_schema_available(None)?;
        spec.directory = schema_catalog_file::absolute(&spec.directory)?;
        self.projections.preflight_location(&spec.directory)?;
        if spec.directory.join("projection.nbcmanifest").exists() {
            return Err(
                StorageError::from(netbadb_storage::ColumnarError::InvalidInput(
                    "projection directory already contains a manifest",
                ))
                .into(),
            );
        }
        let id = self.projections.reserve_id()?;
        let captured = self.capture_incremental_columnar_source(spec.table_id, &spec.columns)?;
        let prepared = ColumnarProjection::prepare_incremental(
            &spec.directory,
            id,
            ColumnarGeneration(1),
            &captured.table,
            captured.storage_id,
            captured.token,
            captured.cursor,
            &spec.columns,
            &captured.rows,
            spec.row_group_rows,
        )?;
        columnar::crash("incremental-build-files-synced");
        after_scan(self)?;
        let projection = prepared.publish()?;
        columnar::crash("incremental-build-manifest-published");
        self.projections.publish(projection)?;
        Ok(id)
    }

    fn build_columnar_projection_with<F>(
        &mut self,
        mut spec: ColumnarProjectionSpec,
        after_scan: F,
    ) -> Result<ColumnarProjectionId, DatabaseError>
    where
        F: FnOnce(&mut Self) -> Result<(), DatabaseError>,
    {
        self.ensure_schema_available(None)?;
        spec.directory = schema_catalog_file::absolute(&spec.directory)?;
        self.projections.preflight_location(&spec.directory)?;
        if spec.directory.join("projection.nbcmanifest").exists() {
            return Err(
                StorageError::from(netbadb_storage::ColumnarError::InvalidInput(
                    "projection directory already contains a manifest",
                ))
                .into(),
            );
        }
        let id = self.projections.reserve_id()?;
        let CapturedColumnarSource {
            storage_id,
            table,
            token: source_token,
            rows,
        } = self.capture_columnar_source(spec.table_id, &spec.columns)?;
        let prepared = ColumnarProjection::prepare(
            &spec.directory,
            id,
            ColumnarGeneration(1),
            &table,
            storage_id,
            source_token,
            &spec.columns,
            &rows,
            spec.row_group_rows,
        )?;
        columnar::crash("build-files-synced");
        after_scan(self)?;
        let current = self
            .registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .current_snapshot_token()?;
        if current != source_token {
            return Err(DatabaseError::ColumnarBuildSourceChanged { storage_id });
        }
        let projection = prepared.publish()?;
        columnar::crash("build-manifest-published");
        self.projections.publish(projection)?;
        Ok(id)
    }

    /// Applies one bounded contiguous NBCL range and publishes at most one
    /// immutable NBCD segment. DML never invokes this path.
    pub fn advance_columnar_projection(
        &mut self,
        id: ColumnarProjectionId,
        budget: ColumnarAdvanceBudget,
    ) -> Result<ColumnarAdvanceReport, DatabaseError> {
        if budget.max_batches == 0 || budget.max_change_bytes == 0 {
            return Err(
                StorageError::from(netbadb_storage::ColumnarError::InvalidInput(
                    "columnar advance budget must be nonzero",
                ))
                .into(),
            );
        }
        let projection = self
            .projections
            .get(id)
            .cloned()
            .ok_or(DatabaseError::ColumnarProjectionNotFound(id))?;
        let metadata = projection.metadata().clone();
        let incremental = metadata
            .incremental
            .as_ref()
            .ok_or(DatabaseError::ColumnarProjectionNotIncremental(id))?;
        let storage = self.registry.get(metadata.source_storage_id).ok_or(
            StorageRegistryError::UnknownStorageId {
                storage_id: metadata.source_storage_id,
            },
        )?;
        if storage.table().fingerprint()? != metadata.schema_fingerprint {
            return Err(
                StorageError::from(netbadb_storage::ColumnarError::IdentityMismatch(
                    "incremental projection schema",
                ))
                .into(),
            );
        }
        let cursor = ChangeStreamCursor {
            storage_id: metadata.source_storage_id,
            generation: incremental.stream_generation,
            frontier: incremental.applied_frontier,
        };
        let changes = storage.read_changes(cursor, budget.max_batches, budget.max_change_bytes)?;
        let before = incremental.applied_frontier;
        if changes.batches.is_empty() {
            return Ok(ColumnarAdvanceReport {
                before_frontier: before,
                after_frontier: before,
                source_current_frontier: changes.current_frontier,
                batches_applied: 0,
                mutations_applied: 0,
                delta_segment_created: false,
                bytes_written: 0,
                caught_up: before == changes.current_frontier,
            });
        }
        let table = storage.table().clone();
        let mutations_applied = changes
            .batches
            .iter()
            .map(|batch| batch.mutations.len() as u64)
            .sum();
        let batches_applied = changes.batches.len() as u64;
        let prepared = projection.prepare_advance(&table, &changes.batches)?;
        let replacement = prepared.publish()?;
        let replacement_incremental = replacement
            .metadata()
            .incremental
            .as_ref()
            .ok_or(DatabaseError::ColumnarProjectionNotIncremental(id))?;
        let after = replacement_incremental.applied_frontier;
        let bytes_written = replacement_incremental
            .delta_bytes
            .saturating_sub(incremental.delta_bytes);
        let _old = self.projections.replace(id, replacement)?;
        Ok(ColumnarAdvanceReport {
            before_frontier: before,
            after_frontier: after,
            source_current_frontier: changes.current_frontier,
            batches_applied,
            mutations_applied,
            delta_segment_created: true,
            bytes_written,
            caught_up: after == changes.current_frontier,
        })
    }

    /// Rewrites one incremental projection's already-applied Base+Delta state
    /// as a new immutable Base generation. It neither reads the source nor
    /// advances the projection's change-stream acknowledgement.
    pub fn compact_columnar_projection(
        &mut self,
        id: ColumnarProjectionId,
    ) -> Result<ColumnarCompactionReport, DatabaseError> {
        let projection = self
            .projections
            .get(id)
            .cloned()
            .ok_or(DatabaseError::ColumnarProjectionNotFound(id))?;
        let metadata = projection.metadata().clone();
        let incremental = metadata
            .incremental
            .as_ref()
            .ok_or(DatabaseError::ColumnarProjectionNotIncremental(id))?;
        let bytes_before = metadata
            .segment_bytes
            .saturating_add(incremental.delta_bytes);
        if incremental.delta_segments.is_empty() {
            return Ok(ColumnarCompactionReport {
                projection_id: id,
                old_generation: metadata.generation,
                new_generation: metadata.generation,
                compacted_frontier: incremental.applied_frontier,
                base_rows_before: metadata.row_count,
                base_rows_after: metadata.row_count,
                delta_segments_consumed: 0,
                delta_mutations_consumed: 0,
                suppressed_versions_removed: 0,
                live_delta_rows_folded: 0,
                bytes_before,
                bytes_after: bytes_before,
                bytes_reclaimed: 0,
                compacted: false,
            });
        }
        let storage = self.registry.get(metadata.source_storage_id).ok_or(
            StorageRegistryError::UnknownStorageId {
                storage_id: metadata.source_storage_id,
            },
        )?;
        let stream = storage.inspect_change_stream();
        if stream.status != netbadb_storage::ChangeStreamStatus::Enabled
            || stream.generation != Some(incremental.stream_generation)
            || storage.table().fingerprint()? != metadata.schema_fingerprint
        {
            return Err(DatabaseError::ColumnarProjectionRebuildRequired(id));
        }
        let table = storage.table().clone();
        let generation = ColumnarGeneration(
            metadata
                .generation
                .0
                .checked_add(1)
                .ok_or(DatabaseError::ColumnarProjectionIdExhausted)?,
        );
        let prepared = projection.prepare_compaction(&table, generation)?;
        let replacement = prepared.publish()?;
        columnar::crash("compact-manifest-published");
        let bytes_after = replacement.metadata().segment_bytes;
        let base_rows_after = replacement.metadata().row_count;
        let retired = self.projections.replace(id, replacement)?;
        retired.retire_segment()?;
        columnar::crash("compact-old-retired");
        Ok(ColumnarCompactionReport {
            projection_id: id,
            old_generation: metadata.generation,
            new_generation: generation,
            compacted_frontier: incremental.applied_frontier,
            base_rows_before: metadata.row_count,
            base_rows_after,
            delta_segments_consumed: incremental.delta_segments.len() as u64,
            delta_mutations_consumed: incremental.delta_mutation_count,
            suppressed_versions_removed: incremental.suppressed_version_count,
            live_delta_rows_folded: incremental.delta_live_row_count,
            bytes_before,
            bytes_after,
            bytes_reclaimed: bytes_before.saturating_sub(bytes_after),
            compacted: true,
        })
    }

    /// Rebuilds the same projection identity and atomically publishes a new generation.
    pub fn refresh_columnar_projection(
        &mut self,
        id: ColumnarProjectionId,
    ) -> Result<ColumnarGeneration, DatabaseError> {
        let existing = self
            .projections
            .get(id)
            .ok_or(DatabaseError::ColumnarProjectionNotFound(id))?;
        let metadata = existing.metadata().clone();
        let directory = existing.root().to_owned();
        let columns = metadata
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        let generation = ColumnarGeneration(
            metadata
                .generation
                .0
                .checked_add(1)
                .ok_or(DatabaseError::ColumnarProjectionIdExhausted)?,
        );
        let incremental_mode = metadata.incremental.is_some();
        let (prepared, storage_id, source_token) = if incremental_mode {
            let captured = self.capture_incremental_columnar_source(metadata.table_id, &columns)?;
            let prepared = ColumnarProjection::prepare_incremental(
                directory,
                id,
                generation,
                &captured.table,
                captured.storage_id,
                captured.token,
                captured.cursor,
                &columns,
                &captured.rows,
                None,
            )?;
            (prepared, captured.storage_id, captured.token)
        } else {
            let captured = self.capture_columnar_source(metadata.table_id, &columns)?;
            let prepared = ColumnarProjection::prepare(
                directory,
                id,
                generation,
                &captured.table,
                captured.storage_id,
                captured.token,
                &columns,
                &captured.rows,
                None,
            )?;
            (prepared, captured.storage_id, captured.token)
        };
        columnar::crash("refresh-files-synced");
        if !incremental_mode {
            let current = self
                .registry
                .get(storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                .current_snapshot_token()?;
            if current != source_token {
                return Err(DatabaseError::ColumnarBuildSourceChanged { storage_id });
            }
        }
        let replacement = prepared.publish()?;
        columnar::crash("refresh-manifest-published");
        let retired = self.projections.replace(id, replacement)?;
        retired.retire_segment()?;
        columnar::crash("refresh-old-retired");
        Ok(generation)
    }

    /// Reattaches an explicitly located projection after reopening a database.
    pub fn attach_columnar_projection(
        &mut self,
        directory: impl AsRef<Path>,
        table_id: TableId,
    ) -> Result<ColumnarProjectionId, DatabaseError> {
        self.ensure_schema_available(None)?;
        let table = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == table_id)
            .cloned()
            .ok_or(StorageRegistryError::MissingPhysicalBinding { table_id })?;
        let projection = ColumnarProjection::open(directory, &table)?;
        let metadata = projection.metadata();
        let expected_storage = match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(DatabaseError::ColumnarProjectionRequiresSingleStorage(
                    table_id,
                ));
            }
        };
        if metadata.source_storage_id != expected_storage {
            return Err(
                StorageError::from(netbadb_storage::ColumnarError::IdentityMismatch(
                    "source storage",
                ))
                .into(),
            );
        }
        let id = metadata.id;
        self.projections.adopt(projection)?;
        Ok(id)
    }

    pub fn drop_columnar_projection(
        &mut self,
        id: ColumnarProjectionId,
    ) -> Result<(), DatabaseError> {
        if let Some(projection) = self.projections.remove(id)? {
            projection.drop_files()?;
        }
        columnar::crash("drop-physical-cleanup");
        Ok(())
    }

    /// Enables a durable committed-change stream for one exactly placed table.
    pub fn enable_change_stream(
        &mut self,
        table_id: TableId,
    ) -> Result<ChangeStreamCursor, DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        self.registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .enable_change_stream()
            .map_err(Into::into)
    }

    /// Explicitly abandons the current stream incarnation and its history.
    pub fn disable_change_stream(&mut self, table_id: TableId) -> Result<(), DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        self.registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .disable_change_stream()
            .map_err(Into::into)
    }

    pub fn read_changes(
        &self,
        table_id: TableId,
        cursor: ChangeStreamCursor,
        max_batches: usize,
        max_bytes: u64,
    ) -> Result<ChangeReadResult, DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        self.registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .read_changes(cursor, max_batches, max_bytes)
            .map_err(Into::into)
    }

    pub fn committed_read_anchor(
        &self,
        table_id: TableId,
    ) -> Result<CommittedReadAnchor, DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        self.registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .committed_read_anchor()
            .map_err(Into::into)
    }

    pub fn inspect_change_stream(
        &self,
        table_id: TableId,
    ) -> Result<ChangeStreamInspection, DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        Ok(self
            .registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .inspect_change_stream())
    }

    /// Reclaims committed NBCL history acknowledged by every managed
    /// incremental projection that depends on the active stream incarnation.
    pub fn gc_change_stream(
        &mut self,
        table_id: TableId,
    ) -> Result<ChangeStreamGcReport, DatabaseError> {
        self.projections.ensure_retention_catalog_available()?;
        let storage_id = self.bindings.resolve_single(table_id)?;
        let stream = self
            .registry
            .get(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .inspect_change_stream();
        let generation = stream
            .generation
            .ok_or_else(|| DatabaseError::ChangeStreamGcUnsafe {
                storage_id,
                reason: "active stream generation is unavailable".into(),
            })?;
        if stream.status != netbadb_storage::ChangeStreamStatus::Enabled {
            return Err(DatabaseError::ChangeStreamGcUnsafe {
                storage_id,
                reason: format!("change stream status is {:?}", stream.status),
            });
        }
        let mut consumers = Vec::new();
        for entry in self
            .projections
            .iter()
            .filter(|entry| entry.identity.source_storage_id == storage_id)
        {
            let Some(projection) = &entry.projection else {
                if entry.managed {
                    return Err(DatabaseError::ChangeStreamGcUnsafe {
                        storage_id,
                        reason: format!(
                            "managed projection {} metadata is unavailable",
                            entry.identity.id.0
                        ),
                    });
                }
                continue;
            };
            let Some(incremental) = &projection.metadata().incremental else {
                continue;
            };
            if !entry.managed {
                return Err(DatabaseError::ChangeStreamGcUnsafe {
                    storage_id,
                    reason: format!(
                        "unmanaged incremental projection {} may require retained history",
                        entry.identity.id.0
                    ),
                });
            }
            if incremental.stream_generation != generation {
                continue;
            }
            if incremental.applied_frontier.0 > stream.current_data_version.0 {
                return Err(DatabaseError::ChangeStreamGcUnsafe {
                    storage_id,
                    reason: format!(
                        "projection {} applied frontier is ahead of the source",
                        entry.identity.id.0
                    ),
                });
            }
            consumers.push((entry.identity.id, incremental.applied_frontier));
        }
        let safe_frontier = consumers
            .iter()
            .map(|(_, frontier)| *frontier)
            .min_by_key(|frontier| frontier.0)
            .ok_or(DatabaseError::ChangeStreamGcNoRetentionConsumer(storage_id))?;
        let mut limiting_projection_ids = consumers
            .iter()
            .filter_map(|(id, frontier)| (*frontier == safe_frontier).then_some(*id))
            .collect::<Vec<_>>();
        limiting_projection_ids.sort_by_key(|id| id.0);
        let reclaimed = self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
            .gc_change_stream(safe_frontier)?;
        Ok(ChangeStreamGcReport {
            storage_id: reclaimed.storage_id,
            generation: reclaimed.generation,
            previous_earliest_frontier: reclaimed.previous_earliest_frontier,
            new_earliest_frontier: reclaimed.new_earliest_frontier,
            current_frontier: reclaimed.current_frontier,
            limiting_projection_ids,
            batches_removed: reclaimed.batches_removed,
            mutations_removed: reclaimed.mutations_removed,
            bytes_before: reclaimed.bytes_before,
            bytes_after: reclaimed.bytes_after,
            bytes_reclaimed: reclaimed.bytes_reclaimed,
        })
    }

    #[must_use]
    pub fn inspect_columnar_projections(&self) -> Vec<ColumnarProjectionInspection> {
        self.projections
            .iter()
            .map(|entry| {
                let metadata = &entry.identity;
                let current = self
                    .registry
                    .get(metadata.source_storage_id)
                    .and_then(|storage| storage.current_snapshot_token().ok());
                let current_schema_fingerprint = self
                    .registry
                    .get(metadata.source_storage_id)
                    .and_then(|storage| storage.table().fingerprint().ok());
                let current_stream = self
                    .registry
                    .get(metadata.source_storage_id)
                    .map(TableStorage::inspect_change_stream);
                columnar::inspection(entry, current, current_stream, current_schema_fingerprint)
            })
            .collect()
    }

    #[must_use]
    pub fn inspect_columnar_projection_catalog(&self) -> ColumnarProjectionCatalogInspection {
        self.projections.catalog_inspection()
    }

    #[must_use]
    pub fn inspect_columnar_projection_path(
        directory: impl AsRef<Path>,
        table: &TableDef,
    ) -> ColumnarProjectionInspection {
        let directory = directory.as_ref();
        match ColumnarProjection::open(directory, table) {
            Ok(projection) => {
                columnar::unmanaged_projection_inspection(projection, table.fingerprint().ok())
            }
            Err(error) => {
                columnar::unmanaged_path_inspection(directory, table.id, error.to_string())
            }
        }
    }

    fn capture_columnar_source(
        &mut self,
        table_id: TableId,
        columns: &[ColumnId],
    ) -> Result<CapturedColumnarSource, DatabaseError> {
        let storage_id = match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(DatabaseError::ColumnarProjectionRequiresSingleStorage(
                    table_id,
                ));
            }
        };
        let storage = self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
        if storage.kind() == StorageKind::Lsm {
            // Stabilize the WAL-generation token component so a clean
            // close/reopen preserves freshness for unchanged LSM data.
            storage.flush()?;
        }
        let global_boundary = self
            .published_visibility
            .as_ref()
            .map(|published| {
                published
                    .try_borrow()
                    .map_err(|_| CoordinatorError::PublishedVisibilityBusy)?
                    .snapshot
                    .boundary(storage_id)
                    .ok_or(CoordinatorError::SnapshotMissingStorage { storage_id })
            })
            .transpose()?;
        if let Some(boundary) = global_boundary
            && storage.current_visibility_boundary()? != boundary
        {
            return Err(DatabaseError::ColumnarBuildSourceChanged { storage_id });
        }
        let table = storage.table().clone();
        let view = match global_boundary {
            Some(boundary) => storage.read_view_at(boundary)?,
            None => storage.read_view()?,
        };
        let token = storage.snapshot_token(&view)?;
        let rows = storage
            .scan_columns_with_view(columns, &view)?
            .into_iter()
            .map(|(_, values)| values)
            .collect();
        Ok(CapturedColumnarSource {
            storage_id,
            table,
            token,
            rows,
        })
    }

    fn capture_incremental_columnar_source(
        &mut self,
        table_id: TableId,
        columns: &[ColumnId],
    ) -> Result<CapturedIncrementalColumnarSource, DatabaseError> {
        let storage_id = match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(DatabaseError::ColumnarProjectionRequiresSingleStorage(
                    table_id,
                ));
            }
        };
        let storage = self
            .registry
            .get_mut(storage_id)
            .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
        if let Some(published) = &self.published_visibility {
            let boundary = published
                .try_borrow()
                .map_err(|_| CoordinatorError::PublishedVisibilityBusy)?
                .snapshot
                .boundary(storage_id)
                .ok_or(CoordinatorError::SnapshotMissingStorage { storage_id })?;
            if storage.current_visibility_boundary()? != boundary {
                return Err(DatabaseError::ColumnarBuildSourceChanged { storage_id });
            }
        }
        let table = storage.table().clone();
        let anchor = storage.committed_read_anchor()?;
        let token = storage.snapshot_token(&anchor.read_view)?;
        let rows = storage.scan_versioned_columns_with_view(columns, &anchor.read_view)?;
        Ok(CapturedIncrementalColumnarSource {
            storage_id,
            table,
            token,
            cursor: anchor.cursor,
            rows,
        })
    }

    pub fn vacuum(&mut self, table_id: TableId) -> Result<u64, DatabaseError> {
        self.ensure_schema_available(None)?;
        let storage_ids = self
            .bindings
            .placement(table_id)?
            .storage_ids()
            .collect::<Vec<_>>();
        let mut total = 0_u64;
        for storage_id in storage_ids {
            total = total
                .checked_add(
                    self.registry
                        .get_mut(storage_id)
                        .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                        .vacuum()?,
                )
                .ok_or(ExecutionError::AffectedRowsOverflow)?;
        }
        Ok(total)
    }

    /// Explicitly closes the embedded database after flushing dirty pages.
    pub fn close(self) -> Result<(), DatabaseError> {
        self.ensure_schema_available(None)?;
        for entry in self.registry.into_entries() {
            entry.storage.close()?;
        }
        Ok(())
    }

    pub fn query(&mut self, source: &str) -> Result<QueryResult, DatabaseError> {
        let (compiled, physical) = self.compile_and_plan(source)?;
        let PhysicalStatement::Query(plan) = physical else {
            return Err(DatabaseError::ExpectedQuery);
        };
        let storage_ids = self.storage_ids_for_tables(compiled.logical_statement.read_tables())?;
        let view = self.autocommit_read_view(&storage_ids)?;
        match self.execute_query_plan(&plan, &view, None, None) {
            Ok(result) => Ok(result),
            Err(error) => {
                let Some((projection_id, detail)) = columnar_read_failure(&error) else {
                    return Err(error);
                };
                self.projections.quarantine(projection_id, detail);
                let (_, retry) = self.compile_and_plan(source)?;
                let PhysicalStatement::Query(retry) = retry else {
                    return Err(DatabaseError::ExpectedQuery);
                };
                self.execute_query_plan(&retry, &view, None, None)
            }
        }
    }

    /// Executes a query and returns exact columnar row-group scan counters.
    /// Counters remain zero when planning falls back to authoritative storage.
    pub fn query_with_columnar_statistics(
        &mut self,
        source: &str,
    ) -> Result<(QueryResult, ColumnarExecutionStatistics), DatabaseError> {
        let (compiled, physical) = self.compile_and_plan(source)?;
        let PhysicalStatement::Query(plan) = physical else {
            return Err(DatabaseError::ExpectedQuery);
        };
        let storage_ids = self.storage_ids_for_tables(compiled.logical_statement.read_tables())?;
        let view = self.autocommit_read_view(&storage_ids)?;
        let mut statistics = ColumnarExecutionStatistics::default();
        match self.execute_query_plan(&plan, &view, None, Some(&mut statistics)) {
            Ok(result) => Ok((result, statistics)),
            Err(error) => {
                let Some((projection_id, detail)) = columnar_read_failure(&error) else {
                    return Err(error);
                };
                self.projections.quarantine(projection_id, detail);
                let (_, retry) = self.compile_and_plan(source)?;
                let PhysicalStatement::Query(retry) = retry else {
                    return Err(DatabaseError::ExpectedQuery);
                };
                statistics = ColumnarExecutionStatistics::default();
                let result = self.execute_query_plan(&retry, &view, None, Some(&mut statistics))?;
                Ok((result, statistics))
            }
        }
    }

    /// Compiles SQL and reports its canonical table access without planning,
    /// reading rows, starting a transaction, or touching persistent state.
    pub fn statement_access(&self, source: &str) -> Result<StatementAccess, DatabaseError> {
        Ok(self.prepare_sql_statement(source, &[])?.access())
    }

    /// Parses, resolves, and type-checks a reusable parameterized statement.
    /// Declared parameter constraints are physical frontend capabilities;
    /// contextual inference still preserves canonical semantic types.
    pub fn prepare_statement(
        &self,
        source: &str,
        declared: &[Option<PhysicalType>],
    ) -> Result<PreparedStatement, DatabaseError> {
        let compiled = compile_statement_with_parameters(&self.committed.schema, source, declared)?;
        let dependencies = self.statement_dependencies(&compiled, None)?;
        Ok(PreparedStatement {
            compiled,
            dependencies,
            scope: None,
        })
    }

    /// Parses and resolves a generic schema mutation without touching storage.
    pub fn prepare_ddl_statement(
        &self,
        source: &str,
    ) -> Result<PreparedDdlStatement, DatabaseError> {
        let indexes = self.index_name_bindings(None);
        let tables = self.table_identity_bindings(None)?;
        Ok(PreparedDdlStatement {
            compiled: compile_ddl_statement(&self.committed.schema, source, &indexes, &tables)?,
        })
    }

    fn index_name_bindings(&self, transaction: Option<&Transaction>) -> Vec<IndexNameBinding> {
        self.registry
            .iter()
            .filter(|entry| {
                transaction.is_none_or(|transaction| {
                    transaction
                        .schema_composition
                        .plan()
                        .is_none_or(|plan| !plan.touched.contains_key(&entry.storage.table().id))
                })
            })
            .flat_map(|entry| {
                entry.storage.indexes().iter().filter_map(|index| {
                    index.name.as_ref().map(|name| IndexNameBinding {
                        name: name.clone(),
                        target: DropIndexTarget {
                            table_id: entry.storage.table().id,
                            index_id: index.id,
                        },
                    })
                })
            })
            .chain(
                transaction
                    .and_then(|transaction| transaction.schema_composition.plan())
                    .into_iter()
                    .flat_map(|plan| plan.touched.iter())
                    .filter(|(table_id, _)| {
                        transaction
                            .and_then(|transaction| transaction.schema_composition.plan())
                            .is_some_and(|plan| {
                                plan.overlay
                                    .schema
                                    .tables()
                                    .iter()
                                    .any(|table| table.id == **table_id)
                            })
                    })
                    .flat_map(|(table_id, table)| {
                        table.indexes.active.iter().filter_map(move |index| {
                            index.name.as_ref().map(|name| IndexNameBinding {
                                name: name.clone(),
                                target: DropIndexTarget {
                                    table_id: *table_id,
                                    index_id: index.id,
                                },
                            })
                        })
                    }),
            )
            .chain(
                transaction
                    .and_then(|transaction| transaction.schema_composition.plan())
                    .into_iter()
                    .flat_map(|plan| plan.created.iter())
                    .filter(|(_, table)| table.present)
                    .flat_map(|(table_id, table)| {
                        table.indexes.active.iter().filter_map(move |index| {
                            index.name.as_ref().map(|name| IndexNameBinding {
                                name: name.clone(),
                                target: DropIndexTarget {
                                    table_id: *table_id,
                                    index_id: index.id,
                                },
                            })
                        })
                    }),
            )
            .collect::<Vec<_>>()
    }

    fn table_identity_bindings(
        &self,
        transaction: Option<&Transaction>,
    ) -> Result<Vec<TableIdentityBinding>, DatabaseError> {
        let schema = transaction.map_or(&self.committed.schema, |value| {
            value.visible_schema(&self.committed.schema)
        });
        schema
            .tables()
            .iter()
            .map(|table| {
                let table_version = self
                    .committed
                    .tables
                    .iter()
                    .find(|lineage| lineage.table_id == table.id)
                    .map(|lineage| lineage.version)
                    .and_then(|committed_version| {
                        transaction
                            .and_then(|transaction| transaction.schema_composition.plan())
                            .and_then(|plan| {
                                plan.overlay
                                    .tables
                                    .iter()
                                    .find(|lineage| lineage.table_id == table.id)
                                    .map(|lineage| lineage.version)
                            })
                            .or(Some(committed_version))
                    })
                    .or_else(|| {
                        transaction
                            .filter(|value| value.owns_staged_table(table.id))
                            .map(|_| TableSchemaVersion(1))
                    })
                    .ok_or(SchemaMutationError::Corrupt(
                        "visible table lineage absent during compilation",
                    ))?;
                Ok(TableIdentityBinding {
                    target: DropTableTarget {
                        table_id: table.id,
                        table_version,
                        fingerprint: table.fingerprint()?,
                    },
                })
            })
            .collect()
    }

    /// Prepare any generic SQL through one parser pass, with no execution effects.
    pub fn prepare_sql_statement(
        &self,
        source: &str,
        declared: &[Option<PhysicalType>],
    ) -> Result<PreparedSqlStatement, DatabaseError> {
        self.prepare_sql_with_transaction(None, source, declared)
    }

    /// Prepares against the transaction view, including logical DDL. Unlike
    /// explicitly scoped `prepare_statement_in`, only statements referencing
    /// private tables are transaction-bound; committed-table plans remain reusable.
    pub fn prepare_sql_statement_in(
        &self,
        transaction: &Transaction,
        source: &str,
        declared: &[Option<PhysicalType>],
    ) -> Result<PreparedSqlStatement, DatabaseError> {
        self.validate_transaction(transaction)?;
        self.prepare_sql_with_transaction(Some(transaction), source, declared)
    }

    fn prepare_sql_with_transaction(
        &self,
        transaction: Option<&Transaction>,
        source: &str,
        declared: &[Option<PhysicalType>],
    ) -> Result<PreparedSqlStatement, DatabaseError> {
        let schema = transaction.map_or(&self.committed.schema, |t| {
            t.visible_schema(&self.committed.schema)
        });
        let compiled = netbadb_compiler::compile_sql_statement(
            schema,
            source,
            &self.index_name_bindings(transaction),
            &self.table_identity_bindings(transaction)?,
            declared,
        )?;
        match compiled {
            netbadb_compiler::CompiledSqlStatement::Ddl(compiled) => {
                if transaction.is_some_and(|t| t.schema_mutation.is_some())
                    && !matches!(compiled, CompiledDdlStatement::CreateTable(_))
                {
                    return Err(DatabaseError::UnsupportedDdlCombination);
                }
                if transaction.is_some_and(|transaction| {
                    transaction.schema_composition.is_started()
                        && !matches!(
                            compiled,
                            CompiledDdlStatement::CreateTable(_)
                                | CompiledDdlStatement::DropTable(_)
                                | CompiledDdlStatement::AlterTable(_)
                                | CompiledDdlStatement::CreateIndex(_)
                                | CompiledDdlStatement::DropIndex(_)
                        )
                }) {
                    return if transaction
                        .is_some_and(|transaction| transaction.schema_composition.is_sealed())
                    {
                        Err(SchemaMutationError::SchemaMutationAfterMaterialization.into())
                    } else {
                        Err(DatabaseError::UnsupportedDdlCombination)
                    };
                }
                Ok(PreparedSqlStatement::Ddl(PreparedDdlStatement { compiled }))
            }
            netbadb_compiler::CompiledSqlStatement::Relational(compiled) => {
                let dependencies = self.statement_dependencies(&compiled, transaction)?;
                // Only private identities require transaction scope. Drivers may
                // cache statements prepared inside a transaction over committed
                // tables and safely reuse them after unrelated schema commits.
                let scope = transaction
                    .filter(|t| dependencies.iter().any(|d| t.owns_staged_table(d.table_id)))
                    .map(|t| std::rc::Rc::downgrade(&t.preparation_scope));
                Ok(PreparedSqlStatement::Relational(Box::new(
                    PreparedStatement {
                        compiled: *compiled,
                        dependencies,
                        scope,
                    },
                )))
            }
        }
    }

    /// Prepares an already resolved generic identity, including unnamed legacy
    /// indexes. Compatibility aliases and OIDs must be resolved by the caller.
    pub fn prepare_drop_index(
        &self,
        table_id: TableId,
        index_id: netbadb_types::IndexId,
        if_exists: bool,
    ) -> Result<PreparedDdlStatement, DatabaseError> {
        if !self
            .indexes(table_id)?
            .iter()
            .any(|index| index.id == index_id)
        {
            return Err(DatabaseError::UndefinedIndex);
        }
        Ok(PreparedDdlStatement {
            compiled: CompiledDdlStatement::DropIndex(TypedDropIndex {
                name: None,
                target: Some(DropIndexTarget { table_id, index_id }),
                if_exists,
            }),
        })
    }

    fn validate_drop_target(
        &self,
        statement: &TypedDropIndex,
        transaction: Option<&Transaction>,
    ) -> Result<Option<DropIndexTarget>, DatabaseError> {
        if let Some(target) = statement.target {
            let active = transaction
                .and_then(|transaction| transaction.schema_composition.plan())
                .and_then(|plan| plan.touched.get(&target.table_id))
                .map(|table| {
                    table
                        .indexes
                        .active
                        .iter()
                        .any(|index| index.id == target.index_id)
                })
                .unwrap_or(
                    self.indexes(target.table_id)?
                        .iter()
                        .any(|index| index.id == target.index_id),
                );
            if active {
                return Ok(Some(target));
            }
        }
        // A prepared DROP never resolves a replacement registration by name:
        // its original table authorization and logical identity remain binding.
        if statement.if_exists {
            Ok(None)
        } else {
            Err(DatabaseError::UndefinedIndex)
        }
    }

    /// Executes one prepared generic DDL statement in its storage-owned atomic
    /// transaction. Frontends remain responsible for their protocol tag.
    pub fn execute_ddl(
        &mut self,
        prepared: &PreparedDdlStatement,
    ) -> Result<DdlOutcome, DatabaseError> {
        match &prepared.compiled {
            CompiledDdlStatement::CreateTable(statement) => {
                let mut transaction = self.begin_transaction()?;
                if let Err(error) =
                    self.create_heap_table_in(&mut transaction, CreateTableSpec::from(statement))
                {
                    transaction.rollback()?;
                    return Err(error);
                }
                self.commit_transaction(&mut transaction)?;
                Ok(DdlOutcome::Created)
            }
            CompiledDdlStatement::DropTable(statement) => {
                let mut transaction = self.begin_transaction()?;
                if let Err(error) = self.drop_table_in(&mut transaction, statement.target) {
                    transaction.rollback()?;
                    return Err(error);
                }
                self.commit_transaction(&mut transaction)?;
                Ok(DdlOutcome::Dropped)
            }
            CompiledDdlStatement::AlterTable(statement) => {
                let mut transaction = self.begin_transaction()?;
                if let Err(error) = self
                    .compose_heap_table_schema_in(&mut transaction, AlterTableSpec::from(statement))
                {
                    transaction.rollback()?;
                    return Err(error);
                }
                self.commit_transaction(&mut transaction)?;
                Ok(DdlOutcome::Altered)
            }
            CompiledDdlStatement::DropIndex(_) => {
                let mut transaction = self.begin_transaction()?;
                let outcome = match self.execute_ddl_in(&mut transaction, prepared) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        transaction.rollback()?;
                        return Err(error);
                    }
                };
                self.commit_transaction(&mut transaction)?;
                Ok(outcome)
            }

            CompiledDdlStatement::CreateIndex(_) => {
                let mut transaction = self.begin_transaction()?;
                let outcome = match self.execute_ddl_in(&mut transaction, prepared) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        transaction.rollback()?;
                        return Err(error);
                    }
                };
                self.commit_transaction(&mut transaction)?;
                Ok(outcome)
            }
        }
    }

    /// Executes generic DDL inside an existing database transaction. Active
    /// registry publication is deferred until commit; rollback undoes CREATE
    /// tree/backfill/registration or restores DROP catalog state together.
    pub fn execute_ddl_in(
        &mut self,
        transaction: &mut Transaction,
        prepared: &PreparedDdlStatement,
    ) -> Result<DdlOutcome, DatabaseError> {
        if transaction.schema_mutation.is_some() && !prepared.is_table_create() {
            return Err(DatabaseError::UnsupportedDdlCombination);
        }
        if transaction.schema_composition.is_started()
            && !matches!(
                prepared.compiled,
                CompiledDdlStatement::CreateTable(_)
                    | CompiledDdlStatement::DropTable(_)
                    | CompiledDdlStatement::AlterTable(_)
                    | CompiledDdlStatement::CreateIndex(_)
                    | CompiledDdlStatement::DropIndex(_)
            )
        {
            return if transaction.schema_composition.is_sealed() {
                Err(SchemaMutationError::SchemaMutationAfterMaterialization.into())
            } else {
                Err(DatabaseError::UnsupportedDdlCombination)
            };
        }
        self.validate_transaction(transaction)?;
        if (transaction.schema_composition.backfill().is_some()
            || transaction.schema_composition.backfill_index().is_some())
            && !matches!(
                prepared.compiled,
                CompiledDdlStatement::AlterTable(_)
                    | CompiledDdlStatement::CreateIndex(_)
                    | CompiledDdlStatement::DropIndex(_)
            )
        {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
            )
            .into());
        }
        match &prepared.compiled {
            CompiledDdlStatement::CreateTable(statement) => {
                self.create_heap_table_in(transaction, CreateTableSpec::from(statement))?;
                Ok(DdlOutcome::Created)
            }
            CompiledDdlStatement::DropTable(statement) => {
                self.drop_table_in(transaction, statement.target)?;
                Ok(DdlOutcome::Dropped)
            }
            CompiledDdlStatement::AlterTable(statement) => {
                self.compose_heap_table_schema_in(transaction, AlterTableSpec::from(statement))?;
                Ok(DdlOutcome::Altered)
            }
            CompiledDdlStatement::DropIndex(statement) => {
                if transaction.has_pending_index_creations() {
                    return Err(DatabaseError::UnsupportedDdlCombination);
                }
                let Some(target) = self.validate_drop_target(statement, Some(transaction))? else {
                    return Ok(DdlOutcome::Unchanged);
                };
                if self.should_route_drop_index_to_backfill_evacuation(transaction, target) {
                    self.evacuate_staged_backfill_index_in(transaction, target)?;
                    return Ok(DdlOutcome::Dropped);
                }
                if self.is_cross_table_drop_during_backfill_evacuation(transaction, target) {
                    return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                        crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
                    )
                    .into());
                }
                if self.should_compose_index_ddl(transaction, target.table_id)? {
                    return self.compose_drop_index_in(transaction, target);
                }
                self.drop_index_in(transaction, target.table_id, target.index_id)?;
                Ok(DdlOutcome::Dropped)
            }

            CompiledDdlStatement::CreateIndex(statement) => {
                if self.should_compose_index_ddl(transaction, statement.target.table_id)? {
                    return self.compose_create_index_in(transaction, statement);
                }
                if transaction.has_pending_index_drops() {
                    return Err(DatabaseError::UnsupportedDdlCombination);
                }
                if let Some((existing_table_id, existing)) =
                    self.registry.iter().find_map(|entry| {
                        entry
                            .storage
                            .indexes()
                            .iter()
                            .find(|definition| definition.name.as_ref() == Some(&statement.name))
                            .map(|definition| (entry.storage.table().id, definition))
                    })
                {
                    if statement.if_not_exists
                        && existing_table_id == statement.target.table_id
                        && existing.column_id == statement.column_id
                    {
                        return Ok(DdlOutcome::Unchanged);
                    }
                    return Err(DatabaseError::DuplicateIndexName(statement.name.clone()));
                }
                let storage_id = match self.bindings.placement(statement.target.table_id)? {
                    TablePlacement::Single { storage_id, .. } => *storage_id,
                    TablePlacement::RangePartitioned { .. } => {
                        return Err(PartitionError::PartitionedIndexCreationNotSupported(
                            statement.target.table_id,
                        )
                        .into());
                    }
                };
                if let Some((pending_storage, existing)) =
                    transaction.pending_index_by_name(&statement.name)
                {
                    if statement.if_not_exists
                        && pending_storage == storage_id
                        && existing.column_id == statement.column_id
                    {
                        return Ok(DdlOutcome::Unchanged);
                    }
                    return Err(DatabaseError::DuplicateIndexName(statement.name.clone()));
                }
                if transaction.has_pending_index_column(storage_id, statement.column_id) {
                    return Err(StorageError::from(
                        netbadb_index::IndexError::IndexAlreadyExists {
                            column_id: statement.column_id,
                        },
                    )
                    .into());
                }
                let floor = self.index_allocation_floor(statement.target.table_id, storage_id)?;
                let context = transaction.write_context(storage_id, &mut self.registry)?;
                let definition = {
                    let storage = self
                        .registry
                        .get_mut(storage_id)
                        .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?;
                    storage.advance_index_id_floor_in(context, floor)?;
                    storage.create_named_index_in(
                        context,
                        statement.name.clone(),
                        statement.column_id,
                    )?
                };
                transaction.stage_index(storage_id, definition);
                Ok(DdlOutcome::Created)
            }
        }
    }

    /// Commits a transaction and publishes any schema-cache changes only after
    /// the physical commit decision is durable.
    pub fn commit_transaction(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        transaction.validate_commit_owner(&self.transaction_owner)?;
        self.ensure_schema_materialized(transaction)?;
        if matches!(
            transaction.schema_composition,
            schema_composition::SchemaCompositionState::AdoptedSourceRefining(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceBackfilling(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceFinalRefining(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
        ) {
            self.finalize_adopted_source(transaction)?;
        }
        if transaction.schema_composition.source_backfill().is_some() {
            self.finalize_source_backfill(transaction)?;
        }
        if (transaction.schema_composition.backfill().is_some()
            || transaction.schema_composition.backfill_index().is_some())
            && !matches!(
                transaction.schema_composition,
                schema_composition::SchemaCompositionState::Finalized(_)
                    | schema_composition::SchemaCompositionState::FinalizedIndex(_)
            )
        {
            self.finalize_backfill(transaction)?;
        }
        let no_effective = matches!(
            transaction.schema_composition,
            schema_composition::SchemaCompositionState::SealedNoEffectiveChange(_)
        );
        transaction.commit_with_schema_mutations()?;
        if transaction.schema_composition.materialized().is_some() {
            self.finish_composition_commit(transaction)?;
            self.reconcile_global_snapshot(transaction.database_commit_seq())?;
            return Ok(());
        }
        if transaction
            .schema_composition
            .materialized_index()
            .is_some()
        {
            self.finish_schema_index_composition_commit(transaction)?;
            self.reconcile_global_snapshot(transaction.database_commit_seq())?;
            return Ok(());
        }
        if no_effective {
            self.finish_no_effective_composition(transaction)?;
        }
        if transaction.schema_mutation.is_some() {
            self.finish_schema_commit(transaction)?;
            self.reconcile_global_snapshot(transaction.database_commit_seq())?;
            return Ok(());
        }
        let pending = transaction.take_pending_indexes();
        let drops = transaction.take_pending_index_drops();
        if !pending.is_empty() || !drops.is_empty() {
            for (storage_id, id) in drops {
                self.registry
                    .get_mut(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                    .publish_committed_index_drop(id);
            }
            for (storage_id, definition) in pending {
                self.registry
                    .get_mut(storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId { storage_id })?
                    .publish_committed_index(definition);
            }
            self.catalog_generation = self.catalog_generation.saturating_add(1);
        }
        Ok(())
    }

    fn reconcile_global_snapshot(
        &mut self,
        commit_seq: Option<DatabaseCommitSeq>,
    ) -> Result<(), DatabaseError> {
        let (Some(published), Some(commit_seq)) = (&self.published_visibility, commit_seq) else {
            return Ok(());
        };
        let snapshot =
            DatabaseSnapshot::new(commit_seq, current_visibility_boundaries(&self.registry)?)?;
        published
            .try_borrow_mut()
            .map_err(|_| CoordinatorError::PublishedVisibilityBusy)?
            .snapshot = snapshot;
        Ok(())
    }

    /// Compiles and plans a statement and exposes only transport-neutral output
    /// metadata. No transaction starts and no storage row is read or written.
    pub fn describe_statement(&self, source: &str) -> Result<StatementDescription, DatabaseError> {
        let (_, physical) = self.compile_and_plan(source)?;
        let PhysicalStatement::Query(plan) = physical else {
            return Ok(StatementDescription {
                columns: Vec::new(),
                is_query: false,
            });
        };
        let columns = plan
            .output_fields()
            .into_iter()
            .map(|field| ResultColumn {
                name: field.name().to_owned(),
                data_type: field.data_type().clone(),
                nullable: field.nullable(),
            })
            .collect();
        Ok(StatementDescription {
            columns,
            is_query: true,
        })
    }

    /// Returns canonical catalog metadata, storage-neutral logical index
    /// identity/kind/uniqueness in persistent registration order, and cached
    /// `ANALYZE` snapshots without scanning data or refreshing statistics.
    pub fn inspect_catalog(&self) -> Result<CatalogInspection, DatabaseError> {
        inspection::catalog(&self.committed.schema, &self.bindings, &self.registry)
    }

    /// Reports the explicit physical storage kind for a non-partitioned table.
    pub fn storage_kind(&self, table_id: TableId) -> Result<StorageKind, DatabaseError> {
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => Ok(self
                .registry
                .get(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .kind()),
            TablePlacement::RangePartitioned { .. } => {
                Err(PartitionError::PartitionedIndexCreationNotSupported(table_id).into())
            }
        }
    }

    /// Returns LSM physical inspection without exposing mutable handles.
    pub fn inspect_lsm_storage(
        &self,
        table_id: TableId,
    ) -> Result<Option<LsmInspection>, DatabaseError> {
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => Ok(self
                .registry
                .get(*storage_id)
                .ok_or(StorageRegistryError::UnknownStorageId {
                    storage_id: *storage_id,
                })?
                .lsm_inspection()),
            TablePlacement::RangePartitioned { .. } => Ok(None),
        }
    }

    /// Compiles and physically plans one statement, then converts that exact
    /// plan into stable read-only inspection values without executing it.
    pub fn inspect_statement(&self, source: &str) -> Result<StatementInspection, DatabaseError> {
        let (compiled, physical) = self.compile_and_plan(source)?;
        Ok(inspection::statement(
            &compiled.logical_statement,
            &physical,
        ))
    }

    /// Executes SELECT, typed DML or generic DDL. Mutations use existing implicit
    /// transactions; DDL returns AffectedRows(0). For fallible schema commits
    /// requiring explicit retry, use execute_ddl_in and retain the transaction.
    pub fn execute(&mut self, source: &str) -> Result<ExecutionResult, DatabaseError> {
        match self.prepare_sql_statement(source, &[])? {
            PreparedSqlStatement::Relational(prepared) => self.execute_prepared(&prepared, &[]),
            PreparedSqlStatement::Ddl(prepared) => self
                .execute_ddl(&prepared)
                .map(|_| ExecutionResult::AffectedRows(0)),
        }
    }

    /// Binds typed values into a prepared logical statement, then optimizes,
    /// plans, and executes that concrete ephemeral statement.
    pub fn execute_prepared(
        &mut self,
        prepared: &PreparedStatement,
        values: &[ScalarValue],
    ) -> Result<ExecutionResult, DatabaseError> {
        self.validate_prepared_dependencies(prepared, None)?;
        let logical = bind_statement(&prepared.compiled, values)?;
        let physical = self.plan_logical_statement(&logical);
        if let PhysicalStatement::Query(plan) = &physical {
            let storage_ids = self.storage_ids_for_tables(logical.read_tables())?;
            let view = self.autocommit_read_view(&storage_ids)?;
            return self
                .execute_query_plan(plan, &view, None, None)
                .map(ExecutionResult::Query);
        }

        let mut transaction = self.begin_database_transaction(IsolationLevel::ReadCommitted)?;
        match self.execute_mutation_in(&mut transaction, &physical) {
            Ok(result) => {
                transaction.commit()?;
                Ok(result)
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    /// Executes a statement using an existing database transaction. Reads may
    /// span physical storages. Writes may span storages only when the database
    /// was opened with an explicit durable coordinator log. Until savepoints
    /// exist, an execution-time DML failure rolls back the whole transaction;
    /// legacy second-writer preflight rejection leaves it active for rollback.
    pub fn execute_in(
        &mut self,
        transaction: &mut Transaction,
        source: &str,
    ) -> Result<ExecutionResult, DatabaseError> {
        match self.prepare_sql_statement_in(transaction, source, &[])? {
            PreparedSqlStatement::Relational(prepared) => {
                self.execute_prepared_in(transaction, &prepared, &[])
            }
            PreparedSqlStatement::Ddl(prepared) => self
                .execute_ddl_in(transaction, &prepared)
                .map(|_| ExecutionResult::AffectedRows(0)),
        }
    }

    pub fn execute_prepared_in(
        &mut self,
        transaction: &mut Transaction,
        prepared: &PreparedStatement,
        values: &[ScalarValue],
    ) -> Result<ExecutionResult, DatabaseError> {
        self.validate_transaction(transaction)?;
        self.validate_prepared_dependencies(prepared, Some(transaction))?;
        let logical = bind_statement(&prepared.compiled, values)?;
        if let Some(affected_rows) =
            deferred_backfill::try_execute_adopted_update(self, transaction, &logical)?
        {
            return Ok(ExecutionResult::AffectedRows(affected_rows));
        }
        if let Some(composition) = transaction.schema_composition.plan()
            && Self::is_backfill_candidate(composition)
        {
            let target = composition
                .touched
                .keys()
                .next()
                .copied()
                .ok_or(SchemaMutationError::Corrupt("backfill target absent"))?;
            if logical
                .read_tables()
                .into_iter()
                .chain(logical.write_tables())
                .any(|table| table != target)
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
                )
                .into());
            }
        }
        if let Some(source_backfill) = transaction.schema_composition.source_backfill() {
            let target = source_backfill
                .logical
                .touched
                .keys()
                .next()
                .copied()
                .ok_or(SchemaMutationError::Corrupt(
                    "source-backfill target absent",
                ))?;
            if logical
                .read_tables()
                .into_iter()
                .chain(logical.write_tables())
                .any(|table| table != target)
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
                )
                .into());
            }
        }
        self.ensure_schema_materialized_with_backfill(transaction, true)?;
        if matches!(
            transaction.schema_composition,
            schema_composition::SchemaCompositionState::Refining(_)
                | schema_composition::SchemaCompositionState::IndexEvacuating(_)
                | schema_composition::SchemaCompositionState::RefiningAfterEvacuation(_)
                | schema_composition::SchemaCompositionState::IndexFinalizing(_)
                | schema_composition::SchemaCompositionState::RefiningIndex(_)
                | schema_composition::SchemaCompositionState::SourceRefining(_)
                | schema_composition::SchemaCompositionState::SourceIndexFinalizing(_)
                | schema_composition::SchemaCompositionState::LateCloneMaterializing(_)
                | schema_composition::SchemaCompositionState::LateCloneReady(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceRefining(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceBackfilling(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceFinalRefining(_)
                | schema_composition::SchemaCompositionState::AdoptedSourceIndexFinalizing(_)
        ) && (!logical.read_tables().is_empty() || !logical.write_tables().is_empty())
        {
            return Err(SchemaMutationError::MigrationDataAccessAfterRefinement.into());
        }
        let physical = self.plan_logical_statement_in(&logical, transaction);
        if let PhysicalStatement::Query(plan) = &physical {
            let storage_ids = self.storage_ids_for_tables_in(logical.read_tables(), transaction)?;
            let view = transaction.begin_read_view(&storage_ids, &mut self.registry)?;
            return self
                .execute_query_plan_in(plan, &view, transaction)
                .map(ExecutionResult::Query);
        }
        if transaction.has_pending_index_creations() {
            return Err(CoordinatorError::SchemaMutationRequiresDatabaseCommit.into());
        }
        let table_id = statement_table_id(&physical).ok_or(DatabaseError::ExpectedQuery)?;
        if let Some(storage_id) = self.single_storage_in(table_id, transaction)? {
            let _ = transaction.write_context(storage_id, &mut self.registry)?;
        }
        match self.execute_mutation_in(transaction, &physical) {
            Ok(result) => Ok(result),
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    /// Round 31 audit probe only: validate a prospective `SET NOT NULL`
    /// against the transaction-visible private staged Heap. This deliberately
    /// does not mutate schema state or make post-materialization DDL legal.
    #[cfg(test)]
    fn audit_validate_staged_not_null(
        &mut self,
        transaction: &mut Transaction,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        let storage_id = transaction
            .staged_binding(table_id)
            .ok_or(SchemaMutationError::Corrupt("audit staged binding absent"))?;
        let view = transaction.begin_read_view(&[storage_id], &mut self.registry)?;
        let storage_view = view
            .iter()
            .find_map(|(id, view)| (id == storage_id).then_some(view))
            .ok_or(SchemaMutationError::Corrupt(
                "audit staged read view absent",
            ))?;
        let storage = transaction
            .execution_staged_storages_mut()
            .into_iter()
            .find(|storage| storage.storage_id() == storage_id)
            .ok_or(SchemaMutationError::Corrupt("audit staged Heap absent"))?;
        let rows = storage.scan_columns_with_view(&[column_id], storage_view)?;
        if rows
            .iter()
            .any(|(_, values)| matches!(values.as_slice(), [ScalarValue::Null]))
        {
            return Err(SchemaMutationError::NotNullViolation(column_id).into());
        }
        Ok(())
    }

    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.committed.schema
    }

    #[cfg(test)]
    fn plan_source(&self, source: &str) -> Result<PhysicalStatement, DatabaseError> {
        self.compile_and_plan(source).map(|(_, physical)| physical)
    }

    fn compile_and_plan(
        &self,
        source: &str,
    ) -> Result<(CompiledStatement, PhysicalStatement), DatabaseError> {
        let compiled = compile_statement(&self.committed.schema, source)?;
        let physical = self.plan_logical_statement(&compiled.logical_statement);
        Ok((compiled, physical))
    }

    fn plan_logical_statement(&self, logical: &netbadb_rel::LogicalStatement) -> PhysicalStatement {
        plan_statement_with_columnar_snapshots(
            logical,
            &self.planner_table_statistics(),
            &self.planner_access_paths(),
            &self.planner_range_tables(),
            &self.planner_columnar_projections(),
        )
    }

    fn plan_logical_statement_in(
        &self,
        logical: &netbadb_rel::LogicalStatement,
        transaction: &Transaction,
    ) -> PhysicalStatement {
        let mut staged = transaction
            .schema_composition
            .materialized()
            .map(|materialized| materialized.staged.values().collect::<Vec<_>>())
            .unwrap_or_default();
        if let Some(materialized) = transaction.schema_composition.materialized_index() {
            staged.extend(materialized.staged.values());
        }
        if let Some(storage) = transaction
            .schema_mutation
            .as_ref()
            .and_then(|mutation| mutation.staged.as_ref())
        {
            staged.push(storage);
        }
        let staged_tables = staged
            .iter()
            .map(|storage| storage.table().id)
            .collect::<Vec<_>>();
        let mut statistics = self
            .planner_table_statistics()
            .into_iter()
            .filter(|entry| !staged_tables.contains(&entry.table_id))
            .collect::<Vec<_>>();
        let mut access_paths = self
            .planner_access_paths()
            .into_iter()
            .filter(|entry| !staged_tables.contains(&entry.table_id))
            .collect::<Vec<_>>();
        if let Some(materialized) = transaction.schema_composition.materialized_index() {
            for plan in &materialized.intent.tables {
                if let schema_mutation_journal::SchemaIndexTablePlan::InPlaceIndexDelta {
                    table,
                    base_indexes,
                    final_indexes,
                    storage,
                    ..
                } = plan
                {
                    let dropped_columns = base_indexes
                        .active
                        .iter()
                        .filter(|base| {
                            !final_indexes.active.iter().any(|final_index| {
                                final_index.id == base.id && final_index == *base
                            })
                        })
                        .map(|index| index.column_id)
                        .collect::<Vec<_>>();
                    access_paths.retain(|path| {
                        path.table_id != *table || !dropped_columns.contains(&path.column_id)
                    });
                    if let Some(publication) = materialized
                        .publications
                        .iter()
                        .find(|publication| publication.storage == *storage)
                    {
                        access_paths.extend(publication.creates.iter().map(|definition| {
                            AccessPath {
                                table_id: *table,
                                column_id: definition.column_id,
                                id: AccessPathId(definition.handle.meta_page.page_id().0),
                                capabilities: AccessPathCapabilities {
                                    point_lookup: true,
                                    range_lookup: true,
                                    ordered: true,
                                },
                                statistics: None,
                                cost_hints: None,
                            }
                        }));
                    }
                }
            }
        }
        for storage in staged {
            statistics.push(TableAccessStatistics {
                table_id: storage.table().id,
                statistics: None,
            });
            access_paths.extend(storage.access_paths().into_iter().map(|path| AccessPath {
                table_id: storage.table().id,
                column_id: path.column_id,
                id: path.id,
                capabilities: AccessPathCapabilities {
                    point_lookup: path.capabilities.point_lookup,
                    range_lookup: path.capabilities.range_lookup,
                    ordered: path.capabilities.ordered,
                },
                statistics: None,
                cost_hints: path.cost_hints.map(|hints| AccessCostHints {
                    point_probe_base_cost: hints.point_probe_base_cost,
                    expected_point_io: hints.expected_point_io,
                    range_startup_cost: hints.range_startup_cost,
                    sequential_unit_cost: hints.sequential_unit_cost,
                }),
            }));
        }
        plan_statement_with_partition_snapshots(
            logical,
            &statistics,
            &access_paths,
            &self.planner_range_tables(),
        )
    }

    fn planner_table_statistics(&self) -> Vec<TableAccessStatistics> {
        self.bindings
            .iter()
            .filter_map(|placement| {
                let TablePlacement::Single {
                    table_id,
                    storage_id,
                } = placement
                else {
                    return None;
                };
                self.registry
                    .get(*storage_id)
                    .map(|storage| TableAccessStatistics {
                        table_id: *table_id,
                        statistics: storage.table_statistics(),
                    })
            })
            .collect()
    }

    fn planner_access_paths(&self) -> Vec<AccessPath> {
        let mut paths = Vec::new();
        for placement in self.bindings.iter() {
            let TablePlacement::Single {
                table_id,
                storage_id,
            } = placement
            else {
                continue;
            };
            let Some(storage) = self.registry.get(*storage_id) else {
                continue;
            };
            paths.extend(storage.access_paths().into_iter().map(|path| AccessPath {
                table_id: *table_id,
                column_id: path.column_id,
                id: path.id,
                capabilities: AccessPathCapabilities {
                    point_lookup: path.capabilities.point_lookup,
                    range_lookup: path.capabilities.range_lookup,
                    ordered: path.capabilities.ordered,
                },
                statistics: path.statistics,
                cost_hints: path.cost_hints.map(|hints| AccessCostHints {
                    point_probe_base_cost: hints.point_probe_base_cost,
                    expected_point_io: hints.expected_point_io,
                    range_startup_cost: hints.range_startup_cost,
                    sequential_unit_cost: hints.sequential_unit_cost,
                }),
            }));
        }
        paths
    }

    fn planner_range_tables(&self) -> Vec<RangeTablePlanningSnapshot> {
        self.bindings
            .iter()
            .filter_map(|placement| {
                let TablePlacement::RangePartitioned {
                    table_id,
                    partition_key,
                    partitions,
                    ..
                } = placement
                else {
                    return None;
                };
                Some(RangeTablePlanningSnapshot {
                    table_id: *table_id,
                    partition_key: *partition_key,
                    partitions: partitions
                        .iter()
                        .filter_map(|partition| {
                            let storage = self.registry.get(partition.storage_id)?;
                            Some(PartitionPlanningSnapshot {
                                partition_id: partition.partition_id,
                                storage_id: partition.storage_id,
                                lower: partition.lower.clone(),
                                upper: partition.upper.clone(),
                                statistics: storage.table_statistics(),
                                access_paths: storage
                                    .access_paths()
                                    .into_iter()
                                    .map(|path| AccessPath {
                                        table_id: *table_id,
                                        column_id: path.column_id,
                                        id: path.id,
                                        capabilities: AccessPathCapabilities {
                                            point_lookup: path.capabilities.point_lookup,
                                            range_lookup: path.capabilities.range_lookup,
                                            ordered: path.capabilities.ordered,
                                        },
                                        statistics: path.statistics,
                                        cost_hints: path.cost_hints.map(|hints| AccessCostHints {
                                            point_probe_base_cost: hints.point_probe_base_cost,
                                            expected_point_io: hints.expected_point_io,
                                            range_startup_cost: hints.range_startup_cost,
                                            sequential_unit_cost: hints.sequential_unit_cost,
                                        }),
                                    })
                                    .collect(),
                            })
                        })
                        .collect(),
                })
            })
            .collect()
    }

    fn planner_columnar_projections(&self) -> Vec<ColumnarProjectionPlanningSnapshot> {
        self.projections
            .iter()
            .filter_map(|entry| {
                let projection = entry.projection.as_ref()?;
                let metadata = projection.metadata();
                let placement = self.bindings.placement(metadata.table_id).ok()?;
                let TablePlacement::Single { storage_id, .. } = placement else {
                    return None;
                };
                if *storage_id != metadata.source_storage_id {
                    return None;
                }
                let storage = self.registry.get(*storage_id)?;
                if storage.table().fingerprint().ok()? != metadata.schema_fingerprint {
                    return None;
                }
                let (
                    delta_segment_count,
                    delta_bytes,
                    delta_mutation_count,
                    delta_live_row_count,
                    suppressed_version_count,
                ) = match &metadata.incremental {
                    Some(incremental) => {
                        let stream = storage.inspect_change_stream();
                        if stream.status != netbadb_storage::ChangeStreamStatus::Enabled
                            || stream.generation != Some(incremental.stream_generation)
                            || stream.current_data_version != incremental.applied_frontier
                        {
                            return None;
                        }
                        (
                            incremental.delta_segments.len() as u64,
                            incremental.delta_bytes,
                            incremental.delta_mutation_count,
                            incremental.delta_live_row_count,
                            incremental.suppressed_version_count,
                        )
                    }
                    None => {
                        if storage.current_snapshot_token().ok()? != metadata.source_token {
                            return None;
                        }
                        (0, 0, 0, 0, 0)
                    }
                };
                Some(ColumnarProjectionPlanningSnapshot {
                    projection_id: metadata.id,
                    generation: metadata.generation,
                    table_id: metadata.table_id,
                    source_storage_id: metadata.source_storage_id,
                    projected_columns: metadata
                        .columns
                        .iter()
                        .map(|column| column.column_id)
                        .collect(),
                    row_count: metadata.row_count,
                    row_group_count: metadata.row_group_count,
                    segment_bytes: metadata.segment_bytes,
                    delta_segment_count,
                    delta_bytes,
                    delta_mutation_count,
                    delta_live_row_count,
                    suppressed_version_count,
                    row_groups: projection
                        .row_group_statistics()
                        .into_iter()
                        .map(|group| ColumnarRowGroupPlanningSnapshot {
                            rows: group.rows,
                            columns: group
                                .columns
                                .into_iter()
                                .map(|(column_id, statistics)| ColumnarZoneMapPlanningSnapshot {
                                    column_id,
                                    null_count: statistics.null_count,
                                    minimum: statistics.minimum,
                                    maximum: statistics.maximum,
                                    encoded_bytes: statistics.encoded_bytes,
                                })
                                .collect(),
                        })
                        .collect(),
                })
            })
            .collect()
    }

    fn begin_database_transaction(
        &mut self,
        isolation_level: IsolationLevel,
    ) -> Result<Transaction, DatabaseError> {
        self.ensure_schema_available(None)?;
        let id = self.next_transaction_id;
        let next =
            id.0.checked_add(1)
                .ok_or(CoordinatorError::TransactionIdExhausted)?;
        self.next_transaction_id = DatabaseTxnId(next);
        Ok(DatabaseTransaction::new(
            Rc::clone(&self.transaction_owner),
            id,
            isolation_level,
            self.coordinator.as_ref().map(Rc::clone),
            self.published_visibility.as_ref().map(Rc::clone),
        ))
    }

    fn autocommit_read_view(
        &self,
        storage_ids: &[StorageId],
    ) -> Result<DatabaseReadView, DatabaseError> {
        self.ensure_schema_available(None)?;
        let snapshot = self
            .published_visibility
            .as_ref()
            .map(|published| {
                published
                    .try_borrow()
                    .map(|state| state.snapshot.clone())
                    .map_err(|_| CoordinatorError::PublishedVisibilityBusy)
            })
            .transpose()?;
        let mut views = Vec::with_capacity(storage_ids.len());
        for storage_id in storage_ids {
            let storage =
                self.registry
                    .get(*storage_id)
                    .ok_or(StorageRegistryError::UnknownStorageId {
                        storage_id: *storage_id,
                    })?;
            let view = match &snapshot {
                Some(snapshot) => storage.read_view_at(snapshot.boundary(*storage_id).ok_or(
                    CoordinatorError::SnapshotMissingStorage {
                        storage_id: *storage_id,
                    },
                )?)?,
                None => storage.read_view()?,
            };
            views.push((*storage_id, view));
        }
        Ok(match snapshot {
            Some(snapshot) => {
                DatabaseReadView::global(None, IsolationLevel::ReadCommitted, snapshot, views)
            }
            None => DatabaseReadView::autocommit(IsolationLevel::ReadCommitted, views),
        })
    }

    fn execute_query_plan(
        &mut self,
        plan: &netbadb_planner::PhysicalPlan,
        view: &DatabaseReadView,
        staged: Option<&mut TableStorage>,
        columnar_statistics: Option<&mut ColumnarExecutionStatistics>,
    ) -> Result<QueryResult, DatabaseError> {
        let staged_table = staged.as_ref().map(|storage| storage.table().id);
        let mut bindings = self
            .bindings
            .iter()
            .filter_map(|placement| match placement {
                TablePlacement::Single {
                    table_id,
                    storage_id,
                } if Some(*table_id) != staged_table => Some(ExecutionStorageBinding {
                    table_id: *table_id,
                    storage_id: *storage_id,
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        if let Some(storage) = staged.as_ref() {
            bindings.push(ExecutionStorageBinding {
                table_id: storage.table().id,
                storage_id: storage.storage_id(),
            });
        }
        let read_views = view
            .iter()
            .map(|(storage_id, view)| ExecutionReadView { storage_id, view })
            .collect::<Vec<_>>();
        let mut storages = self
            .registry
            .iter_mut()
            .map(|entry| ExecutionStorage {
                storage_id: entry.id,
                storage: &mut entry.storage,
            })
            .chain(staged.into_iter().map(|storage| ExecutionStorage {
                storage_id: storage.storage_id(),
                storage,
            }))
            .collect::<Vec<_>>();
        let projections = self
            .projections
            .iter()
            .filter_map(|entry| entry.projection.as_ref())
            .map(|projection| ExecutionColumnarProjection {
                projection_id: projection.metadata().id,
                projection,
            })
            .collect::<Vec<_>>();
        Ok(execute_with_columnar_context(
            plan,
            &bindings,
            &mut storages,
            &read_views,
            &projections,
            columnar_statistics,
        )?)
    }

    fn execute_query_plan_in(
        &mut self,
        plan: &netbadb_planner::PhysicalPlan,
        view: &DatabaseReadView,
        transaction: &mut Transaction,
    ) -> Result<QueryResult, DatabaseError> {
        let staged_tables = transaction
            .schema_composition
            .materialized()
            .map(|materialized| {
                materialized
                    .staged
                    .values()
                    .map(|storage| storage.table().id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut staged_tables = staged_tables;
        if let Some(materialized) = transaction.schema_composition.materialized_index() {
            staged_tables.extend(
                materialized
                    .staged
                    .values()
                    .map(|storage| storage.table().id),
            );
        }
        let legacy_staged_table = transaction
            .schema_mutation
            .as_ref()
            .and_then(|mutation| mutation.staged.as_ref())
            .map(|storage| storage.table().id);
        let mut bindings = self
            .bindings
            .iter()
            .filter_map(|placement| match placement {
                TablePlacement::Single {
                    table_id,
                    storage_id,
                } if !staged_tables.contains(table_id)
                    && Some(*table_id) != legacy_staged_table =>
                {
                    Some(ExecutionStorageBinding {
                        table_id: *table_id,
                        storage_id: *storage_id,
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if let Some(materialized) = transaction.schema_composition.materialized() {
            bindings.extend(
                materialized
                    .staged
                    .values()
                    .map(|storage| ExecutionStorageBinding {
                        table_id: storage.table().id,
                        storage_id: storage.storage_id(),
                    }),
            );
        }
        if let Some(materialized) = transaction.schema_composition.materialized_index() {
            bindings.extend(
                materialized
                    .staged
                    .values()
                    .map(|storage| ExecutionStorageBinding {
                        table_id: storage.table().id,
                        storage_id: storage.storage_id(),
                    }),
            );
        }
        if let Some(storage) = transaction
            .schema_mutation
            .as_ref()
            .and_then(|mutation| mutation.staged.as_ref())
        {
            bindings.push(ExecutionStorageBinding {
                table_id: storage.table().id,
                storage_id: storage.storage_id(),
            });
        }
        let read_views = view
            .iter()
            .map(|(storage_id, view)| ExecutionReadView { storage_id, view })
            .collect::<Vec<_>>();
        let mut storages = self
            .registry
            .iter_mut()
            .map(|entry| ExecutionStorage {
                storage_id: entry.id,
                storage: &mut entry.storage,
            })
            .collect::<Vec<_>>();
        storages.extend(
            transaction
                .execution_staged_storages_mut()
                .into_iter()
                .map(|storage| ExecutionStorage {
                    storage_id: storage.storage_id(),
                    storage,
                }),
        );
        Ok(execute_with_columnar_context(
            plan,
            &bindings,
            &mut storages,
            &read_views,
            &[],
            None,
        )?)
    }

    fn execute_mutation_in(
        &mut self,
        transaction: &mut Transaction,
        physical: &PhysicalStatement,
    ) -> Result<ExecutionResult, DatabaseError> {
        let table_id = statement_table_id(physical).ok_or(DatabaseError::ExpectedQuery)?;
        let storage_ids = self.storage_ids_for_tables_in(vec![table_id], transaction)?;
        let view = transaction.begin_read_view(&storage_ids, &mut self.registry)?;
        let staged_tables = transaction
            .schema_composition
            .materialized()
            .map(|materialized| {
                materialized
                    .staged
                    .values()
                    .map(|storage| storage.table().id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut staged_tables = staged_tables;
        if let Some(materialized) = transaction.schema_composition.materialized_index() {
            staged_tables.extend(
                materialized
                    .staged
                    .values()
                    .map(|storage| storage.table().id),
            );
        }
        let legacy_staged_table = transaction
            .schema_mutation
            .as_ref()
            .and_then(|mutation| mutation.staged.as_ref())
            .map(|storage| storage.table().id);
        let mut bindings = self
            .bindings
            .iter()
            .filter_map(|placement| match placement {
                TablePlacement::Single {
                    table_id,
                    storage_id,
                } if !staged_tables.contains(table_id)
                    && Some(*table_id) != legacy_staged_table =>
                {
                    Some(ExecutionStorageBinding {
                        table_id: *table_id,
                        storage_id: *storage_id,
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if let Some(materialized) = transaction.schema_composition.materialized() {
            bindings.extend(
                materialized
                    .staged
                    .values()
                    .map(|storage| ExecutionStorageBinding {
                        table_id: storage.table().id,
                        storage_id: storage.storage_id(),
                    }),
            );
        }
        if let Some(materialized) = transaction.schema_composition.materialized_index() {
            bindings.extend(
                materialized
                    .staged
                    .values()
                    .map(|storage| ExecutionStorageBinding {
                        table_id: storage.table().id,
                        storage_id: storage.storage_id(),
                    }),
            );
        }
        if let Some(storage) = transaction
            .schema_mutation
            .as_ref()
            .and_then(|mutation| mutation.staged.as_ref())
        {
            bindings.push(ExecutionStorageBinding {
                table_id: storage.table().id,
                storage_id: storage.storage_id(),
            });
        }
        let read_views = view
            .iter()
            .map(|(storage_id, view)| ExecutionReadView { storage_id, view })
            .collect::<Vec<_>>();
        let prepared = {
            let mut storages = self
                .registry
                .iter_mut()
                .map(|entry| ExecutionStorage {
                    storage_id: entry.id,
                    storage: &mut entry.storage,
                })
                .collect::<Vec<_>>();
            storages.extend(transaction.execution_staged_storages_mut().into_iter().map(
                |storage| ExecutionStorage {
                    storage_id: storage.storage_id(),
                    storage,
                },
            ));
            prepare_mutation_with_storage_context(physical, &bindings, &mut storages, &read_views)?
        };
        self.apply_prepared_mutation(transaction, prepared)
    }

    fn apply_prepared_mutation(
        &mut self,
        transaction: &mut Transaction,
        prepared: PreparedMutation,
    ) -> Result<ExecutionResult, DatabaseError> {
        match prepared {
            PreparedMutation::Insert { table_id, values } => {
                let storage_id =
                    self.route_storage_for_values_in(table_id, &values, transaction)?;
                transaction
                    .with_write_storage(storage_id, &mut self.registry, |storage, context| {
                        storage.insert_in(context, &values)
                    })
                    .map_err(dml_storage_error)?;
                Ok(ExecutionResult::AffectedRows(1))
            }
            PreparedMutation::Update { table_id, rows } => {
                let affected =
                    u64::try_from(rows.len()).map_err(|_| ExecutionError::AffectedRowsOverflow)?;
                let routed = rows
                    .into_iter()
                    .map(|row| {
                        let destination =
                            self.route_storage_for_values_in(table_id, &row.values, transaction)?;
                        Ok((row, destination))
                    })
                    .collect::<Result<Vec<_>, DatabaseError>>()?;
                for (row, destination) in routed {
                    let source = row.row.storage_id();
                    if source == destination {
                        transaction
                            .with_write_storage(source, &mut self.registry, |storage, context| {
                                storage.update_in(context, row.row, &row.values)
                            })
                            .map_err(dml_storage_error)?;
                    } else {
                        transaction
                            .with_write_storage(source, &mut self.registry, |storage, context| {
                                storage.delete_in(context, row.row)
                            })
                            .map_err(dml_storage_error)?;
                        transaction
                            .with_write_storage(
                                destination,
                                &mut self.registry,
                                |storage, context| storage.insert_in(context, &row.values),
                            )
                            .map_err(dml_storage_error)?;
                    }
                }
                Ok(ExecutionResult::AffectedRows(affected))
            }
            PreparedMutation::Delete { rows, .. } => {
                let affected =
                    u64::try_from(rows.len()).map_err(|_| ExecutionError::AffectedRowsOverflow)?;
                for row in rows {
                    let storage_id = row.storage_id();
                    transaction
                        .with_write_storage(storage_id, &mut self.registry, |storage, context| {
                            storage.delete_in(context, row)
                        })
                        .map_err(dml_storage_error)?;
                }
                Ok(ExecutionResult::AffectedRows(affected))
            }
        }
    }

    fn storage_ids_for_tables(
        &self,
        table_ids: Vec<TableId>,
    ) -> Result<Vec<StorageId>, DatabaseError> {
        let mut storage_ids = Vec::with_capacity(table_ids.len());
        for table_id in table_ids {
            for storage_id in self.bindings.placement(table_id)?.storage_ids() {
                if !storage_ids.contains(&storage_id) {
                    storage_ids.push(storage_id);
                }
            }
        }
        Ok(storage_ids)
    }

    fn primary_storage_id(&self) -> Result<StorageId, DatabaseError> {
        if self.registry.len() != 1 {
            return if self.registry.len() == 0 {
                Err(DatabaseError::EmptyCatalog)
            } else {
                Err(DatabaseError::TableSelectionRequired)
            };
        }
        self.bindings
            .iter()
            .next()
            .and_then(|placement| placement.storage_ids().next())
            .ok_or(DatabaseError::EmptyCatalog)
    }

    #[cfg(test)]
    fn storage_mut(&mut self, table_id: TableId) -> Result<&mut TableStorage, DatabaseError> {
        let storage_id = self.bindings.resolve_single(table_id)?;
        self.registry
            .get_mut(storage_id)
            .ok_or_else(|| StorageRegistryError::UnknownStorageId { storage_id }.into())
    }

    fn route_storage_for_values(
        &self,
        table_id: TableId,
        values: &[ScalarValue],
    ) -> Result<StorageId, DatabaseError> {
        match self.bindings.placement(table_id)? {
            TablePlacement::Single { storage_id, .. } => Ok(*storage_id),
            TablePlacement::RangePartitioned {
                partition_key,
                partitions,
                ..
            } => {
                let table = self
                    .committed
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == table_id)
                    .ok_or(PartitionError::CatalogCorrupt(
                        "partition table schema is missing",
                    ))?;
                let position = table
                    .columns
                    .iter()
                    .position(|column| column.id == *partition_key)
                    .ok_or(PartitionError::PartitionKeyMissing {
                        table_id,
                        column_id: *partition_key,
                    })?;
                let value = values
                    .get(position)
                    .ok_or(PartitionError::NoPartitionForValue(ScalarValue::Null))?;
                Ok(route_partition(partitions, value)?.storage_id)
            }
        }
    }

    fn validate_transaction(&self, transaction: &Transaction) -> Result<(), DatabaseError> {
        self.ensure_schema_available(Some(transaction.id()))?;
        transaction
            .validate_owner(&self.transaction_owner)
            .map_err(DatabaseError::from)
    }
}

fn dml_storage_error(error: CoordinatorError) -> DatabaseError {
    match error {
        CoordinatorError::Storage(error) => {
            DatabaseError::Execution(ExecutionError::Storage(error))
        }
        other => other.into(),
    }
}

fn statement_table_id(statement: &PhysicalStatement) -> Option<TableId> {
    match statement {
        PhysicalStatement::Query(_) => None,
        PhysicalStatement::Insert { table_id, .. }
        | PhysicalStatement::Update { table_id, .. }
        | PhysicalStatement::Delete { table_id, .. } => Some(*table_id),
    }
}

fn validate_catalog_paths(entries: &[(PathBuf, TableDef)]) -> Result<(), DatabaseError> {
    if entries.is_empty() {
        return Err(DatabaseError::EmptyCatalog);
    }
    for (index, (path, _)) in entries.iter().enumerate() {
        for (other_path, _) in &entries[..index] {
            if path == other_path {
                return Err(DatabaseError::DuplicateStoragePath(path.clone()));
            }
        }
    }
    Ok(())
}

fn storage_id_for_position(position: usize) -> Result<StorageId, StorageRegistryError> {
    let ordinal = position
        .checked_add(1)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(StorageRegistryError::StorageIdExhausted)?;
    Ok(StorageId(ordinal))
}

fn validate_partition_key(
    table: &TableDef,
    partition_key: ColumnId,
) -> Result<PhysicalType, DatabaseError> {
    let column = table
        .column_by_id(partition_key)
        .ok_or(PartitionError::PartitionKeyMissing {
            table_id: table.id,
            column_id: partition_key,
        })?;
    let physical = column.semantic_type().physical;
    if !matches!(physical, PhysicalType::Int64 | PhysicalType::UInt64) {
        return Err(PartitionError::UnsupportedPartitionKeyType(physical).into());
    }
    if column.nullable {
        return Err(PartitionError::NullablePartitionKey {
            table_id: table.id,
            column_id: partition_key,
        }
        .into());
    }
    Ok(physical)
}

fn prevalidate_placement_specs(specs: &[TablePlacementSpec]) -> Result<(), DatabaseError> {
    let mut next_storage_id = 1_u64;
    let mut tables = Vec::with_capacity(specs.len());
    for spec in specs {
        let table = spec.table();
        let placement = match spec {
            TablePlacementSpec::Single { .. } => {
                let storage_id = StorageId(next_storage_id);
                next_storage_id = next_storage_id
                    .checked_add(1)
                    .ok_or(StorageRegistryError::StorageIdExhausted)?;
                TablePlacement::Single {
                    table_id: table.id,
                    storage_id,
                }
            }
            TablePlacementSpec::RangePartitioned {
                partition_key,
                partitions,
                ..
            } => {
                let key_type = validate_partition_key(table, *partition_key)?;
                let mut bindings = Vec::with_capacity(partitions.len());
                for partition in partitions {
                    let storage_id = StorageId(next_storage_id);
                    next_storage_id = next_storage_id
                        .checked_add(1)
                        .ok_or(StorageRegistryError::StorageIdExhausted)?;
                    bindings.push(RangePartitionBinding {
                        partition_id: partition.partition_id,
                        storage_id,
                        lower: partition.lower.clone(),
                        upper: partition.upper.clone(),
                    });
                }
                TablePlacement::RangePartitioned {
                    table_id: table.id,
                    partition_key: *partition_key,
                    key_type,
                    partitions: canonicalize_partitions(key_type, bindings)?,
                }
            }
        };
        tables.push(CatalogTable {
            table_id: table.id,
            schema_fingerprint: table.fingerprint()?,
            placement,
        });
    }
    PartitionCatalog { tables }.validate()?;
    Ok(())
}

fn validate_physical_paths(
    paths: &[PathBuf],
    config: &PartitionCatalogConfig,
) -> Result<(), DatabaseError> {
    if paths.is_empty() {
        return Err(DatabaseError::EmptyCatalog);
    }
    for (position, path) in paths.iter().enumerate() {
        if paths[..position].contains(path) {
            return Err(DatabaseError::DuplicateStoragePath(path.clone()));
        }
        if path == config.catalog_path() || path == config.coordinator_log_path() {
            return Err(DatabaseError::CoordinatorPathConflictsWithStorage(
                path.clone(),
            ));
        }
    }
    if config.catalog_path() == config.coordinator_log_path() {
        return Err(DatabaseError::CoordinatorPathConflictsWithStorage(
            config.catalog_path().to_owned(),
        ));
    }
    Ok(())
}

fn create_partition_storage(
    path: &Path,
    table: TableDef,
    storage_id: StorageId,
    storages: &mut Vec<TableStorage>,
    created_paths: &mut Vec<PathBuf>,
) -> Result<(), DatabaseError> {
    match TableStorage::create_heap_with_storage_id(path, table, storage_id) {
        Ok(storage) => {
            storages.push(storage);
            created_paths.push(path.to_owned());
            Ok(())
        }
        Err(creation) => {
            storages.clear();
            if let Some((cleanup_path, cleanup)) = cleanup_created_table_files(created_paths) {
                return Err(DatabaseError::CreateTablesRollback {
                    creation,
                    cleanup_path,
                    cleanup,
                });
            }
            Err(creation.into())
        }
    }
}

fn validate_catalog_schemas(
    catalog: &PartitionCatalog,
    tables: &[TableDef],
) -> Result<(), DatabaseError> {
    if catalog.tables.len() != tables.len() {
        return Err(PartitionError::CatalogCorrupt("logical table count mismatch").into());
    }
    for entry in &catalog.tables {
        let table = tables
            .iter()
            .find(|table| table.id == entry.table_id)
            .ok_or(PartitionError::SchemaFingerprintMismatch {
                table_id: entry.table_id,
            })?;
        if table.fingerprint()? != entry.schema_fingerprint {
            return Err(PartitionError::SchemaFingerprintMismatch {
                table_id: entry.table_id,
            }
            .into());
        }
        if let TablePlacement::RangePartitioned {
            partition_key,
            key_type,
            ..
        } = &entry.placement
        {
            if validate_partition_key(table, *partition_key)? != *key_type {
                return Err(PartitionError::SchemaFingerprintMismatch {
                    table_id: entry.table_id,
                }
                .into());
            }
        }
    }
    Ok(())
}

fn validate_catalog_storage_set(
    catalog: &PartitionCatalog,
    storages: &[InspectedStorage],
) -> Result<(), DatabaseError> {
    let expected = catalog
        .tables
        .iter()
        .flat_map(|entry| entry.placement.storage_ids())
        .collect::<Vec<_>>();
    if expected.len() != storages.len() {
        return Err(PartitionError::CatalogCorrupt("physical storage count mismatch").into());
    }
    for storage_id in expected {
        if !storages
            .iter()
            .any(|storage| storage.recovery.storage_id == storage_id)
        {
            return Err(PartitionError::PartitionStorageMissing(storage_id).into());
        }
    }
    Ok(())
}

#[derive(Debug)]
struct InspectedStorage {
    path: PathBuf,
    table: TableDef,
    recovery: HeapRecoveryInspection,
}

#[derive(Debug)]
struct GenericRecoveryInspection {
    storage_id: StorageId,
    prepared_transactions: Vec<PreparedTransaction>,
}

#[derive(Debug)]
struct GenericInspectedStorage {
    recovery: GenericRecoveryInspection,
}

fn create_explicit_storages(
    specs: Vec<TableStorageCreateSpec>,
) -> Result<(Schema, Vec<TableStorage>), DatabaseError> {
    if specs.is_empty() {
        return Err(DatabaseError::EmptyCatalog);
    }
    validate_create_specs(&specs)?;
    let schema = Schema::new(specs.iter().map(|spec| spec.table().clone()).collect())?;
    let mut storages = Vec::with_capacity(specs.len());
    let mut created = Vec::new();
    for (position, spec) in specs.into_iter().enumerate() {
        let storage_id = storage_id_for_position(position)?;
        let result = match &spec {
            TableStorageCreateSpec::Heap { path, table } => {
                TableStorage::create_heap_with_storage_id(path, table.clone(), storage_id)
            }
            TableStorageCreateSpec::Lsm {
                directory,
                table,
                clustering_column,
            } => TableStorage::create_lsm_with_storage_id(
                directory,
                table.clone(),
                *clustering_column,
                storage_id,
            ),
        };
        match result {
            Ok(storage) => {
                storages.push(storage);
                created.push(spec);
            }
            Err(error) => {
                drop(storages);
                cleanup_explicit_specs(&created);
                return Err(error.into());
            }
        }
    }
    Ok((schema, storages))
}

fn validate_create_specs(specs: &[TableStorageCreateSpec]) -> Result<(), DatabaseError> {
    validate_unique_paths(specs.iter().map(TableStorageCreateSpec::path))
}

fn validate_open_specs(specs: &[TableStorageOpenSpec]) -> Result<(), DatabaseError> {
    if specs.is_empty() {
        return Err(DatabaseError::EmptyCatalog);
    }
    validate_unique_paths(specs.iter().map(TableStorageOpenSpec::path))
}

fn validate_unique_paths<'a>(paths: impl Iterator<Item = &'a Path>) -> Result<(), DatabaseError> {
    let mut seen = std::collections::BTreeSet::new();
    for path in paths {
        if !seen.insert(path.to_path_buf()) {
            return Err(DatabaseError::DuplicateStoragePath(path.to_path_buf()));
        }
    }
    Ok(())
}

fn validate_explicit_coordinator_path_create(
    specs: &[TableStorageCreateSpec],
    config: &DatabaseCoordinatorConfig,
) -> Result<(), DatabaseError> {
    validate_create_specs(specs)?;
    if specs.iter().any(|spec| spec.path() == config.log_path()) {
        return Err(DatabaseError::CoordinatorPathConflictsWithStorage(
            config.log_path().to_owned(),
        ));
    }
    Ok(())
}

fn cleanup_explicit_specs(specs: &[TableStorageCreateSpec]) {
    for spec in specs.iter().rev() {
        match spec {
            TableStorageCreateSpec::Heap { path, .. } => {
                let _ = cleanup_created_table_files(std::slice::from_ref(path));
            }
            TableStorageCreateSpec::Lsm { directory, .. } => {
                if directory.exists() {
                    let _ = std::fs::remove_dir_all(directory);
                }
            }
        }
    }
}

fn resolution_for_prepared(
    prepared: &PreparedTransaction,
    storage_id: StorageId,
    decisions: &[CoordinatorDecision],
) -> Result<PreparedTxnResolution, DatabaseError> {
    let decision = decisions
        .iter()
        .find(|decision| decision.database_txn_id == prepared.database_txn_id);
    let resolution = if let Some(decision) = decision {
        if !decision.participants.iter().any(|participant| {
            participant.storage_id == storage_id
                && participant.physical_txn_id == prepared.physical_txn_id
        }) || prepared.state == PreparedTransactionState::RolledBack
        {
            return Err(DatabaseError::PreparedParticipantMismatch {
                database_txn_id: prepared.database_txn_id,
                storage_id,
                physical_txn_id: prepared.physical_txn_id,
            });
        }
        PreparedDecision::Commit
    } else {
        PreparedDecision::Abort
    };
    Ok(PreparedTxnResolution {
        database_txn_id: prepared.database_txn_id,
        physical_txn_id: prepared.physical_txn_id,
        decision: resolution,
    })
}

fn validate_generic_coordinator_recovery(
    decisions: &[CoordinatorDecision],
    storages: &[GenericInspectedStorage],
    retired_storage_ids: &[StorageId],
) -> Result<(), DatabaseError> {
    for (position, storage) in storages.iter().enumerate() {
        if storages[..position]
            .iter()
            .any(|previous| previous.recovery.storage_id == storage.recovery.storage_id)
        {
            return Err(StorageRegistryError::DuplicateStorageId {
                storage_id: storage.recovery.storage_id,
            }
            .into());
        }
    }
    for decision in decisions {
        for participant in &decision.participants {
            let Some(storage) = storages
                .iter()
                .find(|storage| storage.recovery.storage_id == participant.storage_id)
            else {
                if decision.complete && retired_storage_ids.contains(&participant.storage_id) {
                    continue;
                }
                return Err(DatabaseError::MissingCommitParticipant {
                    database_txn_id: decision.database_txn_id,
                    storage_id: participant.storage_id,
                    physical_txn_id: participant.physical_txn_id,
                });
            };
            if !decision.complete
                && !storage
                    .recovery
                    .prepared_transactions
                    .iter()
                    .any(|prepared| {
                        prepared.database_txn_id == decision.database_txn_id
                            && prepared.physical_txn_id == participant.physical_txn_id
                            && prepared.state != PreparedTransactionState::RolledBack
                    })
            {
                return Err(DatabaseError::MissingCommitParticipant {
                    database_txn_id: decision.database_txn_id,
                    storage_id: participant.storage_id,
                    physical_txn_id: participant.physical_txn_id,
                });
            }
        }
    }
    Ok(())
}

fn validate_coordinator_path(
    tables: &[(PathBuf, TableDef)],
    config: &DatabaseCoordinatorConfig,
) -> Result<(), DatabaseError> {
    if tables.iter().any(|(path, _)| path == config.log_path()) {
        return Err(DatabaseError::CoordinatorPathConflictsWithStorage(
            config.log_path().to_owned(),
        ));
    }
    Ok(())
}

fn validate_coordinator_recovery(
    decisions: &[CoordinatorDecision],
    storages: &[InspectedStorage],
    retired_storage_ids: &[StorageId],
) -> Result<(), DatabaseError> {
    for (position, storage) in storages.iter().enumerate() {
        if storages[..position]
            .iter()
            .any(|previous| previous.recovery.storage_id == storage.recovery.storage_id)
        {
            return Err(StorageRegistryError::DuplicateStorageId {
                storage_id: storage.recovery.storage_id,
            }
            .into());
        }
    }
    for decision in decisions {
        for participant in &decision.participants {
            let Some(storage) = storages
                .iter()
                .find(|storage| storage.recovery.storage_id == participant.storage_id)
            else {
                if decision.complete && retired_storage_ids.contains(&participant.storage_id) {
                    continue;
                }
                return Err(DatabaseError::MissingCommitParticipant {
                    database_txn_id: decision.database_txn_id,
                    storage_id: participant.storage_id,
                    physical_txn_id: participant.physical_txn_id,
                });
            };
            if !decision.complete
                && !storage
                    .recovery
                    .prepared_transactions
                    .iter()
                    .any(|prepared| {
                        prepared.database_txn_id == decision.database_txn_id
                            && prepared.physical_txn_id == participant.physical_txn_id
                            && prepared.state != PreparedTransactionState::RolledBack
                    })
            {
                return Err(DatabaseError::MissingCommitParticipant {
                    database_txn_id: decision.database_txn_id,
                    storage_id: participant.storage_id,
                    physical_txn_id: participant.physical_txn_id,
                });
            }
        }
    }
    Ok(())
}

fn cleanup_created_table_files(paths: &[PathBuf]) -> Option<(PathBuf, std::io::Error)> {
    let mut first_error = None;
    for database_path in paths.iter().rev() {
        let wal_path = netbadb_storage::wal_path(database_path);
        let targets = [
            database_path.clone(),
            wal_path.clone(),
            netbadb_storage::wal_alternate_path(&wal_path),
            netbadb_storage::txn_status_path(database_path),
        ];
        for target in targets {
            match std::fs::remove_file(&target) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) if first_error.is_none() => {
                    first_error = Some((target, error));
                }
                Err(_) => {}
            }
        }
    }
    first_error
}

fn columnar_read_failure(error: &DatabaseError) -> Option<(ColumnarProjectionId, String)> {
    let DatabaseError::Execution(ExecutionError::ColumnarRead {
        projection_id,
        source: StorageError::Columnar(columnar),
    }) = error
    else {
        return None;
    };
    if !columnar.invalidates_projection() {
        return None;
    }
    Some((*projection_id, columnar.to_string()))
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::{
        CoordinatorError, Database, DatabaseCoordinatorConfig, DatabaseError, DdlOutcome,
        ExecutionResult, IsolationLevel, ParticipantMode, PartitionCatalogConfig,
        PhysicalStatement, RangePartitionSpec, TablePlacementSpec, TableStorageCreateSpec,
        TableStorageOpenSpec, TransactionState, cleanup_created_table_files,
    };
    use crate::registry::{
        PhysicalBindings, StorageRegistry, StorageRegistryEntry, StorageRegistryError,
        TablePlacement,
    };
    use netbadb_inspect::{
        AggregateOutputInspection, BinaryOpInspection, ExpressionInspection,
        ExpressionKindInspection, IndexKindInspection, NullOrderInspection, PlanNodeInspection,
        SortDirectionInspection, StatementPlanInspection, StatementResultInspection,
        UnaryOpInspection,
    };
    use netbadb_planner::PhysicalPlan;
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_storage::{HeapStorage, TableStorage};
    use netbadb_types::{
        AccessPathId, ColumnId, DatabaseCommitSeq, DatabaseTxnId, PartitionId, PhysicalType,
        ScalarValue, StorageId, TableId,
    };

    fn table() -> TableDef {
        TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text)),
            ],
        )
    }

    fn indexed_table() -> TableDef {
        TableDef::new(
            TableId(9),
            "members",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "team_id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )
                .nullable(true),
                ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
                ColumnDef::new(
                    ColumnId(4),
                    "active",
                    TypeSpec::Physical(PhysicalType::Bool),
                ),
            ],
        )
    }

    fn teams_table() -> TableDef {
        TableDef::new(
            TableId(2),
            "teams",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    }

    fn projects_table() -> TableDef {
        TableDef::new(
            TableId(3),
            "projects",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    }

    fn coordinator_fixture_paths(
        root: &std::path::Path,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        (
            root.with_extension("users.db"),
            root.with_extension("teams.db"),
            root.with_extension("projects.db"),
            root.with_extension("coordinator"),
        )
    }

    fn coordinator_fixture_tables(
        root: &std::path::Path,
    ) -> (Vec<(std::path::PathBuf, TableDef)>, std::path::PathBuf) {
        let (users, teams, projects, coordinator) = coordinator_fixture_paths(root);
        (
            vec![
                (users, table()),
                (teams, teams_table()),
                (projects, projects_table()),
            ],
            coordinator,
        )
    }

    fn cleanup_coordinator_fixture(root: &std::path::Path) {
        let (users, teams, projects, coordinator) = coordinator_fixture_paths(root);
        let _ = cleanup_created_table_files(&[users, teams, projects]);
        let _ = std::fs::remove_file(coordinator);
    }

    #[test]
    fn coordinator_crash_child_entrypoint() {
        if std::env::var_os(crate::coordinator_crash::CHILD_ENV).is_none() {
            return;
        }
        let root = std::env::var_os(crate::coordinator_crash::ROOT_ENV)
            .map(std::path::PathBuf::from)
            .expect("coordinator crash root");
        let case = std::env::var(crate::coordinator_crash::CASE_ENV).expect("crash case");
        if case.starts_with("mixed:") {
            mixed_crash_child(&root);
            panic!("mixed crash child returned without reaching its crash point");
        }
        if case.starts_with("global-single-heap:") {
            global_single_crash_child(&root, TableId(1));
            panic!("global single-Heap crash child returned without reaching its crash point");
        }
        if case.starts_with("global-single-lsm:") {
            global_single_crash_child(&root, TableId(2));
            panic!("global single-LSM crash child returned without reaching its crash point");
        }
        if let Some(operation) = case.strip_prefix("partition-") {
            partition_crash_child(&root, operation);
            panic!("partition crash child returned without reaching its crash point");
        }
        let (tables, coordinator_path) = coordinator_fixture_tables(&root);
        let mut database = Database::open_tables_with_coordinator(
            tables,
            DatabaseCoordinatorConfig::new(coordinator_path),
        )
        .expect("open crash child database");
        let mut transaction = database
            .begin_transaction_for(TableId(1))
            .expect("begin crash child transaction");
        database
            .insert_into_in(
                TableId(1),
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("atomic".into())],
            )
            .expect("write users participant");
        database
            .insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(2)])
            .expect("write teams participant");
        database
            .insert_into_in(TableId(3), &mut transaction, &[ScalarValue::Int64(3)])
            .expect("write projects participant");
        transaction.commit().expect("commit until crash point");
        panic!("coordinator crash child returned without reaching its crash point");
    }

    fn assert_crash_outcome(root: &std::path::Path, committed: bool) {
        for pass in 0..2 {
            let (tables, coordinator_path) = coordinator_fixture_tables(root);
            let mut database = Database::open_tables_with_coordinator(
                tables,
                DatabaseCoordinatorConfig::new(coordinator_path),
            )
            .expect("recover coordinator database");
            for (table_name, expected_id) in [("users", 1), ("teams", 2), ("projects", 3)] {
                let rows = database
                    .query(&format!("SELECT id FROM {table_name}"))
                    .expect("query recovered participant")
                    .rows;
                let expected = if committed {
                    vec![vec![ScalarValue::Int64(expected_id)]]
                } else {
                    Vec::new()
                };
                assert_eq!(rows, expected, "pass {pass}, table {table_name}");
            }
            database.close().expect("close recovered database");
        }
    }

    fn mixed_crash_paths(
        root: &std::path::Path,
    ) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        (
            root.with_extension("mixed-heap"),
            root.with_extension("mixed-lsm"),
            root.with_extension("mixed-coordinator"),
        )
    }

    fn mixed_lsm_table() -> TableDef {
        TableDef::new(
            TableId(2),
            "lsm_items",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    }

    fn mixed_create_specs(root: &std::path::Path) -> Vec<TableStorageCreateSpec> {
        let (heap, lsm, _) = mixed_crash_paths(root);
        vec![
            TableStorageCreateSpec::heap(heap, table()),
            TableStorageCreateSpec::lsm(lsm, mixed_lsm_table(), ColumnId(1)),
        ]
    }

    fn mixed_open_specs(root: &std::path::Path) -> Vec<TableStorageOpenSpec> {
        let (heap, lsm, _) = mixed_crash_paths(root);
        vec![
            TableStorageOpenSpec::heap(heap, table()),
            TableStorageOpenSpec::lsm(lsm, mixed_lsm_table()),
        ]
    }

    fn cleanup_mixed_crash_fixture(root: &std::path::Path) {
        let (heap, lsm, coordinator) = mixed_crash_paths(root);
        let _ = cleanup_created_table_files(&[heap]);
        let _ = std::fs::remove_dir_all(lsm);
        let _ = std::fs::remove_file(coordinator);
    }

    fn mixed_crash_child(root: &std::path::Path) {
        let (_, _, coordinator) = mixed_crash_paths(root);
        let mut database = Database::open_storages_with_coordinator(
            mixed_open_specs(root),
            DatabaseCoordinatorConfig::new(coordinator),
        )
        .expect("open mixed crash child database");
        let mut transaction = database
            .begin_transaction_for(TableId(1))
            .expect("begin mixed crash transaction");
        database
            .insert_into_in(
                TableId(1),
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("heap".into())],
            )
            .expect("write Heap participant");
        database
            .insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(2)])
            .expect("write LSM participant");
        transaction
            .commit()
            .expect("commit until mixed crash point");
    }

    fn global_single_crash_child(root: &std::path::Path, table_id: TableId) {
        let (_, _, coordinator) = mixed_crash_paths(root);
        let mut database = Database::open_storages_with_coordinator(
            mixed_open_specs(root),
            DatabaseCoordinatorConfig::new(coordinator),
        )
        .expect("open global single-writer crash child database");
        let mut transaction = database
            .begin_transaction_for(table_id)
            .expect("begin global single-writer crash transaction");
        match table_id {
            TableId(1) => database
                .insert_into_in(
                    TableId(1),
                    &mut transaction,
                    &[ScalarValue::Int64(1), ScalarValue::Text("heap".into())],
                )
                .expect("write single Heap participant"),
            TableId(2) => database
                .insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(2)])
                .expect("write single LSM participant"),
            _ => panic!("unexpected single-writer table"),
        }
        transaction
            .commit()
            .expect("commit until global single-writer crash point");
    }

    fn assert_mixed_crash_outcome(root: &std::path::Path, committed: bool) {
        for pass in 0..2 {
            let (_, _, coordinator) = mixed_crash_paths(root);
            let mut database = Database::open_storages_with_coordinator(
                mixed_open_specs(root),
                DatabaseCoordinatorConfig::new(coordinator),
            )
            .expect("recover mixed database");
            let heap = database
                .query("SELECT id FROM users")
                .expect("Heap query")
                .rows;
            let lsm = database
                .query("SELECT id FROM lsm_items")
                .expect("LSM query")
                .rows;
            if committed {
                assert_eq!(heap, vec![vec![ScalarValue::Int64(1)]], "pass {pass}");
                assert_eq!(lsm, vec![vec![ScalarValue::Int64(2)]], "pass {pass}");
            } else {
                assert!(heap.is_empty(), "pass {pass}");
                assert!(lsm.is_empty(), "pass {pass}");
            }
            database.close().expect("close recovered mixed database");
        }
    }

    #[test]
    fn subprocess_heap_lsm_atomic_commit_crash_matrix_is_all_or_nothing() {
        let cases = [
            ("before-first-prepare", false),
            ("after-prepare-1", false),
            ("after-all-prepares", false),
            ("after-durable-decision", true),
            ("after-commit-1", true),
            ("after-all-commits", true),
            ("before-complete", true),
            ("after-durable-complete", true),
        ];
        for lsm_first in [false, true] {
            for (point, committed) in cases {
                let root = std::env::temp_dir().join(format!(
                    "netbadb-core-mixed-crash-{lsm_first}-{point}-{}",
                    std::process::id()
                ));
                cleanup_mixed_crash_fixture(&root);
                let (heap, lsm, coordinator) = mixed_crash_paths(&root);
                let specs = if lsm_first {
                    vec![
                        TableStorageCreateSpec::lsm(&lsm, mixed_lsm_table(), ColumnId(1)),
                        TableStorageCreateSpec::heap(&heap, table()),
                    ]
                } else {
                    mixed_create_specs(&root)
                };
                Database::create_storages_with_coordinator(
                    specs,
                    DatabaseCoordinatorConfig::new(coordinator),
                )
                .expect("create mixed crash fixture")
                .close()
                .expect("close mixed crash fixture");
                let actual_point = if point == "before-complete" {
                    "during-complete-append"
                } else {
                    point
                };
                let mut command = std::process::Command::new(
                    std::env::current_exe().expect("current core test executable"),
                );
                command
                    .arg("--exact")
                    .arg("tests::coordinator_crash_child_entrypoint")
                    .arg("--nocapture");
                crate::coordinator_crash::configure_child(
                    &mut command,
                    &format!("mixed:{point}"),
                    &root,
                    actual_point,
                );
                let status = command.status().expect("start mixed crash child");
                assert_eq!(
                    status.code(),
                    Some(crate::coordinator_crash::EXIT_CODE),
                    "LSM first {lsm_first}, point {point}"
                );
                assert_mixed_crash_outcome(&root, committed);
                cleanup_mixed_crash_fixture(&root);
            }
        }
    }

    #[test]
    fn change_stream_recovers_both_local_batches_after_durable_coordinator_decision() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-core-change-stream-decision-crash-{}",
            std::process::id()
        ));
        cleanup_mixed_crash_fixture(&root);
        let (_, _, coordinator) = mixed_crash_paths(&root);
        let mut database = Database::create_storages_with_coordinator(
            mixed_create_specs(&root),
            DatabaseCoordinatorConfig::new(&coordinator),
        )
        .expect("create change-stream crash fixture");
        let heap_cursor = database
            .enable_change_stream(TableId(1))
            .expect("enable Heap change stream");
        let lsm_cursor = database
            .enable_change_stream(TableId(2))
            .expect("enable LSM change stream");
        database.close().expect("close change-stream fixture");

        let mut command =
            std::process::Command::new(std::env::current_exe().expect("core test executable"));
        command
            .arg("--exact")
            .arg("tests::coordinator_crash_child_entrypoint")
            .arg("--nocapture");
        crate::coordinator_crash::configure_child(
            &mut command,
            "mixed:change-stream",
            &root,
            "after-durable-decision",
        );
        let status = command.status().expect("start change-stream crash child");
        assert_eq!(status.code(), Some(crate::coordinator_crash::EXIT_CODE));

        for pass in 0..2 {
            let recovered = Database::open_storages_with_coordinator(
                mixed_open_specs(&root),
                DatabaseCoordinatorConfig::new(&coordinator),
            )
            .expect("recover change-stream database");
            let heap = recovered
                .read_changes(TableId(1), heap_cursor, 10, 1_000_000)
                .expect("read recovered Heap changes");
            let lsm = recovered
                .read_changes(TableId(2), lsm_cursor, 10, 1_000_000)
                .expect("read recovered LSM changes");
            assert_eq!(heap.batches.len(), 1, "pass {pass}");
            assert_eq!(lsm.batches.len(), 1, "pass {pass}");
            assert_eq!(
                heap.batches[0].database_txn_id, lsm.batches[0].database_txn_id,
                "pass {pass}"
            );
            assert!(heap.batches[0].database_txn_id.is_some(), "pass {pass}");
            assert_ne!(
                heap.batches[0].storage_id, lsm.batches[0].storage_id,
                "pass {pass}"
            );
            recovered.close().expect("close recovered database");
        }
        cleanup_mixed_crash_fixture(&root);
    }

    #[test]
    fn heap_lsm_prepare_failure_in_either_order_rolls_back_without_a_decision() {
        for lsm_first in [false, true] {
            let root = std::env::temp_dir().join(format!(
                "netbadb-core-mixed-prepare-failure-{lsm_first}-{}",
                std::process::id()
            ));
            cleanup_mixed_crash_fixture(&root);
            let (heap, lsm, coordinator) = mixed_crash_paths(&root);
            let specs = if lsm_first {
                vec![
                    TableStorageCreateSpec::lsm(&lsm, mixed_lsm_table(), ColumnId(1)),
                    TableStorageCreateSpec::heap(&heap, table()),
                ]
            } else {
                vec![
                    TableStorageCreateSpec::heap(&heap, table()),
                    TableStorageCreateSpec::lsm(&lsm, mixed_lsm_table(), ColumnId(1)),
                ]
            };
            let mut database = Database::create_storages_with_coordinator(
                specs,
                DatabaseCoordinatorConfig::new(&coordinator),
            )
            .expect("create prepare-failure fixture");
            let mut transaction = database
                .begin_transaction_for(TableId(1))
                .expect("begin transaction");
            database
                .execute_in(
                    &mut transaction,
                    "INSERT INTO users (id, name) VALUES (1, 'heap')",
                )
                .expect("write Heap");
            database
                .execute_in(&mut transaction, "INSERT INTO lsm_items (id) VALUES (2)")
                .expect("write LSM");
            let failing_table = if lsm_first { TableId(1) } else { TableId(2) };
            let failing_storage = database
                .bindings
                .resolve_single(failing_table)
                .expect("failing storage identity");
            transaction
                .force_participant_rollback_for_prepare_failure(failing_storage)
                .expect("inject terminal participant state");
            assert!(matches!(
                transaction.commit(),
                Err(CoordinatorError::PrepareFailed { storage_id, .. })
                    if storage_id == failing_storage
            ));
            assert_eq!(transaction.state(), TransactionState::RolledBack);
            assert!(
                database
                    .query("SELECT id FROM users")
                    .expect("Heap query")
                    .rows
                    .is_empty()
            );
            assert!(
                database
                    .query("SELECT id FROM lsm_items")
                    .expect("LSM query")
                    .rows
                    .is_empty()
            );
            database.close().expect("close prepare-failure fixture");
            cleanup_mixed_crash_fixture(&root);
        }
    }

    #[test]
    fn subprocess_atomic_commit_crash_window_matrix_is_all_or_nothing() {
        let cases = [
            ("before-first-prepare", false),
            ("after-prepare-1", false),
            ("after-prepare-2", false),
            ("after-all-prepares", false),
            ("during-decision-append", false),
            ("after-decision-append", true),
            ("after-durable-decision", true),
            ("after-commit-1", true),
            ("after-commit-2", true),
            ("after-all-commits", true),
            ("during-complete-append", true),
            ("after-complete-append", true),
            ("after-durable-complete", true),
        ];
        for (case, committed) in cases {
            let root = std::env::temp_dir().join(format!(
                "netbadb-core-coordinator-crash-{case}-{}",
                std::process::id()
            ));
            cleanup_coordinator_fixture(&root);
            let (tables, coordinator_path) = coordinator_fixture_tables(&root);
            Database::create_tables_with_coordinator(
                tables,
                DatabaseCoordinatorConfig::new(coordinator_path),
            )
            .expect("create crash fixture")
            .close()
            .expect("close crash fixture");

            let mut command = std::process::Command::new(
                std::env::current_exe().expect("current core test executable"),
            );
            command
                .arg("--exact")
                .arg("tests::coordinator_crash_child_entrypoint")
                .arg("--nocapture");
            crate::coordinator_crash::configure_child(&mut command, case, &root, case);
            let status = command.status().expect("start coordinator crash child");
            assert_eq!(
                status.code(),
                Some(crate::coordinator_crash::EXIT_CODE),
                "case {case} did not terminate at its crash point: {status}"
            );
            assert_crash_outcome(&root, committed);
            cleanup_coordinator_fixture(&root);
        }
    }

    #[test]
    fn subprocess_global_snapshot_crash_windows_recover_before_serving_reads() {
        let cases = [
            ("before-first-prepare", false),
            ("after-prepare-1", false),
            ("after-prepare-2", false),
            ("after-all-prepares", false),
            ("during-decision-append", false),
            ("after-decision-append", true),
            ("after-durable-decision", true),
            ("after-commit-1", true),
            ("after-commit-2", true),
            ("after-all-commits", true),
            ("during-complete-append", true),
            ("after-complete-append", true),
            ("after-durable-complete-before-publication", true),
            ("after-durable-complete", true),
        ];
        for reverse in [false, true] {
            for (case, committed) in cases {
                let root = std::env::temp_dir().join(format!(
                    "netbadb-global-coordinator-crash-{reverse}-{case}-{}",
                    std::process::id()
                ));
                cleanup_coordinator_fixture(&root);
                let (tables, coordinator_path) = coordinator_fixture_tables(&root);
                Database::create_tables_with_coordinator(
                    tables,
                    DatabaseCoordinatorConfig::new(coordinator_path).with_global_visibility(),
                )
                .expect("create global crash fixture")
                .close()
                .expect("close global crash fixture");

                let mut command = std::process::Command::new(
                    std::env::current_exe().expect("current core test executable"),
                );
                command
                    .arg("--exact")
                    .arg("tests::coordinator_crash_child_entrypoint")
                    .arg("--nocapture");
                if reverse {
                    command.env("NETBADB_REVERSE_PARTICIPANT_COMMIT", "1");
                }
                crate::coordinator_crash::configure_child(&mut command, case, &root, case);
                let status = command.status().expect("start global crash child");
                assert_eq!(status.code(), Some(crate::coordinator_crash::EXIT_CODE));
                assert_crash_outcome(&root, committed);
                let (tables, coordinator_path) = coordinator_fixture_tables(&root);
                let recovered = Database::open_tables_with_coordinator(
                    tables,
                    DatabaseCoordinatorConfig::new(coordinator_path),
                )
                .expect("inspect recovered global state");
                assert_eq!(
                    recovered.visibility_mode(),
                    super::DatabaseVisibilityMode::Global
                );
                let inspection = recovered
                    .inspect_global_visibility()
                    .expect("inspect recovered visibility");
                assert_eq!(
                    inspection.published_commit_seq,
                    Some(DatabaseCommitSeq(u64::from(committed)))
                );
                assert_eq!(
                    inspection.next_commit_seq,
                    Some(DatabaseCommitSeq(u64::from(committed) + 1))
                );
                recovered.close().expect("close recovered global state");
                cleanup_coordinator_fixture(&root);
            }
        }
    }

    #[test]
    fn global_single_heap_and_lsm_crashes_publish_one_sequence_exactly_once() {
        for (engine, table_id) in [("heap", TableId(1)), ("lsm", TableId(2))] {
            for point in [
                "after-durable-decision",
                "after-commit-1",
                "after-durable-complete-before-publication",
            ] {
                let root = std::env::temp_dir().join(format!(
                    "netbadb-global-single-{engine}-{point}-{}",
                    std::process::id()
                ));
                cleanup_mixed_crash_fixture(&root);
                let (_, _, coordinator) = mixed_crash_paths(&root);
                Database::create_storages_with_coordinator(
                    mixed_create_specs(&root),
                    DatabaseCoordinatorConfig::new(coordinator).with_global_visibility(),
                )
                .expect("create global single-writer fixture")
                .close()
                .expect("close global single-writer fixture");

                let mut command = std::process::Command::new(
                    std::env::current_exe().expect("current core test executable"),
                );
                command
                    .arg("--exact")
                    .arg("tests::coordinator_crash_child_entrypoint")
                    .arg("--nocapture");
                crate::coordinator_crash::configure_child(
                    &mut command,
                    &format!("global-single-{engine}:{point}"),
                    &root,
                    point,
                );
                let status = command.status().expect("start global single-writer child");
                assert_eq!(status.code(), Some(crate::coordinator_crash::EXIT_CODE));

                let (_, _, coordinator) = mixed_crash_paths(&root);
                let mut recovered = Database::open_storages_with_coordinator(
                    mixed_open_specs(&root),
                    DatabaseCoordinatorConfig::new(coordinator),
                )
                .expect("recover global single-writer database");
                let heap = recovered.query("SELECT id FROM users").expect("Heap query");
                let lsm = recovered
                    .query("SELECT id FROM lsm_items")
                    .expect("LSM query");
                if table_id == TableId(1) {
                    assert_eq!(heap.rows, vec![vec![ScalarValue::Int64(1)]]);
                    assert!(lsm.rows.is_empty());
                } else {
                    assert!(heap.rows.is_empty());
                    assert_eq!(lsm.rows, vec![vec![ScalarValue::Int64(2)]]);
                }
                let inspection = recovered
                    .inspect_global_visibility()
                    .expect("inspect single-writer recovery");
                assert_eq!(inspection.published_commit_seq, Some(DatabaseCommitSeq(1)));
                assert_eq!(inspection.next_commit_seq, Some(DatabaseCommitSeq(2)));
                recovered
                    .close()
                    .expect("close recovered single-writer database");
                cleanup_mixed_crash_fixture(&root);
            }
        }
    }

    fn partition_crash_table() -> TableDef {
        TableDef::new(
            TableId(50),
            "items",
            vec![ColumnDef::new(
                ColumnId(1),
                "key",
                TypeSpec::Physical(PhysicalType::Int64),
            )],
        )
    }

    fn partition_crash_fixture(
        root: &std::path::Path,
    ) -> (Vec<std::path::PathBuf>, PartitionCatalogConfig) {
        (
            vec![
                root.with_extension("left.db"),
                root.with_extension("right.db"),
            ],
            PartitionCatalogConfig::new(
                root.with_extension("partitions"),
                root.with_extension("partition-coordinator"),
            ),
        )
    }

    fn partition_crash_specs(root: &std::path::Path) -> Vec<TablePlacementSpec> {
        let (paths, _) = partition_crash_fixture(root);
        vec![TablePlacementSpec::range_partitioned(
            partition_crash_table(),
            ColumnId(1),
            vec![
                RangePartitionSpec::new(
                    PartitionId(1),
                    &paths[0],
                    None,
                    Some(ScalarValue::Int64(0)),
                ),
                RangePartitionSpec::new(
                    PartitionId(2),
                    &paths[1],
                    Some(ScalarValue::Int64(0)),
                    None,
                ),
            ],
        )]
    }

    fn cleanup_partition_crash_fixture(root: &std::path::Path) {
        let (paths, config) = partition_crash_fixture(root);
        let _ = cleanup_created_table_files(&paths);
        let _ = std::fs::remove_file(config.catalog_path());
        let _ = std::fs::remove_file(config.coordinator_log_path());
    }

    fn partition_crash_child(root: &std::path::Path, operation: &str) {
        let (mut paths, config) = partition_crash_fixture(root);
        paths.reverse();
        let mut database =
            Database::open_with_placements(vec![partition_crash_table()], paths, config)
                .expect("open partition crash child");
        match operation {
            "update" => {
                let _ = database
                    .execute("UPDATE items SET key = 1 WHERE key = -1")
                    .expect("update until crash");
            }
            "delete" => {
                let _ = database
                    .execute("DELETE FROM items WHERE key >= -1")
                    .expect("delete until crash");
            }
            "insert" => {
                let mut transaction = database
                    .begin_transaction_for(TableId(50))
                    .expect("begin insert crash transaction");
                database
                    .execute_in(&mut transaction, "INSERT INTO items (key) VALUES (-1)")
                    .expect("insert left");
                database
                    .execute_in(&mut transaction, "INSERT INTO items (key) VALUES (1)")
                    .expect("insert right");
                transaction.commit().expect("commit inserts until crash");
            }
            other => panic!("unknown partition crash operation {other}"),
        }
    }

    fn seed_partition_crash_fixture(root: &std::path::Path, operation: &str) {
        cleanup_partition_crash_fixture(root);
        let (_, config) = partition_crash_fixture(root);
        let mut database = Database::create_with_placements(partition_crash_specs(root), config)
            .expect("create partition crash fixture");
        match operation {
            "update" => {
                database
                    .execute("INSERT INTO items (key) VALUES (-1)")
                    .unwrap();
            }
            "delete" => {
                database
                    .execute("INSERT INTO items (key) VALUES (-1)")
                    .unwrap();
                database
                    .execute("INSERT INTO items (key) VALUES (1)")
                    .unwrap();
            }
            "insert" => {}
            other => panic!("unknown seed operation {other}"),
        }
        database.close().expect("close partition crash seed");
    }

    fn assert_partition_crash_outcome(root: &std::path::Path, operation: &str, committed: bool) {
        for pass in 0..2 {
            let (mut paths, config) = partition_crash_fixture(root);
            if pass == 1 {
                paths.reverse();
            }
            let mut database =
                Database::open_with_placements(vec![partition_crash_table()], paths, config)
                    .expect("recover partition crash fixture");
            let rows = database
                .query("SELECT key FROM items")
                .expect("read recovered partitions")
                .rows;
            let expected = match (operation, committed) {
                ("update", false) => vec![vec![ScalarValue::Int64(-1)]],
                ("update", true) => vec![vec![ScalarValue::Int64(1)]],
                ("delete", false) => {
                    vec![vec![ScalarValue::Int64(-1)], vec![ScalarValue::Int64(1)]]
                }
                ("delete", true) | ("insert", false) => Vec::new(),
                ("insert", true) => vec![vec![ScalarValue::Int64(-1)], vec![ScalarValue::Int64(1)]],
                _ => panic!("invalid crash expectation"),
            };
            assert_eq!(rows, expected, "pass {pass}, operation {operation}");
            database.close().expect("close recovered partition fixture");
        }
    }

    #[test]
    fn partition_dml_subprocess_crash_matrix_is_all_or_nothing() {
        let windows = [
            ("after-all-prepares", false),
            ("after-durable-decision", true),
            ("after-commit-1", true),
        ];
        for operation in ["update", "delete", "insert"] {
            for (point, committed) in windows {
                let root = std::env::temp_dir().join(format!(
                    "netbadb-partition-crash-{operation}-{point}-{}",
                    std::process::id()
                ));
                seed_partition_crash_fixture(&root, operation);
                let mut command = std::process::Command::new(
                    std::env::current_exe().expect("current core test executable"),
                );
                command
                    .arg("--exact")
                    .arg("tests::coordinator_crash_child_entrypoint")
                    .arg("--nocapture");
                crate::coordinator_crash::configure_child(
                    &mut command,
                    &format!("partition-{operation}"),
                    &root,
                    point,
                );
                let status = command.status().expect("start partition crash child");
                assert_eq!(status.code(), Some(crate::coordinator_crash::EXIT_CODE));
                assert_partition_crash_outcome(&root, operation, committed);
                cleanup_partition_crash_fixture(&root);
            }
        }
    }

    #[test]
    fn coordinator_database_commits_two_write_storages_and_reopens_in_reversed_order() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-core-atomic-two-write-{}",
            std::process::id()
        ));
        let users_path = root.with_extension("users.db");
        let teams_path = root.with_extension("teams.db");
        let coordinator_path = root.with_extension("coordinator");
        let table_paths = vec![users_path.clone(), teams_path.clone()];
        let _ = cleanup_created_table_files(&table_paths);
        let _ = std::fs::remove_file(&coordinator_path);

        let config = DatabaseCoordinatorConfig::new(&coordinator_path);
        let mut database = Database::create_tables_with_coordinator(
            vec![
                (users_path.clone(), table()),
                (teams_path.clone(), teams_table()),
            ],
            config.clone(),
        )
        .expect("create coordinator database");
        let mut transaction = database
            .begin_transaction_for(TableId(1))
            .expect("begin database transaction");
        let committed_database_txn_id = transaction.id();
        database
            .insert_into_in(
                TableId(1),
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())],
            )
            .expect("write first participant");
        database
            .insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(7)])
            .expect("write second participant");
        transaction.commit().expect("atomic commit");
        assert_eq!(transaction.state(), TransactionState::Committed);
        drop(transaction);
        database.close().expect("close database");

        let mut reopened = Database::open_tables_with_coordinator(
            vec![
                (teams_path.clone(), teams_table()),
                (users_path.clone(), table()),
            ],
            config,
        )
        .expect("reopen in reversed catalog order");
        assert_eq!(
            reopened
                .query("SELECT id FROM users")
                .expect("read users")
                .rows,
            vec![vec![ScalarValue::Int64(1)]]
        );
        assert_eq!(
            reopened
                .query("SELECT id FROM teams")
                .expect("read teams")
                .rows,
            vec![vec![ScalarValue::Int64(7)]]
        );
        let mut later = reopened
            .begin_transaction_for(TableId(1))
            .expect("allocate transaction after restart");
        assert!(later.id().0 > committed_database_txn_id.0);
        later.rollback().expect("finish allocation check");
        reopened.close().expect("close reopened database");
        let _ = cleanup_created_table_files(&table_paths);
        let _ = std::fs::remove_file(coordinator_path);
    }

    #[test]
    fn coordinator_multi_write_rolls_back_before_the_global_decision() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-core-atomic-rollback-{}",
            std::process::id()
        ));
        cleanup_coordinator_fixture(&root);
        let (tables, coordinator_path) = coordinator_fixture_tables(&root);
        let mut database = Database::create_tables_with_coordinator(
            tables,
            DatabaseCoordinatorConfig::new(coordinator_path),
        )
        .expect("create rollback fixture");
        let mut transaction = database
            .begin_transaction_for(TableId(1))
            .expect("begin multi-write transaction");
        database
            .execute_in(
                &mut transaction,
                "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            )
            .expect("write first participant");
        database
            .execute_in(&mut transaction, "INSERT INTO teams (id) VALUES (2)")
            .expect("write second participant");
        assert!(
            database
                .execute_in(
                    &mut transaction,
                    "INSERT INTO projects (id) VALUES ('not an integer')",
                )
                .is_err()
        );
        transaction.rollback().expect("rollback before decision");
        assert_eq!(transaction.state(), TransactionState::RolledBack);
        drop(transaction);
        assert!(
            database
                .query("SELECT id FROM users")
                .expect("users")
                .rows
                .is_empty()
        );
        assert!(
            database
                .query("SELECT id FROM teams")
                .expect("teams")
                .rows
                .is_empty()
        );
        database.close().expect("close rollback fixture");
        cleanup_coordinator_fixture(&root);
    }

    #[test]
    fn drop_publication_retries_coordinator_decision_and_finalize_failures() {
        for failure in ["decision", "finalize"] {
            let root = std::env::temp_dir().join(format!(
                "netbadb-drop-retry-{failure}-{}",
                std::process::id()
            ));
            cleanup_coordinator_fixture(&root);
            let (tables, coordinator_path) = coordinator_fixture_tables(&root);
            let mut database = Database::create_tables_with_coordinator(
                tables,
                DatabaseCoordinatorConfig::new(coordinator_path),
            )
            .unwrap();
            let users = database.create_index(TableId(1), ColumnId(2)).unwrap();
            let teams = database.create_index(TableId(2), ColumnId(1)).unwrap();
            let mut tx = database.begin_transaction_for(TableId(1)).unwrap();
            database
                .drop_index_in(&mut tx, TableId(1), users.id)
                .unwrap();
            database
                .drop_index_in(&mut tx, TableId(2), teams.id)
                .unwrap();
            let generation = database.catalog_generation();
            let coordinator = database.coordinator.as_ref().unwrap();
            if failure == "decision" {
                coordinator.borrow_mut().inject_decision_sync_failure();
            } else {
                coordinator.borrow_mut().inject_complete_sync_failure();
            }
            assert!(database.commit_transaction(&mut tx).is_err());
            assert!(tx.rollback().is_err());
            assert_eq!(database.catalog_generation(), generation);
            assert_eq!(
                database.indexes(TableId(1)).unwrap(),
                std::slice::from_ref(&users)
            );
            assert_eq!(
                database.indexes(TableId(2)).unwrap(),
                std::slice::from_ref(&teams)
            );
            database.commit_transaction(&mut tx).unwrap();
            assert_eq!(database.catalog_generation(), generation + 1);
            assert!(database.indexes(TableId(1)).unwrap().is_empty());
            assert!(database.indexes(TableId(2)).unwrap().is_empty());
            drop(tx);
            database.close().unwrap();
            cleanup_coordinator_fixture(&root);
        }
    }

    #[test]
    fn uncertain_decision_and_complete_sync_failures_are_retryable_not_rollbackable() {
        for failure in ["decision-sync", "complete-sync"] {
            let root = std::env::temp_dir().join(format!(
                "netbadb-core-atomic-retry-{failure}-{}",
                std::process::id()
            ));
            cleanup_coordinator_fixture(&root);
            let (tables, coordinator_path) = coordinator_fixture_tables(&root);
            let mut database = Database::create_tables_with_coordinator(
                tables,
                DatabaseCoordinatorConfig::new(coordinator_path),
            )
            .expect("create retry fixture");
            let mut transaction = database
                .begin_transaction_for(TableId(1))
                .expect("begin retry transaction");
            let original_id = transaction.id();
            database
                .execute_in(
                    &mut transaction,
                    "INSERT INTO users (id, name) VALUES (1, 'Ada')",
                )
                .expect("write first participant");
            database
                .execute_in(&mut transaction, "INSERT INTO teams (id) VALUES (2)")
                .expect("write second participant");
            let coordinator = database.coordinator.as_ref().expect("coordinator");
            if failure == "decision-sync" {
                coordinator.borrow_mut().inject_decision_sync_failure();
            } else {
                coordinator.borrow_mut().inject_complete_sync_failure();
            }
            assert!(transaction.commit().is_err());
            let expected_state = if failure == "decision-sync" {
                TransactionState::DecisionPending
            } else {
                TransactionState::FinalizePending
            };
            assert_eq!(transaction.state(), expected_state);
            assert!(matches!(
                transaction.rollback(),
                Err(CoordinatorError::CommitAlreadyDecided { .. })
            ));
            assert_eq!(transaction.id(), original_id);
            transaction.commit().expect("retry the same transaction");
            assert_eq!(transaction.state(), TransactionState::Committed);
            drop(transaction);
            assert_eq!(
                database
                    .query("SELECT id FROM users")
                    .expect("users")
                    .rows
                    .len(),
                1
            );
            assert_eq!(
                database
                    .query("SELECT id FROM teams")
                    .expect("teams")
                    .rows
                    .len(),
                1
            );
            database.close().expect("close retry fixture");
            cleanup_coordinator_fixture(&root);
        }
    }

    #[test]
    fn coordinator_open_rejects_missing_participants_and_corruption_before_recovery() {
        for case in ["missing-participant", "corrupt-log"] {
            let root = std::env::temp_dir().join(format!(
                "netbadb-core-atomic-open-{case}-{}",
                std::process::id()
            ));
            cleanup_coordinator_fixture(&root);
            let (tables, coordinator_path) = coordinator_fixture_tables(&root);
            let mut database = Database::create_tables_with_coordinator(
                tables,
                DatabaseCoordinatorConfig::new(&coordinator_path),
            )
            .expect("create open-validation fixture");
            let mut transaction = database
                .begin_transaction_for(TableId(1))
                .expect("begin fixture transaction");
            database
                .execute_in(
                    &mut transaction,
                    "INSERT INTO users (id, name) VALUES (1, 'Ada')",
                )
                .expect("write users");
            database
                .execute_in(&mut transaction, "INSERT INTO teams (id) VALUES (2)")
                .expect("write teams");
            transaction.commit().expect("commit fixture transaction");
            drop(transaction);
            database.close().expect("close fixture database");

            let error = if case == "missing-participant" {
                let (users, _teams, projects, _) = coordinator_fixture_paths(&root);
                let subset = vec![(users, table()), (projects, projects_table())];
                let complete = Database::open_tables_with_coordinator(
                    subset.clone(),
                    DatabaseCoordinatorConfig::new(&coordinator_path),
                )
                .expect("subset expectation opens every committed participant");
                assert_eq!(complete.schema().tables().len(), 3);
                let committed = complete.committed.clone();
                complete.close().unwrap();
                // The lower recovery validator still rejects an actually
                // incomplete inventory; public open cannot construct one.
                Database::physical_open_tables_with_coordinator(
                    subset,
                    DatabaseCoordinatorConfig::new(&coordinator_path),
                    committed,
                )
                .err()
                .expect("missing recovery participant must fail")
            } else {
                let mut bytes = std::fs::read(&coordinator_path).expect("read coordinator");
                bytes[12] ^= 1;
                std::fs::write(&coordinator_path, bytes).expect("corrupt coordinator");
                let (tables, _) = coordinator_fixture_tables(&root);
                Database::open_tables_with_coordinator(
                    tables,
                    DatabaseCoordinatorConfig::new(&coordinator_path),
                )
                .err()
                .expect("corrupt coordinator must fail")
            };
            if case == "missing-participant" {
                assert!(matches!(
                    error,
                    DatabaseError::MissingCommitParticipant { .. }
                ));
            } else {
                assert!(matches!(error, DatabaseError::CoordinatorLog(_)));
            }
            cleanup_coordinator_fixture(&root);
        }
    }

    fn planned_index(plan: &PhysicalPlan) -> Option<(AccessPathId, &ScalarValue)> {
        match plan {
            PhysicalPlan::IndexScan {
                access_path, key, ..
            } => Some((*access_path, key)),
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::ScalarProject { input, .. }
            | PhysicalPlan::Aggregate { input, .. }
            | PhysicalPlan::Limit { input, .. } => planned_index(input),
            PhysicalPlan::IndexNestedLoopJoin { left, .. } => planned_index(left),
            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. } => {
                planned_index(left).or_else(|| planned_index(right))
            }
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::ColumnarScan { .. }
            | PhysicalPlan::RangeIndexScan { .. }
            | PhysicalPlan::PartitionedScan { .. }
            | PhysicalPlan::OneRow => None,
        }
    }

    fn planned_statement_index(
        statement: &PhysicalStatement,
    ) -> Option<(AccessPathId, &ScalarValue)> {
        match statement {
            PhysicalStatement::Query(plan)
            | PhysicalStatement::Update { input: plan, .. }
            | PhysicalStatement::Delete { input: plan, .. } => planned_index(plan),
            PhysicalStatement::Insert { .. } => None,
        }
    }

    fn heap_storage(storage: &mut TableStorage) -> &mut HeapStorage {
        match storage {
            TableStorage::Heap(storage) => storage,
            TableStorage::Lsm(_) => panic!("test fixture expected Heap storage"),
        }
    }

    #[test]
    fn database_composes_heap_through_the_table_storage_boundary() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-table-storage-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut database = Database::create(&path, table()).expect("create database");
        assert!(matches!(
            database.registry.iter().collect::<Vec<_>>().as_slice(),
            [entry] if matches!(entry.storage, TableStorage::Heap(_))
        ));
        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .expect("execute through table storage");
        assert_eq!(
            database
                .query("SELECT name FROM users WHERE id = 1")
                .expect("query through table storage")
                .rows,
            vec![vec![ScalarValue::Text("Ada".into())]]
        );
        database.close().expect("close database");
        cleanup_created_table_files(std::slice::from_ref(&path));
    }

    #[test]
    fn physical_registry_identity_is_deterministic_validated_and_not_vector_position() {
        let suffix = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let users_path = std::env::temp_dir().join(format!("netbadb-registry-users-{suffix}"));
        let teams_path = std::env::temp_dir().join(format!("netbadb-registry-teams-{suffix}"));
        let paths = [users_path.clone(), teams_path.clone()];
        cleanup_created_table_files(&paths);
        let users = TableStorage::create_heap_with_storage_id(&users_path, table(), StorageId(10))
            .expect("create users storage");
        let teams =
            TableStorage::create_heap_with_storage_id(&teams_path, teams_table(), StorageId(20))
                .expect("create teams storage");

        // Registry order is deliberately the reverse of logical binding order,
        // and neither physical identity equals a vector position.
        let registry = StorageRegistry::new(vec![
            StorageRegistryEntry {
                id: StorageId(20),
                storage: teams,
            },
            StorageRegistryEntry {
                id: StorageId(10),
                storage: users,
            },
        ])
        .expect("build reordered registry");
        let bindings = PhysicalBindings::new(
            vec![
                TablePlacement::Single {
                    table_id: TableId(1),
                    storage_id: StorageId(10),
                },
                TablePlacement::Single {
                    table_id: TableId(2),
                    storage_id: StorageId(20),
                },
            ],
            &registry,
        )
        .expect("build physical bindings");
        assert!(matches!(
            PhysicalBindings::new(
                vec![
                    TablePlacement::Single {
                        table_id: TableId(1),
                        storage_id: StorageId(10),
                    },
                    TablePlacement::Single {
                        table_id: TableId(1),
                        storage_id: StorageId(20),
                    },
                ],
                &registry,
            ),
            Err(StorageRegistryError::DuplicateTableBinding {
                table_id: TableId(1)
            })
        ));
        let empty_registry = StorageRegistry::new(Vec::new()).expect("empty registry");
        let missing =
            PhysicalBindings::new(Vec::new(), &empty_registry).expect("empty binding set");
        assert!(matches!(
            missing.resolve_single(TableId(1)),
            Err(StorageRegistryError::MissingPhysicalBinding {
                table_id: TableId(1)
            })
        ));
        assert!(matches!(
            PhysicalBindings::new(
                vec![TablePlacement::Single {
                    table_id: TableId(1),
                    storage_id: StorageId(99),
                }],
                &registry,
            ),
            Err(StorageRegistryError::UnknownStorageId {
                storage_id: StorageId(99)
            })
        ));

        let schema = Schema::new(vec![table(), teams_table()]).expect("build schema");
        let mut database = Database {
            committed: crate::schema_catalog::CommittedCatalogState::initial(
                schema,
                bindings.iter().cloned(),
            ),
            bindings,
            registry,
            projections: crate::columnar::ProjectionRegistry::new(),
            transaction_owner: Rc::new(()),
            next_transaction_id: DatabaseTxnId(1),
            coordinator: None,
            published_visibility: None,
            catalog_generation: 0,
            catalog_path: None,
            mutation_journal: None,
            schema_writer: Rc::new(std::cell::Cell::new(None)),
            maintenance_cursor: None,
        };
        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .expect("route users insert");
        database
            .execute("INSERT INTO teams (id) VALUES (1)")
            .expect("route teams insert");
        assert_eq!(
            database
                .query("SELECT u.name FROM users u JOIN teams t ON u.id = t.id")
                .expect("route join through bindings")
                .rows,
            vec![vec![ScalarValue::Text("Ada".into())]]
        );
        database
            .create_index(TableId(1), ColumnId(2))
            .expect("route index creation");
        database.analyze(TableId(1)).expect("route analyze");
        assert_eq!(
            affected(
                database
                    .execute("UPDATE users SET name = 'Grace' WHERE id = 1")
                    .expect("route update"),
            ),
            1
        );
        assert_eq!(
            affected(
                database
                    .execute("DELETE FROM teams WHERE id = 1")
                    .expect("route delete"),
            ),
            1
        );
        assert_eq!(
            database.query("SELECT name FROM users").unwrap().rows,
            vec![vec![ScalarValue::Text("Grace".into())]]
        );
        assert!(
            database
                .query("SELECT id FROM teams")
                .unwrap()
                .rows
                .is_empty()
        );
        database.close().expect("close reordered registry");
        cleanup_created_table_files(&paths);
    }

    #[test]
    fn registry_rejects_duplicate_physical_storage_identity() {
        let suffix = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let users_path =
            std::env::temp_dir().join(format!("netbadb-registry-duplicate-users-{suffix}"));
        let teams_path =
            std::env::temp_dir().join(format!("netbadb-registry-duplicate-teams-{suffix}"));
        let paths = [users_path.clone(), teams_path.clone()];
        cleanup_created_table_files(&paths);
        let users = TableStorage::create_heap_with_storage_id(&users_path, table(), StorageId(7))
            .expect("create users storage");
        let teams =
            TableStorage::create_heap_with_storage_id(&teams_path, teams_table(), StorageId(7))
                .expect("create teams storage");
        assert!(matches!(
            StorageRegistry::new(vec![
                StorageRegistryEntry {
                    id: StorageId(7),
                    storage: users,
                },
                StorageRegistryEntry {
                    id: StorageId(7),
                    storage: teams,
                },
            ]),
            Err(StorageRegistryError::DuplicateStorageId {
                storage_id: StorageId(7)
            })
        ));
        cleanup_created_table_files(&paths);
    }

    #[test]
    fn table_storage_boundary_finds_a_transaction_owned_by_a_later_table() {
        let suffix = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let users_path = std::env::temp_dir().join(format!("netbadb-core-owner-users-{suffix}"));
        let teams_path = std::env::temp_dir().join(format!("netbadb-core-owner-teams-{suffix}"));
        let paths = [users_path.clone(), teams_path.clone()];
        cleanup_created_table_files(&paths);
        let mut database =
            Database::create_tables(vec![(users_path, table()), (teams_path, teams_table())])
                .expect("create multi-table database");

        let mut transaction = database
            .begin_transaction_for(TableId(2))
            .expect("begin transaction for later table");
        assert_eq!(
            affected(
                database
                    .execute_in(&mut transaction, "INSERT INTO teams (id) VALUES (7)")
                    .expect("write through owned storage context"),
            ),
            1
        );
        let own_read = database
            .execute_in(&mut transaction, "SELECT id FROM teams")
            .expect("read through owned storage context");
        assert_eq!(
            match own_read {
                ExecutionResult::Query(result) => result.rows,
                ExecutionResult::AffectedRows(_) => panic!("expected query result"),
            },
            vec![vec![ScalarValue::Int64(7)]]
        );
        transaction
            .rollback()
            .expect("rollback later-table transaction");
        assert!(
            database
                .query("SELECT id FROM teams")
                .unwrap()
                .rows
                .is_empty()
        );

        database.close().expect("close multi-table database");
        cleanup_created_table_files(&paths);
    }

    #[test]
    fn database_transaction_reads_two_storages_and_joins_in_one_read_context() {
        let suffix = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let users_path = std::env::temp_dir().join(format!("netbadb-txn-read-users-{suffix}"));
        let teams_path = std::env::temp_dir().join(format!("netbadb-txn-read-teams-{suffix}"));
        let paths = [users_path.clone(), teams_path.clone()];
        cleanup_created_table_files(&paths);
        let mut database =
            Database::create_tables(vec![(users_path, table()), (teams_path, teams_table())])
                .expect("create multi-storage database");
        assert_eq!(
            database.bindings.iter().collect::<Vec<_>>(),
            vec![
                &TablePlacement::Single {
                    table_id: TableId(1),
                    storage_id: StorageId(1),
                },
                &TablePlacement::Single {
                    table_id: TableId(2),
                    storage_id: StorageId(2),
                },
            ]
        );
        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .expect("insert user");
        database
            .execute("INSERT INTO teams (id) VALUES (1)")
            .expect("insert team");

        let mut transaction = database
            .begin_transaction_for(TableId(1))
            .expect("begin database transaction");
        for source in ["SELECT name FROM users", "SELECT id FROM teams"] {
            assert!(matches!(
                database.execute_in(&mut transaction, source),
                Ok(ExecutionResult::Query(_))
            ));
        }
        let joined = database
            .execute_in(
                &mut transaction,
                "SELECT u.name FROM users u JOIN teams t ON u.id = t.id",
            )
            .expect("join through database read view");
        assert_eq!(
            match joined {
                ExecutionResult::Query(result) => result.rows,
                ExecutionResult::AffectedRows(_) => panic!("expected query"),
            },
            vec![vec![ScalarValue::Text("Ada".into())]]
        );
        assert_eq!(transaction.participant_count(), 2);
        assert_eq!(
            transaction.participant_mode(StorageId(1)),
            Some(ParticipantMode::Read)
        );
        assert_eq!(
            transaction.participant_mode(StorageId(2)),
            Some(ParticipantMode::Read)
        );
        assert_eq!(transaction.write_participant(), None);
        transaction.commit().expect("commit read-only transaction");
        assert_eq!(transaction.state(), TransactionState::Committed);

        database.close().expect("close database");
        cleanup_created_table_files(&paths);
    }

    #[test]
    fn one_writer_with_multiple_readers_and_read_to_write_upgrade_commits() {
        let suffix = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let users_path =
            std::env::temp_dir().join(format!("netbadb-txn-one-writer-users-{suffix}"));
        let teams_path =
            std::env::temp_dir().join(format!("netbadb-txn-one-writer-teams-{suffix}"));
        let paths = [users_path.clone(), teams_path.clone()];
        cleanup_created_table_files(&paths);
        let mut database =
            Database::create_tables(vec![(users_path, table()), (teams_path, teams_table())])
                .expect("create multi-storage database");
        database
            .execute("INSERT INTO teams (id) VALUES (1)")
            .expect("seed read participant");

        let mut transaction = database.begin_transaction_for(TableId(1)).unwrap();
        database
            .execute_in(&mut transaction, "SELECT id FROM users")
            .expect("read future writer");
        database
            .execute_in(&mut transaction, "SELECT id FROM teams")
            .expect("read second storage");
        database
            .execute_in(
                &mut transaction,
                "INSERT INTO users (id, name) VALUES (2, 'Grace')",
            )
            .expect("upgrade users participant to writer");
        database
            .execute_in(&mut transaction, "SELECT id FROM teams")
            .expect("read after write");
        assert_eq!(transaction.write_participant(), Some(StorageId(1)));
        assert_eq!(
            transaction.participant_mode(StorageId(1)),
            Some(ParticipantMode::Write)
        );
        assert_eq!(
            transaction.participant_mode(StorageId(2)),
            Some(ParticipantMode::Read)
        );
        transaction.commit().expect("commit unique writer");
        assert_eq!(
            database.query("SELECT name FROM users").unwrap().rows,
            vec![vec![ScalarValue::Text("Grace".into())]]
        );

        let mut write_later = database.begin_transaction_for(TableId(1)).unwrap();
        database
            .execute_in(&mut write_later, "SELECT id FROM users")
            .expect("read users");
        database
            .execute_in(&mut write_later, "SELECT id FROM teams")
            .expect("read teams before upgrade");
        database
            .execute_in(&mut write_later, "INSERT INTO teams (id) VALUES (3)")
            .expect("upgrade teams participant");
        assert_eq!(write_later.write_participant(), Some(StorageId(2)));
        write_later.commit().expect("commit later writer");

        database.close().expect("close database");
        cleanup_created_table_files(&paths);
    }

    #[test]
    fn reverse_second_writer_is_rejected_before_mutation_and_rollback_cleans_first() {
        let suffix = format!("{}-{:?}", std::process::id(), std::thread::current().id());
        let users_path = std::env::temp_dir().join(format!("netbadb-txn-reverse-users-{suffix}"));
        let teams_path = std::env::temp_dir().join(format!("netbadb-txn-reverse-teams-{suffix}"));
        let paths = [users_path.clone(), teams_path.clone()];
        cleanup_created_table_files(&paths);
        let mut database =
            Database::create_tables(vec![(users_path, table()), (teams_path, teams_table())])
                .expect("create multi-storage database");
        let mut transaction = database.begin_transaction_for(TableId(2)).unwrap();
        database
            .execute_in(&mut transaction, "SELECT id FROM users")
            .expect("register users reader");
        database
            .execute_in(&mut transaction, "SELECT id FROM teams")
            .expect("register teams reader");
        database
            .execute_in(&mut transaction, "INSERT INTO teams (id) VALUES (9)")
            .expect("write teams first");
        assert!(matches!(
            database.execute_in(
                &mut transaction,
                "INSERT INTO users (id, name) VALUES (9, 'must not appear')"
            ),
            Err(DatabaseError::Transaction(
                CoordinatorError::MultipleWriteParticipantsUnsupported {
                    existing: StorageId(2),
                    requested: StorageId(1)
                }
            ))
        ));
        assert_eq!(transaction.state(), TransactionState::Active);
        transaction.rollback().expect("coordinate rollback");
        assert!(
            database
                .query("SELECT id FROM teams")
                .unwrap()
                .rows
                .is_empty()
        );
        assert!(
            database
                .query("SELECT id FROM users")
                .unwrap()
                .rows
                .is_empty()
        );

        database.close().expect("close database");
        cleanup_created_table_files(&paths);
    }

    #[test]
    fn participant_commit_error_does_not_mark_database_transaction_committed() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-coordinator-commit-state-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        cleanup_created_table_files(std::slice::from_ref(&path));
        let mut database = Database::create(&path, table()).expect("create database");
        let mut transaction = database.begin_transaction().expect("begin transaction");
        let context = transaction
            .write_context(StorageId(1), &mut database.registry)
            .expect("register write participant");
        context
            .rollback()
            .expect("force non-committable participant state");
        assert!(matches!(
            transaction.commit(),
            Err(CoordinatorError::ParticipantStateViolation {
                storage_id: StorageId(1),
                ..
            })
        ));
        assert_eq!(transaction.state(), TransactionState::CommitPending);

        database.close().expect("close database");
        cleanup_created_table_files(std::slice::from_ref(&path));
    }

    fn affected(result: ExecutionResult) -> u64 {
        match result {
            ExecutionResult::AffectedRows(rows) => rows,
            ExecutionResult::Query(_) => panic!("expected affected rows"),
        }
    }

    #[test]
    fn mvcc_fast_paths_hide_dirty_insert_and_explicit_transaction_reads_own_write() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-mvcc-fast-paths-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'A')")
            .expect("insert A");
        let mut writer = database.begin_transaction().expect("begin writer");
        database
            .insert_in(
                &mut writer,
                &[ScalarValue::Int64(2), ScalarValue::Text("B".into())],
            )
            .expect("insert dirty B");

        assert_eq!(
            database.query("SELECT COUNT(*) FROM users").unwrap().rows,
            vec![vec![ScalarValue::UInt64(1)]]
        );
        assert_eq!(
            database
                .query("SELECT name FROM users WHERE name IS NOT NULL")
                .unwrap()
                .rows,
            vec![vec![ScalarValue::Text("A".into())]]
        );
        let own = database
            .execute_in(&mut writer, "SELECT name FROM users ORDER BY id")
            .expect("own read");
        assert_eq!(
            match own {
                ExecutionResult::Query(result) => result.rows,
                ExecutionResult::AffectedRows(_) => unreachable!(),
            },
            vec![
                vec![ScalarValue::Text("A".into())],
                vec![ScalarValue::Text("B".into())],
            ]
        );
        writer.rollback().expect("rollback dirty insert");
        database.close().expect("close database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(netbadb_storage::txn_status_path(&path));
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn mvcc_repeatable_read_and_index_scan_keep_the_old_view() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-mvcc-rr-index-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .create_index(TableId(1), ColumnId(2))
            .expect("create name index");
        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'A')")
            .expect("insert A");
        let mut repeatable = database
            .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
            .expect("begin RR");
        let before = database
            .execute_in(&mut repeatable, "SELECT name FROM users WHERE name = 'A'")
            .expect("RR before");
        assert!(matches!(before, ExecutionResult::Query(ref result) if result.rows.len() == 1));

        database
            .execute("UPDATE users SET name = 'B' WHERE id = 1")
            .expect("commit B");
        let old = database
            .execute_in(&mut repeatable, "SELECT name FROM users WHERE name = 'A'")
            .expect("RR old index candidate");
        assert_eq!(
            match old {
                ExecutionResult::Query(result) => result.rows,
                ExecutionResult::AffectedRows(_) => unreachable!(),
            },
            vec![vec![ScalarValue::Text("A".into())]]
        );
        assert_eq!(
            database
                .query("SELECT name FROM users WHERE name = 'B'")
                .unwrap()
                .rows,
            vec![vec![ScalarValue::Text("B".into())]]
        );
        repeatable.commit().expect("finish RR");
        database.close().expect("close database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(netbadb_storage::txn_status_path(&path));
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(path);
    }

    fn inspected_index(plan: &PlanNodeInspection) -> Option<(ColumnId, &ScalarValue)> {
        match plan {
            PlanNodeInspection::IndexScan {
                index_column, key, ..
            } => Some((index_column.column_id, key)),
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                inspected_index(left).or_else(|| inspected_index(right))
            }
            PlanNodeInspection::IndexNestedLoopJoin { left, .. } => inspected_index(left),
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => inspected_index(input),
            PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::ColumnarScan { .. }
            | PlanNodeInspection::RangeIndexScan { .. }
            | PlanNodeInspection::PartitionedScan { .. }
            | PlanNodeInspection::OneRow => None,
        }
    }

    fn inspected_range(
        plan: &PlanNodeInspection,
    ) -> Option<&netbadb_inspect::IndexRangeInspection> {
        match plan {
            PlanNodeInspection::RangeIndexScan { range, .. } => Some(range),
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                inspected_range(left).or_else(|| inspected_range(right))
            }
            PlanNodeInspection::IndexNestedLoopJoin { left, .. } => inspected_range(left),
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => inspected_range(input),
            PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::ColumnarScan { .. }
            | PlanNodeInspection::IndexScan { .. }
            | PlanNodeInspection::PartitionedScan { .. }
            | PlanNodeInspection::OneRow => None,
        }
    }

    fn inspected_root(statement: &StatementPlanInspection) -> &PlanNodeInspection {
        match statement {
            StatementPlanInspection::Query { root } => root,
            StatementPlanInspection::Update { input, .. }
            | StatementPlanInspection::Delete { input, .. } => input,
            StatementPlanInspection::Insert { .. } => panic!("insert has no input plan"),
        }
    }

    fn scan_bindings(plan: &PlanNodeInspection, bindings: &mut Vec<(TableId, u32)>) {
        match plan {
            PlanNodeInspection::SeqScan {
                table_id,
                binding_id,
                ..
            }
            | PlanNodeInspection::ColumnarScan {
                table_id,
                binding_id,
                ..
            }
            | PlanNodeInspection::IndexScan {
                table_id,
                binding_id,
                ..
            }
            | PlanNodeInspection::RangeIndexScan {
                table_id,
                binding_id,
                ..
            }
            | PlanNodeInspection::PartitionedScan {
                table_id,
                binding_id,
                ..
            } => bindings.push((*table_id, binding_id.0)),
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                scan_bindings(left, bindings);
                scan_bindings(right, bindings);
            }
            PlanNodeInspection::IndexNestedLoopJoin {
                left,
                right_table_id,
                right_binding_id,
                ..
            } => {
                scan_bindings(left, bindings);
                bindings.push((*right_table_id, right_binding_id.0));
            }
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => scan_bindings(input, bindings),
            PlanNodeInspection::OneRow => {}
        }
    }

    fn inspected_scan_columns(plan: &PlanNodeInspection) -> Option<Vec<ColumnId>> {
        match plan {
            PlanNodeInspection::SeqScan { columns, .. }
            | PlanNodeInspection::ColumnarScan { columns, .. }
            | PlanNodeInspection::IndexScan { columns, .. }
            | PlanNodeInspection::RangeIndexScan { columns, .. } => {
                Some(columns.iter().map(|column| column.column_id).collect())
            }
            PlanNodeInspection::PartitionedScan { columns, .. } => {
                Some(columns.iter().map(|column| column.column_id).collect())
            }
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                inspected_scan_columns(left).or_else(|| inspected_scan_columns(right))
            }
            PlanNodeInspection::IndexNestedLoopJoin { left, .. } => inspected_scan_columns(left),
            PlanNodeInspection::Filter { input, .. }
            | PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => inspected_scan_columns(input),
            PlanNodeInspection::OneRow => None,
        }
    }

    fn inspected_filter(plan: &PlanNodeInspection) -> Option<&ExpressionInspection> {
        match plan {
            PlanNodeInspection::Filter { predicate, .. } => Some(predicate),
            PlanNodeInspection::NestedLoopJoin { left, right, .. }
            | PlanNodeInspection::HashJoin { left, right, .. } => {
                inspected_filter(left).or_else(|| inspected_filter(right))
            }
            PlanNodeInspection::IndexNestedLoopJoin { left, .. } => inspected_filter(left),
            PlanNodeInspection::Sort { input, .. }
            | PlanNodeInspection::Project { input, .. }
            | PlanNodeInspection::ScalarProject { input, .. }
            | PlanNodeInspection::Aggregate { input, .. }
            | PlanNodeInspection::Limit { input, .. } => inspected_filter(input),
            PlanNodeInspection::SeqScan { .. }
            | PlanNodeInspection::ColumnarScan { .. }
            | PlanNodeInspection::IndexScan { .. }
            | PlanNodeInspection::RangeIndexScan { .. }
            | PlanNodeInspection::PartitionedScan { .. }
            | PlanNodeInspection::OneRow => None,
        }
    }

    #[test]
    fn statement_access_uses_typed_logical_table_identity_without_storage_mutation() {
        let users_path =
            std::env::temp_dir().join(format!("netbadb-core-access-users-{}", std::process::id()));
        let teams_path =
            std::env::temp_dir().join(format!("netbadb-core-access-teams-{}", std::process::id()));
        let users_wal = netbadb_storage::wal_path(&users_path);
        let teams_wal = netbadb_storage::wal_path(&teams_path);
        for path in [&users_path, &users_wal, &teams_path, &teams_wal] {
            let _ = std::fs::remove_file(path);
        }
        let mut database = Database::create_tables(vec![
            (users_path.clone(), table()),
            (teams_path.clone(), teams_table()),
        ])
        .unwrap();

        let users = database
            .statement_access("SELECT u.name FROM users u")
            .unwrap();
        assert_eq!(users.read_tables(), &[TableId(1)]);
        assert!(users.write_tables().is_empty());

        let joined = database
            .statement_access("SELECT u.name FROM users u JOIN teams t ON u.id = t.id")
            .unwrap();
        assert_eq!(joined.read_tables(), &[TableId(1), TableId(2)]);
        assert!(joined.write_tables().is_empty());

        let self_join = database
            .statement_access("SELECT e.name FROM users e JOIN users m ON e.id = m.id")
            .unwrap();
        assert_eq!(self_join.read_tables(), &[TableId(1)]);

        for source in [
            "INSERT INTO users (id, name) VALUES (1, 'Ada')",
            "UPDATE users SET name = 'Grace' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
        ] {
            let access = database.statement_access(source).unwrap();
            assert!(access.read_tables().is_empty());
            assert_eq!(access.write_tables(), &[TableId(1)]);
        }

        assert!(matches!(
            database.statement_access("SELECT FROM"),
            Err(DatabaseError::Compile(_))
        ));
        assert!(
            database
                .query("SELECT id FROM users")
                .unwrap()
                .rows
                .is_empty()
        );

        database.close().unwrap();
        for path in [&users_path, &users_wal, &teams_path, &teams_wal] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn sql_projection_preserves_text_values_order_duplicates_and_scan_columns() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-move-projection-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let wal = netbadb_storage::wal_path(&path);
        let alternate_wal = netbadb_storage::wal_alternate_path(&wal);
        for target in [&path, &wal, &alternate_wal] {
            let _ = std::fs::remove_file(target);
        }
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert Ada");
        database
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("Lin".into())])
            .expect("insert Lin");

        let predicate_only = database
            .query("SELECT id FROM users WHERE name IS NOT NULL")
            .expect("predicate-only Text projection");
        assert_eq!(
            predicate_only.rows,
            vec![vec![ScalarValue::Int64(1)], vec![ScalarValue::Int64(2)]]
        );
        let inspected = database
            .inspect_statement("SELECT id FROM users WHERE name IS NOT NULL")
            .expect("inspect predicate-only Text projection");
        assert_eq!(
            inspected_scan_columns(inspected_root(&inspected.plan)),
            Some(vec![ColumnId(1), ColumnId(2)])
        );

        let retained_filter = database
            .query("SELECT name FROM users WHERE name IS NOT NULL")
            .expect("retained Text filter");
        assert_eq!(
            retained_filter.rows,
            vec![
                vec![ScalarValue::Text("Ada".into())],
                vec![ScalarValue::Text("Lin".into())]
            ]
        );
        let inspected = database
            .inspect_statement("SELECT name FROM users WHERE name IS NOT NULL")
            .expect("inspect retained Text filter");
        assert_eq!(
            inspected_scan_columns(inspected_root(&inspected.plan)),
            Some(vec![ColumnId(2)])
        );

        let duplicate_filtered = database
            .query("SELECT name, name FROM users WHERE id IS NOT NULL")
            .expect("duplicate retained Text after predicate-only filter");
        assert_eq!(
            duplicate_filtered.rows,
            vec![
                vec![
                    ScalarValue::Text("Ada".into()),
                    ScalarValue::Text("Ada".into())
                ],
                vec![
                    ScalarValue::Text("Lin".into()),
                    ScalarValue::Text("Lin".into())
                ]
            ]
        );

        let payload = database.query("SELECT name FROM users").expect("payload");
        assert_eq!(
            payload.rows,
            vec![
                vec![ScalarValue::Text("Ada".into())],
                vec![ScalarValue::Text("Lin".into())]
            ]
        );
        let inspected = database
            .inspect_statement("SELECT name FROM users")
            .expect("inspect payload");
        assert_eq!(
            inspected_scan_columns(inspected_root(&inspected.plan)),
            Some(vec![ColumnId(2)])
        );

        let reordered = database
            .query("SELECT name, id FROM users")
            .expect("reordered");
        assert_eq!(
            reordered.rows,
            vec![
                vec![ScalarValue::Text("Ada".into()), ScalarValue::Int64(1)],
                vec![ScalarValue::Text("Lin".into()), ScalarValue::Int64(2)]
            ]
        );
        let inspected = database
            .inspect_statement("SELECT name, id FROM users")
            .expect("inspect reordered");
        assert_eq!(
            inspected_scan_columns(inspected_root(&inspected.plan)),
            Some(vec![ColumnId(1), ColumnId(2)])
        );

        let duplicate = database
            .query("SELECT name, name FROM users")
            .expect("duplicate");
        assert_eq!(
            duplicate.rows,
            vec![
                vec![
                    ScalarValue::Text("Ada".into()),
                    ScalarValue::Text("Ada".into())
                ],
                vec![
                    ScalarValue::Text("Lin".into()),
                    ScalarValue::Text("Lin".into())
                ]
            ]
        );
        let inspected = database
            .inspect_statement("SELECT name, name FROM users")
            .expect("inspect duplicate");
        assert_eq!(
            inspected_scan_columns(inspected_root(&inspected.plan)),
            Some(vec![ColumnId(2)])
        );

        database.close().expect("close database");
        for target in [&path, &wal, &alternate_wal] {
            let _ = std::fs::remove_file(target);
        }
    }

    #[test]
    fn embedded_database_runs_a_query_after_reopen() {
        let path = std::env::temp_dir().join(format!("netbadb-core-{}", std::process::id()));
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert");
        database
            .insert(&[ScalarValue::Int64(2), ScalarValue::Text("Lin".into())])
            .expect("insert");
        database.close().expect("close database");

        let mut reopened = Database::open(&path, table()).expect("open database");
        let result = reopened
            .query("SELECT name FROM users WHERE id >= 2 LIMIT 1")
            .expect("query");
        assert_eq!(result.rows, vec![vec![ScalarValue::Text("Lin".into())]]);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn embedded_database_supports_an_explicit_multi_insert_transaction() {
        let path = std::env::temp_dir().join(format!("netbadb-core-txn-{}", std::process::id()));
        let mut database = Database::create(&path, table()).expect("create database");
        let mut transaction = database.begin_transaction().expect("begin transaction");
        database
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())],
            )
            .expect("first insert");
        database
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(2), ScalarValue::Text("Lin".into())],
            )
            .expect("second insert");
        transaction.commit().expect("commit transaction");
        assert_eq!(transaction.state(), TransactionState::Committed);
        database.close().expect("close database");

        let mut reopened = Database::open(&path, table()).expect("open database");
        let result = reopened
            .query("SELECT name FROM users WHERE id >= 1")
            .expect("query");
        assert_eq!(result.rows.len(), 2);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn embedded_database_supports_explicit_runtime_rollback() {
        let path =
            std::env::temp_dir().join(format!("netbadb-core-rollback-{}", std::process::id()));
        let mut database = Database::create(&path, table()).expect("create database");
        let mut transaction = database.begin_transaction().expect("begin transaction");
        database
            .insert_in(
                &mut transaction,
                &[ScalarValue::Int64(1), ScalarValue::Text("temporary".into())],
            )
            .expect("insert temporary row");
        transaction.rollback().expect("rollback transaction");
        assert_eq!(transaction.state(), TransactionState::RolledBack);
        database.close().expect("close database");

        let mut reopened = Database::open(&path, table()).expect("reopen database");
        let result = reopened
            .query("SELECT name FROM users WHERE id >= 1")
            .expect("query after rollback");
        assert!(result.rows.is_empty());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_path(&path));
    }

    #[test]
    fn embedded_database_exposes_explicit_checkpoint() {
        let path =
            std::env::temp_dir().join(format!("netbadb-core-checkpoint-{}", std::process::id()));
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert");
        database.checkpoint().expect("checkpoint database");
        database.close().expect("close database");

        let mut reopened = Database::open(&path, table()).expect("reopen database");
        assert_eq!(
            reopened.query("SELECT id FROM users").expect("query").rows,
            vec![vec![ScalarValue::Int64(1)]]
        );
        reopened.close().expect("close reopened database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn embedded_database_creates_and_rediscovers_registered_index() {
        let path = std::env::temp_dir().join(format!("netbadb-core-index-{}", std::process::id()));
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert existing row");
        let definition = database
            .create_index(TableId(1), ColumnId(2))
            .expect("create registered index");
        assert_eq!(
            database.indexes(TableId(1)).expect("list indexes"),
            std::slice::from_ref(&definition)
        );
        database.close().expect("close database");

        let reopened = Database::open(&path, table()).expect("reopen database");
        assert_eq!(
            reopened.indexes(TableId(1)).expect("rediscover index"),
            std::slice::from_ref(&definition)
        );
        reopened.close().expect("close reopened database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn generic_create_index_ddl_commits_rolls_back_plans_and_reopens_with_its_name() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-named-index-ddl-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let wal = netbadb_storage::wal_path(&path);
        for target in [&path, &wal, &netbadb_storage::wal_alternate_path(&wal)] {
            let _ = std::fs::remove_file(target);
        }
        let mut database = Database::create(&path, table()).expect("create database");
        database
            .insert(&[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())])
            .expect("insert existing row");
        let prepared_select = database
            .prepare_statement(
                "SELECT id FROM users WHERE name = $1",
                &[Some(PhysicalType::Text)],
            )
            .expect("prepare SELECT before CREATE INDEX");
        let ddl = database
            .prepare_ddl_statement("CREATE INDEX users_name_idx ON public.users (name)")
            .expect("prepare generic DDL");

        let mut rolled_back = database.begin_transaction().expect("begin rollback DDL");
        assert_eq!(
            database.execute_ddl_in(&mut rolled_back, &ddl).unwrap(),
            DdlOutcome::Created
        );
        assert!(database.indexes(TableId(1)).unwrap().is_empty());
        rolled_back.rollback().expect("rollback DDL");
        assert!(database.indexes(TableId(1)).unwrap().is_empty());

        let mut committed = database.begin_transaction().expect("begin committed DDL");
        database
            .execute_ddl_in(&mut committed, &ddl)
            .expect("stage DDL");
        database
            .commit_transaction(&mut committed)
            .expect("commit DDL through database");
        drop(rolled_back);
        drop(committed);
        let inspection_before = database.inspect_catalog().unwrap();
        let generation_before = database.catalog_generation();
        let lazy_reader = database.begin_transaction().unwrap();
        assert!(database.compact_index_catalog(TableId(1)).is_err());
        assert!(database.inspect_index_reclaim(TableId(1)).is_err());
        assert!(database.inspect_reusable_pages(TableId(1)).is_err());
        assert!(database.reclaim_retired_index_tail(TableId(1)).is_err());
        assert!(database.adopt_historical_btree_orphans(TableId(1)).is_err());
        drop(lazy_reader);
        let mut retained_terminal = database.begin_transaction().unwrap();
        database.commit_transaction(&mut retained_terminal).unwrap();
        assert!(database.reclaim_retired_index_tail(TableId(1)).is_err());
        assert!(database.adopt_historical_btree_orphans(TableId(1)).is_err());
        drop(retained_terminal);
        assert_eq!(
            database
                .adopt_historical_btree_orphans(TableId(1))
                .unwrap()
                .adopted,
            0
        );
        assert_eq!(database.inspect_catalog().unwrap(), inspection_before);
        assert_eq!(database.catalog_generation(), generation_before);
        let retired = database.create_index(TableId(1), ColumnId(1)).unwrap();
        database.drop_index(TableId(1), retired.id).unwrap();
        let generation_after_drop = database.catalog_generation();
        assert!(generation_after_drop > generation_before);
        database.compact_index_catalog(TableId(1)).unwrap();
        let reclaim = database.inspect_index_reclaim(TableId(1)).unwrap();
        assert_eq!(reclaim.active_indexes, 1);
        assert_eq!(reclaim.pending_reclaim_indexes, 1);
        assert_eq!(reclaim.pages_reclaimed, 0);
        let reusable = database.inspect_reusable_pages(TableId(1)).unwrap();
        assert_eq!(reusable.candidates.len(), 2);
        assert!(
            reusable
                .candidates
                .iter()
                .all(|page| page.retired_index_id == retired.id)
        );
        let tail = database.reclaim_retired_index_tail(TableId(1)).unwrap();
        assert_eq!((tail.reclaimed_pages, tail.reclaimed_indexes), (2, 1));
        assert_eq!(
            database
                .inspect_index_reclaim(TableId(1))
                .unwrap()
                .pending_reclaim_indexes,
            0
        );
        assert!(
            database
                .inspect_reusable_pages(TableId(1))
                .unwrap()
                .candidates
                .is_empty()
        );
        assert_eq!(database.inspect_catalog().unwrap(), inspection_before);
        assert_eq!(database.catalog_generation(), generation_after_drop);
        let indexes = database.indexes(TableId(1)).unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(
            indexes[0].name.as_ref().map(|name| name.as_str()),
            Some("users_name_idx")
        );
        let PhysicalStatement::Query(plan) = database
            .plan_source("SELECT id FROM users WHERE name = 'Ada'")
            .unwrap()
        else {
            panic!("SELECT must plan as a query");
        };
        assert!(planned_index(&plan).is_some());
        let ExecutionResult::Query(prepared_result) = database
            .execute_prepared(&prepared_select, &[ScalarValue::Text("Ada".into())])
            .expect("old prepared SELECT replans after CREATE INDEX")
        else {
            panic!("prepared SELECT must return rows");
        };
        assert_eq!(prepared_result.rows.len(), 1);
        database
            .execute("INSERT INTO users (id, name) VALUES (2, 'Ada')")
            .expect("DML maintains SQL-created index");
        database
            .execute("UPDATE users SET name = 'Grace' WHERE id = 2")
            .expect("UPDATE maintains SQL-created index");
        assert_eq!(
            database
                .query("SELECT id FROM users WHERE name = 'Grace'")
                .unwrap()
                .rows
                .len(),
            1
        );
        database
            .execute("DELETE FROM users WHERE id = 2")
            .expect("DELETE maintains SQL-created index");
        assert!(
            database
                .query("SELECT id FROM users WHERE name = 'Grace'")
                .unwrap()
                .rows
                .is_empty()
        );
        database.checkpoint().expect("checkpoint SQL-created index");
        database.close().expect("close named-index database");

        let mut reopened = Database::open(&path, table()).expect("reopen named index");
        let inspection = reopened
            .inspect_catalog()
            .expect("inspect reopened catalog");
        assert_eq!(
            inspection.tables[0].indexes[0]
                .name
                .as_ref()
                .map(|name| name.as_str()),
            Some("users_name_idx")
        );
        assert_eq!(
            reopened
                .query("SELECT id FROM users WHERE name = 'Ada'")
                .unwrap()
                .rows
                .len(),
            1
        );
        reopened.close().expect("close reopened named index");
        for target in [&path, &wal, &netbadb_storage::wal_alternate_path(&wal)] {
            let _ = std::fs::remove_file(target);
        }
    }

    #[test]
    fn sql_dml_maintains_registered_index_without_executor_index_knowledge() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-index-dml-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let mut database = Database::create(&path, table()).expect("create database");
        let definition = database
            .create_index(TableId(1), ColumnId(2))
            .expect("create registered index");

        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .expect("SQL insert");
        let inserted = heap_storage(database.storage_mut(TableId(1)).unwrap())
            .btree()
            .lookup(definition.handle, &ScalarValue::Text("Ada".into()))
            .expect("lookup inserted index entry");
        assert_eq!(inserted.len(), 1);

        database
            .execute("UPDATE users SET name = 'Grace' WHERE id = 1")
            .expect("SQL update");
        assert_eq!(
            heap_storage(database.storage_mut(TableId(1)).unwrap())
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("Ada".into()))
                .unwrap()
                .len(),
            1,
            "MVCC retains the old index candidate until vacuum"
        );
        assert!(
            database
                .query("SELECT id FROM users WHERE name = 'Ada'")
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(
            heap_storage(database.storage_mut(TableId(1)).unwrap())
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("Grace".into()))
                .unwrap()
                .len(),
            1
        );

        database
            .execute("DELETE FROM users WHERE id = 1")
            .expect("SQL delete");
        assert_eq!(
            heap_storage(database.storage_mut(TableId(1)).unwrap())
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("Grace".into()))
                .unwrap()
                .len(),
            1
        );

        database
            .execute("INSERT INTO users (id, name) VALUES (2, 'A')")
            .expect("first multi-row insert");
        database
            .execute("INSERT INTO users (id, name) VALUES (3, 'B')")
            .expect("second multi-row insert");
        database
            .execute("UPDATE users SET name = 'shared' WHERE id >= 2")
            .expect("multi-row SQL update");
        assert_eq!(
            heap_storage(database.storage_mut(TableId(1)).unwrap())
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("shared".into()))
                .unwrap()
                .len(),
            2
        );
        database
            .execute("DELETE FROM users WHERE id >= 2")
            .expect("multi-row SQL delete");
        assert!(
            heap_storage(database.storage_mut(TableId(1)).unwrap())
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("shared".into()))
                .unwrap()
                .len()
                >= 2
        );
        database.close().expect("close database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn registered_point_scans_cover_planning_null_dml_reopen_and_read_your_writes() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-index-scan-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let schema = indexed_table();
        let mut database = Database::create(&path, schema.clone()).expect("create database");
        for row in [
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(10),
                ScalarValue::Text("Ada".into()),
                ScalarValue::Bool(true),
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Int64(10),
                ScalarValue::Text("Bea".into()),
                ScalarValue::Bool(false),
            ],
            vec![
                ScalarValue::Int64(3),
                ScalarValue::Int64(10),
                ScalarValue::Text("Cal".into()),
                ScalarValue::Bool(true),
            ],
            vec![
                ScalarValue::Int64(4),
                ScalarValue::Int64(20),
                ScalarValue::Text("Dee".into()),
                ScalarValue::Bool(true),
            ],
            vec![
                ScalarValue::Int64(5),
                ScalarValue::Null,
                ScalarValue::Text("Eve".into()),
                ScalarValue::Bool(true),
            ],
        ] {
            database.insert(&row).expect("insert indexed row");
        }
        let team = database
            .create_index(TableId(9), ColumnId(2))
            .expect("create team index");
        let name = database
            .create_index(TableId(9), ColumnId(3))
            .expect("create name index");

        let select = database
            .plan_source("SELECT name FROM members m WHERE m.team_id = 10 AND m.active = true")
            .expect("plan indexed select");
        assert_eq!(
            planned_statement_index(&select),
            Some((
                AccessPathId(team.handle.meta_page.page_id().0),
                &ScalarValue::Int64(10)
            ))
        );
        assert_eq!(
            database
                .query("SELECT name FROM members m WHERE m.team_id = 10 AND m.active = true")
                .expect("query residual predicate")
                .rows,
            vec![
                vec![ScalarValue::Text("Ada".into())],
                vec![ScalarValue::Text("Cal".into())],
            ]
        );

        let deterministic = database
            .plan_source("SELECT id FROM members WHERE name = 'Ada' AND team_id = 10")
            .expect("plan deterministic choice");
        assert_eq!(
            planned_statement_index(&deterministic),
            Some((
                AccessPathId(team.handle.meta_page.page_id().0),
                &ScalarValue::Int64(10)
            ))
        );
        assert_ne!(team.handle, name.handle);

        let is_null = database
            .plan_source("SELECT id FROM members WHERE team_id IS NULL")
            .expect("plan IS NULL");
        assert_eq!(
            planned_statement_index(&is_null),
            Some((
                AccessPathId(team.handle.meta_page.page_id().0),
                &ScalarValue::Null
            ))
        );
        assert_eq!(
            database
                .query("SELECT id FROM members WHERE team_id IS NULL")
                .expect("query NULL key")
                .rows,
            vec![vec![ScalarValue::Int64(5)]]
        );
        let equals_null = database
            .plan_source("SELECT id FROM members WHERE team_id = NULL")
            .expect("plan NULL equality");
        assert!(planned_statement_index(&equals_null).is_none());
        assert!(
            database
                .query("SELECT id FROM members WHERE team_id = NULL")
                .expect("query NULL equality")
                .rows
                .is_empty()
        );

        let mut transaction = database.begin_transaction().expect("begin transaction");
        assert_eq!(
            affected(
                database
                    .execute_in(
                        &mut transaction,
                        "UPDATE members SET team_id = 30 WHERE id = 4",
                    )
                    .expect("update indexed key in transaction")
            ),
            1
        );
        let visible = database
            .execute_in(
                &mut transaction,
                "SELECT id FROM members WHERE team_id = 30",
            )
            .expect("read own indexed write");
        assert!(matches!(
            visible,
            ExecutionResult::Query(result)
                if result.rows == vec![vec![ScalarValue::Int64(4)]]
        ));
        transaction.rollback().expect("rollback indexed write");
        assert!(
            database
                .query("SELECT id FROM members WHERE team_id = 30")
                .expect("query rolled back key")
                .rows
                .is_empty()
        );

        let update = database
            .plan_source("UPDATE members SET team_id = 20 WHERE team_id = 10")
            .expect("plan self-index update");
        assert_eq!(
            planned_statement_index(&update),
            Some((
                AccessPathId(team.handle.meta_page.page_id().0),
                &ScalarValue::Int64(10)
            ))
        );
        assert_eq!(
            affected(
                database
                    .execute("UPDATE members SET team_id = 20 WHERE team_id = 10")
                    .expect("execute self-index update")
            ),
            3
        );
        assert_eq!(
            heap_storage(database.storage_mut(TableId(9)).unwrap())
                .btree()
                .lookup(team.handle, &ScalarValue::Int64(10))
                .unwrap()
                .len(),
            3,
            "MVCC retains old-key candidates until vacuum"
        );
        assert_eq!(
            heap_storage(database.storage_mut(TableId(9)).unwrap())
                .btree()
                .lookup(team.handle, &ScalarValue::Int64(20))
                .unwrap()
                .len(),
            4
        );

        for id in [6, 7] {
            database
                .execute(&format!(
                    "INSERT INTO members (id, team_id, name, active) VALUES ({id}, 10, 'X{id}', true)"
                ))
                .expect("insert delete target");
        }
        let delete = database
            .plan_source("DELETE FROM members WHERE team_id = 10")
            .expect("plan self-index delete");
        assert_eq!(
            planned_statement_index(&delete),
            Some((
                AccessPathId(team.handle.meta_page.page_id().0),
                &ScalarValue::Int64(10)
            ))
        );
        assert_eq!(
            affected(
                database
                    .execute("DELETE FROM members WHERE team_id = 10")
                    .expect("execute self-index delete")
            ),
            2
        );
        assert!(
            heap_storage(database.storage_mut(TableId(9)).unwrap())
                .btree()
                .lookup(team.handle, &ScalarValue::Int64(10))
                .unwrap()
                .len()
                >= 2
        );
        assert!(
            database
                .query("SELECT id FROM members WHERE team_id = 10")
                .unwrap()
                .rows
                .is_empty()
        );

        database.close().expect("close indexed database");
        let mut reopened = Database::open(&path, schema).expect("reopen indexed database");
        let reopened_plan = reopened
            .plan_source("SELECT id FROM members WHERE name = 'Ada'")
            .expect("plan reopened text lookup");
        assert_eq!(
            planned_statement_index(&reopened_plan),
            Some((
                AccessPathId(name.handle.meta_page.page_id().0),
                &ScalarValue::Text("Ada".into())
            ))
        );
        assert_eq!(
            reopened
                .query("SELECT id FROM members WHERE name = 'Ada'")
                .expect("query reopened text index")
                .rows,
            vec![vec![ScalarValue::Int64(1)]]
        );
        reopened.close().expect("close reopened database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn analyze_changes_access_path_costs_while_stale_statistics_preserve_results() {
        let path =
            std::env::temp_dir().join(format!("netbadb-core-analyze-{}", std::process::id()));
        let schema = indexed_table();
        let mut database = Database::create(&path, schema.clone()).expect("create database");
        for id_value in 0..80_i64 {
            database
                .insert(&[
                    ScalarValue::Int64(id_value),
                    ScalarValue::Int64(id_value % 2),
                    ScalarValue::Text(format!("user-{id_value:03}-{}", "x".repeat(500))),
                    ScalarValue::Bool(true),
                ])
                .expect("insert cost row");
        }
        let team = database
            .create_index(TableId(9), ColumnId(2))
            .expect("create duplicate-heavy index");
        let id = database
            .create_index(TableId(9), ColumnId(1))
            .expect("create selective index");

        let source = "SELECT name FROM members WHERE team_id = 0 AND id = 42";
        assert_eq!(
            planned_statement_index(&database.plan_source(source).expect("fallback plan")),
            Some((
                AccessPathId(team.handle.meta_page.page_id().0),
                &ScalarValue::Int64(0)
            ))
        );
        database.analyze(TableId(9)).expect("analyze table");
        assert_eq!(
            planned_statement_index(&database.plan_source(source).expect("costed plan")),
            Some((
                AccessPathId(id.handle.meta_page.page_id().0),
                &ScalarValue::Int64(42)
            ))
        );

        assert_eq!(
            affected(
                database
                    .execute("UPDATE members SET id = 1 WHERE active = true")
                    .expect("change indexed distribution")
            ),
            80
        );
        let stale_source = "SELECT name FROM members WHERE team_id = 0 AND id = 1";
        assert_eq!(
            planned_statement_index(
                &database
                    .plan_source(stale_source)
                    .expect("stale statistics plan")
            ),
            Some((
                AccessPathId(id.handle.meta_page.page_id().0),
                &ScalarValue::Int64(1)
            ))
        );
        assert_eq!(
            database
                .query(stale_source)
                .expect("query through stale plan")
                .rows
                .len(),
            40
        );

        database.analyze(TableId(9)).expect("refresh statistics");
        assert!(
            planned_statement_index(&database.plan_source(stale_source).expect("refreshed plan"))
                .is_none()
        );
        assert_eq!(
            database
                .query(stale_source)
                .expect("query through refreshed plan")
                .rows
                .len(),
            40
        );
        database.checkpoint().expect("checkpoint statistics");
        database.close().expect("close database");

        let reopened = Database::open(&path, schema).expect("reopen database");
        assert!(
            planned_statement_index(
                &reopened
                    .plan_source(stale_source)
                    .expect("reopened costed plan")
            )
            .is_none()
        );
        reopened.close().expect("close reopened database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn bounded_range_scan_executes_select_and_materialized_dml_with_rollback() {
        let path =
            std::env::temp_dir().join(format!("netbadb-core-range-index-{}", std::process::id()));
        let schema = indexed_table();
        let mut database = Database::create(&path, schema.clone()).expect("create database");
        for id in 0..200_i64 {
            database
                .insert(&[
                    ScalarValue::Int64(id),
                    ScalarValue::Int64(id % 4),
                    ScalarValue::Text(format!("member-{id:03}-{}", "x".repeat(500))),
                    ScalarValue::Bool(id % 2 == 0),
                ])
                .expect("insert range row");
        }
        database
            .create_index(TableId(9), ColumnId(2))
            .expect("create team index");
        database
            .create_index(TableId(9), ColumnId(1))
            .expect("create id index");

        let range_sql = "SELECT id FROM members WHERE 100 <= id AND 110 > id AND active = true";
        assert!(
            inspected_range(inspected_root(
                &database
                    .inspect_statement(range_sql)
                    .expect("inspect no-statistics range")
                    .plan
            ))
            .is_none()
        );
        assert!(
            inspected_index(inspected_root(
                &database
                    .inspect_statement("SELECT id FROM members WHERE id = 105")
                    .expect("inspect fallback point")
                    .plan
            ))
            .is_some()
        );

        database.analyze(TableId(9)).expect("analyze range table");
        let inspected = database
            .inspect_statement(range_sql)
            .expect("inspect costed range");
        let range = inspected_range(inspected_root(&inspected.plan)).expect("range scan");
        assert_eq!(
            inspected_scan_columns(inspected_root(&inspected.plan)),
            Some(vec![ColumnId(1), ColumnId(4)])
        );
        assert!(matches!(
            range.lower,
            netbadb_inspect::RangeBoundInspection::Included(ScalarValue::Int64(100))
        ));
        assert!(matches!(
            range.upper,
            netbadb_inspect::RangeBoundInspection::Excluded(ScalarValue::Int64(110))
        ));
        assert_eq!(
            database.query(range_sql).expect("execute range query").rows,
            [100_i64, 102, 104, 106, 108].map(|id| vec![ScalarValue::Int64(id)])
        );

        let wide = database
            .inspect_statement("SELECT id FROM members WHERE id >= 50 AND id < 150")
            .expect("inspect wide range");
        assert!(inspected_range(inspected_root(&wide.plan)).is_none());

        let mut transaction = database.begin_transaction().expect("begin range update");
        assert_eq!(
            affected(
                database
                    .execute_in(
                        &mut transaction,
                        "UPDATE members SET team_id = 999 WHERE id >= 100 AND id < 110",
                    )
                    .expect("execute range update")
            ),
            10
        );
        transaction.rollback().expect("rollback range update");
        assert!(
            database
                .query("SELECT id FROM members WHERE team_id = 999")
                .expect("verify rollback")
                .rows
                .is_empty()
        );
        assert_eq!(
            affected(
                database
                    .execute("DELETE FROM members WHERE id >= 190 AND id < 200")
                    .expect("execute range delete")
            ),
            10
        );
        assert!(
            database
                .query("SELECT id FROM members WHERE id >= 190 AND id < 200")
                .expect("verify range delete")
                .rows
                .is_empty()
        );

        database.close().expect("close range database");
        let mut reopened = Database::open(&path, schema).expect("reopen range database");
        assert_eq!(
            reopened
                .query("SELECT id FROM members WHERE id >= 100 AND id < 110")
                .expect("query after reopen")
                .rows
                .len(),
            10
        );
        reopened.close().expect("close reopened range database");
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
    }

    #[test]
    fn catalog_inspection_preserves_schema_registry_and_stale_statistics_order() {
        let users_path = std::env::temp_dir().join(format!(
            "netbadb-core-inspect-catalog-users-{}",
            std::process::id()
        ));
        let teams_path = std::env::temp_dir().join(format!(
            "netbadb-core-inspect-catalog-teams-{}",
            std::process::id()
        ));
        let users_wal = netbadb_storage::wal_path(&users_path);
        let teams_wal = netbadb_storage::wal_path(&teams_path);
        for path in [&users_path, &users_wal, &teams_path, &teams_wal] {
            let _ = std::fs::remove_file(path);
        }
        let users = TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Semantic {
                        name: "UserId".into(),
                        physical: PhysicalType::Int64,
                    },
                )
                .primary_key(true),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
            ],
        );
        let teams = teams_table();
        let mut database = Database::create_tables(vec![
            (users_path.clone(), users.clone()),
            (teams_path.clone(), teams.clone()),
        ])
        .unwrap();
        database
            .insert_into(
                users.id,
                &[ScalarValue::Int64(1), ScalarValue::Text("Ada".into())],
            )
            .unwrap();
        database.create_index(users.id, ColumnId(2)).unwrap();
        database.create_index(users.id, ColumnId(1)).unwrap();
        database.analyze(users.id).unwrap();

        let catalog = database.inspect_catalog().unwrap();
        assert_eq!(
            catalog
                .tables
                .iter()
                .map(|table| table.table_id)
                .collect::<Vec<_>>(),
            vec![users.id, teams.id]
        );
        let inspected_users = &catalog.tables[0];
        assert_eq!(inspected_users.fingerprint, users.fingerprint().unwrap());
        assert_eq!(
            inspected_users
                .columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
        assert_eq!(
            inspected_users
                .indexes
                .iter()
                .map(|index| (
                    index.table_id,
                    index.registration_order,
                    index.column_id,
                    index.kind,
                    index.unique,
                ))
                .collect::<Vec<_>>(),
            vec![
                (users.id, 0, ColumnId(2), IndexKindInspection::BTree, false,),
                (users.id, 1, ColumnId(1), IndexKindInspection::BTree, false,),
            ]
        );
        assert_eq!(
            inspected_users.columns[0].data_type.name.as_deref(),
            Some("UserId")
        );
        assert!(inspected_users.columns[0].primary_key);
        assert!(inspected_users.columns[1].nullable);
        let analyzed = inspected_users.statistics.unwrap();
        assert_eq!(analyzed.row_count, 1);
        assert!(
            inspected_users
                .indexes
                .iter()
                .all(|index| index.statistics.is_some())
        );

        database
            .insert_into(users.id, &[ScalarValue::Int64(2), ScalarValue::Null])
            .unwrap();
        assert_eq!(
            database.inspect_catalog().unwrap().tables[0].statistics,
            Some(analyzed)
        );

        let index_snapshot = inspected_users.indexes.clone();
        let users_len = std::fs::metadata(&users_path).unwrap().len();
        let users_wal_len = std::fs::metadata(&users_wal).unwrap().len();
        assert_eq!(
            database.inspect_catalog().unwrap().tables[0].indexes,
            index_snapshot
        );
        assert_eq!(std::fs::metadata(&users_path).unwrap().len(), users_len);
        assert_eq!(std::fs::metadata(&users_wal).unwrap().len(), users_wal_len);

        database.close().unwrap();
        let reopened = Database::open_tables(vec![
            (users_path.clone(), users.clone()),
            (teams_path.clone(), teams.clone()),
        ])
        .unwrap();
        assert_eq!(
            reopened.inspect_catalog().unwrap().tables[0].indexes,
            index_snapshot
        );
        reopened.close().unwrap();
        for path in [&users_path, &users_wal, &teams_path, &teams_wal] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn catalog_inspection_does_not_report_lsm_clustering_as_secondary_index() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-core-index-metadata-lsm-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let database = Database::create_storages(vec![TableStorageCreateSpec::lsm(
            &root,
            mixed_lsm_table(),
            ColumnId(1),
        )])
        .unwrap();
        let catalog = database.inspect_catalog().unwrap();
        assert!(catalog.tables[0].indexes.is_empty());
        database.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn statement_inspection_observes_real_plans_and_never_executes_dml() {
        let path = std::env::temp_dir().join(format!(
            "netbadb-core-inspect-statement-{}",
            std::process::id()
        ));
        let wal = netbadb_storage::wal_path(&path);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&wal);
        let schema = indexed_table();
        let mut database = Database::create(&path, schema).unwrap();
        for id in 0..80_i64 {
            database
                .insert(&[
                    ScalarValue::Int64(id),
                    ScalarValue::Int64(id % 2),
                    ScalarValue::Text(format!("member-{id:03}-{}", "x".repeat(500))),
                    ScalarValue::Bool(id % 3 == 0),
                ])
                .unwrap();
        }
        database.create_index(TableId(9), ColumnId(2)).unwrap();
        database.create_index(TableId(9), ColumnId(1)).unwrap();

        let fallback = database
            .inspect_statement("SELECT name FROM members WHERE team_id = 0 AND id = 42")
            .unwrap();
        assert_eq!(
            inspected_index(inspected_root(&fallback.plan)),
            Some((ColumnId(2), &ScalarValue::Int64(0)))
        );
        assert_eq!(
            inspected_scan_columns(inspected_root(&fallback.plan)),
            Some(vec![ColumnId(1), ColumnId(2), ColumnId(3)])
        );

        database.analyze(TableId(9)).unwrap();
        let selective = database
            .inspect_statement("SELECT name FROM members WHERE team_id = 0 AND id = 42")
            .unwrap();
        assert_eq!(
            inspected_index(inspected_root(&selective.plan)),
            Some((ColumnId(1), &ScalarValue::Int64(42)))
        );
        assert_eq!(
            inspected_scan_columns(inspected_root(&selective.plan)),
            Some(vec![ColumnId(1), ColumnId(2), ColumnId(3)])
        );
        let duplicate_heavy = database
            .inspect_statement("SELECT name FROM members WHERE team_id = 0")
            .unwrap();
        assert!(inspected_index(inspected_root(&duplicate_heavy.plan)).is_none());
        assert_eq!(
            inspected_scan_columns(inspected_root(&duplicate_heavy.plan)),
            Some(vec![ColumnId(2), ColumnId(3)])
        );

        let sorted = database
            .inspect_statement("SELECT id FROM members ORDER BY team_id DESC NULLS LAST LIMIT 3")
            .unwrap();
        let PlanNodeInspection::Limit { limit: 3, input } = inspected_root(&sorted.plan) else {
            panic!("expected LIMIT above the query plan");
        };
        let PlanNodeInspection::Project { input, .. } = input.as_ref() else {
            panic!("expected projection below LIMIT");
        };
        let PlanNodeInspection::Sort { keys, .. } = input.as_ref() else {
            panic!("expected sort below projection");
        };
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].column.column_id, ColumnId(2));
        assert_eq!(keys[0].direction, SortDirectionInspection::Desc);
        assert_eq!(keys[0].null_order, NullOrderInspection::Last);
        assert_eq!(
            inspected_scan_columns(inspected_root(&sorted.plan)),
            Some(vec![ColumnId(1), ColumnId(2)])
        );

        let self_join = database
            .inspect_statement(
                "SELECT e.id, m.id FROM members e JOIN members m ON e.team_id = m.team_id",
            )
            .unwrap();
        let mut bindings = Vec::new();
        scan_bindings(inspected_root(&self_join.plan), &mut bindings);
        assert_eq!(bindings, vec![(TableId(9), 0), (TableId(9), 1)]);

        let aggregate = database
            .inspect_statement("SELECT team_id, COUNT(*) FROM members GROUP BY team_id")
            .unwrap();
        let StatementResultInspection::Query { columns } = &aggregate.result else {
            panic!("aggregate should return query fields");
        };
        assert!(columns[0].source.is_some());
        assert!(columns[1].source.is_none());
        let PlanNodeInspection::Aggregate { outputs, .. } = inspected_root(&aggregate.plan) else {
            panic!("expected aggregate root");
        };
        assert!(matches!(
            outputs.as_slice(),
            [
                AggregateOutputInspection::GroupKey(_),
                AggregateOutputInspection::Aggregate { .. }
            ]
        ));
        assert_eq!(
            inspected_scan_columns(inspected_root(&aggregate.plan)),
            Some(vec![ColumnId(2)])
        );

        let expression = database
            .inspect_statement(
                "SELECT id FROM members WHERE team_id IS NULL AND NOT(active = false)",
            )
            .unwrap();
        let predicate = inspected_filter(inspected_root(&expression.plan)).unwrap();
        let ExpressionKindInspection::Binary {
            operator: BinaryOpInspection::And,
            left,
            right,
        } = &predicate.kind
        else {
            panic!("expected AND predicate");
        };
        assert!(matches!(
            left.kind,
            ExpressionKindInspection::IsNull { negated: false, .. }
        ));
        assert!(matches!(
            right.kind,
            ExpressionKindInspection::Unary {
                operator: UnaryOpInspection::Not,
                ..
            }
        ));
        assert_eq!(
            inspected_scan_columns(inspected_root(&expression.plan)),
            Some(vec![ColumnId(1), ColumnId(2), ColumnId(4)])
        );

        let before = database
            .query("SELECT id FROM members ORDER BY id")
            .unwrap()
            .rows;
        let wal_length = std::fs::metadata(&wal).unwrap().len();
        let insert = database
            .inspect_statement(
                "INSERT INTO members (id, team_id, name, active) VALUES (100, 1, 'new', true)",
            )
            .unwrap();
        assert!(matches!(
            insert.plan,
            StatementPlanInspection::Insert { .. }
        ));
        let update = database
            .inspect_statement("UPDATE members SET name = 'Grace' WHERE id = 42")
            .unwrap();
        assert_eq!(
            inspected_index(inspected_root(&update.plan)),
            Some((ColumnId(1), &ScalarValue::Int64(42)))
        );
        assert_eq!(
            inspected_scan_columns(inspected_root(&update.plan)),
            Some(vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)])
        );
        let delete = database
            .inspect_statement("DELETE FROM members WHERE id = 42")
            .unwrap();
        assert_eq!(
            inspected_index(inspected_root(&delete.plan)),
            Some((ColumnId(1), &ScalarValue::Int64(42)))
        );
        assert_eq!(
            inspected_scan_columns(inspected_root(&delete.plan)),
            Some(vec![ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)])
        );
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), wal_length);
        assert_eq!(
            database
                .query("SELECT id FROM members ORDER BY id")
                .unwrap()
                .rows,
            before
        );
        assert!(matches!(
            database.inspect_statement("SELECT FROM"),
            Err(DatabaseError::Compile(_))
        ));

        let mut transaction = database.begin_transaction().unwrap();
        database
            .insert_in(
                &mut transaction,
                &[
                    ScalarValue::Int64(100),
                    ScalarValue::Int64(1),
                    ScalarValue::Text("writer still available".into()),
                    ScalarValue::Bool(true),
                ],
            )
            .unwrap();
        transaction.rollback().unwrap();

        assert_eq!(
            affected(database.execute("UPDATE members SET id = 1").unwrap()),
            80
        );
        let stale = database
            .inspect_statement("SELECT name FROM members WHERE team_id = 0 AND id = 1")
            .unwrap();
        assert_eq!(
            inspected_index(inspected_root(&stale.plan)),
            Some((ColumnId(1), &ScalarValue::Int64(1)))
        );
        database.analyze(TableId(9)).unwrap();
        let refreshed = database
            .inspect_statement("SELECT name FROM members WHERE team_id = 0 AND id = 1")
            .unwrap();
        assert!(inspected_index(inspected_root(&refreshed.plan)).is_none());

        database.close().unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(netbadb_storage::wal_alternate_path(&wal));
        let _ = std::fs::remove_file(&wal);
    }

    #[test]
    fn global_snapshot_sequences_single_and_three_storage_commits_and_pins_repeatable_read() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-phase3a-global-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        cleanup_coordinator_fixture(&root);
        let (tables, coordinator_path) = coordinator_fixture_tables(&root);
        let config = DatabaseCoordinatorConfig::new(&coordinator_path).with_global_visibility();
        let mut database = Database::create_tables_with_coordinator(tables.clone(), config.clone())
            .expect("create global database");
        assert_eq!(
            database.visibility_mode(),
            super::DatabaseVisibilityMode::Global
        );
        let baseline = database
            .current_database_snapshot()
            .expect("inspect G0")
            .expect("global snapshot");
        assert_eq!(baseline.commit_seq(), DatabaseCommitSeq(0));
        assert_eq!(baseline.boundaries().len(), 3);

        let mut read_only = database.begin_transaction().expect("begin read-only");
        database
            .execute_in(&mut read_only, "SELECT id FROM users")
            .expect("read-only statement");
        read_only.commit().expect("commit read-only");
        assert_eq!(
            database
                .current_database_snapshot()
                .expect("inspect after read")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(0)
        );

        database
            .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .expect("single-storage global commit");
        assert_eq!(
            database
                .current_database_snapshot()
                .expect("inspect G1")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(1)
        );

        let mut repeatable = database
            .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
            .expect("begin repeatable read");
        database
            .execute_in(&mut repeatable, "SELECT id FROM users")
            .expect("pin G1");

        let mut writer = database
            .begin_transaction()
            .expect("begin three-storage writer");
        database
            .insert_into_in(
                TableId(1),
                &mut writer,
                &[ScalarValue::Int64(2), ScalarValue::Text("Grace".into())],
            )
            .expect("write users");
        database
            .insert_into_in(TableId(2), &mut writer, &[ScalarValue::Int64(2)])
            .expect("write teams");
        database
            .insert_into_in(TableId(3), &mut writer, &[ScalarValue::Int64(2)])
            .expect("write projects");
        writer.commit().expect("publish G2");
        assert_eq!(
            database
                .current_database_snapshot()
                .expect("inspect G2")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(2)
        );

        let repeatable_teams = database
            .execute_in(&mut repeatable, "SELECT id FROM teams")
            .expect("late storage read at pinned G1");
        assert!(
            matches!(repeatable_teams, ExecutionResult::Query(result) if result.rows.is_empty())
        );
        let repeatable_users = database
            .execute_in(&mut repeatable, "SELECT id FROM users ORDER BY id")
            .expect("repeat old users");
        assert!(
            matches!(repeatable_users, ExecutionResult::Query(result) if result.rows == vec![vec![ScalarValue::Int64(1)]])
        );
        repeatable.commit().expect("finish repeatable reader");

        let mut own_writes = database
            .begin_transaction_with_isolation(IsolationLevel::RepeatableRead)
            .expect("begin own-write RR");
        database
            .execute_in(&mut own_writes, "SELECT id FROM projects")
            .expect("pin G2 for own-write RR");
        database
            .insert_into_in(
                TableId(1),
                &mut own_writes,
                &[ScalarValue::Int64(3), ScalarValue::Text("own".into())],
            )
            .expect("stage own Heap write");
        let mut outside = database.begin_transaction().expect("begin outside writer");
        database
            .insert_into_in(TableId(2), &mut outside, &[ScalarValue::Int64(3)])
            .expect("write newer external state");
        outside.commit().expect("publish external G3");
        let own_users = database
            .execute_in(&mut own_writes, "SELECT id FROM users ORDER BY id")
            .expect("read own Heap write");
        assert!(
            matches!(own_users, ExecutionResult::Query(result) if result.rows == vec![
                vec![ScalarValue::Int64(1)],
                vec![ScalarValue::Int64(2)],
                vec![ScalarValue::Int64(3)],
            ])
        );
        let old_teams = database
            .execute_in(&mut own_writes, "SELECT id FROM teams ORDER BY id")
            .expect("hide external G3");
        assert!(
            matches!(old_teams, ExecutionResult::Query(result) if result.rows == vec![vec![ScalarValue::Int64(2)]])
        );
        own_writes.rollback().expect("discard own write");

        let mut read_committed = database
            .begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
            .expect("begin RC");
        let before = database
            .execute_in(&mut read_committed, "SELECT id FROM projects ORDER BY id")
            .expect("RC before external commit");
        assert!(
            matches!(before, ExecutionResult::Query(result) if result.rows == vec![vec![ScalarValue::Int64(2)]])
        );
        database
            .execute("INSERT INTO projects (id) VALUES (4)")
            .expect("publish external G4");
        let after = database
            .execute_in(&mut read_committed, "SELECT id FROM projects ORDER BY id")
            .expect("RC after external commit");
        assert!(
            matches!(after, ExecutionResult::Query(result) if result.rows == vec![
                vec![ScalarValue::Int64(2)],
                vec![ScalarValue::Int64(4)],
            ])
        );
        read_committed.commit().expect("finish RC reader");

        let mut uncertain = database
            .begin_transaction_for(TableId(1))
            .expect("begin single-writer retry");
        database
            .insert_into_in(
                TableId(1),
                &mut uncertain,
                &[ScalarValue::Int64(5), ScalarValue::Text("retry".into())],
            )
            .expect("write single participant");
        database
            .coordinator
            .as_ref()
            .expect("coordinator")
            .borrow_mut()
            .inject_decision_sync_failure();
        assert!(uncertain.commit().is_err());
        assert_eq!(uncertain.state(), TransactionState::DecisionPending);
        assert_eq!(
            database
                .current_database_snapshot()
                .expect("snapshot after uncertain decision")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(4)
        );
        assert!(uncertain.rollback().is_err());
        uncertain.commit().expect("retry same G5 decision");

        database.close().expect("close global database");
        let reopened =
            Database::open_tables_with_coordinator(tables, config).expect("reopen global database");
        assert_eq!(
            reopened.visibility_mode(),
            super::DatabaseVisibilityMode::Global
        );
        assert_eq!(
            reopened
                .current_database_snapshot()
                .expect("inspect reopened G5")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(5)
        );
        reopened.close().expect("close reopened database");
        cleanup_coordinator_fixture(&root);
    }

    #[test]
    fn global_mode_commits_mixed_heap_lsm_and_legacy_upgrade_is_durable() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-phase3a-mixed-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        cleanup_mixed_crash_fixture(&root);
        let (_, _, coordinator_path) = mixed_crash_paths(&root);
        let config = DatabaseCoordinatorConfig::new(&coordinator_path);
        let mut database =
            Database::create_storages_with_coordinator(mixed_create_specs(&root), config.clone())
                .expect("create legacy mixed database");
        assert_eq!(
            database.visibility_mode(),
            super::DatabaseVisibilityMode::LegacyLocal
        );
        let mut blocker = database.begin_transaction().expect("begin upgrade blocker");
        assert!(matches!(
            database.enable_global_visibility(),
            Err(DatabaseError::Transaction(
                CoordinatorError::GlobalEnableRequiresQuiescence { outstanding: 1 }
            ))
        ));
        blocker.rollback().expect("roll back blocker");
        drop(blocker);
        database
            .enable_global_visibility()
            .expect("durably enable global mode");

        let mut transaction = database.begin_transaction().expect("begin mixed writer");
        database
            .insert_into_in(
                TableId(1),
                &mut transaction,
                &[ScalarValue::Int64(7), ScalarValue::Text("heap".into())],
            )
            .expect("write Heap");
        database
            .insert_into_in(TableId(2), &mut transaction, &[ScalarValue::Int64(7)])
            .expect("write LSM");
        transaction.commit().expect("publish mixed G1");
        let inspection = database
            .inspect_global_visibility()
            .expect("inspect mixed publication");
        assert_eq!(inspection.published_commit_seq, Some(DatabaseCommitSeq(1)));
        assert_eq!(inspection.next_commit_seq, Some(DatabaseCommitSeq(2)));
        assert_eq!(inspection.boundaries.len(), 2);
        database.close().expect("close mixed database");

        let mut reopened = Database::open_storages_with_coordinator(
            mixed_open_specs(&root),
            DatabaseCoordinatorConfig::new(&coordinator_path),
        )
        .expect("reopen durable global mode");
        assert_eq!(
            reopened.visibility_mode(),
            super::DatabaseVisibilityMode::Global
        );
        assert_eq!(
            reopened
                .query("SELECT id FROM users")
                .expect("read Heap")
                .rows,
            vec![vec![ScalarValue::Int64(7)]]
        );
        assert_eq!(
            reopened
                .query("SELECT id FROM lsm_items")
                .expect("read LSM")
                .rows,
            vec![vec![ScalarValue::Int64(7)]]
        );
        reopened.close().expect("close reopened mixed database");
        cleanup_mixed_crash_fixture(&root);
    }

    #[test]
    fn global_complete_sync_failure_keeps_cross_storage_readers_on_old_snapshot() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-phase3a-publication-gate-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        cleanup_mixed_crash_fixture(&root);
        let (_, _, coordinator) = mixed_crash_paths(&root);
        let mut database = Database::create_storages_with_coordinator(
            mixed_create_specs(&root),
            DatabaseCoordinatorConfig::new(coordinator).with_global_visibility(),
        )
        .expect("create publication-gate database");

        let mut seed = database
            .begin_transaction()
            .expect("begin seed transaction");
        database
            .insert_into_in(
                TableId(1),
                &mut seed,
                &[ScalarValue::Int64(1), ScalarValue::Text("old".into())],
            )
            .expect("seed Heap");
        database
            .insert_into_in(TableId(2), &mut seed, &[ScalarValue::Int64(1)])
            .expect("seed LSM");
        seed.commit().expect("publish seed G1");

        let mut writer = database.begin_transaction().expect("begin G2 writer");
        database
            .execute_in(&mut writer, "UPDATE users SET id = 2 WHERE id = 1")
            .expect("update Heap");
        database
            .execute_in(&mut writer, "UPDATE lsm_items SET id = 2 WHERE id = 1")
            .expect("update LSM");
        database
            .coordinator
            .as_ref()
            .expect("coordinator")
            .borrow_mut()
            .inject_complete_sync_failure();
        assert!(writer.commit().is_err());
        assert_eq!(writer.state(), TransactionState::FinalizePending);
        assert_eq!(
            database
                .current_database_snapshot()
                .expect("snapshot while Complete is uncertain")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(1)
        );

        let old_join = database
            .query(
                "SELECT users.id, lsm_items.id FROM users JOIN lsm_items ON users.id = lsm_items.id",
            )
            .expect("join while G2 is not published");
        assert_eq!(
            old_join.rows,
            vec![vec![ScalarValue::Int64(1), ScalarValue::Int64(1)]]
        );

        writer.commit().expect("retry Complete and publish G2");
        assert_eq!(
            database
                .current_database_snapshot()
                .expect("snapshot after Complete retry")
                .expect("global snapshot")
                .commit_seq(),
            DatabaseCommitSeq(2)
        );
        let new_join = database
            .query(
                "SELECT users.id, lsm_items.id FROM users JOIN lsm_items ON users.id = lsm_items.id",
            )
            .expect("join after G2 publication");
        assert_eq!(
            new_join.rows,
            vec![vec![ScalarValue::Int64(2), ScalarValue::Int64(2)]]
        );

        database.close().expect("close publication-gate database");
        cleanup_mixed_crash_fixture(&root);
    }

    #[test]
    fn global_schema_publication_rebaselines_the_authoritative_storage_vector() {
        let root = std::env::temp_dir().join(format!(
            "netbadb-phase3a-schema-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create schema root");
        let catalog = root.join("catalog");
        let heap = root.join("users");
        let coordinator = root.join("coordinator");
        let mut database = Database::create_catalog(
            &catalog,
            vec![TableStorageCreateSpec::heap(&heap, table())],
            Some(DatabaseCoordinatorConfig::new(&coordinator).with_global_visibility()),
        )
        .expect("create managed global database");
        assert_eq!(
            database
                .current_database_snapshot()
                .unwrap()
                .unwrap()
                .boundaries()
                .len(),
            1
        );
        database
            .execute("CREATE TABLE extras (id BIGINT NOT NULL)")
            .expect("publish create table");
        let created = database.current_database_snapshot().unwrap().unwrap();
        assert_eq!(created.commit_seq(), DatabaseCommitSeq(1));
        assert_eq!(created.boundaries().len(), 2);
        database
            .execute("INSERT INTO extras (id) VALUES (8)")
            .expect("publish new-table row");
        database
            .execute("DROP TABLE extras")
            .expect("publish drop table");
        let dropped = database.current_database_snapshot().unwrap().unwrap();
        assert_eq!(dropped.commit_seq(), DatabaseCommitSeq(3));
        assert_eq!(dropped.boundaries().len(), 1);
        database.close().expect("close managed global database");

        let reopened = Database::open_catalog(&catalog).expect("reopen managed global database");
        assert_eq!(
            reopened.visibility_mode(),
            super::DatabaseVisibilityMode::Global
        );
        assert_eq!(
            reopened
                .current_database_snapshot()
                .unwrap()
                .unwrap()
                .commit_seq(),
            DatabaseCommitSeq(3)
        );
        reopened.close().expect("close reopened managed database");
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(test)]
mod adopted_source_refinement_expansion_audit_tests;
#[cfg(test)]
mod change_stream_schema_replacement_audit_tests;
#[cfg(test)]
mod deferred_backfill_tests;
#[cfg(test)]
mod deferred_terminal_structural_tests;
#[cfg(test)]
mod deferred_virtual_row_tests;
#[cfg(test)]
mod indexed_nullability_audit_tests;
#[cfg(test)]
mod late_clone_layout_audit_tests;
#[cfg(test)]
mod migration_backfill_audit_tests;
#[cfg(test)]
mod migration_backfill_index_tests;
#[cfg(test)]
mod migration_clone_audit_tests;
#[cfg(test)]
mod post_dml_source_adoption_audit_tests;
#[cfg(test)]
mod source_backfill_layout_projection_tests;
#[cfg(test)]
mod source_backfill_tests;
#[cfg(test)]
mod sql_alter_table_tests;
#[cfg(test)]
mod sql_create_table_tests;
#[cfg(test)]
mod sql_drop_table_tests;
#[cfg(test)]
mod table_ddl_composition_tests;
