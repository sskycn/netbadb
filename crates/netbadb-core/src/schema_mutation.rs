//! Frontend-independent transactional Heap creation. No SQL parsing occurs here.
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_schema::{ColumnDef, DropTableTarget, Schema, SchemaFingerprint, TableDef, TypeSpec};
use netbadb_storage::TableStorage;
use netbadb_types::{
    ColumnId, DatabaseTxnId, ScalarValue, SemanticType, StorageId, TableId, TableSchemaVersion,
};
use sha2::{Digest, Sha256};

use crate::coordinator_log::{CoordinatorLog, SchemaParticipantReference};
use crate::partition_catalog::{CatalogTable, PartitionCatalog};
use crate::registry::TablePlacement;
use crate::schema_catalog::{
    CatalogStorage, CatalogStorageKind, CommittedCatalogState, SchemaCatalogError,
    SchemaCatalogSnapshot, TableLineage,
};
use crate::schema_catalog_file as file;
use crate::schema_mutation_journal::{
    CreateIntent, DropIntent, Reservation, RetiredHeapGcRecord, RewriteIntent, RewriteReservation,
    SchemaMutationJournal, final_locator, namespace, prepared_locator, stage_locator,
};
use crate::{Database, DatabaseError, Transaction, TransactionState};

/// A generic column request. IDs are allocated by Core, never supplied by a frontend.
#[derive(Debug, Clone)]
pub struct CreateColumnSpec {
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
}
impl CreateColumnSpec {
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: SemanticType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// Runtime Heap creation supports columns only. Constraints, indexes, defaults,
/// generated values and non-Heap placements are deliberately unrepresentable.
///
/// ```compile_fail
/// use netbadb_core::CreateTableSpec;
/// let unsupported = CreateTableSpec { name: "projects".into(), columns: vec![], primary_key: true };
/// ```
/// ```compile_fail
/// use netbadb_core::CreateTableSpec;
/// let unsupported = CreateTableSpec { name: "projects".into(), columns: vec![], unique: true };
/// ```
/// ```compile_fail
/// use netbadb_core::CreateTableSpec;
/// let unsupported = CreateTableSpec { name: "projects".into(), columns: vec![], placement: "LSM" };
/// ```
#[derive(Debug, Clone)]
pub struct CreateTableSpec {
    pub name: String,
    pub columns: Vec<CreateColumnSpec>,
}
impl CreateTableSpec {
    #[must_use]
    pub fn new(name: impl Into<String>, columns: Vec<CreateColumnSpec>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }
}

// Pure mapping only. Validation, identity reservation and physical lifecycle
// remain in create_heap_table_in, shared with direct embedded callers.
impl From<&netbadb_compiler::TypedCreateTable> for CreateTableSpec {
    fn from(statement: &netbadb_compiler::TypedCreateTable) -> Self {
        Self::new(
            statement.name.clone(),
            statement
                .columns
                .iter()
                .map(|c| CreateColumnSpec::new(c.name.clone(), c.data_type.clone(), c.nullable))
                .collect(),
        )
    }
}

/// A prepared statement depends on stable table identity/version and canonical
/// schema meaning, not on the database-wide generation alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDependency {
    pub table_id: TableId,
    pub table_version: TableSchemaVersion,
    pub fingerprint: SchemaFingerprint,
}

/// Exact frontend-neutral request for one Heap schema rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableSpec {
    pub target: SchemaDependency,
    pub operation: AlterTableOperation,
}

impl AlterTableSpec {
    #[must_use]
    pub fn new(target: SchemaDependency, operation: AlterTableOperation) -> Self {
        Self { target, operation }
    }
}

/// First-version logical operations. Defaults, physical conversions and
/// declaration-position controls are deliberately unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableOperation {
    RenameTable {
        new_name: String,
    },
    RenameColumn {
        column_id: ColumnId,
        new_name: String,
    },
    AddNullableColumn {
        name: String,
        data_type: SemanticType,
    },
    DropColumn {
        column_id: ColumnId,
    },
    SetNotNull {
        column_id: ColumnId,
    },
    DropNotNull {
        column_id: ColumnId,
    },
    ChangeNominalType {
        column_id: ColumnId,
        target_type: SemanticType,
    },
}

impl From<SchemaDependency> for DropTableTarget {
    fn from(dependency: SchemaDependency) -> Self {
        Self {
            table_id: dependency.table_id,
            table_version: dependency.table_version,
            fingerprint: dependency.fingerprint,
        }
    }
}

/// Read-only physical-lifecycle projection. Retired resources are deliberately
/// absent from the active logical catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredTableResource {
    pub table_id: TableId,
    pub table_version: TableSchemaVersion,
    pub fingerprint: SchemaFingerprint,
    pub storage_id: StorageId,
    pub engine: netbadb_storage::StorageKind,
    pub relative_locator: String,
    pub retired_generation: crate::SchemaGeneration,
}

/// Durable physical history produced by a schema rewrite. The logical TableId
/// remains active on `new_storage_id`; this token is not a dropped table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacementRetiredHeap {
    pub table_id: TableId,
    pub base_table_version: TableSchemaVersion,
    pub target_table_version: TableSchemaVersion,
    pub base_fingerprint: SchemaFingerprint,
    pub target_fingerprint: SchemaFingerprint,
    pub old_storage_id: StorageId,
    pub new_storage_id: StorageId,
    pub old_relative_locator: String,
    pub replacement_transaction: DatabaseTxnId,
    pub retired_generation: crate::SchemaGeneration,
}

/// Exact durable identity of one supported retired Heap incarnation. The cause
/// controls logical eligibility; physical deletion always uses the embedded
/// StorageId, generated locator and Heap identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetiredHeapGcTarget {
    TableDrop(RetiredTableResource),
    SchemaRewrite(ReplacementRetiredHeap),
}

impl RetiredHeapGcTarget {
    #[must_use]
    pub fn storage_id(&self) -> StorageId {
        match self {
            Self::TableDrop(resource) => resource.storage_id,
            Self::SchemaRewrite(resource) => resource.old_storage_id,
        }
    }
}

impl From<RetiredTableResource> for RetiredHeapGcTarget {
    fn from(resource: RetiredTableResource) -> Self {
        Self::TableDrop(resource)
    }
}

impl From<ReplacementRetiredHeap> for RetiredHeapGcTarget {
    fn from(resource: ReplacementRetiredHeap) -> Self {
        Self::SchemaRewrite(resource)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiredHeapGcState {
    Retained,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetiredHeapGcComponentKind {
    Owner = 1,
    Main = 2,
    Wal = 3,
    TransactionStatus = 4,
    AlternateWal = 5,
    CatalogLink = 6,
    CatalogLinkShadow = 7,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredHeapGcComponent {
    pub kind: RetiredHeapGcComponentKind,
    pub path: PathBuf,
    pub required: bool,
    pub present: bool,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetiredHeapGcBlocker {
    ActiveTransactionHandles { count: u64 },
    SchemaWriter,
    UnsupportedLocator,
    CoordinatorDecisionIncomplete { transaction: DatabaseTxnId },
    RequiredComponentMissing { kind: RetiredHeapGcComponentKind },
    HeapRecoveryPending { transaction: DatabaseTxnId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredHeapGcInspection {
    pub target: RetiredHeapGcTarget,
    pub state: RetiredHeapGcState,
    pub coordinator_horizon: Option<DatabaseTxnId>,
    pub manifest_digest: [u8; 32],
    pub components: Vec<RetiredHeapGcComponent>,
    pub blockers: Vec<RetiredHeapGcBlocker>,
    pub total_present_bytes: u64,
}

impl RetiredHeapGcInspection {
    #[must_use]
    pub fn eligible(&self) -> bool {
        self.state == RetiredHeapGcState::Retained && self.blockers.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredHeapGcReport {
    pub storage_id: StorageId,
    pub coordinator_horizon: DatabaseTxnId,
    pub files_deleted: u64,
    pub bytes_deleted: u64,
    pub state: RetiredHeapGcState,
}

#[derive(Debug)]
pub enum SchemaMutationError {
    SchemaBusy,
    RecoveryRequired,
    MultipleCreatesUnsupported,
    TransactionNotPristine,
    UnsupportedConstraint,
    UnsupportedPlacement,
    UnsupportedSchemaEvolution,
    InvalidSchemaEvolution(&'static str),
    TableNotFound(TableId),
    ColumnNotFound(ColumnId),
    IndexedColumn(ColumnId),
    PrimaryKeyColumn(ColumnId),
    NotNullViolation(ColumnId),
    UndefinedTable(String),
    StaleSchemaDependency,
    StalePreparedStatement,
    IdentityExhausted(&'static str),
    Corrupt(&'static str),
    RetiredHeapNotFound(StorageId),
    RetiredHeapTargetMismatch(StorageId),
    RetiredHeapGcIneligible,
    Catalog(SchemaCatalogError),
}
impl fmt::Display for SchemaMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaBusy => {
                f.write_str("schema writer requires exclusive transaction admission")
            }
            Self::RecoveryRequired => {
                f.write_str("schema mutation requires recovery before further work")
            }
            Self::MultipleCreatesUnsupported => {
                f.write_str("one Heap creation per transaction is supported")
            }
            Self::TransactionNotPristine => f.write_str(
                "Heap schema rewrite must precede every user read, write and schema mutation",
            ),
            Self::UnsupportedConstraint => {
                f.write_str("runtime table constraints are not supported")
            }
            Self::UnsupportedPlacement => {
                f.write_str("runtime table mutation supports only a single Heap")
            }
            Self::UnsupportedSchemaEvolution => {
                f.write_str("requested schema evolution is not supported")
            }
            Self::InvalidSchemaEvolution(reason) => write!(f, "invalid schema evolution: {reason}"),
            Self::TableNotFound(table) => {
                write!(f, "table identity {} is not active", table.0)
            }
            Self::ColumnNotFound(column) => write!(f, "column identity {} is not active", column.0),
            Self::IndexedColumn(column) => {
                write!(f, "column {} is referenced by an active index", column.0)
            }
            Self::PrimaryKeyColumn(column) => {
                write!(f, "column {} is primary-key metadata", column.0)
            }
            Self::NotNullViolation(column) => write!(f, "column {} contains NULL", column.0),
            Self::UndefinedTable(name) => {
                write!(f, "table `{name}` is not active")
            }
            Self::StaleSchemaDependency => f.write_str("exact table schema dependency is stale"),
            Self::StalePreparedStatement => {
                f.write_str("prepared statement schema or transaction scope is no longer valid")
            }
            Self::IdentityExhausted(kind) => write!(f, "{kind} identity space is exhausted"),
            Self::Corrupt(reason) => {
                write!(f, "schema mutation journal/recovery corrupt: {reason}")
            }
            Self::RetiredHeapNotFound(storage) => {
                write!(f, "retired Heap storage {} was not found", storage.0)
            }
            Self::RetiredHeapTargetMismatch(storage) => write!(
                f,
                "retired Heap GC target does not match durable identity for storage {}",
                storage.0
            ),
            Self::RetiredHeapGcIneligible => {
                f.write_str("retired Heap is not eligible for physical deletion")
            }
            Self::Catalog(error) => error.fmt(f),
        }
    }
}
impl Error for SchemaMutationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Catalog(e) => Some(e),
            _ => None,
        }
    }
}
impl From<SchemaCatalogError> for SchemaMutationError {
    fn from(error: SchemaCatalogError) -> Self {
        Self::Catalog(error)
    }
}
impl From<SchemaMutationError> for DatabaseError {
    fn from(error: SchemaMutationError) -> Self {
        Self::SchemaMutation(error)
    }
}

pub(crate) type SharedMutationJournal = Rc<RefCell<SchemaMutationJournal>>;
pub(crate) type SchemaWriter = Rc<Cell<Option<DatabaseTxnId>>>;

#[derive(Debug)]
pub(crate) struct SchemaMutation {
    pub(crate) catalog: PathBuf,
    pub(crate) reservation: Reservation,
    pub(crate) target: SchemaCatalogSnapshot,
    pub(crate) reference: SchemaParticipantReference,
    pub(crate) staged: Option<TableStorage>,
    pub(crate) drop: Option<DropIntent>,
    pub(crate) rewrite: Option<crate::schema_mutation_journal::RewriteIntent>,
    pub(crate) journal: SharedMutationJournal,
    pub(crate) writer: SchemaWriter,
}
impl SchemaMutation {
    pub(crate) fn transaction(&self) -> DatabaseTxnId {
        self.drop.as_ref().map_or_else(
            || {
                self.rewrite
                    .as_ref()
                    .map_or(self.reservation.transaction, |rewrite| {
                        rewrite.reservation.transaction
                    })
            },
            |drop| drop.transaction,
        )
    }

    pub(crate) fn prepare(&self) -> Result<(), SchemaMutationError> {
        let bytes = self.target.encode()?;
        if digest(&bytes) != self.reference.digest {
            return Err(SchemaMutationError::Corrupt("prepared schema changed"));
        }
        let path = file::resolve(
            &self.catalog,
            &prepared_locator(&self.catalog, self.target.incarnation, self.transaction())?,
        );
        // Retained until durable completion, allowing both NBSC/state renames to retry.
        validate_resource_path(&self.catalog, &path)?;
        ensure_parent(&path)?;
        file::atomic_write(&path, &bytes, false)?;
        file::sync_parent(&path)?;
        crash("prepared-catalog-durable");
        Ok(())
    }
    pub(crate) fn cleanup_loser(&mut self) -> Result<(), SchemaMutationError> {
        self.journal.borrow().ensure_ready()?;
        if let Some(drop) = &self.drop {
            cleanup_drop_prepared(&self.catalog, drop)?;
            crash("drop-rollback-cleanup");
            self.journal
                .borrow_mut()
                .resolve_drop(drop.transaction, false)?;
            self.writer.set(None);
            return Ok(());
        }
        if let Some(rewrite) = &self.rewrite {
            self.staged.take();
            cleanup_loser(&self.catalog, &self.reservation, self.target.incarnation)?;
            if self
                .journal
                .borrow()
                .rewrites
                .contains_key(&rewrite.reservation.transaction)
            {
                self.journal
                    .borrow_mut()
                    .resolve_rewrite(rewrite.reservation.transaction, false)?;
            }
            self.writer.set(None);
            return Ok(());
        }
        // Physical rollback has already synchronized all participants.
        self.staged.take();
        cleanup_loser(&self.catalog, &self.reservation, self.target.incarnation)?;
        if self
            .journal
            .borrow()
            .reservations
            .contains_key(&self.reservation.transaction)
        {
            self.journal
                .borrow_mut()
                .resolve(self.reservation.transaction, false)?;
        }
        self.writer.set(None);
        Ok(())
    }
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn type_spec(data_type: &SemanticType) -> TypeSpec {
    match &data_type.name {
        Some(name) => TypeSpec::Semantic {
            physical: data_type.physical,
            name: name.clone(),
        },
        None => TypeSpec::Physical(data_type.physical),
    }
}

fn build_alter_target(
    base: &TableDef,
    operation: &AlterTableOperation,
    reserved_column: Option<ColumnId>,
) -> Result<TableDef, DatabaseError> {
    let mut target = base.clone();
    match operation {
        AlterTableOperation::RenameTable { new_name } => {
            if new_name == &base.name {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "new table name equals current name",
                )
                .into());
            }
            target.name = new_name.clone();
        }
        AlterTableOperation::RenameColumn {
            column_id,
            new_name,
        } => {
            let column = target
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(SchemaMutationError::ColumnNotFound(*column_id))?;
            if new_name == &column.name {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "new column name equals current name",
                )
                .into());
            }
            column.name = new_name.clone();
        }
        AlterTableOperation::AddNullableColumn { name, data_type } => {
            let id = reserved_column.ok_or(SchemaMutationError::Corrupt(
                "ADD has no ColumnId reservation",
            ))?;
            target
                .columns
                .push(ColumnDef::new(id, name.clone(), type_spec(data_type)).nullable(true));
        }
        AlterTableOperation::DropColumn { column_id } => {
            let position = target
                .columns
                .iter()
                .position(|column| column.id == *column_id)
                .ok_or(SchemaMutationError::ColumnNotFound(*column_id))?;
            target.columns.remove(position);
        }
        AlterTableOperation::SetNotNull { column_id } => {
            let column = target
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(SchemaMutationError::ColumnNotFound(*column_id))?;
            if !column.nullable {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "column is already NOT NULL",
                )
                .into());
            }
            column.nullable = false;
        }
        AlterTableOperation::DropNotNull { column_id } => {
            let column = target
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(SchemaMutationError::ColumnNotFound(*column_id))?;
            if column.nullable {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "column is already nullable",
                )
                .into());
            }
            column.nullable = true;
        }
        AlterTableOperation::ChangeNominalType {
            column_id,
            target_type,
        } => {
            let column = target
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(SchemaMutationError::ColumnNotFound(*column_id))?;
            let current = column.semantic_type();
            if current.physical != target_type.physical {
                return Err(SchemaMutationError::UnsupportedSchemaEvolution.into());
            }
            if current == *target_type {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "target semantic type equals current type",
                )
                .into());
            }
            column.type_spec = type_spec(target_type);
        }
    }
    target.validate()?;
    for target_column in &target.columns {
        if let Some(base_column) = base.column_by_id(target_column.id) {
            if base_column.semantic_type().physical != target_column.semantic_type().physical {
                return Err(SchemaMutationError::UnsupportedSchemaEvolution.into());
            }
        }
    }
    Ok(target)
}

impl Database {
    /// Resolves a convenience name to the exact durable identity required by
    /// [`Database::drop_table_in`]. This method has no persistent side effects.
    pub fn resolve_drop_table(&self, name: &str) -> Result<DropTableTarget, DatabaseError> {
        let table = self
            .committed
            .schema
            .table(name)
            .ok_or_else(|| SchemaMutationError::UndefinedTable(name.to_owned()))?;
        let lineage = self
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == table.id)
            .ok_or(SchemaMutationError::Corrupt("active table lineage absent"))?;
        Ok(DropTableTarget {
            table_id: table.id,
            table_version: lineage.version,
            fingerprint: table.fingerprint()?,
        })
    }

    /// Resolves a table name to the exact identity required by a rewrite.
    /// This helper performs no allocation or persistent mutation.
    pub fn resolve_alter_table(&self, name: &str) -> Result<SchemaDependency, DatabaseError> {
        let table = self
            .committed
            .schema
            .table(name)
            .ok_or_else(|| SchemaMutationError::UndefinedTable(name.to_owned()))?;
        let lineage = self
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == table.id)
            .ok_or(SchemaMutationError::Corrupt("active table lineage absent"))?;
        Ok(SchemaDependency {
            table_id: table.id,
            table_version: lineage.version,
            fingerprint: table.fingerprint()?,
        })
    }

    /// Resolves one column name to its stable identity without mutation.
    pub fn resolve_alter_column(
        &self,
        target: &SchemaDependency,
        name: &str,
    ) -> Result<ColumnId, DatabaseError> {
        let table = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(target.table_id))?;
        let lineage = self
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == target.table_id)
            .ok_or(SchemaMutationError::Corrupt("active table lineage absent"))?;
        if lineage.version != target.table_version || table.fingerprint()? != target.fingerprint {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        table.column(name).map(|column| column.id).ok_or_else(|| {
            SchemaMutationError::InvalidSchemaEvolution("column name is not active").into()
        })
    }

    /// Rewrites one exact runtime-created Single Heap into a private replacement.
    /// The source remains read-only; every visible row is decoded under the base
    /// schema and immediately inserted under the target schema.
    pub fn rewrite_heap_table_schema_in(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        if !transaction.is_pristine_for_schema_rewrite() {
            return Err(SchemaMutationError::TransactionNotPristine.into());
        }
        if self.schema_writer.get().is_some() || Rc::strong_count(&self.transaction_owner) != 2 {
            return Err(SchemaMutationError::SchemaBusy.into());
        }
        let base_table = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == spec.target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(spec.target.table_id))?
            .clone();
        let base_lineage = self
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt("active table lineage absent"))?
            .clone();
        if base_lineage.version != spec.target.table_version
            || base_table.fingerprint()? != spec.target.fingerprint
        {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let old_storage_id = match self.bindings.placement(spec.target.table_id)? {
            TablePlacement::Single { storage_id, .. } => *storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(SchemaMutationError::UnsupportedPlacement.into());
            }
        };
        let source = self
            .registry
            .get_mut(old_storage_id)
            .ok_or(SchemaMutationError::Corrupt("active Heap storage absent"))?;
        if source.kind() != netbadb_storage::StorageKind::Heap
            || source.table() != &base_table
            || source.storage_id() != old_storage_id
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let rewrite_indexes = source.heap_rewrite_indexes()?;
        if let AlterTableOperation::DropColumn { column_id } = &spec.operation {
            if base_table
                .column_by_id(*column_id)
                .is_some_and(|column| column.primary_key)
            {
                return Err(SchemaMutationError::PrimaryKeyColumn(*column_id).into());
            }
            if rewrite_indexes
                .active
                .iter()
                .any(|index| index.column_id == *column_id)
            {
                return Err(SchemaMutationError::IndexedColumn(*column_id).into());
            }
        }
        for entry in self.registry.iter() {
            entry.storage.ensure_recovery_ready()?;
        }

        let catalog = self
            .catalog_path
            .clone()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let base = file::load(&catalog)?;
        let catalog_table = base
            .placements
            .tables
            .iter()
            .find(|entry| entry.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt(
                "catalog table placement absent",
            ))?
            .clone();
        let descriptor = base
            .storages
            .iter()
            .find(|storage| storage.id == old_storage_id)
            .ok_or(SchemaMutationError::Corrupt(
                "catalog Heap descriptor absent",
            ))?
            .clone();
        let expected_old_locator = final_locator(&catalog, base.incarnation, old_storage_id)?;
        if catalog_table.schema_fingerprint != spec.target.fingerprint
            || !matches!(catalog_table.placement, TablePlacement::Single { table_id, storage_id }
                if table_id == spec.target.table_id && storage_id == old_storage_id)
            || descriptor.table_id != spec.target.table_id
            || !matches!(descriptor.kind, CatalogStorageKind::Heap)
            || descriptor.locator != expected_old_locator
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let catalog_base_table = base
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == spec.target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(spec.target.table_id))?;
        let catalog_base_lineage = base
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt("catalog table lineage absent"))?;
        if catalog_base_table != &base_table || catalog_base_lineage != &base_lineage {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let next_generation = base
            .committed
            .generation
            .0
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("SchemaGeneration"))?;
        let next_epoch = base
            .epoch
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("catalog epoch"))?;
        let next_version = base_lineage
            .version
            .0
            .checked_add(1)
            .map(TableSchemaVersion)
            .ok_or(SchemaMutationError::IdentityExhausted("TableSchemaVersion"))?;
        self.catalog_generation
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted(
                "runtime catalog revision",
            ))?;
        let new_storage_id = self
            .next_storage_id()
            .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        let next_storage_floor = new_storage_id
            .0
            .checked_add(1)
            .map(StorageId)
            .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        let column_floor =
            self.mutation_journal
                .as_ref()
                .map_or(base_lineage.next_column_id, |journal| {
                    journal
                        .borrow()
                        .effective_column(spec.target.table_id, base_lineage.next_column_id)
                });
        let reserved_column = matches!(
            &spec.operation,
            AlterTableOperation::AddNullableColumn { .. }
        )
        .then_some(column_floor.ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?);
        let target_next_column =
            reserved_column.map_or(column_floor, |column| column.0.checked_add(1).map(ColumnId));
        if reserved_column.is_some() && target_next_column.is_none() {
            return Err(SchemaMutationError::IdentityExhausted("ColumnId").into());
        }
        let target_table = build_alter_target(&base_table, &spec.operation, reserved_column)?;
        let target_fingerprint = target_table.fingerprint()?;
        if target_fingerprint == spec.target.fingerprint {
            return Err(SchemaMutationError::InvalidSchemaEvolution(
                "operation does not change canonical schema",
            )
            .into());
        }
        for index in &rewrite_indexes.active {
            let old = base_table
                .column_by_id(index.column_id)
                .ok_or(SchemaMutationError::Corrupt("active index column absent"))?;
            let target = target_table
                .column_by_id(index.column_id)
                .ok_or(SchemaMutationError::IndexedColumn(index.column_id))?;
            if old.semantic_type().physical != target.semantic_type().physical {
                return Err(SchemaMutationError::UnsupportedSchemaEvolution.into());
            }
        }
        let coordinator_locator = base
            .coordinator
            .clone()
            .or_else(|| {
                self.mutation_journal
                    .as_ref()
                    .map(|journal| journal.borrow().coordinator.clone())
            })
            .unwrap_or(format!(
                "{}/coordinator",
                namespace(&catalog, base.incarnation)?
            ));
        let new_descriptor = CatalogStorage {
            id: new_storage_id,
            table_id: spec.target.table_id,
            locator: final_locator(&catalog, base.incarnation, new_storage_id)?,
            kind: CatalogStorageKind::Heap,
        };
        let target_lineage = TableLineage {
            table_id: spec.target.table_id,
            version: next_version,
            next_column_id: target_next_column,
        };
        let target_catalog_table = CatalogTable {
            table_id: spec.target.table_id,
            schema_fingerprint: target_fingerprint,
            placement: TablePlacement::Single {
                table_id: spec.target.table_id,
                storage_id: new_storage_id,
            },
        };
        let mut target = base.clone();
        target.epoch = next_epoch;
        target.committed.generation = crate::SchemaGeneration(next_generation);
        target.committed.schema = Schema::new(
            base.committed
                .schema
                .tables()
                .iter()
                .map(|table| {
                    if table.id == spec.target.table_id {
                        target_table.clone()
                    } else {
                        table.clone()
                    }
                })
                .collect(),
        )?;
        *target
            .committed
            .tables
            .iter_mut()
            .find(|lineage| lineage.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt("target lineage absent"))? = target_lineage.clone();
        target.committed.next_storage_id = Some(next_storage_floor);
        *target
            .placements
            .tables
            .iter_mut()
            .find(|entry| entry.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt("target placement absent"))? =
            target_catalog_table.clone();
        *target
            .storages
            .iter_mut()
            .find(|storage| storage.id == old_storage_id)
            .ok_or(SchemaMutationError::Corrupt("target descriptor absent"))? =
            new_descriptor.clone();
        target.coordinator = Some(coordinator_locator.clone());
        let target_bytes = target.encode()?;
        let base_fragment = SchemaCatalogSnapshot {
            incarnation: base.incarnation,
            epoch: base.epoch,
            committed: CommittedCatalogState {
                schema: Schema::new(vec![base_table.clone()])?,
                generation: base.committed.generation,
                next_table_id: base.committed.next_table_id,
                next_storage_id: base.committed.next_storage_id,
                next_partition_id: base.committed.next_partition_id,
                tables: vec![base_lineage.clone()],
            },
            placements: PartitionCatalog {
                tables: vec![catalog_table],
            },
            storages: vec![descriptor],
            coordinator: Some(coordinator_locator.clone()),
            partition_evidence: None,
        };
        let target_fragment = SchemaCatalogSnapshot {
            incarnation: base.incarnation,
            epoch: next_epoch,
            committed: CommittedCatalogState {
                schema: Schema::new(vec![target_table.clone()])?,
                generation: crate::SchemaGeneration(next_generation),
                next_table_id: target.committed.next_table_id,
                next_storage_id: target.committed.next_storage_id,
                next_partition_id: target.committed.next_partition_id,
                tables: vec![target_lineage],
            },
            placements: PartitionCatalog {
                tables: vec![target_catalog_table],
            },
            storages: vec![new_descriptor],
            coordinator: Some(coordinator_locator.clone()),
            partition_evidence: None,
        };
        let rewrite_reservation = RewriteReservation {
            transaction: transaction.id(),
            table: spec.target.table_id,
            storage: new_storage_id,
            column: reserved_column,
            base_generation: base.committed.generation,
            base_epoch: base.epoch,
        };
        let rewrite = RewriteIntent {
            reservation: rewrite_reservation.clone(),
            operation: spec.operation.clone(),
            base: base_fragment,
            target: target_fragment.clone(),
            snapshot_digest: digest(&target_bytes),
            retired: false,
            resolved: None,
            gc: None,
        };
        let journal = match &self.mutation_journal {
            Some(journal) => Rc::clone(journal),
            None => {
                let journal = SchemaMutationJournal::initialize(
                    &catalog,
                    base.incarnation,
                    coordinator_locator.clone(),
                )?;
                let journal = Rc::new(RefCell::new(journal));
                self.mutation_journal = Some(Rc::clone(&journal));
                journal
            }
        };
        journal.borrow().ensure_ready()?;
        journal
            .borrow()
            .prepare_rewrite(&rewrite_reservation, &rewrite)?;
        let coordinator = match &self.coordinator {
            Some(coordinator) => Rc::clone(coordinator),
            None => {
                let path = file::resolve(&catalog, &coordinator_locator);
                validate_resource_path(&catalog, &path)?;
                ensure_parent(&path)?;
                let log = if path
                    .try_exists()
                    .map_err(|error| file::io("inspect schema coordinator", &path, error))?
                {
                    CoordinatorLog::open(&path)?
                } else {
                    CoordinatorLog::create(&path)?
                };
                Rc::new(RefCell::new(log))
            }
        };
        transaction.set_coordinator(coordinator);
        let reference = SchemaParticipantReference {
            incarnation: base.incarnation,
            target_epoch: next_epoch,
            digest: rewrite.snapshot_digest,
        };
        let reservation = Reservation {
            transaction: transaction.id(),
            table: spec.target.table_id,
            storage: new_storage_id,
            base_generation: base.committed.generation,
            base_epoch: base.epoch,
            intent: Some(CreateIntent {
                fragment: target_fragment,
                snapshot_digest: rewrite.snapshot_digest,
            }),
            resolved: None,
        };
        self.schema_writer.set(Some(transaction.id()));
        transaction.schema_mutation = Some(SchemaMutation {
            catalog: catalog.clone(),
            reservation,
            target,
            reference,
            staged: None,
            drop: None,
            rewrite: Some(rewrite.clone()),
            journal: Rc::clone(&journal),
            writer: Rc::clone(&self.schema_writer),
        });
        let result = (|| -> Result<(), DatabaseError> {
            journal.borrow_mut().reserve_rewrite(rewrite_reservation)?;
            crash("rewrite-reservation-durable");
            journal.borrow_mut().rewrite_intent(rewrite.clone())?;
            crash("rewrite-intent-durable");
            let stage = file::resolve(
                &catalog,
                &stage_locator(&catalog, base.incarnation, transaction.id(), new_storage_id)?,
            );
            validate_resource_path(&catalog, &stage)?;
            ensure_parent(&stage)?;
            write_owner(
                &file::suffix(&stage, ".owner"),
                base.incarnation,
                transaction.id(),
                spec.target.table_id,
                new_storage_id,
                target_fingerprint,
            )?;
            crash("rewrite-stage-first-file");
            let storage = TableStorage::create_heap_with_storage_id(
                &stage,
                target_table.clone(),
                new_storage_id,
            )?;
            storage.flush()?;
            file::sync_parent(&stage)?;
            transaction.enlist_staged(storage)?;
            crash("rewrite-before-index-install");
            transaction.with_staged_write(|storage, context| {
                storage.install_heap_rewrite_indexes_in(context, &rewrite_indexes)
            })?;
            crash("rewrite-stage-synced");

            let old_columns = base_table
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>();
            let transform = target_table
                .columns
                .iter()
                .map(|target_column| {
                    base_table
                        .columns
                        .iter()
                        .position(|old_column| old_column.id == target_column.id)
                })
                .collect::<Vec<_>>();
            let source_view = self
                .registry
                .get(old_storage_id)
                .ok_or(SchemaMutationError::Corrupt("rewrite source disappeared"))?
                .read_view()?;
            let mut copied = 0_u64;
            let flow = self
                .registry
                .get_mut(old_storage_id)
                .ok_or(SchemaMutationError::Corrupt("rewrite source disappeared"))?
                .visit_rows_with_view_control::<DatabaseError, _>(
                    &old_columns,
                    &source_view,
                    |_row, old_values| {
                        let values = transform
                            .iter()
                            .map(|position| {
                                position.map_or(ScalarValue::Null, |position| {
                                    old_values[position].clone()
                                })
                            })
                            .collect::<Vec<_>>();
                        if let AlterTableOperation::SetNotNull { column_id } = &spec.operation {
                            let position = target_table
                                .columns
                                .iter()
                                .position(|column| column.id == *column_id)
                                .ok_or(SchemaMutationError::ColumnNotFound(*column_id))?;
                            if matches!(values[position], ScalarValue::Null) {
                                return Err(
                                    SchemaMutationError::NotNullViolation(*column_id).into()
                                );
                            }
                        }
                        transaction.with_staged_write(|storage, context| {
                            storage.insert_in(context, &values).map(|_| ())
                        })?;
                        copied = copied
                            .checked_add(1)
                            .ok_or(SchemaMutationError::Corrupt("rewrite row count overflow"))?;
                        if copied == 1 {
                            crash("rewrite-first-row");
                        } else if copied == 2 {
                            crash("rewrite-mid-copy");
                        }
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
            if flow.is_break() {
                return Err(SchemaMutationError::Corrupt(
                    "rewrite row visitor stopped unexpectedly",
                )
                .into());
            }
            crash("rewrite-copy-complete");
            // Deliberately do not force target data/index pages here. Physical
            // participant prepare synchronizes WAL/status before the schema
            // decision; winner recovery must be able to replay those durable
            // records into S2 without rescanning S1.
            crash("rewrite-indexes-complete");
            Ok(())
        })();
        if result.is_err() {
            transaction.require_schema_rollback();
        }
        result
    }

    /// Logically retires one exact active non-partitioned Heap in `transaction`.
    /// The physical Heap and its index pages remain intact for deferred GC.
    pub fn drop_table_in(
        &mut self,
        transaction: &mut Transaction,
        target: DropTableTarget,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        if transaction.schema_mutation.is_some() || transaction.has_pending_schema_mutations() {
            return Err(DatabaseError::UnsupportedDdlCombination);
        }
        if self.schema_writer.get().is_some() || Rc::strong_count(&self.transaction_owner) != 2 {
            return Err(SchemaMutationError::SchemaBusy.into());
        }
        let table = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(target.table_id))?
            .clone();
        let lineage = self
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == target.table_id)
            .ok_or(SchemaMutationError::Corrupt("active table lineage absent"))?
            .clone();
        if lineage.version != target.table_version || table.fingerprint()? != target.fingerprint {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let placement = self.bindings.placement(target.table_id)?.clone();
        let storage_id = match placement {
            TablePlacement::Single { storage_id, .. } => storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(SchemaMutationError::UnsupportedPlacement.into());
            }
        };
        let active_storage = self
            .registry
            .get(storage_id)
            .ok_or(SchemaMutationError::Corrupt("active Heap storage absent"))?;
        if active_storage.kind() != netbadb_storage::StorageKind::Heap
            || active_storage.table().id != target.table_id
            || active_storage.table().fingerprint()? != target.fingerprint
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        for entry in self.registry.iter() {
            entry.storage.ensure_recovery_ready()?;
        }
        let catalog = self
            .catalog_path
            .clone()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let base = file::load(&catalog)?;
        let base_table = base
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(target.table_id))?;
        let base_lineage = base
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == target.table_id)
            .ok_or(SchemaMutationError::Corrupt("catalog table lineage absent"))?;
        if base_lineage.version != target.table_version
            || base_table.fingerprint()? != target.fingerprint
        {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let catalog_table = base
            .placements
            .tables
            .iter()
            .find(|entry| entry.table_id == target.table_id)
            .ok_or(SchemaMutationError::Corrupt(
                "catalog table placement absent",
            ))?
            .clone();
        if catalog_table.placement != placement
            || catalog_table.schema_fingerprint != target.fingerprint
        {
            return Err(
                SchemaMutationError::Corrupt("catalog binding differs from active view").into(),
            );
        }
        let descriptor = base
            .storages
            .iter()
            .find(|storage| storage.id == storage_id)
            .ok_or(SchemaMutationError::Corrupt(
                "catalog Heap descriptor absent",
            ))?
            .clone();
        if descriptor.table_id != target.table_id
            || !matches!(descriptor.kind, CatalogStorageKind::Heap)
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let next_generation = base
            .committed
            .generation
            .0
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("SchemaGeneration"))?;
        let next_epoch = base
            .epoch
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("catalog epoch"))?;
        self.catalog_generation
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted(
                "runtime catalog revision",
            ))?;
        let coordinator_locator = base
            .coordinator
            .clone()
            .or_else(|| {
                self.mutation_journal
                    .as_ref()
                    .map(|journal| journal.borrow().coordinator.clone())
            })
            .unwrap_or(format!(
                "{}/coordinator",
                namespace(&catalog, base.incarnation)?
            ));
        let mut target_snapshot = base.clone();
        target_snapshot.epoch = next_epoch;
        target_snapshot.committed.generation = crate::SchemaGeneration(next_generation);
        target_snapshot.committed.schema = Schema::new(
            base.committed
                .schema
                .tables()
                .iter()
                .filter(|candidate| candidate.id != target.table_id)
                .cloned()
                .collect(),
        )?;
        target_snapshot
            .committed
            .tables
            .retain(|entry| entry.table_id != target.table_id);
        target_snapshot
            .placements
            .tables
            .retain(|entry| entry.table_id != target.table_id);
        target_snapshot
            .storages
            .retain(|storage| storage.id != storage_id);
        target_snapshot.coordinator = Some(coordinator_locator.clone());
        let target_bytes = target_snapshot.encode()?;
        let fragment = SchemaCatalogSnapshot {
            incarnation: base.incarnation,
            epoch: base.epoch,
            committed: CommittedCatalogState {
                schema: Schema::new(vec![table])?,
                generation: base.committed.generation,
                next_table_id: base.committed.next_table_id,
                next_storage_id: base.committed.next_storage_id,
                next_partition_id: base.committed.next_partition_id,
                tables: vec![lineage],
            },
            placements: PartitionCatalog {
                tables: vec![catalog_table],
            },
            storages: vec![descriptor],
            coordinator: Some(coordinator_locator.clone()),
            partition_evidence: None,
        };
        let drop = DropIntent {
            transaction: transaction.id(),
            fragment,
            target_generation: target_snapshot.committed.generation,
            target_epoch: target_snapshot.epoch,
            snapshot_digest: digest(&target_bytes),
            retired: false,
            resolved: None,
            gc: None,
        };
        validate_drop_resource(&catalog, &drop)?;
        let journal = match &self.mutation_journal {
            Some(journal) => Rc::clone(journal),
            None => {
                let journal = SchemaMutationJournal::initialize(
                    &catalog,
                    base.incarnation,
                    coordinator_locator.clone(),
                )?;
                let journal = Rc::new(RefCell::new(journal));
                self.mutation_journal = Some(Rc::clone(&journal));
                journal
            }
        };
        journal.borrow().ensure_ready()?;
        journal.borrow().prepare_drop(&drop)?;
        let coordinator = match &self.coordinator {
            Some(coordinator) => Rc::clone(coordinator),
            None => {
                let path = file::resolve(&catalog, &coordinator_locator);
                validate_resource_path(&catalog, &path)?;
                ensure_parent(&path)?;
                let log = if path
                    .try_exists()
                    .map_err(|error| file::io("inspect schema coordinator", &path, error))?
                {
                    CoordinatorLog::open(&path)?
                } else {
                    CoordinatorLog::create(&path)?
                };
                Rc::new(RefCell::new(log))
            }
        };
        transaction.set_coordinator(coordinator);
        let reference = SchemaParticipantReference {
            incarnation: base.incarnation,
            target_epoch: next_epoch,
            digest: drop.snapshot_digest,
        };
        let reservation = Reservation {
            transaction: transaction.id(),
            table: target.table_id,
            storage: storage_id,
            base_generation: base.committed.generation,
            base_epoch: base.epoch,
            intent: None,
            resolved: None,
        };
        self.schema_writer.set(Some(transaction.id()));
        transaction.schema_mutation = Some(SchemaMutation {
            catalog,
            reservation,
            target: target_snapshot,
            reference,
            staged: None,
            drop: Some(drop.clone()),
            rewrite: None,
            journal: Rc::clone(&journal),
            writer: Rc::clone(&self.schema_writer),
        });
        if let Err(error) = journal.borrow_mut().drop_intent(drop) {
            transaction.require_schema_rollback();
            return Err(error.into());
        }
        crash("drop-intent-durable");
        crash("drop-overlay-established");
        Ok(())
    }

    /// Lists durable retirement identities, including resources whose explicit
    /// physical GC has reached Deleted. Inspect the GC state separately.
    pub fn inspect_retired_table_resources(&self) -> Vec<RetiredTableResource> {
        self.mutation_journal
            .as_ref()
            .into_iter()
            .flat_map(|journal| {
                journal
                    .borrow()
                    .drops
                    .values()
                    .filter(|drop| drop.retired && drop.resolved == Some(true))
                    .map(|drop| {
                        let table = &drop.fragment.committed.schema.tables()[0];
                        RetiredTableResource {
                            table_id: table.id,
                            table_version: drop.fragment.committed.tables[0].version,
                            fingerprint: drop.fragment.placements.tables[0].schema_fingerprint,
                            storage_id: drop.fragment.storages[0].id,
                            engine: netbadb_storage::StorageKind::Heap,
                            relative_locator: drop.fragment.storages[0].locator.clone(),
                            retired_generation: drop.target_generation,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Lists replacement-retired physical Heaps without exposing them through
    /// the active logical catalog.
    pub fn inspect_replacement_retired_heaps(&self) -> Vec<ReplacementRetiredHeap> {
        self.mutation_journal
            .as_ref()
            .into_iter()
            .flat_map(|journal| {
                journal
                    .borrow()
                    .rewrites
                    .values()
                    .filter(|rewrite| rewrite.retired && rewrite.resolved == Some(true))
                    .map(|rewrite| ReplacementRetiredHeap {
                        table_id: rewrite.table(),
                        base_table_version: rewrite.base.committed.tables[0].version,
                        target_table_version: rewrite.target.committed.tables[0].version,
                        base_fingerprint: rewrite.base.placements.tables[0].schema_fingerprint,
                        target_fingerprint: rewrite.target.placements.tables[0].schema_fingerprint,
                        old_storage_id: rewrite.old_storage(),
                        new_storage_id: rewrite.new_storage(),
                        old_relative_locator: rewrite.base.storages[0].locator.clone(),
                        replacement_transaction: rewrite.reservation.transaction,
                        retired_generation: rewrite.target.committed.generation,
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Inspects one exact replacement-retired Heap through the same physical
    /// proof used for DROP retirement.
    pub fn inspect_replacement_retired_heap_gc(
        &self,
        target: &ReplacementRetiredHeap,
    ) -> Result<RetiredHeapGcInspection, DatabaseError> {
        self.inspect_retired_heap_gc_target(&RetiredHeapGcTarget::SchemaRewrite(target.clone()))
    }

    /// Reclaims one exact replacement-retired Heap through the generic retired
    /// resource state machine.
    pub fn gc_replacement_retired_heap(
        &mut self,
        target: &ReplacementRetiredHeap,
    ) -> Result<RetiredHeapGcReport, DatabaseError> {
        self.gc_retired_heap_resource(&RetiredHeapGcTarget::SchemaRewrite(target.clone()))
    }

    /// Inspects one exact durable retirement. This never selects candidates or
    /// mutates storage. The returned proof includes the coordinator horizon and
    /// every exact file in the supported single-Heap bundle.
    pub fn inspect_retired_heap_gc(
        &self,
        target: &RetiredTableResource,
    ) -> Result<RetiredHeapGcInspection, DatabaseError> {
        self.inspect_retired_heap_gc_target(&RetiredHeapGcTarget::TableDrop(target.clone()))
    }

    /// Generic read-only proof for either supported retirement cause.
    pub fn inspect_retired_heap_gc_target(
        &self,
        target: &RetiredHeapGcTarget,
    ) -> Result<RetiredHeapGcInspection, DatabaseError> {
        let catalog = self
            .catalog_path
            .as_deref()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let journal =
            self.mutation_journal
                .as_ref()
                .ok_or(SchemaMutationError::RetiredHeapNotFound(
                    target.storage_id(),
                ))?;
        let (intent, create_transaction) = {
            let journal = journal.borrow();
            let intent = retired_intent_for_target(&journal, target)?;
            let create_transaction = create_transaction_for_storage(&journal, intent.storage());
            (intent, create_transaction)
        };
        let decisions = self
            .coordinator
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt(
                "retired Heap has no coordinator",
            ))?
            .borrow()
            .decisions()
            .cloned()
            .collect::<Vec<_>>();
        inspect_gc(catalog, &intent, create_transaction, &decisions, self)
    }

    /// Physically deletes one exact runtime-created retired single Heap.
    /// A durable NBSJ intent is synchronized before the first unlink; after
    /// that point recovery is retry-only until durable Complete.
    pub fn gc_retired_heap(
        &mut self,
        target: &RetiredTableResource,
    ) -> Result<RetiredHeapGcReport, DatabaseError> {
        self.gc_retired_heap_resource(&RetiredHeapGcTarget::TableDrop(target.clone()))
    }

    /// Physically deletes one exact retired runtime Heap, independent of
    /// whether logical retirement was caused by DROP or schema replacement.
    pub fn gc_retired_heap_resource(
        &mut self,
        target: &RetiredHeapGcTarget,
    ) -> Result<RetiredHeapGcReport, DatabaseError> {
        let catalog = self
            .catalog_path
            .clone()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let journal = self.mutation_journal.as_ref().cloned().ok_or(
            SchemaMutationError::RetiredHeapNotFound(target.storage_id()),
        )?;
        let (intent, create_transaction) = {
            let journal = journal.borrow();
            let intent = retired_intent_for_target(&journal, target)?;
            let create_transaction = create_transaction_for_storage(&journal, intent.storage());
            (intent, create_transaction)
        };
        let decisions = self
            .coordinator
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt(
                "retired Heap has no coordinator",
            ))?
            .borrow()
            .decisions()
            .cloned()
            .collect::<Vec<_>>();
        let inspection = inspect_gc(&catalog, &intent, create_transaction, &decisions, self)?;
        if inspection.state == RetiredHeapGcState::Deleted {
            return Ok(RetiredHeapGcReport {
                storage_id: target.storage_id(),
                coordinator_horizon: inspection
                    .coordinator_horizon
                    .ok_or(SchemaMutationError::Corrupt("deleted GC has no horizon"))?,
                files_deleted: 0,
                bytes_deleted: 0,
                state: RetiredHeapGcState::Deleted,
            });
        }
        if inspection.state == RetiredHeapGcState::Retained {
            let hard_blocker = inspection.blockers.iter().any(|blocker| {
                !matches!(blocker, RetiredHeapGcBlocker::HeapRecoveryPending { .. })
            });
            if hard_blocker {
                return Err(SchemaMutationError::RetiredHeapGcIneligible.into());
            }
            let horizon = inspection
                .coordinator_horizon
                .ok_or(SchemaMutationError::RetiredHeapGcIneligible)?;
            let gc = RetiredHeapGcRecord {
                coordinator_horizon: horizon,
                manifest_digest: inspection.manifest_digest,
                complete: false,
            };
            journal.borrow().prepare_gc(intent.transaction(), &gc)?;

            // Resolve all local prepared history while the full physical bundle
            // is still present, then close the only recovery-only handle.
            let path = file::resolve(&catalog, intent.relative_locator());
            let table = &intent.fragment().committed.schema.tables()[0];
            let recovery = TableStorage::inspect_heap_recovery(&path, table)?;
            let resolutions = recovery
                .prepared_transactions
                .iter()
                .map(|prepared| {
                    crate::resolution_for_prepared(prepared, intent.storage(), &decisions)
                })
                .collect::<Result<Vec<_>, _>>()?;
            TableStorage::open_heap_with_prepared_resolutions(&path, table.clone(), &resolutions)?
                .close()?;
            let remaining = TableStorage::inspect_heap_recovery(&path, table)?;
            if remaining.prepared_transactions.iter().any(|prepared| {
                prepared.state == netbadb_storage::PreparedTransactionState::Prepared
            }) {
                return Err(SchemaMutationError::RetiredHeapGcIneligible.into());
            }
            // Revalidate the append-only proof immediately before making the
            // retry-only transition.
            validate_gc_horizon(&intent, &decisions, horizon)?;
            crash("gc-before-intent");
            journal.borrow_mut().gc_intent(intent.transaction(), gc)?;
            crash("gc-intent-durable");
        }
        let before = gc_components(&catalog, &intent)?
            .into_iter()
            .map(|component| component_metadata(&component.path))
            .collect::<Result<Vec<_>, _>>()?;
        let files_deleted = before.iter().filter(|bytes| bytes.is_some()).count() as u64;
        let bytes_deleted = before.iter().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(bytes.unwrap_or(0))
                .ok_or(SchemaMutationError::Corrupt("GC byte count overflow"))
        })?;
        resume_gc_intent(&catalog, &mut journal.borrow_mut(), &intent, &decisions)?;
        crash("gc-before-api-return");
        let coordinator_horizon = {
            let journal = journal.borrow();
            journal
                .drops
                .get(&intent.transaction())
                .and_then(|drop| drop.gc.as_ref())
                .or_else(|| {
                    journal
                        .rewrites
                        .get(&intent.transaction())
                        .and_then(|rewrite| rewrite.gc.as_ref())
                })
                .map(|gc| gc.coordinator_horizon)
        }
        .ok_or(SchemaMutationError::Corrupt("completed GC state absent"))?;
        Ok(RetiredHeapGcReport {
            storage_id: target.storage_id(),
            coordinator_horizon,
            files_deleted,
            bytes_deleted,
            state: RetiredHeapGcState::Deleted,
        })
    }

    /// Stages one new Heap and enlists it in `transaction`. Existing-table DML
    /// may precede/follow creation. Use `prepare_statement_in` for its private
    /// schema and `commit_transaction` to publish it; rollback never reuses IDs.
    pub fn create_heap_table_in(
        &mut self,
        transaction: &mut Transaction,
        spec: CreateTableSpec,
    ) -> Result<TableId, DatabaseError> {
        self.validate_transaction(transaction)?;
        if transaction.schema_mutation.is_some() {
            return Err(SchemaMutationError::MultipleCreatesUnsupported.into());
        }
        if transaction.has_pending_schema_mutations() {
            return Err(DatabaseError::UnsupportedDdlCombination);
        }
        if self.schema_writer.get().is_some() || Rc::strong_count(&self.transaction_owner) != 2 {
            return Err(SchemaMutationError::SchemaBusy.into());
        }
        let catalog = self
            .catalog_path
            .clone()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let base = file::load(&catalog)?;
        let next_generation = base
            .committed
            .generation
            .0
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("SchemaGeneration"))?;
        let next_epoch = base
            .epoch
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("catalog epoch"))?;
        self.catalog_generation
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted(
                "runtime catalog revision",
            ))?;
        // Validate before reserving, including duplicates and bounded canonical metadata.
        let table_id = self
            .next_table_id()
            .ok_or(SchemaMutationError::IdentityExhausted("TableId"))?;
        let storage_id = self
            .next_storage_id()
            .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        let mut columns = Vec::with_capacity(spec.columns.len().min(4096));
        if spec.columns.len() > 4096 {
            return Err(SchemaCatalogError::CapacityExceeded("columns").into());
        }
        for (position, column) in spec.columns.into_iter().enumerate() {
            let id = u32::try_from(position + 1)
                .map_err(|_| SchemaMutationError::IdentityExhausted("ColumnId"))?;
            let type_spec = match column.data_type.name {
                Some(name) => TypeSpec::Semantic {
                    physical: column.data_type.physical,
                    name,
                },
                None => TypeSpec::Physical(column.data_type.physical),
            };
            columns.push(
                ColumnDef::new(ColumnId(id), column.name, type_spec).nullable(column.nullable),
            );
        }
        let next_column_id = ColumnId(
            u32::try_from(columns.len())
                .ok()
                .and_then(|n| n.checked_add(1))
                .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?,
        );
        let table = TableDef::new(table_id, spec.name, columns);
        let mut schema = base.committed.schema.clone();
        schema.add_table(table.clone())?;
        let coordinator_locator = base
            .coordinator
            .clone()
            .or_else(|| {
                self.mutation_journal
                    .as_ref()
                    .map(|j| j.borrow().coordinator.clone())
            })
            .unwrap_or(format!(
                "{}/coordinator",
                namespace(&catalog, base.incarnation)?
            ));
        let placement = TablePlacement::Single {
            table_id,
            storage_id,
        };
        let catalog_table = CatalogTable {
            table_id,
            schema_fingerprint: table.fingerprint()?,
            placement: placement.clone(),
        };
        let descriptor = CatalogStorage {
            id: storage_id,
            table_id,
            locator: final_locator(&catalog, base.incarnation, storage_id)?,
            kind: CatalogStorageKind::Heap,
        };
        let lineage = TableLineage {
            table_id,
            version: TableSchemaVersion(1),
            next_column_id: Some(next_column_id),
        };
        let mut target = base.clone();
        target.epoch = next_epoch;
        target.committed.generation = crate::SchemaGeneration(next_generation);
        target.committed.schema = schema;
        target.committed.tables.push(lineage.clone());
        target.committed.next_table_id = table_id.0.checked_add(1).map(TableId);
        target.committed.next_storage_id = storage_id.0.checked_add(1).map(StorageId);
        target.placements.tables.push(catalog_table.clone());
        target.storages.push(descriptor.clone());
        target.coordinator = Some(coordinator_locator.clone());
        let bytes = target.encode()?;
        // Refuse dirty/dropped physical transactions before acquiring schema ownership.
        for entry in self.registry.iter() {
            entry.storage.ensure_recovery_ready()?;
        }
        let journal = match &self.mutation_journal {
            Some(journal) => Rc::clone(journal),
            None => {
                let journal = SchemaMutationJournal::initialize(
                    &catalog,
                    base.incarnation,
                    coordinator_locator.clone(),
                )?;
                let journal = Rc::new(RefCell::new(journal));
                self.mutation_journal = Some(Rc::clone(&journal));
                journal
            }
        };
        journal.borrow().ensure_ready()?;
        let coordinator = match &self.coordinator {
            Some(coordinator) => Rc::clone(coordinator),
            None => {
                let path = file::resolve(&catalog, &coordinator_locator);
                validate_resource_path(&catalog, &path)?;
                ensure_parent(&path)?;
                let log = if path
                    .try_exists()
                    .map_err(|e| file::io("inspect schema coordinator", &path, e))?
                {
                    CoordinatorLog::open(&path)?
                } else {
                    if !journal.borrow().reservations.is_empty() {
                        return Err(SchemaMutationError::Corrupt(
                            "reserved schema coordinator is missing",
                        )
                        .into());
                    }
                    CoordinatorLog::create(&path)?
                };
                Rc::new(RefCell::new(log))
            }
        };
        transaction.set_coordinator(coordinator);
        let reference = SchemaParticipantReference {
            incarnation: base.incarnation,
            target_epoch: next_epoch,
            digest: digest(&bytes),
        };
        let fragment = SchemaCatalogSnapshot {
            incarnation: base.incarnation,
            epoch: next_epoch,
            committed: CommittedCatalogState {
                schema: Schema::new(vec![table.clone()])?,
                generation: target.committed.generation,
                next_table_id: target.committed.next_table_id,
                next_storage_id: target.committed.next_storage_id,
                next_partition_id: target.committed.next_partition_id,
                tables: vec![lineage],
            },
            placements: PartitionCatalog {
                tables: vec![catalog_table],
            },
            storages: vec![descriptor],
            coordinator: Some(coordinator_locator),
            partition_evidence: None,
        };
        let intent = CreateIntent {
            fragment,
            snapshot_digest: reference.digest,
        };
        let reservation = Reservation {
            transaction: transaction.id(),
            table: table_id,
            storage: storage_id,
            base_generation: base.committed.generation,
            base_epoch: base.epoch,
            intent: None,
            resolved: None,
        };
        journal
            .borrow()
            .prepare_reservation(&reservation, &intent)?;
        self.schema_writer.set(Some(transaction.id()));
        transaction.schema_mutation = Some(SchemaMutation {
            catalog: catalog.clone(),
            reservation: reservation.clone(),
            target,
            reference,
            staged: None,
            drop: None,
            rewrite: None,
            journal: Rc::clone(&journal),
            writer: Rc::clone(&self.schema_writer),
        });
        let result = (|| -> Result<TableId, DatabaseError> {
            journal.borrow_mut().reserve(reservation)?;
            crash("reservation-durable");
            journal
                .borrow_mut()
                .intent(transaction.id(), intent.clone())?;
            if let Some(mutation) = &mut transaction.schema_mutation {
                mutation.reservation.intent = Some(intent);
            }
            crash("intent-durable");
            let stage = file::resolve(
                &catalog,
                &stage_locator(&catalog, base.incarnation, transaction.id(), storage_id)?,
            );
            validate_resource_path(&catalog, &stage)?;
            ensure_parent(&stage)?;
            write_owner(
                &file::suffix(&stage, ".owner"),
                base.incarnation,
                transaction.id(),
                table_id,
                storage_id,
                table.fingerprint()?,
            )?;
            crash("stage-first-file");
            let storage = TableStorage::create_heap_with_storage_id(&stage, table, storage_id)?;
            storage.flush()?;
            file::sync_parent(&stage)?;
            transaction.enlist_staged(storage)?;
            crash("stage-synced");
            Ok(table_id)
        })();
        if result.is_err() {
            transaction.require_schema_rollback();
        }
        result
    }

    pub(crate) fn ensure_schema_available(
        &self,
        transaction: Option<DatabaseTxnId>,
    ) -> Result<(), DatabaseError> {
        if self.schema_writer.get().is_some() && Rc::strong_count(&self.schema_writer) == 1 {
            return Err(SchemaMutationError::RecoveryRequired.into());
        }
        if self.schema_writer.get().is_some() && self.schema_writer.get() != transaction {
            return Err(SchemaMutationError::SchemaBusy.into());
        }
        if let Some(journal) = &self.mutation_journal {
            journal.borrow().ensure_ready()?;
        }
        Ok(())
    }

    pub(crate) fn finish_schema_commit(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        if transaction.state() != TransactionState::FinalizePending {
            return Err(SchemaMutationError::RecoveryRequired.into());
        }
        if transaction
            .schema_mutation
            .as_ref()
            .is_some_and(|mutation| mutation.drop.is_some())
        {
            return self.finish_drop_schema_commit(transaction);
        }
        if transaction
            .schema_mutation
            .as_ref()
            .is_some_and(|mutation| mutation.rewrite.is_some())
        {
            return self.finish_rewrite_schema_commit(transaction);
        }
        let mutation = transaction
            .schema_mutation
            .as_mut()
            .ok_or(SchemaMutationError::Corrupt("schema participant absent"))?;
        if let Some(storage) = &mutation.staged {
            storage.flush()?;
        }
        // Close path-bearing storage handles before promotion. The transaction's
        // physical context is already terminal and is removed separately below.
        mutation.staged.take();
        let catalog = mutation.catalog.clone();
        let reservation = mutation.reservation.clone();
        let target = mutation.target.clone();
        let reference = mutation.reference.clone();
        transaction.release_staged_context(reservation.storage);
        promote(&catalog, &reservation, &reference)?;
        crash("promotion-complete");
        let final_path = file::resolve(
            &catalog,
            &final_locator(&catalog, target.incarnation, reservation.storage)?,
        );
        let storage = open_winner_heap(
            &final_path,
            &reservation,
            &transaction.coordinator_decisions()?,
        )?;
        storage.flush()?;
        if self.registry.get(reservation.storage).is_some()
            || self
                .committed
                .schema
                .tables()
                .iter()
                .any(|t| t.id == reservation.table)
        {
            return Err(SchemaMutationError::Corrupt("publication identity collision").into());
        }
        let revision = self.catalog_generation.checked_add(1).ok_or(
            SchemaMutationError::IdentityExhausted("runtime catalog revision"),
        )?;
        crash("before-nbsc-publication");
        let published = file::publish_runtime(&catalog, &target)?;
        transaction.finish_schema_decision()?;
        if let Some(mutation) = &transaction.schema_mutation {
            mutation
                .journal
                .borrow_mut()
                .resolve(reservation.transaction, true)?;
        }
        cleanup_prepared(&catalog, &reservation, target.incarnation)?;
        crash("before-memory-publish");
        // All validation, I/O and allocation above; exclusive synchronous worker
        // publication has no observable mixed schema/binding/registry interval.
        self.registry.publish_created(storage);
        self.bindings.publish_created(TablePlacement::Single {
            table_id: reservation.table,
            storage_id: reservation.storage,
        });
        self.committed = published.committed;
        self.catalog_generation = revision;
        self.coordinator = transaction.shared_coordinator();
        self.schema_writer.set(None);
        transaction.complete_schema_publication();
        crash("after-memory-publish");
        crash("before-api-return");
        Ok(())
    }

    fn finish_rewrite_schema_commit(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        let mutation = transaction
            .schema_mutation
            .as_mut()
            .ok_or(SchemaMutationError::Corrupt("schema participant absent"))?;
        let rewrite = mutation
            .rewrite
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt("rewrite participant absent"))?
            .clone();
        if let Some(storage) = &mutation.staged {
            storage.flush()?;
        }
        mutation.staged.take();
        let catalog = mutation.catalog.clone();
        let reservation = mutation.reservation.clone();
        let target = mutation.target.clone();
        let reference = mutation.reference.clone();
        let journal = Rc::clone(&mutation.journal);
        transaction.release_staged_context(reservation.storage);
        promote(&catalog, &reservation, &reference)?;
        crash("rewrite-promotion-complete");
        let final_path = file::resolve(
            &catalog,
            &final_locator(&catalog, target.incarnation, reservation.storage)?,
        );
        let storage = open_winner_heap(
            &final_path,
            &reservation,
            &transaction.coordinator_decisions()?,
        )?;
        storage.flush()?;
        validate_rewrite_source(&catalog, &rewrite)?;
        crash("rewrite-final-heap-synced");
        if self.registry.get(reservation.storage).is_some()
            || self.registry.get(rewrite.old_storage()).is_none()
        {
            return Err(
                SchemaMutationError::Corrupt("rewrite publication identity collision").into(),
            );
        }
        let revision = self.catalog_generation.checked_add(1).ok_or(
            SchemaMutationError::IdentityExhausted("runtime catalog revision"),
        )?;
        journal
            .borrow_mut()
            .retire_rewrite(rewrite.reservation.transaction)?;
        crash("rewrite-retirement-durable");
        crash("before-nbsc-publication");
        let published = file::publish_runtime(&catalog, &target)?;
        crash("rewrite-nbsc-durable");
        crash("rewrite-before-coordinator-complete");
        transaction.finish_schema_decision()?;
        crash("rewrite-coordinator-complete");
        crash("rewrite-before-winner-resolution");
        journal
            .borrow_mut()
            .resolve_rewrite(rewrite.reservation.transaction, true)?;
        crash("rewrite-winner-resolved");
        cleanup_prepared(&catalog, &reservation, target.incarnation)?;
        crash("before-memory-publish");
        let old = self
            .registry
            .publish_replaced(rewrite.old_storage(), storage)
            .ok_or(SchemaMutationError::Corrupt("rewrite source disappeared"))?;
        self.bindings
            .publish_replaced(rewrite.table(), rewrite.new_storage());
        self.committed = published.committed;
        self.catalog_generation = revision;
        self.coordinator = transaction.shared_coordinator();
        self.schema_writer.set(None);
        transaction.complete_schema_publication();
        drop(old);
        crash("after-memory-publish");
        crash("before-api-return");
        Ok(())
    }

    fn finish_drop_schema_commit(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        let mutation = transaction
            .schema_mutation
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt("schema participant absent"))?;
        let intent = mutation
            .drop
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt("drop participant absent"))?
            .clone();
        let catalog = mutation.catalog.clone();
        let target = mutation.target.clone();
        let journal = Rc::clone(&mutation.journal);
        validate_drop_resource(&catalog, &intent)?;
        if self
            .registry
            .get(intent.storage())
            .is_none_or(|storage| storage.table().id != intent.table())
        {
            return Err(SchemaMutationError::Corrupt("drop publication storage absent").into());
        }
        let revision = self.catalog_generation.checked_add(1).ok_or(
            SchemaMutationError::IdentityExhausted("runtime catalog revision"),
        )?;
        journal.borrow_mut().retire_drop(intent.transaction)?;
        crash("drop-retirement-durable");
        crash("before-nbsc-publication");
        let published = file::publish_runtime(&catalog, &target)?;
        crash("drop-nbsc-durable");
        transaction.finish_schema_decision()?;
        journal
            .borrow_mut()
            .resolve_drop(intent.transaction, true)?;
        cleanup_drop_prepared(&catalog, &intent)?;
        crash("before-memory-publish");
        // The synchronous Database owner exposes no callback between these
        // infallible moves, so schema, binding and registry change together.
        let retired = self
            .registry
            .publish_dropped(intent.storage())
            .ok_or(SchemaMutationError::Corrupt("retired storage disappeared"))?;
        self.bindings.publish_dropped(intent.table());
        self.committed = published.committed;
        self.catalog_generation = revision;
        self.coordinator = transaction.shared_coordinator();
        self.schema_writer.set(None);
        transaction.complete_schema_publication();
        drop(retired);
        crash("after-memory-publish");
        crash("before-api-return");
        Ok(())
    }
}

// Generated private locators must not follow an ancestor symlink when creating,
// promoting or deleting a known artifact. Existing external catalog locators keep
// their Round 17 semantics; this check covers only the owned resource namespace.
fn validate_resource_path(catalog: &Path, resource: &Path) -> Result<(), SchemaMutationError> {
    let root = catalog
        .parent()
        .ok_or(SchemaMutationError::Corrupt("catalog has no parent"))?;
    let relative = resource
        .strip_prefix(root)
        .map_err(|_| SchemaMutationError::Corrupt("private resource escapes catalog root"))?;
    let mut path = root.to_owned();
    let components = relative.components().collect::<Vec<_>>();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(SchemaMutationError::Corrupt(
                "invalid private resource locator",
            ));
        }
        path.push(component.as_os_str());
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
            Ok(_) => return Err(SchemaCatalogError::PathConflict(path).into()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => return Err(file::io("inspect private resource ancestor", &path, e).into()),
        }
    }
    Ok(())
}

pub(crate) fn ensure_parent(path: &Path) -> Result<(), SchemaMutationError> {
    let parent = path
        .parent()
        .ok_or(SchemaMutationError::Corrupt("resource has no parent"))?;
    match std::fs::symlink_metadata(parent) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(SchemaCatalogError::PathConflict(parent.to_owned()).into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            ensure_parent(parent)?;
            std::fs::create_dir(parent)
                .map_err(|e| file::io("create schema resource directory", parent, e))?;
            file::sync_parent(parent)?;
            Ok(())
        }
        Err(e) => Err(file::io("inspect schema resource directory", parent, e).into()),
    }
}
fn owner_bytes(
    incarnation: [u8; 16],
    txn: DatabaseTxnId,
    table: TableId,
    storage: StorageId,
    fingerprint: SchemaFingerprint,
) -> Result<Vec<u8>, SchemaMutationError> {
    let mut w = crate::schema_catalog::Writer(incarnation.to_vec());
    w.u64(txn.0);
    w.u64(table.0);
    w.u64(storage.0);
    w.0.extend_from_slice(fingerprint.as_bytes());
    Ok(crate::schema_catalog::envelope(b"NBST", &w.0)?)
}
fn write_owner(
    path: &Path,
    incarnation: [u8; 16],
    txn: DatabaseTxnId,
    table: TableId,
    storage: StorageId,
    fingerprint: SchemaFingerprint,
) -> Result<(), SchemaMutationError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| file::io("create staged owner", path, e))?;
    file.write_all(&owner_bytes(incarnation, txn, table, storage, fingerprint)?)
        .and_then(|_| file.sync_all())
        .map_err(|e| file::io("sync staged owner", path, e))?;
    file::sync_parent(path)?;
    Ok(())
}

fn retired_resource(intent: &DropIntent) -> RetiredTableResource {
    let table = &intent.fragment.committed.schema.tables()[0];
    RetiredTableResource {
        table_id: table.id,
        table_version: intent.fragment.committed.tables[0].version,
        fingerprint: intent.fragment.placements.tables[0].schema_fingerprint,
        storage_id: intent.fragment.storages[0].id,
        engine: netbadb_storage::StorageKind::Heap,
        relative_locator: intent.fragment.storages[0].locator.clone(),
        retired_generation: intent.target_generation,
    }
}

fn replacement_resource(intent: &RewriteIntent) -> ReplacementRetiredHeap {
    ReplacementRetiredHeap {
        table_id: intent.table(),
        base_table_version: intent.base.committed.tables[0].version,
        target_table_version: intent.target.committed.tables[0].version,
        base_fingerprint: intent.base.placements.tables[0].schema_fingerprint,
        target_fingerprint: intent.target.placements.tables[0].schema_fingerprint,
        old_storage_id: intent.old_storage(),
        new_storage_id: intent.new_storage(),
        old_relative_locator: intent.base.storages[0].locator.clone(),
        replacement_transaction: intent.reservation.transaction,
        retired_generation: intent.target.committed.generation,
    }
}

#[derive(Debug, Clone)]
enum RetiredHeapIntent {
    TableDrop(Box<DropIntent>),
    SchemaRewrite(Box<RewriteIntent>),
}

impl RetiredHeapIntent {
    fn transaction(&self) -> DatabaseTxnId {
        match self {
            Self::TableDrop(intent) => intent.transaction,
            Self::SchemaRewrite(intent) => intent.reservation.transaction,
        }
    }

    fn table(&self) -> TableId {
        match self {
            Self::TableDrop(intent) => intent.table(),
            Self::SchemaRewrite(intent) => intent.table(),
        }
    }

    fn storage(&self) -> StorageId {
        match self {
            Self::TableDrop(intent) => intent.storage(),
            Self::SchemaRewrite(intent) => intent.old_storage(),
        }
    }

    fn fragment(&self) -> &SchemaCatalogSnapshot {
        match self {
            Self::TableDrop(intent) => &intent.fragment,
            Self::SchemaRewrite(intent) => &intent.base,
        }
    }

    fn relative_locator(&self) -> &str {
        &self.fragment().storages[0].locator
    }

    fn gc(&self) -> Option<&RetiredHeapGcRecord> {
        match self {
            Self::TableDrop(intent) => intent.gc.as_ref(),
            Self::SchemaRewrite(intent) => intent.gc.as_ref(),
        }
    }

    fn target(&self) -> RetiredHeapGcTarget {
        match self {
            Self::TableDrop(intent) => RetiredHeapGcTarget::TableDrop(retired_resource(intent)),
            Self::SchemaRewrite(intent) => {
                RetiredHeapGcTarget::SchemaRewrite(replacement_resource(intent))
            }
        }
    }
}

fn retired_intent_for_target(
    journal: &SchemaMutationJournal,
    target: &RetiredHeapGcTarget,
) -> Result<RetiredHeapIntent, SchemaMutationError> {
    let intent = match target {
        RetiredHeapGcTarget::TableDrop(resource) => journal
            .drops
            .values()
            .find(|drop| drop.storage() == resource.storage_id && drop.retired)
            .cloned()
            .map(|drop| RetiredHeapIntent::TableDrop(Box::new(drop))),
        RetiredHeapGcTarget::SchemaRewrite(resource) => journal
            .rewrites
            .values()
            .find(|rewrite| rewrite.old_storage() == resource.old_storage_id && rewrite.retired)
            .cloned()
            .map(|rewrite| RetiredHeapIntent::SchemaRewrite(Box::new(rewrite))),
    }
    .ok_or(SchemaMutationError::RetiredHeapNotFound(
        target.storage_id(),
    ))?;
    if intent.target() != *target {
        return Err(SchemaMutationError::RetiredHeapTargetMismatch(
            target.storage_id(),
        ));
    }
    Ok(intent)
}

fn create_transaction_for_storage(
    journal: &SchemaMutationJournal,
    storage: StorageId,
) -> Option<DatabaseTxnId> {
    journal
        .reservations
        .values()
        .find(|reservation| reservation.storage == storage && reservation.resolved == Some(true))
        .map(|reservation| reservation.transaction)
        .or_else(|| {
            journal
                .rewrites
                .values()
                .find(|rewrite| rewrite.new_storage() == storage && rewrite.resolved == Some(true))
                .map(|rewrite| rewrite.reservation.transaction)
        })
}

fn gc_components(
    catalog: &Path,
    intent: &RetiredHeapIntent,
) -> Result<Vec<RetiredHeapGcComponent>, SchemaMutationError> {
    let heap = file::resolve(catalog, intent.relative_locator());
    validate_resource_path(catalog, &heap)?;
    let mut components = vec![RetiredHeapGcComponent {
        kind: RetiredHeapGcComponentKind::Owner,
        path: file::suffix(&heap, ".owner"),
        required: true,
        present: false,
        bytes: 0,
    }];
    components.extend(
        netbadb_storage::heap_resource_components(&heap)
            .into_iter()
            .map(|component| RetiredHeapGcComponent {
                kind: match component.kind {
                    netbadb_storage::HeapResourceComponentKind::Main => {
                        RetiredHeapGcComponentKind::Main
                    }
                    netbadb_storage::HeapResourceComponentKind::Wal => {
                        RetiredHeapGcComponentKind::Wal
                    }
                    netbadb_storage::HeapResourceComponentKind::TransactionStatus => {
                        RetiredHeapGcComponentKind::TransactionStatus
                    }
                    netbadb_storage::HeapResourceComponentKind::AlternateWal => {
                        RetiredHeapGcComponentKind::AlternateWal
                    }
                },
                path: component.path,
                required: component.required,
                present: false,
                bytes: 0,
            }),
    );
    let link = file::link_path(&heap);
    components.push(RetiredHeapGcComponent {
        kind: RetiredHeapGcComponentKind::CatalogLink,
        path: link.clone(),
        required: true,
        present: false,
        bytes: 0,
    });
    components.push(RetiredHeapGcComponent {
        kind: RetiredHeapGcComponentKind::CatalogLinkShadow,
        path: file::suffix(&link, ".next"),
        required: false,
        present: false,
        bytes: 0,
    });
    for component in &components {
        validate_resource_path(catalog, &component.path)?;
    }
    Ok(components)
}

fn gc_manifest_digest(
    catalog: &Path,
    components: &[RetiredHeapGcComponent],
) -> Result<[u8; 32], SchemaMutationError> {
    let root = catalog
        .parent()
        .ok_or(SchemaMutationError::Corrupt("catalog has no parent"))?;
    let mut digest = Sha256::new();
    digest.update(b"NetbaDB retired Heap bundle v1\0");
    for component in components {
        digest.update([component.kind as u8, u8::from(component.required)]);
        let relative = component
            .path
            .strip_prefix(root)
            .map_err(|_| SchemaMutationError::Corrupt("GC component escapes catalog root"))?
            .to_str()
            .ok_or(SchemaMutationError::Corrupt(
                "GC component path is not UTF-8",
            ))?;
        let length = u32::try_from(relative.len())
            .map_err(|_| SchemaMutationError::Corrupt("GC component path too long"))?;
        digest.update(length.to_le_bytes());
        digest.update(relative.as_bytes());
    }
    Ok(digest.finalize().into())
}

fn component_metadata(path: &Path) -> Result<Option<u64>, SchemaMutationError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Ok(Some(metadata.len()))
        }
        Ok(_) => Err(SchemaCatalogError::PathConflict(path.to_owned()).into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(file::io("inspect retired Heap component", path, error).into()),
    }
}

fn coordinator_horizon(
    intent: &RetiredHeapIntent,
    decisions: &[crate::CoordinatorDecision],
) -> Result<(DatabaseTxnId, Vec<RetiredHeapGcBlocker>), SchemaMutationError> {
    let retirement_decision = decisions
        .iter()
        .find(|decision| decision.database_txn_id == intent.transaction())
        .ok_or(SchemaMutationError::Corrupt(
            "retirement decision is missing",
        ))?;
    let reference = retirement_decision
        .schema
        .as_ref()
        .ok_or(SchemaMutationError::Corrupt(
            "retirement has no schema decision",
        ))?;
    match intent {
        RetiredHeapIntent::TableDrop(drop) => validate_drop_reference(drop, reference)?,
        RetiredHeapIntent::SchemaRewrite(rewrite) => {
            validate_rewrite_reference(rewrite, reference)?
        }
    }
    let mut highest = intent.transaction().0;
    let mut blockers = Vec::new();
    if !retirement_decision.complete {
        blockers.push(RetiredHeapGcBlocker::CoordinatorDecisionIncomplete {
            transaction: retirement_decision.database_txn_id,
        });
    }
    for decision in decisions {
        if decision
            .participants
            .iter()
            .any(|participant| participant.storage_id == intent.storage())
        {
            highest = highest.max(decision.database_txn_id.0);
            if !decision.complete {
                blockers.push(RetiredHeapGcBlocker::CoordinatorDecisionIncomplete {
                    transaction: decision.database_txn_id,
                });
            }
        }
    }
    Ok((DatabaseTxnId(highest), blockers))
}

fn validate_gc_horizon(
    intent: &RetiredHeapIntent,
    decisions: &[crate::CoordinatorDecision],
    expected: DatabaseTxnId,
) -> Result<(), SchemaMutationError> {
    let (actual, blockers) = coordinator_horizon(intent, decisions)?;
    if actual != expected || !blockers.is_empty() {
        return Err(SchemaMutationError::Corrupt(
            "retired Heap coordinator horizon changed or is incomplete",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapIncarnation {
    table: TableId,
    version: TableSchemaVersion,
    fingerprint: SchemaFingerprint,
    storage: StorageId,
}

fn fragment_incarnation(snapshot: &SchemaCatalogSnapshot) -> HeapIncarnation {
    HeapIncarnation {
        table: snapshot.committed.schema.tables()[0].id,
        version: snapshot.committed.tables[0].version,
        fingerprint: snapshot.placements.tables[0].schema_fingerprint,
        storage: snapshot.storages[0].id,
    }
}

fn active_incarnation(
    snapshot: &SchemaCatalogSnapshot,
    table: TableId,
) -> Result<Option<HeapIncarnation>, SchemaMutationError> {
    let Some(lineage) = snapshot
        .committed
        .tables
        .iter()
        .find(|lineage| lineage.table_id == table)
    else {
        return Ok(None);
    };
    let placement = snapshot
        .placements
        .tables
        .iter()
        .find(|placement| placement.table_id == table)
        .ok_or(SchemaMutationError::Corrupt(
            "active table has no physical placement",
        ))?;
    let mut storages = placement.placement.storage_ids();
    let storage = storages
        .next()
        .ok_or(SchemaMutationError::Corrupt("active table has no storage"))?;
    if storages.next().is_some() {
        return Err(SchemaMutationError::Corrupt(
            "retired Heap lineage reached partitioned active storage",
        ));
    }
    Ok(Some(HeapIncarnation {
        table,
        version: lineage.version,
        fingerprint: placement.schema_fingerprint,
        storage,
    }))
}

fn validate_retirement_lineage(
    active: &SchemaCatalogSnapshot,
    journal: &SchemaMutationJournal,
    intent: &RetiredHeapIntent,
) -> Result<(), SchemaMutationError> {
    match intent {
        RetiredHeapIntent::TableDrop(drop) => {
            if active_incarnation(active, drop.table())?.is_some() {
                return Err(SchemaMutationError::Corrupt(
                    "dropped TableId remains active",
                ));
            }
        }
        RetiredHeapIntent::SchemaRewrite(rewrite) => {
            if !rewrite.retired || rewrite.resolved != Some(true) {
                return Err(SchemaMutationError::Corrupt(
                    "replacement retirement is not terminal",
                ));
            }
            let mut current = fragment_incarnation(&rewrite.target);
            let mut transaction = rewrite.reservation.transaction;
            loop {
                if let Some(next) = journal.rewrites.values().find(|candidate| {
                    candidate.old_storage() == current.storage
                        && candidate.retired
                        && candidate.resolved == Some(true)
                }) {
                    if next.reservation.transaction.0 <= transaction.0
                        || fragment_incarnation(&next.base) != current
                    {
                        return Err(SchemaMutationError::Corrupt(
                            "replacement rewrite lineage is inconsistent",
                        ));
                    }
                    current = fragment_incarnation(&next.target);
                    transaction = next.reservation.transaction;
                    continue;
                }
                if let Some(drop) = journal.drops.values().find(|candidate| {
                    candidate.storage() == current.storage
                        && candidate.retired
                        && candidate.resolved == Some(true)
                }) {
                    if drop.transaction.0 <= transaction.0
                        || fragment_incarnation(&drop.fragment) != current
                        || active_incarnation(active, current.table)?.is_some()
                    {
                        return Err(SchemaMutationError::Corrupt(
                            "replacement-to-drop lineage is inconsistent",
                        ));
                    }
                    return Ok(());
                }
                if active_incarnation(active, current.table)? != Some(current) {
                    return Err(SchemaMutationError::Corrupt(
                        "replacement lineage does not reach active storage",
                    ));
                }
                return Ok(());
            }
        }
    }
    Ok(())
}

fn inspect_gc(
    catalog: &Path,
    intent: &RetiredHeapIntent,
    create_transaction: Option<DatabaseTxnId>,
    decisions: &[crate::CoordinatorDecision],
    database: &Database,
) -> Result<RetiredHeapGcInspection, DatabaseError> {
    if intent.relative_locator()
        != final_locator(catalog, intent.fragment().incarnation, intent.storage())?
    {
        return Ok(RetiredHeapGcInspection {
            target: intent.target(),
            state: RetiredHeapGcState::Retained,
            coordinator_horizon: None,
            manifest_digest: [0; 32],
            components: Vec::new(),
            blockers: vec![RetiredHeapGcBlocker::UnsupportedLocator],
            total_present_bytes: 0,
        });
    }
    let create_transaction = create_transaction.ok_or(SchemaMutationError::Corrupt(
        "runtime retired Heap has no create history",
    ))?;
    let active = file::load(catalog)?;
    if active
        .storages
        .iter()
        .any(|storage| storage.id == intent.storage())
        || database.registry.get(intent.storage()).is_some()
        || database.bindings.iter().any(|placement| {
            placement
                .storage_ids()
                .any(|storage| storage == intent.storage())
        })
    {
        return Err(SchemaMutationError::Corrupt("active state references retired Heap").into());
    }
    {
        let journal = database
            .mutation_journal
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt(
                "retirement history disappeared",
            ))?
            .borrow();
        validate_retirement_lineage(&active, &journal, intent)?;
    }
    let mut components = gc_components(catalog, intent)?;
    let manifest_digest = gc_manifest_digest(catalog, &components)?;
    let state = match intent.gc() {
        None => RetiredHeapGcState::Retained,
        Some(gc) if gc.complete => RetiredHeapGcState::Deleted,
        Some(_) => RetiredHeapGcState::Deleting,
    };
    let (horizon, mut blockers) = coordinator_horizon(intent, decisions)?;
    if let Some(gc) = intent.gc() {
        if gc.manifest_digest != manifest_digest {
            return Err(SchemaMutationError::Corrupt("retired Heap GC manifest changed").into());
        }
        validate_gc_horizon(intent, decisions, gc.coordinator_horizon)?;
    }
    if state == RetiredHeapGcState::Retained {
        if database.schema_writer.get().is_some() {
            blockers.push(RetiredHeapGcBlocker::SchemaWriter);
        }
        let handles = Rc::strong_count(&database.transaction_owner).saturating_sub(1);
        if handles != 0 {
            blockers.push(RetiredHeapGcBlocker::ActiveTransactionHandles {
                count: handles as u64,
            });
        }
    }
    let mut total = 0_u64;
    for component in &mut components {
        if let Some(bytes) = component_metadata(&component.path)? {
            component.present = true;
            component.bytes = bytes;
            total = total
                .checked_add(bytes)
                .ok_or(SchemaMutationError::Corrupt("GC byte count overflow"))?;
        } else if component.required && state == RetiredHeapGcState::Retained {
            blockers.push(RetiredHeapGcBlocker::RequiredComponentMissing {
                kind: component.kind,
            });
        }
    }
    if state == RetiredHeapGcState::Deleted && components.iter().any(|component| component.present)
    {
        return Err(SchemaMutationError::Corrupt("deleted Heap component reappeared").into());
    }
    if state == RetiredHeapGcState::Retained {
        let heap = file::resolve(catalog, intent.relative_locator());
        if components
            .iter()
            .find(|component| component.kind == RetiredHeapGcComponentKind::Owner)
            .is_some_and(|component| component.present)
            && file::read(&file::suffix(&heap, ".owner"))?
                != owner_bytes(
                    intent.fragment().incarnation,
                    create_transaction,
                    intent.table(),
                    intent.storage(),
                    intent.fragment().placements.tables[0].schema_fingerprint,
                )?
        {
            return Err(SchemaMutationError::Corrupt("retired Heap owner mismatch").into());
        }
        if components
            .iter()
            .find(|component| component.kind == RetiredHeapGcComponentKind::Main)
            .is_some_and(|component| component.present)
        {
            validate_retired_resource(catalog, intent)?;
            let recovery = TableStorage::inspect_heap_recovery(
                &heap,
                &intent.fragment().committed.schema.tables()[0],
            )?;
            blockers.extend(
                recovery
                    .prepared_transactions
                    .iter()
                    .filter(|prepared| {
                        prepared.state == netbadb_storage::PreparedTransactionState::Prepared
                    })
                    .map(|prepared| RetiredHeapGcBlocker::HeapRecoveryPending {
                        transaction: prepared.database_txn_id,
                    }),
            );
        }
        let link = file::link_path(&heap);
        if component_metadata(&link)?.is_some() && file::discover(&heap)? != catalog {
            return Err(SchemaMutationError::Corrupt("retired Heap catalog link mismatch").into());
        }
    }
    Ok(RetiredHeapGcInspection {
        target: intent.target(),
        state,
        coordinator_horizon: Some(intent.gc().map_or(horizon, |gc| gc.coordinator_horizon)),
        manifest_digest,
        components,
        blockers,
        total_present_bytes: total,
    })
}

fn resume_gc_intent(
    catalog: &Path,
    journal: &mut SchemaMutationJournal,
    intent: &RetiredHeapIntent,
    decisions: &[crate::CoordinatorDecision],
) -> Result<(), DatabaseError> {
    let gc = intent
        .gc()
        .or_else(|| {
            journal
                .drops
                .get(&intent.transaction())
                .and_then(|drop| drop.gc.as_ref())
        })
        .or_else(|| {
            journal
                .rewrites
                .get(&intent.transaction())
                .and_then(|rewrite| rewrite.gc.as_ref())
        })
        .ok_or(SchemaMutationError::Corrupt(
            "GC deletion without durable intent",
        ))?
        .clone();
    let components = gc_components(catalog, intent)?;
    if gc_manifest_digest(catalog, &components)? != gc.manifest_digest {
        return Err(SchemaMutationError::Corrupt("retired Heap GC manifest changed").into());
    }
    validate_gc_horizon(intent, decisions, gc.coordinator_horizon)?;
    let active = file::load(catalog)?;
    if active
        .storages
        .iter()
        .any(|storage| storage.id == intent.storage())
    {
        return Err(SchemaMutationError::Corrupt("GC target returned to active catalog").into());
    }
    validate_retirement_lineage(&active, journal, intent)?;
    let present = components
        .iter()
        .map(|component| component_metadata(&component.path))
        .collect::<Result<Vec<_>, _>>()?;
    if gc.complete {
        if present.iter().any(Option::is_some) {
            return Err(SchemaMutationError::Corrupt("deleted Heap component reappeared").into());
        }
        return Ok(());
    }
    let heap = file::resolve(catalog, intent.relative_locator());
    if present.first().is_some_and(|component| component.is_some()) {
        let create_transaction = create_transaction_for_storage(journal, intent.storage()).ok_or(
            SchemaMutationError::Corrupt("runtime retired Heap has no create history"),
        )?;
        if file::read(&file::suffix(&heap, ".owner"))?
            != owner_bytes(
                intent.fragment().incarnation,
                create_transaction,
                intent.table(),
                intent.storage(),
                intent.fragment().placements.tables[0].schema_fingerprint,
            )?
        {
            return Err(SchemaMutationError::Corrupt("retired Heap owner mismatch").into());
        }
    }
    if present.get(1).is_some_and(|component| component.is_some()) {
        validate_retired_resource(catalog, intent)?;
    }
    if present.get(5).is_some_and(|component| component.is_some())
        && file::discover(&heap)? != catalog
    {
        return Err(SchemaMutationError::Corrupt("retired Heap catalog link mismatch").into());
    }
    crash("gc-before-first-delete");
    const CRASH_POINTS: [&str; 7] = [
        "gc-after-owner-delete",
        "gc-after-main-delete",
        "gc-after-wal-delete",
        "gc-after-status-delete",
        "gc-after-alternate-delete",
        "gc-after-link-delete",
        "gc-after-link-shadow-delete",
    ];
    for ((component, exists), crash_point) in components.iter().zip(present).zip(CRASH_POINTS) {
        if exists.is_some() {
            std::fs::remove_file(&component.path).map_err(|error| {
                file::io("delete retired Heap component", &component.path, error)
            })?;
        }
        crash(crash_point);
    }
    let first = components
        .first()
        .ok_or(SchemaMutationError::Corrupt("empty GC component manifest"))?;
    file::sync_parent(&first.path)?;
    crash("gc-directory-synced");
    journal.complete_gc(intent.transaction())?;
    crash("gc-complete-durable");
    Ok(())
}

fn components(heap: &Path) -> Vec<(PathBuf, bool)> {
    let wal = netbadb_storage::wal_path(heap);
    vec![
        (file::suffix(heap, ".owner"), true),
        (heap.to_owned(), true),
        (wal.clone(), true),
        (netbadb_storage::txn_status_path(heap), true),
        (netbadb_storage::wal_alternate_path(wal), false),
    ]
}
fn exists_file(path: &Path) -> Result<bool, SchemaMutationError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(SchemaCatalogError::PathConflict(path.to_owned()).into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(file::io("inspect schema component", path, e).into()),
    }
}

fn validate_retired_resource(
    catalog: &Path,
    intent: &RetiredHeapIntent,
) -> Result<(), DatabaseError> {
    match intent {
        RetiredHeapIntent::TableDrop(drop) => validate_drop_resource(catalog, drop),
        RetiredHeapIntent::SchemaRewrite(rewrite) => validate_rewrite_source(catalog, rewrite),
    }
}

fn validate_drop_resource(catalog: &Path, intent: &DropIntent) -> Result<(), DatabaseError> {
    let descriptor = &intent.fragment.storages[0];
    if !matches!(descriptor.kind, CatalogStorageKind::Heap) {
        return Err(SchemaMutationError::UnsupportedPlacement.into());
    }
    let path = file::resolve(catalog, &descriptor.locator);
    let identity = TableStorage::inspect_heap_identity(&path)?;
    let recovery =
        TableStorage::inspect_heap_recovery(&path, &intent.fragment.committed.schema.tables()[0])?;
    if identity.storage_id != descriptor.id
        || recovery.storage_id != descriptor.id
        || identity.table_id != descriptor.table_id
        || identity.schema_fingerprint != intent.fragment.placements.tables[0].schema_fingerprint
    {
        return Err(SchemaMutationError::Corrupt("retained Heap identity mismatch").into());
    }
    Ok(())
}
fn validate_rewrite_source(catalog: &Path, intent: &RewriteIntent) -> Result<(), DatabaseError> {
    let descriptor = &intent.base.storages[0];
    if !matches!(descriptor.kind, CatalogStorageKind::Heap) || descriptor.id == intent.new_storage()
    {
        return Err(SchemaMutationError::Corrupt("invalid replacement-retired identity").into());
    }
    let path = file::resolve(catalog, &descriptor.locator);
    let table = &intent.base.committed.schema.tables()[0];
    let identity = TableStorage::inspect_heap_identity(&path)?;
    let recovery = TableStorage::inspect_heap_recovery(&path, table)?;
    if identity.storage_id != descriptor.id
        || recovery.storage_id != descriptor.id
        || identity.table_id != intent.table()
        || identity.schema_fingerprint != intent.base.placements.tables[0].schema_fingerprint
    {
        return Err(SchemaMutationError::Corrupt("replacement-retired Heap mismatch").into());
    }
    Ok(())
}
fn validate_intent(
    catalog: &Path,
    reservation: &Reservation,
    reference: &SchemaParticipantReference,
) -> Result<(), SchemaMutationError> {
    let intent = reservation
        .intent
        .as_ref()
        .ok_or(SchemaMutationError::Corrupt("winner has no create intent"))?;
    if reference.incarnation != intent.fragment.incarnation
        || reference.target_epoch != intent.fragment.epoch
        || reference.digest != intent.snapshot_digest
        || intent.fragment.storages[0].locator
            != final_locator(catalog, reference.incarnation, reservation.storage)?
    {
        return Err(SchemaMutationError::Corrupt(
            "coordinator/create intent mismatch",
        ));
    }
    Ok(())
}
fn promote(
    catalog: &Path,
    reservation: &Reservation,
    reference: &SchemaParticipantReference,
) -> Result<(), DatabaseError> {
    validate_intent(catalog, reservation, reference)?;
    let intent = reservation
        .intent
        .as_ref()
        .ok_or(SchemaMutationError::Corrupt("missing intent"))?;
    let stage = file::resolve(
        catalog,
        &stage_locator(
            catalog,
            reference.incarnation,
            reservation.transaction,
            reservation.storage,
        )?,
    );
    let final_path = file::resolve(
        catalog,
        &final_locator(catalog, reference.incarnation, reservation.storage)?,
    );
    validate_resource_path(catalog, &stage)?;
    validate_resource_path(catalog, &final_path)?;
    ensure_parent(&final_path)?;
    let expected_owner = owner_bytes(
        reference.incarnation,
        reservation.transaction,
        reservation.table,
        reservation.storage,
        intent.fragment.placements.tables[0].schema_fingerprint,
    )?;
    let source_owner = file::suffix(&stage, ".owner");
    let final_owner = file::suffix(&final_path, ".owner");
    let owner = if exists_file(&source_owner)? {
        &source_owner
    } else {
        &final_owner
    };
    if file::read(owner)? != expected_owner {
        return Err(SchemaMutationError::Corrupt("staged owner identity mismatch").into());
    }
    let heap = if exists_file(&stage)? {
        &stage
    } else {
        &final_path
    };
    validate_heap_identity(heap, reservation)?;
    for (position, ((source, required), (destination, _))) in components(&stage)
        .into_iter()
        .zip(components(&final_path))
        .enumerate()
    {
        let source_exists = exists_file(&source)?;
        let destination_exists = exists_file(&destination)?;
        match (source_exists, destination_exists) {
            (true, true) => return Err(SchemaCatalogError::PathConflict(destination).into()),
            (false, false) if required => {
                return Err(
                    SchemaMutationError::Corrupt("winner physical component is missing").into(),
                );
            }
            (true, false) => {
                std::fs::rename(&source, &destination)
                    .map_err(|e| file::io("promote staged Heap component", &destination, e))?;
                file::sync_parent(&destination)?;
                file::sync_parent(&source)?;
            }
            _ => {}
        }
        if position == 1 {
            crash("promotion-partial");
        }
    }
    validate_heap_identity(&final_path, reservation)?;
    Ok(())
}
fn validate_heap_identity(path: &Path, reservation: &Reservation) -> Result<(), DatabaseError> {
    let identity = TableStorage::inspect_heap_identity(path)?;
    let intent = reservation
        .intent
        .as_ref()
        .ok_or(SchemaMutationError::Corrupt(
            "physical validation without intent",
        ))?;
    if identity.storage_id != reservation.storage
        || identity.table_id != reservation.table
        || identity.schema_fingerprint != intent.fragment.placements.tables[0].schema_fingerprint
    {
        return Err(SchemaMutationError::Corrupt("staged/final Heap identity mismatch").into());
    }
    Ok(())
}
fn open_winner_heap(
    path: &Path,
    reservation: &Reservation,
    decisions: &[crate::CoordinatorDecision],
) -> Result<TableStorage, DatabaseError> {
    validate_heap_identity(path, reservation)?;
    let intent = reservation
        .intent
        .as_ref()
        .ok_or(SchemaMutationError::Corrupt("missing winner intent"))?;
    let table = &intent.fragment.committed.schema.tables()[0];
    let recovery = TableStorage::inspect_heap_recovery(path, table)?;
    let decision = decisions
        .iter()
        .find(|d| d.database_txn_id == reservation.transaction)
        .ok_or(SchemaMutationError::Corrupt("missing winner decision"))?;
    if !decision.complete
        && !recovery.prepared_transactions.iter().any(|p| {
            p.database_txn_id == reservation.transaction
                && decision.participants.iter().any(|d| {
                    d.storage_id == reservation.storage && d.physical_txn_id == p.physical_txn_id
                })
        })
    {
        return Err(SchemaMutationError::Corrupt("winner Heap prepare is missing").into());
    }
    let resolutions = recovery
        .prepared_transactions
        .iter()
        .map(|p| crate::resolution_for_prepared(p, reservation.storage, decisions))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TableStorage::open_heap_with_prepared_resolutions(
        path,
        table.clone(),
        &resolutions,
    )?)
}
fn remove_file(path: &Path) -> Result<(), SchemaMutationError> {
    if exists_file(path)? {
        std::fs::remove_file(path)
            .map_err(|e| file::io("remove private schema artifact", path, e))?;
        file::sync_parent(path)?;
    }
    Ok(())
}
fn cleanup_prepared(
    catalog: &Path,
    reservation: &Reservation,
    incarnation: [u8; 16],
) -> Result<(), SchemaMutationError> {
    let prepared = file::resolve(
        catalog,
        &prepared_locator(catalog, incarnation, reservation.transaction)?,
    );
    validate_resource_path(catalog, &prepared)?;
    remove_file(&prepared)?;
    remove_file(&file::suffix(&prepared, ".next"))?;
    // Only the exact transaction directory is removed, never a scanned tree.
    if let Some(parent) = prepared.parent() {
        match std::fs::remove_dir(parent) {
            Ok(()) => file::sync_parent(parent)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(file::io("remove private schema directory", parent, e).into()),
        }
    }
    Ok(())
}
fn cleanup_drop_prepared(catalog: &Path, intent: &DropIntent) -> Result<(), SchemaMutationError> {
    let prepared = file::resolve(
        catalog,
        &prepared_locator(catalog, intent.fragment.incarnation, intent.transaction)?,
    );
    validate_resource_path(catalog, &prepared)?;
    remove_file(&prepared)?;
    remove_file(&file::suffix(&prepared, ".next"))?;
    if let Some(parent) = prepared.parent() {
        match std::fs::remove_dir(parent) {
            Ok(()) => file::sync_parent(parent)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
            Err(error) => {
                return Err(file::io("remove private drop directory", parent, error).into());
            }
        }
    }
    Ok(())
}
fn cleanup_loser(
    catalog: &Path,
    reservation: &Reservation,
    incarnation: [u8; 16],
) -> Result<(), SchemaMutationError> {
    let final_path = file::resolve(
        catalog,
        &final_locator(catalog, incarnation, reservation.storage)?,
    );
    validate_resource_path(catalog, &final_path)?;
    for (path, _) in components(&final_path) {
        if exists_file(&path)? {
            return Err(SchemaMutationError::Corrupt(
                "loser has a final physical resource",
            ));
        }
    }
    let stage = file::resolve(
        catalog,
        &stage_locator(
            catalog,
            incarnation,
            reservation.transaction,
            reservation.storage,
        )?,
    );
    validate_resource_path(catalog, &stage)?;
    for (path, _) in components(&stage) {
        remove_file(&path)?;
    }
    crash("rollback-cleanup");
    cleanup_prepared(catalog, reservation, incarnation)
}

/// Resolve schema obligations before strict active NBSC/state pair validation.
pub(crate) fn recover(catalog: &Path) -> Result<Option<SchemaMutationJournal>, DatabaseError> {
    let marker = file::marker(catalog)?
        .filter(|m| m.initialized)
        .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
    let Some(mut journal) = SchemaMutationJournal::open(catalog, marker.incarnation)? else {
        return Ok(None);
    };
    let coordinator_path = file::resolve(catalog, &journal.coordinator);
    if journal.reservations.is_empty()
        && journal.drops.is_empty()
        && journal.rewrite_reservations.is_empty()
    {
        return Ok(Some(journal));
    }
    let mut coordinator = CoordinatorLog::open(&coordinator_path)?;
    let decisions = coordinator.decisions().cloned().collect::<Vec<_>>();
    for decision in decisions.iter().filter(|d| d.schema.is_some()) {
        let reference = decision
            .schema
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt("missing schema reference"))?;
        if let Some(reservation) = journal.reservations.get(&decision.database_txn_id) {
            validate_intent(catalog, reservation, reference)?;
            if reservation.resolved == Some(false)
                || !decision
                    .participants
                    .iter()
                    .any(|participant| participant.storage_id == reservation.storage)
            {
                return Err(SchemaMutationError::Corrupt(
                    "create winner contradicts journal/participants",
                )
                .into());
            }
        } else if let Some(intent) = journal.drops.get(&decision.database_txn_id) {
            validate_drop_reference(intent, reference)?;
            if intent.resolved == Some(false) {
                return Err(SchemaMutationError::Corrupt("drop loser has commit decision").into());
            }
        } else if let Some(intent) = journal.rewrites.get(&decision.database_txn_id) {
            validate_rewrite_reference(intent, reference)?;
            if intent.resolved == Some(false)
                || !decision
                    .participants
                    .iter()
                    .any(|participant| participant.storage_id == intent.new_storage())
            {
                return Err(SchemaMutationError::Corrupt(
                    "rewrite winner contradicts journal/participants",
                )
                .into());
            }
        } else {
            return Err(
                SchemaMutationError::Corrupt("schema decision has no mutation intent").into(),
            );
        }
    }
    // Startup never chooses a candidate. It only resumes or verifies deletions
    // that already crossed the durable retry-only GC-intent boundary.
    let mut durable_gc = journal
        .drops
        .values()
        .filter(|drop| drop.gc.is_some())
        .cloned()
        .map(|drop| RetiredHeapIntent::TableDrop(Box::new(drop)))
        .chain(
            journal
                .rewrites
                .values()
                .filter(|rewrite| rewrite.gc.is_some())
                .cloned()
                .map(|rewrite| RetiredHeapIntent::SchemaRewrite(Box::new(rewrite))),
        )
        .collect::<Vec<_>>();
    durable_gc.sort_by_key(RetiredHeapIntent::transaction);
    for intent in durable_gc {
        resume_gc_intent(catalog, &mut journal, &intent, &decisions)?;
    }
    let mut reservations = journal.reservations.values().cloned().collect::<Vec<_>>();
    // Finish the newest unresolved publication before validating older completed
    // history against the NBSC/state pair (which may be between two renames).
    reservations.sort_by_key(|r| (r.resolved.is_some(), r.transaction));
    for reservation in reservations {
        let decision = decisions
            .iter()
            .find(|d| d.database_txn_id == reservation.transaction);
        if let Some(decision) = decision {
            let reference = decision
                .schema
                .as_ref()
                .ok_or(SchemaMutationError::Corrupt(
                    "mutation has a storage-only decision",
                ))?;
            validate_intent(catalog, &reservation, reference)?;
            if reservation.resolved == Some(true) {
                let later_drop = journal.drops.values().find(|drop| {
                    drop.table() == reservation.table
                        && drop.storage() == reservation.storage
                        && drop.retired
                        && drop.resolved == Some(true)
                });
                let later_rewrite = journal.rewrites.values().find(|rewrite| {
                    rewrite.reservation.transaction.0 > reservation.transaction.0
                        && rewrite.table() == reservation.table
                        && rewrite.old_storage() == reservation.storage
                        && decisions.iter().any(|decision| {
                            decision.database_txn_id == rewrite.reservation.transaction
                        })
                });
                if later_drop.is_none() && later_rewrite.is_none() {
                    let active = file::load(catalog)?;
                    verify_published(&active, &reservation)?;
                }
                if !decision.complete {
                    return Err(SchemaMutationError::Corrupt(
                        "resolved winner lacks coordinator completion",
                    )
                    .into());
                }
                let final_path = file::resolve(
                    catalog,
                    &final_locator(catalog, marker.incarnation, reservation.storage)?,
                );
                let intent = reservation
                    .intent
                    .as_ref()
                    .ok_or(SchemaMutationError::Corrupt(
                        "resolved winner intent absent",
                    ))?;
                let physical_deleted = later_drop
                    .is_some_and(|drop| drop.gc.as_ref().is_some_and(|gc| gc.complete))
                    || later_rewrite
                        .is_some_and(|rewrite| rewrite.gc.as_ref().is_some_and(|gc| gc.complete));
                if !physical_deleted
                    && file::read(&file::suffix(&final_path, ".owner"))?
                        != owner_bytes(
                            marker.incarnation,
                            reservation.transaction,
                            reservation.table,
                            reservation.storage,
                            intent.fragment.placements.tables[0].schema_fingerprint,
                        )?
                {
                    return Err(SchemaMutationError::Corrupt(
                        "committed Heap owner differs from intent",
                    )
                    .into());
                }
                cleanup_prepared(catalog, &reservation, marker.incarnation)?;
                continue;
            }
            let prepared = file::resolve(
                catalog,
                &prepared_locator(catalog, marker.incarnation, reservation.transaction)?,
            );
            let bytes = file::read(&prepared)?;
            if digest(&bytes) != reference.digest {
                return Err(SchemaMutationError::Corrupt("prepared NBSC digest mismatch").into());
            }
            let target = SchemaCatalogSnapshot::decode(&bytes)?;
            verify_published(&target, &reservation)?;
            if target.epoch != reference.target_epoch
                || target.incarnation != marker.incarnation
                || target.coordinator.as_deref() != Some(&journal.coordinator)
            {
                return Err(
                    SchemaMutationError::Corrupt("prepared NBSC reference mismatch").into(),
                );
            }
            promote(catalog, &reservation, reference)?;
            // Complete ALL old and new participants with the existing physical
            // resolver before exposing target schema. Missing/corrupt winners fail.
            let database = crate::schema_catalog_api::recover_physical(catalog, &target, &[])?;
            database.close()?;
            file::publish_runtime(catalog, &target)?;
            coordinator.complete(reservation.transaction)?;
            journal.resolve(reservation.transaction, true)?;
            cleanup_prepared(catalog, &reservation, marker.incarnation)?;
        } else {
            if reservation.resolved == Some(true) {
                return Err(SchemaMutationError::Corrupt(
                    "journal winner lacks coordinator decision",
                )
                .into());
            }
            if reservation.resolved.is_none() {
                // Physical losers in old tables are undone by ordinary open next.
                // The private new Heap may be incomplete; never open/recreate it.
                cleanup_loser(catalog, &reservation, marker.incarnation)?;
                journal.resolve(reservation.transaction, false)?;
            }
        }
    }
    let mut rewrite_reservations = journal
        .rewrite_reservations
        .values()
        .cloned()
        .collect::<Vec<_>>();
    rewrite_reservations.sort_by_key(|reservation| {
        (
            journal.rewrite_losers.contains(&reservation.transaction)
                || journal
                    .rewrites
                    .get(&reservation.transaction)
                    .is_some_and(|rewrite| rewrite.resolved.is_some()),
            reservation.transaction,
        )
    });
    for rewrite_reservation in rewrite_reservations {
        let txn = rewrite_reservation.transaction;
        let decision = decisions
            .iter()
            .find(|decision| decision.database_txn_id == txn);
        let rewrite = journal.rewrites.get(&txn).cloned();
        if let Some(decision) = decision {
            let rewrite = rewrite.ok_or(SchemaMutationError::Corrupt(
                "rewrite decision has no intent",
            ))?;
            let reference = decision
                .schema
                .as_ref()
                .ok_or(SchemaMutationError::Corrupt(
                    "rewrite mutation has a storage-only decision",
                ))?;
            validate_rewrite_reference(&rewrite, reference)?;
            let reservation = rewrite_physical_reservation(&rewrite);
            if rewrite.resolved == Some(true) {
                let later_rewrite = journal.rewrites.values().any(|later| {
                    later.reservation.transaction.0 > txn.0
                        && later.table() == rewrite.table()
                        && decisions.iter().any(|decision| {
                            decision.database_txn_id == later.reservation.transaction
                        })
                });
                let later_drop = journal.drops.values().any(|drop| {
                    drop.transaction.0 > txn.0
                        && drop.table() == rewrite.table()
                        && drop.resolved == Some(true)
                });
                if !later_rewrite && !later_drop {
                    verify_rewrite_published(&file::load(catalog)?, &rewrite, false)?;
                }
                if !decision.complete || !rewrite.retired {
                    return Err(SchemaMutationError::Corrupt(
                        "resolved rewrite winner is incomplete",
                    )
                    .into());
                }
                if rewrite.gc.is_none() {
                    validate_rewrite_source(catalog, &rewrite)?;
                } else if !rewrite.gc.as_ref().is_some_and(|gc| gc.complete) {
                    return Err(SchemaMutationError::Corrupt("GC recovery did not complete").into());
                }
                cleanup_prepared(catalog, &reservation, marker.incarnation)?;
                continue;
            }
            let prepared = file::resolve(
                catalog,
                &prepared_locator(catalog, marker.incarnation, txn)?,
            );
            let bytes = file::read(&prepared)?;
            if digest(&bytes) != reference.digest {
                return Err(
                    SchemaMutationError::Corrupt("prepared rewrite NBSC digest mismatch").into(),
                );
            }
            let target = SchemaCatalogSnapshot::decode(&bytes)?;
            verify_rewrite_published(&target, &rewrite, true)?;
            promote(catalog, &reservation, reference)?;
            journal.retire_rewrite(txn)?;
            crash("rewrite-retirement-durable");
            let database = crate::schema_catalog_api::recover_physical(catalog, &target, &[])?;
            database.close()?;
            validate_rewrite_source(catalog, &rewrite)?;
            file::publish_runtime(catalog, &target)?;
            coordinator.complete(txn)?;
            journal.resolve_rewrite(txn, true)?;
            cleanup_prepared(catalog, &reservation, marker.incarnation)?;
        } else {
            if rewrite
                .as_ref()
                .is_some_and(|rewrite| rewrite.resolved == Some(true) || rewrite.retired)
            {
                return Err(SchemaMutationError::Corrupt(
                    "retired rewrite lacks coordinator decision",
                )
                .into());
            }
            if !journal.rewrite_losers.contains(&txn) {
                let reservation = rewrite.as_ref().map_or_else(
                    || rewrite_cleanup_reservation(&rewrite_reservation),
                    rewrite_physical_reservation,
                );
                cleanup_loser(catalog, &reservation, marker.incarnation)?;
                journal.resolve_rewrite(txn, false)?;
            }
        }
    }
    let mut drops = journal.drops.values().cloned().collect::<Vec<_>>();
    drops.sort_by_key(|drop| (drop.resolved.is_some(), drop.transaction));
    for intent in drops {
        let decision = decisions
            .iter()
            .find(|decision| decision.database_txn_id == intent.transaction);
        if let Some(decision) = decision {
            let reference = decision
                .schema
                .as_ref()
                .ok_or(SchemaMutationError::Corrupt(
                    "drop mutation has a storage-only decision",
                ))?;
            validate_drop_reference(&intent, reference)?;
            if intent.resolved == Some(true) {
                let active = file::load(catalog)?;
                verify_drop_published(&active, &intent, false)?;
                if !decision.complete || !intent.retired {
                    return Err(
                        SchemaMutationError::Corrupt("resolved drop winner is incomplete").into(),
                    );
                }
                if intent.gc.is_none() {
                    validate_drop_resource(catalog, &intent)?;
                }
                cleanup_drop_prepared(catalog, &intent)?;
                continue;
            }
            let prepared = file::resolve(
                catalog,
                &prepared_locator(catalog, marker.incarnation, intent.transaction)?,
            );
            let bytes = file::read(&prepared)?;
            if digest(&bytes) != reference.digest {
                return Err(
                    SchemaMutationError::Corrupt("prepared DROP NBSC digest mismatch").into(),
                );
            }
            let target = SchemaCatalogSnapshot::decode(&bytes)?;
            verify_drop_published(&target, &intent, true)?;
            validate_drop_resource(catalog, &intent)?;
            if !decision.participants.is_empty() {
                let recovery = drop_recovery_snapshot(&target, &intent)?;
                let database =
                    crate::schema_catalog_api::recover_physical(catalog, &recovery, &[])?;
                database.close()?;
            }
            journal.retire_drop(intent.transaction)?;
            crash("drop-retirement-durable");
            file::publish_runtime(catalog, &target)?;
            coordinator.complete(intent.transaction)?;
            journal.resolve_drop(intent.transaction, true)?;
            cleanup_drop_prepared(catalog, &intent)?;
        } else {
            if intent.resolved == Some(true) || intent.retired {
                return Err(SchemaMutationError::Corrupt(
                    "retired drop lacks coordinator decision",
                )
                .into());
            }
            if intent.resolved.is_none() {
                cleanup_drop_prepared(catalog, &intent)?;
                journal.resolve_drop(intent.transaction, false)?;
            }
        }
    }
    let active = file::load(catalog)?;
    for intent in journal
        .drops
        .values()
        .filter(|intent| intent.retired && intent.resolved == Some(true))
    {
        validate_retirement_lineage(
            &active,
            &journal,
            &RetiredHeapIntent::TableDrop(Box::new(intent.clone())),
        )?;
        if intent.gc.is_none() {
            validate_drop_resource(catalog, intent)?;
        } else if !intent.gc.as_ref().is_some_and(|gc| gc.complete) {
            return Err(SchemaMutationError::Corrupt("GC recovery did not complete").into());
        }
        if active
            .storages
            .iter()
            .any(|storage| storage.id == intent.storage())
        {
            return Err(
                SchemaMutationError::Corrupt("active catalog references retired storage").into(),
            );
        }
    }
    for rewrite in journal
        .rewrites
        .values()
        .filter(|rewrite| rewrite.retired && rewrite.resolved == Some(true))
    {
        validate_retirement_lineage(
            &active,
            &journal,
            &RetiredHeapIntent::SchemaRewrite(Box::new(rewrite.clone())),
        )?;
        if rewrite.gc.is_none() {
            validate_rewrite_source(catalog, rewrite)?;
        } else if !rewrite.gc.as_ref().is_some_and(|gc| gc.complete) {
            return Err(SchemaMutationError::Corrupt("GC recovery did not complete").into());
        }
        if active
            .storages
            .iter()
            .any(|storage| storage.id == rewrite.old_storage())
        {
            return Err(SchemaMutationError::Corrupt(
                "active catalog references replacement-retired storage",
            )
            .into());
        }
    }
    Ok(Some(journal))
}

fn rewrite_physical_reservation(intent: &RewriteIntent) -> Reservation {
    Reservation {
        transaction: intent.reservation.transaction,
        table: intent.table(),
        storage: intent.new_storage(),
        base_generation: intent.reservation.base_generation,
        base_epoch: intent.reservation.base_epoch,
        intent: Some(CreateIntent {
            fragment: intent.target.clone(),
            snapshot_digest: intent.snapshot_digest,
        }),
        resolved: intent.resolved,
    }
}

fn rewrite_cleanup_reservation(intent: &RewriteReservation) -> Reservation {
    Reservation {
        transaction: intent.transaction,
        table: intent.table,
        storage: intent.storage,
        base_generation: intent.base_generation,
        base_epoch: intent.base_epoch,
        intent: None,
        resolved: Some(false),
    }
}

fn validate_rewrite_reference(
    intent: &RewriteIntent,
    reference: &SchemaParticipantReference,
) -> Result<(), SchemaMutationError> {
    if reference.incarnation != intent.target.incarnation
        || reference.target_epoch != intent.target.epoch
        || reference.digest != intent.snapshot_digest
    {
        return Err(SchemaMutationError::Corrupt(
            "coordinator/rewrite intent mismatch",
        ));
    }
    Ok(())
}

fn verify_rewrite_published(
    snapshot: &SchemaCatalogSnapshot,
    intent: &RewriteIntent,
    exact: bool,
) -> Result<(), DatabaseError> {
    let target_table = &intent.target.committed.schema.tables()[0];
    let target_lineage = &intent.target.committed.tables[0];
    let target_placement = &intent.target.placements.tables[0];
    let target_storage = &intent.target.storages[0];
    let table_matches = snapshot
        .committed
        .schema
        .tables()
        .iter()
        .any(|table| table == target_table);
    let lineage_matches = snapshot
        .committed
        .tables
        .iter()
        .any(|lineage| lineage == target_lineage);
    let placement_matches = snapshot
        .placements
        .tables
        .iter()
        .any(|placement| placement == target_placement);
    let storage_matches = snapshot
        .storages
        .iter()
        .any(|storage| storage == target_storage);
    if snapshot.incarnation != intent.target.incarnation
        || if exact {
            snapshot.epoch != intent.target.epoch
                || snapshot.committed.generation != intent.target.committed.generation
        } else {
            snapshot.epoch < intent.target.epoch
                || snapshot.committed.generation < intent.target.committed.generation
        }
        || !table_matches
        || !lineage_matches
        || !placement_matches
        || !storage_matches
        || snapshot
            .storages
            .iter()
            .any(|storage| storage.id == intent.old_storage())
    {
        return Err(SchemaMutationError::Corrupt("published rewrite differs from intent").into());
    }
    Ok(())
}

fn validate_drop_reference(
    intent: &DropIntent,
    reference: &SchemaParticipantReference,
) -> Result<(), SchemaMutationError> {
    if reference.incarnation != intent.fragment.incarnation
        || reference.target_epoch != intent.target_epoch
        || reference.digest != intent.snapshot_digest
    {
        return Err(SchemaMutationError::Corrupt(
            "coordinator/drop intent mismatch",
        ));
    }
    Ok(())
}

fn verify_drop_published(
    snapshot: &SchemaCatalogSnapshot,
    intent: &DropIntent,
    exact: bool,
) -> Result<(), DatabaseError> {
    if snapshot.incarnation != intent.fragment.incarnation
        || if exact {
            snapshot.epoch != intent.target_epoch
                || snapshot.committed.generation != intent.target_generation
        } else {
            snapshot.epoch < intent.target_epoch
                || snapshot.committed.generation < intent.target_generation
        }
        || snapshot
            .committed
            .schema
            .tables()
            .iter()
            .any(|table| table.id == intent.table())
        || snapshot
            .committed
            .tables
            .iter()
            .any(|lineage| lineage.table_id == intent.table())
        || snapshot
            .placements
            .tables
            .iter()
            .any(|placement| placement.table_id == intent.table())
        || snapshot
            .storages
            .iter()
            .any(|storage| storage.id == intent.storage())
        || if exact {
            snapshot.committed.next_table_id != intent.fragment.committed.next_table_id
                || snapshot.committed.next_storage_id != intent.fragment.committed.next_storage_id
                || snapshot.committed.next_partition_id
                    != intent.fragment.committed.next_partition_id
        } else {
            !high_water_at_least(
                snapshot.committed.next_table_id.map(|id| id.0),
                intent.fragment.committed.next_table_id.map(|id| id.0),
            ) || !high_water_at_least(
                snapshot.committed.next_storage_id.map(|id| id.0),
                intent.fragment.committed.next_storage_id.map(|id| id.0),
            ) || !high_water_at_least(
                snapshot.committed.next_partition_id.map(|id| id.0),
                intent.fragment.committed.next_partition_id.map(|id| id.0),
            )
        }
    {
        return Err(SchemaMutationError::Corrupt("published DROP differs from intent").into());
    }
    Ok(())
}

fn high_water_at_least(current: Option<u64>, retired: Option<u64>) -> bool {
    match (current, retired) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(current), Some(retired)) => current >= retired,
    }
}

fn drop_recovery_snapshot(
    target: &SchemaCatalogSnapshot,
    intent: &DropIntent,
) -> Result<SchemaCatalogSnapshot, DatabaseError> {
    let mut recovery = target.clone();
    recovery
        .committed
        .schema
        .add_table(intent.fragment.committed.schema.tables()[0].clone())?;
    recovery
        .committed
        .tables
        .push(intent.fragment.committed.tables[0].clone());
    recovery
        .placements
        .tables
        .push(intent.fragment.placements.tables[0].clone());
    recovery.storages.push(intent.fragment.storages[0].clone());
    recovery.validate()?;
    Ok(recovery)
}
fn verify_published(
    snapshot: &SchemaCatalogSnapshot,
    reservation: &Reservation,
) -> Result<(), DatabaseError> {
    let intent = reservation
        .intent
        .as_ref()
        .ok_or(SchemaMutationError::Corrupt("winner intent absent"))?;
    if snapshot.committed.generation < intent.fragment.committed.generation
        || !snapshot
            .committed
            .schema
            .tables()
            .contains(&intent.fragment.committed.schema.tables()[0])
        || !snapshot.storages.contains(&intent.fragment.storages[0])
        || !snapshot
            .committed
            .tables
            .contains(&intent.fragment.committed.tables[0])
    {
        return Err(
            SchemaMutationError::Corrupt("winner catalog differs from create intent").into(),
        );
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn crash(point: &str) {
    if std::env::var("NETBADB_CREATE_CRASH_POINT").as_deref() == Ok(point)
        || std::env::var("NETBADB_DROP_CRASH_POINT").as_deref() == Ok(point)
        || std::env::var("NETBADB_GC_CRASH_POINT").as_deref() == Ok(point)
        || std::env::var("NETBADB_REWRITE_CRASH_POINT").as_deref() == Ok(point)
    {
        std::process::exit(90);
    }
}
#[cfg(not(test))]
pub(crate) fn crash(_: &str) {}
