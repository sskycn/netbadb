//! Frontend-independent transactional Heap creation. No SQL parsing occurs here.
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use netbadb_schema::{ColumnDef, DropTableTarget, Schema, SchemaFingerprint, TableDef, TypeSpec};
use netbadb_storage::TableStorage;
use netbadb_types::{
    ColumnId, DatabaseTxnId, SemanticType, StorageId, TableId, TableSchemaVersion,
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
    CreateIntent, DropIntent, Reservation, SchemaMutationJournal, final_locator, namespace,
    prepared_locator, stage_locator,
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

#[derive(Debug)]
pub enum SchemaMutationError {
    SchemaBusy,
    RecoveryRequired,
    MultipleCreatesUnsupported,
    UnsupportedConstraint,
    UnsupportedPlacement,
    TableNotFound(TableId),
    UndefinedTable(String),
    StaleSchemaDependency,
    StalePreparedStatement,
    IdentityExhausted(&'static str),
    Corrupt(&'static str),
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
            Self::UnsupportedConstraint => {
                f.write_str("runtime table constraints are not supported")
            }
            Self::UnsupportedPlacement => {
                f.write_str("runtime table mutation supports only a single Heap")
            }
            Self::TableNotFound(table) => {
                write!(f, "table identity {} is not active", table.0)
            }
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
    pub(crate) journal: SharedMutationJournal,
    pub(crate) writer: SchemaWriter,
}
impl SchemaMutation {
    pub(crate) fn transaction(&self) -> DatabaseTxnId {
        self.drop
            .as_ref()
            .map_or(self.reservation.transaction, |drop| drop.transaction)
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

    /// Lists durably retained physical resources for committed table drops.
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
    if journal.reservations.is_empty() && journal.drops.is_empty() {
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
        } else {
            return Err(
                SchemaMutationError::Corrupt("schema decision has no mutation intent").into(),
            );
        }
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
                let active = file::load(catalog)?;
                let later_drop = journal.drops.values().find(|drop| {
                    drop.table() == reservation.table
                        && drop.storage() == reservation.storage
                        && drop.retired
                        && drop.resolved == Some(true)
                });
                if later_drop.is_none() {
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
                if file::read(&file::suffix(&final_path, ".owner"))?
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
                validate_drop_resource(catalog, &intent)?;
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
        validate_drop_resource(catalog, intent)?;
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
    Ok(Some(journal))
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
    {
        std::process::exit(90);
    }
}
#[cfg(not(test))]
pub(crate) fn crash(_: &str) {}
