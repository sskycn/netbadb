//! Bootstrap/expectation adapters around the single persisted schema authority.
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use netbadb_schema::{Schema, TableDef};
use netbadb_storage::{StorageError, TableStorage};
use netbadb_types::{ColumnId, PartitionId, StorageId, TableId};

use crate::partition_catalog::{CatalogTable, PartitionCatalog, canonicalize_partitions};
use crate::registry::{RangePartitionBinding, TablePlacement};
use crate::schema_catalog::{
    CatalogStorage, CatalogStorageKind, CommittedCatalogState, SchemaCatalogError,
    SchemaCatalogSnapshot,
};
use crate::schema_catalog_file as file;
use crate::{
    Database, DatabaseCoordinatorConfig, DatabaseError, PartitionCatalogConfig, SchemaGeneration,
    TablePlacementSpec, TableSchemaVersion, TableStorageCreateSpec, TableStorageOpenSpec,
};

/// Physical evidence for explicit legacy migration. It contains no logical schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyStorageLocation {
    Heap(PathBuf),
    Lsm(PathBuf),
}
impl LegacyStorageLocation {
    fn path(&self) -> &Path {
        match self {
            Self::Heap(path) | Self::Lsm(path) => path,
        }
    }
}

/// Operator-attested complete legacy database boundary. Old arbitrary file
/// compositions have no database-owned filename namespace to scan. The caller
/// must include every owned storage; this attestation is distinct from a schema
/// expectation. Schema subsets are rejected against this independent inventory.
#[derive(Debug, Clone)]
pub struct CompleteLegacyInventory {
    storages: Vec<LegacyStorageLocation>,
    coordinator: Option<DatabaseCoordinatorConfig>,
    partition_catalog: Option<PathBuf>,
}
impl CompleteLegacyInventory {
    #[must_use]
    pub fn attest_complete(
        storages: Vec<LegacyStorageLocation>,
        coordinator: Option<DatabaseCoordinatorConfig>,
        partition_catalog: Option<PathBuf>,
    ) -> Self {
        Self {
            storages,
            coordinator,
            partition_catalog,
        }
    }
}

impl Database {
    /// Creates a database at an explicit database-level catalog location. The
    /// supplied schema/placements are bootstrap input exactly once. Empty input
    /// is supported by this explicit-root API. Runtime Heap creation uses
    /// `create_heap_table_in`; SQL table DDL remains unsupported.
    pub fn create_catalog(
        catalog_path: impl AsRef<Path>,
        specs: Vec<TableStorageCreateSpec>,
        coordinator: Option<DatabaseCoordinatorConfig>,
    ) -> Result<Self, DatabaseError> {
        crate::validate_create_specs(&specs)?;
        if let Some(config) = &coordinator {
            crate::validate_explicit_coordinator_path_create(&specs, config)?;
        }
        let path = file::absolute(catalog_path.as_ref())?;
        let schema = Schema::new(specs.iter().map(|s| s.table().clone()).collect())?;
        let mut storages = Vec::new();
        let mut placements = Vec::new();
        for (position, spec) in specs.iter().enumerate() {
            let id = crate::storage_id_for_position(position)?;
            storages.push(CatalogStorage {
                id,
                table_id: spec.table().id,
                locator: file::relative(&path, spec.path())?,
                kind: match spec {
                    TableStorageCreateSpec::Heap { .. } => CatalogStorageKind::Heap,
                    TableStorageCreateSpec::Lsm {
                        clustering_column, ..
                    } => CatalogStorageKind::Lsm {
                        clustering_column: *clustering_column,
                    },
                },
            });
            placements.push(CatalogTable {
                table_id: spec.table().id,
                schema_fingerprint: spec.table().fingerprint()?,
                placement: TablePlacement::Single {
                    table_id: spec.table().id,
                    storage_id: id,
                },
            });
        }
        let snapshot = make_snapshot(
            &path,
            schema,
            PartitionCatalog { tables: placements },
            storages,
            coordinator
                .as_ref()
                .map(DatabaseCoordinatorConfig::log_path),
            None,
        )?;
        preflight_paths(&path, &snapshot, true)?;
        file::begin(&path, &snapshot, true)?;
        let all_heap = specs
            .iter()
            .all(|s| matches!(s, TableStorageCreateSpec::Heap { .. }));
        let database = if specs.is_empty() {
            if let Some(config) = coordinator {
                Self::compose_with_coordinator(
                    Schema::default(),
                    Vec::new(),
                    crate::CoordinatorLog::create(config.log_path())?,
                    netbadb_types::DatabaseTxnId(1),
                )?
            } else {
                Self::compose(Schema::default(), Vec::new())?
            }
        } else if all_heap {
            let tables = specs
                .into_iter()
                .map(|s| match s {
                    TableStorageCreateSpec::Heap { path, table }
                    | TableStorageCreateSpec::Lsm {
                        directory: path,
                        table,
                        ..
                    } => (path, table),
                })
                .collect();
            match coordinator {
                Some(config) => Self::physical_create_tables_with_coordinator(tables, config)?,
                None => Self::physical_create_tables(tables)?,
            }
        } else {
            match coordinator {
                Some(config) => Self::physical_create_storages_with_coordinator(specs, config)?,
                None => Self::physical_create_storages(specs)?,
            }
        };
        finish_install(database, &path, snapshot)
    }

    /// Creates an initial range placement catalog at an explicit schema root.
    pub fn create_catalog_with_placements(
        catalog_path: impl AsRef<Path>,
        specs: Vec<TablePlacementSpec>,
        config: PartitionCatalogConfig,
    ) -> Result<Self, DatabaseError> {
        crate::prevalidate_placement_specs(&specs)?;
        let path = file::absolute(catalog_path.as_ref())?;
        let schema = Schema::new(specs.iter().map(|s| s.table().clone()).collect())?;
        let mut storages = Vec::new();
        let mut placements = Vec::new();
        for spec in &specs {
            let table = spec.table();
            let placement = match spec {
                TablePlacementSpec::Single { path: physical, .. } => {
                    let id = crate::storage_id_for_position(storages.len())?;
                    storages.push(CatalogStorage {
                        id,
                        table_id: table.id,
                        locator: file::relative(&path, physical)?,
                        kind: CatalogStorageKind::Heap,
                    });
                    TablePlacement::Single {
                        table_id: table.id,
                        storage_id: id,
                    }
                }
                TablePlacementSpec::RangePartitioned {
                    partition_key,
                    partitions,
                    ..
                } => {
                    let key_type = crate::validate_partition_key(table, *partition_key)?;
                    let mut bindings = Vec::new();
                    for partition in partitions {
                        let id = crate::storage_id_for_position(storages.len())?;
                        storages.push(CatalogStorage {
                            id,
                            table_id: table.id,
                            locator: file::relative(&path, &partition.path)?,
                            kind: CatalogStorageKind::Heap,
                        });
                        bindings.push(RangePartitionBinding {
                            partition_id: partition.partition_id,
                            storage_id: id,
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
            placements.push(CatalogTable {
                table_id: table.id,
                schema_fingerprint: table.fingerprint()?,
                placement,
            });
        }
        let snapshot = make_snapshot(
            &path,
            schema,
            PartitionCatalog { tables: placements },
            storages,
            Some(config.coordinator_log_path()),
            Some(config.catalog_path()),
        )?;
        preflight_paths(&path, &snapshot, true)?;
        file::begin(&path, &snapshot, true)?;
        let database = Self::physical_create_with_placements(specs, config)?;
        finish_install(database, &path, snapshot)
    }

    /// Reconstructs the entire committed Schema and all physical bindings from
    /// the persisted catalog alone. Missing/corrupt initialized catalogs fail.
    pub fn open_catalog(catalog_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Self::open_catalog_with_expectation(catalog_path, None)
    }

    /// Required tables must match TableId and the exact canonical fingerprint.
    /// Additional committed tables remain visible and are opened in full.
    pub fn open_catalog_with_expectation(
        catalog_path: impl AsRef<Path>,
        expectation: Option<&Schema>,
    ) -> Result<Self, DatabaseError> {
        open_authority(catalog_path.as_ref(), expectation, &[], None, None)
    }

    /// Offline one-time import. Never called by ordinary open or server startup.
    /// Retry after an interrupted install requires the identical complete
    /// inventory. Incomplete physical creation is not silently repaired.
    pub fn open_legacy_and_install_catalog(
        catalog_path: impl AsRef<Path>,
        complete_schema: Schema,
        inventory: CompleteLegacyInventory,
    ) -> Result<Self, DatabaseError> {
        let path = file::absolute(catalog_path.as_ref())?;
        let incarnation = file::incarnation(&path)?; // Reject repeat import before touching storage.
        complete_schema.validate()?;
        let mut storages = Vec::new();
        let mut seen_tables = BTreeSet::new();
        for location in &inventory.storages {
            // Do not adopt a resource already attached to another managed root.
            if file::link_path(location.path())
                .try_exists()
                .map_err(|e| file::io("inspect legacy link", location.path(), e))?
                && file::discover(location.path())? != path
            {
                return Err(SchemaCatalogError::InventoryMismatch(
                    "legacy storage belongs to another catalog",
                )
                .into());
            }
            let (id, table_id, fingerprint, kind) = inspect_location(location)?;
            let table = complete_schema
                .tables()
                .iter()
                .find(|table| table.id == table_id)
                .ok_or(SchemaCatalogError::IncompleteLegacyInventory)?;
            if table.fingerprint()? != fingerprint {
                return Err(StorageError::SchemaMismatch {
                    expected: table.fingerprint()?,
                    actual: fingerprint,
                }
                .into());
            }
            seen_tables.insert(table_id);
            storages.push(CatalogStorage {
                id,
                table_id,
                locator: file::relative(&path, location.path())?,
                kind,
            });
        }
        if seen_tables != complete_schema.tables().iter().map(|t| t.id).collect() {
            return Err(SchemaCatalogError::IncompleteLegacyInventory.into());
        }
        let placements = if let Some(catalog) = &inventory.partition_catalog {
            let persisted = PartitionCatalog::open(catalog)?;
            crate::validate_catalog_schemas(&persisted, complete_schema.tables())?;
            // Reorder physical evidence into authoritative declaration order.
            PartitionCatalog {
                tables: complete_schema
                    .tables()
                    .iter()
                    .map(|t| {
                        persisted
                            .tables
                            .iter()
                            .find(|p| p.table_id == t.id)
                            .cloned()
                            .ok_or(SchemaCatalogError::IncompleteLegacyInventory)
                    })
                    .collect::<Result<_, _>>()?,
            }
        } else {
            let mut tables = Vec::new();
            for table in complete_schema.tables() {
                let matching = storages
                    .iter()
                    .filter(|s| s.table_id == table.id)
                    .collect::<Vec<_>>();
                if matching.len() != 1 {
                    return Err(SchemaCatalogError::IncompleteLegacyInventory.into());
                }
                tables.push(CatalogTable {
                    table_id: table.id,
                    schema_fingerprint: table.fingerprint()?,
                    placement: TablePlacement::Single {
                        table_id: table.id,
                        storage_id: matching[0].id,
                    },
                });
            }
            PartitionCatalog { tables }
        };
        // Stable physical ordering is independent of the order of operator paths.
        storages.sort_by_key(|s| s.id);
        let mut snapshot = make_snapshot(
            &path,
            complete_schema,
            placements,
            storages,
            inventory
                .coordinator
                .as_ref()
                .map(DatabaseCoordinatorConfig::log_path),
            inventory.partition_catalog.as_deref(),
        )?;
        snapshot.incarnation = incarnation;
        preflight_paths(&path, &snapshot, false)?;
        // Full identity/fingerprint/placement validation precedes intent publication.
        validate_physical(&path, &snapshot, &[])?;
        validate_partition_evidence(&path, &snapshot)?;
        file::begin(&path, &snapshot, false)?;
        let database = recover_physical(&path, &snapshot, &[])?;
        finish_install(database, &path, snapshot)
    }

    #[must_use]
    pub fn schema_generation(&self) -> SchemaGeneration {
        self.committed.generation
    }
    #[must_use]
    pub fn table_schema_version(&self, id: TableId) -> Option<TableSchemaVersion> {
        self.committed
            .tables
            .iter()
            .find(|t| t.table_id == id)
            .map(|t| t.version)
    }
    /// None denotes exhausted identity space, not permission to recompute it.
    #[must_use]
    pub fn next_table_id(&self) -> Option<TableId> {
        self.mutation_journal
            .as_ref()
            .map_or(self.committed.next_table_id, |j| {
                j.borrow().effective_table(self.committed.next_table_id)
            })
    }
    /// Inspection only: None means unknown table or exhausted column identity.
    /// Use table_schema_version to distinguish absence; this is not allocation.
    #[must_use]
    pub fn next_column_id(&self, id: TableId) -> Option<ColumnId> {
        let floor = self
            .committed
            .tables
            .iter()
            .find(|t| t.table_id == id)
            .and_then(|t| t.next_column_id);
        self.mutation_journal.as_ref().map_or(floor, |journal| {
            journal.borrow().effective_column(id, floor)
        })
    }
    #[must_use]
    pub fn next_storage_id(&self) -> Option<StorageId> {
        self.mutation_journal
            .as_ref()
            .map_or(self.committed.next_storage_id, |j| {
                j.borrow().effective_storage(self.committed.next_storage_id)
            })
    }
    #[must_use]
    pub fn next_partition_id(&self) -> Option<PartitionId> {
        self.committed.next_partition_id
    }

    /// Transitional constructor: uses `<path>.schema` as an explicit root
    /// passed to create_catalog. Prefer create_catalog for new integrations.
    pub fn create(path: impl AsRef<Path>, table: TableDef) -> Result<Self, DatabaseError> {
        Self::create_catalog(
            file::suffix(path.as_ref(), ".schema"),
            vec![TableStorageCreateSpec::heap(path.as_ref(), table)],
            None,
        )
    }
    /// Transitional expectation-only wrapper. The TableDef cannot reconstruct
    /// or replace live schema and never triggers legacy bootstrap.
    pub fn open(path: impl AsRef<Path>, expectation: TableDef) -> Result<Self, DatabaseError> {
        Self::open_tables(vec![(path.as_ref().to_owned(), expectation)])
    }
    /// Transitional bootstrap wrapper. Its first path chooses a root only on
    /// create; persisted discovery links locate that root on every later open.
    pub fn create_tables(tables: Vec<(PathBuf, TableDef)>) -> Result<Self, DatabaseError> {
        crate::validate_catalog_paths(&tables)?;
        Self::create_storages(
            tables
                .into_iter()
                .map(|(p, t)| TableStorageCreateSpec::heap(p, t))
                .collect(),
        )
    }
    pub fn create_storages(specs: Vec<TableStorageCreateSpec>) -> Result<Self, DatabaseError> {
        let first = specs.first().ok_or(DatabaseError::EmptyCatalog)?;
        Self::create_catalog(file::suffix(first.path(), ".schema"), specs, None)
    }
    pub fn create_tables_with_coordinator(
        tables: Vec<(PathBuf, TableDef)>,
        config: DatabaseCoordinatorConfig,
    ) -> Result<Self, DatabaseError> {
        crate::validate_catalog_paths(&tables)?;
        Self::create_storages_with_coordinator(
            tables
                .into_iter()
                .map(|(p, t)| TableStorageCreateSpec::heap(p, t))
                .collect(),
            config,
        )
    }
    pub fn create_storages_with_coordinator(
        specs: Vec<TableStorageCreateSpec>,
        config: DatabaseCoordinatorConfig,
    ) -> Result<Self, DatabaseError> {
        Self::create_catalog(
            file::suffix(config.log_path(), ".schema"),
            specs,
            Some(config),
        )
    }
    pub fn create_with_placements(
        specs: Vec<TablePlacementSpec>,
        config: PartitionCatalogConfig,
    ) -> Result<Self, DatabaseError> {
        Self::create_catalog_with_placements(
            file::suffix(config.catalog_path(), ".schema"),
            specs,
            config,
        )
    }
    /// Tables are required exact expectations, never a physical-open subset.
    pub fn open_tables(tables: Vec<(PathBuf, TableDef)>) -> Result<Self, DatabaseError> {
        Self::open_tables_with_expectation(tables)
    }
    /// Opens the complete persisted database located by the supplied Heap
    /// paths, checking their definitions only as required table expectations.
    pub fn open_tables_with_expectation(
        tables: Vec<(PathBuf, TableDef)>,
    ) -> Result<Self, DatabaseError> {
        crate::validate_catalog_paths(&tables)?;
        Self::open_storages(
            tables
                .into_iter()
                .map(|(p, t)| TableStorageOpenSpec::heap(p, t))
                .collect(),
        )
    }
    /// Transitional expectation/path-validation wrapper. Prefer open_catalog.
    pub fn open_storages(specs: Vec<TableStorageOpenSpec>) -> Result<Self, DatabaseError> {
        crate::validate_open_specs(&specs)?;
        let root = discover_specs(&specs)?;
        let expectation = Schema::new(specs.iter().map(|s| s.table().clone()).collect())?;
        open_authority(&root, Some(&expectation), &specs, None, None)
    }
    /// Required exact table expectations; the persisted catalog supplies every
    /// participant and the supplied coordinator path must agree with it.
    pub fn open_tables_with_coordinator(
        tables: Vec<(PathBuf, TableDef)>,
        config: DatabaseCoordinatorConfig,
    ) -> Result<Self, DatabaseError> {
        Self::open_storages_with_coordinator(
            tables
                .into_iter()
                .map(|(p, t)| TableStorageOpenSpec::heap(p, t))
                .collect(),
            config,
        )
    }
    /// Mixed-engine expectation/path adapter with exact coordinator validation.
    /// It never bootstraps or narrows the committed physical inventory.
    pub fn open_storages_with_coordinator(
        specs: Vec<TableStorageOpenSpec>,
        config: DatabaseCoordinatorConfig,
    ) -> Result<Self, DatabaseError> {
        crate::validate_open_specs(&specs)?;
        let root = discover_specs(&specs)?;
        let expectation = Schema::new(specs.iter().map(|s| s.table().clone()).collect())?;
        open_authority(
            &root,
            Some(&expectation),
            &specs,
            Some(config.log_path()),
            None,
        )
    }
    /// The supplied schema is an expectation. Unordered paths may relocate
    /// existing identities, but cannot change committed placement or bindings.
    pub fn open_with_placements(
        tables: Vec<TableDef>,
        storage_paths: Vec<PathBuf>,
        config: PartitionCatalogConfig,
    ) -> Result<Self, DatabaseError> {
        let root = if let Some(first) = storage_paths.first() {
            file::discover(first)?
        } else {
            file::suffix(config.catalog_path(), ".schema")
        };
        let snapshot = file::load(&root)?;
        let expectation = Schema::new(tables)?;
        validate_expectation(&snapshot.committed.schema, &expectation)?;
        let specs = storage_paths
            .into_iter()
            .map(|path| {
                let identity = TableStorage::inspect_heap_identity(&path)?;
                let table = snapshot
                    .committed
                    .schema
                    .tables()
                    .iter()
                    .find(|t| t.id == identity.table_id)
                    .ok_or(SchemaCatalogError::InventoryMismatch(
                        "unrecognized partition table",
                    ))?;
                Ok(TableStorageOpenSpec::heap(path, table.clone()))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        open_authority(
            &root,
            Some(&expectation),
            &specs,
            Some(config.coordinator_log_path()),
            Some(config.catalog_path()),
        )
    }
}

fn make_snapshot(
    path: &Path,
    schema: Schema,
    placements: PartitionCatalog,
    storages: Vec<CatalogStorage>,
    coordinator: Option<&Path>,
    partition: Option<&Path>,
) -> Result<SchemaCatalogSnapshot, DatabaseError> {
    let snapshot = SchemaCatalogSnapshot {
        incarnation: file::incarnation(path)?,
        epoch: 1,
        committed: CommittedCatalogState::initial(
            schema,
            placements.tables.iter().map(|t| t.placement.clone()),
        ),
        placements,
        storages,
        coordinator: coordinator.map(|p| file::relative(path, p)).transpose()?,
        partition_evidence: partition.map(|p| file::relative(path, p)).transpose()?,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn finish_install(
    mut database: Database,
    path: &Path,
    snapshot: SchemaCatalogSnapshot,
) -> Result<Database, DatabaseError> {
    for storage in database.registry.iter() {
        storage.storage.flush()?;
    }
    for descriptor in &snapshot.storages {
        file::sync_parent(&file::resolve(path, &descriptor.locator))?;
    }
    for locator in snapshot
        .coordinator
        .iter()
        .chain(snapshot.partition_evidence.iter())
    {
        file::sync_parent(&file::resolve(path, locator))?;
    }
    validate_physical(path, &snapshot, &[])?;
    // Publication transfers a freshly decoded committed Schema, never the
    // caller's TableDefs. Physical handles have already validated against it.
    database.committed = file::install_initial(path, &snapshot)?.committed;
    database.catalog_path = Some(path.to_owned());
    Ok(database)
}

fn discover_specs(specs: &[TableStorageOpenSpec]) -> Result<PathBuf, DatabaseError> {
    let mut root = None;
    for spec in specs {
        let found = match file::discover(spec.path()) {
            Ok(found) => found,
            Err(SchemaCatalogError::LegacyCatalogRequired) => continue,
            Err(error) => return Err(error.into()),
        };
        if root.as_ref().is_some_and(|root| root != &found) {
            return Err(SchemaCatalogError::InventoryMismatch(
                "paths belong to different database catalogs",
            )
            .into());
        }
        root = Some(found);
    }
    root.ok_or(SchemaCatalogError::LegacyCatalogRequired.into())
}

fn validate_expectation(schema: &Schema, expectation: &Schema) -> Result<(), DatabaseError> {
    expectation.validate()?;
    for expected in expectation.tables() {
        let actual = schema
            .table(&expected.name)
            .or_else(|| schema.tables().iter().find(|t| t.id == expected.id))
            .ok_or(SchemaCatalogError::ExpectationMissingTable(expected.id))?;
        if expected.id != actual.id {
            return Err(StorageError::TableIdMismatch {
                expected: expected.id,
                actual: actual.id,
            }
            .into());
        }
        let expected = expected.fingerprint()?;
        let actual = actual.fingerprint()?;
        if expected != actual {
            return Err(StorageError::SchemaMismatch { expected, actual }.into());
        }
    }
    Ok(())
}

fn open_authority(
    path: &Path,
    expectation: Option<&Schema>,
    overrides: &[TableStorageOpenSpec],
    coordinator: Option<&Path>,
    partition: Option<&Path>,
) -> Result<Database, DatabaseError> {
    let path = file::absolute(path)?;
    let journal = crate::schema_mutation::recover(&path)?;
    let snapshot = file::load(&path)?;
    preflight_paths(&path, &snapshot, false)?;
    if let Some(expectation) = expectation {
        validate_expectation(&snapshot.committed.schema, expectation)?;
    }
    for (expected, locator) in [
        (coordinator, snapshot.coordinator.as_ref()),
        (partition, snapshot.partition_evidence.as_ref()),
    ] {
        if let Some(expected) = expected {
            let actual = locator.ok_or(SchemaCatalogError::InventoryMismatch(
                "unexpected metadata configuration",
            ))?;
            if file::absolute(expected)? != file::absolute(&file::resolve(&path, actual))? {
                return Err(SchemaCatalogError::InventoryMismatch(
                    "metadata path differs from catalog",
                )
                .into());
            }
        }
    }
    validate_partition_evidence(&path, &snapshot)?;
    validate_physical(&path, &snapshot, overrides)?;
    let mut database = recover_physical(&path, &snapshot, overrides)?;
    database.committed = snapshot.committed;
    database.catalog_path = Some(path);
    if let Some(journal) = journal {
        if let Some(id) = journal
            .reservations
            .keys()
            .chain(journal.drops.keys())
            .chain(journal.rewrite_reservations.keys())
            .max()
        {
            let next =
                id.0.checked_add(1)
                    .ok_or(crate::CoordinatorError::TransactionIdExhausted)?;
            database.next_transaction_id.0 = database.next_transaction_id.0.max(next);
        }
        database.mutation_journal = Some(std::rc::Rc::new(std::cell::RefCell::new(journal)));
    } else if database
        .coordinator
        .as_ref()
        .is_some_and(|c| c.borrow().decisions().any(|d| d.schema.is_some()))
    {
        return Err(crate::SchemaMutationError::Corrupt(
            "schema coordinator has no mutation journal",
        )
        .into());
    }
    Ok(database)
}

fn validate_partition_evidence(
    path: &Path,
    snapshot: &SchemaCatalogSnapshot,
) -> Result<(), DatabaseError> {
    if let Some(locator) = &snapshot.partition_evidence {
        let evidence = PartitionCatalog::open(&file::resolve(path, locator))?;
        let journal = crate::schema_mutation_journal::SchemaMutationJournal::open(
            path,
            snapshot.incarnation,
        )?;
        let explained_retirement = |placement: &CatalogTable| {
            journal.as_ref().is_some_and(|journal| {
                journal.drops.values().any(|drop| {
                    drop.retired
                        && drop.resolved == Some(true)
                        && drop.fragment.placements.tables[0] == *placement
                })
            })
        };
        let explained_create = |placement: &CatalogTable| {
            journal.as_ref().is_some_and(|journal| {
                journal.reservations.values().any(|reservation| {
                    reservation.resolved == Some(true)
                        && reservation.intent.as_ref().is_some_and(|intent| {
                            intent.fragment.placements.tables[0] == *placement
                        })
                })
            })
        };
        if evidence.tables.iter().any(|placement| {
            !snapshot.placements.tables.contains(placement) && !explained_retirement(placement)
        }) || snapshot
            .placements
            .tables
            .iter()
            .any(|placement| !evidence.tables.contains(placement) && !explained_create(placement))
        {
            return Err(SchemaCatalogError::InventoryMismatch(
                "legacy partition catalog differs from committed placement",
            )
            .into());
        }
    }
    Ok(())
}

fn materialize(
    path: &Path,
    snapshot: &SchemaCatalogSnapshot,
    overrides: &[TableStorageOpenSpec],
) -> Result<Vec<TableStorageOpenSpec>, DatabaseError> {
    let mut replacements = Vec::new();
    let mut ids = BTreeSet::new();
    for spec in overrides {
        let location = match spec {
            TableStorageOpenSpec::Heap { path, .. } => LegacyStorageLocation::Heap(path.clone()),
            TableStorageOpenSpec::Lsm { directory, .. } => {
                LegacyStorageLocation::Lsm(directory.clone())
            }
        };
        let (id, table_id, fingerprint, kind) = inspect_location(&location)?;
        if spec.table().id != table_id {
            return Err(StorageError::TableIdMismatch {
                expected: spec.table().id,
                actual: table_id,
            }
            .into());
        }
        let descriptor = snapshot.storages.iter().find(|s| s.id == id).ok_or(
            SchemaCatalogError::InventoryMismatch("unexpected physical storage id"),
        )?;
        let table = snapshot
            .committed
            .schema
            .tables()
            .iter()
            .find(|t| t.id == descriptor.table_id)
            .ok_or(SchemaCatalogError::InventoryMismatch(
                "missing logical table",
            ))?;
        if descriptor.table_id != table_id
            || descriptor.kind != kind
            || table.fingerprint()? != fingerprint
            || !ids.insert(id)
        {
            return Err(SchemaCatalogError::InventoryMismatch(
                "path override identity mismatch or duplicate",
            )
            .into());
        }
        replacements.push((id, spec.path().to_owned()));
    }
    snapshot
        .storages
        .iter()
        .map(|storage| {
            let path = replacements
                .iter()
                .find(|(id, _)| *id == storage.id)
                .map(|(_, p)| p.clone())
                .unwrap_or_else(|| file::resolve(path, &storage.locator));
            let table = snapshot
                .committed
                .schema
                .tables()
                .iter()
                .find(|t| t.id == storage.table_id)
                .ok_or(SchemaCatalogError::InventoryMismatch("missing table"))?
                .clone();
            Ok(match storage.kind {
                CatalogStorageKind::Heap => TableStorageOpenSpec::heap(path, table),
                CatalogStorageKind::Lsm { .. } => TableStorageOpenSpec::lsm(path, table),
            })
        })
        .collect()
}
fn validate_physical(
    path: &Path,
    snapshot: &SchemaCatalogSnapshot,
    overrides: &[TableStorageOpenSpec],
) -> Result<(), DatabaseError> {
    for (descriptor, spec) in snapshot
        .storages
        .iter()
        .zip(materialize(path, snapshot, overrides)?)
    {
        let location = match &spec {
            TableStorageOpenSpec::Heap { path, .. } => LegacyStorageLocation::Heap(path.clone()),
            TableStorageOpenSpec::Lsm { directory, .. } => {
                LegacyStorageLocation::Lsm(directory.clone())
            }
        };
        let (id, table_id, fingerprint, kind) = inspect_location(&location)?;
        if id != descriptor.id || table_id != descriptor.table_id || kind != descriptor.kind {
            return Err(SchemaCatalogError::InventoryMismatch(
                "physical identity or engine differs from catalog",
            )
            .into());
        }
        if fingerprint != spec.table().fingerprint()? {
            return Err(StorageError::SchemaMismatch {
                expected: spec.table().fingerprint()?,
                actual: fingerprint,
            }
            .into());
        }
    }
    Ok(())
}
fn inspect_location(
    location: &LegacyStorageLocation,
) -> Result<
    (
        StorageId,
        TableId,
        netbadb_schema::SchemaFingerprint,
        CatalogStorageKind,
    ),
    DatabaseError,
> {
    Ok(match location {
        LegacyStorageLocation::Heap(path) => {
            let i = TableStorage::inspect_heap_identity(path)?;
            (
                i.storage_id,
                i.table_id,
                i.schema_fingerprint,
                CatalogStorageKind::Heap,
            )
        }
        LegacyStorageLocation::Lsm(path) => {
            let i = TableStorage::inspect_lsm_identity(path)?;
            (
                i.storage_id,
                i.table_id,
                i.schema_fingerprint,
                CatalogStorageKind::Lsm {
                    clustering_column: i.clustering_column,
                },
            )
        }
    })
}
pub(crate) fn recover_physical(
    path: &Path,
    snapshot: &SchemaCatalogSnapshot,
    overrides: &[TableStorageOpenSpec],
) -> Result<Database, DatabaseError> {
    let specs = materialize(path, snapshot, overrides)?;
    let journal =
        crate::schema_mutation_journal::SchemaMutationJournal::open(path, snapshot.incarnation)?;
    let coordinator_locator = snapshot.coordinator.as_ref().or_else(|| {
        journal
            .as_ref()
            .filter(|j| {
                !j.reservations.is_empty()
                    || !j.drops.is_empty()
                    || !j.rewrite_reservations.is_empty()
                    || !j.compositions.is_empty()
            })
            .map(|j| &j.coordinator)
    });
    let retired_storage_ids = journal
        .as_ref()
        .into_iter()
        .flat_map(|journal| {
            journal
                .drops
                .values()
                .filter(|drop| drop.retired && drop.resolved == Some(true))
                .map(crate::schema_mutation_journal::DropIntent::storage)
                .chain(
                    journal
                        .rewrites
                        .values()
                        .filter(|rewrite| rewrite.retired)
                        .map(crate::schema_mutation_journal::RewriteIntent::old_storage),
                )
                .chain(
                    journal
                        .compositions
                        .values()
                        .filter(|composition| {
                            !matches!(
                                composition.resolution,
                                Some(
                                    crate::schema_mutation_journal::CompositionResolution::Loser
                                        | crate::schema_mutation_journal::CompositionResolution::NoEffectiveChange
                                )
                            )
                        })
                        .filter_map(|composition| composition.intent.as_ref())
                        .flat_map(|intent| {
                            intent
                                .tables
                                .iter()
                                .map(crate::schema_mutation_journal::CompositionTablePlan::old_storage)
                        }),
                )
        })
        .collect::<Vec<_>>();
    let coordinator = coordinator_locator.map(|p| {
        DatabaseCoordinatorConfig::new(file::resolve(path, p))
            .with_retired_storage_ids(retired_storage_ids)
    });
    if let (Some(partition), Some(coordinator)) = (&snapshot.partition_evidence, &coordinator) {
        if snapshot.committed.generation.0 == 1 {
            return Database::physical_open_with_placements(
                snapshot.committed.schema.tables().to_vec(),
                specs.iter().map(|s| s.path().to_owned()).collect(),
                PartitionCatalogConfig::new(file::resolve(path, partition), coordinator.log_path()),
                snapshot.committed.clone(),
            );
        }
    }
    if specs.is_empty() {
        let coordinator = coordinator
            .map(|config| {
                let log = crate::CoordinatorLog::open(config.log_path())?;
                let decisions = log.decisions().cloned().collect::<Vec<_>>();
                crate::validate_generic_coordinator_recovery(
                    &decisions,
                    &[],
                    config.retired_storage_ids(),
                )?;
                let next = decisions
                    .iter()
                    .map(|d| d.database_txn_id.0)
                    .max()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(crate::CoordinatorError::TransactionIdExhausted)?;
                Ok::<_, DatabaseError>((log, netbadb_types::DatabaseTxnId(next)))
            })
            .transpose()?;
        return Database::compose_recovered(
            snapshot.committed.clone(),
            Vec::new(),
            coordinator,
            None,
        );
    }
    if snapshot.partition_evidence.is_none()
        && specs
            .iter()
            .all(|s| matches!(s, TableStorageOpenSpec::Heap { .. }))
    {
        let tables = specs
            .into_iter()
            .map(|s| match s {
                TableStorageOpenSpec::Heap { path, table }
                | TableStorageOpenSpec::Lsm {
                    directory: path,
                    table,
                } => (path, table),
            })
            .collect();
        return match coordinator {
            Some(config) => Database::physical_open_tables_with_coordinator(
                tables,
                config,
                snapshot.committed.clone(),
            ),
            None => Database::physical_open_tables(tables, snapshot.committed.clone()),
        };
    }
    match coordinator {
        Some(config) => Database::physical_open_storages_with_coordinator(
            specs,
            config,
            snapshot.committed.clone(),
            Some(snapshot.placements.clone()),
        ),
        None => Database::physical_open_storages(specs, snapshot.committed.clone()),
    }
}

fn preflight_paths(
    path: &Path,
    snapshot: &SchemaCatalogSnapshot,
    fresh: bool,
) -> Result<(), DatabaseError> {
    let mut claimed = BTreeSet::new();
    let mut claim = |path: PathBuf| -> Result<(), DatabaseError> {
        let path = file::absolute(&path)?;
        if !claimed.insert(path.clone()) {
            return Err(SchemaCatalogError::PathConflict(path).into());
        }
        Ok(())
    };
    for p in [path.to_owned(), file::marker_path(path)] {
        claim(p.clone())?;
        claim(file::suffix(&p, ".next"))?;
    }
    for suffix in [
        ".mutations",
        ".mutations.next",
        ".mutations.state",
        ".mutations.state.next",
    ] {
        claim(file::suffix(path, suffix))?;
    }
    for storage in &snapshot.storages {
        let physical = file::resolve(path, &storage.locator);
        if fresh
            && physical
                .try_exists()
                .map_err(|e| file::io("inspect fresh storage path", &physical, e))?
        {
            return Err(SchemaCatalogError::PathConflict(physical).into());
        }
        let link = file::link_path(&physical);
        if link
            .try_exists()
            .map_err(|e| file::io("inspect catalog discovery link", &link, e))?
        {
            if fresh {
                return Err(SchemaCatalogError::PathConflict(link).into());
            }
            if file::discover(&physical)? != file::absolute(path)? {
                return Err(SchemaCatalogError::InventoryMismatch(
                    "storage discovery link belongs to another database",
                )
                .into());
            }
        }
        claim(physical.clone())?;
        claim(link)?;
        claim(file::suffix(&file::link_path(&physical), ".next"))?;
        if matches!(storage.kind, CatalogStorageKind::Heap) {
            let wal = netbadb_storage::wal_path(&physical);
            for sidecar in [
                wal.clone(),
                netbadb_storage::wal_alternate_path(&wal),
                netbadb_storage::txn_status_path(&physical),
            ] {
                claim(sidecar)?;
            }
        }
    }
    for locator in snapshot
        .coordinator
        .iter()
        .chain(snapshot.partition_evidence.iter())
    {
        let resource = file::resolve(path, locator);
        if fresh
            && resource
                .try_exists()
                .map_err(|e| file::io("inspect fresh metadata path", &resource, e))?
        {
            return Err(SchemaCatalogError::PathConflict(resource).into());
        }
        claim(resource)?;
    }
    for storage in &snapshot.storages {
        if matches!(storage.kind, CatalogStorageKind::Lsm { .. }) {
            let directory = file::absolute(&file::resolve(path, &storage.locator))?;
            if claimed
                .iter()
                .any(|p| p != &directory && p.starts_with(&directory))
            {
                return Err(SchemaCatalogError::PathConflict(directory).into());
            }
        }
    }
    Ok(())
}
