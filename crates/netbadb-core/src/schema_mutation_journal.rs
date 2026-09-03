//! Ordered reservation/intention history; NBSC floors + this history are one allocator.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use netbadb_schema::{SchemaFingerprint, TypeSpec};
use netbadb_storage::{HeapRewriteIndex, HeapRewriteIndexes};
use netbadb_types::{
    ColumnId, DatabaseTxnId, IndexId, IndexName, PhysicalType, SchemaGeneration, SemanticType,
    StorageId, TableId, TableSchemaVersion,
};

use crate::schema_catalog::{Reader, SchemaCatalogSnapshot, Writer, envelope, open_envelope};
use crate::schema_catalog_file as file;
use crate::schema_mutation::{AlterTableOperation, SchemaMutationError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) storage: StorageId,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
    pub(crate) intent: Option<CreateIntent>,
    pub(crate) resolved: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateIntent {
    // Only the new table, never a repeated full database schema.
    pub(crate) fragment: SchemaCatalogSnapshot,
    pub(crate) snapshot_digest: [u8; 32],
}

/// Durable description of one logically dropped Heap. The single-table
/// fragment preserves the exact retired logical/physical identity for recovery;
/// it is never consulted by normal schema lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DropIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) fragment: SchemaCatalogSnapshot,
    pub(crate) target_generation: SchemaGeneration,
    pub(crate) target_epoch: u64,
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) retired: bool,
    pub(crate) resolved: Option<bool>,
    pub(crate) gc: Option<RetiredHeapGcRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RewriteReservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) storage: StorageId,
    pub(crate) column: Option<ColumnId>,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
}

/// Durable old/new logical and physical identity for one Heap replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RewriteIntent {
    pub(crate) reservation: RewriteReservation,
    pub(crate) operation: AlterTableOperation,
    pub(crate) base: SchemaCatalogSnapshot,
    pub(crate) target: SchemaCatalogSnapshot,
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) retired: bool,
    pub(crate) resolved: Option<bool>,
    pub(crate) gc: Option<RetiredHeapGcRecord>,
}

impl RewriteIntent {
    pub(crate) fn old_storage(&self) -> StorageId {
        self.base.storages[0].id
    }

    pub(crate) fn new_storage(&self) -> StorageId {
        self.reservation.storage
    }

    pub(crate) fn table(&self) -> TableId {
        self.reservation.table
    }
}

/// A logical identity consumed while an ALTER-only transaction is still
/// composable. It is allocator history, never committed schema authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositionColumnReservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) column: ColumnId,
    pub(crate) next_column_id: Option<ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositionIndexReservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) table_version: TableSchemaVersion,
    pub(crate) fingerprint: SchemaFingerprint,
    pub(crate) index: IndexId,
    pub(crate) next_index_id: Option<IndexId>,
}

/// A logical table identity consumed when CREATE TABLE is accepted into an
/// open schema composition. It deliberately carries no StorageId: physical
/// identity is allocated only if the final aggregate contains the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositionTableReservation {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) next_table_id: Option<TableId>,
}

/// One base-to-final physical replacement in an aggregate schema transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositionTablePlan {
    pub(crate) base: SchemaCatalogSnapshot,
    pub(crate) target: SchemaCatalogSnapshot,
    pub(crate) retired: bool,
    pub(crate) gc: Option<RetiredHeapGcRecord>,
}

impl CompositionTablePlan {
    pub(crate) fn table(&self) -> TableId {
        self.base.committed.schema.tables()[0].id
    }

    pub(crate) fn old_storage(&self) -> StorageId {
        self.base.storages[0].id
    }

    pub(crate) fn new_storage(&self) -> StorageId {
        self.target.storages[0].id
    }
}

/// Durable final recovery plan for a composed ALTER-only transaction. Recovery
/// never replays SQL or the action sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaChangeSetIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) target_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
    pub(crate) target_epoch: u64,
    pub(crate) action_count: u32,
    pub(crate) action_digest: [u8; 32],
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) tables: Vec<CompositionTablePlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaIndexTablePlan {
    CreateHeap {
        target: Box<SchemaCatalogSnapshot>,
        final_indexes: HeapRewriteIndexes,
    },
    DropHeap {
        base: Box<SchemaCatalogSnapshot>,
        retired: bool,
        gc: Option<RetiredHeapGcRecord>,
    },
    RewriteHeap {
        replacement: Box<CompositionTablePlan>,
        base_indexes: HeapRewriteIndexes,
        final_indexes: HeapRewriteIndexes,
    },
    InPlaceIndexDelta {
        table: TableId,
        table_version: TableSchemaVersion,
        fingerprint: SchemaFingerprint,
        storage: StorageId,
        base_indexes: HeapRewriteIndexes,
        final_indexes: HeapRewriteIndexes,
    },
}

impl SchemaIndexTablePlan {
    pub(crate) fn table(&self) -> TableId {
        match self {
            Self::CreateHeap { target, .. } => target.committed.schema.tables()[0].id,
            Self::DropHeap { base, .. } => base.committed.schema.tables()[0].id,
            Self::RewriteHeap { replacement, .. } => replacement.table(),
            Self::InPlaceIndexDelta { table, .. } => *table,
        }
    }

    pub(crate) fn participant_storage(&self) -> Option<StorageId> {
        match self {
            Self::CreateHeap { target, .. } => Some(target.storages[0].id),
            Self::DropHeap { .. } => None,
            Self::RewriteHeap { replacement, .. } => Some(replacement.new_storage()),
            Self::InPlaceIndexDelta { storage, .. } => Some(*storage),
        }
    }

    pub(crate) fn replacement(&self) -> Option<&CompositionTablePlan> {
        match self {
            Self::RewriteHeap { replacement, .. } => Some(replacement),
            Self::CreateHeap { .. } | Self::DropHeap { .. } | Self::InPlaceIndexDelta { .. } => {
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaIndexChangeSetIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) target_generation: Option<SchemaGeneration>,
    pub(crate) base_epoch: u64,
    pub(crate) target_epoch: Option<u64>,
    pub(crate) action_count: u32,
    pub(crate) action_digest: [u8; 32],
    pub(crate) snapshot_digest: Option<[u8; 32]>,
    pub(crate) tables: Vec<SchemaIndexTablePlan>,
}

/// Round 30 aggregate. Tag 25 remains the Round 29 schema/index byte contract;
/// this superset is encoded only by the new table-object intent tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableObjectChangeSetIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) target_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
    pub(crate) target_epoch: u64,
    pub(crate) action_count: u32,
    pub(crate) action_digest: [u8; 32],
    pub(crate) snapshot_digest: [u8; 32],
    pub(crate) tables: Vec<SchemaIndexTablePlan>,
}

impl From<SchemaIndexChangeSetIntent> for TableObjectChangeSetIntent {
    fn from(value: SchemaIndexChangeSetIntent) -> Self {
        Self {
            transaction: value.transaction,
            base_generation: value.base_generation,
            target_generation: value.target_generation.unwrap_or(value.base_generation),
            base_epoch: value.base_epoch,
            target_epoch: value.target_epoch.unwrap_or(value.base_epoch),
            action_count: value.action_count,
            action_digest: value.action_digest,
            snapshot_digest: value.snapshot_digest.unwrap_or([0; 32]),
            tables: value.tables,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompositionResolution {
    Loser,
    NoEffectiveChange,
    Winner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositionRecord {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table_reservations: Vec<CompositionTableReservation>,
    pub(crate) reservations: Vec<CompositionColumnReservation>,
    pub(crate) intent: Option<SchemaChangeSetIntent>,
    pub(crate) index_reservations: Vec<CompositionIndexReservation>,
    pub(crate) index_intent: Option<SchemaIndexChangeSetIntent>,
    pub(crate) table_intent: Option<TableObjectChangeSetIntent>,
    pub(crate) resolution: Option<CompositionResolution>,
}

/// Durable proof that the one private staged resource was allocated before its
/// files were created. It contains only exact locators and a single-table
/// provisional fragment; it is not a committed schema claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StageResourceIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) storage: StorageId,
    pub(crate) base_generation: SchemaGeneration,
    pub(crate) base_epoch: u64,
    pub(crate) provisional: SchemaCatalogSnapshot,
    pub(crate) stage_locator: String,
    pub(crate) final_locator: String,
    pub(crate) digest: [u8; 32],
}

/// Durable proof that private metadata was retargeted and the final NBSC
/// digest was prepared. CORD is allowed to publish this only after this record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizationIntent {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) table: TableId,
    pub(crate) storage: StorageId,
    pub(crate) final_snapshot: SchemaCatalogSnapshot,
    pub(crate) stage_locator: String,
    pub(crate) final_locator: String,
    pub(crate) digest: [u8; 32],
}

fn composition_replacement(
    record: &CompositionRecord,
    table: TableId,
) -> Option<&CompositionTablePlan> {
    record
        .intent
        .as_ref()
        .and_then(|intent| intent.tables.iter().find(|plan| plan.table() == table))
        .or_else(|| {
            record.index_intent.as_ref().and_then(|intent| {
                intent.tables.iter().find_map(|plan| match plan {
                    SchemaIndexTablePlan::RewriteHeap { replacement, .. }
                        if replacement.table() == table =>
                    {
                        Some(replacement.as_ref())
                    }
                    _ => None,
                })
            })
        })
}

fn composition_replacement_mut(
    record: &mut CompositionRecord,
    table: TableId,
) -> Option<&mut CompositionTablePlan> {
    if let Some(intent) = record.intent.as_mut() {
        return intent.tables.iter_mut().find(|plan| plan.table() == table);
    }
    record.index_intent.as_mut().and_then(|intent| {
        intent.tables.iter_mut().find_map(|plan| match plan {
            SchemaIndexTablePlan::RewriteHeap { replacement, .. }
                if replacement.table() == table =>
            {
                Some(replacement.as_mut())
            }
            _ => None,
        })
    })
}

fn table_object_retirement(
    record: &CompositionRecord,
    table: TableId,
) -> Option<&SchemaIndexTablePlan> {
    record.table_intent.as_ref().and_then(|intent| {
        intent.tables.iter().find(|plan| {
            plan.table() == table
                && matches!(
                    plan,
                    SchemaIndexTablePlan::RewriteHeap { .. }
                        | SchemaIndexTablePlan::DropHeap { .. }
                )
        })
    })
}

fn table_object_retirement_mut(
    record: &mut CompositionRecord,
    table: TableId,
) -> Option<&mut SchemaIndexTablePlan> {
    record.table_intent.as_mut().and_then(|intent| {
        intent.tables.iter_mut().find(|plan| {
            plan.table() == table
                && matches!(
                    plan,
                    SchemaIndexTablePlan::RewriteHeap { .. }
                        | SchemaIndexTablePlan::DropHeap { .. }
                )
        })
    })
}

fn table_object_gc(plan: &SchemaIndexTablePlan) -> Option<&RetiredHeapGcRecord> {
    match plan {
        SchemaIndexTablePlan::RewriteHeap { replacement, .. } => replacement.gc.as_ref(),
        SchemaIndexTablePlan::DropHeap { gc, .. } => gc.as_ref(),
        SchemaIndexTablePlan::CreateHeap { .. }
        | SchemaIndexTablePlan::InPlaceIndexDelta { .. } => None,
    }
}

fn table_object_gc_mut(
    plan: &mut SchemaIndexTablePlan,
) -> Option<&mut Option<RetiredHeapGcRecord>> {
    match plan {
        SchemaIndexTablePlan::RewriteHeap { replacement, .. } => Some(&mut replacement.gc),
        SchemaIndexTablePlan::DropHeap { gc, .. } => Some(gc),
        SchemaIndexTablePlan::CreateHeap { .. }
        | SchemaIndexTablePlan::InPlaceIndexDelta { .. } => None,
    }
}

/// Durable retry-only physical deletion state. The surrounding DROP or rewrite
/// intent carries the exact database incarnation, table/version/fingerprint,
/// StorageId, locator and retirement transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetiredHeapGcRecord {
    pub(crate) coordinator_horizon: DatabaseTxnId,
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) complete: bool,
}

impl DropIntent {
    pub(crate) fn table(&self) -> TableId {
        self.fragment.committed.schema.tables()[0].id
    }

    pub(crate) fn storage(&self) -> StorageId {
        self.fragment.storages[0].id
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaMutationJournal {
    pub(crate) path: PathBuf,
    pub(crate) incarnation: [u8; 16],
    pub(crate) coordinator: String,
    pub(crate) reservations: BTreeMap<DatabaseTxnId, Reservation>,
    pub(crate) drops: BTreeMap<DatabaseTxnId, DropIntent>,
    pub(crate) rewrite_reservations: BTreeMap<DatabaseTxnId, RewriteReservation>,
    pub(crate) rewrites: BTreeMap<DatabaseTxnId, RewriteIntent>,
    pub(crate) rewrite_losers: BTreeSet<DatabaseTxnId>,
    pub(crate) compositions: BTreeMap<DatabaseTxnId, CompositionRecord>,
    pub(crate) stage_intents: BTreeMap<DatabaseTxnId, StageResourceIntent>,
    pub(crate) finalization_intents: BTreeMap<DatabaseTxnId, FinalizationIntent>,
    poisoned: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

fn corrupt(reason: &'static str) -> SchemaMutationError {
    SchemaMutationError::Corrupt(reason)
}

fn digest_snapshot(snapshot: &SchemaCatalogSnapshot) -> Result<[u8; 32], SchemaMutationError> {
    Ok(crate::schema_mutation::digest(&snapshot.encode()?))
}

pub(crate) fn namespace(
    catalog: &Path,
    incarnation: [u8; 16],
) -> Result<String, SchemaMutationError> {
    let name = catalog
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or(corrupt("catalog filename is not UTF-8"))?;
    let identity = incarnation
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok(format!("{name}.resources-{identity}"))
}

pub(crate) fn stage_locator(
    catalog: &Path,
    incarnation: [u8; 16],
    txn: DatabaseTxnId,
    storage: StorageId,
) -> Result<String, SchemaMutationError> {
    Ok(format!(
        "{}/staging/{}/{}.heap",
        namespace(catalog, incarnation)?,
        txn.0,
        storage.0
    ))
}
pub(crate) fn final_locator(
    catalog: &Path,
    incarnation: [u8; 16],
    storage: StorageId,
) -> Result<String, SchemaMutationError> {
    Ok(format!(
        "{}/storage/{}.heap",
        namespace(catalog, incarnation)?,
        storage.0
    ))
}
pub(crate) fn prepared_locator(
    catalog: &Path,
    incarnation: [u8; 16],
    txn: DatabaseTxnId,
) -> Result<String, SchemaMutationError> {
    Ok(format!(
        "{}/staging/{}/catalog.nbsc",
        namespace(catalog, incarnation)?,
        txn.0
    ))
}

impl SchemaMutationJournal {
    pub(crate) fn open(
        catalog: &Path,
        incarnation: [u8; 16],
    ) -> Result<Option<Self>, SchemaMutationError> {
        let path = file::suffix(catalog, ".mutations");
        let witness = file::suffix(&path, ".state");
        let exists = path
            .try_exists()
            .map_err(|e| file::io("inspect mutation journal", &path, e))?;
        let activated = witness
            .try_exists()
            .map_err(|e| file::io("inspect mutation activation", &witness, e))?;
        if !exists && !activated {
            return Ok(None);
        }
        if !exists {
            return Err(corrupt("activated mutation journal is missing"));
        }
        let mut journal = Self::decode(&file::read(&path)?)?;
        if journal.incarnation != incarnation {
            return Err(corrupt("mutation journal incarnation mismatch"));
        }
        journal.path = path;
        for reservation in journal.reservations.values() {
            if let Some(intent) = &reservation.intent {
                if intent.fragment.storages[0].locator
                    != final_locator(catalog, incarnation, reservation.storage)?
                {
                    return Err(corrupt(
                        "intent final locator differs from reserved identity",
                    ));
                }
            }
        }
        for rewrite in journal.rewrites.values() {
            if rewrite.target.storages[0].locator
                != final_locator(catalog, incarnation, rewrite.new_storage())?
                || rewrite.base.storages[0].locator
                    != final_locator(catalog, incarnation, rewrite.old_storage())?
            {
                return Err(corrupt("rewrite locator differs from physical identity"));
            }
        }
        for composition in journal.compositions.values() {
            if let Some(intent) = &composition.intent {
                for plan in &intent.tables {
                    if plan.target.storages[0].locator
                        != final_locator(catalog, incarnation, plan.new_storage())?
                        || plan.base.storages[0].locator
                            != final_locator(catalog, incarnation, plan.old_storage())?
                    {
                        return Err(corrupt(
                            "composition locator differs from physical identity",
                        ));
                    }
                }
            }
            if let Some(intent) = &composition.index_intent {
                for plan in &intent.tables {
                    if let Some(plan) = plan.replacement() {
                        if plan.target.storages[0].locator
                            != final_locator(catalog, incarnation, plan.new_storage())?
                            || plan.base.storages[0].locator
                                != final_locator(catalog, incarnation, plan.old_storage())?
                        {
                            return Err(corrupt(
                                "schema/index composition locator differs from physical identity",
                            ));
                        }
                    }
                }
            }
            if let Some(intent) = &composition.table_intent {
                for plan in &intent.tables {
                    match plan {
                        SchemaIndexTablePlan::CreateHeap { target, .. } => {
                            if target.storages[0].locator
                                != final_locator(catalog, incarnation, target.storages[0].id)?
                            {
                                return Err(corrupt("CreateHeap locator differs from identity"));
                            }
                        }
                        SchemaIndexTablePlan::DropHeap { base, .. } => {
                            if base.storages[0].locator
                                != final_locator(catalog, incarnation, base.storages[0].id)?
                            {
                                return Err(corrupt("DropHeap locator differs from identity"));
                            }
                        }
                        SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                            if replacement.target.storages[0].locator
                                != final_locator(catalog, incarnation, replacement.new_storage())?
                                || replacement.base.storages[0].locator
                                    != final_locator(
                                        catalog,
                                        incarnation,
                                        replacement.old_storage(),
                                    )?
                            {
                                return Err(corrupt("table-object rewrite locator mismatch"));
                            }
                        }
                        SchemaIndexTablePlan::InPlaceIndexDelta { .. } => {}
                    }
                }
            }
            if let Some(intent) = &composition.table_intent {
                for plan in &intent.tables {
                    match plan {
                        SchemaIndexTablePlan::CreateHeap { target, .. } => {
                            let reservation = composition
                                .table_reservations
                                .iter()
                                .find(|reservation| reservation.table == plan.table())
                                .ok_or(corrupt("CreateHeap lacks TableId reservation"))?;
                            if !floor_at_least(
                                target.committed.next_table_id.map(|table| table.0),
                                reservation.next_table_id.map(|table| table.0),
                            ) {
                                return Err(corrupt("CreateHeap TableId floor mismatch"));
                            }
                        }
                        SchemaIndexTablePlan::DropHeap { .. }
                        | SchemaIndexTablePlan::RewriteHeap { .. }
                        | SchemaIndexTablePlan::InPlaceIndexDelta { .. } => {
                            if composition
                                .table_reservations
                                .iter()
                                .any(|reservation| reservation.table == plan.table())
                            {
                                return Err(corrupt(
                                    "committed table plan reuses private TableId reservation",
                                ));
                            }
                        }
                    }
                }
            }
        }
        for intent in journal.stage_intents.values() {
            if intent.stage_locator
                != stage_locator(catalog, incarnation, intent.transaction, intent.storage)?
                || intent.final_locator != final_locator(catalog, incarnation, intent.storage)?
                || intent.provisional.incarnation != incarnation
                || intent.provisional.storages.len() != 1
                || intent.provisional.storages[0].id != intent.storage
                || intent.digest != digest_snapshot(&intent.provisional)?
            {
                return Err(corrupt("backfill stage intent locator or digest mismatch"));
            }
        }
        for intent in journal.finalization_intents.values() {
            let stage = journal
                .stage_intents
                .get(&intent.transaction)
                .ok_or(corrupt("backfill finalization lacks stage intent"))?;
            if intent.stage_locator != stage.stage_locator
                || intent.final_locator != stage.final_locator
                || intent.storage != stage.storage
                || intent.table != stage.table
                || intent.final_snapshot.incarnation != incarnation
                || intent.digest != digest_snapshot(&intent.final_snapshot)?
            {
                return Err(corrupt("backfill finalization locator or digest mismatch"));
            }
        }
        if activated {
            if open_envelope(&file::read(&witness)?, b"NBSA")? != incarnation {
                return Err(corrupt("mutation activation incarnation mismatch"));
            }
        } else if !journal.reservations.is_empty()
            || !journal.drops.is_empty()
            || !journal.rewrite_reservations.is_empty()
            || !journal.compositions.is_empty()
            || !journal.stage_intents.is_empty()
            || !journal.finalization_intents.is_empty()
        {
            return Err(corrupt("reservation history has no activation witness"));
        }
        Ok(Some(journal))
    }

    pub(crate) fn initialize(
        catalog: &Path,
        incarnation: [u8; 16],
        coordinator: String,
    ) -> Result<Self, SchemaMutationError> {
        let journal = match Self::open(catalog, incarnation)? {
            Some(journal) => {
                if journal.coordinator != coordinator {
                    return Err(corrupt("coordinator locator changed"));
                }
                journal
            }
            None => {
                let journal = Self {
                    path: file::suffix(catalog, ".mutations"),
                    incarnation,
                    coordinator,
                    reservations: BTreeMap::new(),
                    drops: BTreeMap::new(),
                    rewrite_reservations: BTreeMap::new(),
                    rewrites: BTreeMap::new(),
                    rewrite_losers: BTreeSet::new(),
                    compositions: BTreeMap::new(),
                    stage_intents: BTreeMap::new(),
                    finalization_intents: BTreeMap::new(),
                    poisoned: false,
                    #[cfg(test)]
                    fail_next_sync: false,
                };
                file::atomic_write(&journal.path, &journal.encode()?, false)?;
                journal
            }
        };
        journal.activate()?;
        Ok(journal)
    }

    fn activate(&self) -> Result<(), SchemaMutationError> {
        let witness = file::suffix(&self.path, ".state");
        if !witness
            .try_exists()
            .map_err(|e| file::io("inspect mutation activation", &witness, e))?
        {
            file::atomic_write(&witness, &envelope(b"NBSA", &self.incarnation)?, false)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn prepare_reservation(
        &self,
        reservation: &Reservation,
        intent: &CreateIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.compositions.contains_key(&reservation.transaction) {
            return Err(corrupt("legacy create overlaps composition"));
        }
        // Reserve room for the entire obligation before consuming an identity:
        // a full journal must never prevent winner/loser resolution.
        let mut complete = self.clone();
        let mut projected = reservation.clone();
        projected.intent = Some(intent.clone());
        projected.resolved = Some(false);
        complete
            .reservations
            .insert(projected.transaction, projected);
        complete.encode()?;
        // An empty journal may survive a crash before its activation witness.
        // Reopened handles must finish activation before their first reservation.
        self.activate()
    }

    #[cfg(test)]
    pub(crate) fn prepare_drop(&self, intent: &DropIntent) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&intent.transaction)
            || self.drops.contains_key(&intent.transaction)
            || self.compositions.contains_key(&intent.transaction)
        {
            return Err(corrupt("duplicate schema mutation transaction"));
        }
        let mut complete = self.clone();
        let mut projected = intent.clone();
        projected.retired = true;
        projected.resolved = Some(true);
        complete.drops.insert(projected.transaction, projected);
        complete.encode()?;
        self.activate()
    }

    #[cfg(test)]
    pub(crate) fn prepare_rewrite(
        &self,
        reservation: &RewriteReservation,
        intent: &RewriteIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
            || self.compositions.contains_key(&reservation.transaction)
            || reservation != &intent.reservation
        {
            return Err(corrupt("duplicate or mismatched rewrite transaction"));
        }
        let mut complete = self.clone();
        complete
            .rewrite_reservations
            .insert(reservation.transaction, reservation.clone());
        let mut projected = intent.clone();
        projected.retired = true;
        projected.resolved = Some(true);
        complete.rewrites.insert(reservation.transaction, projected);
        complete.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_composition_reservation(
        &self,
        reservation: &CompositionColumnReservation,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
        {
            return Err(corrupt("composition overlaps legacy schema mutation"));
        }
        let mut projected = self.clone();
        let record = projected
            .compositions
            .entry(reservation.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: reservation.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            });
        if record.intent.is_some()
            || record.index_intent.is_some()
            || record.table_intent.is_some()
            || record.resolution.is_some()
            || record.reservations.iter().any(|existing| {
                existing.table == reservation.table && existing.column == reservation.column
            })
        {
            return Err(corrupt("duplicate or out-of-order composition reservation"));
        }
        record.reservations.push(reservation.clone());
        record
            .reservations
            .sort_by_key(|entry| (entry.table, entry.column));
        projected.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_composition_table_reservation(
        &self,
        reservation: &CompositionTableReservation,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if reservation.table.0 == 0
            || reservation.next_table_id.map(|next| next.0) != reservation.table.0.checked_add(1)
            || self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
        {
            return Err(corrupt("invalid composition TableId reservation"));
        }
        let mut projected = self.clone();
        let record = projected
            .compositions
            .entry(reservation.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: reservation.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            });
        if record.intent.is_some()
            || record.index_intent.is_some()
            || record.table_intent.is_some()
            || record.resolution.is_some()
            || record
                .table_reservations
                .last()
                .is_some_and(|previous| previous.table >= reservation.table)
        {
            return Err(corrupt("duplicate or out-of-order TableId reservation"));
        }
        record.table_reservations.push(reservation.clone());
        projected.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_composition_index_reservation(
        &self,
        reservation: &CompositionIndexReservation,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
        {
            return Err(corrupt("composition overlaps legacy schema mutation"));
        }
        let mut projected = self.clone();
        let record = projected
            .compositions
            .entry(reservation.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: reservation.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            });
        if record.intent.is_some()
            || record.index_intent.is_some()
            || record.table_intent.is_some()
            || record.resolution.is_some()
            || record.index_reservations.iter().any(|existing| {
                existing.table == reservation.table && existing.index == reservation.index
            })
        {
            return Err(corrupt("duplicate or out-of-order IndexId reservation"));
        }
        record.index_reservations.push(reservation.clone());
        record
            .index_reservations
            .sort_by_key(|entry| (entry.table, entry.index));
        projected.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_composition_intent(
        &self,
        intent: &SchemaChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&intent.transaction)
            || self.drops.contains_key(&intent.transaction)
            || self.rewrite_reservations.contains_key(&intent.transaction)
        {
            return Err(corrupt("composition overlaps legacy schema mutation"));
        }
        let mut projected = self.clone();
        let record = projected
            .compositions
            .entry(intent.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: intent.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            });
        if record.intent.is_some()
            || record.index_intent.is_some()
            || record.table_intent.is_some()
            || record.resolution.is_some()
        {
            return Err(corrupt("duplicate composition intent"));
        }
        record.intent = Some(intent.clone());
        let bytes = projected.encode()?;
        if bytes.len() > crate::schema_catalog::MAX_BYTES {
            return Err(corrupt("composition intent capacity exhausted"));
        }
        self.activate()
    }

    pub(crate) fn prepare_schema_index_intent(
        &self,
        intent: &SchemaIndexChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        validate_schema_index_intent(
            &intent.tables,
            SchemaIndexValidationContext {
                target_generation: intent.target_generation,
                target_epoch: intent.target_epoch,
                snapshot_digest: intent.snapshot_digest,
                action_count: intent.action_count,
                incarnation: self.incarnation,
                coordinator: &self.coordinator,
                base_generation: intent.base_generation,
                base_epoch: intent.base_epoch,
            },
        )?;
        if self.reservations.contains_key(&intent.transaction)
            || self.drops.contains_key(&intent.transaction)
            || self.rewrite_reservations.contains_key(&intent.transaction)
        {
            return Err(corrupt("composition overlaps legacy schema mutation"));
        }
        let mut projected = self.clone();
        let record = projected
            .compositions
            .entry(intent.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: intent.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            });
        if record.intent.is_some()
            || record.index_intent.is_some()
            || record.table_intent.is_some()
            || record.resolution.is_some()
        {
            return Err(corrupt("duplicate composition intent"));
        }
        record.index_intent = Some(intent.clone());
        projected.encode()?;
        self.activate()
    }

    pub(crate) fn prepare_gc(
        &self,
        txn: DatabaseTxnId,
        gc: &RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let terminal = self
            .drops
            .get(&txn)
            .map(|drop| (drop.retired, drop.resolved, drop.gc.is_some()))
            .or_else(|| {
                self.rewrites
                    .get(&txn)
                    .map(|rewrite| (rewrite.retired, rewrite.resolved, rewrite.gc.is_some()))
            })
            .ok_or(corrupt("GC intent without retirement history"))?;
        if !terminal.0 || terminal.1 != Some(true) || terminal.2 || gc.complete {
            return Err(corrupt("out-of-order GC intent"));
        }
        // Reserve both terminal records before the first unlink. Capacity can
        // therefore never strand a durable deleting state without Complete.
        let mut projected = self.clone();
        let complete = RetiredHeapGcRecord {
            complete: true,
            ..gc.clone()
        };
        if let Some(drop) = projected.drops.get_mut(&txn) {
            drop.gc = Some(complete);
        } else {
            projected
                .rewrites
                .get_mut(&txn)
                .ok_or(corrupt("projected GC retirement disappeared"))?
                .gc = Some(complete);
        }
        projected.encode()?;
        Ok(())
    }

    pub(crate) fn effective_table(&self, floor: Option<TableId>) -> Option<TableId> {
        let floor = floor?;
        self.reservations
            .values()
            .map(|r| r.table.0)
            .chain(
                self.compositions
                    .values()
                    .flat_map(|record| record.table_reservations.iter())
                    .map(|reservation| reservation.table.0),
            )
            .max()
            .map_or(Some(floor), |max| {
                max.checked_add(1).map(|n| TableId(n.max(floor.0)))
            })
    }
    pub(crate) fn effective_storage(&self, floor: Option<StorageId>) -> Option<StorageId> {
        let floor = floor?;
        self.reservations
            .values()
            .map(|r| r.storage.0)
            .chain(
                self.rewrite_reservations
                    .values()
                    .map(|reservation| reservation.storage.0),
            )
            .chain(
                self.compositions
                    .values()
                    .filter_map(|record| record.intent.as_ref())
                    .flat_map(|intent| intent.tables.iter().map(CompositionTablePlan::new_storage))
                    .map(|storage| storage.0),
            )
            .chain(
                self.compositions
                    .values()
                    .filter_map(|record| record.index_intent.as_ref())
                    .flat_map(|intent| intent.tables.iter())
                    .filter_map(SchemaIndexTablePlan::replacement)
                    .map(CompositionTablePlan::new_storage)
                    .map(|storage| storage.0),
            )
            .chain(
                self.compositions
                    .values()
                    .filter_map(|record| record.table_intent.as_ref())
                    .flat_map(|intent| intent.tables.iter())
                    .filter_map(SchemaIndexTablePlan::participant_storage)
                    .map(|storage| storage.0),
            )
            .max()
            .map_or(Some(floor), |max| {
                max.checked_add(1).map(|n| StorageId(n.max(floor.0)))
            })
    }
    pub(crate) fn effective_index(&self, table: TableId, floor: IndexId) -> Option<IndexId> {
        self.compositions
            .values()
            .flat_map(|record| record.index_reservations.iter())
            .filter(|reservation| reservation.table == table)
            .map(|reservation| reservation.index.0)
            .max()
            .map_or(Some(floor), |maximum| {
                maximum
                    .checked_add(1)
                    .map(|next| IndexId(next.max(floor.0)))
            })
    }
    pub(crate) fn effective_column(
        &self,
        table: TableId,
        floor: Option<ColumnId>,
    ) -> Option<ColumnId> {
        let floor = floor?;
        self.rewrite_reservations
            .values()
            .filter(|reservation| reservation.table == table)
            .filter_map(|reservation| reservation.column)
            .map(|column| column.0)
            .chain(
                self.compositions
                    .values()
                    .flat_map(|record| record.reservations.iter())
                    .filter(|reservation| reservation.table == table)
                    .map(|reservation| reservation.column.0),
            )
            .max()
            .map_or(Some(floor), |maximum| {
                maximum
                    .checked_add(1)
                    .map(|next| ColumnId(next.max(floor.0)))
            })
    }
    pub(crate) fn ensure_ready(&self) -> Result<(), SchemaMutationError> {
        if self.poisoned {
            Err(SchemaMutationError::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    #[cfg(test)]
    pub(crate) fn reserve(&mut self, reservation: Reservation) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction) {
            return Err(corrupt("duplicate reservation transaction"));
        }
        self.reservations
            .insert(reservation.transaction, reservation);
        self.persist()
    }
    #[cfg(test)]
    pub(crate) fn reserve_rewrite(
        &mut self,
        reservation: RewriteReservation,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&reservation.transaction)
            || self.drops.contains_key(&reservation.transaction)
            || self
                .rewrite_reservations
                .contains_key(&reservation.transaction)
        {
            return Err(corrupt("duplicate rewrite reservation"));
        }
        self.rewrite_reservations
            .insert(reservation.transaction, reservation);
        self.persist()
    }

    pub(crate) fn reserve_composition_column(
        &mut self,
        reservation: CompositionColumnReservation,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_composition_reservation(&reservation)?;
        self.compositions
            .entry(reservation.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: reservation.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            })
            .reservations
            .push(reservation);
        self.persist()
    }

    pub(crate) fn reserve_composition_table(
        &mut self,
        reservation: CompositionTableReservation,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_composition_table_reservation(&reservation)?;
        self.compositions
            .entry(reservation.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: reservation.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            })
            .table_reservations
            .push(reservation);
        self.persist()
    }

    pub(crate) fn reserve_composition_index(
        &mut self,
        reservation: CompositionIndexReservation,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_composition_index_reservation(&reservation)?;
        self.compositions
            .entry(reservation.transaction)
            .or_insert_with(|| CompositionRecord {
                transaction: reservation.transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            })
            .index_reservations
            .push(reservation);
        self.persist()
    }

    pub(crate) fn composition_intent(
        &mut self,
        intent: SchemaChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_composition_intent(&intent)?;
        let transaction = intent.transaction;
        self.compositions
            .entry(transaction)
            .or_insert_with(|| CompositionRecord {
                transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            })
            .intent = Some(intent);
        self.persist()
    }

    pub(crate) fn replace_composition_intent(
        &mut self,
        intent: SchemaChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let record = self
            .compositions
            .get_mut(&intent.transaction)
            .ok_or(corrupt("backfill composition intent is absent"))?;
        if record.intent.is_none() || record.resolution.is_some() {
            return Err(corrupt("backfill composition intent is not replaceable"));
        }
        record.intent = Some(intent);
        self.persist()
    }

    pub(crate) fn replace_table_object_intent(
        &mut self,
        intent: TableObjectChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let record = self
            .compositions
            .get_mut(&intent.transaction)
            .ok_or(corrupt("backfill table-object intent is absent"))?;
        if record.table_intent.is_none() || record.resolution.is_some() {
            return Err(corrupt("backfill table-object intent is not replaceable"));
        }
        record.table_intent = Some(intent);
        self.persist()
    }

    pub(crate) fn stage_resource_intent(
        &mut self,
        intent: StageResourceIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.stage_intents.contains_key(&intent.transaction)
            || self.finalization_intents.contains_key(&intent.transaction)
            || !self.compositions.contains_key(&intent.transaction)
        {
            return Err(corrupt("duplicate or out-of-order backfill stage intent"));
        }
        if intent.provisional.storages.len() != 1
            || intent.provisional.storages[0].id != intent.storage
            || intent.provisional.committed.schema.tables().len() != 1
            || intent.provisional.committed.schema.tables()[0].id != intent.table
            || intent.digest != digest_snapshot(&intent.provisional)?
        {
            return Err(corrupt("invalid backfill stage resource identity"));
        }
        self.stage_intents.insert(intent.transaction, intent);
        self.persist()
    }

    pub(crate) fn finalization_intent(
        &mut self,
        intent: FinalizationIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let stage = self
            .stage_intents
            .get(&intent.transaction)
            .ok_or(corrupt("backfill finalization lacks stage intent"))?;
        if self.finalization_intents.contains_key(&intent.transaction)
            || stage.table != intent.table
            || stage.storage != intent.storage
            || stage.stage_locator != intent.stage_locator
            || stage.final_locator != intent.final_locator
            || intent.digest != digest_snapshot(&intent.final_snapshot)?
        {
            return Err(corrupt("invalid backfill finalization identity"));
        }
        self.finalization_intents.insert(intent.transaction, intent);
        self.persist()
    }

    pub(crate) fn schema_index_intent(
        &mut self,
        intent: SchemaIndexChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_schema_index_intent(&intent)?;
        let transaction = intent.transaction;
        self.compositions
            .entry(transaction)
            .or_insert_with(|| CompositionRecord {
                transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            })
            .index_intent = Some(intent);
        self.persist()
    }

    pub(crate) fn table_object_intent(
        &mut self,
        intent: TableObjectChangeSetIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        validate_table_object_intent(&intent, self.incarnation, &self.coordinator)?;
        let transaction = intent.transaction;
        let record = self
            .compositions
            .entry(transaction)
            .or_insert_with(|| CompositionRecord {
                transaction,
                table_reservations: Vec::new(),
                reservations: Vec::new(),
                intent: None,
                index_reservations: Vec::new(),
                index_intent: None,
                table_intent: None,
                resolution: None,
            });
        if record.intent.is_some()
            || record.index_intent.is_some()
            || record.table_intent.is_some()
            || record.resolution.is_some()
        {
            return Err(corrupt("duplicate table-object composition intent"));
        }
        record.table_intent = Some(intent);
        self.persist()
    }

    pub(crate) fn retire_composition_table(
        &mut self,
        txn: DatabaseTxnId,
        table: TableId,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let record = self
            .compositions
            .get_mut(&txn)
            .ok_or(corrupt("composition retirement without history"))?;
        let plan = if let Some(intent) = record.intent.as_mut() {
            intent.tables.iter_mut().find(|plan| plan.table() == table)
        } else {
            record.index_intent.as_mut().and_then(|intent| {
                intent.tables.iter_mut().find_map(|plan| match plan {
                    SchemaIndexTablePlan::RewriteHeap { replacement, .. }
                        if replacement.table() == table =>
                    {
                        Some(replacement.as_mut())
                    }
                    _ => None,
                })
            })
        }
        .ok_or(corrupt("composition retirement without table plan"))?;
        if plan.retired {
            return Ok(());
        }
        plan.retired = true;
        self.persist()
    }

    pub(crate) fn retire_table_object(
        &mut self,
        txn: DatabaseTxnId,
        table: TableId,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let plan = self
            .compositions
            .get_mut(&txn)
            .and_then(|record| record.table_intent.as_mut())
            .and_then(|intent| intent.tables.iter_mut().find(|plan| plan.table() == table))
            .ok_or(corrupt("table-object retirement without plan"))?;
        match plan {
            SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                if replacement.retired {
                    return Ok(());
                }
                replacement.retired = true;
            }
            SchemaIndexTablePlan::DropHeap { retired, .. } => {
                if *retired {
                    return Ok(());
                }
                *retired = true;
            }
            _ => return Err(corrupt("table-object plan has no predecessor")),
        }
        self.persist()
    }

    pub(crate) fn resolve_composition(
        &mut self,
        txn: DatabaseTxnId,
        resolution: CompositionResolution,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let record = self
            .compositions
            .get_mut(&txn)
            .ok_or(corrupt("composition resolution without history"))?;
        if let Some(previous) = record.resolution {
            return if previous == resolution {
                Ok(())
            } else {
                Err(corrupt("conflicting composition resolution"))
            };
        }
        match resolution {
            CompositionResolution::Winner => {
                if record.intent.is_none()
                    && record.index_intent.is_none()
                    && record.table_intent.is_none()
                {
                    return Err(corrupt("composition winner without intent"));
                }
                if record
                    .intent
                    .as_ref()
                    .is_some_and(|intent| intent.tables.iter().any(|plan| !plan.retired))
                    || record.index_intent.as_ref().is_some_and(|intent| {
                        intent.tables.iter().any(|plan| {
                            plan.replacement()
                                .is_some_and(|replacement| !replacement.retired)
                        })
                    })
                    || record.table_intent.as_ref().is_some_and(|intent| {
                        intent.tables.iter().any(|plan| match plan {
                            SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                                !replacement.retired
                            }
                            SchemaIndexTablePlan::DropHeap { retired, .. } => !*retired,
                            _ => false,
                        })
                    })
                {
                    return Err(corrupt("composition winner has unretired predecessor"));
                }
            }
            CompositionResolution::NoEffectiveChange
                if record.intent.is_some()
                    || record.index_intent.is_some()
                    || record.table_intent.is_some() =>
            {
                return Err(corrupt("no-change composition has physical intent"));
            }
            CompositionResolution::Loser | CompositionResolution::NoEffectiveChange => {}
        }
        record.resolution = Some(resolution);
        self.persist()
    }

    pub(crate) fn prepare_composition_gc(
        &self,
        txn: DatabaseTxnId,
        table: TableId,
        gc: &RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let plan = self
            .compositions
            .get(&txn)
            .filter(|record| record.resolution == Some(CompositionResolution::Winner))
            .and_then(|record| composition_replacement(record, table))
            .ok_or(corrupt("composition GC without winner table plan"))?;
        if !plan.retired || plan.gc.is_some() || gc.complete {
            return Err(corrupt("out-of-order composition GC intent"));
        }
        let mut projected = self.clone();
        projected
            .compositions
            .get_mut(&txn)
            .and_then(|record| composition_replacement_mut(record, table))
            .ok_or(corrupt("projected composition GC plan disappeared"))?
            .gc = Some(RetiredHeapGcRecord {
            complete: true,
            ..gc.clone()
        });
        projected.encode()?;
        Ok(())
    }

    pub(crate) fn composition_gc_intent(
        &mut self,
        txn: DatabaseTxnId,
        table: TableId,
        gc: RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_composition_gc(txn, table, &gc)?;
        self.compositions
            .get_mut(&txn)
            .and_then(|record| composition_replacement_mut(record, table))
            .ok_or(corrupt("composition GC plan disappeared"))?
            .gc = Some(gc);
        self.persist()
    }

    pub(crate) fn complete_composition_gc(
        &mut self,
        txn: DatabaseTxnId,
        table: TableId,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let gc = self
            .compositions
            .get_mut(&txn)
            .and_then(|record| composition_replacement_mut(record, table))
            .and_then(|plan| plan.gc.as_mut())
            .ok_or(corrupt("composition GC complete without intent"))?;
        if gc.complete {
            return Ok(());
        }
        gc.complete = true;
        self.persist()
    }

    pub(crate) fn prepare_table_object_gc(
        &self,
        txn: DatabaseTxnId,
        table: TableId,
        gc: &RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let plan = self
            .compositions
            .get(&txn)
            .filter(|record| record.resolution == Some(CompositionResolution::Winner))
            .and_then(|record| table_object_retirement(record, table))
            .ok_or(corrupt("table-object GC without winner retirement"))?;
        let retired = match plan {
            SchemaIndexTablePlan::RewriteHeap { replacement, .. } => replacement.retired,
            SchemaIndexTablePlan::DropHeap { retired, .. } => *retired,
            _ => false,
        };
        if !retired || table_object_gc(plan).is_some() || gc.complete {
            return Err(corrupt("out-of-order table-object GC intent"));
        }
        let mut projected = self.clone();
        let projected_plan = projected
            .compositions
            .get_mut(&txn)
            .and_then(|record| table_object_retirement_mut(record, table))
            .ok_or(corrupt("projected table-object GC plan disappeared"))?;
        *table_object_gc_mut(projected_plan)
            .ok_or(corrupt("projected table-object GC state disappeared"))? =
            Some(RetiredHeapGcRecord {
                complete: true,
                ..gc.clone()
            });
        projected.encode()?;
        Ok(())
    }

    pub(crate) fn table_object_gc_intent(
        &mut self,
        txn: DatabaseTxnId,
        table: TableId,
        gc: RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_table_object_gc(txn, table, &gc)?;
        let plan = self
            .compositions
            .get_mut(&txn)
            .and_then(|record| table_object_retirement_mut(record, table))
            .ok_or(corrupt("table-object GC plan disappeared"))?;
        *table_object_gc_mut(plan).ok_or(corrupt("table-object GC state disappeared"))? = Some(gc);
        self.persist()
    }

    pub(crate) fn complete_table_object_gc(
        &mut self,
        txn: DatabaseTxnId,
        table: TableId,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let gc = self
            .compositions
            .get_mut(&txn)
            .and_then(|record| table_object_retirement_mut(record, table))
            .and_then(table_object_gc_mut)
            .and_then(Option::as_mut)
            .ok_or(corrupt("table-object GC complete without intent"))?;
        if gc.complete {
            return Ok(());
        }
        gc.complete = true;
        self.persist()
    }
    #[cfg(test)]
    pub(crate) fn rewrite_intent(
        &mut self,
        intent: RewriteIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let reservation = self
            .rewrite_reservations
            .get(&intent.reservation.transaction)
            .ok_or(corrupt("rewrite intent without reservation"))?;
        if reservation != &intent.reservation
            || self.rewrites.contains_key(&intent.reservation.transaction)
            || self
                .rewrite_losers
                .contains(&intent.reservation.transaction)
        {
            return Err(corrupt("duplicate or mismatched rewrite intent"));
        }
        self.rewrites.insert(intent.reservation.transaction, intent);
        self.persist()
    }

    pub(crate) fn retire_rewrite(&mut self, txn: DatabaseTxnId) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let intent = self
            .rewrites
            .get_mut(&txn)
            .ok_or(corrupt("replacement retirement without rewrite intent"))?;
        if intent.resolved == Some(false) {
            return Err(corrupt("loser rewrite cannot retire source"));
        }
        if intent.retired {
            return Ok(());
        }
        intent.retired = true;
        self.persist()
    }

    pub(crate) fn resolve_rewrite(
        &mut self,
        txn: DatabaseTxnId,
        committed: bool,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if !committed && !self.rewrites.contains_key(&txn) {
            if !self.rewrite_reservations.contains_key(&txn) {
                return Err(corrupt("rewrite resolution without reservation"));
            }
            if !self.rewrite_losers.insert(txn) {
                return Ok(());
            }
            return self.persist();
        }
        let intent = self
            .rewrites
            .get_mut(&txn)
            .ok_or(corrupt("rewrite winner without intent"))?;
        if committed && !intent.retired {
            return Err(corrupt("rewrite winner source is not durably retired"));
        }
        if !committed && intent.retired {
            return Err(corrupt("retired rewrite cannot resolve as loser"));
        }
        if let Some(previous) = intent.resolved {
            return if previous == committed {
                Ok(())
            } else {
                Err(corrupt("conflicting rewrite resolution"))
            };
        }
        intent.resolved = Some(committed);
        self.persist()
    }
    #[cfg(test)]
    pub(crate) fn intent(
        &mut self,
        txn: DatabaseTxnId,
        intent: CreateIntent,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let reservation = self
            .reservations
            .get_mut(&txn)
            .ok_or(corrupt("intent without reservation"))?;
        if reservation.intent.is_some() || reservation.resolved.is_some() {
            return Err(corrupt("out-of-order create intent"));
        }
        reservation.intent = Some(intent);
        self.persist()
    }

    #[cfg(test)]
    pub(crate) fn drop_intent(&mut self, intent: DropIntent) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        if self.reservations.contains_key(&intent.transaction)
            || self.drops.contains_key(&intent.transaction)
        {
            return Err(corrupt("duplicate schema mutation transaction"));
        }
        self.drops.insert(intent.transaction, intent);
        self.persist()
    }

    pub(crate) fn retire_drop(&mut self, txn: DatabaseTxnId) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let intent = self
            .drops
            .get_mut(&txn)
            .ok_or(corrupt("retirement without drop intent"))?;
        if intent.resolved == Some(false) {
            return Err(corrupt("loser drop cannot retire storage"));
        }
        if intent.retired {
            return Ok(());
        }
        intent.retired = true;
        self.persist()
    }

    pub(crate) fn resolve_drop(
        &mut self,
        txn: DatabaseTxnId,
        committed: bool,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let intent = self
            .drops
            .get_mut(&txn)
            .ok_or(corrupt("drop resolution without intent"))?;
        if committed && !intent.retired {
            return Err(corrupt("drop winner is not durably retired"));
        }
        if !committed && intent.retired {
            return Err(corrupt("retired drop cannot resolve as loser"));
        }
        if let Some(previous) = intent.resolved {
            return if previous == committed {
                Ok(())
            } else {
                Err(corrupt("conflicting drop resolution"))
            };
        }
        intent.resolved = Some(committed);
        self.persist()
    }

    pub(crate) fn gc_intent(
        &mut self,
        txn: DatabaseTxnId,
        gc: RetiredHeapGcRecord,
    ) -> Result<(), SchemaMutationError> {
        self.prepare_gc(txn, &gc)?;
        if let Some(drop) = self.drops.get_mut(&txn) {
            drop.gc = Some(gc);
        } else {
            self.rewrites
                .get_mut(&txn)
                .ok_or(corrupt("GC intent without retirement history"))?
                .gc = Some(gc);
        }
        self.persist()
    }

    pub(crate) fn complete_gc(&mut self, txn: DatabaseTxnId) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let gc = if let Some(drop) = self.drops.get_mut(&txn) {
            drop.gc.as_mut()
        } else {
            self.rewrites
                .get_mut(&txn)
                .and_then(|rewrite| rewrite.gc.as_mut())
        }
        .ok_or(corrupt("GC complete without intent"))?;
        if gc.complete {
            return Ok(());
        }
        gc.complete = true;
        self.persist()
    }
    pub(crate) fn resolve(
        &mut self,
        txn: DatabaseTxnId,
        committed: bool,
    ) -> Result<(), SchemaMutationError> {
        self.ensure_ready()?;
        let reservation = self
            .reservations
            .get_mut(&txn)
            .ok_or(corrupt("resolution without reservation"))?;
        if let Some(previous) = reservation.resolved {
            return if previous == committed {
                Ok(())
            } else {
                Err(corrupt("conflicting mutation resolution"))
            };
        }
        reservation.resolved = Some(committed);
        self.persist()
    }
    #[cfg(test)]
    pub(crate) fn inject_sync_failure(&mut self) {
        self.fail_next_sync = true;
    }

    fn persist(&mut self) -> Result<(), SchemaMutationError> {
        // An uncertain metadata rename/sync consumes the in-process allocation and
        // poisons further writes. Reopen selects the complete durable history.
        let result = self
            .encode()
            .and_then(|bytes| file::atomic_write(&self.path, &bytes, false).map_err(Into::into));
        #[cfg(test)]
        let result = if result.is_ok() && std::mem::take(&mut self.fail_next_sync) {
            Err(file::io(
                "sync mutation journal (injected uncertain result)",
                &self.path,
                std::io::Error::other("injected sync result"),
            )
            .into())
        } else {
            result
        };
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, SchemaMutationError> {
        let mut w = Writer(self.incarnation.to_vec());
        w.string(&self.coordinator)?;
        let create_count = self
            .reservations
            .values()
            .map(|r| 1 + usize::from(r.intent.is_some()) + usize::from(r.resolved.is_some()))
            .sum::<usize>();
        let drop_count = self
            .drops
            .values()
            .map(|d| {
                1 + usize::from(d.retired)
                    + usize::from(d.resolved.is_some())
                    + usize::from(d.gc.is_some())
                    + usize::from(d.gc.as_ref().is_some_and(|gc| gc.complete))
            })
            .sum::<usize>();
        let rewrite_count = self
            .rewrite_reservations
            .values()
            .map(|reservation| {
                1 + self
                    .rewrites
                    .get(&reservation.transaction)
                    .map_or(0, |rewrite| {
                        1 + usize::from(rewrite.retired)
                            + usize::from(rewrite.resolved.is_some())
                            + usize::from(rewrite.gc.is_some())
                            + usize::from(rewrite.gc.as_ref().is_some_and(|gc| gc.complete))
                    })
                    + usize::from(
                        !self.rewrites.contains_key(&reservation.transaction)
                            && self.rewrite_losers.contains(&reservation.transaction),
                    )
            })
            .sum::<usize>();
        let composition_count = self
            .compositions
            .values()
            .map(|record| {
                record.table_reservations.len()
                    + record.reservations.len()
                    + record.index_reservations.len()
                    + usize::from(record.intent.is_some())
                    + usize::from(record.index_intent.is_some())
                    + usize::from(record.table_intent.is_some())
                    + record.intent.as_ref().map_or(0, |intent| {
                        intent.tables.iter().filter(|plan| plan.retired).count()
                    })
                    + record.intent.as_ref().map_or(0, |intent| {
                        intent
                            .tables
                            .iter()
                            .map(|plan| {
                                usize::from(plan.gc.is_some())
                                    + usize::from(plan.gc.as_ref().is_some_and(|gc| gc.complete))
                            })
                            .sum::<usize>()
                    })
                    + usize::from(record.resolution.is_some())
                    + record.index_intent.as_ref().map_or(0, |intent| {
                        intent
                            .tables
                            .iter()
                            .filter_map(SchemaIndexTablePlan::replacement)
                            .filter(|plan| plan.retired)
                            .count()
                    })
                    + record.index_intent.as_ref().map_or(0, |intent| {
                        intent
                            .tables
                            .iter()
                            .filter_map(SchemaIndexTablePlan::replacement)
                            .map(|plan| {
                                usize::from(plan.gc.is_some())
                                    + usize::from(plan.gc.as_ref().is_some_and(|gc| gc.complete))
                            })
                            .sum::<usize>()
                    })
                    + record.table_intent.as_ref().map_or(0, |intent| {
                        intent
                            .tables
                            .iter()
                            .filter(|plan| match plan {
                                SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                                    replacement.retired
                                }
                                SchemaIndexTablePlan::DropHeap { retired, .. } => *retired,
                                _ => false,
                            })
                            .count()
                    })
                    + record.table_intent.as_ref().map_or(0, |intent| {
                        intent
                            .tables
                            .iter()
                            .map(|plan| {
                                usize::from(table_object_gc(plan).is_some())
                                    + usize::from(
                                        table_object_gc(plan).is_some_and(|gc| gc.complete),
                                    )
                            })
                            .sum::<usize>()
                    })
            })
            .sum::<usize>();
        let intent_count = self
            .stage_intents
            .len()
            .checked_add(self.finalization_intents.len())
            .ok_or(corrupt("too many backfill intents"))?;
        let count = create_count
            .checked_add(drop_count)
            .and_then(|count| count.checked_add(rewrite_count))
            .and_then(|count| count.checked_add(composition_count))
            .and_then(|count| count.checked_add(intent_count))
            .ok_or(corrupt("too many journal records"))?;
        w.u32(u32::try_from(count).map_err(|_| corrupt("too many journal records"))?);
        let mut transactions = self
            .reservations
            .keys()
            .chain(self.drops.keys())
            .chain(self.rewrite_reservations.keys())
            .chain(self.compositions.keys())
            .chain(self.stage_intents.keys())
            .chain(self.finalization_intents.keys())
            .copied()
            .collect::<Vec<_>>();
        transactions.sort_unstable();
        transactions.dedup();
        for txn in transactions {
            if let Some(r) = self.reservations.get(&txn) {
                let mut record = Writer(vec![1]);
                record.u64(r.transaction.0);
                record.u64(r.table.0);
                record.u64(r.storage.0);
                record.u64(r.base_generation.0);
                record.u64(r.base_epoch);
                put_record(&mut w, &record.0)?;
                if let Some(intent) = &r.intent {
                    let mut record = Writer(vec![2]);
                    record.u64(r.transaction.0);
                    record.0.extend_from_slice(&intent.snapshot_digest);
                    record.0.extend_from_slice(&intent.fragment.encode()?);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(committed) = r.resolved {
                    let mut record = Writer(vec![if committed { 4 } else { 3 }]);
                    record.u64(r.transaction.0);
                    put_record(&mut w, &record.0)?;
                }
            } else if let Some(d) = self.drops.get(&txn) {
                let mut record = Writer(vec![5]);
                record.u64(d.transaction.0);
                record.u64(d.target_generation.0);
                record.u64(d.target_epoch);
                record.0.extend_from_slice(&d.snapshot_digest);
                record.0.extend_from_slice(&d.fragment.encode()?);
                put_record(&mut w, &record.0)?;
                if d.retired {
                    let mut record = Writer(vec![6]);
                    record.u64(d.transaction.0);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(committed) = d.resolved {
                    let mut record = Writer(vec![if committed { 8 } else { 7 }]);
                    record.u64(d.transaction.0);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(gc) = &d.gc {
                    let mut record = Writer(vec![9]);
                    record.u64(d.transaction.0);
                    record.u64(gc.coordinator_horizon.0);
                    record.0.extend_from_slice(&gc.manifest_digest);
                    put_record(&mut w, &record.0)?;
                    if gc.complete {
                        let mut record = Writer(vec![10]);
                        record.u64(d.transaction.0);
                        put_record(&mut w, &record.0)?;
                    }
                }
            } else if let Some(reservation) = self.rewrite_reservations.get(&txn) {
                let mut record = Writer(vec![11]);
                record.u64(reservation.transaction.0);
                record.u64(reservation.table.0);
                record.u64(reservation.storage.0);
                record.u32(reservation.column.map_or(0, |column| column.0));
                record.u64(reservation.base_generation.0);
                record.u64(reservation.base_epoch);
                put_record(&mut w, &record.0)?;
                if let Some(rewrite) = self.rewrites.get(&txn) {
                    let mut record = Writer(vec![12]);
                    record.u64(txn.0);
                    record.0.extend_from_slice(&rewrite.snapshot_digest);
                    encode_operation(&mut record, &rewrite.operation)?;
                    let base = rewrite.base.encode()?;
                    record.u32(
                        u32::try_from(base.len()).map_err(|_| corrupt("rewrite base too large"))?,
                    );
                    record.0.extend_from_slice(&base);
                    let target = rewrite.target.encode()?;
                    record.u32(
                        u32::try_from(target.len())
                            .map_err(|_| corrupt("rewrite target too large"))?,
                    );
                    record.0.extend_from_slice(&target);
                    put_record(&mut w, &record.0)?;
                    if rewrite.retired {
                        let mut record = Writer(vec![13]);
                        record.u64(txn.0);
                        put_record(&mut w, &record.0)?;
                    }
                    if let Some(committed) = rewrite.resolved {
                        let mut record = Writer(vec![if committed { 15 } else { 14 }]);
                        record.u64(txn.0);
                        put_record(&mut w, &record.0)?;
                    }
                    if let Some(gc) = &rewrite.gc {
                        let mut record = Writer(vec![9]);
                        record.u64(txn.0);
                        record.u64(gc.coordinator_horizon.0);
                        record.0.extend_from_slice(&gc.manifest_digest);
                        put_record(&mut w, &record.0)?;
                        if gc.complete {
                            let mut record = Writer(vec![10]);
                            record.u64(txn.0);
                            put_record(&mut w, &record.0)?;
                        }
                    }
                } else if self.rewrite_losers.contains(&txn) {
                    let mut record = Writer(vec![14]);
                    record.u64(txn.0);
                    put_record(&mut w, &record.0)?;
                }
            } else if let Some(composition) = self.compositions.get(&txn) {
                for reservation in &composition.table_reservations {
                    let mut record = Writer(vec![26]);
                    record.u64(txn.0);
                    record.u64(reservation.table.0);
                    record.u64(reservation.next_table_id.map_or(0, |table| table.0));
                    put_record(&mut w, &record.0)?;
                }
                let mut reservations = composition.reservations.iter().collect::<Vec<_>>();
                reservations.sort_by_key(|entry| (entry.table, entry.column));
                for reservation in reservations {
                    let mut record = Writer(vec![16]);
                    record.u64(txn.0);
                    record.u64(reservation.table.0);
                    record.u32(reservation.column.0);
                    record.u32(reservation.next_column_id.map_or(0, |column| column.0));
                    put_record(&mut w, &record.0)?;
                }
                let mut index_reservations =
                    composition.index_reservations.iter().collect::<Vec<_>>();
                index_reservations.sort_by_key(|entry| (entry.table, entry.index));
                for reservation in index_reservations {
                    let mut record = Writer(vec![24]);
                    record.u64(txn.0);
                    record.u64(reservation.table.0);
                    record.u64(reservation.table_version.0);
                    record
                        .0
                        .extend_from_slice(reservation.fingerprint.as_bytes());
                    record.u64(reservation.index.0);
                    record.u64(reservation.next_index_id.map_or(0, |index| index.0));
                    put_record(&mut w, &record.0)?;
                }
                if let Some(intent) = &composition.intent {
                    let mut record = Writer(vec![17]);
                    record.u64(txn.0);
                    record.u64(intent.base_generation.0);
                    record.u64(intent.target_generation.0);
                    record.u64(intent.base_epoch);
                    record.u64(intent.target_epoch);
                    record.u32(intent.action_count);
                    record.0.extend_from_slice(&intent.action_digest);
                    record.0.extend_from_slice(&intent.snapshot_digest);
                    record.u32(
                        u32::try_from(intent.tables.len())
                            .map_err(|_| corrupt("too many composition table plans"))?,
                    );
                    for plan in &intent.tables {
                        let base = plan.base.encode()?;
                        let target = plan.target.encode()?;
                        record.u32(
                            u32::try_from(base.len())
                                .map_err(|_| corrupt("composition base fragment too large"))?,
                        );
                        record.0.extend_from_slice(&base);
                        record.u32(
                            u32::try_from(target.len())
                                .map_err(|_| corrupt("composition target fragment too large"))?,
                        );
                        record.0.extend_from_slice(&target);
                    }
                    if record.0.len() > 4 * 1024 * 1024 {
                        return Err(corrupt("aggregate composition intent exceeds 4 MiB"));
                    }
                    put_record(&mut w, &record.0)?;
                    for plan in intent.tables.iter().filter(|plan| plan.retired) {
                        let mut record = Writer(vec![18]);
                        record.u64(txn.0);
                        record.u64(plan.table().0);
                        put_record(&mut w, &record.0)?;
                    }
                }
                if let Some(intent) = &composition.index_intent {
                    let mut record = Writer(vec![25]);
                    record.u64(txn.0);
                    record.u64(intent.base_generation.0);
                    record.u64(intent.target_generation.map_or(0, |value| value.0));
                    record.u64(intent.base_epoch);
                    record.u64(intent.target_epoch.unwrap_or(0));
                    record.u32(intent.action_count);
                    record.0.extend_from_slice(&intent.action_digest);
                    match intent.snapshot_digest {
                        Some(digest) => {
                            record.u8(1);
                            record.0.extend_from_slice(&digest);
                        }
                        None => record.u8(0),
                    }
                    record.u32(
                        u32::try_from(intent.tables.len())
                            .map_err(|_| corrupt("too many schema/index table plans"))?,
                    );
                    for plan in &intent.tables {
                        encode_schema_index_table_plan(&mut record, plan)?;
                    }
                    if record.0.len() > 4 * 1024 * 1024 {
                        return Err(corrupt("aggregate schema/index intent exceeds 4 MiB"));
                    }
                    put_record(&mut w, &record.0)?;
                    for replacement in intent
                        .tables
                        .iter()
                        .filter_map(SchemaIndexTablePlan::replacement)
                        .filter(|plan| plan.retired)
                    {
                        let mut record = Writer(vec![18]);
                        record.u64(txn.0);
                        record.u64(replacement.table().0);
                        put_record(&mut w, &record.0)?;
                    }
                    for replacement in intent
                        .tables
                        .iter()
                        .filter_map(SchemaIndexTablePlan::replacement)
                        .filter(|plan| plan.gc.is_some())
                    {
                        let gc = replacement
                            .gc
                            .as_ref()
                            .ok_or(corrupt("schema/index composition GC disappeared"))?;
                        let mut record = Writer(vec![22]);
                        record.u64(txn.0);
                        record.u64(replacement.table().0);
                        record.u64(gc.coordinator_horizon.0);
                        record.0.extend_from_slice(&gc.manifest_digest);
                        put_record(&mut w, &record.0)?;
                        if gc.complete {
                            let mut record = Writer(vec![23]);
                            record.u64(txn.0);
                            record.u64(replacement.table().0);
                            put_record(&mut w, &record.0)?;
                        }
                    }
                }
                if let Some(intent) = &composition.table_intent {
                    let mut record = Writer(vec![27]);
                    record.u64(txn.0);
                    record.u64(intent.base_generation.0);
                    record.u64(intent.target_generation.0);
                    record.u64(intent.base_epoch);
                    record.u64(intent.target_epoch);
                    record.u32(intent.action_count);
                    record.0.extend_from_slice(&intent.action_digest);
                    record.0.extend_from_slice(&intent.snapshot_digest);
                    record.u32(
                        u32::try_from(intent.tables.len())
                            .map_err(|_| corrupt("too many table-object plans"))?,
                    );
                    for plan in &intent.tables {
                        encode_table_object_plan(&mut record, plan)?;
                    }
                    if record.0.len() > 4 * 1024 * 1024 {
                        return Err(corrupt("table-object intent exceeds 4 MiB"));
                    }
                    put_record(&mut w, &record.0)?;
                    for plan in &intent.tables {
                        let (kind, retired) = match plan {
                            SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                                (1, replacement.retired)
                            }
                            SchemaIndexTablePlan::DropHeap { retired, .. } => (2, *retired),
                            _ => (0, false),
                        };
                        if retired {
                            let mut record = Writer(vec![28]);
                            record.u64(txn.0);
                            record.u8(kind);
                            record.u64(plan.table().0);
                            put_record(&mut w, &record.0)?;
                        }
                    }
                }
                if let Some(resolution) = composition.resolution {
                    let mut record = Writer(vec![match resolution {
                        CompositionResolution::Loser => 19,
                        CompositionResolution::NoEffectiveChange => 20,
                        CompositionResolution::Winner => 21,
                    }]);
                    record.u64(txn.0);
                    put_record(&mut w, &record.0)?;
                }
                if let Some(intent) = &composition.intent {
                    for plan in intent.tables.iter().filter(|plan| plan.gc.is_some()) {
                        let gc = plan
                            .gc
                            .as_ref()
                            .ok_or(corrupt("composition GC disappeared"))?;
                        let mut record = Writer(vec![22]);
                        record.u64(txn.0);
                        record.u64(plan.table().0);
                        record.u64(gc.coordinator_horizon.0);
                        record.0.extend_from_slice(&gc.manifest_digest);
                        put_record(&mut w, &record.0)?;
                        if gc.complete {
                            let mut record = Writer(vec![23]);
                            record.u64(txn.0);
                            record.u64(plan.table().0);
                            put_record(&mut w, &record.0)?;
                        }
                    }
                }
                if let Some(intent) = &composition.table_intent {
                    for plan in intent
                        .tables
                        .iter()
                        .filter(|plan| table_object_gc(plan).is_some())
                    {
                        let gc =
                            table_object_gc(plan).ok_or(corrupt("table-object GC disappeared"))?;
                        let mut record = Writer(vec![29]);
                        record.u64(txn.0);
                        record.u64(plan.table().0);
                        record.u64(gc.coordinator_horizon.0);
                        record.0.extend_from_slice(&gc.manifest_digest);
                        put_record(&mut w, &record.0)?;
                        if gc.complete {
                            let mut record = Writer(vec![30]);
                            record.u64(txn.0);
                            record.u64(plan.table().0);
                            put_record(&mut w, &record.0)?;
                        }
                    }
                }
            }
        }
        for intent in self.stage_intents.values() {
            let mut record = Writer(vec![31]);
            record.u64(intent.transaction.0);
            record.u64(intent.table.0);
            record.u64(intent.storage.0);
            record.u64(intent.base_generation.0);
            record.u64(intent.base_epoch);
            record.string(&intent.stage_locator)?;
            record.string(&intent.final_locator)?;
            record.0.extend_from_slice(&intent.digest);
            let snapshot = intent.provisional.encode()?;
            record.u32(
                u32::try_from(snapshot.len())
                    .map_err(|_| corrupt("backfill stage snapshot too large"))?,
            );
            record.0.extend_from_slice(&snapshot);
            put_record(&mut w, &record.0)?;
        }
        for intent in self.finalization_intents.values() {
            let mut record = Writer(vec![32]);
            record.u64(intent.transaction.0);
            record.u64(intent.table.0);
            record.u64(intent.storage.0);
            record.string(&intent.stage_locator)?;
            record.string(&intent.final_locator)?;
            record.0.extend_from_slice(&intent.digest);
            let snapshot = intent.final_snapshot.encode()?;
            record.u32(
                u32::try_from(snapshot.len())
                    .map_err(|_| corrupt("backfill final snapshot too large"))?,
            );
            record.0.extend_from_slice(&snapshot);
            put_record(&mut w, &record.0)?;
        }
        let bytes = envelope(b"NBSJ", &w.0)?;
        Self::decode(&bytes)?;
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SchemaMutationError> {
        let mut reader = Reader(open_envelope(bytes, b"NBSJ")?);
        let incarnation = reader
            .take(16)?
            .try_into()
            .map_err(|_| corrupt("journal incarnation"))?;
        if incarnation == [0; 16] {
            return Err(corrupt("zero journal incarnation"));
        }
        let coordinator = reader.string()?;
        if coordinator.is_empty()
            || Path::new(&coordinator).is_absolute()
            || coordinator.as_bytes().contains(&0)
        {
            return Err(corrupt("invalid journal coordinator locator"));
        }
        let count = reader.count(65536, 29)?;
        let mut reservations: BTreeMap<DatabaseTxnId, Reservation> = BTreeMap::new();
        let mut drops: BTreeMap<DatabaseTxnId, DropIntent> = BTreeMap::new();
        let mut rewrite_reservations: BTreeMap<DatabaseTxnId, RewriteReservation> = BTreeMap::new();
        let mut rewrites: BTreeMap<DatabaseTxnId, RewriteIntent> = BTreeMap::new();
        let mut rewrite_losers = BTreeSet::new();
        let mut compositions: BTreeMap<DatabaseTxnId, CompositionRecord> = BTreeMap::new();
        let mut stage_intents = BTreeMap::new();
        let mut finalization_intents = BTreeMap::new();
        let mut last_columns: BTreeMap<TableId, ColumnId> = BTreeMap::new();
        let mut last_indexes: BTreeMap<TableId, IndexId> = BTreeMap::new();
        let mut last = (0, 0, 0, 0, 0);
        let mut current = None;
        for _ in 0..count {
            let length =
                usize::try_from(reader.u32()?).map_err(|_| corrupt("record length overflow"))?;
            let mut record = Reader(open_envelope(reader.take(length)?, b"NBSR")?);
            let tag = record.u8()?;
            let txn = DatabaseTxnId(record.u64()?);
            if txn.0 == 0 {
                return Err(corrupt("zero mutation transaction"));
            }
            match tag {
                1 => {
                    let table = TableId(record.u64()?);
                    let storage = StorageId(record.u64()?);
                    let generation = SchemaGeneration(record.u64()?);
                    let epoch = record.u64()?;
                    if txn.0 <= last.0
                        || table.0 <= last.1
                        || storage.0 <= last.2
                        || generation.0 == 0
                        || generation.0 < last.3
                        || epoch == 0
                        || epoch < last.4
                        || generation.0.checked_add(1).is_none()
                        || epoch.checked_add(1).is_none()
                    {
                        return Err(corrupt("duplicate, exhausted or nonmonotonic reservation"));
                    }
                    if let Some(previous) = current {
                        if reservations
                            .get(&previous)
                            .is_some_and(|r| r.resolved.is_none())
                            || drops
                                .get(&previous)
                                .is_some_and(|drop| drop.resolved.is_none())
                            || rewrite_reservations.contains_key(&previous)
                                && !rewrite_losers.contains(&previous)
                                && rewrites
                                    .get(&previous)
                                    .is_none_or(|rewrite| rewrite.resolved.is_none())
                            || compositions
                                .get(&previous)
                                .is_some_and(|composition| composition.resolution.is_none())
                        {
                            return Err(corrupt("overlapping schema transactions"));
                        }
                    }
                    last = (txn.0, table.0, storage.0, generation.0, epoch);
                    current = Some(txn);
                    reservations.insert(
                        txn,
                        Reservation {
                            transaction: txn,
                            table,
                            storage,
                            base_generation: generation,
                            base_epoch: epoch,
                            intent: None,
                            resolved: None,
                        },
                    );
                }
                2 => {
                    if current != Some(txn) {
                        return Err(corrupt("intent for unknown transaction"));
                    }
                    let r = reservations
                        .get_mut(&txn)
                        .ok_or(corrupt("intent without reservation"))?;
                    if r.intent.is_some() || r.resolved.is_some() {
                        return Err(corrupt("duplicate or out-of-order intent"));
                    }
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("intent digest"))?;
                    let fragment = SchemaCatalogSnapshot::decode(record.0)?;
                    record.0 = &[];
                    if fragment.incarnation != incarnation
                        || fragment.epoch != r.base_epoch + 1
                        || fragment.committed.generation.0 != r.base_generation.0 + 1
                        || fragment.committed.schema.tables().len() != 1
                        || fragment.storages.len() != 1
                        || fragment.committed.schema.tables()[0].id != r.table
                        || fragment.storages[0].id != r.storage
                        || fragment.coordinator.as_deref() != Some(&coordinator)
                        || fragment.partition_evidence.is_some()
                        || fragment.committed.tables[0].version.0 != 1
                        || fragment.committed.schema.tables()[0]
                            .columns
                            .iter()
                            .enumerate()
                            .any(|(i, c)| c.id.0 as usize != i + 1 || c.primary_key)
                        || fragment.committed.tables[0]
                            .next_column_id
                            .map(|c| c.0 as usize)
                            != Some(fragment.committed.schema.tables()[0].columns.len() + 1)
                        || !matches!(
                            fragment.storages[0].kind,
                            crate::schema_catalog::CatalogStorageKind::Heap
                        )
                    {
                        return Err(corrupt("create intent identity or schema mismatch"));
                    }
                    if !matches!(
                        fragment.placements.tables[0].placement,
                        crate::registry::TablePlacement::Single { .. }
                    ) || fragment.committed.next_table_id
                        != r.table.0.checked_add(1).map(TableId)
                        || fragment.committed.next_storage_id
                            != r.storage.0.checked_add(1).map(StorageId)
                    {
                        return Err(corrupt("create intent placement or allocator mismatch"));
                    }
                    r.intent = Some(CreateIntent {
                        fragment,
                        snapshot_digest,
                    });
                }
                3 | 4 => {
                    if current != Some(txn) {
                        return Err(corrupt("resolution for unknown transaction"));
                    }
                    let r = reservations
                        .get_mut(&txn)
                        .ok_or(corrupt("resolution without reservation"))?;
                    if r.resolved.is_some() || (tag == 4 && r.intent.is_none()) {
                        return Err(corrupt("duplicate or out-of-order resolution"));
                    }
                    r.resolved = Some(tag == 4);
                }
                5 => {
                    if txn.0 <= last.0 {
                        return Err(corrupt("duplicate or nonmonotonic drop transaction"));
                    }
                    if let Some(previous) = current {
                        let unresolved_create = reservations
                            .get(&previous)
                            .is_some_and(|r| r.resolved.is_none());
                        let unresolved_drop =
                            drops.get(&previous).is_some_and(|d| d.resolved.is_none());
                        let unresolved_rewrite = rewrite_reservations.contains_key(&previous)
                            && !rewrite_losers.contains(&previous)
                            && rewrites
                                .get(&previous)
                                .is_none_or(|rewrite| rewrite.resolved.is_none());
                        let unresolved_composition = compositions
                            .get(&previous)
                            .is_some_and(|composition| composition.resolution.is_none());
                        if unresolved_create
                            || unresolved_drop
                            || unresolved_rewrite
                            || unresolved_composition
                        {
                            return Err(corrupt("overlapping schema transactions"));
                        }
                    }
                    let target_generation = SchemaGeneration(record.u64()?);
                    let target_epoch = record.u64()?;
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("drop intent digest"))?;
                    let fragment = SchemaCatalogSnapshot::decode(record.0)?;
                    record.0 = &[];
                    validate_drop_fragment(
                        &fragment,
                        target_generation,
                        target_epoch,
                        incarnation,
                        &coordinator,
                    )?;
                    if fragment.committed.generation.0 < last.3 || fragment.epoch < last.4 {
                        return Err(corrupt("nonmonotonic drop base"));
                    }
                    last.0 = txn.0;
                    last.1 = last.1.max(allocator_predecessor(
                        fragment.committed.next_table_id.map(|id| id.0),
                    )?);
                    last.2 = last.2.max(allocator_predecessor(
                        fragment.committed.next_storage_id.map(|id| id.0),
                    )?);
                    last.3 = fragment.committed.generation.0;
                    last.4 = fragment.epoch;
                    current = Some(txn);
                    drops.insert(
                        txn,
                        DropIntent {
                            transaction: txn,
                            fragment,
                            target_generation,
                            target_epoch,
                            snapshot_digest,
                            retired: false,
                            resolved: None,
                            gc: None,
                        },
                    );
                }
                6 => {
                    if current != Some(txn) {
                        return Err(corrupt("retirement for unknown transaction"));
                    }
                    let intent = drops
                        .get_mut(&txn)
                        .ok_or(corrupt("retirement without drop intent"))?;
                    if intent.retired || intent.resolved.is_some() {
                        return Err(corrupt("duplicate or out-of-order retirement"));
                    }
                    intent.retired = true;
                }
                7 | 8 => {
                    if current != Some(txn) {
                        return Err(corrupt("drop resolution for unknown transaction"));
                    }
                    let intent = drops
                        .get_mut(&txn)
                        .ok_or(corrupt("drop resolution without intent"))?;
                    if intent.resolved.is_some() || (tag == 8 && !intent.retired) {
                        return Err(corrupt("duplicate or out-of-order drop resolution"));
                    }
                    if tag == 7 && intent.retired {
                        return Err(corrupt("retired drop resolved as loser"));
                    }
                    intent.resolved = Some(tag == 8);
                }
                9 => {
                    if current != Some(txn) {
                        return Err(corrupt("GC intent for unknown transaction"));
                    }
                    let coordinator_horizon = DatabaseTxnId(record.u64()?);
                    let manifest_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("GC manifest digest"))?;
                    let terminal = drops
                        .get(&txn)
                        .map(|drop| (drop.retired, drop.resolved, drop.gc.is_some()))
                        .or_else(|| {
                            rewrites.get(&txn).map(|rewrite| {
                                (rewrite.retired, rewrite.resolved, rewrite.gc.is_some())
                            })
                        })
                        .ok_or(corrupt("GC intent without retirement history"))?;
                    if !terminal.0
                        || terminal.1 != Some(true)
                        || terminal.2
                        || coordinator_horizon.0 < txn.0
                    {
                        return Err(corrupt("duplicate or out-of-order GC intent"));
                    }
                    let gc = RetiredHeapGcRecord {
                        coordinator_horizon,
                        manifest_digest,
                        complete: false,
                    };
                    if let Some(drop) = drops.get_mut(&txn) {
                        drop.gc = Some(gc);
                    } else {
                        rewrites
                            .get_mut(&txn)
                            .ok_or(corrupt("GC rewrite history disappeared"))?
                            .gc = Some(gc);
                    }
                }
                10 => {
                    if current != Some(txn) {
                        return Err(corrupt("GC complete for unknown transaction"));
                    }
                    let gc = if let Some(drop) = drops.get_mut(&txn) {
                        drop.gc.as_mut()
                    } else {
                        rewrites
                            .get_mut(&txn)
                            .and_then(|rewrite| rewrite.gc.as_mut())
                    }
                    .ok_or(corrupt("GC complete without intent"))?;
                    if gc.complete {
                        return Err(corrupt("duplicate GC complete"));
                    }
                    gc.complete = true;
                }
                11 => {
                    let table = TableId(record.u64()?);
                    let storage = StorageId(record.u64()?);
                    let raw_column = record.u32()?;
                    let column = (raw_column != 0).then_some(ColumnId(raw_column));
                    let generation = SchemaGeneration(record.u64()?);
                    let epoch = record.u64()?;
                    if txn.0 <= last.0
                        || table.0 == 0
                        || storage.0 <= last.2
                        || generation.0 == 0
                        || generation.0 < last.3
                        || epoch == 0
                        || epoch < last.4
                        || generation.0.checked_add(1).is_none()
                        || epoch.checked_add(1).is_none()
                    {
                        return Err(corrupt("invalid or nonmonotonic rewrite reservation"));
                    }
                    if column.is_some_and(|column| {
                        last_columns
                            .get(&table)
                            .is_some_and(|previous| *previous >= column)
                    }) {
                        return Err(corrupt("reused rewrite ColumnId reservation"));
                    }
                    if let Some(previous) = current {
                        let unresolved_create = reservations
                            .get(&previous)
                            .is_some_and(|reservation| reservation.resolved.is_none());
                        let unresolved_drop = drops
                            .get(&previous)
                            .is_some_and(|drop| drop.resolved.is_none());
                        let unresolved_rewrite = rewrite_reservations.contains_key(&previous)
                            && !rewrite_losers.contains(&previous)
                            && rewrites
                                .get(&previous)
                                .is_none_or(|rewrite| rewrite.resolved.is_none());
                        let unresolved_composition = compositions
                            .get(&previous)
                            .is_some_and(|composition| composition.resolution.is_none());
                        if unresolved_create
                            || unresolved_drop
                            || unresolved_rewrite
                            || unresolved_composition
                        {
                            return Err(corrupt("overlapping schema transactions"));
                        }
                    }
                    last = (txn.0, last.1.max(table.0), storage.0, generation.0, epoch);
                    current = Some(txn);
                    if let Some(column) = column {
                        last_columns.insert(table, column);
                    }
                    rewrite_reservations.insert(
                        txn,
                        RewriteReservation {
                            transaction: txn,
                            table,
                            storage,
                            column,
                            base_generation: generation,
                            base_epoch: epoch,
                        },
                    );
                }
                12 => {
                    if current != Some(txn) || rewrites.contains_key(&txn) {
                        return Err(corrupt("rewrite intent without current reservation"));
                    }
                    let reservation = rewrite_reservations
                        .get(&txn)
                        .cloned()
                        .ok_or(corrupt("rewrite intent without reservation"))?;
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("rewrite intent digest"))?;
                    let operation = decode_operation(&mut record)?;
                    let base_len = usize::try_from(record.u32()?)
                        .map_err(|_| corrupt("rewrite base length overflow"))?;
                    let base = SchemaCatalogSnapshot::decode(record.take(base_len)?)?;
                    let target_len = usize::try_from(record.u32()?)
                        .map_err(|_| corrupt("rewrite target length overflow"))?;
                    let target = SchemaCatalogSnapshot::decode(record.take(target_len)?)?;
                    validate_rewrite_fragments(
                        &reservation,
                        &operation,
                        &base,
                        &target,
                        incarnation,
                        &coordinator,
                    )?;
                    rewrites.insert(
                        txn,
                        RewriteIntent {
                            reservation,
                            operation,
                            base,
                            target,
                            snapshot_digest,
                            retired: false,
                            resolved: None,
                            gc: None,
                        },
                    );
                }
                13 => {
                    if current != Some(txn) {
                        return Err(corrupt("rewrite retirement for unknown transaction"));
                    }
                    let rewrite = rewrites
                        .get_mut(&txn)
                        .ok_or(corrupt("rewrite retirement without intent"))?;
                    if rewrite.retired || rewrite.resolved.is_some() {
                        return Err(corrupt("duplicate or out-of-order rewrite retirement"));
                    }
                    rewrite.retired = true;
                }
                14 | 15 => {
                    if current != Some(txn) {
                        return Err(corrupt("rewrite resolution for unknown transaction"));
                    }
                    if tag == 14 && !rewrites.contains_key(&txn) {
                        if !rewrite_reservations.contains_key(&txn) || !rewrite_losers.insert(txn) {
                            return Err(corrupt("duplicate rewrite reservation loser"));
                        }
                    } else {
                        let rewrite = rewrites
                            .get_mut(&txn)
                            .ok_or(corrupt("rewrite winner without intent"))?;
                        if rewrite.resolved.is_some() || (tag == 15 && !rewrite.retired) {
                            return Err(corrupt("duplicate or out-of-order rewrite resolution"));
                        }
                        if tag == 14 && rewrite.retired {
                            return Err(corrupt("retired rewrite resolved as loser"));
                        }
                        rewrite.resolved = Some(tag == 15);
                    }
                }
                26 => {
                    let table = TableId(record.u64()?);
                    let raw_next = record.u64()?;
                    let next_table_id = (raw_next != 0).then_some(TableId(raw_next));
                    if table.0 == 0
                        || next_table_id.map(|next| next.0) != table.0.checked_add(1)
                        || table.0 <= last.1
                        || reservations.contains_key(&txn)
                        || drops.contains_key(&txn)
                        || rewrite_reservations.contains_key(&txn)
                    {
                        return Err(corrupt("invalid composition TableId reservation"));
                    }
                    if current != Some(txn) {
                        if txn.0 <= last.0 {
                            return Err(corrupt("nonmonotonic composition transaction"));
                        }
                        if let Some(previous) = current {
                            if compositions
                                .get(&previous)
                                .is_some_and(|entry| entry.resolution.is_none())
                            {
                                return Err(corrupt("overlapping schema transactions"));
                            }
                        }
                        last.0 = txn.0;
                        current = Some(txn);
                    }
                    last.1 = table.0;
                    let composition =
                        compositions
                            .entry(txn)
                            .or_insert_with(|| CompositionRecord {
                                transaction: txn,
                                table_reservations: Vec::new(),
                                reservations: Vec::new(),
                                intent: None,
                                index_reservations: Vec::new(),
                                index_intent: None,
                                table_intent: None,
                                resolution: None,
                            });
                    if composition.intent.is_some()
                        || composition.index_intent.is_some()
                        || composition.table_intent.is_some()
                        || composition.resolution.is_some()
                    {
                        return Err(corrupt("out-of-order TableId reservation"));
                    }
                    composition
                        .table_reservations
                        .push(CompositionTableReservation {
                            transaction: txn,
                            table,
                            next_table_id,
                        });
                }
                16 => {
                    let table = TableId(record.u64()?);
                    let column = ColumnId(record.u32()?);
                    let raw_next = record.u32()?;
                    let next_column_id = (raw_next != 0).then_some(ColumnId(raw_next));
                    if table.0 == 0
                        || column.0 == 0
                        || next_column_id.map(|next| next.0) != column.0.checked_add(1)
                        || last_columns
                            .get(&table)
                            .is_some_and(|previous| *previous >= column)
                        || reservations.contains_key(&txn)
                        || drops.contains_key(&txn)
                        || rewrite_reservations.contains_key(&txn)
                    {
                        return Err(corrupt("invalid composition ColumnId reservation"));
                    }
                    if current != Some(txn) {
                        if txn.0 <= last.0 {
                            return Err(corrupt("nonmonotonic composition transaction"));
                        }
                        if let Some(previous) = current {
                            let unresolved = reservations
                                .get(&previous)
                                .is_some_and(|entry| entry.resolved.is_none())
                                || drops
                                    .get(&previous)
                                    .is_some_and(|entry| entry.resolved.is_none())
                                || rewrite_reservations.contains_key(&previous)
                                    && !rewrite_losers.contains(&previous)
                                    && rewrites
                                        .get(&previous)
                                        .is_none_or(|entry| entry.resolved.is_none())
                                || compositions
                                    .get(&previous)
                                    .is_some_and(|entry| entry.resolution.is_none());
                            if unresolved {
                                return Err(corrupt("overlapping schema transactions"));
                            }
                        }
                        last.0 = txn.0;
                        current = Some(txn);
                    }
                    let composition =
                        compositions
                            .entry(txn)
                            .or_insert_with(|| CompositionRecord {
                                transaction: txn,
                                table_reservations: Vec::new(),
                                reservations: Vec::new(),
                                intent: None,
                                index_reservations: Vec::new(),
                                index_intent: None,
                                table_intent: None,
                                resolution: None,
                            });
                    if composition.intent.is_some()
                        || composition.index_intent.is_some()
                        || composition.resolution.is_some()
                        || composition.reservations.last().is_some_and(|previous| {
                            (previous.table, previous.column) >= (table, column)
                        })
                    {
                        return Err(corrupt(
                            "duplicate, noncanonical or out-of-order composition reservation",
                        ));
                    }
                    composition.reservations.push(CompositionColumnReservation {
                        transaction: txn,
                        table,
                        column,
                        next_column_id,
                    });
                    last_columns.insert(table, column);
                }
                24 => {
                    let table = TableId(record.u64()?);
                    let table_version = TableSchemaVersion(record.u64()?);
                    let fingerprint = SchemaFingerprint::from_bytes(
                        record
                            .take(32)?
                            .try_into()
                            .map_err(|_| corrupt("IndexId reservation fingerprint"))?,
                    );
                    let index = IndexId(record.u64()?);
                    let raw_next = record.u64()?;
                    let next_index_id = (raw_next != 0).then_some(IndexId(raw_next));
                    if table.0 == 0
                        || table_version.0 == 0
                        || index.0 == 0
                        || next_index_id.map(|next| next.0) != index.0.checked_add(1)
                        || last_indexes
                            .get(&table)
                            .is_some_and(|previous| *previous >= index)
                        || reservations.contains_key(&txn)
                        || drops.contains_key(&txn)
                        || rewrite_reservations.contains_key(&txn)
                    {
                        return Err(corrupt("invalid composition IndexId reservation"));
                    }
                    if current != Some(txn) {
                        if txn.0 <= last.0 {
                            return Err(corrupt("nonmonotonic composition transaction"));
                        }
                        if let Some(previous) = current {
                            if compositions
                                .get(&previous)
                                .is_some_and(|entry| entry.resolution.is_none())
                            {
                                return Err(corrupt("overlapping schema transactions"));
                            }
                        }
                        last.0 = txn.0;
                        current = Some(txn);
                    }
                    let composition =
                        compositions
                            .entry(txn)
                            .or_insert_with(|| CompositionRecord {
                                transaction: txn,
                                table_reservations: Vec::new(),
                                reservations: Vec::new(),
                                intent: None,
                                index_reservations: Vec::new(),
                                index_intent: None,
                                table_intent: None,
                                resolution: None,
                            });
                    if composition.intent.is_some()
                        || composition.index_intent.is_some()
                        || composition.resolution.is_some()
                        || composition
                            .index_reservations
                            .last()
                            .is_some_and(|previous| {
                                (previous.table, previous.index) >= (table, index)
                            })
                    {
                        return Err(corrupt(
                            "duplicate, noncanonical or out-of-order IndexId reservation",
                        ));
                    }
                    composition
                        .index_reservations
                        .push(CompositionIndexReservation {
                            transaction: txn,
                            table,
                            table_version,
                            fingerprint,
                            index,
                            next_index_id,
                        });
                    last_indexes.insert(table, index);
                }
                17 => {
                    if current != Some(txn) {
                        if txn.0 <= last.0 {
                            return Err(corrupt("nonmonotonic composition intent transaction"));
                        }
                        if let Some(previous) = current {
                            let unresolved = reservations
                                .get(&previous)
                                .is_some_and(|entry| entry.resolved.is_none())
                                || drops
                                    .get(&previous)
                                    .is_some_and(|entry| entry.resolved.is_none())
                                || rewrite_reservations.contains_key(&previous)
                                    && !rewrite_losers.contains(&previous)
                                    && rewrites
                                        .get(&previous)
                                        .is_none_or(|entry| entry.resolved.is_none())
                                || compositions
                                    .get(&previous)
                                    .is_some_and(|entry| entry.resolution.is_none());
                            if unresolved {
                                return Err(corrupt("overlapping schema transactions"));
                            }
                        }
                        last.0 = txn.0;
                        current = Some(txn);
                    }
                    let base_generation = SchemaGeneration(record.u64()?);
                    let target_generation = SchemaGeneration(record.u64()?);
                    let base_epoch = record.u64()?;
                    let target_epoch = record.u64()?;
                    let action_count = record.u32()?;
                    let action_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("composition action digest"))?;
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("composition snapshot digest"))?;
                    let table_count = record.count(64, 8)?;
                    if action_count == 0
                        || target_generation.0
                            != base_generation
                                .0
                                .checked_add(1)
                                .ok_or(corrupt("composition generation exhausted"))?
                        || target_epoch
                            != base_epoch
                                .checked_add(1)
                                .ok_or(corrupt("composition epoch exhausted"))?
                        || table_count == 0
                        || reservations.contains_key(&txn)
                        || drops.contains_key(&txn)
                        || rewrite_reservations.contains_key(&txn)
                    {
                        return Err(corrupt("invalid composition generation or counts"));
                    }
                    let mut tables = Vec::with_capacity(table_count);
                    for _ in 0..table_count {
                        let base_len = usize::try_from(record.u32()?)
                            .map_err(|_| corrupt("composition base length overflow"))?;
                        let base = SchemaCatalogSnapshot::decode(record.take(base_len)?)?;
                        let target_len = usize::try_from(record.u32()?)
                            .map_err(|_| corrupt("composition target length overflow"))?;
                        let target = SchemaCatalogSnapshot::decode(record.take(target_len)?)?;
                        validate_composition_table_plan(
                            &base,
                            &target,
                            incarnation,
                            &coordinator,
                            base_generation,
                            target_generation,
                            base_epoch,
                            target_epoch,
                            false,
                        )?;
                        let plan = CompositionTablePlan {
                            base,
                            target,
                            retired: false,
                            gc: None,
                        };
                        if tables
                            .last()
                            .is_some_and(|previous: &CompositionTablePlan| {
                                previous.table() >= plan.table()
                                    || previous.new_storage() >= plan.new_storage()
                            })
                        {
                            return Err(corrupt("noncanonical composition table plans"));
                        }
                        tables.push(plan);
                    }
                    if tables[0].new_storage().0 <= last.2
                        || base_generation.0 < last.3
                        || base_epoch < last.4
                    {
                        return Err(corrupt("nonmonotonic composition allocation"));
                    }
                    last.2 = tables
                        .last()
                        .ok_or(corrupt("empty composition table plans"))?
                        .new_storage()
                        .0;
                    last.3 = base_generation.0;
                    last.4 = base_epoch;
                    let composition =
                        compositions
                            .entry(txn)
                            .or_insert_with(|| CompositionRecord {
                                transaction: txn,
                                table_reservations: Vec::new(),
                                reservations: Vec::new(),
                                intent: None,
                                index_reservations: Vec::new(),
                                index_intent: None,
                                table_intent: None,
                                resolution: None,
                            });
                    if composition.intent.is_some()
                        || composition.index_intent.is_some()
                        || composition.resolution.is_some()
                        || !composition.index_reservations.is_empty()
                    {
                        return Err(corrupt("duplicate or out-of-order composition intent"));
                    }
                    composition.intent = Some(SchemaChangeSetIntent {
                        transaction: txn,
                        base_generation,
                        target_generation,
                        base_epoch,
                        target_epoch,
                        action_count,
                        action_digest,
                        snapshot_digest,
                        tables,
                    });
                }
                25 => {
                    if current != Some(txn) {
                        if txn.0 <= last.0 {
                            return Err(corrupt("nonmonotonic schema/index intent transaction"));
                        }
                        if let Some(previous) = current {
                            if compositions
                                .get(&previous)
                                .is_some_and(|entry| entry.resolution.is_none())
                            {
                                return Err(corrupt("overlapping schema transactions"));
                            }
                        }
                        last.0 = txn.0;
                        current = Some(txn);
                    }
                    let base_generation = SchemaGeneration(record.u64()?);
                    let raw_target_generation = record.u64()?;
                    let target_generation = (raw_target_generation != 0)
                        .then_some(SchemaGeneration(raw_target_generation));
                    let base_epoch = record.u64()?;
                    let raw_target_epoch = record.u64()?;
                    let target_epoch = (raw_target_epoch != 0).then_some(raw_target_epoch);
                    let action_count = record.u32()?;
                    let action_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("schema/index action digest"))?;
                    let snapshot_digest = match record.u8()? {
                        0 => None,
                        1 => Some(
                            record
                                .take(32)?
                                .try_into()
                                .map_err(|_| corrupt("schema/index snapshot digest"))?,
                        ),
                        _ => return Err(corrupt("invalid schema/index snapshot option")),
                    };
                    let table_count = record.count(64, 2)?;
                    if action_count == 0
                        || table_count == 0
                        || base_generation.0 == 0
                        || base_epoch == 0
                        || target_generation.is_some() != target_epoch.is_some()
                        || target_generation.is_some() != snapshot_digest.is_some()
                        || target_generation.is_some_and(|target| {
                            target.0 != base_generation.0.checked_add(1).unwrap_or(0)
                        })
                        || target_epoch
                            .is_some_and(|target| target != base_epoch.checked_add(1).unwrap_or(0))
                    {
                        return Err(corrupt("invalid schema/index intent header"));
                    }
                    let mut tables = Vec::with_capacity(table_count);
                    for _ in 0..table_count {
                        let plan = decode_schema_index_table_plan(&mut record)?;
                        if tables
                            .last()
                            .is_some_and(|previous: &SchemaIndexTablePlan| {
                                previous.table() >= plan.table()
                            })
                        {
                            return Err(corrupt("noncanonical schema/index table plans"));
                        }
                        tables.push(plan);
                    }
                    validate_schema_index_intent(
                        &tables,
                        SchemaIndexValidationContext {
                            target_generation,
                            target_epoch,
                            snapshot_digest,
                            action_count,
                            incarnation,
                            coordinator: &coordinator,
                            base_generation,
                            base_epoch,
                        },
                    )?;
                    let composition =
                        compositions
                            .entry(txn)
                            .or_insert_with(|| CompositionRecord {
                                transaction: txn,
                                table_reservations: Vec::new(),
                                reservations: Vec::new(),
                                intent: None,
                                index_reservations: Vec::new(),
                                index_intent: None,
                                table_intent: None,
                                resolution: None,
                            });
                    if composition.intent.is_some()
                        || composition.index_intent.is_some()
                        || composition.resolution.is_some()
                    {
                        return Err(corrupt("duplicate or out-of-order schema/index intent"));
                    }
                    composition.index_intent = Some(SchemaIndexChangeSetIntent {
                        transaction: txn,
                        base_generation,
                        target_generation,
                        base_epoch,
                        target_epoch,
                        action_count,
                        action_digest,
                        snapshot_digest,
                        tables,
                    });
                }
                27 => {
                    if current != Some(txn) {
                        if txn.0 <= last.0 {
                            return Err(corrupt("nonmonotonic table-object transaction"));
                        }
                        if let Some(previous) = current {
                            if compositions
                                .get(&previous)
                                .is_some_and(|entry| entry.resolution.is_none())
                            {
                                return Err(corrupt("overlapping schema transactions"));
                            }
                        }
                        last.0 = txn.0;
                        current = Some(txn);
                        compositions.insert(
                            txn,
                            CompositionRecord {
                                transaction: txn,
                                table_reservations: Vec::new(),
                                reservations: Vec::new(),
                                intent: None,
                                index_reservations: Vec::new(),
                                index_intent: None,
                                table_intent: None,
                                resolution: None,
                            },
                        );
                    }
                    let base_generation = SchemaGeneration(record.u64()?);
                    let target_generation = SchemaGeneration(record.u64()?);
                    let base_epoch = record.u64()?;
                    let target_epoch = record.u64()?;
                    let action_count = record.u32()?;
                    let action_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("table-object action digest"))?;
                    let snapshot_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("table-object snapshot digest"))?;
                    let count = record.count(64, 2)?;
                    let mut tables = Vec::with_capacity(count);
                    for _ in 0..count {
                        let plan = decode_table_object_plan(&mut record)?;
                        if tables
                            .last()
                            .is_some_and(|previous: &SchemaIndexTablePlan| {
                                previous.table() >= plan.table()
                            })
                        {
                            return Err(corrupt("noncanonical table-object plans"));
                        }
                        tables.push(plan);
                    }
                    let intent = TableObjectChangeSetIntent {
                        transaction: txn,
                        base_generation,
                        target_generation,
                        base_epoch,
                        target_epoch,
                        action_count,
                        action_digest,
                        snapshot_digest,
                        tables,
                    };
                    validate_table_object_intent(&intent, incarnation, &coordinator)?;
                    let composition = compositions
                        .get_mut(&txn)
                        .ok_or(corrupt("table-object intent without composition"))?;
                    if composition.intent.is_some()
                        || composition.index_intent.is_some()
                        || composition.table_intent.is_some()
                        || composition.resolution.is_some()
                    {
                        return Err(corrupt("duplicate table-object intent"));
                    }
                    composition.table_intent = Some(intent);
                }
                28 => {
                    if current != Some(txn) {
                        return Err(corrupt("table-object retirement without transaction"));
                    }
                    let kind = record.u8()?;
                    let table = TableId(record.u64()?);
                    let plan = compositions
                        .get_mut(&txn)
                        .and_then(|composition| composition.table_intent.as_mut())
                        .and_then(|intent| {
                            intent.tables.iter_mut().find(|plan| plan.table() == table)
                        })
                        .ok_or(corrupt("table-object retirement without plan"))?;
                    match (kind, plan) {
                        (1, SchemaIndexTablePlan::RewriteHeap { replacement, .. }) => {
                            if replacement.retired {
                                return Err(corrupt("duplicate rewrite retirement"));
                            }
                            replacement.retired = true;
                        }
                        (2, SchemaIndexTablePlan::DropHeap { retired, .. }) => {
                            if *retired {
                                return Err(corrupt("duplicate drop retirement"));
                            }
                            *retired = true;
                        }
                        _ => return Err(corrupt("table-object retirement kind mismatch")),
                    }
                }
                18 => {
                    if current != Some(txn) {
                        return Err(corrupt("composition retirement for unknown transaction"));
                    }
                    let table = TableId(record.u64()?);
                    let composition = compositions
                        .get_mut(&txn)
                        .ok_or(corrupt("composition retirement without plan"))?;
                    let plan = if let Some(intent) = composition.intent.as_mut() {
                        intent.tables.iter_mut().find(|plan| plan.table() == table)
                    } else {
                        composition.index_intent.as_mut().and_then(|intent| {
                            intent.tables.iter_mut().find_map(|plan| match plan {
                                SchemaIndexTablePlan::RewriteHeap { replacement, .. }
                                    if replacement.table() == table =>
                                {
                                    Some(replacement.as_mut())
                                }
                                _ => None,
                            })
                        })
                    }
                    .ok_or(corrupt("composition retirement without plan"))?;
                    if plan.retired {
                        return Err(corrupt("duplicate composition retirement"));
                    }
                    plan.retired = true;
                }
                19..=21 => {
                    if current != Some(txn) {
                        return Err(corrupt("composition resolution for unknown transaction"));
                    }
                    let resolution = match tag {
                        19 => CompositionResolution::Loser,
                        20 => CompositionResolution::NoEffectiveChange,
                        21 => CompositionResolution::Winner,
                        _ => return Err(corrupt("invalid composition resolution tag")),
                    };
                    let composition = compositions
                        .get_mut(&txn)
                        .ok_or(corrupt("composition resolution without history"))?;
                    if composition.resolution.is_some()
                        || resolution == CompositionResolution::Winner
                            && composition.intent.is_none()
                            && composition.index_intent.is_none()
                            && composition.table_intent.is_none()
                        || resolution == CompositionResolution::Winner
                            && composition.intent.as_ref().is_some_and(|intent| {
                                intent.tables.iter().any(|plan| !plan.retired)
                            })
                        || resolution == CompositionResolution::Winner
                            && composition.index_intent.as_ref().is_some_and(|intent| {
                                intent.tables.iter().any(|plan| {
                                    plan.replacement()
                                        .is_some_and(|replacement| !replacement.retired)
                                })
                            })
                        || resolution == CompositionResolution::Winner
                            && composition.table_intent.as_ref().is_some_and(|intent| {
                                intent.tables.iter().any(|plan| match plan {
                                    SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                                        !replacement.retired
                                    }
                                    SchemaIndexTablePlan::DropHeap { retired, .. } => !*retired,
                                    _ => false,
                                })
                            })
                        || resolution == CompositionResolution::NoEffectiveChange
                            && (composition.intent.is_some()
                                || composition.index_intent.is_some()
                                || composition.table_intent.is_some())
                    {
                        return Err(corrupt("duplicate or out-of-order composition resolution"));
                    }
                    composition.resolution = Some(resolution);
                }
                22 => {
                    if current != Some(txn) {
                        return Err(corrupt("composition GC for unknown transaction"));
                    }
                    let table = TableId(record.u64()?);
                    let coordinator_horizon = DatabaseTxnId(record.u64()?);
                    let manifest_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("composition GC manifest digest"))?;
                    let composition = compositions
                        .get_mut(&txn)
                        .filter(|entry| entry.resolution == Some(CompositionResolution::Winner))
                        .ok_or(corrupt("composition GC without winner"))?;
                    let plan = composition_replacement_mut(composition, table)
                        .ok_or(corrupt("composition GC without table plan"))?;
                    if !plan.retired || plan.gc.is_some() || coordinator_horizon.0 < txn.0 {
                        return Err(corrupt("duplicate or out-of-order composition GC"));
                    }
                    plan.gc = Some(RetiredHeapGcRecord {
                        coordinator_horizon,
                        manifest_digest,
                        complete: false,
                    });
                }
                23 => {
                    if current != Some(txn) {
                        return Err(corrupt("composition GC complete for unknown transaction"));
                    }
                    let table = TableId(record.u64()?);
                    let gc = compositions
                        .get_mut(&txn)
                        .and_then(|record| composition_replacement_mut(record, table))
                        .and_then(|plan| plan.gc.as_mut())
                        .ok_or(corrupt("composition GC complete without intent"))?;
                    if gc.complete {
                        return Err(corrupt("duplicate composition GC complete"));
                    }
                    gc.complete = true;
                }
                29 => {
                    if current != Some(txn) {
                        return Err(corrupt("table-object GC for unknown transaction"));
                    }
                    let table = TableId(record.u64()?);
                    let coordinator_horizon = DatabaseTxnId(record.u64()?);
                    let manifest_digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("table-object GC manifest digest"))?;
                    let composition = compositions
                        .get_mut(&txn)
                        .filter(|entry| entry.resolution == Some(CompositionResolution::Winner))
                        .ok_or(corrupt("table-object GC without winner"))?;
                    let plan = table_object_retirement_mut(composition, table)
                        .ok_or(corrupt("table-object GC without retirement"))?;
                    let retired = match &*plan {
                        SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                            replacement.retired
                        }
                        SchemaIndexTablePlan::DropHeap { retired, .. } => *retired,
                        _ => false,
                    };
                    let gc =
                        table_object_gc_mut(plan).ok_or(corrupt("table-object GC state absent"))?;
                    if !retired || gc.is_some() || coordinator_horizon.0 < txn.0 {
                        return Err(corrupt("duplicate or out-of-order table-object GC"));
                    }
                    *gc = Some(RetiredHeapGcRecord {
                        coordinator_horizon,
                        manifest_digest,
                        complete: false,
                    });
                }
                30 => {
                    if current != Some(txn) {
                        return Err(corrupt("table-object GC complete for unknown transaction"));
                    }
                    let table = TableId(record.u64()?);
                    let gc = compositions
                        .get_mut(&txn)
                        .and_then(|record| table_object_retirement_mut(record, table))
                        .and_then(table_object_gc_mut)
                        .and_then(Option::as_mut)
                        .ok_or(corrupt("table-object GC complete without intent"))?;
                    if gc.complete {
                        return Err(corrupt("duplicate table-object GC complete"));
                    }
                    gc.complete = true;
                }
                31 => {
                    let table = TableId(record.u64()?);
                    let storage = StorageId(record.u64()?);
                    let base_generation = SchemaGeneration(record.u64()?);
                    let base_epoch = record.u64()?;
                    let stage_locator = record.string()?;
                    let final_locator = record.string()?;
                    let digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("backfill stage digest"))?;
                    let length = usize::try_from(record.u32()?)
                        .map_err(|_| corrupt("backfill stage snapshot length"))?;
                    let provisional = SchemaCatalogSnapshot::decode(record.take(length)?)?;
                    if stage_intents
                        .insert(
                            txn,
                            StageResourceIntent {
                                transaction: txn,
                                table,
                                storage,
                                base_generation,
                                base_epoch,
                                provisional,
                                stage_locator,
                                final_locator,
                                digest,
                            },
                        )
                        .is_some()
                    {
                        return Err(corrupt("duplicate backfill stage intent"));
                    }
                }
                32 => {
                    let table = TableId(record.u64()?);
                    let storage = StorageId(record.u64()?);
                    let stage_locator = record.string()?;
                    let final_locator = record.string()?;
                    let digest = record
                        .take(32)?
                        .try_into()
                        .map_err(|_| corrupt("backfill finalization digest"))?;
                    let length = usize::try_from(record.u32()?)
                        .map_err(|_| corrupt("backfill final snapshot length"))?;
                    let final_snapshot = SchemaCatalogSnapshot::decode(record.take(length)?)?;
                    if finalization_intents
                        .insert(
                            txn,
                            FinalizationIntent {
                                transaction: txn,
                                table,
                                storage,
                                final_snapshot,
                                stage_locator,
                                final_locator,
                                digest,
                            },
                        )
                        .is_some()
                    {
                        return Err(corrupt("duplicate backfill finalization intent"));
                    }
                }
                _ => return Err(corrupt("unknown journal record tag")),
            }
            if !record.0.is_empty() {
                return Err(corrupt("trailing journal record bytes"));
            }
        }
        if !reader.0.is_empty() {
            return Err(corrupt("trailing journal bytes"));
        }
        for composition in compositions.values() {
            if usize::from(composition.intent.is_some())
                + usize::from(composition.index_intent.is_some())
                + usize::from(composition.table_intent.is_some())
                > 1
            {
                return Err(corrupt("composition has multiple aggregate intents"));
            }
            if let Some(intent) = &composition.index_intent {
                for reservation in &composition.index_reservations {
                    let plan = intent
                        .tables
                        .iter()
                        .find(|plan| plan.table() == reservation.table)
                        .ok_or(corrupt("IndexId reservation table absent from intent"))?;
                    let final_indexes = match plan {
                        SchemaIndexTablePlan::CreateHeap { .. }
                        | SchemaIndexTablePlan::DropHeap { .. } => {
                            return Err(corrupt("table-object plan in schema/index intent"));
                        }
                        SchemaIndexTablePlan::RewriteHeap { final_indexes, .. }
                        | SchemaIndexTablePlan::InPlaceIndexDelta { final_indexes, .. } => {
                            final_indexes
                        }
                    };
                    if reservation.index >= final_indexes.next_index_id {
                        return Err(corrupt("IndexId reservation exceeds target high-water"));
                    }
                    if let SchemaIndexTablePlan::InPlaceIndexDelta {
                        table_version,
                        fingerprint,
                        ..
                    } = plan
                    {
                        if reservation.table_version != *table_version
                            || reservation.fingerprint != *fingerprint
                        {
                            return Err(corrupt("IndexId reservation table lineage mismatch"));
                        }
                    }
                }
                for plan in &intent.tables {
                    let (base_indexes, final_indexes) = match plan {
                        SchemaIndexTablePlan::CreateHeap { .. }
                        | SchemaIndexTablePlan::DropHeap { .. } => {
                            return Err(corrupt("table-object plan in schema/index intent"));
                        }
                        SchemaIndexTablePlan::RewriteHeap {
                            base_indexes,
                            final_indexes,
                            ..
                        }
                        | SchemaIndexTablePlan::InPlaceIndexDelta {
                            base_indexes,
                            final_indexes,
                            ..
                        } => (base_indexes, final_indexes),
                    };
                    for created in final_indexes.active.iter().filter(|created| {
                        !base_indexes.active.iter().any(|base| base.id == created.id)
                    }) {
                        if !composition.index_reservations.iter().any(|reservation| {
                            reservation.table == plan.table() && reservation.index == created.id
                        }) {
                            return Err(corrupt("schema/index create lacks IndexId reservation"));
                        }
                    }
                }
            }
        }
        for intent in stage_intents.values() {
            let composition = compositions
                .get(&intent.transaction)
                .ok_or(corrupt("backfill stage intent has no composition"))?;
            let matches_table = composition.intent.as_ref().is_some_and(|aggregate| {
                aggregate.tables.iter().any(|plan| {
                    plan.table() == intent.table && plan.new_storage() == intent.storage
                })
            }) || composition.table_intent.as_ref().is_some_and(|aggregate| {
                aggregate.tables.iter().any(|plan| {
                    plan.table() == intent.table
                        && plan.participant_storage() == Some(intent.storage)
                })
            });
            if !matches_table {
                return Err(corrupt("backfill stage target is absent from composition"));
            }
        }
        for intent in finalization_intents.values() {
            if !stage_intents.contains_key(&intent.transaction) {
                return Err(corrupt("backfill finalization has no stage authority"));
            }
        }
        let mut retired = BTreeMap::new();
        for intent in drops.values().filter(|d| d.retired) {
            if let Some(previous) = retired.insert(intent.storage(), intent.table()) {
                if previous != intent.table() {
                    return Err(corrupt("storage retired by multiple tables"));
                }
                return Err(corrupt("duplicate storage retirement"));
            }
        }
        for rewrite in rewrites.values().filter(|rewrite| rewrite.retired) {
            if retired
                .insert(rewrite.old_storage(), rewrite.table())
                .is_some()
            {
                return Err(corrupt("duplicate storage retirement"));
            }
            if rewrite.old_storage() == rewrite.new_storage() {
                return Err(corrupt("rewrite reuses old StorageId"));
            }
        }
        for plan in compositions
            .values()
            .filter_map(|record| record.intent.as_ref())
            .flat_map(|intent| intent.tables.iter())
            .filter(|plan| plan.retired)
        {
            if retired.insert(plan.old_storage(), plan.table()).is_some()
                || plan.old_storage() == plan.new_storage()
            {
                return Err(corrupt("duplicate composition storage retirement"));
            }
        }
        for plan in compositions
            .values()
            .filter_map(|record| record.index_intent.as_ref())
            .flat_map(|intent| intent.tables.iter())
            .filter_map(SchemaIndexTablePlan::replacement)
            .filter(|plan| plan.retired)
        {
            if retired.insert(plan.old_storage(), plan.table()).is_some()
                || plan.old_storage() == plan.new_storage()
            {
                return Err(corrupt("duplicate schema/index storage retirement"));
            }
        }
        for plan in compositions
            .values()
            .filter_map(|record| record.table_intent.as_ref())
            .flat_map(|intent| intent.tables.iter())
        {
            let retired_storage = match plan {
                SchemaIndexTablePlan::RewriteHeap { replacement, .. } if replacement.retired => {
                    if replacement.old_storage() == replacement.new_storage() {
                        return Err(corrupt("table-object rewrite reuses old StorageId"));
                    }
                    Some(replacement.old_storage())
                }
                SchemaIndexTablePlan::DropHeap {
                    base,
                    retired: true,
                    ..
                } => Some(base.storages[0].id),
                _ => None,
            };
            if let Some(storage) = retired_storage {
                if retired.insert(storage, plan.table()).is_some() {
                    return Err(corrupt("duplicate table-object storage retirement"));
                }
            }
        }
        Ok(Self {
            path: PathBuf::new(),
            incarnation,
            coordinator,
            reservations,
            drops,
            rewrite_reservations,
            rewrites,
            rewrite_losers,
            compositions,
            stage_intents,
            finalization_intents,
            poisoned: false,
            #[cfg(test)]
            fail_next_sync: false,
        })
    }
}

fn encode_heap_indexes(
    writer: &mut Writer,
    indexes: &HeapRewriteIndexes,
) -> Result<(), SchemaMutationError> {
    validate_heap_indexes(indexes)?;
    writer.u64(indexes.next_index_id.0);
    writer
        .u32(u32::try_from(indexes.active.len()).map_err(|_| corrupt("too many active indexes"))?);
    for index in &indexes.active {
        writer.u64(index.id.0);
        match &index.name {
            Some(name) => {
                writer.u8(1);
                writer.string(name.as_str())?;
            }
            None => writer.u8(0),
        }
        writer.u32(index.column_id.0);
    }
    Ok(())
}

fn decode_heap_indexes(reader: &mut Reader<'_>) -> Result<HeapRewriteIndexes, SchemaMutationError> {
    let next_index_id = IndexId(reader.u64()?);
    let count = reader.count(65536, 13)?;
    let mut active = Vec::with_capacity(count);
    for _ in 0..count {
        let id = IndexId(reader.u64()?);
        let name = match reader.u8()? {
            0 => None,
            1 => Some(
                IndexName::new(reader.string()?)
                    .map_err(|_| corrupt("invalid persisted index name"))?,
            ),
            _ => return Err(corrupt("invalid persisted index name option")),
        };
        active.push(HeapRewriteIndex {
            id,
            name,
            column_id: ColumnId(reader.u32()?),
        });
    }
    let indexes = HeapRewriteIndexes {
        active,
        next_index_id,
    };
    validate_heap_indexes(&indexes)?;
    Ok(indexes)
}

fn validate_heap_indexes(indexes: &HeapRewriteIndexes) -> Result<(), SchemaMutationError> {
    if indexes.next_index_id.0 == 0 {
        return Err(corrupt("zero index high-water"));
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut columns = BTreeSet::new();
    let mut previous = None;
    for index in &indexes.active {
        if index.id.0 == 0
            || index.id >= indexes.next_index_id
            || previous.is_some_and(|previous| previous >= index.id)
            || !ids.insert(index.id)
            || !columns.insert(index.column_id)
            || index
                .name
                .as_ref()
                .is_some_and(|name| !names.insert(name.clone()))
        {
            return Err(corrupt("invalid active index inventory"));
        }
        previous = Some(index.id);
    }
    Ok(())
}

fn encode_schema_index_table_plan(
    writer: &mut Writer,
    plan: &SchemaIndexTablePlan,
) -> Result<(), SchemaMutationError> {
    match plan {
        SchemaIndexTablePlan::CreateHeap { .. } | SchemaIndexTablePlan::DropHeap { .. } => {
            return Err(corrupt("table-object plan cannot use tag 25"));
        }
        SchemaIndexTablePlan::RewriteHeap {
            replacement,
            base_indexes,
            final_indexes,
        } => {
            writer.u8(1);
            let base = replacement.base.encode()?;
            let target = replacement.target.encode()?;
            writer
                .u32(u32::try_from(base.len()).map_err(|_| corrupt("composition base too large"))?);
            writer.0.extend_from_slice(&base);
            writer.u32(
                u32::try_from(target.len()).map_err(|_| corrupt("composition target too large"))?,
            );
            writer.0.extend_from_slice(&target);
            encode_heap_indexes(writer, base_indexes)?;
            encode_heap_indexes(writer, final_indexes)?;
        }
        SchemaIndexTablePlan::InPlaceIndexDelta {
            table,
            table_version,
            fingerprint,
            storage,
            base_indexes,
            final_indexes,
        } => {
            writer.u8(2);
            writer.u64(table.0);
            writer.u64(table_version.0);
            writer.0.extend_from_slice(fingerprint.as_bytes());
            writer.u64(storage.0);
            encode_heap_indexes(writer, base_indexes)?;
            encode_heap_indexes(writer, final_indexes)?;
        }
    }
    Ok(())
}

fn decode_schema_index_table_plan(
    reader: &mut Reader<'_>,
) -> Result<SchemaIndexTablePlan, SchemaMutationError> {
    match reader.u8()? {
        1 => {
            let base_len = usize::try_from(reader.u32()?)
                .map_err(|_| corrupt("composition base length overflow"))?;
            let base = SchemaCatalogSnapshot::decode(reader.take(base_len)?)?;
            let target_len = usize::try_from(reader.u32()?)
                .map_err(|_| corrupt("composition target length overflow"))?;
            let target = SchemaCatalogSnapshot::decode(reader.take(target_len)?)?;
            let base_indexes = decode_heap_indexes(reader)?;
            let final_indexes = decode_heap_indexes(reader)?;
            Ok(SchemaIndexTablePlan::RewriteHeap {
                replacement: Box::new(CompositionTablePlan {
                    base,
                    target,
                    retired: false,
                    gc: None,
                }),
                base_indexes,
                final_indexes,
            })
        }
        2 => Ok(SchemaIndexTablePlan::InPlaceIndexDelta {
            table: TableId(reader.u64()?),
            table_version: TableSchemaVersion(reader.u64()?),
            fingerprint: SchemaFingerprint::from_bytes(
                reader
                    .take(32)?
                    .try_into()
                    .map_err(|_| corrupt("index delta schema fingerprint"))?,
            ),
            storage: StorageId(reader.u64()?),
            base_indexes: decode_heap_indexes(reader)?,
            final_indexes: decode_heap_indexes(reader)?,
        }),
        _ => Err(corrupt("unknown schema/index table plan")),
    }
}

fn encode_table_object_plan(
    writer: &mut Writer,
    plan: &SchemaIndexTablePlan,
) -> Result<(), SchemaMutationError> {
    match plan {
        SchemaIndexTablePlan::CreateHeap {
            target,
            final_indexes,
        } => {
            writer.u8(1);
            let target = target.encode()?;
            writer
                .u32(u32::try_from(target.len()).map_err(|_| corrupt("create target too large"))?);
            writer.0.extend_from_slice(&target);
            encode_heap_indexes(writer, final_indexes)?;
        }
        SchemaIndexTablePlan::DropHeap { base, .. } => {
            writer.u8(2);
            let base = base.encode()?;
            writer.u32(u32::try_from(base.len()).map_err(|_| corrupt("drop base too large"))?);
            writer.0.extend_from_slice(&base);
        }
        SchemaIndexTablePlan::RewriteHeap {
            replacement,
            base_indexes,
            final_indexes,
        } => {
            writer.u8(3);
            let base = replacement.base.encode()?;
            let target = replacement.target.encode()?;
            writer.u32(u32::try_from(base.len()).map_err(|_| corrupt("rewrite base too large"))?);
            writer.0.extend_from_slice(&base);
            writer
                .u32(u32::try_from(target.len()).map_err(|_| corrupt("rewrite target too large"))?);
            writer.0.extend_from_slice(&target);
            encode_heap_indexes(writer, base_indexes)?;
            encode_heap_indexes(writer, final_indexes)?;
        }
        SchemaIndexTablePlan::InPlaceIndexDelta {
            table,
            table_version,
            fingerprint,
            storage,
            base_indexes,
            final_indexes,
        } => {
            writer.u8(4);
            writer.u64(table.0);
            writer.u64(table_version.0);
            writer.0.extend_from_slice(fingerprint.as_bytes());
            writer.u64(storage.0);
            encode_heap_indexes(writer, base_indexes)?;
            encode_heap_indexes(writer, final_indexes)?;
        }
    }
    Ok(())
}

fn decode_table_object_plan(
    reader: &mut Reader<'_>,
) -> Result<SchemaIndexTablePlan, SchemaMutationError> {
    match reader.u8()? {
        1 => {
            let len = usize::try_from(reader.u32()?)
                .map_err(|_| corrupt("create target length overflow"))?;
            Ok(SchemaIndexTablePlan::CreateHeap {
                target: Box::new(SchemaCatalogSnapshot::decode(reader.take(len)?)?),
                final_indexes: decode_heap_indexes(reader)?,
            })
        }
        2 => {
            let len =
                usize::try_from(reader.u32()?).map_err(|_| corrupt("drop base length overflow"))?;
            Ok(SchemaIndexTablePlan::DropHeap {
                base: Box::new(SchemaCatalogSnapshot::decode(reader.take(len)?)?),
                retired: false,
                gc: None,
            })
        }
        3 => {
            let base_len = usize::try_from(reader.u32()?)
                .map_err(|_| corrupt("rewrite base length overflow"))?;
            let base = SchemaCatalogSnapshot::decode(reader.take(base_len)?)?;
            let target_len = usize::try_from(reader.u32()?)
                .map_err(|_| corrupt("rewrite target length overflow"))?;
            let target = SchemaCatalogSnapshot::decode(reader.take(target_len)?)?;
            Ok(SchemaIndexTablePlan::RewriteHeap {
                replacement: Box::new(CompositionTablePlan {
                    base,
                    target,
                    retired: false,
                    gc: None,
                }),
                base_indexes: decode_heap_indexes(reader)?,
                final_indexes: decode_heap_indexes(reader)?,
            })
        }
        4 => Ok(SchemaIndexTablePlan::InPlaceIndexDelta {
            table: TableId(reader.u64()?),
            table_version: TableSchemaVersion(reader.u64()?),
            fingerprint: SchemaFingerprint::from_bytes(
                reader
                    .take(32)?
                    .try_into()
                    .map_err(|_| corrupt("index delta fingerprint"))?,
            ),
            storage: StorageId(reader.u64()?),
            base_indexes: decode_heap_indexes(reader)?,
            final_indexes: decode_heap_indexes(reader)?,
        }),
        _ => Err(corrupt("unknown table-object plan")),
    }
}

fn validate_table_object_intent(
    intent: &TableObjectChangeSetIntent,
    incarnation: [u8; 16],
    coordinator: &str,
) -> Result<(), SchemaMutationError> {
    if intent.tables.is_empty()
        || intent.tables.len() > 64
        || intent.action_count == 0
        || intent.base_generation.0 == 0
        || intent.target_generation.0 != intent.base_generation.0.checked_add(1).unwrap_or(0)
        || intent.base_epoch == 0
        || intent.target_epoch != intent.base_epoch.checked_add(1).unwrap_or(0)
    {
        return Err(corrupt("invalid table-object intent header"));
    }
    let mut tables = BTreeSet::new();
    let mut participants = BTreeSet::new();
    let mut new_storages = BTreeSet::new();
    let mut old_storages = BTreeSet::new();
    let mut names = BTreeSet::new();
    for plan in &intent.tables {
        if !tables.insert(plan.table()) {
            return Err(corrupt("duplicate table-object plan"));
        }
        if let Some(storage) = plan.participant_storage() {
            if !participants.insert(storage) {
                return Err(corrupt("duplicate table-object participant"));
            }
        }
        match plan {
            SchemaIndexTablePlan::CreateHeap {
                target,
                final_indexes,
            } => {
                validate_one_table_fragment(target, incarnation, coordinator)?;
                if target.committed.tables[0].version != TableSchemaVersion(1)
                    || target.epoch != intent.target_epoch
                    || target.committed.generation != intent.target_generation
                    || target
                        .committed
                        .next_table_id
                        .is_some_and(|next| next <= plan.table())
                    || !new_storages.insert(target.storages[0].id)
                {
                    return Err(corrupt("invalid CreateHeap identity"));
                }
                validate_heap_indexes(final_indexes)?;
            }
            SchemaIndexTablePlan::DropHeap { base, retired, gc } => {
                validate_one_table_fragment(base, incarnation, coordinator)?;
                if base.epoch != intent.base_epoch
                    || base.committed.generation != intent.base_generation
                    || !matches!(
                        base.storages[0].kind,
                        crate::schema_catalog::CatalogStorageKind::Heap
                    )
                    || !old_storages.insert(base.storages[0].id)
                    || *retired
                    || gc.is_some()
                {
                    return Err(corrupt("new DropHeap intent is already retired"));
                }
            }
            SchemaIndexTablePlan::RewriteHeap {
                replacement,
                base_indexes,
                final_indexes,
            } => {
                validate_composition_table_plan(
                    &replacement.base,
                    &replacement.target,
                    incarnation,
                    coordinator,
                    intent.base_generation,
                    intent.target_generation,
                    intent.base_epoch,
                    intent.target_epoch,
                    true,
                )?;
                if !new_storages.insert(replacement.new_storage()) {
                    return Err(corrupt("duplicate replacement StorageId"));
                }
                if !old_storages.insert(replacement.old_storage()) {
                    return Err(corrupt("duplicate table-object predecessor StorageId"));
                }
                validate_heap_indexes(base_indexes)?;
                validate_heap_indexes(final_indexes)?;
            }
            SchemaIndexTablePlan::InPlaceIndexDelta {
                table,
                table_version,
                storage,
                base_indexes,
                final_indexes,
                ..
            } => {
                if table.0 == 0
                    || table_version.0 == 0
                    || storage.0 == 0
                    || base_indexes == final_indexes
                {
                    return Err(corrupt("invalid table-object index delta"));
                }
                validate_heap_indexes(base_indexes)?;
                validate_heap_indexes(final_indexes)?;
            }
        }
        let final_indexes = match plan {
            SchemaIndexTablePlan::CreateHeap { final_indexes, .. }
            | SchemaIndexTablePlan::RewriteHeap { final_indexes, .. }
            | SchemaIndexTablePlan::InPlaceIndexDelta { final_indexes, .. } => Some(final_indexes),
            SchemaIndexTablePlan::DropHeap { .. } => None,
        };
        if final_indexes.is_some_and(|indexes| {
            indexes.active.iter().any(|index| {
                index
                    .name
                    .as_ref()
                    .is_some_and(|name| !names.insert(name.clone()))
            })
        }) {
            return Err(corrupt("duplicate final index name"));
        }
    }
    if new_storages
        .iter()
        .any(|storage| old_storages.contains(storage))
    {
        return Err(corrupt("table-object plan reuses predecessor StorageId"));
    }
    Ok(())
}

fn validate_one_table_fragment(
    snapshot: &SchemaCatalogSnapshot,
    incarnation: [u8; 16],
    coordinator: &str,
) -> Result<(), SchemaMutationError> {
    snapshot.validate()?;
    if snapshot.incarnation != incarnation
        || snapshot.coordinator.as_deref() != Some(coordinator)
        || snapshot.committed.schema.tables().len() != 1
        || snapshot.committed.tables.len() != 1
        || snapshot.placements.tables.len() != 1
        || snapshot.storages.len() != 1
        || snapshot.partition_evidence.is_some()
    {
        return Err(corrupt("invalid one-table fragment"));
    }
    Ok(())
}

struct SchemaIndexValidationContext<'a> {
    target_generation: Option<SchemaGeneration>,
    target_epoch: Option<u64>,
    snapshot_digest: Option<[u8; 32]>,
    action_count: u32,
    incarnation: [u8; 16],
    coordinator: &'a str,
    base_generation: SchemaGeneration,
    base_epoch: u64,
}

fn validate_schema_index_intent(
    tables: &[SchemaIndexTablePlan],
    context: SchemaIndexValidationContext<'_>,
) -> Result<(), SchemaMutationError> {
    let SchemaIndexValidationContext {
        target_generation,
        target_epoch,
        snapshot_digest,
        action_count,
        incarnation,
        coordinator,
        base_generation,
        base_epoch,
    } = context;
    let has_rewrite = tables
        .iter()
        .any(|plan| matches!(plan, SchemaIndexTablePlan::RewriteHeap { .. }));
    if tables.is_empty()
        || tables.len() > 64
        || action_count == 0
        || has_rewrite != target_generation.is_some()
        || target_generation.is_some() != target_epoch.is_some()
        || target_generation.is_some() != snapshot_digest.is_some()
        || target_generation
            .is_some_and(|target| target.0 != base_generation.0.checked_add(1).unwrap_or(0))
        || target_epoch.is_some_and(|target| target != base_epoch.checked_add(1).unwrap_or(0))
    {
        return Err(corrupt("schema/index target presence mismatch"));
    }
    let mut participants = BTreeSet::new();
    let mut final_names = BTreeSet::new();
    for plan in tables {
        let (participant, base_indexes, final_indexes) = match plan {
            SchemaIndexTablePlan::CreateHeap { .. } | SchemaIndexTablePlan::DropHeap { .. } => {
                return Err(corrupt("table-object plan in schema/index intent"));
            }
            SchemaIndexTablePlan::RewriteHeap {
                replacement,
                base_indexes,
                final_indexes,
            } => {
                validate_composition_table_plan(
                    &replacement.base,
                    &replacement.target,
                    incarnation,
                    coordinator,
                    base_generation,
                    target_generation.ok_or(corrupt("rewrite target generation absent"))?,
                    base_epoch,
                    target_epoch.ok_or(corrupt("rewrite target epoch absent"))?,
                    false,
                )?;
                let table = &replacement.target.committed.schema.tables()[0];
                if final_indexes
                    .active
                    .iter()
                    .any(|index| table.column_by_id(index.column_id).is_none())
                    || final_indexes.next_index_id < base_indexes.next_index_id
                {
                    return Err(corrupt("rewrite index targets missing ColumnId"));
                }
                (replacement.new_storage(), base_indexes, final_indexes)
            }
            SchemaIndexTablePlan::InPlaceIndexDelta {
                table,
                table_version,
                storage,
                base_indexes,
                final_indexes,
                ..
            } => {
                if table.0 == 0
                    || table_version.0 == 0
                    || storage.0 == 0
                    || base_indexes == final_indexes
                    || final_indexes.next_index_id < base_indexes.next_index_id
                {
                    return Err(corrupt("invalid in-place index delta"));
                }
                (*storage, base_indexes, final_indexes)
            }
        };
        if !participants.insert(participant)
            || final_indexes.active.iter().any(|index| {
                index
                    .name
                    .as_ref()
                    .is_some_and(|name| !final_names.insert(name.clone()))
            })
            || final_indexes.active.iter().any(|final_index| {
                base_indexes
                    .active
                    .iter()
                    .find(|base_index| base_index.id == final_index.id)
                    .is_some_and(|base_index| base_index != final_index)
            })
        {
            return Err(corrupt(
                "invalid schema/index participant or identity reuse",
            ));
        }
    }
    Ok(())
}

fn allocator_predecessor(floor: Option<u64>) -> Result<u64, SchemaMutationError> {
    match floor {
        Some(floor) => floor
            .checked_sub(1)
            .ok_or(corrupt("zero allocator floor in drop intent")),
        None => Ok(u64::MAX),
    }
}

fn encode_operation(
    writer: &mut Writer,
    operation: &AlterTableOperation,
) -> Result<(), SchemaMutationError> {
    match operation {
        AlterTableOperation::RenameTable { new_name } => {
            writer.u8(1);
            writer.string(new_name)?;
        }
        AlterTableOperation::RenameColumn {
            column_id,
            new_name,
        } => {
            writer.u8(2);
            writer.u32(column_id.0);
            writer.string(new_name)?;
        }
        AlterTableOperation::AddNullableColumn { name, data_type } => {
            writer.u8(3);
            writer.string(name)?;
            encode_semantic_type(writer, data_type)?;
        }
        AlterTableOperation::DropColumn { column_id } => {
            writer.u8(4);
            writer.u32(column_id.0);
        }
        AlterTableOperation::SetNotNull { column_id } => {
            writer.u8(5);
            writer.u32(column_id.0);
        }
        AlterTableOperation::DropNotNull { column_id } => {
            writer.u8(6);
            writer.u32(column_id.0);
        }
        AlterTableOperation::ChangeNominalType {
            column_id,
            target_type,
        } => {
            writer.u8(7);
            writer.u32(column_id.0);
            encode_semantic_type(writer, target_type)?;
        }
    }
    Ok(())
}

fn decode_operation(reader: &mut Reader<'_>) -> Result<AlterTableOperation, SchemaMutationError> {
    let column = |reader: &mut Reader<'_>| -> Result<ColumnId, SchemaMutationError> {
        let id = ColumnId(reader.u32()?);
        if id.0 == 0 {
            return Err(corrupt("zero rewrite ColumnId"));
        }
        Ok(id)
    };
    Ok(match reader.u8()? {
        1 => AlterTableOperation::RenameTable {
            new_name: reader.string()?,
        },
        2 => AlterTableOperation::RenameColumn {
            column_id: column(reader)?,
            new_name: reader.string()?,
        },
        3 => AlterTableOperation::AddNullableColumn {
            name: reader.string()?,
            data_type: decode_semantic_type(reader)?,
        },
        4 => AlterTableOperation::DropColumn {
            column_id: column(reader)?,
        },
        5 => AlterTableOperation::SetNotNull {
            column_id: column(reader)?,
        },
        6 => AlterTableOperation::DropNotNull {
            column_id: column(reader)?,
        },
        7 => AlterTableOperation::ChangeNominalType {
            column_id: column(reader)?,
            target_type: decode_semantic_type(reader)?,
        },
        _ => return Err(corrupt("unknown rewrite operation tag")),
    })
}

fn encode_semantic_type(
    writer: &mut Writer,
    data_type: &SemanticType,
) -> Result<(), SchemaMutationError> {
    writer.u8(match data_type.physical {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
    });
    writer.u8(u8::from(data_type.name.is_some()));
    if let Some(name) = &data_type.name {
        writer.string(name)?;
    }
    Ok(())
}

fn decode_semantic_type(reader: &mut Reader<'_>) -> Result<SemanticType, SchemaMutationError> {
    let physical = match reader.u8()? {
        1 => PhysicalType::Bool,
        2 => PhysicalType::Int64,
        3 => PhysicalType::UInt64,
        4 => PhysicalType::Text,
        _ => return Err(corrupt("unknown rewrite physical type")),
    };
    match reader.u8()? {
        0 => Ok(SemanticType::physical(physical)),
        1 => Ok(SemanticType::named(reader.string()?, physical)),
        _ => Err(corrupt("invalid rewrite semantic type flag")),
    }
}

fn validate_drop_fragment(
    fragment: &SchemaCatalogSnapshot,
    target_generation: SchemaGeneration,
    target_epoch: u64,
    incarnation: [u8; 16],
    coordinator: &str,
) -> Result<(), SchemaMutationError> {
    if fragment.incarnation != incarnation
        || fragment.committed.schema.tables().len() != 1
        || fragment.committed.tables.len() != 1
        || fragment.placements.tables.len() != 1
        || fragment.storages.len() != 1
        || fragment.coordinator.as_deref() != Some(coordinator)
        || fragment.partition_evidence.is_some()
        || target_generation.0
            != fragment
                .committed
                .generation
                .0
                .checked_add(1)
                .ok_or(corrupt("drop generation exhausted"))?
        || target_epoch
            != fragment
                .epoch
                .checked_add(1)
                .ok_or(corrupt("drop epoch exhausted"))?
    {
        return Err(corrupt("drop intent generation or inventory mismatch"));
    }
    let table = &fragment.committed.schema.tables()[0];
    let lineage = &fragment.committed.tables[0];
    let placement = &fragment.placements.tables[0];
    let storage = &fragment.storages[0];
    if table.id != lineage.table_id
        || table.id != placement.table_id
        || table.id != storage.table_id
        || lineage.version.0 == 0
        || table
            .fingerprint()
            .map_err(|_| corrupt("invalid retired table fingerprint"))?
            != placement.schema_fingerprint
        || !matches!(
            storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            placement.placement,
            crate::registry::TablePlacement::Single {
                table_id,
                storage_id
            } if table_id == table.id && storage_id == storage.id
        )
    {
        return Err(corrupt("drop intent exact identity mismatch"));
    }
    Ok(())
}

fn validate_rewrite_fragments(
    reservation: &RewriteReservation,
    operation: &AlterTableOperation,
    base: &SchemaCatalogSnapshot,
    target: &SchemaCatalogSnapshot,
    incarnation: [u8; 16],
    coordinator: &str,
) -> Result<(), SchemaMutationError> {
    if base.incarnation != incarnation
        || target.incarnation != incarnation
        || base.epoch != reservation.base_epoch
        || base.committed.generation != reservation.base_generation
        || target.epoch
            != base
                .epoch
                .checked_add(1)
                .ok_or(corrupt("rewrite epoch exhausted"))?
        || target.committed.generation.0
            != base
                .committed
                .generation
                .0
                .checked_add(1)
                .ok_or(corrupt("rewrite generation exhausted"))?
        || base.coordinator.as_deref() != Some(coordinator)
        || target.coordinator.as_deref() != Some(coordinator)
        || base.partition_evidence.is_some()
        || target.partition_evidence.is_some()
        || base.committed.schema.tables().len() != 1
        || target.committed.schema.tables().len() != 1
        || base.committed.tables.len() != 1
        || target.committed.tables.len() != 1
        || base.placements.tables.len() != 1
        || target.placements.tables.len() != 1
        || base.storages.len() != 1
        || target.storages.len() != 1
    {
        return Err(corrupt("rewrite fragment generation or inventory mismatch"));
    }
    let base_table = &base.committed.schema.tables()[0];
    let target_table = &target.committed.schema.tables()[0];
    let base_lineage = &base.committed.tables[0];
    let target_lineage = &target.committed.tables[0];
    let base_storage = &base.storages[0];
    let target_storage = &target.storages[0];
    if base_table.id != reservation.table
        || target_table.id != reservation.table
        || base_lineage.table_id != reservation.table
        || target_lineage.table_id != reservation.table
        || target_lineage.version.0
            != base_lineage
                .version
                .0
                .checked_add(1)
                .ok_or(corrupt("rewrite table version exhausted"))?
        || base_storage.table_id != reservation.table
        || target_storage.table_id != reservation.table
        || base_storage.id == reservation.storage
        || target_storage.id != reservation.storage
        || !matches!(
            base_storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            target_storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            base.placements.tables[0].placement,
            crate::registry::TablePlacement::Single { table_id, storage_id }
                if table_id == reservation.table && storage_id == base_storage.id
        )
        || !matches!(
            target.placements.tables[0].placement,
            crate::registry::TablePlacement::Single { table_id, storage_id }
                if table_id == reservation.table && storage_id == reservation.storage
        )
        || base_table
            .fingerprint()
            .map_err(|_| corrupt("invalid rewrite base schema"))?
            != base.placements.tables[0].schema_fingerprint
        || target_table
            .fingerprint()
            .map_err(|_| corrupt("invalid rewrite target schema"))?
            != target.placements.tables[0].schema_fingerprint
        || base.placements.tables[0].schema_fingerprint
            == target.placements.tables[0].schema_fingerprint
        || base.committed.next_table_id != target.committed.next_table_id
        || base.committed.next_partition_id != target.committed.next_partition_id
        || !floor_at_least(
            target.committed.next_storage_id.map(|id| id.0),
            reservation.storage.0.checked_add(1),
        )
        || !floor_at_least(
            target_lineage.next_column_id.map(|id| u64::from(id.0)),
            base_lineage.next_column_id.map(|id| u64::from(id.0)),
        )
    {
        return Err(corrupt("rewrite exact identity mismatch"));
    }

    let mut expected = base_table.clone();
    match operation {
        AlterTableOperation::RenameTable { new_name } => {
            if reservation.column.is_some() {
                return Err(corrupt("rename unexpectedly reserves ColumnId"));
            }
            expected.name = new_name.clone();
        }
        AlterTableOperation::RenameColumn {
            column_id,
            new_name,
        } => {
            if reservation.column.is_some() {
                return Err(corrupt("rename unexpectedly reserves ColumnId"));
            }
            expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?
                .name = new_name.clone();
        }
        AlterTableOperation::AddNullableColumn { name, data_type } => {
            let id = reservation
                .column
                .ok_or(corrupt("ADD rewrite has no ColumnId reservation"))?;
            let type_spec = match &data_type.name {
                Some(type_name) => TypeSpec::Semantic {
                    physical: data_type.physical,
                    name: type_name.clone(),
                },
                None => TypeSpec::Physical(data_type.physical),
            };
            expected
                .columns
                .push(netbadb_schema::ColumnDef::new(id, name.clone(), type_spec).nullable(true));
            if target_lineage.next_column_id.map(|next| next.0) != id.0.checked_add(1) {
                return Err(corrupt("ADD rewrite ColumnId floor mismatch"));
            }
        }
        AlterTableOperation::DropColumn { column_id } => {
            if reservation.column.is_some() {
                return Err(corrupt("DROP unexpectedly reserves ColumnId"));
            }
            expected.columns.retain(|column| column.id != *column_id);
        }
        AlterTableOperation::SetNotNull { column_id } => {
            if reservation.column.is_some() {
                return Err(corrupt(
                    "nullability rewrite unexpectedly reserves ColumnId",
                ));
            }
            expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?
                .nullable = false;
        }
        AlterTableOperation::DropNotNull { column_id } => {
            if reservation.column.is_some() {
                return Err(corrupt(
                    "nullability rewrite unexpectedly reserves ColumnId",
                ));
            }
            expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?
                .nullable = true;
        }
        AlterTableOperation::ChangeNominalType {
            column_id,
            target_type,
        } => {
            if reservation.column.is_some() {
                return Err(corrupt("type rewrite unexpectedly reserves ColumnId"));
            }
            let column = expected
                .columns
                .iter_mut()
                .find(|column| column.id == *column_id)
                .ok_or(corrupt("rewrite column absent"))?;
            column.type_spec = match &target_type.name {
                Some(type_name) => TypeSpec::Semantic {
                    physical: target_type.physical,
                    name: type_name.clone(),
                },
                None => TypeSpec::Physical(target_type.physical),
            };
        }
    }
    if expected != *target_table {
        return Err(corrupt("rewrite operation differs from target schema"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_composition_table_plan(
    base: &SchemaCatalogSnapshot,
    target: &SchemaCatalogSnapshot,
    incarnation: [u8; 16],
    coordinator: &str,
    base_generation: SchemaGeneration,
    target_generation: SchemaGeneration,
    base_epoch: u64,
    target_epoch: u64,
    allow_table_high_water_advance: bool,
) -> Result<(), SchemaMutationError> {
    if base.incarnation != incarnation
        || target.incarnation != incarnation
        || base.epoch != base_epoch
        || target.epoch != target_epoch
        || base.committed.generation != base_generation
        || target.committed.generation != target_generation
        || base.coordinator.as_deref() != Some(coordinator)
        || target.coordinator.as_deref() != Some(coordinator)
        || base.partition_evidence.is_some()
        || target.partition_evidence.is_some()
        || base.committed.schema.tables().len() != 1
        || target.committed.schema.tables().len() != 1
        || base.committed.tables.len() != 1
        || target.committed.tables.len() != 1
        || base.placements.tables.len() != 1
        || target.placements.tables.len() != 1
        || base.storages.len() != 1
        || target.storages.len() != 1
    {
        return Err(corrupt("composition table fragment inventory mismatch"));
    }
    let base_table = &base.committed.schema.tables()[0];
    let target_table = &target.committed.schema.tables()[0];
    let base_lineage = &base.committed.tables[0];
    let target_lineage = &target.committed.tables[0];
    let base_placement = &base.placements.tables[0];
    let target_placement = &target.placements.tables[0];
    let base_storage = &base.storages[0];
    let target_storage = &target.storages[0];
    if base_table.id != target_table.id
        || base_table.id != base_lineage.table_id
        || base_table.id != target_lineage.table_id
        || base_table.id != base_placement.table_id
        || base_table.id != target_placement.table_id
        || base_table.id != base_storage.table_id
        || base_table.id != target_storage.table_id
        || base_storage.id == target_storage.id
        || target_lineage.version.0
            != base_lineage
                .version
                .0
                .checked_add(1)
                .ok_or(corrupt("composition table version exhausted"))?
        || base_table
            .fingerprint()
            .map_err(|_| corrupt("invalid composition base schema"))?
            != base_placement.schema_fingerprint
        || target_table
            .fingerprint()
            .map_err(|_| corrupt("invalid composition target schema"))?
            != target_placement.schema_fingerprint
        || base_placement.schema_fingerprint == target_placement.schema_fingerprint
        || !matches!(
            base_storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            target_storage.kind,
            crate::schema_catalog::CatalogStorageKind::Heap
        )
        || !matches!(
            base_placement.placement,
            crate::registry::TablePlacement::Single { table_id, storage_id }
                if table_id == base_table.id && storage_id == base_storage.id
        )
        || !matches!(
            target_placement.placement,
            crate::registry::TablePlacement::Single { table_id, storage_id }
                if table_id == target_table.id && storage_id == target_storage.id
        )
        || if allow_table_high_water_advance {
            !floor_at_least(
                target.committed.next_table_id.map(|id| id.0),
                base.committed.next_table_id.map(|id| id.0),
            )
        } else {
            base.committed.next_table_id != target.committed.next_table_id
        }
        || base.committed.next_partition_id != target.committed.next_partition_id
        || !floor_at_least(
            target.committed.next_storage_id.map(|id| id.0),
            target_storage.id.0.checked_add(1),
        )
        || !floor_at_least(
            target_lineage.next_column_id.map(|id| u64::from(id.0)),
            base_lineage.next_column_id.map(|id| u64::from(id.0)),
        )
    {
        return Err(corrupt("composition table exact identity mismatch"));
    }
    for column in &target_table.columns {
        if let Some(base_column) = base_table.column_by_id(column.id) {
            if base_column.semantic_type().physical != column.semantic_type().physical {
                return Err(corrupt("composition changes a physical column type"));
            }
        }
    }
    Ok(())
}
fn floor_at_least(current: Option<u64>, required: Option<u64>) -> bool {
    match (current, required) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(current), Some(required)) => current >= required,
    }
}
fn put_record(w: &mut Writer, payload: &[u8]) -> Result<(), SchemaMutationError> {
    let record = envelope(b"NBSR", payload)?;
    if w.0
        .len()
        .checked_add(record.len())
        .and_then(|n| n.checked_add(4))
        .is_none_or(|n| n > crate::schema_catalog::MAX_BYTES - 16)
    {
        return Err(corrupt("mutation journal capacity exhausted"));
    }
    w.u32(u32::try_from(record.len()).map_err(|_| corrupt("record too large"))?);
    w.0.extend_from_slice(&record);
    Ok(())
}
