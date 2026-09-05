//! ALTER-only transaction composition: logical overlay first, one base-to-final
//! Heap replacement per effectively changed table at the global seal.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::rc::Rc;

#[cfg(test)]
use std::cell::Cell;

use netbadb_index::IndexDefinition;
use netbadb_schema::{ColumnDef, DropTableTarget, Schema, TableDef, TypeSpec};
use netbadb_storage::{HeapRewriteIndex, HeapRewriteIndexes, TableStorage};
use netbadb_types::{
    ColumnId, DatabaseTxnId, IndexId, ScalarValue, StorageId, TableId, TableSchemaVersion,
};
use sha2::{Digest, Sha256};

use crate::coordinator_log::{CoordinatorLog, SchemaParticipantReference};
use crate::partition_catalog::{CatalogTable, PartitionCatalog};
use crate::registry::TablePlacement;
use crate::schema_catalog::{
    CatalogStorage, CatalogStorageKind, SchemaCatalogError, SchemaCatalogSnapshot, TableLineage,
};
use crate::schema_catalog_file as file;
use crate::schema_mutation::{
    AlterTableOperation, AlterTableSpec, CreateTableSpec, SchemaDependency, SchemaMutationError,
    SchemaWriter, SharedMutationJournal, build_alter_target, cleanup_prepared,
    cleanup_staged_loser, crash, digest, ensure_parent, open_winner_heap, promote, retarget_owner,
    validate_resource_path, write_owner,
};
use crate::schema_mutation_journal::{
    CompositionColumnReservation, CompositionIndexReservation, CompositionResolution,
    CompositionTablePlan, CompositionTableReservation, CreateIntent, FinalizationIntent,
    MigrationIndexFinalizationIntent, Reservation, SchemaChangeSetIntent,
    SchemaIndexChangeSetIntent, SchemaIndexTablePlan, SchemaMutationJournal, SourceBackfillIntent,
    StageResourceIntent, TableObjectChangeSetIntent, final_locator, namespace, prepared_locator,
    stage_locator,
};
use crate::{Database, DatabaseError, Transaction, TransactionState};

pub(crate) const MAX_SCHEMA_ACTIONS: usize = 128;
pub(crate) const MAX_TOUCHED_TABLES: usize = 64;
pub(crate) const MAX_COLUMN_RESERVATIONS: usize = 128;
pub(crate) const MAX_INDEX_RESERVATIONS: usize = 128;

#[cfg(test)]
std::thread_local! {
    static SOURCE_NOT_NULL_VALIDATION_COUNT: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_source_not_null_validation_count() {
    SOURCE_NOT_NULL_VALIDATION_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn source_not_null_validation_count() -> u64 {
    SOURCE_NOT_NULL_VALIDATION_COUNT.get()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowProjectionEntry {
    Source {
        column_id: ColumnId,
        source_position: usize,
        target_nullable: bool,
    },
    SynthesizedNull {
        column_id: ColumnId,
        target_nullable: bool,
    },
}

/// Checked, target-ordered row mapping shared by ordinary schema rewrites and
/// transaction-visible late clones. Column names and raw ordinals never confer
/// identity; an ordinal is cached only after an exact ColumnId match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowProjection {
    pub(crate) source: SchemaDependency,
    pub(crate) target: SchemaDependency,
    pub(crate) source_width: usize,
    pub(crate) target_entries: Vec<RowProjectionEntry>,
}

impl RowProjection {
    pub(crate) fn build(
        source: &TableDef,
        source_version: TableSchemaVersion,
        target: &TableDef,
        target_version: TableSchemaVersion,
        reserved_new_columns: &BTreeSet<ColumnId>,
    ) -> Result<Self, DatabaseError> {
        source.validate()?;
        target.validate()?;
        if source.id != target.id {
            return Err(SchemaMutationError::InvalidSchemaEvolution(
                "row projection changes TableId",
            )
            .into());
        }
        let source_by_id = source
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| (column.id, (position, column)))
            .collect::<HashMap<_, _>>();
        let mut target_entries = Vec::with_capacity(target.columns.len());
        for target_column in &target.columns {
            if let Some((source_position, source_column)) = source_by_id.get(&target_column.id) {
                if source_column.semantic_type().physical != target_column.semantic_type().physical
                    || source_column.semantic_type() != target_column.semantic_type()
                {
                    return Err(SchemaMutationError::UnsupportedSchemaEvolution.into());
                }
                target_entries.push(RowProjectionEntry::Source {
                    column_id: target_column.id,
                    source_position: *source_position,
                    target_nullable: target_column.nullable,
                });
            } else if reserved_new_columns.contains(&target_column.id) {
                target_entries.push(RowProjectionEntry::SynthesizedNull {
                    column_id: target_column.id,
                    target_nullable: target_column.nullable,
                });
            } else {
                return Err(SchemaMutationError::InvalidSchemaEvolution(
                    "row projection target column lacks durable reservation",
                )
                .into());
            }
        }
        Ok(Self {
            source: SchemaDependency {
                table_id: source.id,
                table_version: source_version,
                fingerprint: source.fingerprint()?,
            },
            target: SchemaDependency {
                table_id: target.id,
                table_version: target_version,
                fingerprint: target.fingerprint()?,
            },
            source_width: source.columns.len(),
            target_entries,
        })
    }

    pub(crate) fn project(
        &self,
        source_values: &[ScalarValue],
    ) -> Result<Vec<ScalarValue>, DatabaseError> {
        if source_values.len() != self.source_width {
            return Err(
                SchemaMutationError::Corrupt("row projection source width mismatch").into(),
            );
        }
        let mut values = Vec::with_capacity(self.target_entries.len());
        for entry in &self.target_entries {
            let (column_id, target_nullable, value) = match entry {
                RowProjectionEntry::Source {
                    column_id,
                    source_position,
                    target_nullable,
                } => (
                    *column_id,
                    *target_nullable,
                    source_values
                        .get(*source_position)
                        .ok_or(SchemaMutationError::Corrupt(
                            "row projection source ordinal out of bounds",
                        ))?
                        .clone(),
                ),
                RowProjectionEntry::SynthesizedNull {
                    column_id,
                    target_nullable,
                } => (*column_id, *target_nullable, ScalarValue::Null),
            };
            if !target_nullable && matches!(value, ScalarValue::Null) {
                return Err(SchemaMutationError::NotNullViolation(column_id).into());
            }
            values.push(value);
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, Copy)]
enum SchemaIndexMaterialization {
    Ordinary,
    SourceBackfill {
        source_storage: StorageId,
        source_physical_txn_id: netbadb_types::TxnId,
    },
    AdoptedSourceBackfill {
        source_storage: StorageId,
        source_physical_txn_id: netbadb_types::TxnId,
    },
}

impl SchemaIndexMaterialization {
    fn source(self) -> Option<(StorageId, netbadb_types::TxnId)> {
        match self {
            Self::Ordinary => None,
            Self::SourceBackfill {
                source_storage,
                source_physical_txn_id,
            } => Some((source_storage, source_physical_txn_id)),
            Self::AdoptedSourceBackfill {
                source_storage,
                source_physical_txn_id,
            } => Some((source_storage, source_physical_txn_id)),
        }
    }

    fn replaces_existing_index_intent(self) -> bool {
        matches!(self, Self::SourceBackfill { .. })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AdoptedSourceTransaction {
    pub(crate) logical: SchemaTransactionPlan,
    pub(crate) source_storage: StorageId,
    pub(crate) source_physical_txn_id: netbadb_types::TxnId,
    pub(crate) source_table_version: TableSchemaVersion,
    pub(crate) source_fingerprint: netbadb_schema::SchemaFingerprint,
    pub(crate) source_locator: String,
    pub(crate) base_generation: netbadb_types::SchemaGeneration,
    pub(crate) base_epoch: u64,
    pub(crate) source_index_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceBackfillRefinementScope {
    PublicCompatible,
    CoreLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlterValidationContext {
    Ordinary,
    AdoptedSource { source_not_null_validated: bool },
}

#[derive(Debug)]
pub(crate) enum SchemaCompositionState {
    None,
    Composing(Box<SchemaTransactionPlan>),
    AdoptedSourceRefining(Box<AdoptedSourceTransaction>),
    SealingAndMaterializing(Box<MaterializedSchemaTransaction>),
    Materialized(Box<MaterializedSchemaTransaction>),
    BackfillMaterializing(Box<MaterializedSchemaTransaction>),
    BackfillOpen(Box<MaterializedSchemaTransaction>),
    IndexEvacuating(Box<MaterializedSchemaTransaction>),
    RefiningAfterEvacuation(Box<MaterializedSchemaTransaction>),
    Refining(Box<MaterializedSchemaTransaction>),
    IndexFinalizing(Box<MaterializedSchemaTransaction>),
    Finalizing(Box<MaterializedSchemaTransaction>),
    Finalized(Box<MaterializedSchemaTransaction>),
    BackfillOpenIndex(Box<MaterializedSchemaIndexTransaction>),
    RefiningIndex(Box<MaterializedSchemaIndexTransaction>),
    FinalizedIndex(Box<MaterializedSchemaIndexTransaction>),
    SealingAndMaterializingIndex(Box<MaterializedSchemaIndexTransaction>),
    MaterializedIndex(Box<MaterializedSchemaIndexTransaction>),
    SourceBackfillOpen(Box<MaterializedSchemaIndexTransaction>),
    SourceRefining(Box<MaterializedSchemaIndexTransaction>),
    SourceIndexFinalizing(Box<MaterializedSchemaIndexTransaction>),
    LateCloneMaterializing(Box<MaterializedSchemaIndexTransaction>),
    LateCloneReady(Box<MaterializedSchemaIndexTransaction>),
    SealedNoEffectiveChange(Box<SchemaTransactionPlan>),
    RollbackRequiredLogical(Box<SchemaTransactionPlan>),
    RollbackRequiredMaterialized(Box<MaterializedSchemaTransaction>),
    RollbackRequiredMaterializedIndex(Box<MaterializedSchemaIndexTransaction>),
}

impl SchemaCompositionState {
    pub(crate) fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub(crate) fn is_started(&self) -> bool {
        !self.is_none()
    }

    pub(crate) fn is_composing(&self) -> bool {
        matches!(self, Self::Composing(_))
    }

    pub(crate) fn is_sealed(&self) -> bool {
        matches!(
            self,
            Self::SealingAndMaterializing(_)
                | Self::Materialized(_)
                | Self::BackfillMaterializing(_)
                | Self::IndexEvacuating(_)
                | Self::RefiningAfterEvacuation(_)
                | Self::IndexFinalizing(_)
                | Self::Finalizing(_)
                | Self::Finalized(_)
                | Self::FinalizedIndex(_)
                | Self::SealingAndMaterializingIndex(_)
                | Self::MaterializedIndex(_)
                | Self::SourceBackfillOpen(_)
                | Self::SourceRefining(_)
                | Self::SourceIndexFinalizing(_)
                | Self::LateCloneMaterializing(_)
                | Self::LateCloneReady(_)
                | Self::SealedNoEffectiveChange(_)
                | Self::RollbackRequiredLogical(_)
                | Self::RollbackRequiredMaterialized(_)
                | Self::RollbackRequiredMaterializedIndex(_)
        )
    }

    pub(crate) fn plan(&self) -> Option<&SchemaTransactionPlan> {
        match self {
            Self::Composing(plan)
            | Self::SealedNoEffectiveChange(plan)
            | Self::RollbackRequiredLogical(plan) => Some(plan),
            Self::AdoptedSourceRefining(adopted) => Some(&adopted.logical),
            Self::SealingAndMaterializing(materialized)
            | Self::Materialized(materialized)
            | Self::BackfillMaterializing(materialized)
            | Self::BackfillOpen(materialized)
            | Self::IndexEvacuating(materialized)
            | Self::RefiningAfterEvacuation(materialized)
            | Self::Refining(materialized)
            | Self::IndexFinalizing(materialized)
            | Self::Finalizing(materialized)
            | Self::Finalized(materialized)
            | Self::RollbackRequiredMaterialized(materialized) => Some(&materialized.logical),
            Self::SealingAndMaterializingIndex(materialized)
            | Self::MaterializedIndex(materialized)
            | Self::SourceBackfillOpen(materialized)
            | Self::SourceRefining(materialized)
            | Self::SourceIndexFinalizing(materialized)
            | Self::LateCloneMaterializing(materialized)
            | Self::LateCloneReady(materialized)
            | Self::BackfillOpenIndex(materialized)
            | Self::RefiningIndex(materialized)
            | Self::FinalizedIndex(materialized)
            | Self::RollbackRequiredMaterializedIndex(materialized) => Some(&materialized.logical),
            Self::None => None,
        }
    }

    pub(crate) fn materialized_index(&self) -> Option<&MaterializedSchemaIndexTransaction> {
        match self {
            Self::SealingAndMaterializingIndex(materialized)
            | Self::MaterializedIndex(materialized)
            | Self::SourceBackfillOpen(materialized)
            | Self::SourceRefining(materialized)
            | Self::SourceIndexFinalizing(materialized)
            | Self::LateCloneMaterializing(materialized)
            | Self::LateCloneReady(materialized)
            | Self::BackfillOpenIndex(materialized)
            | Self::RefiningIndex(materialized)
            | Self::FinalizedIndex(materialized)
            | Self::RollbackRequiredMaterializedIndex(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn materialized_index_mut(
        &mut self,
    ) -> Option<&mut MaterializedSchemaIndexTransaction> {
        match self {
            Self::SealingAndMaterializingIndex(materialized)
            | Self::MaterializedIndex(materialized)
            | Self::SourceBackfillOpen(materialized)
            | Self::SourceRefining(materialized)
            | Self::SourceIndexFinalizing(materialized)
            | Self::LateCloneMaterializing(materialized)
            | Self::LateCloneReady(materialized)
            | Self::BackfillOpenIndex(materialized)
            | Self::RefiningIndex(materialized)
            | Self::FinalizedIndex(materialized)
            | Self::RollbackRequiredMaterializedIndex(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn materialized(&self) -> Option<&MaterializedSchemaTransaction> {
        match self {
            Self::SealingAndMaterializing(materialized)
            | Self::Materialized(materialized)
            | Self::BackfillMaterializing(materialized)
            | Self::BackfillOpen(materialized)
            | Self::IndexEvacuating(materialized)
            | Self::RefiningAfterEvacuation(materialized)
            | Self::Refining(materialized)
            | Self::IndexFinalizing(materialized)
            | Self::Finalizing(materialized)
            | Self::Finalized(materialized)
            | Self::RollbackRequiredMaterialized(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn materialized_mut(&mut self) -> Option<&mut MaterializedSchemaTransaction> {
        match self {
            Self::SealingAndMaterializing(materialized)
            | Self::Materialized(materialized)
            | Self::BackfillMaterializing(materialized)
            | Self::BackfillOpen(materialized)
            | Self::IndexEvacuating(materialized)
            | Self::RefiningAfterEvacuation(materialized)
            | Self::Refining(materialized)
            | Self::IndexFinalizing(materialized)
            | Self::Finalizing(materialized)
            | Self::Finalized(materialized)
            | Self::RollbackRequiredMaterialized(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn backfill(&self) -> Option<&MaterializedSchemaTransaction> {
        match self {
            Self::BackfillMaterializing(materialized)
            | Self::BackfillOpen(materialized)
            | Self::IndexEvacuating(materialized)
            | Self::RefiningAfterEvacuation(materialized)
            | Self::Refining(materialized)
            | Self::IndexFinalizing(materialized)
            | Self::Finalizing(materialized)
            | Self::Finalized(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn backfill_mut(&mut self) -> Option<&mut MaterializedSchemaTransaction> {
        match self {
            Self::BackfillMaterializing(materialized)
            | Self::BackfillOpen(materialized)
            | Self::IndexEvacuating(materialized)
            | Self::RefiningAfterEvacuation(materialized)
            | Self::Refining(materialized)
            | Self::IndexFinalizing(materialized)
            | Self::Finalizing(materialized)
            | Self::Finalized(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn backfill_index(&self) -> Option<&MaterializedSchemaIndexTransaction> {
        match self {
            Self::BackfillOpenIndex(materialized)
            | Self::RefiningIndex(materialized)
            | Self::FinalizedIndex(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn backfill_index_mut(&mut self) -> Option<&mut MaterializedSchemaIndexTransaction> {
        match self {
            Self::BackfillOpenIndex(materialized)
            | Self::RefiningIndex(materialized)
            | Self::FinalizedIndex(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn source_backfill(&self) -> Option<&MaterializedSchemaIndexTransaction> {
        match self {
            Self::SourceBackfillOpen(materialized)
            | Self::SourceRefining(materialized)
            | Self::SourceIndexFinalizing(materialized)
            | Self::LateCloneMaterializing(materialized)
            | Self::LateCloneReady(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn is_late_clone_materializing(&self) -> bool {
        matches!(self, Self::LateCloneMaterializing(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ComposedTable {
    pub(crate) base_table: TableDef,
    pub(crate) base_lineage: TableLineage,
    pub(crate) old_storage: StorageId,
    pub(crate) catalog_table: CatalogTable,
    pub(crate) descriptor: CatalogStorage,
    pub(crate) indexes: HeapRewriteIndexes,
    pub(crate) base_indexes: HeapRewriteIndexes,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionCreatedTable {
    pub(crate) indexes: HeapRewriteIndexes,
    pub(crate) present: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaTransactionPlan {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) catalog: PathBuf,
    pub(crate) base: SchemaCatalogSnapshot,
    pub(crate) overlay: crate::schema_catalog::CommittedCatalogState,
    pub(crate) touched: BTreeMap<TableId, ComposedTable>,
    pub(crate) created: BTreeMap<TableId, TransactionCreatedTable>,
    pub(crate) action_evidence: Vec<[u8; 32]>,
    pub(crate) reservation_count: usize,
    pub(crate) index_reservation_count: usize,
    pub(crate) index_actions: usize,
    pub(crate) table_actions: usize,
    pub(crate) journal: SharedMutationJournal,
    pub(crate) writer: SchemaWriter,
}

impl SchemaTransactionPlan {
    pub(crate) fn transaction(&self) -> DatabaseTxnId {
        self.transaction
    }

    pub(crate) fn dependency(&self, table: TableId) -> Result<SchemaDependency, DatabaseError> {
        let definition = self
            .overlay
            .schema
            .tables()
            .iter()
            .find(|definition| definition.id == table)
            .ok_or(SchemaMutationError::TableNotFound(table))?;
        let lineage = self
            .overlay
            .tables
            .iter()
            .find(|lineage| lineage.table_id == table)
            .ok_or(SchemaMutationError::Corrupt("composition lineage absent"))?;
        Ok(SchemaDependency {
            table_id: table,
            table_version: lineage.version,
            fingerprint: definition.fingerprint()?,
        })
    }

    pub(crate) fn action_count(&self) -> usize {
        self.action_evidence.len()
    }

    fn action_digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        for evidence in &self.action_evidence {
            hash.update(evidence);
        }
        hash.finalize().into()
    }
}

#[derive(Debug)]
pub(crate) struct MaterializedSchemaTransaction {
    pub(crate) logical: SchemaTransactionPlan,
    pub(crate) target: SchemaCatalogSnapshot,
    pub(crate) reference: SchemaParticipantReference,
    pub(crate) intent: SchemaChangeSetIntent,
    pub(crate) staged: BTreeMap<StorageId, TableStorage>,
    /// Transaction-visible physical index inventory for each private staged
    /// Heap. Unlike `base_indexes`, this is updated by immediate evacuation.
    pub(crate) staged_indexes: BTreeMap<TableId, HeapRewriteIndexes>,
    pub(crate) backfill: bool,
    /// Columns indexed at any point in this backfill. This history prevents
    /// DROP INDEX from bypassing the indexed-nullability restriction.
    pub(crate) backfill_indexed_columns: BTreeSet<ColumnId>,
}

#[derive(Debug)]
pub(crate) struct IndexPublication {
    pub(crate) storage: StorageId,
    pub(crate) drops: Vec<IndexId>,
    pub(crate) creates: Vec<IndexDefinition>,
}

#[derive(Debug)]
pub(crate) struct MaterializedSchemaIndexTransaction {
    pub(crate) logical: SchemaTransactionPlan,
    pub(crate) target: Option<SchemaCatalogSnapshot>,
    pub(crate) reference: Option<SchemaParticipantReference>,
    pub(crate) intent: TableObjectChangeSetIntent,
    pub(crate) staged: BTreeMap<StorageId, TableStorage>,
    pub(crate) publications: Vec<IndexPublication>,
    pub(crate) backfill: bool,
    #[cfg(test)]
    pub(crate) source_copy_passes: u64,
    #[cfg(test)]
    pub(crate) source_rows_copied: u64,
}

impl MaterializedSchemaIndexTransaction {
    pub(crate) fn prepare(&self) -> Result<(), SchemaMutationError> {
        let (Some(target), Some(reference)) = (&self.target, &self.reference) else {
            return Ok(());
        };
        let bytes = target.encode()?;
        if digest(&bytes) != reference.digest {
            return Err(SchemaMutationError::Corrupt(
                "prepared schema/index composition changed",
            ));
        }
        let path = file::resolve(
            &self.logical.catalog,
            &prepared_locator(
                &self.logical.catalog,
                target.incarnation,
                self.intent.transaction,
            )?,
        );
        validate_resource_path(&self.logical.catalog, &path)?;
        ensure_parent(&path)?;
        file::atomic_write(&path, &bytes, false)?;
        file::sync_parent(&path)?;
        crash("composition-prepared-catalog-durable");
        Ok(())
    }
}

type CompositionCompletion = (
    SchemaCatalogSnapshot,
    u64,
    Vec<(TableId, StorageId, TableStorage)>,
);

impl MaterializedSchemaTransaction {
    pub(crate) fn transaction(&self) -> DatabaseTxnId {
        self.intent.transaction
    }

    pub(crate) fn prepare(&self) -> Result<(), SchemaMutationError> {
        let bytes = self.target.encode()?;
        if digest(&bytes) != self.reference.digest {
            return Err(SchemaMutationError::Corrupt(
                "prepared composition schema changed",
            ));
        }
        let path = file::resolve(
            &self.logical.catalog,
            &prepared_locator(
                &self.logical.catalog,
                self.target.incarnation,
                self.transaction(),
            )?,
        );
        validate_resource_path(&self.logical.catalog, &path)?;
        ensure_parent(&path)?;
        file::atomic_write(&path, &bytes, false)?;
        file::sync_parent(&path)?;
        crash("composition-prepared-catalog-durable");
        Ok(())
    }
}

impl Database {
    fn source_backfill_candidate_is_eligible(
        &self,
        transaction: &Transaction,
        materialized: &MaterializedSchemaIndexTransaction,
        target: &SchemaDependency,
    ) -> bool {
        let table_id = target.table_id;
        let current_source = materialized
            .logical
            .touched
            .get(&table_id)
            .map(|touched| touched.old_storage);
        self.schema_writer.get() == Some(transaction.id())
            && materialized.logical.transaction == transaction.id()
            && materialized.intent.transaction == transaction.id()
            && materialized.logical.table_actions == 0
            && materialized.logical.created.is_empty()
            && materialized.logical.touched.len() == 1
            && materialized.logical.touched.contains_key(&table_id)
            && materialized
                .logical
                .dependency(table_id)
                .is_ok_and(|dependency| dependency == *target)
            && materialized.intent.tables.len() == 1
            && materialized.target.is_none()
            && materialized.reference.is_none()
            && materialized.staged.is_empty()
            && current_source.is_some_and(|source| {
                matches!(
                    self.bindings.placement(table_id),
                    Ok(TablePlacement::Single { storage_id, .. }) if *storage_id == source
                )
            })
            && matches!(
                &materialized.intent.tables[0],
                SchemaIndexTablePlan::InPlaceIndexDelta {
                    table,
                    storage,
                    base_indexes,
                    final_indexes,
                    ..
                } if *table == table_id
                    && Some(*storage) == current_source
                    && transaction.is_only_write_participant(*storage)
                    && final_indexes.active.len() < base_indexes.active.len()
                    && final_indexes.active.iter().all(|final_index| {
                        base_indexes.active.iter().any(|base| base == final_index)
                    })
            )
    }

    fn try_enter_source_backfill(
        &mut self,
        transaction: &mut Transaction,
        target: &SchemaDependency,
    ) -> Result<bool, DatabaseError> {
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let materialized = match previous {
            SchemaCompositionState::MaterializedIndex(materialized) => materialized,
            other => {
                transaction.schema_composition = other;
                return Ok(false);
            }
        };
        if !self.source_backfill_candidate_is_eligible(transaction, &materialized, target) {
            transaction.schema_composition =
                SchemaCompositionState::MaterializedIndex(materialized);
            return Ok(false);
        }
        transaction.schema_composition = SchemaCompositionState::SourceBackfillOpen(materialized);
        Ok(true)
    }

    /// Routes one exact post-execution-barrier ALTER into Candidate B when the
    /// current physical participant and drop-only index prelude prove that the
    /// existing Heap can be used as the transaction-visible source.  A false
    /// result leaves the prior dispatcher behavior unchanged.
    pub(crate) fn try_apply_source_backfill_refinement(
        &mut self,
        transaction: &mut Transaction,
        spec: &AlterTableSpec,
    ) -> Result<bool, DatabaseError> {
        self.validate_transaction(transaction)?;
        if matches!(
            transaction.schema_composition,
            SchemaCompositionState::SourceBackfillOpen(_)
                | SchemaCompositionState::SourceRefining(_)
        ) {
            self.apply_source_backfill_refinement(
                transaction,
                spec.clone(),
                SourceBackfillRefinementScope::PublicCompatible,
            )?;
            return Ok(true);
        }
        if !matches!(
            spec.operation,
            AlterTableOperation::SetNotNull { .. }
                | AlterTableOperation::DropNotNull { .. }
                | AlterTableOperation::RenameTable { .. }
                | AlterTableOperation::RenameColumn { .. }
                | AlterTableOperation::AddNullableColumn { .. }
                | AlterTableOperation::DropColumn { .. }
        ) || !matches!(
            transaction.schema_composition,
            SchemaCompositionState::MaterializedIndex(_)
        ) {
            return Ok(false);
        }
        if !self.try_enter_source_backfill(transaction, &spec.target)? {
            return Ok(false);
        }
        self.apply_source_backfill_refinement(
            transaction,
            spec.clone(),
            SourceBackfillRefinementScope::PublicCompatible,
        )?;
        Ok(true)
    }

    /// Typed Core refinement entry point. Public SQL uses the narrower
    /// execution-time dispatcher above and never calls this API directly.
    pub fn apply_source_backfill_layout_refinement(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        if !matches!(
            spec.operation,
            AlterTableOperation::SetNotNull { .. }
                | AlterTableOperation::DropNotNull { .. }
                | AlterTableOperation::RenameTable { .. }
                | AlterTableOperation::RenameColumn { .. }
                | AlterTableOperation::AddNullableColumn { .. }
                | AlterTableOperation::DropColumn { .. }
        ) {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
            )
            .into());
        }
        if matches!(
            transaction.schema_composition,
            SchemaCompositionState::MaterializedIndex(_)
        ) && !self.try_enter_source_backfill(transaction, &spec.target)?
        {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
            )
            .into());
        }
        self.apply_source_backfill_refinement(
            transaction,
            spec,
            SourceBackfillRefinementScope::CoreLayout,
        )
    }

    /// Applies one layout-compatible refinement against the transaction-visible
    /// S1 view. The first successful change closes all later relational access.
    fn apply_source_backfill_refinement(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
        scope: SourceBackfillRefinementScope,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        if !matches!(
            transaction.schema_composition,
            SchemaCompositionState::SourceBackfillOpen(_)
                | SchemaCompositionState::SourceRefining(_)
        ) {
            return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
        }
        let (
            table_id,
            source_storage,
            current,
            visible_schema,
            base_version,
            base_table,
            current_lineage,
            reservation_count,
            journal,
        ) = {
            let materialized = transaction
                .schema_composition
                .source_backfill()
                .ok_or(SchemaMutationError::Corrupt("source-backfill state absent"))?;
            let touched = materialized
                .logical
                .touched
                .get(&spec.target.table_id)
                .ok_or(SchemaMutationError::StaleSchemaDependency)?;
            let current_dependency = materialized.logical.dependency(spec.target.table_id)?;
            let base_dependency = SchemaDependency {
                table_id: touched.base_table.id,
                table_version: touched.base_lineage.version,
                fingerprint: touched.base_table.fingerprint()?,
            };
            let layout_target_is_exact = matches!(
                &spec.operation,
                AlterTableOperation::AddNullableColumn { .. }
                    | AlterTableOperation::DropColumn { .. }
            ) && spec.target == base_dependency;
            if materialized.logical.touched.len() != 1
                || (current_dependency != spec.target && !layout_target_is_exact)
            {
                return Err(SchemaMutationError::StaleSchemaDependency.into());
            }
            let current = materialized
                .logical
                .overlay
                .schema
                .tables()
                .iter()
                .find(|table| table.id == spec.target.table_id)
                .cloned()
                .ok_or(SchemaMutationError::TableNotFound(spec.target.table_id))?;
            (
                spec.target.table_id,
                touched.old_storage,
                current,
                materialized.logical.overlay.schema.clone(),
                touched.base_lineage.version,
                touched.base_table.clone(),
                materialized
                    .logical
                    .overlay
                    .tables
                    .iter()
                    .find(|lineage| lineage.table_id == spec.target.table_id)
                    .cloned()
                    .ok_or(SchemaMutationError::Corrupt(
                        "source-backfill lineage absent",
                    ))?,
                materialized.logical.reservation_count,
                Rc::clone(&materialized.logical.journal),
            )
        };
        match (&scope, &spec.operation) {
            (
                SourceBackfillRefinementScope::PublicCompatible,
                AlterTableOperation::SetNotNull { .. }
                | AlterTableOperation::DropNotNull { .. }
                | AlterTableOperation::RenameTable { .. }
                | AlterTableOperation::RenameColumn { .. }
                | AlterTableOperation::AddNullableColumn { .. }
                | AlterTableOperation::DropColumn { .. },
            )
            | (
                SourceBackfillRefinementScope::CoreLayout,
                AlterTableOperation::SetNotNull { .. }
                | AlterTableOperation::DropNotNull { .. }
                | AlterTableOperation::RenameTable { .. }
                | AlterTableOperation::RenameColumn { .. }
                | AlterTableOperation::AddNullableColumn { .. }
                | AlterTableOperation::DropColumn { .. },
            ) => {}
            _ => {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
                )
                .into());
            }
        }
        if let AlterTableOperation::DropColumn { column_id } = &spec.operation {
            if current
                .column_by_id(*column_id)
                .is_some_and(|column| column.primary_key)
            {
                return Err(SchemaMutationError::PrimaryKeyColumn(*column_id).into());
            }
            let materialized = transaction
                .schema_composition
                .source_backfill()
                .ok_or(SchemaMutationError::Corrupt("source-backfill state absent"))?;
            if materialized.logical.touched[&table_id]
                .indexes
                .active
                .iter()
                .any(|index| index.column_id == *column_id)
            {
                return Err(SchemaMutationError::IndexedColumn(*column_id).into());
            }
        }
        let indexed_nullability = match &spec.operation {
            AlterTableOperation::SetNotNull { column_id }
            | AlterTableOperation::DropNotNull { column_id } => Some(*column_id),
            AlterTableOperation::RenameTable { .. } | AlterTableOperation::RenameColumn { .. } => {
                None
            }
            AlterTableOperation::AddNullableColumn { .. }
            | AlterTableOperation::DropColumn { .. } => None,
            AlterTableOperation::ChangeNominalType { .. } => None,
        };
        if let Some(column_id) = indexed_nullability {
            if scope == SourceBackfillRefinementScope::PublicCompatible
                && base_table.column_by_id(column_id).is_none()
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
                )
                .into());
            }
            let materialized = transaction
                .schema_composition
                .source_backfill()
                .ok_or(SchemaMutationError::Corrupt("source-backfill state absent"))?;
            let touched = &materialized.logical.touched[&table_id];
            let dropped = touched
                .base_indexes
                .active
                .iter()
                .filter(|index| index.column_id == column_id)
                .all(|base| {
                    !touched
                        .indexes
                        .active
                        .iter()
                        .any(|index| index.id == base.id)
                        && materialized.publications.iter().any(|publication| {
                            publication.storage == source_storage
                                && publication.drops.contains(&base.id)
                        })
                });
            if !dropped
                || touched
                    .indexes
                    .active
                    .iter()
                    .any(|index| index.column_id == column_id)
                || !transaction.is_write_participant(source_storage)
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::IndexedNullability(column_id),
                )
                .into());
            }
            if matches!(spec.operation, AlterTableOperation::SetNotNull { .. })
                && base_table.column_by_id(column_id).is_some()
            {
                self.validate_source_view_not_null(transaction, source_storage, column_id)?;
            }
        }
        let reserved_column = if matches!(
            spec.operation,
            AlterTableOperation::AddNullableColumn { .. }
        ) {
            if reservation_count >= MAX_COLUMN_RESERVATIONS {
                return Err(
                    SchemaMutationError::CompositionLimitExceeded("ColumnId reservations").into(),
                );
            }
            Some(
                current_lineage
                    .next_column_id
                    .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?,
            )
        } else {
            None
        };
        let candidate = build_alter_target(&current, &spec.operation, reserved_column)?;
        let candidate_schema = Schema::new(
            visible_schema
                .tables()
                .iter()
                .map(|table| {
                    if table.id == table_id {
                        candidate.clone()
                    } else {
                        table.clone()
                    }
                })
                .collect(),
        )?;
        let target_version = base_version
            .0
            .checked_add(1)
            .map(TableSchemaVersion)
            .ok_or(SchemaMutationError::IdentityExhausted("TableSchemaVersion"))?;
        if let Some(column) = reserved_column {
            let next_column_id = column.0.checked_add(1).map(ColumnId);
            if next_column_id.is_none() {
                return Err(SchemaMutationError::IdentityExhausted("ColumnId").into());
            }
            let reservation = CompositionColumnReservation {
                transaction: transaction.id(),
                table: table_id,
                column,
                next_column_id,
            };
            if let Err(error) = journal
                .borrow_mut()
                .reserve_source_backfill_column(reservation, column)
            {
                return if journal.borrow().ensure_ready().is_err() {
                    Err(SchemaMutationError::RecoveryRequired.into())
                } else {
                    Err(error.into())
                };
            }
            crash("source-backfill-column-reservation-durable");
        }
        let materialized = transaction
            .schema_composition
            .materialized_index_mut()
            .ok_or(SchemaMutationError::Corrupt(
                "source-backfill state disappeared",
            ))?;
        materialized.logical.overlay.schema = candidate_schema;
        let lineage = materialized
            .logical
            .overlay
            .tables
            .iter_mut()
            .find(|lineage| lineage.table_id == table_id)
            .ok_or(SchemaMutationError::Corrupt(
                "source-backfill lineage disappeared",
            ))?;
        lineage.version = target_version;
        if let Some(column) = reserved_column {
            lineage.next_column_id = column.0.checked_add(1).map(ColumnId);
            materialized.logical.reservation_count += 1;
        }
        let mut evidence = Sha256::new();
        evidence.update(b"source-backfill-refinement");
        evidence.update(table_id.0.to_le_bytes());
        evidence.update(target_version.0.to_le_bytes());
        evidence.update(candidate.fingerprint()?.as_bytes());
        evidence.update(reserved_column.map_or(0, |column| column.0).to_le_bytes());
        materialized
            .logical
            .action_evidence
            .push(evidence.finalize().into());
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        transaction.schema_composition = match previous {
            SchemaCompositionState::SourceBackfillOpen(materialized)
            | SchemaCompositionState::SourceRefining(materialized) => {
                SchemaCompositionState::SourceRefining(materialized)
            }
            other => other,
        };
        Ok(())
    }

    pub(crate) fn finalize_source_backfill(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let mut materialized = match previous {
            SchemaCompositionState::SourceBackfillOpen(materialized) => {
                transaction.schema_composition =
                    SchemaCompositionState::MaterializedIndex(materialized);
                return Ok(());
            }
            SchemaCompositionState::SourceRefining(materialized)
            | SchemaCompositionState::SourceIndexFinalizing(materialized) => materialized,
            other => {
                transaction.schema_composition = other;
                return Ok(());
            }
        };
        let (table_id, source_storage, base_table) =
            {
                let (table_id, touched) = materialized.logical.touched.iter().next().ok_or(
                    SchemaMutationError::Corrupt("source-backfill target absent"),
                )?;
                (*table_id, touched.old_storage, touched.base_table.clone())
            };
        let final_table = materialized
            .logical
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == table_id)
            .cloned()
            .ok_or(SchemaMutationError::TableNotFound(table_id))?;
        if final_table == base_table {
            materialized
                .logical
                .overlay
                .tables
                .iter_mut()
                .find(|lineage| lineage.table_id == table_id)
                .ok_or(SchemaMutationError::Corrupt(
                    "source-backfill no-op lineage absent",
                ))?
                .version = materialized.logical.touched[&table_id].base_lineage.version;
            transaction.schema_composition =
                SchemaCompositionState::MaterializedIndex(materialized);
            return Ok(());
        }
        let source_physical_txn_id = transaction.physical_transaction_id(source_storage).ok_or(
            SchemaMutationError::Corrupt("source-backfill physical transaction absent"),
        )?;
        let logical = materialized.logical;
        self.materialize_schema_index_composition(
            transaction,
            logical,
            SchemaIndexMaterialization::SourceBackfill {
                source_storage,
                source_physical_txn_id,
            },
        )
    }

    pub(crate) fn should_compose_index_ddl(
        &self,
        transaction: &Transaction,
        table: TableId,
    ) -> Result<bool, DatabaseError> {
        if transaction.schema_composition.is_started() {
            return Ok(true);
        }
        let Some(catalog) = &self.catalog_path else {
            return Ok(false);
        };
        if !transaction.is_pristine_for_schema_composition() {
            return Ok(false);
        }
        let snapshot = file::load(catalog)?;
        let Some(placement) = snapshot
            .placements
            .tables
            .iter()
            .find(|entry| entry.table_id == table)
        else {
            return Ok(false);
        };
        let TablePlacement::Single { storage_id, .. } = placement.placement else {
            return Ok(false);
        };
        let Some(descriptor) = snapshot
            .storages
            .iter()
            .find(|descriptor| descriptor.id == storage_id)
        else {
            return Ok(false);
        };
        Ok(matches!(descriptor.kind, CatalogStorageKind::Heap)
            && descriptor.locator == final_locator(catalog, snapshot.incarnation, storage_id)?)
    }

    /// Resolves an exact ALTER target against the current transaction overlay.
    /// Callers of the typed API use this again after an earlier composed action
    /// instead of rebinding against committed schema.
    pub fn resolve_alter_table_in(
        &self,
        transaction: &Transaction,
        name: &str,
    ) -> Result<SchemaDependency, DatabaseError> {
        self.validate_transaction(transaction)?;
        let table = transaction
            .visible_schema(&self.committed.schema)
            .table(name)
            .ok_or_else(|| SchemaMutationError::UndefinedTable(name.to_owned()))?;
        transaction.schema_composition.plan().map_or_else(
            || self.resolve_alter_table(name),
            |plan| plan.dependency(table.id),
        )
    }

    /// Composes one typed Heap ALTER into the transaction-local final schema.
    /// Physical allocation and rewriting are delayed until the first user
    /// execution against that overlay or commit.
    pub fn rewrite_heap_table_schema_in(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        self.compose_heap_table_schema_in(transaction, spec)
    }

    /// Applies one ALTER to the transaction-local canonical overlay. No
    /// StorageId or Heap is created here.
    pub(crate) fn compose_heap_table_schema_in(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        if self.try_apply_source_backfill_refinement(transaction, &spec)? {
            return Ok(());
        }
        if let SchemaCompositionState::AdoptedSourceRefining(adopted) =
            &transaction.schema_composition
        {
            if !Self::is_adopted_source_operation(&spec.operation) {
                return Err(SchemaMutationError::TransactionNotPristine.into());
            }
            if adopted.logical.touched.len() != 1
                || !adopted.logical.touched.contains_key(&spec.target.table_id)
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
                )
                .into());
            }
            if Self::is_adopted_source_nullability_operation(&spec.operation) {
                return self.apply_adopted_source_nullability(transaction, spec);
            }
            let result = self.apply_composed_alter(transaction, spec);
            self.handle_composition_accept_result(transaction, &result);
            return result;
        }
        if matches!(
            &transaction.schema_composition,
            SchemaCompositionState::BackfillOpen(_)
                | SchemaCompositionState::IndexEvacuating(_)
                | SchemaCompositionState::RefiningAfterEvacuation(_)
                | SchemaCompositionState::Refining(_)
                | SchemaCompositionState::IndexFinalizing(_)
                | SchemaCompositionState::BackfillOpenIndex(_)
                | SchemaCompositionState::RefiningIndex(_)
        ) {
            if matches!(
                transaction.schema_composition,
                SchemaCompositionState::IndexFinalizing(_)
            ) {
                return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
            }
            if transaction.schema_composition.backfill_index().is_some() {
                return self.apply_created_backfill_refinement(transaction, spec);
            }
            return self.apply_backfill_refinement(transaction, spec);
        }
        if transaction.schema_composition.is_none() && transaction.write_participant().is_some() {
            if Self::is_adopted_source_nullability_operation(&spec.operation) {
                return self.apply_adopted_source_nullability(transaction, spec);
            }
            if Self::is_adopted_source_layout_operation(&spec.operation) {
                return self.adopt_post_dml_source_and_refine(transaction, spec);
            }
        }
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_alter(transaction, spec);
        self.handle_composition_accept_result(transaction, &result);
        result
    }

    /// Returns whether an exact SQL DROP can enter the private staged-index
    /// evacuation lifecycle. The execution-time phase and physical S2
    /// inventory are authoritative; committed or in-place participants never
    /// qualify.
    pub(crate) fn should_route_drop_index_to_backfill_evacuation(
        &self,
        transaction: &Transaction,
        target: crate::DropIndexTarget,
    ) -> bool {
        if transaction.state() != TransactionState::Active {
            return false;
        }
        let materialized = match &transaction.schema_composition {
            SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::IndexEvacuating(materialized) => materialized,
            _ => return false,
        };
        if !materialized.backfill
            || materialized.logical.touched.len() != 1
            || !materialized.logical.touched.contains_key(&target.table_id)
            || materialized.staged.len() != 1
            || materialized.intent.tables.len() != 1
        {
            return false;
        }
        let table_plan = &materialized.intent.tables[0];
        if table_plan.table() != target.table_id
            || table_plan.old_storage() == table_plan.new_storage()
            || transaction.staged_binding(target.table_id) != Some(table_plan.new_storage())
        {
            return false;
        }
        let Some(storage) = materialized.staged.get(&table_plan.new_storage()) else {
            return false;
        };
        if storage.kind() != netbadb_storage::StorageKind::Heap
            || storage.table().id != target.table_id
        {
            return false;
        }
        materialized
            .staged_indexes
            .get(&target.table_id)
            .is_some_and(|indexes| {
                indexes
                    .active
                    .iter()
                    .any(|index| index.id == target.index_id)
            })
            && materialized.logical.touched[&target.table_id]
                .indexes
                .active
                .iter()
                .any(|index| index.id == target.index_id)
    }

    pub(crate) fn is_cross_table_drop_during_backfill_evacuation(
        &self,
        transaction: &Transaction,
        target: crate::DropIndexTarget,
    ) -> bool {
        let materialized = match &transaction.schema_composition {
            SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::IndexEvacuating(materialized) => materialized,
            _ => return false,
        };
        materialized.logical.touched.len() == 1
            && !materialized.logical.touched.contains_key(&target.table_id)
    }

    /// Immediately retires one exact incompatible index from the private S2
    /// Heap. SQL reaches this typed primitive only through the exact
    /// execution-time predicate above.
    pub(crate) fn evacuate_staged_backfill_index_in(
        &mut self,
        transaction: &mut Transaction,
        target: crate::DropIndexTarget,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        let (storage_id, staged_indexes) = {
            let materialized = match &transaction.schema_composition {
                SchemaCompositionState::BackfillOpen(materialized)
                | SchemaCompositionState::IndexEvacuating(materialized) => materialized,
                _ => {
                    return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
                }
            };
            if !materialized.backfill
                || materialized.logical.touched.len() != 1
                || materialized.staged.len() != 1
                || materialized.logical.action_count() >= MAX_SCHEMA_ACTIONS
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::MultipleTargets,
                )
                .into());
            }
            let table = materialized
                .logical
                .touched
                .get(&target.table_id)
                .ok_or(DatabaseError::UndefinedIndex)?;
            let mut staged_indexes = materialized
                .staged_indexes
                .get(&target.table_id)
                .cloned()
                .ok_or(SchemaMutationError::Corrupt(
                    "staged physical index inventory absent",
                ))?;
            let physical = staged_indexes
                .active
                .iter()
                .find(|index| index.id == target.index_id)
                .ok_or(DatabaseError::UndefinedIndex)?;
            let logical = table
                .indexes
                .active
                .iter()
                .find(|index| index.id == target.index_id)
                .ok_or(DatabaseError::UndefinedIndex)?;
            if physical != logical {
                return Err(SchemaMutationError::Corrupt(
                    "staged logical/physical index identity mismatch",
                )
                .into());
            }
            let storage_id = materialized
                .intent
                .tables
                .first()
                .filter(|plan| plan.table() == target.table_id)
                .map(CompositionTablePlan::new_storage)
                .ok_or(SchemaMutationError::Corrupt("backfill target absent"))?;
            staged_indexes
                .active
                .retain(|index| index.id != target.index_id);
            (storage_id, staged_indexes)
        };

        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let mut materialized = match previous {
            SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::IndexEvacuating(materialized) => materialized,
            other => {
                transaction.schema_composition = other;
                return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
            }
        };
        crash("backfill-evacuation-before-drop");
        let result = (|| -> Result<(), DatabaseError> {
            let storage = materialized
                .staged
                .get_mut(&storage_id)
                .ok_or(SchemaMutationError::Corrupt("backfill staged Heap absent"))?;
            let staged_table = storage.table().clone();
            transaction.with_detached_composed_staged_write(
                storage_id,
                storage,
                |table, context| {
                    table.drop_index_in(context, target.index_id)?;
                    table.publish_committed_index_drop(target.index_id);
                    table.validate_heap_rewrite_index_inventory(&staged_table, &staged_indexes)
                },
            )?;
            Ok(())
        })();
        if let Err(error) = result {
            transaction.schema_composition =
                SchemaCompositionState::RollbackRequiredMaterialized(materialized);
            transaction.require_schema_rollback();
            return Err(error);
        }
        crash("backfill-evacuation-physical-drop-durable");
        materialized
            .staged_indexes
            .insert(target.table_id, staged_indexes);
        let table = materialized
            .logical
            .touched
            .get_mut(&target.table_id)
            .ok_or(SchemaMutationError::Corrupt("backfill target disappeared"))?;
        table
            .indexes
            .active
            .retain(|index| index.id != target.index_id);
        materialized.logical.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"migration-evacuate-index");
        evidence.update(target.table_id.0.to_le_bytes());
        evidence.update(target.index_id.0.to_le_bytes());
        materialized
            .logical
            .action_evidence
            .push(evidence.finalize().into());
        crash("backfill-evacuation-inventory-updated");
        transaction.schema_composition = SchemaCompositionState::IndexEvacuating(materialized);
        Ok(())
    }

    fn validate_evacuated_index_compatibility(
        &mut self,
        transaction: &mut Transaction,
        table_id: TableId,
        candidate: &TableDef,
    ) -> Result<(), DatabaseError> {
        let (storage_id, indexes) = {
            let materialized = transaction
                .schema_composition
                .backfill()
                .ok_or(SchemaMutationError::Corrupt("backfill state absent"))?;
            let storage_id = materialized
                .intent
                .tables
                .first()
                .filter(|plan| plan.table() == table_id)
                .map(CompositionTablePlan::new_storage)
                .ok_or(SchemaMutationError::Corrupt("backfill target absent"))?;
            let indexes = materialized.staged_indexes.get(&table_id).cloned().ok_or(
                SchemaMutationError::Corrupt("staged physical index inventory absent"),
            )?;
            (storage_id, indexes)
        };
        let storage = transaction
            .execution_staged_storages_mut()
            .into_iter()
            .find(|storage| storage.storage_id() == storage_id)
            .ok_or(SchemaMutationError::Corrupt("backfill staged Heap absent"))?;
        storage.validate_heap_rewrite_index_inventory(candidate, &indexes)?;
        Ok(())
    }

    fn apply_backfill_refinement(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        let materialized = transaction
            .schema_composition
            .backfill()
            .ok_or(SchemaMutationError::Corrupt("backfill state absent"))?;
        let after_evacuation = matches!(
            transaction.schema_composition,
            SchemaCompositionState::IndexEvacuating(_)
                | SchemaCompositionState::RefiningAfterEvacuation(_)
        );
        let table_id = spec.target.table_id;
        if materialized.logical.dependency(table_id)? != spec.target {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let touched = materialized
            .logical
            .touched
            .get(&table_id)
            .ok_or(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::MultipleTargets,
            ))?
            .clone();
        let current = materialized
            .logical
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == table_id)
            .cloned()
            .ok_or(SchemaMutationError::TableNotFound(table_id))?;
        let visible_schema = materialized.logical.overlay.schema.clone();
        let column_id = match spec.operation {
            AlterTableOperation::RenameTable { .. } => None,
            AlterTableOperation::RenameColumn { column_id, .. }
            | AlterTableOperation::SetNotNull { column_id }
            | AlterTableOperation::DropNotNull { column_id } => Some(column_id),
            AlterTableOperation::AddNullableColumn { .. }
            | AlterTableOperation::DropColumn { .. }
            | AlterTableOperation::ChangeNominalType { .. } => {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
                )
                .into());
            }
        };
        if let Some(column_id) = column_id {
            if current.column_by_id(column_id).is_none() {
                return Err(SchemaMutationError::ColumnNotFound(column_id).into());
            }
            if matches!(
                spec.operation,
                AlterTableOperation::SetNotNull { .. } | AlterTableOperation::DropNotNull { .. }
            ) && (touched
                .indexes
                .active
                .iter()
                .any(|index| index.column_id == column_id)
                || (!after_evacuation
                    && transaction
                        .schema_composition
                        .backfill()
                        .is_some_and(|materialized| {
                            materialized.backfill_indexed_columns.contains(&column_id)
                        })))
            {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::IndexedNullability(column_id),
                )
                .into());
            }
        }
        let candidate = build_alter_target(&current, &spec.operation, None)?;
        if after_evacuation {
            self.validate_evacuated_index_compatibility(transaction, table_id, &candidate)?;
            crash("backfill-evacuation-compatibility-validated");
        }
        if matches!(spec.operation, AlterTableOperation::SetNotNull { .. }) {
            let column_id = column_id.ok_or(SchemaMutationError::Corrupt(
                "SET NOT NULL column identity absent",
            ))?;
            self.validate_backfill_not_null(transaction, table_id, column_id)?;
        }
        let schema = Schema::new(
            visible_schema
                .tables()
                .iter()
                .map(|table| {
                    if table.id == table_id {
                        candidate.clone()
                    } else {
                        table.clone()
                    }
                })
                .collect(),
        )?;
        let materialized = transaction
            .schema_composition
            .backfill_mut()
            .ok_or(SchemaMutationError::Corrupt("backfill state disappeared"))?;
        materialized.logical.overlay.schema = schema.clone();
        materialized.target.committed.schema = schema;
        if let Some(placement) = materialized
            .target
            .placements
            .tables
            .iter_mut()
            .find(|placement| placement.table_id == table_id)
        {
            placement.schema_fingerprint = candidate.fingerprint()?;
        }
        let table_plan = materialized
            .intent
            .tables
            .iter_mut()
            .find(|plan| plan.table() == table_id)
            .ok_or(SchemaMutationError::Corrupt("backfill table plan absent"))?;
        table_plan.target.committed.schema = Schema::new(vec![candidate.clone()])?;
        table_plan.target.placements.tables[0].schema_fingerprint = candidate.fingerprint()?;
        table_plan.target.storages[0].table_id = table_id;
        transaction.schema_composition = match std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        ) {
            SchemaCompositionState::BackfillOpen(materialized) => {
                SchemaCompositionState::Refining(materialized)
            }
            SchemaCompositionState::Refining(materialized) => {
                SchemaCompositionState::Refining(materialized)
            }
            SchemaCompositionState::IndexEvacuating(materialized)
            | SchemaCompositionState::RefiningAfterEvacuation(materialized) => {
                SchemaCompositionState::RefiningAfterEvacuation(materialized)
            }
            other => other,
        };
        if after_evacuation {
            crash("backfill-evacuation-refinement-accepted");
        }
        Ok(())
    }

    fn apply_created_backfill_refinement(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        let (table_id, current, visible_schema) = {
            let materialized = transaction.schema_composition.backfill_index().ok_or(
                SchemaMutationError::Corrupt("created backfill state absent"),
            )?;
            let table_id = spec.target.table_id;
            if materialized.logical.dependency(table_id)? != spec.target {
                return Err(SchemaMutationError::StaleSchemaDependency.into());
            }
            let current = materialized
                .logical
                .overlay
                .schema
                .tables()
                .iter()
                .find(|table| table.id == table_id)
                .cloned()
                .ok_or(SchemaMutationError::TableNotFound(table_id))?;
            (
                table_id,
                current,
                materialized.logical.overlay.schema.clone(),
            )
        };
        let column_id = match spec.operation {
            AlterTableOperation::RenameTable { .. } => None,
            AlterTableOperation::RenameColumn { column_id, .. }
            | AlterTableOperation::SetNotNull { column_id }
            | AlterTableOperation::DropNotNull { column_id } => Some(column_id),
            _ => {
                return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
                )
                .into());
            }
        };
        if let Some(column_id) = column_id {
            if current.column_by_id(column_id).is_none() {
                return Err(SchemaMutationError::ColumnNotFound(column_id).into());
            }
        }
        if let Some(column_id) =
            column_id.filter(|_| matches!(&spec.operation, AlterTableOperation::SetNotNull { .. }))
        {
            self.validate_backfill_not_null(transaction, table_id, column_id)?;
        }
        let candidate = build_alter_target(&current, &spec.operation, None)?;
        let schema = Schema::new(
            visible_schema
                .tables()
                .iter()
                .map(|table| {
                    if table.id == table_id {
                        candidate.clone()
                    } else {
                        table.clone()
                    }
                })
                .collect(),
        )?;
        let materialized = transaction.schema_composition.backfill_index_mut().ok_or(
            SchemaMutationError::Corrupt("created backfill state disappeared"),
        )?;
        materialized.logical.overlay.schema = schema.clone();
        if let Some(target) = materialized.target.as_mut() {
            target.committed.schema = schema;
            if let Some(placement) = target
                .placements
                .tables
                .iter_mut()
                .find(|placement| placement.table_id == table_id)
            {
                placement.schema_fingerprint = candidate.fingerprint()?;
            }
        }
        let plan = materialized
            .intent
            .tables
            .iter_mut()
            .find_map(|plan| match plan {
                SchemaIndexTablePlan::CreateHeap { target, .. }
                    if target.committed.schema.tables()[0].id == table_id =>
                {
                    Some(target)
                }
                _ => None,
            })
            .ok_or(SchemaMutationError::Corrupt(
                "created backfill table plan absent",
            ))?;
        plan.committed.schema = Schema::new(vec![candidate.clone()])?;
        plan.placements.tables[0].schema_fingerprint = candidate.fingerprint()?;
        let target_bytes = materialized
            .target
            .as_ref()
            .ok_or(SchemaMutationError::Corrupt(
                "created backfill target absent",
            ))?
            .encode()?;
        materialized.intent.snapshot_digest = digest(&target_bytes);
        if let Some(reference) = materialized.reference.as_mut() {
            reference.digest = materialized.intent.snapshot_digest;
        }
        transaction.schema_composition = match std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        ) {
            SchemaCompositionState::BackfillOpenIndex(materialized) => {
                SchemaCompositionState::RefiningIndex(materialized)
            }
            SchemaCompositionState::RefiningIndex(materialized) => {
                SchemaCompositionState::RefiningIndex(materialized)
            }
            other => other,
        };
        Ok(())
    }

    fn validate_backfill_not_null(
        &mut self,
        transaction: &mut Transaction,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<(), DatabaseError> {
        let storage_id =
            transaction
                .staged_binding(table_id)
                .ok_or(SchemaMutationError::Corrupt(
                    "backfill staged binding absent",
                ))?;
        let view = transaction.begin_read_view(&[storage_id], &mut self.registry)?;
        let storage_view = view
            .iter()
            .find_map(|(id, view)| (id == storage_id).then_some(view))
            .ok_or(SchemaMutationError::Corrupt("backfill staged view absent"))?;
        let storage = transaction
            .execution_staged_storages_mut()
            .into_iter()
            .find(|storage| storage.storage_id() == storage_id)
            .ok_or(SchemaMutationError::Corrupt("backfill staged Heap absent"))?;
        if storage
            .scan_columns_with_view(&[column_id], storage_view)?
            .iter()
            .any(|(_, values)| {
                values
                    .iter()
                    .any(|value| matches!(value, ScalarValue::Null))
            })
        {
            return Err(SchemaMutationError::NotNullViolation(column_id).into());
        }
        Ok(())
    }

    /// Closes the open backfill after all compatible DDL has been accepted.
    /// The staged Heap is retargeted in place, then the final NBSC digest used
    /// by the existing coordinator path is refreshed.  No row or index page is
    /// copied a second time.
    pub(crate) fn finalize_backfill(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        if transaction.schema_composition.backfill_index().is_some() {
            return self.finalize_created_backfill(transaction);
        }
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let mut materialized = match previous {
            SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::RefiningAfterEvacuation(materialized)
            | SchemaCompositionState::Refining(materialized)
            | SchemaCompositionState::IndexFinalizing(materialized) => materialized,
            SchemaCompositionState::IndexEvacuating(materialized) => {
                transaction.schema_composition =
                    SchemaCompositionState::IndexEvacuating(materialized);
                return Err(SchemaMutationError::EvacuationRequiresRefinement.into());
            }
            other => {
                transaction.schema_composition = other;
                return Ok(());
            }
        };
        if !materialized.backfill {
            transaction.schema_composition = SchemaCompositionState::Finalized(materialized);
            return Ok(());
        }
        let result = (|| {
            let plan = materialized
                .intent
                .tables
                .first()
                .ok_or(SchemaMutationError::Corrupt("backfill table plan absent"))?;
            let storage_id = plan.new_storage();
            let final_table = materialized
                .target
                .committed
                .schema
                .tables()
                .iter()
                .find(|table| table.id == plan.table())
                .cloned()
                .ok_or(SchemaMutationError::TableNotFound(plan.table()))?;
            let storage = materialized
                .staged
                .get_mut(&storage_id)
                .ok_or(SchemaMutationError::Corrupt("backfill staged Heap absent"))?;
            let staged_indexes = materialized
                .staged_indexes
                .get(&plan.table())
                .cloned()
                .ok_or(SchemaMutationError::Corrupt(
                    "staged physical index inventory absent",
                ))?;
            storage.validate_heap_rewrite_index_inventory(&final_table, &staged_indexes)?;
            let expected = storage.table().clone();
            let old_fingerprint = expected.fingerprint()?;
            crash("backfill-before-retarget");
            let owner = file::resolve(
                &materialized.logical.catalog,
                &stage_locator(
                    &materialized.logical.catalog,
                    materialized.target.incarnation,
                    materialized.intent.transaction,
                    storage_id,
                )?,
            );
            storage.retarget_private_schema(&expected, final_table.clone())?;
            storage.validate_heap_rewrite_index_inventory(&final_table, &staged_indexes)?;
            crash("backfill-after-heap-retarget");
            retarget_owner(
                &file::suffix(&owner, ".owner"),
                materialized.target.incarnation,
                materialized.intent.transaction,
                plan.table(),
                storage_id,
                old_fingerprint,
                storage.table().fingerprint()?,
            )?;
            crash("backfill-after-owner-retarget");
            let final_indexes = materialized
                .logical
                .touched
                .get(&plan.table())
                .ok_or(SchemaMutationError::Corrupt(
                    "backfill index inventory absent",
                ))?
                .indexes
                .clone();
            if staged_indexes.active != final_indexes.active
                || staged_indexes.next_index_id != final_indexes.next_index_id
            {
                self.finalize_backfill_index_delta_in_storage(
                    transaction,
                    storage_id,
                    storage,
                    &staged_indexes,
                    &final_indexes,
                    &final_table,
                )?;
                materialized
                    .staged_indexes
                    .insert(plan.table(), final_indexes.clone());
                crash("backfill-index-delta-durable");
            }
            storage.validate_heap_rewrite_index_inventory(&final_table, &final_indexes)?;
            materialized.target.validate()?;
            let final_snapshot_digest = digest(&materialized.target.encode()?);
            if materialized.logical.index_actions > 0 {
                materialized.intent.snapshot_digest = final_snapshot_digest;
                materialized.reference.digest = final_snapshot_digest;
                materialized
                    .logical
                    .journal
                    .borrow_mut()
                    .replace_composition_intent(materialized.intent.clone())?;
                crash("backfill-before-index-finalization-intent");
                let dependency = materialized.logical.dependency(plan.table())?;
                let mut index_digest = Sha256::new();
                for index in &final_indexes.active {
                    index_digest.update(index.id.0.to_le_bytes());
                    index_digest.update(index.column_id.0.to_le_bytes());
                    if let Some(name) = &index.name {
                        index_digest.update(name.as_str().as_bytes());
                    }
                }
                index_digest.update(final_indexes.next_index_id.0.to_le_bytes());
                let mut evidence = Sha256::new();
                evidence.update(final_snapshot_digest);
                evidence.update(index_digest.finalize());
                materialized
                    .logical
                    .journal
                    .borrow_mut()
                    .migration_finalization_intent(MigrationIndexFinalizationIntent {
                        transaction: materialized.intent.transaction,
                        table: plan.table(),
                        storage: storage_id,
                        final_table_version: dependency.table_version,
                        final_fingerprint: final_table.fingerprint()?,
                        final_snapshot_digest,
                        stage_locator: stage_locator(
                            &materialized.logical.catalog,
                            materialized.target.incarnation,
                            materialized.intent.transaction,
                            storage_id,
                        )?,
                        final_locator: final_locator(
                            &materialized.logical.catalog,
                            materialized.target.incarnation,
                            storage_id,
                        )?,
                        final_indexes: final_indexes.clone(),
                        digest: evidence.finalize().into(),
                    })?;
                crash("backfill-index-finalization-intent-durable");
                return Ok::<(), DatabaseError>(());
            }
            let target_bytes = materialized.target.encode()?;
            materialized.intent.snapshot_digest = digest(&target_bytes);
            materialized.reference.digest = materialized.intent.snapshot_digest;
            materialized
                .logical
                .journal
                .borrow_mut()
                .replace_composition_intent(materialized.intent.clone())?;
            crash("backfill-before-finalization-intent");
            materialized
                .logical
                .journal
                .borrow_mut()
                .finalization_intent(FinalizationIntent {
                    transaction: materialized.intent.transaction,
                    table: plan.table(),
                    storage: storage_id,
                    final_snapshot: materialized.target.clone(),
                    stage_locator: stage_locator(
                        &materialized.logical.catalog,
                        materialized.target.incarnation,
                        materialized.intent.transaction,
                        storage_id,
                    )?,
                    final_locator: final_locator(
                        &materialized.logical.catalog,
                        materialized.target.incarnation,
                        storage_id,
                    )?,
                    digest: materialized.intent.snapshot_digest,
                })?;
            crash("backfill-finalization-intent-durable");
            Ok::<(), DatabaseError>(())
        })();
        match result {
            Ok(()) => {
                transaction.schema_composition = SchemaCompositionState::Finalized(materialized);
                Ok(())
            }
            Err(error) => {
                transaction.schema_composition = SchemaCompositionState::Finalizing(materialized);
                transaction.require_schema_rollback();
                Err(error)
            }
        }
    }

    fn finalize_backfill_index_delta_in_storage(
        &mut self,
        transaction: &mut Transaction,
        storage_id: StorageId,
        storage: &mut TableStorage,
        staged_indexes: &HeapRewriteIndexes,
        final_indexes: &HeapRewriteIndexes,
        final_table: &TableDef,
    ) -> Result<(), DatabaseError> {
        let drops = staged_indexes
            .active
            .iter()
            .filter(|base| {
                !final_indexes
                    .active
                    .iter()
                    .any(|final_index| final_index.id == base.id && *final_index == **base)
            })
            .map(|index| index.id)
            .collect::<Vec<_>>();
        let creates = final_indexes
            .active
            .iter()
            .filter(|final_index| {
                !staged_indexes
                    .active
                    .iter()
                    .any(|base| **final_index == *base)
            })
            .cloned()
            .collect::<Vec<_>>();
        let target_floor = final_indexes.next_index_id;
        transaction.with_detached_composed_staged_write(
            storage_id,
            storage,
            |table, context| {
                for id in &drops {
                    table.drop_index_in(context, *id)?;
                }
                table.advance_index_id_floor_in(context, target_floor)?;
                let mut definitions = Vec::with_capacity(creates.len());
                for create in &creates {
                    let name = create
                        .name
                        .clone()
                        .ok_or(netbadb_storage::StorageError::from(
                            netbadb_index::IndexError::InvalidIndexHighWater(target_floor),
                        ))?;
                    definitions.push(table.create_named_index_with_reserved_id_in(
                        context,
                        name,
                        create.column_id,
                        create.id,
                        target_floor,
                    )?);
                }
                for id in &drops {
                    table.publish_committed_index_drop(*id);
                }
                for definition in definitions {
                    table.publish_committed_index(definition);
                }
                table.validate_heap_rewrite_index_inventory(final_table, final_indexes)?;
                Ok(())
            },
        )?;
        Ok(())
    }

    fn finalize_created_backfill(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let mut materialized = match previous {
            SchemaCompositionState::BackfillOpenIndex(materialized)
            | SchemaCompositionState::RefiningIndex(materialized) => materialized,
            other => {
                transaction.schema_composition = other;
                return Ok(());
            }
        };
        if !materialized.backfill {
            transaction.schema_composition = SchemaCompositionState::FinalizedIndex(materialized);
            return Ok(());
        }
        let result = (|| {
            let (table_id, storage_id, final_table) = {
                let plan = materialized
                    .intent
                    .tables
                    .first()
                    .ok_or(SchemaMutationError::Corrupt("created backfill plan absent"))?;
                let table_id = plan.table();
                let storage_id = plan
                    .participant_storage()
                    .ok_or(SchemaMutationError::Corrupt(
                        "created backfill storage absent",
                    ))?;
                let final_table = materialized
                    .target
                    .as_ref()
                    .ok_or(SchemaMutationError::Corrupt(
                        "created backfill target absent",
                    ))?
                    .committed
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == table_id)
                    .cloned()
                    .ok_or(SchemaMutationError::TableNotFound(table_id))?;
                (table_id, storage_id, final_table)
            };
            let expected = materialized
                .staged
                .get(&storage_id)
                .ok_or(SchemaMutationError::Corrupt("created staged Heap absent"))?
                .table()
                .clone();
            let old_fingerprint = expected.fingerprint()?;
            crash("backfill-before-retarget");
            let owner = file::resolve(
                &materialized.logical.catalog,
                &stage_locator(
                    &materialized.logical.catalog,
                    materialized.logical.base.incarnation,
                    materialized.intent.transaction,
                    storage_id,
                )?,
            );
            materialized
                .staged
                .get_mut(&storage_id)
                .ok_or(SchemaMutationError::Corrupt("created staged Heap absent"))?
                .retarget_private_schema(&expected, final_table)?;
            crash("backfill-after-heap-retarget");
            let new_fingerprint = materialized
                .staged
                .get(&storage_id)
                .ok_or(SchemaMutationError::Corrupt("created staged Heap absent"))?
                .table()
                .fingerprint()?;
            retarget_owner(
                &file::suffix(&owner, ".owner"),
                materialized.logical.base.incarnation,
                materialized.intent.transaction,
                table_id,
                storage_id,
                old_fingerprint,
                new_fingerprint,
            )?;
            crash("backfill-after-owner-retarget");
            let target = materialized
                .target
                .as_ref()
                .ok_or(SchemaMutationError::Corrupt(
                    "created backfill target absent",
                ))?;
            let target_bytes = target.encode()?;
            materialized.intent.snapshot_digest = digest(&target_bytes);
            if let Some(reference) = materialized.reference.as_mut() {
                reference.digest = materialized.intent.snapshot_digest;
            }
            materialized
                .logical
                .journal
                .borrow_mut()
                .replace_table_object_intent(materialized.intent.clone())?;
            crash("backfill-before-finalization-intent");
            materialized
                .logical
                .journal
                .borrow_mut()
                .finalization_intent(FinalizationIntent {
                    transaction: materialized.intent.transaction,
                    table: table_id,
                    storage: storage_id,
                    final_snapshot: materialized.target.clone().ok_or(
                        SchemaMutationError::Corrupt("created backfill target absent"),
                    )?,
                    stage_locator: stage_locator(
                        &materialized.logical.catalog,
                        materialized.logical.base.incarnation,
                        materialized.intent.transaction,
                        storage_id,
                    )?,
                    final_locator: final_locator(
                        &materialized.logical.catalog,
                        materialized.logical.base.incarnation,
                        storage_id,
                    )?,
                    digest: materialized.intent.snapshot_digest,
                })?;
            crash("backfill-finalization-intent-durable");
            Ok::<(), DatabaseError>(())
        })();
        match result {
            Ok(()) => {
                transaction.schema_composition =
                    SchemaCompositionState::FinalizedIndex(materialized);
                Ok(())
            }
            Err(error) => {
                transaction.schema_composition =
                    SchemaCompositionState::RefiningIndex(materialized);
                transaction.require_schema_rollback();
                Err(error)
            }
        }
    }

    fn ensure_composition_started(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        if transaction.schema_mutation.is_some()
            || transaction.has_pending_index_creations()
            || transaction.has_pending_index_drops()
        {
            return Err(DatabaseError::UnsupportedDdlCombination);
        }
        if transaction.schema_composition.is_sealed() {
            return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
        }
        if transaction.schema_composition.is_none() {
            if !transaction.is_pristine_for_schema_composition() {
                return Err(SchemaMutationError::TransactionNotPristine.into());
            }
            if self.schema_writer.get().is_some() || Rc::strong_count(&self.transaction_owner) != 2
            {
                return Err(SchemaMutationError::SchemaBusy.into());
            }
            let plan = self.start_schema_composition(transaction.id())?;
            self.schema_writer.set(Some(transaction.id()));
            transaction.schema_composition = SchemaCompositionState::Composing(Box::new(plan));
        }
        Ok(())
    }

    fn is_adopted_source_layout_operation(operation: &AlterTableOperation) -> bool {
        matches!(
            operation,
            AlterTableOperation::AddNullableColumn { .. }
                | AlterTableOperation::DropColumn { .. }
                | AlterTableOperation::RenameTable { .. }
                | AlterTableOperation::RenameColumn { .. }
        )
    }

    fn is_adopted_source_nullability_operation(operation: &AlterTableOperation) -> bool {
        matches!(
            operation,
            AlterTableOperation::SetNotNull { .. } | AlterTableOperation::DropNotNull { .. }
        )
    }

    fn is_adopted_source_operation(operation: &AlterTableOperation) -> bool {
        Self::is_adopted_source_layout_operation(operation)
            || Self::is_adopted_source_nullability_operation(operation)
    }

    /// Captures the exact ordinary one-Heap DML authority before either the
    /// schema writer or the mutation journal is changed.
    fn preflight_post_dml_source_adoption(
        &mut self,
        transaction: &Transaction,
        spec: &AlterTableSpec,
    ) -> Result<AdoptedSourceTransaction, DatabaseError> {
        self.validate_transaction(transaction)?;
        if !Self::is_adopted_source_operation(&spec.operation) {
            return Err(SchemaMutationError::TransactionNotPristine.into());
        }
        self.capture_post_dml_source_adoption(transaction, spec)
    }

    fn capture_post_dml_source_adoption(
        &mut self,
        transaction: &Transaction,
        spec: &AlterTableSpec,
    ) -> Result<AdoptedSourceTransaction, DatabaseError> {
        if transaction.schema_mutation.is_some()
            || !transaction.schema_composition.is_none()
            || transaction.has_pending_index_creations()
            || transaction.has_pending_index_drops()
        {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
            )
            .into());
        }
        let source_storage = transaction.write_participant().ok_or(
            SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
            ),
        )?;
        if !transaction.is_only_participant(source_storage)
            || !transaction.is_only_write_participant(source_storage)
        {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
            )
            .into());
        }
        let target = &spec.target;
        let table = self
            .committed
            .schema
            .tables()
            .iter()
            .find(|table| table.id == target.table_id)
            .cloned()
            .ok_or(SchemaMutationError::TableNotFound(target.table_id))?;
        let lineage = self
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == target.table_id)
            .cloned()
            .ok_or(SchemaMutationError::Corrupt(
                "source adoption lineage absent",
            ))?;
        let fingerprint = table.fingerprint()?;
        if lineage.version != target.table_version || fingerprint != target.fingerprint {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        if !matches!(
            self.bindings.placement(target.table_id),
            Ok(TablePlacement::Single { storage_id, .. }) if *storage_id == source_storage
        ) {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let catalog = self
            .catalog_path
            .as_ref()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let snapshot = file::load(catalog)?;
        if snapshot.committed != self.committed {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let descriptor = snapshot
            .storages
            .iter()
            .find(|descriptor| descriptor.id == source_storage)
            .cloned()
            .ok_or(SchemaMutationError::Corrupt(
                "source adoption descriptor absent",
            ))?;
        let expected_locator = final_locator(catalog, snapshot.incarnation, source_storage)?;
        if descriptor.table_id != target.table_id
            || !matches!(descriptor.kind, CatalogStorageKind::Heap)
            || descriptor.locator != expected_locator
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let source = self
            .registry
            .get_mut(source_storage)
            .ok_or(SchemaMutationError::Corrupt("source adoption Heap absent"))?;
        if source.kind() != netbadb_storage::StorageKind::Heap
            || source.storage_id() != source_storage
            || source.table() != &table
            || source.table().fingerprint()? != fingerprint
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let indexes = source.heap_rewrite_indexes()?;
        let physical_txn_id = transaction.physical_transaction_id(source_storage).ok_or(
            SchemaMutationError::Corrupt("source adoption physical transaction absent"),
        )?;
        let source_index_digest =
            crate::schema_mutation_journal::heap_rewrite_indexes_digest(&indexes)?;
        if self.schema_writer.get().is_some() || Rc::strong_count(&self.transaction_owner) != 2 {
            return Err(SchemaMutationError::SchemaBusy.into());
        }
        let mut logical = self.start_schema_composition(transaction.id())?;
        let touched = self.capture_composed_table(&logical.base, &table, target.fingerprint)?;
        if touched.old_storage != source_storage
            || touched.base_lineage.version != lineage.version
            || touched.base_table.fingerprint()? != fingerprint
            || crate::schema_mutation_journal::heap_rewrite_indexes_digest(&touched.base_indexes)?
                != source_index_digest
        {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        logical.touched.insert(target.table_id, touched);
        Self::validate_adopted_source_alter(&logical, spec)?;
        Ok(AdoptedSourceTransaction {
            logical,
            source_storage,
            source_physical_txn_id: physical_txn_id,
            source_table_version: lineage.version,
            source_fingerprint: fingerprint,
            source_locator: descriptor.locator.clone(),
            base_generation: snapshot.committed.generation,
            base_epoch: snapshot.epoch,
            source_index_digest,
        })
    }

    fn validate_source_view_not_null(
        &mut self,
        transaction: &mut Transaction,
        source_storage: StorageId,
        column_id: ColumnId,
    ) -> Result<(), DatabaseError> {
        #[cfg(test)]
        SOURCE_NOT_NULL_VALIDATION_COUNT.set(
            SOURCE_NOT_NULL_VALIDATION_COUNT
                .get()
                .checked_add(1)
                .expect("test validation counter overflow"),
        );
        let view = transaction.begin_read_view(&[source_storage], &mut self.registry)?;
        let source_view = view
            .iter()
            .find_map(|(storage, view)| (storage == source_storage).then_some(view))
            .ok_or(SchemaMutationError::Corrupt(
                "source-backfill read view absent",
            ))?;
        if self
            .registry
            .get_mut(source_storage)
            .ok_or(SchemaMutationError::Corrupt(
                "source-backfill source Heap absent",
            ))?
            .scan_columns_with_view(&[column_id], source_view)?
            .iter()
            .any(|(_, values)| {
                values
                    .iter()
                    .any(|value| matches!(value, ScalarValue::Null))
            })
        {
            return Err(SchemaMutationError::NotNullViolation(column_id).into());
        }
        Ok(())
    }

    fn apply_adopted_source_nullability(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        let column_id = match spec.operation {
            AlterTableOperation::SetNotNull { column_id }
            | AlterTableOperation::DropNotNull { column_id } => column_id,
            _ => return Err(SchemaMutationError::TransactionNotPristine.into()),
        };
        let first = transaction.schema_composition.is_none();
        let source_storage = if first {
            let adopted = self.preflight_post_dml_source_adoption(transaction, &spec)?;
            crash("post-dml-adoption-preflight-complete");
            let touched = adopted
                .logical
                .touched
                .get(&spec.target.table_id)
                .ok_or(SchemaMutationError::Corrupt("adopted source table absent"))?;
            if touched.base_table.column_by_id(column_id).is_none() {
                return Err(SchemaMutationError::TransactionNotPristine.into());
            }
            let source_storage = adopted.source_storage;
            let source_not_null_validated =
                matches!(spec.operation, AlterTableOperation::SetNotNull { .. });
            if source_not_null_validated {
                self.validate_source_view_not_null(transaction, source_storage, column_id)?;
                crash("post-dml-not-null-validation-complete");
            }
            self.schema_writer.set(Some(transaction.id()));
            transaction.schema_composition =
                SchemaCompositionState::AdoptedSourceRefining(Box::new(adopted));
            crash("post-dml-adopted-source-installed");
            source_storage
        } else {
            let adopted = match &transaction.schema_composition {
                SchemaCompositionState::AdoptedSourceRefining(adopted) => adopted,
                _ => {
                    return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into());
                }
            };
            Self::validate_adopted_source_alter(&adopted.logical, &spec)?;
            let touched = adopted
                .logical
                .touched
                .get(&spec.target.table_id)
                .ok_or(SchemaMutationError::Corrupt("adopted source table absent"))?;
            if touched.base_table.column_by_id(column_id).is_none() {
                return Err(SchemaMutationError::TransactionNotPristine.into());
            }
            let source_storage = adopted.source_storage;
            if matches!(spec.operation, AlterTableOperation::SetNotNull { .. }) {
                self.validate_source_view_not_null(transaction, source_storage, column_id)?;
            }
            source_storage
        };
        debug_assert_eq!(
            transaction.write_participant(),
            Some(source_storage),
            "nullability refinement must retain the exact adopted source"
        );
        let source_not_null_validated =
            matches!(spec.operation, AlterTableOperation::SetNotNull { .. });
        let result = self.apply_composed_alter_with_context(
            transaction,
            spec,
            AlterValidationContext::AdoptedSource {
                source_not_null_validated,
            },
        );
        self.handle_composition_accept_result(transaction, &result);
        if first && result.is_ok() {
            crash("post-dml-first-refinement-accepted");
        }
        if first
            && result.is_err()
            && !matches!(
                transaction.schema_composition,
                SchemaCompositionState::RollbackRequiredLogical(_)
            )
        {
            let previous = std::mem::replace(
                &mut transaction.schema_composition,
                SchemaCompositionState::None,
            );
            if let SchemaCompositionState::AdoptedSourceRefining(adopted) = previous {
                adopted.logical.writer.set(None);
            } else {
                transaction.schema_composition = previous;
            }
        }
        result
    }

    fn validate_adopted_source_alter(
        logical: &SchemaTransactionPlan,
        spec: &AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        if logical.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(SchemaMutationError::CompositionLimitExceeded("schema actions").into());
        }
        if logical.dependency(spec.target.table_id)? != spec.target {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let current = logical
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == spec.target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(spec.target.table_id))?;
        let touched = logical
            .touched
            .get(&spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt("adopted source table absent"))?;
        if let AlterTableOperation::DropColumn { column_id } = spec.operation {
            if current
                .column_by_id(column_id)
                .is_some_and(|column| column.primary_key)
            {
                return Err(SchemaMutationError::PrimaryKeyColumn(column_id).into());
            }
            if touched
                .indexes
                .active
                .iter()
                .any(|index| index.column_id == column_id)
            {
                return Err(SchemaMutationError::IndexedColumn(column_id).into());
            }
        }
        let lineage = logical
            .overlay
            .tables
            .iter()
            .find(|lineage| lineage.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt(
                "adopted source lineage absent",
            ))?;
        let reserved = matches!(
            spec.operation,
            AlterTableOperation::AddNullableColumn { .. }
        )
        .then_some(
            lineage
                .next_column_id
                .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?,
        );
        if let Some(column) = reserved {
            if logical.reservation_count >= MAX_COLUMN_RESERVATIONS {
                return Err(
                    SchemaMutationError::CompositionLimitExceeded("ColumnId reservations").into(),
                );
            }
            column
                .0
                .checked_add(1)
                .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?;
        }
        touched
            .base_lineage
            .version
            .0
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("TableSchemaVersion"))?;
        let candidate = build_alter_target(current, &spec.operation, reserved)?;
        Schema::new(
            logical
                .overlay
                .schema
                .tables()
                .iter()
                .map(|table| {
                    if table.id == candidate.id {
                        candidate.clone()
                    } else {
                        table.clone()
                    }
                })
                .collect(),
        )?;
        for index in &touched.indexes.active {
            let target = candidate
                .column_by_id(index.column_id)
                .ok_or(SchemaMutationError::IndexedColumn(index.column_id))?;
            if let Some(source) = touched.base_table.column_by_id(index.column_id) {
                if source.semantic_type().physical != target.semantic_type().physical {
                    return Err(SchemaMutationError::UnsupportedSchemaEvolution.into());
                }
            }
        }
        Ok(())
    }

    fn adopt_post_dml_source_and_refine(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        let adopted = self.preflight_post_dml_source_adoption(transaction, &spec)?;
        crash("post-dml-adoption-preflight-complete");
        self.schema_writer.set(Some(transaction.id()));
        transaction.schema_composition =
            SchemaCompositionState::AdoptedSourceRefining(Box::new(adopted));
        crash("post-dml-adopted-source-installed");
        let result = self.apply_composed_alter(transaction, spec);
        self.handle_composition_accept_result(transaction, &result);
        if result.is_ok() {
            crash("post-dml-first-refinement-accepted");
        } else if !matches!(
            transaction.schema_composition,
            SchemaCompositionState::RollbackRequiredLogical(_)
        ) {
            let previous = std::mem::replace(
                &mut transaction.schema_composition,
                SchemaCompositionState::None,
            );
            if let SchemaCompositionState::AdoptedSourceRefining(adopted) = previous {
                adopted.logical.writer.set(None);
            } else {
                transaction.schema_composition = previous;
            }
        }
        result
    }

    /// Round 43 Candidate A probe. Current NBSJ validation deliberately
    /// rejects an identity InPlaceIndexDelta before it can be persisted.
    #[cfg(test)]
    pub(crate) fn audit_candidate_a_identity_intent(
        &mut self,
        transaction: &Transaction,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        let logical = match &transaction.schema_composition {
            SchemaCompositionState::AdoptedSourceRefining(adopted) => &adopted.logical,
            _ => {
                return Err(SchemaMutationError::Corrupt(
                    "candidate A audit lacks adopted logical state",
                )
                .into());
            }
        };
        let (table, touched) =
            logical
                .touched
                .iter()
                .next()
                .ok_or(SchemaMutationError::Corrupt(
                    "candidate A audit source absent",
                ))?;
        logical
            .journal
            .borrow_mut()
            .schema_index_intent(SchemaIndexChangeSetIntent {
                transaction: transaction.id(),
                base_generation: logical.base.committed.generation,
                target_generation: None,
                base_epoch: logical.base.epoch,
                target_epoch: None,
                action_count: 1,
                action_digest: [0x43; 32],
                snapshot_digest: None,
                tables: vec![SchemaIndexTablePlan::InPlaceIndexDelta {
                    table: *table,
                    table_version: touched.base_lineage.version,
                    fingerprint: touched.base_table.fingerprint()?,
                    storage: touched.old_storage,
                    base_indexes: touched.base_indexes.clone(),
                    final_indexes: touched.base_indexes.clone(),
                }],
            })?;
        Ok(())
    }

    // Shared authority check; physical index finalization may consume this proof
    // only before its authorized delta changes the captured source digest.
    fn revalidate_adopted_source_authority(
        &mut self,
        transaction: &Transaction,
    ) -> Result<(), DatabaseError> {
        self.validate_transaction(transaction)?;
        let adopted = match &transaction.schema_composition {
            SchemaCompositionState::AdoptedSourceRefining(adopted) => adopted,
            _ => {
                return Err(
                    SchemaMutationError::Corrupt("adopted source state is not logical").into(),
                );
            }
        };
        let current_snapshot = file::load(&adopted.logical.catalog)?;
        if current_snapshot.incarnation != adopted.logical.base.incarnation
            || current_snapshot.committed != adopted.logical.base.committed
            || current_snapshot.committed.generation != adopted.base_generation
            || current_snapshot.epoch != adopted.base_epoch
        {
            return Err(SchemaMutationError::Corrupt("adopted source catalog drift").into());
        }
        if !transaction.is_only_participant(adopted.source_storage)
            || !transaction.is_only_write_participant(adopted.source_storage)
            || transaction.physical_transaction_id(adopted.source_storage)
                != Some(adopted.source_physical_txn_id)
            || !matches!(
                self.bindings.placement(
                    adopted
                        .logical
                        .touched
                        .keys()
                        .next()
                        .copied()
                        .ok_or(SchemaMutationError::Corrupt("adopted source table absent"))?
                ),
                Ok(TablePlacement::Single { storage_id, .. })
                    if *storage_id == adopted.source_storage
            )
        {
            return Err(SchemaMutationError::Corrupt("adopted source transaction drift").into());
        }
        let touched = adopted
            .logical
            .touched
            .values()
            .next()
            .ok_or(SchemaMutationError::Corrupt("adopted source table absent"))?;
        let descriptor = current_snapshot
            .storages
            .iter()
            .find(|descriptor| descriptor.id == adopted.source_storage)
            .ok_or(SchemaMutationError::Corrupt(
                "adopted source descriptor absent",
            ))?;
        let source = self
            .registry
            .get_mut(adopted.source_storage)
            .ok_or(SchemaMutationError::Corrupt("adopted source Heap absent"))?;
        if touched.old_storage != adopted.source_storage
            || touched.base_lineage.version != adopted.source_table_version
            || touched.base_table.fingerprint()? != adopted.source_fingerprint
            || descriptor.table_id != touched.base_table.id
            || !matches!(descriptor.kind, CatalogStorageKind::Heap)
            || descriptor.locator != adopted.source_locator
            || descriptor.locator
                != final_locator(
                    &adopted.logical.catalog,
                    current_snapshot.incarnation,
                    adopted.source_storage,
                )?
            || source.kind() != netbadb_storage::StorageKind::Heap
            || source.storage_id() != adopted.source_storage
            || source.table() != &touched.base_table
            || crate::schema_mutation_journal::heap_rewrite_indexes_digest(
                &source.heap_rewrite_indexes()?,
            )? != adopted.source_index_digest
        {
            return Err(SchemaMutationError::Corrupt("adopted source authority drift").into());
        }
        Ok(())
    }

    /// Seals an adopted ordinary DML source. Effective changes create the
    /// first real rewrite intent; canonical no-ops retain S1 and only resolve
    /// allocator history.
    pub(crate) fn finalize_adopted_source(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        self.revalidate_adopted_source_authority(transaction)?;
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let adopted = match previous {
            SchemaCompositionState::AdoptedSourceRefining(adopted) => *adopted,
            other => {
                transaction.schema_composition = other;
                return Err(
                    SchemaMutationError::Corrupt("adopted source state is not logical").into(),
                );
            }
        };
        let rollback_logical = adopted.logical.clone();
        let result = self.materialize_schema_index_composition(
            transaction,
            adopted.logical,
            SchemaIndexMaterialization::AdoptedSourceBackfill {
                source_storage: adopted.source_storage,
                source_physical_txn_id: adopted.source_physical_txn_id,
            },
        );
        if let Err(error) = result {
            transaction.require_schema_rollback();
            let previous = std::mem::replace(
                &mut transaction.schema_composition,
                SchemaCompositionState::None,
            );
            transaction.schema_composition = match previous {
                SchemaCompositionState::SealingAndMaterializingIndex(materialized)
                | SchemaCompositionState::LateCloneMaterializing(materialized) => {
                    SchemaCompositionState::RollbackRequiredMaterializedIndex(materialized)
                }
                SchemaCompositionState::None => {
                    SchemaCompositionState::RollbackRequiredLogical(Box::new(rollback_logical))
                }
                other => other,
            };
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn is_backfill_candidate(logical: &SchemaTransactionPlan) -> bool {
        logical.table_actions == 0
            && logical.index_actions == 0
            && logical.created.is_empty()
            && logical.touched.len() == 1
            && logical.touched.values().all(|table| {
                matches!(table.descriptor.kind, CatalogStorageKind::Heap)
                    && matches!(table.catalog_table.placement, TablePlacement::Single { .. })
            })
    }

    fn handle_composition_accept_result<T>(
        &self,
        transaction: &mut Transaction,
        result: &Result<T, DatabaseError>,
    ) {
        if matches!(
            result,
            Err(DatabaseError::SchemaMutation(
                SchemaMutationError::RecoveryRequired
            ))
        ) {
            transaction.require_schema_rollback();
            let previous = std::mem::replace(
                &mut transaction.schema_composition,
                SchemaCompositionState::None,
            );
            transaction.schema_composition = match previous {
                SchemaCompositionState::Composing(plan) => {
                    SchemaCompositionState::RollbackRequiredLogical(plan)
                }
                SchemaCompositionState::AdoptedSourceRefining(adopted) => {
                    SchemaCompositionState::RollbackRequiredLogical(Box::new(adopted.logical))
                }
                other => other,
            };
        }
    }

    fn start_schema_composition(
        &mut self,
        transaction: DatabaseTxnId,
    ) -> Result<SchemaTransactionPlan, DatabaseError> {
        for entry in self.registry.iter() {
            entry.storage.ensure_recovery_ready()?;
        }
        let catalog = self
            .catalog_path
            .clone()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let base = file::load(&catalog)?;
        if base.committed != self.committed {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
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
        let journal = match &self.mutation_journal {
            Some(journal) => Rc::clone(journal),
            None => {
                let journal = SchemaMutationJournal::initialize(
                    &catalog,
                    base.incarnation,
                    coordinator_locator.clone(),
                )?;
                let journal = Rc::new(std::cell::RefCell::new(journal));
                self.mutation_journal = Some(Rc::clone(&journal));
                journal
            }
        };
        journal.borrow().ensure_ready()?;
        let mut overlay = base.committed.clone();
        for lineage in &mut overlay.tables {
            lineage.next_column_id = journal
                .borrow()
                .effective_column(lineage.table_id, lineage.next_column_id);
        }
        Ok(SchemaTransactionPlan {
            transaction,
            catalog,
            overlay,
            base,
            touched: BTreeMap::new(),
            created: BTreeMap::new(),
            action_evidence: Vec::new(),
            reservation_count: 0,
            index_reservation_count: 0,
            index_actions: 0,
            table_actions: 0,
            journal,
            writer: Rc::clone(&self.schema_writer),
        })
    }

    pub(crate) fn compose_create_heap_table_in(
        &mut self,
        transaction: &mut Transaction,
        spec: CreateTableSpec,
    ) -> Result<TableId, DatabaseError> {
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_create_table(transaction, spec);
        self.handle_composition_accept_result(transaction, &result);
        result
    }

    fn apply_composed_create_table(
        &mut self,
        transaction: &mut Transaction,
        spec: CreateTableSpec,
    ) -> Result<TableId, DatabaseError> {
        let transaction_id = transaction.id();
        let plan = match &mut transaction.schema_composition {
            SchemaCompositionState::Composing(plan) => plan,
            _ => return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into()),
        };
        if plan.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(SchemaMutationError::CompositionLimitExceeded("schema actions").into());
        }
        if plan.touched.len() + plan.created.len() >= MAX_TOUCHED_TABLES {
            return Err(SchemaMutationError::CompositionLimitExceeded("touched tables").into());
        }
        if spec.columns.len() > 4096 {
            return Err(SchemaCatalogError::CapacityExceeded("columns").into());
        }
        let table_id = plan
            .journal
            .borrow()
            .effective_table(plan.overlay.next_table_id)
            .ok_or(SchemaMutationError::IdentityExhausted("TableId"))?;
        let next_table_id = table_id.0.checked_add(1).map(TableId);
        if next_table_id.is_none() {
            return Err(SchemaMutationError::IdentityExhausted("TableId").into());
        }
        let mut columns = Vec::with_capacity(spec.columns.len());
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
        let next_column_id = u32::try_from(columns.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .map(ColumnId)
            .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?;
        let table = TableDef::new(table_id, spec.name, columns);
        let mut candidate = plan.overlay.schema.clone();
        candidate.add_table(table.clone())?;
        let reservation = CompositionTableReservation {
            transaction: transaction_id,
            table: table_id,
            next_table_id,
        };
        if let Err(error) = plan
            .journal
            .borrow_mut()
            .reserve_composition_table(reservation)
        {
            return if plan.journal.borrow().ensure_ready().is_err() {
                Err(SchemaMutationError::RecoveryRequired.into())
            } else {
                Err(error.into())
            };
        }
        crash("composition-table-reservation-durable");
        plan.overlay.schema = candidate;
        plan.overlay.next_table_id = next_table_id;
        plan.overlay.tables.push(TableLineage {
            table_id,
            version: TableSchemaVersion(1),
            next_column_id: Some(next_column_id),
        });
        plan.created.insert(
            table_id,
            TransactionCreatedTable {
                indexes: HeapRewriteIndexes {
                    next_index_id: IndexId(1),
                    active: Vec::new(),
                },
                present: true,
            },
        );
        plan.table_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"create-table");
        evidence.update(table_id.0.to_le_bytes());
        evidence.update(table.fingerprint()?.as_bytes());
        plan.action_evidence.push(evidence.finalize().into());
        Ok(table_id)
    }

    pub(crate) fn compose_drop_table_in(
        &mut self,
        transaction: &mut Transaction,
        target: DropTableTarget,
    ) -> Result<(), DatabaseError> {
        if !transaction.schema_composition.is_started() {
            self.preflight_composed_drop_table(&target)?;
        }
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_drop_table(transaction, target);
        self.handle_composition_accept_result(transaction, &result);
        result
    }

    /// Rejects committed tables that table-object composition cannot own before
    /// starting the durable mutation journal. A table created inside an active
    /// composition deliberately bypasses this check because it has no committed
    /// placement yet.
    fn preflight_composed_drop_table(&self, target: &DropTableTarget) -> Result<(), DatabaseError> {
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

        let catalog = self
            .catalog_path
            .as_ref()
            .ok_or(SchemaCatalogError::LegacyCatalogRequired)?;
        let base = file::load(catalog)?;
        if base.committed != self.committed {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let catalog_table = base
            .placements
            .tables
            .iter()
            .find(|entry| entry.table_id == target.table_id)
            .ok_or(SchemaMutationError::Corrupt(
                "base composition placement absent",
            ))?;
        let old_storage = match catalog_table.placement {
            TablePlacement::Single { storage_id, .. } => storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(SchemaMutationError::UnsupportedPlacement.into());
            }
        };
        let descriptor = base
            .storages
            .iter()
            .find(|storage| storage.id == old_storage)
            .ok_or(SchemaMutationError::Corrupt(
                "base composition storage absent",
            ))?;
        if catalog_table.schema_fingerprint != target.fingerprint
            || !matches!(descriptor.kind, CatalogStorageKind::Heap)
            || descriptor.locator != final_locator(catalog, base.incarnation, old_storage)?
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        Ok(())
    }

    fn apply_composed_drop_table(
        &mut self,
        transaction: &mut Transaction,
        target: DropTableTarget,
    ) -> Result<(), DatabaseError> {
        let plan = match &mut transaction.schema_composition {
            SchemaCompositionState::Composing(plan) => plan,
            _ => return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into()),
        };
        if plan.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(SchemaMutationError::CompositionLimitExceeded("schema actions").into());
        }
        if plan.dependency(target.table_id)?
            != (SchemaDependency {
                table_id: target.table_id,
                table_version: target.table_version,
                fingerprint: target.fingerprint,
            })
        {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let table = plan
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == target.table_id)
            .cloned()
            .ok_or(SchemaMutationError::TableNotFound(target.table_id))?;
        if let Some(created) = plan.created.get_mut(&target.table_id) {
            if !created.present {
                return Err(SchemaMutationError::TableNotFound(target.table_id).into());
            }
            created.present = false;
        } else if !plan.touched.contains_key(&target.table_id) {
            if plan.touched.len() + plan.created.len() >= MAX_TOUCHED_TABLES {
                return Err(SchemaMutationError::CompositionLimitExceeded("touched tables").into());
            }
            let touched = self.capture_composed_table(&plan.base, &table, target.fingerprint)?;
            plan.touched.insert(target.table_id, touched);
        }
        plan.overlay.schema = Schema::new(
            plan.overlay
                .schema
                .tables()
                .iter()
                .filter(|candidate| candidate.id != target.table_id)
                .cloned()
                .collect(),
        )?;
        plan.overlay
            .tables
            .retain(|lineage| lineage.table_id != target.table_id);
        plan.table_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"drop-table");
        evidence.update(target.table_id.0.to_le_bytes());
        evidence.update(target.table_version.0.to_le_bytes());
        evidence.update(target.fingerprint.as_bytes());
        plan.action_evidence.push(evidence.finalize().into());
        Ok(())
    }

    fn apply_composed_alter(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        self.apply_composed_alter_with_context(transaction, spec, AlterValidationContext::Ordinary)
    }

    fn apply_composed_alter_with_context(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
        context: AlterValidationContext,
    ) -> Result<(), DatabaseError> {
        let transaction_id = transaction.id();
        let plan = match &mut transaction.schema_composition {
            SchemaCompositionState::Composing(plan) => plan,
            SchemaCompositionState::AdoptedSourceRefining(adopted) => &mut adopted.logical,
            _ => return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into()),
        };
        if plan.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(SchemaMutationError::CompositionLimitExceeded("schema actions").into());
        }
        let current_table = plan
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == spec.target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(spec.target.table_id))?
            .clone();
        if plan.dependency(spec.target.table_id)? != spec.target {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        if plan
            .created
            .get(&spec.target.table_id)
            .is_some_and(|created| created.present)
        {
            let current_lineage = plan
                .overlay
                .tables
                .iter()
                .find(|lineage| lineage.table_id == spec.target.table_id)
                .cloned()
                .ok_or(SchemaMutationError::Corrupt("created table lineage absent"))?;
            let created = plan
                .created
                .get(&spec.target.table_id)
                .ok_or(SchemaMutationError::Corrupt("created table origin absent"))?;
            if let AlterTableOperation::DropColumn { column_id } = &spec.operation {
                if current_table
                    .column_by_id(*column_id)
                    .is_some_and(|column| column.primary_key)
                {
                    return Err(SchemaMutationError::PrimaryKeyColumn(*column_id).into());
                }
                if created
                    .indexes
                    .active
                    .iter()
                    .any(|index| index.column_id == *column_id)
                {
                    return Err(SchemaMutationError::IndexedColumn(*column_id).into());
                }
            }
            let reserved_column = matches!(
                &spec.operation,
                AlterTableOperation::AddNullableColumn { .. }
            )
            .then_some(
                current_lineage
                    .next_column_id
                    .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?,
            );
            let candidate = build_alter_target(&current_table, &spec.operation, reserved_column)?;
            let candidate_schema = Schema::new(
                plan.overlay
                    .schema
                    .tables()
                    .iter()
                    .map(|table| {
                        if table.id == candidate.id {
                            candidate.clone()
                        } else {
                            table.clone()
                        }
                    })
                    .collect(),
            )?;
            plan.overlay.schema = candidate_schema;
            let lineage = plan
                .overlay
                .tables
                .iter_mut()
                .find(|lineage| lineage.table_id == spec.target.table_id)
                .ok_or(SchemaMutationError::Corrupt(
                    "created table lineage disappeared",
                ))?;
            lineage.version = TableSchemaVersion(1);
            if let Some(column) = reserved_column {
                lineage.next_column_id = column.0.checked_add(1).map(ColumnId);
                if lineage.next_column_id.is_none() {
                    return Err(SchemaMutationError::IdentityExhausted("ColumnId").into());
                }
            }
            let mut evidence = Sha256::new();
            evidence.update(spec.target.table_id.0.to_le_bytes());
            evidence.update(TableSchemaVersion(1).0.to_le_bytes());
            evidence.update(candidate.fingerprint()?.as_bytes());
            evidence.update(reserved_column.map_or(0, |column| column.0).to_le_bytes());
            plan.action_evidence.push(evidence.finalize().into());
            return Ok(());
        }
        let first_touch = !plan.touched.contains_key(&spec.target.table_id);
        if first_touch && plan.touched.len() >= MAX_TOUCHED_TABLES {
            return Err(SchemaMutationError::CompositionLimitExceeded("touched tables").into());
        }

        let touched = if let Some(touched) = plan.touched.get(&spec.target.table_id) {
            touched.clone()
        } else {
            self.capture_composed_table(&plan.base, &current_table, spec.target.fingerprint)?
        };
        let mut touched = touched;
        touched.indexes.next_index_id = plan
            .journal
            .borrow()
            .effective_index(spec.target.table_id, touched.indexes.next_index_id)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        if let AlterTableOperation::DropColumn { column_id } = &spec.operation {
            if current_table
                .column_by_id(*column_id)
                .is_some_and(|column| column.primary_key)
            {
                return Err(SchemaMutationError::PrimaryKeyColumn(*column_id).into());
            }
            if touched
                .indexes
                .active
                .iter()
                .any(|index| index.column_id == *column_id)
            {
                return Err(SchemaMutationError::IndexedColumn(*column_id).into());
            }
        }

        let current_lineage = plan
            .overlay
            .tables
            .iter()
            .find(|lineage| lineage.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt("composition lineage absent"))?
            .clone();
        let reserved_column = if matches!(
            &spec.operation,
            AlterTableOperation::AddNullableColumn { .. }
        ) {
            if plan.reservation_count >= MAX_COLUMN_RESERVATIONS {
                return Err(
                    SchemaMutationError::CompositionLimitExceeded("ColumnId reservations").into(),
                );
            }
            Some(
                current_lineage
                    .next_column_id
                    .ok_or(SchemaMutationError::IdentityExhausted("ColumnId"))?,
            )
        } else {
            None
        };
        let candidate = build_alter_target(&current_table, &spec.operation, reserved_column)?;
        let candidate_schema = Schema::new(
            plan.overlay
                .schema
                .tables()
                .iter()
                .map(|table| {
                    if table.id == candidate.id {
                        candidate.clone()
                    } else {
                        table.clone()
                    }
                })
                .collect(),
        )?;
        for index in &touched.indexes.active {
            let target = candidate
                .column_by_id(index.column_id)
                .ok_or(SchemaMutationError::IndexedColumn(index.column_id))?;
            if let Some(old) = touched.base_table.column_by_id(index.column_id) {
                if old.semantic_type().physical != target.semantic_type().physical {
                    return Err(SchemaMutationError::UnsupportedSchemaEvolution.into());
                }
            }
        }
        let indexed_nullability = match &spec.operation {
            AlterTableOperation::SetNotNull { column_id }
            | AlterTableOperation::DropNotNull { column_id } => Some(*column_id),
            _ => None,
        };
        if matches!(context, AlterValidationContext::Ordinary)
            && indexed_nullability.is_some_and(|column_id| {
                touched
                    .indexes
                    .active
                    .iter()
                    .any(|index| index.column_id == column_id)
            })
        {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::IndexedNullability(
                    indexed_nullability.ok_or(SchemaMutationError::Corrupt(
                        "indexed nullability column absent",
                    ))?,
                ),
            )
            .into());
        }
        if let AlterTableOperation::SetNotNull { column_id } = &spec.operation {
            match context {
                AlterValidationContext::Ordinary => {
                    self.validate_composed_not_null(&touched, *column_id)?;
                }
                AlterValidationContext::AdoptedSource {
                    source_not_null_validated: true,
                } => {}
                AlterValidationContext::AdoptedSource {
                    source_not_null_validated: false,
                } => {
                    return Err(SchemaMutationError::Corrupt(
                        "adopted SET NOT NULL source was not validated",
                    )
                    .into());
                }
            }
        }

        if let Some(column) = reserved_column {
            let next_column_id = column.0.checked_add(1).map(ColumnId);
            if next_column_id.is_none() {
                return Err(SchemaMutationError::IdentityExhausted("ColumnId").into());
            }
            let reservation = CompositionColumnReservation {
                transaction: transaction_id,
                table: spec.target.table_id,
                column,
                next_column_id,
            };
            if let Err(error) = plan
                .journal
                .borrow_mut()
                .reserve_composition_column(reservation)
            {
                return if plan.journal.borrow().ensure_ready().is_err() {
                    Err(SchemaMutationError::RecoveryRequired.into())
                } else {
                    Err(error.into())
                };
            }
            crash("composition-column-reservation-durable");
        }

        let target_fingerprint = candidate.fingerprint()?;
        let target_version = touched
            .base_lineage
            .version
            .0
            .checked_add(1)
            .map(TableSchemaVersion)
            .ok_or(SchemaMutationError::IdentityExhausted("TableSchemaVersion"))?;
        plan.overlay.schema = candidate_schema;
        let lineage = plan
            .overlay
            .tables
            .iter_mut()
            .find(|lineage| lineage.table_id == spec.target.table_id)
            .ok_or(SchemaMutationError::Corrupt(
                "composition lineage disappeared",
            ))?;
        lineage.version = target_version;
        if let Some(column) = reserved_column {
            lineage.next_column_id = column.0.checked_add(1).map(ColumnId);
            plan.reservation_count += 1;
        }
        plan.touched.entry(spec.target.table_id).or_insert(touched);
        let mut evidence = Sha256::new();
        evidence.update(spec.target.table_id.0.to_le_bytes());
        evidence.update(target_version.0.to_le_bytes());
        evidence.update(target_fingerprint.as_bytes());
        evidence.update(reserved_column.map_or(0, |column| column.0).to_le_bytes());
        plan.action_evidence.push(evidence.finalize().into());
        Ok(())
    }

    pub(crate) fn compose_create_index_in(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        if matches!(
            transaction.schema_composition,
            SchemaCompositionState::SourceRefining(_)
                | SchemaCompositionState::SourceIndexFinalizing(_)
        ) {
            return self.apply_source_backfill_create_index(transaction, statement);
        }
        if matches!(
            transaction.schema_composition,
            SchemaCompositionState::BackfillOpen(_)
                | SchemaCompositionState::RefiningAfterEvacuation(_)
                | SchemaCompositionState::Refining(_)
                | SchemaCompositionState::IndexFinalizing(_)
        ) {
            let result = self.apply_backfill_create_index(transaction, statement);
            self.handle_composition_accept_result(transaction, &result);
            return result;
        }
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_create_index(transaction, statement);
        self.handle_composition_accept_result(transaction, &result);
        result
    }

    fn apply_backfill_create_index(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        if let Some(binding) = self
            .index_name_bindings(Some(transaction))
            .into_iter()
            .find(|binding| binding.name == statement.name)
        {
            if statement.if_not_exists
                && binding.target.table_id == statement.target.table_id
                && self
                    .logical_index(transaction, binding.target)
                    .is_some_and(|index| index.column_id == statement.column_id)
            {
                return Ok(crate::DdlOutcome::Unchanged);
            }
            return Err(DatabaseError::DuplicateIndexName(statement.name.clone()));
        }
        let (table_id, dependency, current_table, mut touched, journal) = {
            let plan = transaction
                .schema_composition
                .plan()
                .ok_or(SchemaMutationError::Corrupt("backfill composition absent"))?;
            if plan.action_count() >= MAX_SCHEMA_ACTIONS {
                return Err(
                    SchemaMutationError::CompositionLimitExceeded("schema/index actions").into(),
                );
            }
            let dependency = plan.dependency(statement.target.table_id)?;
            if dependency.table_version != statement.target.table_version
                || dependency.fingerprint != statement.target.fingerprint
            {
                return Err(SchemaMutationError::StaleSchemaDependency.into());
            }
            let current_table = plan
                .overlay
                .schema
                .tables()
                .iter()
                .find(|table| table.id == statement.target.table_id)
                .ok_or(SchemaMutationError::TableNotFound(
                    statement.target.table_id,
                ))?
                .clone();
            let touched = plan
                .touched
                .get(&statement.target.table_id)
                .ok_or(SchemaMutationError::Corrupt(
                    "backfill table capture absent",
                ))?
                .clone();
            (
                statement.target.table_id,
                dependency,
                current_table,
                touched,
                Rc::clone(&plan.journal),
            )
        };
        if current_table.column_by_id(statement.column_id).is_none() {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        touched.indexes.next_index_id = journal
            .borrow()
            .effective_index(table_id, touched.indexes.next_index_id)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        if touched
            .indexes
            .active
            .iter()
            .any(|index| index.column_id == statement.column_id)
        {
            return Err(netbadb_storage::StorageError::from(
                netbadb_index::IndexError::IndexAlreadyExists {
                    column_id: statement.column_id,
                },
            )
            .into());
        }
        if transaction
            .schema_composition
            .plan()
            .is_some_and(|plan| plan.index_reservation_count >= MAX_INDEX_RESERVATIONS)
        {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("IndexId reservations").into(),
            );
        }
        let index = touched.indexes.next_index_id;
        let next_index_id = index
            .0
            .checked_add(1)
            .map(IndexId)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        journal
            .borrow_mut()
            .reserve_migration_index(CompositionIndexReservation {
                transaction: transaction.id(),
                table: table_id,
                table_version: dependency.table_version,
                fingerprint: dependency.fingerprint,
                index,
                next_index_id: Some(next_index_id),
            })?;
        crash("migration-index-reservation-durable");
        touched.indexes.next_index_id = next_index_id;
        touched.indexes.active.push(HeapRewriteIndex {
            id: index,
            name: Some(statement.name.clone()),
            column_id: statement.column_id,
        });
        touched.indexes.active.sort_by_key(|entry| entry.id);
        let materialized = transaction.schema_composition.materialized_mut().ok_or(
            SchemaMutationError::Corrupt("backfill materialization absent"),
        )?;
        materialized
            .backfill_indexed_columns
            .insert(statement.column_id);
        materialized.logical.touched.insert(table_id, touched);
        materialized.logical.index_reservation_count += 1;
        materialized.logical.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"migration-create-index");
        evidence.update(table_id.0.to_le_bytes());
        evidence.update(index.0.to_le_bytes());
        evidence.update(statement.column_id.0.to_le_bytes());
        evidence.update(statement.name.as_str().as_bytes());
        materialized
            .logical
            .action_evidence
            .push(evidence.finalize().into());
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        transaction.schema_composition = match previous {
            SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::RefiningAfterEvacuation(materialized)
            | SchemaCompositionState::Refining(materialized)
            | SchemaCompositionState::IndexFinalizing(materialized) => {
                SchemaCompositionState::IndexFinalizing(materialized)
            }
            other => other,
        };
        Ok(crate::DdlOutcome::Created)
    }

    fn apply_source_backfill_create_index(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        if self
            .index_name_bindings(Some(transaction))
            .into_iter()
            .any(|binding| binding.name == statement.name)
        {
            return Err(DatabaseError::DuplicateIndexName(statement.name.clone()));
        }
        let (dependency, mut touched, journal) = {
            let plan =
                transaction
                    .schema_composition
                    .plan()
                    .ok_or(SchemaMutationError::Corrupt(
                        "source-backfill composition absent",
                    ))?;
            let dependency = plan.dependency(statement.target.table_id)?;
            if dependency.table_version != statement.target.table_version
                || dependency.fingerprint != statement.target.fingerprint
            {
                return Err(SchemaMutationError::StaleSchemaDependency.into());
            }
            let touched = plan
                .touched
                .get(&statement.target.table_id)
                .cloned()
                .ok_or(SchemaMutationError::UnsupportedBackfillRefinement(
                    crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
                ))?;
            (dependency, touched, Rc::clone(&plan.journal))
        };
        if transaction
            .schema_composition
            .plan()
            .is_some_and(|plan| plan.index_reservation_count >= MAX_INDEX_RESERVATIONS)
        {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("IndexId reservations").into(),
            );
        }
        if touched
            .base_table
            .column_by_id(statement.column_id)
            .is_none()
        {
            return Err(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::UnsupportedOperation,
            )
            .into());
        }
        touched.indexes.next_index_id = journal
            .borrow()
            .effective_index(statement.target.table_id, touched.indexes.next_index_id)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        if touched
            .indexes
            .active
            .iter()
            .any(|index| index.column_id == statement.column_id)
        {
            return Err(netbadb_storage::StorageError::from(
                netbadb_index::IndexError::IndexAlreadyExists {
                    column_id: statement.column_id,
                },
            )
            .into());
        }
        let index = touched.indexes.next_index_id;
        let next_index_id = index
            .0
            .checked_add(1)
            .map(IndexId)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        journal
            .borrow_mut()
            .reserve_migration_index(CompositionIndexReservation {
                transaction: transaction.id(),
                table: statement.target.table_id,
                table_version: dependency.table_version,
                fingerprint: dependency.fingerprint,
                index,
                next_index_id: Some(next_index_id),
            })?;
        touched.indexes.next_index_id = next_index_id;
        touched.indexes.active.push(HeapRewriteIndex {
            id: index,
            name: Some(statement.name.clone()),
            column_id: statement.column_id,
        });
        touched.indexes.active.sort_by_key(|entry| entry.id);
        let materialized = transaction
            .schema_composition
            .materialized_index_mut()
            .ok_or(SchemaMutationError::Corrupt(
                "source-backfill materialization absent",
            ))?;
        materialized
            .logical
            .touched
            .insert(statement.target.table_id, touched);
        materialized.logical.index_reservation_count += 1;
        materialized.logical.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"source-backfill-create-index");
        evidence.update(statement.target.table_id.0.to_le_bytes());
        evidence.update(index.0.to_le_bytes());
        evidence.update(statement.column_id.0.to_le_bytes());
        evidence.update(statement.name.as_str().as_bytes());
        materialized
            .logical
            .action_evidence
            .push(evidence.finalize().into());
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        transaction.schema_composition = match previous {
            SchemaCompositionState::SourceRefining(materialized)
            | SchemaCompositionState::SourceIndexFinalizing(materialized) => {
                SchemaCompositionState::SourceIndexFinalizing(materialized)
            }
            other => other,
        };
        Ok(crate::DdlOutcome::Created)
    }

    fn apply_composed_create_index(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        self.apply_composed_create_index_with_options(transaction, statement, false)
    }

    #[cfg(test)]
    pub(crate) fn audit_apply_adopted_source_create_index(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        self.validate_transaction(transaction)?;
        self.apply_composed_create_index_with_options(transaction, statement, true)
    }

    fn apply_composed_create_index_with_options(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
        allow_adopted_source: bool,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        if let Some(binding) = self
            .index_name_bindings(Some(transaction))
            .into_iter()
            .find(|binding| binding.name == statement.name)
        {
            let matches = binding.target.table_id == statement.target.table_id
                && self
                    .logical_index(transaction, binding.target)
                    .is_some_and(|index| index.column_id == statement.column_id);
            if statement.if_not_exists && matches {
                return Ok(crate::DdlOutcome::Unchanged);
            }
            return Err(DatabaseError::DuplicateIndexName(statement.name.clone()));
        }
        let transaction_id = transaction.id();
        let plan = match &mut transaction.schema_composition {
            SchemaCompositionState::Composing(plan) => plan,
            SchemaCompositionState::AdoptedSourceRefining(adopted) if allow_adopted_source => {
                &mut adopted.logical
            }
            _ => return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into()),
        };
        if plan.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("schema/index actions").into(),
            );
        }
        let dependency = plan.dependency(statement.target.table_id)?;
        if dependency.table_version != statement.target.table_version
            || dependency.fingerprint != statement.target.fingerprint
        {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        let current_table = plan
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == statement.target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(
                statement.target.table_id,
            ))?
            .clone();
        if current_table.column_by_id(statement.column_id).is_none() {
            return Err(SchemaMutationError::StaleSchemaDependency.into());
        }
        if let Some(created) = plan
            .created
            .get_mut(&statement.target.table_id)
            .filter(|created| created.present)
        {
            if created
                .indexes
                .active
                .iter()
                .any(|index| index.column_id == statement.column_id)
            {
                return Err(netbadb_storage::StorageError::from(
                    netbadb_index::IndexError::IndexAlreadyExists {
                        column_id: statement.column_id,
                    },
                )
                .into());
            }
            let index = created.indexes.next_index_id;
            created.indexes.next_index_id = index
                .0
                .checked_add(1)
                .map(IndexId)
                .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
            created.indexes.active.push(HeapRewriteIndex {
                id: index,
                name: Some(statement.name.clone()),
                column_id: statement.column_id,
            });
            plan.index_actions += 1;
            let mut evidence = Sha256::new();
            evidence.update(b"create-index");
            evidence.update(statement.target.table_id.0.to_le_bytes());
            evidence.update(index.0.to_le_bytes());
            evidence.update(statement.column_id.0.to_le_bytes());
            evidence.update(statement.name.as_str().as_bytes());
            plan.action_evidence.push(evidence.finalize().into());
            return Ok(crate::DdlOutcome::Created);
        }
        let first_touch = !plan.touched.contains_key(&statement.target.table_id);
        if first_touch && plan.touched.len() >= MAX_TOUCHED_TABLES {
            return Err(SchemaMutationError::CompositionLimitExceeded("touched tables").into());
        }
        let mut touched = if let Some(touched) = plan.touched.get(&statement.target.table_id) {
            touched.clone()
        } else {
            self.capture_composed_table(&plan.base, &current_table, statement.target.fingerprint)?
        };
        touched.indexes.next_index_id = plan
            .journal
            .borrow()
            .effective_index(statement.target.table_id, touched.indexes.next_index_id)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        if touched
            .indexes
            .active
            .iter()
            .any(|index| index.column_id == statement.column_id)
        {
            return Err(netbadb_storage::StorageError::from(
                netbadb_index::IndexError::IndexAlreadyExists {
                    column_id: statement.column_id,
                },
            )
            .into());
        }
        if plan.index_reservation_count >= MAX_INDEX_RESERVATIONS {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("IndexId reservations").into(),
            );
        }
        let index = touched.indexes.next_index_id;
        let next_index_id = index.0.checked_add(1).map(IndexId);
        if next_index_id.is_none() {
            return Err(SchemaMutationError::IdentityExhausted("IndexId").into());
        }
        let reservation = CompositionIndexReservation {
            transaction: transaction_id,
            table: statement.target.table_id,
            table_version: statement.target.table_version,
            fingerprint: statement.target.fingerprint,
            index,
            next_index_id,
        };
        let reservation_result = plan
            .journal
            .borrow_mut()
            .reserve_composition_index(reservation);
        if let Err(error) = reservation_result {
            return if plan.journal.borrow().ensure_ready().is_err() {
                Err(SchemaMutationError::RecoveryRequired.into())
            } else {
                Err(error.into())
            };
        }
        crash("composition-index-reservation-durable");
        touched.indexes.next_index_id =
            next_index_id.ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        touched.indexes.active.push(HeapRewriteIndex {
            id: index,
            name: Some(statement.name.clone()),
            column_id: statement.column_id,
        });
        touched.indexes.active.sort_by_key(|index| index.id);
        plan.touched.insert(statement.target.table_id, touched);
        plan.index_reservation_count += 1;
        plan.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"create-index");
        evidence.update(statement.target.table_id.0.to_le_bytes());
        evidence.update(index.0.to_le_bytes());
        evidence.update(statement.column_id.0.to_le_bytes());
        evidence.update(statement.name.as_str().as_bytes());
        plan.action_evidence.push(evidence.finalize().into());
        Ok(crate::DdlOutcome::Created)
    }

    pub(crate) fn compose_drop_index_in(
        &mut self,
        transaction: &mut Transaction,
        target: crate::DropIndexTarget,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        if matches!(
            transaction.schema_composition,
            SchemaCompositionState::SourceRefining(_)
                | SchemaCompositionState::SourceIndexFinalizing(_)
        ) {
            return self.apply_source_backfill_drop_index(transaction, target);
        }
        if matches!(
            transaction.schema_composition,
            SchemaCompositionState::BackfillOpen(_)
                | SchemaCompositionState::Refining(_)
                | SchemaCompositionState::IndexFinalizing(_)
        ) {
            let result = self.apply_backfill_drop_index(transaction, target);
            self.handle_composition_accept_result(transaction, &result);
            return result;
        }
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_drop_index(transaction, target);
        self.handle_composition_accept_result(transaction, &result);
        result
    }

    fn apply_backfill_drop_index(
        &mut self,
        transaction: &mut Transaction,
        target: crate::DropIndexTarget,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        let materialized = transaction.schema_composition.materialized_mut().ok_or(
            SchemaMutationError::Corrupt("backfill materialization absent"),
        )?;
        let plan = &mut materialized.logical;
        if plan.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("schema/index actions").into(),
            );
        }
        let table = plan
            .touched
            .get_mut(&target.table_id)
            .ok_or(DatabaseError::UndefinedIndex)?;
        let position = table
            .indexes
            .active
            .iter()
            .position(|index| index.id == target.index_id)
            .ok_or(DatabaseError::UndefinedIndex)?;
        table.indexes.active.remove(position);
        plan.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"migration-drop-index");
        evidence.update(target.table_id.0.to_le_bytes());
        evidence.update(target.index_id.0.to_le_bytes());
        plan.action_evidence.push(evidence.finalize().into());
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        transaction.schema_composition = match previous {
            SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::Refining(materialized)
            | SchemaCompositionState::IndexFinalizing(materialized) => {
                SchemaCompositionState::IndexFinalizing(materialized)
            }
            other => other,
        };
        Ok(crate::DdlOutcome::Dropped)
    }

    fn apply_source_backfill_drop_index(
        &mut self,
        transaction: &mut Transaction,
        target: crate::DropIndexTarget,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        let materialized = transaction
            .schema_composition
            .materialized_index_mut()
            .ok_or(SchemaMutationError::Corrupt(
                "source-backfill materialization absent",
            ))?;
        let table = materialized
            .logical
            .touched
            .get_mut(&target.table_id)
            .ok_or(SchemaMutationError::UnsupportedBackfillRefinement(
                crate::schema_mutation::BackfillRefinementReason::CrossTableAccess,
            ))?;
        let position = table
            .indexes
            .active
            .iter()
            .position(|index| index.id == target.index_id)
            .ok_or(DatabaseError::UndefinedIndex)?;
        table.indexes.active.remove(position);
        materialized.logical.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"source-backfill-drop-index");
        evidence.update(target.table_id.0.to_le_bytes());
        evidence.update(target.index_id.0.to_le_bytes());
        materialized
            .logical
            .action_evidence
            .push(evidence.finalize().into());
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        transaction.schema_composition = match previous {
            SchemaCompositionState::SourceRefining(materialized)
            | SchemaCompositionState::SourceIndexFinalizing(materialized) => {
                SchemaCompositionState::SourceIndexFinalizing(materialized)
            }
            other => other,
        };
        Ok(crate::DdlOutcome::Dropped)
    }

    fn apply_composed_drop_index(
        &mut self,
        transaction: &mut Transaction,
        target: crate::DropIndexTarget,
    ) -> Result<crate::DdlOutcome, DatabaseError> {
        let plan = match &mut transaction.schema_composition {
            SchemaCompositionState::Composing(plan) => plan,
            _ => return Err(SchemaMutationError::SchemaMutationAfterMaterialization.into()),
        };
        if plan.action_count() >= MAX_SCHEMA_ACTIONS {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("schema/index actions").into(),
            );
        }
        let current_table = plan
            .overlay
            .schema
            .tables()
            .iter()
            .find(|table| table.id == target.table_id)
            .ok_or(SchemaMutationError::TableNotFound(target.table_id))?
            .clone();
        let dependency = plan.dependency(target.table_id)?;
        if let Some(created) = plan
            .created
            .get_mut(&target.table_id)
            .filter(|created| created.present)
        {
            let position = created
                .indexes
                .active
                .iter()
                .position(|index| index.id == target.index_id)
                .ok_or(DatabaseError::UndefinedIndex)?;
            created.indexes.active.remove(position);
            plan.index_actions += 1;
            let mut evidence = Sha256::new();
            evidence.update(b"drop-index");
            evidence.update(target.table_id.0.to_le_bytes());
            evidence.update(target.index_id.0.to_le_bytes());
            plan.action_evidence.push(evidence.finalize().into());
            return Ok(crate::DdlOutcome::Dropped);
        }
        let mut touched = if let Some(touched) = plan.touched.get(&target.table_id) {
            touched.clone()
        } else {
            self.capture_composed_table(&plan.base, &current_table, dependency.fingerprint)?
        };
        touched.indexes.next_index_id = plan
            .journal
            .borrow()
            .effective_index(target.table_id, touched.indexes.next_index_id)
            .ok_or(SchemaMutationError::IdentityExhausted("IndexId"))?;
        let position = touched
            .indexes
            .active
            .iter()
            .position(|index| index.id == target.index_id)
            .ok_or(DatabaseError::UndefinedIndex)?;
        touched.indexes.active.remove(position);
        plan.touched.insert(target.table_id, touched);
        plan.index_actions += 1;
        let mut evidence = Sha256::new();
        evidence.update(b"drop-index");
        evidence.update(target.table_id.0.to_le_bytes());
        evidence.update(target.index_id.0.to_le_bytes());
        plan.action_evidence.push(evidence.finalize().into());
        Ok(crate::DdlOutcome::Dropped)
    }

    fn logical_index(
        &self,
        transaction: &Transaction,
        target: crate::DropIndexTarget,
    ) -> Option<HeapRewriteIndex> {
        if let Some(plan) = transaction.schema_composition.plan() {
            if let Some(created) = plan.created.get(&target.table_id) {
                return created.present.then(|| {
                    created
                        .indexes
                        .active
                        .iter()
                        .find(|index| index.id == target.index_id)
                        .cloned()
                })?;
            }
            if let Some(table) = plan.touched.get(&target.table_id) {
                return plan
                    .overlay
                    .schema
                    .tables()
                    .iter()
                    .any(|candidate| candidate.id == target.table_id)
                    .then(|| {
                        table
                            .indexes
                            .active
                            .iter()
                            .find(|index| index.id == target.index_id)
                            .cloned()
                    })?;
            }
        }
        self.indexes(target.table_id)
            .ok()?
            .iter()
            .find(|index| index.id == target.index_id)
            .map(|index| HeapRewriteIndex {
                id: index.id,
                name: index.name.clone(),
                column_id: index.column_id,
            })
    }

    fn capture_composed_table(
        &mut self,
        base: &SchemaCatalogSnapshot,
        table: &TableDef,
        fingerprint: netbadb_schema::SchemaFingerprint,
    ) -> Result<ComposedTable, DatabaseError> {
        let lineage = base
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == table.id)
            .ok_or(SchemaMutationError::Corrupt(
                "base composition lineage absent",
            ))?
            .clone();
        let catalog_table = base
            .placements
            .tables
            .iter()
            .find(|entry| entry.table_id == table.id)
            .ok_or(SchemaMutationError::Corrupt(
                "base composition placement absent",
            ))?
            .clone();
        let old_storage = match catalog_table.placement {
            TablePlacement::Single { storage_id, .. } => storage_id,
            TablePlacement::RangePartitioned { .. } => {
                return Err(SchemaMutationError::UnsupportedPlacement.into());
            }
        };
        let descriptor = base
            .storages
            .iter()
            .find(|storage| storage.id == old_storage)
            .ok_or(SchemaMutationError::Corrupt(
                "base composition storage absent",
            ))?
            .clone();
        if !matches!(descriptor.kind, CatalogStorageKind::Heap)
            || catalog_table.schema_fingerprint != fingerprint
            || descriptor.locator
                != final_locator(
                    &self
                        .catalog_path
                        .clone()
                        .ok_or(SchemaCatalogError::LegacyCatalogRequired)?,
                    base.incarnation,
                    old_storage,
                )?
        {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let storage = self
            .registry
            .get_mut(old_storage)
            .ok_or(SchemaMutationError::Corrupt("base composition Heap absent"))?;
        if storage.kind() != netbadb_storage::StorageKind::Heap || storage.table() != table {
            return Err(SchemaMutationError::UnsupportedPlacement.into());
        }
        let indexes = storage.heap_rewrite_indexes()?;
        Ok(ComposedTable {
            base_table: table.clone(),
            base_lineage: lineage,
            old_storage,
            catalog_table,
            descriptor,
            base_indexes: indexes.clone(),
            indexes,
        })
    }

    fn validate_composed_not_null(
        &mut self,
        touched: &ComposedTable,
        column: ColumnId,
    ) -> Result<(), DatabaseError> {
        let columns = touched
            .base_table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let position = touched
            .base_table
            .columns
            .iter()
            .position(|candidate| candidate.id == column);
        let view = self
            .registry
            .get(touched.old_storage)
            .ok_or(SchemaMutationError::Corrupt("validation source absent"))?
            .read_view()?;
        let flow = self
            .registry
            .get_mut(touched.old_storage)
            .ok_or(SchemaMutationError::Corrupt("validation source absent"))?
            .visit_rows_with_view_control::<DatabaseError, _>(&columns, &view, |_row, values| {
                if position.is_none_or(|position| matches!(values[position], ScalarValue::Null)) {
                    return Ok(ControlFlow::Break(()));
                }
                Ok(ControlFlow::Continue(()))
            })?;
        if flow.is_break() {
            return Err(SchemaMutationError::NotNullViolation(column).into());
        }
        Ok(())
    }

    pub(crate) fn ensure_schema_materialized(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        self.ensure_schema_materialized_with_backfill(transaction, false)
    }

    pub(crate) fn ensure_schema_materialized_with_backfill(
        &mut self,
        transaction: &mut Transaction,
        activate_backfill: bool,
    ) -> Result<(), DatabaseError> {
        if !transaction.schema_composition.is_composing() {
            return Ok(());
        }
        let state = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let SchemaCompositionState::Composing(logical) = state else {
            return Err(SchemaMutationError::Corrupt("composition state transition").into());
        };
        let rollback_logical = logical.clone();
        let materialized = if logical.table_actions > 0 {
            self.materialize_table_object_composition(transaction, *logical, activate_backfill)
        } else if logical.index_actions == 0 {
            self.materialize_schema_composition(transaction, *logical, activate_backfill)
        } else {
            self.materialize_schema_index_composition(
                transaction,
                *logical,
                SchemaIndexMaterialization::Ordinary,
            )
        };
        match materialized {
            Ok(()) => Ok(()),
            Err(error) => {
                transaction.require_schema_rollback();
                let previous = std::mem::replace(
                    &mut transaction.schema_composition,
                    SchemaCompositionState::None,
                );
                transaction.schema_composition = match previous {
                    SchemaCompositionState::SealingAndMaterializing(materialized) => {
                        SchemaCompositionState::RollbackRequiredMaterialized(materialized)
                    }
                    SchemaCompositionState::SealingAndMaterializingIndex(materialized) => {
                        SchemaCompositionState::RollbackRequiredMaterializedIndex(materialized)
                    }
                    SchemaCompositionState::None => {
                        SchemaCompositionState::RollbackRequiredLogical(rollback_logical)
                    }
                    other => other,
                };
                Err(error)
            }
        }
    }

    fn materialize_schema_composition(
        &mut self,
        transaction: &mut Transaction,
        logical: SchemaTransactionPlan,
        activate_backfill: bool,
    ) -> Result<(), DatabaseError> {
        let backfill = activate_backfill && Self::is_backfill_candidate(&logical);
        let effective = logical
            .touched
            .iter()
            .filter_map(|(table_id, touched)| {
                logical
                    .overlay
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == *table_id)
                    .filter(|table| **table != touched.base_table)
                    .map(|table| (*table_id, touched, table.clone()))
            })
            .collect::<Vec<_>>();
        if effective.is_empty() {
            transaction.schema_composition =
                SchemaCompositionState::SealedNoEffectiveChange(Box::new(logical));
            return Ok(());
        }
        if effective.len() > MAX_TOUCHED_TABLES {
            return Err(
                SchemaMutationError::CompositionLimitExceeded("schema-created StorageIds").into(),
            );
        }
        self.catalog_generation
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted(
                "runtime catalog revision",
            ))?;
        let target_generation = logical
            .base
            .committed
            .generation
            .0
            .checked_add(1)
            .map(netbadb_types::SchemaGeneration)
            .ok_or(SchemaMutationError::IdentityExhausted("SchemaGeneration"))?;
        let target_epoch = logical
            .base
            .epoch
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("catalog epoch"))?;
        let mut next_storage = self
            .next_storage_id()
            .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        let mut allocations = BTreeMap::new();
        for (table_id, _, _) in &effective {
            allocations.insert(*table_id, next_storage);
            next_storage = next_storage
                .0
                .checked_add(1)
                .map(StorageId)
                .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        }
        let coordinator_locator = logical
            .base
            .coordinator
            .clone()
            .unwrap_or_else(|| logical.journal.borrow().coordinator.clone());
        let mut target = logical.base.clone();
        target.epoch = target_epoch;
        target.committed = logical.overlay.clone();
        target.committed.generation = target_generation;
        target.committed.next_storage_id = Some(next_storage);
        target.coordinator = Some(coordinator_locator.clone());
        let effective_ids = effective
            .iter()
            .map(|(table, _, _)| *table)
            .collect::<BTreeSet<_>>();
        for lineage in &mut target.committed.tables {
            if !effective_ids.contains(&lineage.table_id) {
                let base_lineage = logical
                    .base
                    .committed
                    .tables
                    .iter()
                    .find(|base| base.table_id == lineage.table_id)
                    .ok_or(SchemaMutationError::Corrupt("base lineage disappeared"))?;
                lineage.version = base_lineage.version;
            }
        }
        let mut table_plans = Vec::with_capacity(effective.len());
        for (table_id, touched, final_table) in &effective {
            let new_storage = allocations[table_id];
            let final_fingerprint = final_table.fingerprint()?;
            let new_descriptor = CatalogStorage {
                id: new_storage,
                table_id: *table_id,
                locator: final_locator(&logical.catalog, target.incarnation, new_storage)?,
                kind: CatalogStorageKind::Heap,
            };
            let target_placement = CatalogTable {
                table_id: *table_id,
                schema_fingerprint: final_fingerprint,
                placement: TablePlacement::Single {
                    table_id: *table_id,
                    storage_id: new_storage,
                },
            };
            *target
                .placements
                .tables
                .iter_mut()
                .find(|placement| placement.table_id == *table_id)
                .ok_or(SchemaMutationError::Corrupt("target placement disappeared"))? =
                target_placement.clone();
            *target
                .storages
                .iter_mut()
                .find(|storage| storage.id == touched.old_storage)
                .ok_or(SchemaMutationError::Corrupt(
                    "target descriptor disappeared",
                ))? = new_descriptor.clone();
            let target_lineage = target
                .committed
                .tables
                .iter()
                .find(|lineage| lineage.table_id == *table_id)
                .ok_or(SchemaMutationError::Corrupt("target lineage disappeared"))?
                .clone();
            let base_fragment = one_table_fragment(
                &logical.base,
                touched.base_table.clone(),
                touched.base_lineage.clone(),
                touched.catalog_table.clone(),
                touched.descriptor.clone(),
                coordinator_locator.clone(),
            )?;
            let target_fragment = one_table_fragment(
                &target,
                final_table.clone(),
                target_lineage,
                target_placement,
                new_descriptor,
                coordinator_locator.clone(),
            )?;
            table_plans.push(CompositionTablePlan {
                base: base_fragment,
                target: target_fragment,
                retired: false,
                gc: None,
            });
        }
        target.validate()?;
        let target_bytes = target.encode()?;
        let intent = SchemaChangeSetIntent {
            transaction: transaction.id(),
            base_generation: logical.base.committed.generation,
            target_generation,
            base_epoch: logical.base.epoch,
            target_epoch,
            action_count: u32::try_from(logical.action_count())
                .map_err(|_| SchemaMutationError::CompositionLimitExceeded("schema actions"))?,
            action_digest: logical.action_digest(),
            snapshot_digest: digest(&target_bytes),
            tables: table_plans,
        };
        crash("composition-before-intent");
        if let Err(error) = logical
            .journal
            .borrow_mut()
            .composition_intent(intent.clone())
        {
            return if logical.journal.borrow().ensure_ready().is_err() {
                Err(SchemaMutationError::RecoveryRequired.into())
            } else {
                Err(error.into())
            };
        }
        crash("composition-intent-durable");
        if backfill {
            let table_plan = intent
                .tables
                .first()
                .ok_or(SchemaMutationError::Corrupt("backfill table plan absent"))?;
            let stage_path = stage_locator(
                &logical.catalog,
                logical.base.incarnation,
                intent.transaction,
                table_plan.new_storage(),
            )?;
            let final_path = final_locator(
                &logical.catalog,
                logical.base.incarnation,
                table_plan.new_storage(),
            )?;
            logical
                .journal
                .borrow_mut()
                .stage_resource_intent(StageResourceIntent {
                    transaction: intent.transaction,
                    table: table_plan.table(),
                    storage: table_plan.new_storage(),
                    base_generation: intent.base_generation,
                    base_epoch: intent.base_epoch,
                    provisional: table_plan.target.clone(),
                    stage_locator: stage_path,
                    final_locator: final_path,
                    digest: digest(&table_plan.target.encode()?),
                })?;
            crash("backfill-stage-intent-durable");
        }

        let coordinator = match &self.coordinator {
            Some(coordinator) => Rc::clone(coordinator),
            None => {
                let path = file::resolve(&logical.catalog, &coordinator_locator);
                validate_resource_path(&logical.catalog, &path)?;
                ensure_parent(&path)?;
                let log = if path
                    .try_exists()
                    .map_err(|error| file::io("inspect schema coordinator", &path, error))?
                {
                    CoordinatorLog::open(&path)?
                } else {
                    CoordinatorLog::create(&path)?
                };
                Rc::new(std::cell::RefCell::new(log))
            }
        };
        transaction.set_coordinator(coordinator);
        let reference = SchemaParticipantReference {
            incarnation: target.incarnation,
            target_epoch,
            digest: intent.snapshot_digest,
        };
        let backfill_indexed_columns = logical
            .touched
            .values()
            .flat_map(|table| {
                table
                    .base_indexes
                    .active
                    .iter()
                    .map(|index| index.column_id)
            })
            .collect();
        let materialized = MaterializedSchemaTransaction {
            logical,
            target,
            reference,
            intent: intent.clone(),
            staged: BTreeMap::new(),
            staged_indexes: BTreeMap::new(),
            backfill,
            backfill_indexed_columns,
        };
        transaction.schema_composition = if backfill {
            SchemaCompositionState::BackfillMaterializing(Box::new(materialized))
        } else {
            SchemaCompositionState::SealingAndMaterializing(Box::new(materialized))
        };

        for (position, table_plan) in intent.tables.iter().enumerate() {
            let table_id = table_plan.table();
            let old_storage = table_plan.old_storage();
            let new_storage = table_plan.new_storage();
            let final_table = table_plan.target.committed.schema.tables()[0].clone();
            let stage = file::resolve(
                &transaction
                    .schema_composition
                    .plan()
                    .ok_or(SchemaMutationError::Corrupt("composition plan absent"))?
                    .catalog,
                &stage_locator(
                    &transaction
                        .schema_composition
                        .plan()
                        .ok_or(SchemaMutationError::Corrupt("composition plan absent"))?
                        .catalog,
                    table_plan.target.incarnation,
                    transaction.id(),
                    new_storage,
                )?,
            );
            let catalog = &transaction
                .schema_composition
                .plan()
                .ok_or(SchemaMutationError::Corrupt("composition plan absent"))?
                .catalog;
            validate_resource_path(catalog, &stage)?;
            ensure_parent(&stage)?;
            write_owner(
                &file::suffix(&stage, ".owner"),
                table_plan.target.incarnation,
                transaction.id(),
                table_id,
                new_storage,
                final_table.fingerprint()?,
            )?;
            crash("composition-stage-first-file");
            let storage = TableStorage::create_heap_with_storage_id(
                &stage,
                final_table.clone(),
                new_storage,
            )?;
            crash(&format!("composition-after-target-create-{}", position + 1));
            storage.flush()?;
            file::sync_parent(&stage)?;
            transaction.enlist_composed_staged(storage)?;
            let indexes = transaction
                .schema_composition
                .plan()
                .and_then(|plan| plan.touched.get(&table_id))
                .ok_or(SchemaMutationError::Corrupt("composition indexes absent"))?
                .indexes
                .clone();
            transaction.with_composed_staged_write(new_storage, |storage, context| {
                storage.install_heap_rewrite_indexes_in(context, &indexes)
            })?;
            transaction
                .schema_composition
                .materialized_mut()
                .ok_or(SchemaMutationError::Corrupt(
                    "composition materialization disappeared",
                ))?
                .staged_indexes
                .insert(table_id, indexes);
            let old_columns = table_plan.base.committed.schema.tables()[0]
                .columns
                .iter()
                .map(|column| column.id)
                .collect::<Vec<_>>();
            let transform = final_table
                .columns
                .iter()
                .map(|target_column| {
                    table_plan.base.committed.schema.tables()[0]
                        .columns
                        .iter()
                        .position(|old_column| old_column.id == target_column.id)
                })
                .collect::<Vec<_>>();
            let source_view = self
                .registry
                .get(old_storage)
                .ok_or(SchemaMutationError::Corrupt(
                    "composition source disappeared",
                ))?
                .read_view()?;
            let flow = self
                .registry
                .get_mut(old_storage)
                .ok_or(SchemaMutationError::Corrupt(
                    "composition source disappeared",
                ))?
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
                        transaction
                            .with_composed_staged_write(new_storage, |storage, context| {
                                storage.insert_in(context, &values).map(|_| ())
                            })?;
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
            if flow.is_break() {
                return Err(SchemaMutationError::Corrupt(
                    "composition row visitor stopped unexpectedly",
                )
                .into());
            }
            crash("composition-table-copy-complete");
            crash(&format!("composition-after-table-copy-{}", position + 1));
        }
        crash("composition-all-targets-staged");
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let (materialized, backfill) = match previous {
            SchemaCompositionState::SealingAndMaterializing(materialized) => (materialized, false),
            SchemaCompositionState::BackfillMaterializing(materialized) => (materialized, true),
            _ => {
                return Err(
                    SchemaMutationError::Corrupt("composition materialization state").into(),
                );
            }
        };
        transaction.schema_composition = if backfill {
            SchemaCompositionState::BackfillOpen(materialized)
        } else {
            SchemaCompositionState::Materialized(materialized)
        };
        Ok(())
    }

    fn materialize_table_object_composition(
        &mut self,
        transaction: &mut Transaction,
        logical: SchemaTransactionPlan,
        activate_backfill: bool,
    ) -> Result<(), DatabaseError> {
        let backfill = activate_backfill
            && logical.created.len() == 1
            && logical.touched.is_empty()
            && logical.index_actions == 0
            && logical.created.values().all(|created| created.present);
        let schema_dirty = logical.created.values().any(|created| created.present)
            || logical.touched.iter().any(|(table_id, touched)| {
                logical
                    .overlay
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == *table_id)
                    .is_none_or(|table| table != &touched.base_table)
            });
        if !schema_dirty {
            return self.materialize_schema_index_composition(
                transaction,
                logical,
                SchemaIndexMaterialization::Ordinary,
            );
        }
        self.catalog_generation
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted(
                "runtime catalog revision",
            ))?;
        let target_generation = logical
            .base
            .committed
            .generation
            .0
            .checked_add(1)
            .map(netbadb_types::SchemaGeneration)
            .ok_or(SchemaMutationError::IdentityExhausted("SchemaGeneration"))?;
        let target_epoch = logical
            .base
            .epoch
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted("catalog epoch"))?;

        let mut allocation_ids = BTreeSet::new();
        allocation_ids.extend(
            logical
                .created
                .iter()
                .filter(|(_, created)| created.present)
                .map(|(table, _)| *table),
        );
        allocation_ids.extend(logical.touched.iter().filter_map(|(table, touched)| {
            logical
                .overlay
                .schema
                .tables()
                .iter()
                .find(|candidate| candidate.id == *table)
                .filter(|candidate| *candidate != &touched.base_table)
                .map(|_| *table)
        }));
        let mut next_storage = self
            .next_storage_id()
            .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        let mut allocations = BTreeMap::new();
        for table in allocation_ids {
            allocations.insert(table, next_storage);
            next_storage = next_storage
                .0
                .checked_add(1)
                .map(StorageId)
                .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        }
        let coordinator_locator = logical
            .base
            .coordinator
            .clone()
            .unwrap_or_else(|| logical.journal.borrow().coordinator.clone());
        let mut target = logical.base.clone();
        target.epoch = target_epoch;
        target.committed = logical.overlay.clone();
        target.committed.generation = target_generation;
        target.committed.next_storage_id = Some(next_storage);
        target.coordinator = Some(coordinator_locator.clone());

        let mut table_ids = logical.touched.keys().copied().collect::<BTreeSet<_>>();
        table_ids.extend(logical.created.keys().copied());
        let mut table_plans = Vec::new();
        for table_id in table_ids {
            if let Some(created) = logical.created.get(&table_id) {
                if !created.present {
                    continue;
                }
                let final_table = target
                    .committed
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == table_id)
                    .cloned()
                    .ok_or(SchemaMutationError::Corrupt("created target table absent"))?;
                let storage = allocations[&table_id];
                let descriptor = CatalogStorage {
                    id: storage,
                    table_id,
                    locator: final_locator(&logical.catalog, target.incarnation, storage)?,
                    kind: CatalogStorageKind::Heap,
                };
                let placement = CatalogTable {
                    table_id,
                    schema_fingerprint: final_table.fingerprint()?,
                    placement: TablePlacement::Single {
                        table_id,
                        storage_id: storage,
                    },
                };
                target.placements.tables.push(placement.clone());
                target.storages.push(descriptor.clone());
                let lineage = target
                    .committed
                    .tables
                    .iter()
                    .find(|lineage| lineage.table_id == table_id)
                    .cloned()
                    .ok_or(SchemaMutationError::Corrupt(
                        "created target lineage absent",
                    ))?;
                if lineage.version != TableSchemaVersion(1) {
                    return Err(
                        SchemaMutationError::Corrupt("created table version is not one").into(),
                    );
                }
                let fragment = one_table_fragment(
                    &target,
                    final_table,
                    lineage,
                    placement,
                    descriptor,
                    coordinator_locator.clone(),
                )?;
                table_plans.push(SchemaIndexTablePlan::CreateHeap {
                    target: Box::new(fragment),
                    final_indexes: created.indexes.clone(),
                });
                continue;
            }
            let touched = logical
                .touched
                .get(&table_id)
                .ok_or(SchemaMutationError::Corrupt("table-object origin absent"))?;
            let final_table = target
                .committed
                .schema
                .tables()
                .iter()
                .find(|table| table.id == table_id)
                .cloned();
            let base_fragment = one_table_fragment(
                &logical.base,
                touched.base_table.clone(),
                touched.base_lineage.clone(),
                touched.catalog_table.clone(),
                touched.descriptor.clone(),
                coordinator_locator.clone(),
            )?;
            let Some(final_table) = final_table else {
                target
                    .placements
                    .tables
                    .retain(|placement| placement.table_id != table_id);
                target
                    .storages
                    .retain(|storage| storage.id != touched.old_storage);
                table_plans.push(SchemaIndexTablePlan::DropHeap {
                    base: Box::new(base_fragment),
                    retired: false,
                    gc: None,
                });
                continue;
            };
            if final_table != touched.base_table {
                let storage = allocations[&table_id];
                let descriptor = CatalogStorage {
                    id: storage,
                    table_id,
                    locator: final_locator(&logical.catalog, target.incarnation, storage)?,
                    kind: CatalogStorageKind::Heap,
                };
                let placement = CatalogTable {
                    table_id,
                    schema_fingerprint: final_table.fingerprint()?,
                    placement: TablePlacement::Single {
                        table_id,
                        storage_id: storage,
                    },
                };
                *target
                    .placements
                    .tables
                    .iter_mut()
                    .find(|entry| entry.table_id == table_id)
                    .ok_or(SchemaMutationError::Corrupt("rewrite placement absent"))? =
                    placement.clone();
                *target
                    .storages
                    .iter_mut()
                    .find(|entry| entry.id == touched.old_storage)
                    .ok_or(SchemaMutationError::Corrupt("rewrite storage absent"))? =
                    descriptor.clone();
                let lineage = target
                    .committed
                    .tables
                    .iter()
                    .find(|lineage| lineage.table_id == table_id)
                    .cloned()
                    .ok_or(SchemaMutationError::Corrupt("rewrite lineage absent"))?;
                let target_fragment = one_table_fragment(
                    &target,
                    final_table,
                    lineage,
                    placement,
                    descriptor,
                    coordinator_locator.clone(),
                )?;
                table_plans.push(SchemaIndexTablePlan::RewriteHeap {
                    replacement: Box::new(CompositionTablePlan {
                        base: base_fragment,
                        target: target_fragment,
                        retired: false,
                        gc: None,
                    }),
                    base_indexes: touched.base_indexes.clone(),
                    final_indexes: touched.indexes.clone(),
                });
            } else if touched.indexes.active != touched.base_indexes.active {
                if let Some(lineage) = target
                    .committed
                    .tables
                    .iter_mut()
                    .find(|lineage| lineage.table_id == table_id)
                {
                    lineage.version = touched.base_lineage.version;
                }
                table_plans.push(SchemaIndexTablePlan::InPlaceIndexDelta {
                    table: table_id,
                    table_version: touched.base_lineage.version,
                    fingerprint: touched.base_table.fingerprint()?,
                    storage: touched.old_storage,
                    base_indexes: touched.base_indexes.clone(),
                    final_indexes: touched.indexes.clone(),
                });
            } else if let Some(lineage) = target
                .committed
                .tables
                .iter_mut()
                .find(|lineage| lineage.table_id == table_id)
            {
                lineage.version = touched.base_lineage.version;
            }
        }
        if table_plans.is_empty() {
            transaction.schema_composition =
                SchemaCompositionState::SealedNoEffectiveChange(Box::new(logical));
            return Ok(());
        }
        target.placements.tables.sort_by_key(|entry| entry.table_id);
        target.storages.sort_by_key(|entry| entry.id);
        target.committed.tables.sort_by_key(|entry| entry.table_id);
        target.validate()?;
        let bytes = target.encode()?;
        let intent = TableObjectChangeSetIntent {
            transaction: transaction.id(),
            base_generation: logical.base.committed.generation,
            target_generation,
            base_epoch: logical.base.epoch,
            target_epoch,
            action_count: u32::try_from(logical.action_count())
                .map_err(|_| SchemaMutationError::CompositionLimitExceeded("schema actions"))?,
            action_digest: logical.action_digest(),
            snapshot_digest: digest(&bytes),
            tables: table_plans,
        };
        crash("composition-before-intent");
        let intent_result = logical
            .journal
            .borrow_mut()
            .table_object_intent(intent.clone());
        if let Err(error) = intent_result {
            return if logical.journal.borrow().ensure_ready().is_err() {
                Err(SchemaMutationError::RecoveryRequired.into())
            } else {
                Err(error.into())
            };
        }
        crash("composition-intent-durable");
        if backfill {
            let (table_id, storage, provisional) = match intent.tables.first() {
                Some(SchemaIndexTablePlan::CreateHeap { target, .. }) => (
                    intent.tables[0].table(),
                    target.storages[0].id,
                    target.as_ref().clone(),
                ),
                _ => {
                    return Err(SchemaMutationError::Corrupt("created backfill plan absent").into());
                }
            };
            logical
                .journal
                .borrow_mut()
                .stage_resource_intent(StageResourceIntent {
                    transaction: intent.transaction,
                    table: table_id,
                    storage,
                    base_generation: intent.base_generation,
                    base_epoch: intent.base_epoch,
                    provisional: provisional.clone(),
                    stage_locator: stage_locator(
                        &logical.catalog,
                        logical.base.incarnation,
                        intent.transaction,
                        storage,
                    )?,
                    final_locator: final_locator(
                        &logical.catalog,
                        logical.base.incarnation,
                        storage,
                    )?,
                    digest: digest(&provisional.encode()?),
                })?;
            crash("backfill-stage-intent-durable");
        }
        let coordinator = match &self.coordinator {
            Some(coordinator) => Rc::clone(coordinator),
            None => {
                let path = file::resolve(&logical.catalog, &coordinator_locator);
                validate_resource_path(&logical.catalog, &path)?;
                ensure_parent(&path)?;
                let log = if path
                    .try_exists()
                    .map_err(|error| file::io("inspect schema coordinator", &path, error))?
                {
                    CoordinatorLog::open(&path)?
                } else {
                    CoordinatorLog::create(&path)?
                };
                Rc::new(std::cell::RefCell::new(log))
            }
        };
        transaction.set_coordinator(coordinator);
        let reference = SchemaParticipantReference {
            incarnation: target.incarnation,
            target_epoch,
            digest: intent.snapshot_digest,
        };
        transaction.schema_composition = SchemaCompositionState::SealingAndMaterializingIndex(
            Box::new(MaterializedSchemaIndexTransaction {
                logical,
                target: Some(target),
                reference: Some(reference),
                intent: intent.clone(),
                staged: BTreeMap::new(),
                publications: Vec::new(),
                backfill,
                #[cfg(test)]
                source_copy_passes: 0,
                #[cfg(test)]
                source_rows_copied: 0,
            }),
        );
        for (position, plan) in intent.tables.iter().enumerate() {
            match plan {
                SchemaIndexTablePlan::CreateHeap {
                    target,
                    final_indexes,
                } => self.materialize_created_heap(transaction, target, final_indexes, position)?,
                SchemaIndexTablePlan::DropHeap { .. } => {}
                SchemaIndexTablePlan::RewriteHeap {
                    replacement,
                    final_indexes,
                    ..
                } => self.materialize_schema_index_rewrite(
                    transaction,
                    replacement,
                    final_indexes,
                    position,
                )?,
                SchemaIndexTablePlan::InPlaceIndexDelta {
                    storage,
                    base_indexes,
                    final_indexes,
                    ..
                } => {
                    let publication = self.materialize_in_place_index_delta(
                        transaction,
                        *storage,
                        base_indexes,
                        final_indexes,
                    )?;
                    transaction
                        .schema_composition
                        .materialized_index_mut()
                        .ok_or(SchemaMutationError::Corrupt("table-object state absent"))?
                        .publications
                        .push(publication);
                }
            }
        }
        crash("composition-all-targets-staged");
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let SchemaCompositionState::SealingAndMaterializingIndex(materialized) = previous else {
            return Err(SchemaMutationError::Corrupt("table-object materialization state").into());
        };
        transaction.schema_composition = if backfill {
            SchemaCompositionState::BackfillOpenIndex(materialized)
        } else {
            SchemaCompositionState::MaterializedIndex(materialized)
        };
        Ok(())
    }

    fn materialize_schema_index_composition(
        &mut self,
        transaction: &mut Transaction,
        logical: SchemaTransactionPlan,
        materialization: SchemaIndexMaterialization,
    ) -> Result<(), DatabaseError> {
        let effective = logical
            .touched
            .iter()
            .filter_map(|(table_id, touched)| {
                let final_table = logical
                    .overlay
                    .schema
                    .tables()
                    .iter()
                    .find(|table| table.id == *table_id)?
                    .clone();
                let schema_dirty = final_table != touched.base_table;
                let index_dirty = touched.indexes.active != touched.base_indexes.active;
                (schema_dirty || index_dirty).then_some((
                    *table_id,
                    touched,
                    final_table,
                    schema_dirty,
                    index_dirty,
                ))
            })
            .collect::<Vec<_>>();
        if effective.is_empty() {
            transaction.schema_composition =
                SchemaCompositionState::SealedNoEffectiveChange(Box::new(logical));
            return Ok(());
        }
        if effective.len() > MAX_TOUCHED_TABLES {
            return Err(SchemaMutationError::CompositionLimitExceeded("touched tables").into());
        }
        self.catalog_generation
            .checked_add(1)
            .ok_or(SchemaMutationError::IdentityExhausted(
                "runtime catalog revision",
            ))?;
        let schema_dirty = effective.iter().any(|entry| entry.3);
        let (target_generation, target_epoch) = if schema_dirty {
            (
                Some(
                    logical
                        .base
                        .committed
                        .generation
                        .0
                        .checked_add(1)
                        .map(netbadb_types::SchemaGeneration)
                        .ok_or(SchemaMutationError::IdentityExhausted("SchemaGeneration"))?,
                ),
                Some(
                    logical
                        .base
                        .epoch
                        .checked_add(1)
                        .ok_or(SchemaMutationError::IdentityExhausted("catalog epoch"))?,
                ),
            )
        } else {
            (None, None)
        };
        let mut next_storage = self
            .next_storage_id()
            .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
        let mut allocations = BTreeMap::new();
        for (table_id, _, _, schema_dirty, _) in &effective {
            if *schema_dirty {
                allocations.insert(*table_id, next_storage);
                next_storage = next_storage
                    .0
                    .checked_add(1)
                    .map(StorageId)
                    .ok_or(SchemaMutationError::IdentityExhausted("StorageId"))?;
            }
        }
        let coordinator_locator = logical
            .base
            .coordinator
            .clone()
            .unwrap_or_else(|| logical.journal.borrow().coordinator.clone());
        let mut target = schema_dirty.then(|| logical.base.clone());
        if let Some(target) = &mut target {
            target.epoch = target_epoch.ok_or(SchemaMutationError::Corrupt(
                "schema/index target epoch absent",
            ))?;
            target.committed = logical.overlay.clone();
            target.committed.generation = target_generation.ok_or(SchemaMutationError::Corrupt(
                "schema/index target generation absent",
            ))?;
            target.committed.next_storage_id = Some(next_storage);
            target.coordinator = Some(coordinator_locator.clone());
            let dirty_ids = effective
                .iter()
                .filter(|entry| entry.3)
                .map(|entry| entry.0)
                .collect::<BTreeSet<_>>();
            for lineage in &mut target.committed.tables {
                if !dirty_ids.contains(&lineage.table_id) {
                    let base_lineage = logical
                        .base
                        .committed
                        .tables
                        .iter()
                        .find(|base| base.table_id == lineage.table_id)
                        .ok_or(SchemaMutationError::Corrupt("base lineage disappeared"))?;
                    lineage.version = base_lineage.version;
                }
            }
        }
        let mut table_plans = Vec::with_capacity(effective.len());
        for (table_id, touched, final_table, table_schema_dirty, _) in &effective {
            if *table_schema_dirty {
                let target_snapshot = target.as_mut().ok_or(SchemaMutationError::Corrupt(
                    "rewrite target snapshot absent",
                ))?;
                let new_storage = allocations[table_id];
                let new_descriptor = CatalogStorage {
                    id: new_storage,
                    table_id: *table_id,
                    locator: final_locator(
                        &logical.catalog,
                        target_snapshot.incarnation,
                        new_storage,
                    )?,
                    kind: CatalogStorageKind::Heap,
                };
                let target_placement = CatalogTable {
                    table_id: *table_id,
                    schema_fingerprint: final_table.fingerprint()?,
                    placement: TablePlacement::Single {
                        table_id: *table_id,
                        storage_id: new_storage,
                    },
                };
                *target_snapshot
                    .placements
                    .tables
                    .iter_mut()
                    .find(|placement| placement.table_id == *table_id)
                    .ok_or(SchemaMutationError::Corrupt("target placement disappeared"))? =
                    target_placement.clone();
                *target_snapshot
                    .storages
                    .iter_mut()
                    .find(|storage| storage.id == touched.old_storage)
                    .ok_or(SchemaMutationError::Corrupt(
                        "target descriptor disappeared",
                    ))? = new_descriptor.clone();
                let target_lineage = target_snapshot
                    .committed
                    .tables
                    .iter()
                    .find(|lineage| lineage.table_id == *table_id)
                    .ok_or(SchemaMutationError::Corrupt("target lineage disappeared"))?
                    .clone();
                let base_fragment = one_table_fragment(
                    &logical.base,
                    touched.base_table.clone(),
                    touched.base_lineage.clone(),
                    touched.catalog_table.clone(),
                    touched.descriptor.clone(),
                    coordinator_locator.clone(),
                )?;
                let target_fragment = one_table_fragment(
                    target_snapshot,
                    final_table.clone(),
                    target_lineage,
                    target_placement,
                    new_descriptor,
                    coordinator_locator.clone(),
                )?;
                table_plans.push(SchemaIndexTablePlan::RewriteHeap {
                    replacement: Box::new(CompositionTablePlan {
                        base: base_fragment,
                        target: target_fragment,
                        retired: false,
                        gc: None,
                    }),
                    base_indexes: touched.base_indexes.clone(),
                    final_indexes: touched.indexes.clone(),
                });
            } else {
                table_plans.push(SchemaIndexTablePlan::InPlaceIndexDelta {
                    table: *table_id,
                    table_version: touched.base_lineage.version,
                    fingerprint: touched.base_table.fingerprint()?,
                    storage: touched.old_storage,
                    base_indexes: touched.base_indexes.clone(),
                    final_indexes: touched.indexes.clone(),
                });
            }
        }
        if let Some(target) = &target {
            target.validate()?;
        }
        let snapshot_digest = target
            .as_ref()
            .map(SchemaCatalogSnapshot::encode)
            .transpose()?
            .map(|bytes| digest(&bytes));
        let intent = SchemaIndexChangeSetIntent {
            transaction: transaction.id(),
            base_generation: logical.base.committed.generation,
            target_generation,
            base_epoch: logical.base.epoch,
            target_epoch,
            action_count: u32::try_from(logical.action_count()).map_err(|_| {
                SchemaMutationError::CompositionLimitExceeded("schema/index actions")
            })?,
            action_digest: logical.action_digest(),
            snapshot_digest,
            tables: table_plans,
        };
        if materialization.source().is_some() {
            crash("source-backfill-target-reserved");
        }
        crash("composition-before-intent");
        let intent_result = if materialization.replaces_existing_index_intent() {
            logical
                .journal
                .borrow_mut()
                .replace_schema_index_intent(intent.clone())
        } else {
            logical
                .journal
                .borrow_mut()
                .schema_index_intent(intent.clone())
        };
        if let Err(error) = intent_result {
            return if logical.journal.borrow().ensure_ready().is_err() {
                Err(SchemaMutationError::RecoveryRequired.into())
            } else {
                Err(error.into())
            };
        }
        crash("composition-intent-durable");
        let coordinator = match &self.coordinator {
            Some(coordinator) => Rc::clone(coordinator),
            None => {
                let path = file::resolve(&logical.catalog, &coordinator_locator);
                validate_resource_path(&logical.catalog, &path)?;
                ensure_parent(&path)?;
                let log = if path
                    .try_exists()
                    .map_err(|error| file::io("inspect schema coordinator", &path, error))?
                {
                    CoordinatorLog::open(&path)?
                } else {
                    CoordinatorLog::create(&path)?
                };
                Rc::new(std::cell::RefCell::new(log))
            }
        };
        transaction.set_coordinator(coordinator);
        let reference = match (&target, snapshot_digest, target_epoch) {
            (Some(target), Some(digest), Some(target_epoch)) => Some(SchemaParticipantReference {
                incarnation: target.incarnation,
                target_epoch,
                digest,
            }),
            (None, None, None) => None,
            _ => {
                return Err(
                    SchemaMutationError::Corrupt("schema/index target evidence mismatch").into(),
                );
            }
        };
        if let Some((source_storage, source_physical_txn_id)) = materialization.source() {
            let target_snapshot = target.as_ref().ok_or(SchemaMutationError::Corrupt(
                "source-backfill target snapshot absent",
            ))?;
            let (replacement, final_indexes) = match intent.tables.as_slice() {
                [
                    SchemaIndexTablePlan::RewriteHeap {
                        replacement,
                        final_indexes,
                        ..
                    },
                ] if replacement.old_storage() == source_storage => {
                    (replacement.as_ref(), final_indexes)
                }
                _ => {
                    return Err(SchemaMutationError::Corrupt(
                        "source-backfill final plan is not one rewrite",
                    )
                    .into());
                }
            };
            let final_index_digest =
                crate::schema_mutation_journal::heap_rewrite_indexes_digest(final_indexes)?;
            let mut clone_plan = Sha256::new();
            clone_plan.update(intent.transaction.0.to_le_bytes());
            clone_plan.update(source_storage.0.to_le_bytes());
            clone_plan.update(replacement.new_storage().0.to_le_bytes());
            clone_plan.update(intent.action_digest);
            clone_plan.update(intent.snapshot_digest.ok_or(SchemaMutationError::Corrupt(
                "source-backfill snapshot digest absent",
            ))?);
            let source_intent =
                SourceBackfillIntent {
                    transaction: intent.transaction,
                    incarnation: target_snapshot.incarnation,
                    table: replacement.table(),
                    source_storage,
                    source_table_version: replacement.base.committed.tables[0].version,
                    source_fingerprint: replacement.base.placements.tables[0].schema_fingerprint,
                    source_locator: replacement.base.storages[0].locator.clone(),
                    source_physical_txn_id,
                    target_storage: replacement.new_storage(),
                    target_table_version: replacement.target.committed.tables[0].version,
                    target_fingerprint: replacement.target.placements.tables[0].schema_fingerprint,
                    base_generation: intent.base_generation,
                    target_generation: intent.target_generation.ok_or(
                        SchemaMutationError::Corrupt("source-backfill target generation absent"),
                    )?,
                    base_epoch: intent.base_epoch,
                    target_epoch: intent.target_epoch.ok_or(SchemaMutationError::Corrupt(
                        "source-backfill target epoch absent",
                    ))?,
                    target_stage_locator: stage_locator(
                        &logical.catalog,
                        target_snapshot.incarnation,
                        intent.transaction,
                        replacement.new_storage(),
                    )?,
                    target_final_locator: replacement.target.storages[0].locator.clone(),
                    final_index_digest,
                    clone_plan_digest: clone_plan.finalize().into(),
                };
            logical
                .journal
                .borrow_mut()
                .source_backfill_intent(source_intent.clone())?;
            crash("source-backfill-intent-durable");
            logical
                .journal
                .borrow_mut()
                .stage_resource_intent(StageResourceIntent {
                    transaction: intent.transaction,
                    table: replacement.table(),
                    storage: replacement.new_storage(),
                    base_generation: intent.base_generation,
                    base_epoch: intent.base_epoch,
                    provisional: replacement.target.clone(),
                    stage_locator: source_intent.target_stage_locator,
                    final_locator: source_intent.target_final_locator,
                    digest: digest(&replacement.target.encode()?),
                })?;
            crash("source-backfill-stage-intent-durable");
        }
        let materialized = Box::new(MaterializedSchemaIndexTransaction {
            logical,
            target,
            reference,
            intent: intent.clone().into(),
            staged: BTreeMap::new(),
            publications: Vec::new(),
            backfill: false,
            #[cfg(test)]
            source_copy_passes: 0,
            #[cfg(test)]
            source_rows_copied: 0,
        });
        transaction.schema_composition = if materialization.source().is_some() {
            SchemaCompositionState::LateCloneMaterializing(materialized)
        } else {
            SchemaCompositionState::SealingAndMaterializingIndex(materialized)
        };

        for (position, table_plan) in intent.tables.iter().enumerate() {
            match table_plan {
                SchemaIndexTablePlan::CreateHeap { .. } | SchemaIndexTablePlan::DropHeap { .. } => {
                    return Err(SchemaMutationError::Corrupt(
                        "table-object plan in Round 29 materializer",
                    )
                    .into());
                }
                SchemaIndexTablePlan::RewriteHeap {
                    replacement,
                    final_indexes,
                    ..
                } => {
                    self.materialize_schema_index_rewrite(
                        transaction,
                        replacement,
                        final_indexes,
                        position,
                    )?;
                }
                SchemaIndexTablePlan::InPlaceIndexDelta {
                    storage,
                    base_indexes,
                    final_indexes,
                    ..
                } => {
                    let drops = base_indexes
                        .active
                        .iter()
                        .filter(|base| {
                            !final_indexes.active.iter().any(|final_index| {
                                final_index.id == base.id && final_index == *base
                            })
                        })
                        .map(|index| index.id)
                        .collect::<Vec<_>>();
                    let creates = final_indexes
                        .active
                        .iter()
                        .filter(|final_index| {
                            !base_indexes
                                .active
                                .iter()
                                .any(|base| base.id == final_index.id && base == *final_index)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let target_floor = final_indexes.next_index_id;
                    let publication = transaction.with_write_storage(
                        *storage,
                        &mut self.registry,
                        |table, context| {
                            for id in &drops {
                                table.drop_index_in(context, *id)?;
                            }
                            table.advance_index_id_floor_in(context, target_floor)?;
                            let mut definitions = Vec::with_capacity(creates.len());
                            for create in &creates {
                                let name = create.name.clone().ok_or(
                                    netbadb_storage::StorageError::from(
                                        netbadb_index::IndexError::InvalidIndexHighWater(
                                            target_floor,
                                        ),
                                    ),
                                )?;
                                definitions.push(table.create_named_index_with_reserved_id_in(
                                    context,
                                    name,
                                    create.column_id,
                                    create.id,
                                    target_floor,
                                )?);
                                #[cfg(test)]
                                crash("adopted-index-delta-first-tree-built");
                            }
                            Ok(IndexPublication {
                                storage: *storage,
                                drops: drops.clone(),
                                creates: definitions,
                            })
                        },
                    )?;
                    transaction
                        .schema_composition
                        .materialized_index_mut()
                        .ok_or(SchemaMutationError::Corrupt(
                            "schema/index materialization state absent",
                        ))?
                        .publications
                        .push(publication);
                    crash(&format!("composition-after-index-delta-{}", position + 1));
                }
            }
        }
        crash("composition-all-targets-staged");
        if transaction.schema_composition.is_late_clone_materializing() {
            let materialized = transaction
                .schema_composition
                .materialized_index_mut()
                .ok_or(SchemaMutationError::Corrupt(
                    "late-clone materialization disappeared",
                ))?;
            let target = materialized
                .target
                .as_ref()
                .ok_or(SchemaMutationError::Corrupt(
                    "late-clone target snapshot absent",
                ))?;
            target.validate()?;
            let (replacement, final_indexes) = match intent.tables.as_slice() {
                [
                    SchemaIndexTablePlan::RewriteHeap {
                        replacement,
                        final_indexes,
                        ..
                    },
                ] => (replacement.as_ref(), final_indexes),
                _ => {
                    return Err(SchemaMutationError::Corrupt(
                        "late-clone final plan is not one rewrite",
                    )
                    .into());
                }
            };
            materialized
                .staged
                .get_mut(&replacement.new_storage())
                .ok_or(SchemaMutationError::Corrupt(
                    "late-clone target Heap absent",
                ))?
                .validate_heap_rewrite_index_inventory(
                    &replacement.target.committed.schema.tables()[0],
                    final_indexes,
                )?;
            crash("source-backfill-target-validated");
            let final_snapshot_digest = digest(&target.encode()?);
            let mut evidence = Sha256::new();
            evidence.update(final_snapshot_digest);
            evidence.update(crate::schema_mutation_journal::heap_rewrite_indexes_digest(
                final_indexes,
            )?);
            materialized
                .logical
                .journal
                .borrow_mut()
                .migration_finalization_intent(MigrationIndexFinalizationIntent {
                    transaction: intent.transaction,
                    table: replacement.table(),
                    storage: replacement.new_storage(),
                    final_table_version: replacement.target.committed.tables[0].version,
                    final_fingerprint: replacement.target.placements.tables[0].schema_fingerprint,
                    final_snapshot_digest,
                    stage_locator: stage_locator(
                        &materialized.logical.catalog,
                        target.incarnation,
                        intent.transaction,
                        replacement.new_storage(),
                    )?,
                    final_locator: replacement.target.storages[0].locator.clone(),
                    final_indexes: final_indexes.clone(),
                    digest: evidence.finalize().into(),
                })?;
            crash("source-backfill-index-finalization-intent-durable");
        }
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        transaction.schema_composition = match previous {
            SchemaCompositionState::SealingAndMaterializingIndex(materialized) => {
                SchemaCompositionState::MaterializedIndex(materialized)
            }
            SchemaCompositionState::LateCloneMaterializing(materialized) => {
                SchemaCompositionState::LateCloneReady(materialized)
            }
            _ => {
                return Err(
                    SchemaMutationError::Corrupt("schema/index materialization state").into(),
                );
            }
        };
        Ok(())
    }

    fn materialize_schema_index_rewrite(
        &mut self,
        transaction: &mut Transaction,
        table_plan: &CompositionTablePlan,
        final_indexes: &HeapRewriteIndexes,
        position: usize,
    ) -> Result<(), DatabaseError> {
        let table_id = table_plan.table();
        let old_storage = table_plan.old_storage();
        let new_storage = table_plan.new_storage();
        let source_table = table_plan.base.committed.schema.tables()[0].clone();
        let final_table = table_plan.target.committed.schema.tables()[0].clone();
        let reserved_new_columns = {
            let plan = transaction
                .schema_composition
                .plan()
                .ok_or(SchemaMutationError::Corrupt("composition plan absent"))?;
            let journal = plan.journal.borrow();
            journal
                .compositions
                .get(&transaction.id())
                .into_iter()
                .flat_map(|record| record.reservations.iter())
                .filter(|reservation| reservation.table == table_id)
                .map(|reservation| reservation.column)
                .collect::<BTreeSet<_>>()
        };
        let projection = RowProjection::build(
            &source_table,
            table_plan.base.committed.tables[0].version,
            &final_table,
            table_plan.target.committed.tables[0].version,
            &reserved_new_columns,
        )?;
        let catalog = &transaction
            .schema_composition
            .plan()
            .ok_or(SchemaMutationError::Corrupt("composition plan absent"))?
            .catalog;
        let stage = file::resolve(
            catalog,
            &stage_locator(
                catalog,
                table_plan.target.incarnation,
                transaction.id(),
                new_storage,
            )?,
        );
        validate_resource_path(catalog, &stage)?;
        ensure_parent(&stage)?;
        write_owner(
            &file::suffix(&stage, ".owner"),
            table_plan.target.incarnation,
            transaction.id(),
            table_id,
            new_storage,
            final_table.fingerprint()?,
        )?;
        crash("composition-stage-first-file");
        let storage =
            TableStorage::create_heap_with_storage_id(&stage, final_table.clone(), new_storage)?;
        crash(&format!("composition-after-target-create-{}", position + 1));
        storage.flush()?;
        file::sync_parent(&stage)?;
        transaction.enlist_composed_staged(storage)?;
        transaction.with_composed_staged_write(new_storage, |storage, context| {
            storage.install_heap_rewrite_indexes_in(context, final_indexes)
        })?;
        let old_columns = source_table
            .columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        let transaction_view = if transaction.schema_composition.is_late_clone_materializing() {
            Some(transaction.begin_read_view(&[old_storage], &mut self.registry)?)
        } else {
            None
        };
        let committed_view = if transaction_view.is_none() {
            Some(
                self.registry
                    .get(old_storage)
                    .ok_or(SchemaMutationError::Corrupt(
                        "composition source disappeared",
                    ))?
                    .read_view()?,
            )
        } else {
            None
        };
        let source_view = match (&transaction_view, &committed_view) {
            (Some(view), None) => view
                .iter()
                .find_map(|(storage, view)| (storage == old_storage).then_some(view))
                .ok_or(SchemaMutationError::Corrupt(
                    "source-backfill transaction view absent",
                ))?,
            (None, Some(view)) => view,
            _ => {
                return Err(
                    SchemaMutationError::Corrupt("composition source view state invalid").into(),
                );
            }
        };
        let mut copied_rows = 0_u64;
        let late_clone = transaction.schema_composition.is_late_clone_materializing();
        let flow = self
            .registry
            .get_mut(old_storage)
            .ok_or(SchemaMutationError::Corrupt(
                "composition source disappeared",
            ))?
            .visit_rows_with_view_control::<DatabaseError, _>(
                &old_columns,
                source_view,
                |_row, old_values| {
                    let values = projection.project(&old_values)?;
                    transaction.with_composed_staged_write(new_storage, |storage, context| {
                        storage.insert_in(context, &values).map(|_| ())
                    })?;
                    copied_rows =
                        copied_rows
                            .checked_add(1)
                            .ok_or(SchemaMutationError::Corrupt(
                                "source-backfill copy count overflow",
                            ))?;
                    if late_clone && copied_rows == 1 {
                        crash("source-backfill-mid-copy");
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
        if flow.is_break() {
            return Err(SchemaMutationError::Corrupt(
                "composition row visitor stopped unexpectedly",
            )
            .into());
        }
        #[cfg(test)]
        if late_clone {
            let materialized = transaction
                .schema_composition
                .materialized_index_mut()
                .ok_or(SchemaMutationError::Corrupt(
                    "source-backfill copy instrumentation state absent",
                ))?;
            materialized.source_copy_passes =
                materialized.source_copy_passes.checked_add(1).ok_or(
                    SchemaMutationError::Corrupt("source-backfill copy pass count overflow"),
                )?;
            materialized.source_rows_copied = copied_rows;
        }
        crash("composition-table-copy-complete");
        if late_clone {
            crash("source-backfill-final-indexes-built");
        }
        crash(&format!("composition-after-table-copy-{}", position + 1));
        Ok(())
    }

    fn materialize_created_heap(
        &mut self,
        transaction: &mut Transaction,
        target: &SchemaCatalogSnapshot,
        final_indexes: &HeapRewriteIndexes,
        position: usize,
    ) -> Result<(), DatabaseError> {
        let table = target.committed.schema.tables()[0].clone();
        let storage = target.storages[0].id;
        let catalog = &transaction
            .schema_composition
            .plan()
            .ok_or(SchemaMutationError::Corrupt("composition plan absent"))?
            .catalog;
        let stage = file::resolve(
            catalog,
            &stage_locator(catalog, target.incarnation, transaction.id(), storage)?,
        );
        validate_resource_path(catalog, &stage)?;
        ensure_parent(&stage)?;
        write_owner(
            &file::suffix(&stage, ".owner"),
            target.incarnation,
            transaction.id(),
            table.id,
            storage,
            table.fingerprint()?,
        )?;
        crash("composition-stage-first-file");
        let heap = TableStorage::create_heap_with_storage_id(&stage, table, storage)?;
        crash(&format!("composition-after-target-create-{}", position + 1));
        heap.flush()?;
        file::sync_parent(&stage)?;
        transaction.enlist_composed_staged(heap)?;
        transaction.with_composed_staged_write(storage, |heap, context| {
            heap.install_heap_rewrite_indexes_in(context, final_indexes)
        })?;
        crash(&format!(
            "composition-after-create-indexes-{}",
            position + 1
        ));
        Ok(())
    }

    fn materialize_in_place_index_delta(
        &mut self,
        transaction: &mut Transaction,
        storage: StorageId,
        base_indexes: &HeapRewriteIndexes,
        final_indexes: &HeapRewriteIndexes,
    ) -> Result<IndexPublication, DatabaseError> {
        let drops = base_indexes
            .active
            .iter()
            .filter(|base| {
                !final_indexes
                    .active
                    .iter()
                    .any(|final_index| final_index.id == base.id && final_index == *base)
            })
            .map(|index| index.id)
            .collect::<Vec<_>>();
        let creates = final_indexes
            .active
            .iter()
            .filter(|final_index| {
                !base_indexes
                    .active
                    .iter()
                    .any(|base| base.id == final_index.id && base == *final_index)
            })
            .cloned()
            .collect::<Vec<_>>();
        let target_floor = final_indexes.next_index_id;
        Ok(
            transaction.with_write_storage(storage, &mut self.registry, |table, context| {
                for id in &drops {
                    table.drop_index_in(context, *id)?;
                }
                table.advance_index_id_floor_in(context, target_floor)?;
                let mut definitions = Vec::with_capacity(creates.len());
                for create in &creates {
                    let name = create
                        .name
                        .clone()
                        .ok_or(netbadb_storage::StorageError::from(
                            netbadb_index::IndexError::InvalidIndexHighWater(target_floor),
                        ))?;
                    definitions.push(table.create_named_index_with_reserved_id_in(
                        context,
                        name,
                        create.column_id,
                        create.id,
                        target_floor,
                    )?);
                }
                Ok(IndexPublication {
                    storage,
                    drops: drops.clone(),
                    creates: definitions,
                })
            })?,
        )
    }

    pub(crate) fn finish_composition_commit(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        if transaction.state() != TransactionState::FinalizePending {
            return Err(SchemaMutationError::RecoveryRequired.into());
        }
        let state = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let mut materialized = match state {
            SchemaCompositionState::Materialized(materialized)
            | SchemaCompositionState::BackfillOpen(materialized)
            | SchemaCompositionState::Finalized(materialized) => materialized,
            state => {
                transaction.schema_composition = state;
                return Err(SchemaMutationError::Corrupt("materialized composition absent").into());
            }
        };
        let completion =
            (|| -> Result<CompositionCompletion, DatabaseError> {
                for storage in materialized.staged.values() {
                    storage.flush()?;
                }
                materialized.staged.clear();
                for plan in &materialized.intent.tables {
                    transaction.release_staged_context(plan.new_storage());
                }
                let decisions = transaction.coordinator_decisions()?;
                let mut winners = Vec::with_capacity(materialized.intent.tables.len());
                for (position, plan) in materialized.intent.tables.iter().enumerate() {
                    let reservation =
                        composition_physical_reservation(&materialized.intent, plan, None);
                    promote(
                        &materialized.logical.catalog,
                        &reservation,
                        &materialized.reference,
                    )?;
                    crash(&format!("composition-after-promotion-{}", position + 1));
                    let final_path = file::resolve(
                        &materialized.logical.catalog,
                        &final_locator(
                            &materialized.logical.catalog,
                            materialized.target.incarnation,
                            plan.new_storage(),
                        )?,
                    );
                    let storage = open_winner_heap(&final_path, &reservation, &decisions)?;
                    storage.flush()?;
                    let source = self.registry.get(plan.old_storage()).ok_or(
                        SchemaMutationError::Corrupt("composition source disappeared"),
                    )?;
                    if source.table() != &plan.base.committed.schema.tables()[0]
                        || source.storage_id() != plan.old_storage()
                    {
                        return Err(SchemaMutationError::Corrupt(
                            "composition source identity changed",
                        )
                        .into());
                    }
                    winners.push((plan.table(), plan.old_storage(), storage));
                }
                crash("composition-final-heaps-synced");
                for (position, plan) in materialized.intent.tables.iter().enumerate() {
                    materialized
                        .logical
                        .journal
                        .borrow_mut()
                        .retire_composition_table(materialized.intent.transaction, plan.table())?;
                    crash(&format!("composition-after-retirement-{}", position + 1));
                }
                crash("composition-retirements-durable");
                let revision = self.catalog_generation.checked_add(1).ok_or(
                    SchemaMutationError::IdentityExhausted("runtime catalog revision"),
                )?;
                crash("composition-before-nbsc-publication");
                let published =
                    file::publish_runtime(&materialized.logical.catalog, &materialized.target)?;
                crash("composition-nbsc-durable");
                transaction.finish_schema_decision()?;
                crash("composition-after-cord-complete");
                crash("composition-before-winner-resolution");
                materialized
                    .logical
                    .journal
                    .borrow_mut()
                    .resolve_composition(
                        materialized.intent.transaction,
                        CompositionResolution::Winner,
                    )?;
                crash("composition-after-winner-resolution");
                if let Some(first) = materialized.intent.tables.first() {
                    cleanup_prepared(
                        &materialized.logical.catalog,
                        &composition_physical_reservation(&materialized.intent, first, Some(true)),
                        materialized.target.incarnation,
                    )?;
                }
                crash("composition-before-memory-publication");
                Ok((published, revision, winners))
            })();
        let (published, revision, winners) = match completion {
            Ok(completion) => completion,
            Err(error) => {
                transaction.schema_composition = SchemaCompositionState::Materialized(materialized);
                return Err(error);
            }
        };
        let mut predecessors = Vec::with_capacity(winners.len());
        for (table, old_storage, storage) in winners {
            let new_storage = storage.storage_id();
            predecessors.push(self.registry.publish_replaced(old_storage, storage).ok_or(
                SchemaMutationError::Corrupt("composition source disappeared"),
            )?);
            self.bindings.publish_replaced(table, new_storage);
        }
        self.committed = published.committed;
        self.catalog_generation = revision;
        self.coordinator = transaction.shared_coordinator();
        materialized.logical.writer.set(None);
        transaction.complete_schema_publication();
        drop(predecessors);
        crash("composition-memory-published");
        crash("composition-before-api-return");
        Ok(())
    }

    pub(crate) fn finish_schema_index_composition_commit(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        if transaction.state() != TransactionState::FinalizePending {
            return Err(SchemaMutationError::RecoveryRequired.into());
        }
        let state = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let (mut materialized, late_clone) = match state {
            SchemaCompositionState::MaterializedIndex(materialized)
            | SchemaCompositionState::FinalizedIndex(materialized) => (materialized, false),
            SchemaCompositionState::LateCloneReady(materialized) => (materialized, true),
            state => {
                transaction.schema_composition = state;
                return Err(SchemaMutationError::Corrupt(
                    "materialized schema/index composition absent",
                )
                .into());
            }
        };
        let completion = (|| -> Result<_, DatabaseError> {
            for storage in materialized.staged.values() {
                storage.flush()?;
            }
            materialized.staged.clear();
            for plan in &materialized.intent.tables {
                match plan {
                    SchemaIndexTablePlan::CreateHeap { target, .. } => {
                        transaction.release_staged_context(target.storages[0].id);
                    }
                    SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                        transaction.release_staged_context(replacement.new_storage());
                    }
                    SchemaIndexTablePlan::DropHeap { .. }
                    | SchemaIndexTablePlan::InPlaceIndexDelta { .. } => {}
                }
            }
            let decisions = transaction.coordinator_decisions()?;
            let mut created = Vec::new();
            let mut winners = Vec::new();
            let mut dropped = Vec::new();
            for (position, plan) in materialized.intent.tables.iter().enumerate() {
                match plan {
                    SchemaIndexTablePlan::CreateHeap { target, .. } => {
                        let reference =
                            materialized
                                .reference
                                .as_ref()
                                .ok_or(SchemaMutationError::Corrupt(
                                    "CreateHeap schema reference absent",
                                ))?;
                        let reservation =
                            create_physical_reservation(&materialized.intent, target, None);
                        promote(&materialized.logical.catalog, &reservation, reference)?;
                        crash(&format!("composition-after-promotion-{}", position + 1));
                        let final_path = file::resolve(
                            &materialized.logical.catalog,
                            &target.storages[0].locator,
                        );
                        let storage = open_winner_heap(&final_path, &reservation, &decisions)?;
                        storage.flush()?;
                        created.push((plan.table(), storage));
                    }
                    SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                        let reference =
                            materialized
                                .reference
                                .as_ref()
                                .ok_or(SchemaMutationError::Corrupt(
                                    "rewrite schema reference absent",
                                ))?;
                        let reservation = schema_index_physical_reservation(
                            &materialized.intent,
                            replacement,
                            None,
                        );
                        promote(&materialized.logical.catalog, &reservation, reference)?;
                        crash(&format!("composition-after-promotion-{}", position + 1));
                        let final_path = file::resolve(
                            &materialized.logical.catalog,
                            &replacement.target.storages[0].locator,
                        );
                        let storage = open_winner_heap(&final_path, &reservation, &decisions)?;
                        storage.flush()?;
                        let source = self.registry.get(replacement.old_storage()).ok_or(
                            SchemaMutationError::Corrupt("composition source disappeared"),
                        )?;
                        if source.table() != &replacement.base.committed.schema.tables()[0]
                            || source.storage_id() != replacement.old_storage()
                        {
                            return Err(SchemaMutationError::Corrupt(
                                "composition source identity changed",
                            )
                            .into());
                        }
                        winners.push((replacement.table(), replacement.old_storage(), storage));
                    }
                    SchemaIndexTablePlan::DropHeap { base, .. } => {
                        let storage = base.storages[0].id;
                        let source = self
                            .registry
                            .get(storage)
                            .ok_or(SchemaMutationError::Corrupt("DropHeap source disappeared"))?;
                        if source.table() != &base.committed.schema.tables()[0] {
                            return Err(SchemaMutationError::Corrupt(
                                "DropHeap source identity changed",
                            )
                            .into());
                        }
                        dropped.push((plan.table(), storage));
                    }
                    SchemaIndexTablePlan::InPlaceIndexDelta { .. } => {}
                }
            }
            crash("composition-final-heaps-synced");
            for (position, plan) in materialized.intent.tables.iter().enumerate() {
                if matches!(
                    plan,
                    SchemaIndexTablePlan::RewriteHeap { .. }
                        | SchemaIndexTablePlan::DropHeap { .. }
                ) {
                    let table_object = materialized
                        .logical
                        .journal
                        .borrow()
                        .compositions
                        .get(&materialized.intent.transaction)
                        .is_some_and(|record| record.table_intent.is_some());
                    if table_object {
                        materialized
                            .logical
                            .journal
                            .borrow_mut()
                            .retire_table_object(materialized.intent.transaction, plan.table())?;
                    } else {
                        materialized
                            .logical
                            .journal
                            .borrow_mut()
                            .retire_composition_table(
                                materialized.intent.transaction,
                                plan.table(),
                            )?;
                    }
                    crash(&format!("composition-after-retirement-{}", position + 1));
                }
            }
            crash("composition-retirements-durable");
            let revision = self.catalog_generation.checked_add(1).ok_or(
                SchemaMutationError::IdentityExhausted("runtime catalog revision"),
            )?;
            let published = if let Some(target) = &materialized.target {
                crash("composition-before-nbsc-publication");
                Some(file::publish_runtime(
                    &materialized.logical.catalog,
                    target,
                )?)
            } else {
                None
            };
            if published.is_some() {
                crash("composition-nbsc-durable");
            }
            transaction.finish_schema_decision()?;
            crash("composition-after-cord-complete");
            crash("composition-before-winner-resolution");
            materialized
                .logical
                .journal
                .borrow_mut()
                .resolve_composition(
                    materialized.intent.transaction,
                    CompositionResolution::Winner,
                )?;
            crash("composition-after-winner-resolution");
            if let Some(plan) = materialized.intent.tables.iter().find(|plan| {
                matches!(
                    plan,
                    SchemaIndexTablePlan::CreateHeap { .. }
                        | SchemaIndexTablePlan::RewriteHeap { .. }
                )
            }) {
                let reservation = match plan {
                    SchemaIndexTablePlan::CreateHeap { target, .. } => {
                        create_physical_reservation(&materialized.intent, target, Some(true))
                    }
                    SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                        schema_index_physical_reservation(
                            &materialized.intent,
                            replacement,
                            Some(true),
                        )
                    }
                    _ => return Err(SchemaMutationError::Corrupt("prepared cleanup plan").into()),
                };
                cleanup_prepared(
                    &materialized.logical.catalog,
                    &reservation,
                    materialized.logical.base.incarnation,
                )?;
            }
            Ok((published, revision, created, winners, dropped))
        })();
        let (published, revision, created, winners, dropped) = match completion {
            Ok(completion) => completion,
            Err(error) => {
                transaction.schema_composition = if late_clone {
                    SchemaCompositionState::LateCloneReady(materialized)
                } else {
                    SchemaCompositionState::MaterializedIndex(materialized)
                };
                return Err(error);
            }
        };
        crash("composition-before-memory-publication");
        let mut predecessors = Vec::with_capacity(winners.len() + dropped.len());
        for (table, storage) in dropped {
            predecessors.push(
                self.registry
                    .publish_dropped(storage)
                    .ok_or(SchemaMutationError::Corrupt("DropHeap source disappeared"))?,
            );
            self.bindings.publish_dropped(table);
        }
        for (table, old_storage, storage) in winners {
            let new_storage = storage.storage_id();
            predecessors.push(self.registry.publish_replaced(old_storage, storage).ok_or(
                SchemaMutationError::Corrupt("composition source disappeared"),
            )?);
            self.bindings.publish_replaced(table, new_storage);
        }
        for (table, storage) in created {
            let storage_id = storage.storage_id();
            self.registry.publish_created(storage);
            self.bindings.publish_created(TablePlacement::Single {
                table_id: table,
                storage_id,
            });
        }
        for publication in &materialized.publications {
            let storage =
                self.registry
                    .get_mut(publication.storage)
                    .ok_or(SchemaMutationError::Corrupt(
                        "index publication storage disappeared",
                    ))?;
            for id in &publication.drops {
                storage.publish_committed_index_drop(*id);
            }
            for definition in &publication.creates {
                storage.publish_committed_index(definition.clone());
            }
        }
        if let Some(published) = published {
            self.committed = published.committed;
        }
        self.catalog_generation = revision;
        self.coordinator = transaction.shared_coordinator();
        materialized.logical.writer.set(None);
        transaction.complete_schema_publication();
        drop(predecessors);
        crash("composition-memory-published");
        crash("composition-before-api-return");
        Ok(())
    }

    pub(crate) fn finish_no_effective_composition(
        &mut self,
        transaction: &mut Transaction,
    ) -> Result<(), DatabaseError> {
        let state = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let SchemaCompositionState::SealedNoEffectiveChange(plan) = state else {
            transaction.schema_composition = state;
            return Ok(());
        };
        if plan
            .journal
            .borrow()
            .compositions
            .contains_key(&plan.transaction())
        {
            plan.journal.borrow_mut().resolve_composition(
                plan.transaction(),
                CompositionResolution::NoEffectiveChange,
            )?;
        }
        plan.writer.set(None);
        Ok(())
    }
}

fn composition_physical_reservation(
    intent: &SchemaChangeSetIntent,
    plan: &CompositionTablePlan,
    resolved: Option<bool>,
) -> Reservation {
    Reservation {
        transaction: intent.transaction,
        table: plan.table(),
        storage: plan.new_storage(),
        base_generation: intent.base_generation,
        base_epoch: intent.base_epoch,
        intent: Some(CreateIntent {
            fragment: plan.target.clone(),
            snapshot_digest: intent.snapshot_digest,
        }),
        resolved,
    }
}

fn schema_index_physical_reservation(
    intent: &TableObjectChangeSetIntent,
    plan: &CompositionTablePlan,
    resolved: Option<bool>,
) -> Reservation {
    Reservation {
        transaction: intent.transaction,
        table: plan.table(),
        storage: plan.new_storage(),
        base_generation: intent.base_generation,
        base_epoch: intent.base_epoch,
        intent: Some(CreateIntent {
            fragment: plan.target.clone(),
            snapshot_digest: intent.snapshot_digest,
        }),
        resolved,
    }
}

fn create_physical_reservation(
    intent: &TableObjectChangeSetIntent,
    target: &SchemaCatalogSnapshot,
    resolved: Option<bool>,
) -> Reservation {
    Reservation {
        transaction: intent.transaction,
        table: target.committed.schema.tables()[0].id,
        storage: target.storages[0].id,
        base_generation: intent.base_generation,
        base_epoch: intent.base_epoch,
        intent: Some(CreateIntent {
            fragment: target.clone(),
            snapshot_digest: intent.snapshot_digest,
        }),
        resolved,
    }
}

fn one_table_fragment(
    snapshot: &SchemaCatalogSnapshot,
    table: TableDef,
    lineage: TableLineage,
    placement: CatalogTable,
    storage: CatalogStorage,
    coordinator: String,
) -> Result<SchemaCatalogSnapshot, DatabaseError> {
    Ok(SchemaCatalogSnapshot {
        incarnation: snapshot.incarnation,
        epoch: snapshot.epoch,
        committed: crate::schema_catalog::CommittedCatalogState {
            schema: Schema::new(vec![table])?,
            generation: snapshot.committed.generation,
            next_table_id: snapshot.committed.next_table_id,
            next_storage_id: snapshot.committed.next_storage_id,
            next_partition_id: snapshot.committed.next_partition_id,
            tables: vec![lineage],
        },
        placements: PartitionCatalog {
            tables: vec![placement],
        },
        storages: vec![storage],
        coordinator: Some(coordinator),
        partition_evidence: None,
    })
}

pub(crate) fn cleanup_composition_loser(
    state: &mut SchemaCompositionState,
) -> Result<(), SchemaMutationError> {
    let previous = std::mem::replace(state, SchemaCompositionState::None);
    let (plan, intent) = match previous {
        SchemaCompositionState::Composing(plan)
        | SchemaCompositionState::SealedNoEffectiveChange(plan)
        | SchemaCompositionState::RollbackRequiredLogical(plan) => (*plan, None),
        SchemaCompositionState::AdoptedSourceRefining(adopted) => (adopted.logical, None),
        SchemaCompositionState::SealingAndMaterializing(mut materialized)
        | SchemaCompositionState::Materialized(mut materialized)
        | SchemaCompositionState::BackfillMaterializing(mut materialized)
        | SchemaCompositionState::BackfillOpen(mut materialized)
        | SchemaCompositionState::IndexEvacuating(mut materialized)
        | SchemaCompositionState::RefiningAfterEvacuation(mut materialized)
        | SchemaCompositionState::Refining(mut materialized)
        | SchemaCompositionState::IndexFinalizing(mut materialized)
        | SchemaCompositionState::Finalizing(mut materialized)
        | SchemaCompositionState::Finalized(mut materialized)
        | SchemaCompositionState::RollbackRequiredMaterialized(mut materialized) => {
            materialized.staged.clear();
            let intent = materialized.intent.clone();
            (materialized.logical, Some(intent))
        }
        SchemaCompositionState::SealingAndMaterializingIndex(mut materialized)
        | SchemaCompositionState::MaterializedIndex(mut materialized)
        | SchemaCompositionState::SourceBackfillOpen(mut materialized)
        | SchemaCompositionState::SourceRefining(mut materialized)
        | SchemaCompositionState::SourceIndexFinalizing(mut materialized)
        | SchemaCompositionState::LateCloneMaterializing(mut materialized)
        | SchemaCompositionState::LateCloneReady(mut materialized)
        | SchemaCompositionState::BackfillOpenIndex(mut materialized)
        | SchemaCompositionState::RefiningIndex(mut materialized)
        | SchemaCompositionState::FinalizedIndex(mut materialized)
        | SchemaCompositionState::RollbackRequiredMaterializedIndex(mut materialized) => {
            materialized.staged.clear();
            let mut first = None;
            for table in &materialized.intent.tables {
                let reservation = match table {
                    SchemaIndexTablePlan::CreateHeap { target, .. } => Some(
                        create_physical_reservation(&materialized.intent, target, Some(false)),
                    ),
                    SchemaIndexTablePlan::RewriteHeap { replacement, .. } => {
                        Some(schema_index_physical_reservation(
                            &materialized.intent,
                            replacement,
                            Some(false),
                        ))
                    }
                    SchemaIndexTablePlan::DropHeap { .. }
                    | SchemaIndexTablePlan::InPlaceIndexDelta { .. } => None,
                };
                if let Some(reservation) = reservation {
                    cleanup_staged_loser(
                        &materialized.logical.catalog,
                        &reservation,
                        materialized.logical.base.incarnation,
                    )?;
                    first.get_or_insert(reservation);
                }
            }
            if let Some(reservation) = first {
                cleanup_prepared(
                    &materialized.logical.catalog,
                    &reservation,
                    materialized.logical.base.incarnation,
                )?;
            }
            (materialized.logical, None)
        }
        SchemaCompositionState::None => return Ok(()),
    };
    if let Some(intent) = &intent {
        for table in &intent.tables {
            let reservation = Reservation {
                transaction: intent.transaction,
                table: table.table(),
                storage: table.new_storage(),
                base_generation: intent.base_generation,
                base_epoch: intent.base_epoch,
                intent: Some(CreateIntent {
                    fragment: table.target.clone(),
                    snapshot_digest: intent.snapshot_digest,
                }),
                resolved: Some(false),
            };
            cleanup_staged_loser(&plan.catalog, &reservation, plan.base.incarnation)?;
        }
        if let Some(first) = intent.tables.first() {
            cleanup_prepared(
                &plan.catalog,
                &composition_physical_reservation(intent, first, Some(false)),
                plan.base.incarnation,
            )?;
        }
    }
    if plan
        .journal
        .borrow()
        .compositions
        .contains_key(&plan.transaction())
    {
        plan.journal
            .borrow_mut()
            .resolve_composition(plan.transaction(), CompositionResolution::Loser)?;
    }
    plan.writer.set(None);
    Ok(())
}

#[cfg(test)]
#[path = "adopted_source_index_finalization_audit_tests.rs"]
mod adopted_source_index_finalization_audit_tests;
