//! ALTER-only transaction composition: logical overlay first, one base-to-final
//! Heap replacement per effectively changed table at the global seal.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::rc::Rc;

use netbadb_index::IndexDefinition;
use netbadb_schema::{Schema, TableDef};
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
    AlterTableOperation, AlterTableSpec, SchemaDependency, SchemaMutationError, SchemaWriter,
    SharedMutationJournal, build_alter_target, cleanup_prepared, cleanup_staged_loser, crash,
    digest, ensure_parent, open_winner_heap, promote, validate_resource_path, write_owner,
};
use crate::schema_mutation_journal::{
    CompositionColumnReservation, CompositionIndexReservation, CompositionResolution,
    CompositionTablePlan, CreateIntent, Reservation, SchemaChangeSetIntent,
    SchemaIndexChangeSetIntent, SchemaIndexTablePlan, SchemaMutationJournal, final_locator,
    namespace, prepared_locator, stage_locator,
};
use crate::{Database, DatabaseError, Transaction, TransactionState};

pub(crate) const MAX_SCHEMA_ACTIONS: usize = 128;
pub(crate) const MAX_TOUCHED_TABLES: usize = 64;
pub(crate) const MAX_COLUMN_RESERVATIONS: usize = 128;
pub(crate) const MAX_INDEX_RESERVATIONS: usize = 128;

#[derive(Debug)]
pub(crate) enum SchemaCompositionState {
    None,
    Composing(Box<SchemaTransactionPlan>),
    SealingAndMaterializing(Box<MaterializedSchemaTransaction>),
    Materialized(Box<MaterializedSchemaTransaction>),
    SealingAndMaterializingIndex(Box<MaterializedSchemaIndexTransaction>),
    MaterializedIndex(Box<MaterializedSchemaIndexTransaction>),
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
                | Self::SealingAndMaterializingIndex(_)
                | Self::MaterializedIndex(_)
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
            Self::SealingAndMaterializing(materialized)
            | Self::Materialized(materialized)
            | Self::RollbackRequiredMaterialized(materialized) => Some(&materialized.logical),
            Self::SealingAndMaterializingIndex(materialized)
            | Self::MaterializedIndex(materialized)
            | Self::RollbackRequiredMaterializedIndex(materialized) => Some(&materialized.logical),
            Self::None => None,
        }
    }

    pub(crate) fn materialized_index(&self) -> Option<&MaterializedSchemaIndexTransaction> {
        match self {
            Self::SealingAndMaterializingIndex(materialized)
            | Self::MaterializedIndex(materialized)
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
            | Self::RollbackRequiredMaterializedIndex(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn materialized(&self) -> Option<&MaterializedSchemaTransaction> {
        match self {
            Self::SealingAndMaterializing(materialized)
            | Self::Materialized(materialized)
            | Self::RollbackRequiredMaterialized(materialized) => Some(materialized),
            _ => None,
        }
    }

    pub(crate) fn materialized_mut(&mut self) -> Option<&mut MaterializedSchemaTransaction> {
        match self {
            Self::SealingAndMaterializing(materialized)
            | Self::Materialized(materialized)
            | Self::RollbackRequiredMaterialized(materialized) => Some(materialized),
            _ => None,
        }
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
pub(crate) struct SchemaTransactionPlan {
    pub(crate) transaction: DatabaseTxnId,
    pub(crate) catalog: PathBuf,
    pub(crate) base: SchemaCatalogSnapshot,
    pub(crate) overlay: crate::schema_catalog::CommittedCatalogState,
    pub(crate) touched: BTreeMap<TableId, ComposedTable>,
    pub(crate) action_evidence: Vec<[u8; 32]>,
    pub(crate) reservation_count: usize,
    pub(crate) index_reservation_count: usize,
    pub(crate) index_actions: usize,
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
    pub(crate) intent: SchemaIndexChangeSetIntent,
    pub(crate) staged: BTreeMap<StorageId, TableStorage>,
    pub(crate) publications: Vec<IndexPublication>,
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
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_alter(transaction, spec);
        self.handle_composition_accept_result(transaction, &result);
        result
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
            action_evidence: Vec::new(),
            reservation_count: 0,
            index_reservation_count: 0,
            index_actions: 0,
            journal,
            writer: Rc::clone(&self.schema_writer),
        })
    }

    fn apply_composed_alter(
        &mut self,
        transaction: &mut Transaction,
        spec: AlterTableSpec,
    ) -> Result<(), DatabaseError> {
        let transaction_id = transaction.id();
        let plan = match &mut transaction.schema_composition {
            SchemaCompositionState::Composing(plan) => plan,
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
        if let AlterTableOperation::SetNotNull { column_id } = &spec.operation {
            self.validate_composed_not_null(&touched, *column_id)?;
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
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_create_index(transaction, statement);
        self.handle_composition_accept_result(transaction, &result);
        result
    }

    fn apply_composed_create_index(
        &mut self,
        transaction: &mut Transaction,
        statement: &crate::TypedCreateIndex,
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
        self.ensure_composition_started(transaction)?;
        let result = self.apply_composed_drop_index(transaction, target);
        self.handle_composition_accept_result(transaction, &result);
        result
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
        transaction
            .schema_composition
            .plan()
            .and_then(|plan| plan.touched.get(&target.table_id))
            .and_then(|table| {
                table
                    .indexes
                    .active
                    .iter()
                    .find(|index| index.id == target.index_id)
                    .cloned()
            })
            .or_else(|| {
                self.indexes(target.table_id)
                    .ok()?
                    .iter()
                    .find(|index| index.id == target.index_id)
                    .map(|index| HeapRewriteIndex {
                        id: index.id,
                        name: index.name.clone(),
                        column_id: index.column_id,
                    })
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
        let materialized = if logical.index_actions == 0 {
            self.materialize_schema_composition(transaction, *logical)
        } else {
            self.materialize_schema_index_composition(transaction, *logical)
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
    ) -> Result<(), DatabaseError> {
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
        transaction.schema_composition = SchemaCompositionState::SealingAndMaterializing(Box::new(
            MaterializedSchemaTransaction {
                logical,
                target,
                reference,
                intent: intent.clone(),
                staged: BTreeMap::new(),
            },
        ));

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
        let SchemaCompositionState::SealingAndMaterializing(materialized) = previous else {
            return Err(SchemaMutationError::Corrupt("composition materialization state").into());
        };
        transaction.schema_composition = SchemaCompositionState::Materialized(materialized);
        Ok(())
    }

    fn materialize_schema_index_composition(
        &mut self,
        transaction: &mut Transaction,
        logical: SchemaTransactionPlan,
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
        crash("composition-before-intent");
        if let Err(error) = logical
            .journal
            .borrow_mut()
            .schema_index_intent(intent.clone())
        {
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
        transaction.schema_composition = SchemaCompositionState::SealingAndMaterializingIndex(
            Box::new(MaterializedSchemaIndexTransaction {
                logical,
                target,
                reference,
                intent: intent.clone(),
                staged: BTreeMap::new(),
                publications: Vec::new(),
            }),
        );

        for (position, table_plan) in intent.tables.iter().enumerate() {
            match table_plan {
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
        let previous = std::mem::replace(
            &mut transaction.schema_composition,
            SchemaCompositionState::None,
        );
        let SchemaCompositionState::SealingAndMaterializingIndex(materialized) = previous else {
            return Err(SchemaMutationError::Corrupt("schema/index materialization state").into());
        };
        transaction.schema_composition = SchemaCompositionState::MaterializedIndex(materialized);
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
        let final_table = table_plan.target.committed.schema.tables()[0].clone();
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
                            position
                                .map_or(ScalarValue::Null, |position| old_values[position].clone())
                        })
                        .collect::<Vec<_>>();
                    transaction.with_composed_staged_write(new_storage, |storage, context| {
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
        Ok(())
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
        let SchemaCompositionState::Materialized(mut materialized) = state else {
            transaction.schema_composition = state;
            return Err(SchemaMutationError::Corrupt("materialized composition absent").into());
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
        let SchemaCompositionState::MaterializedIndex(mut materialized) = state else {
            transaction.schema_composition = state;
            return Err(SchemaMutationError::Corrupt(
                "materialized schema/index composition absent",
            )
            .into());
        };
        let completion = (|| -> Result<_, DatabaseError> {
            for storage in materialized.staged.values() {
                storage.flush()?;
            }
            materialized.staged.clear();
            for plan in &materialized.intent.tables {
                if let Some(replacement) = plan.replacement() {
                    transaction.release_staged_context(replacement.new_storage());
                }
            }
            let decisions = transaction.coordinator_decisions()?;
            let mut winners = Vec::new();
            for (position, plan) in materialized.intent.tables.iter().enumerate() {
                let Some(replacement) = plan.replacement() else {
                    continue;
                };
                let reference =
                    materialized
                        .reference
                        .as_ref()
                        .ok_or(SchemaMutationError::Corrupt(
                            "rewrite schema reference absent",
                        ))?;
                let reservation =
                    schema_index_physical_reservation(&materialized.intent, replacement, None);
                promote(&materialized.logical.catalog, &reservation, reference)?;
                crash(&format!("composition-after-promotion-{}", position + 1));
                let final_path = file::resolve(
                    &materialized.logical.catalog,
                    &final_locator(
                        &materialized.logical.catalog,
                        replacement.target.incarnation,
                        replacement.new_storage(),
                    )?,
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
            crash("composition-final-heaps-synced");
            for (position, plan) in materialized.intent.tables.iter().enumerate() {
                if let Some(replacement) = plan.replacement() {
                    materialized
                        .logical
                        .journal
                        .borrow_mut()
                        .retire_composition_table(
                            materialized.intent.transaction,
                            replacement.table(),
                        )?;
                    crash(&format!("composition-after-retirement-{}", position + 1));
                }
            }
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
            if let Some(replacement) = materialized
                .intent
                .tables
                .iter()
                .find_map(SchemaIndexTablePlan::replacement)
            {
                cleanup_prepared(
                    &materialized.logical.catalog,
                    &schema_index_physical_reservation(
                        &materialized.intent,
                        replacement,
                        Some(true),
                    ),
                    materialized.logical.base.incarnation,
                )?;
            }
            Ok((published, revision, winners))
        })();
        let (published, revision, winners) = match completion {
            Ok(completion) => completion,
            Err(error) => {
                transaction.schema_composition =
                    SchemaCompositionState::MaterializedIndex(materialized);
                return Err(error);
            }
        };
        crash("composition-before-memory-publication");
        let mut predecessors = Vec::with_capacity(winners.len());
        for (table, old_storage, storage) in winners {
            let new_storage = storage.storage_id();
            predecessors.push(self.registry.publish_replaced(old_storage, storage).ok_or(
                SchemaMutationError::Corrupt("composition source disappeared"),
            )?);
            self.bindings.publish_replaced(table, new_storage);
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
    intent: &SchemaIndexChangeSetIntent,
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
            snapshot_digest: intent.snapshot_digest.unwrap_or([0; 32]),
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
        SchemaCompositionState::SealingAndMaterializing(mut materialized)
        | SchemaCompositionState::Materialized(mut materialized)
        | SchemaCompositionState::RollbackRequiredMaterialized(mut materialized) => {
            materialized.staged.clear();
            let intent = materialized.intent.clone();
            (materialized.logical, Some(intent))
        }
        SchemaCompositionState::SealingAndMaterializingIndex(mut materialized)
        | SchemaCompositionState::MaterializedIndex(mut materialized)
        | SchemaCompositionState::RollbackRequiredMaterializedIndex(mut materialized) => {
            materialized.staged.clear();
            let replacements = materialized
                .intent
                .tables
                .iter()
                .filter_map(SchemaIndexTablePlan::replacement)
                .cloned()
                .collect::<Vec<_>>();
            let transaction = materialized.intent.transaction;
            let snapshot_digest = materialized.intent.snapshot_digest;
            let intent = (!replacements.is_empty()).then(|| SchemaChangeSetIntent {
                transaction,
                base_generation: materialized.intent.base_generation,
                target_generation: materialized
                    .intent
                    .target_generation
                    .unwrap_or(materialized.intent.base_generation),
                base_epoch: materialized.intent.base_epoch,
                target_epoch: materialized
                    .intent
                    .target_epoch
                    .unwrap_or(materialized.intent.base_epoch),
                action_count: materialized.intent.action_count,
                action_digest: materialized.intent.action_digest,
                snapshot_digest: snapshot_digest.unwrap_or([0; 32]),
                tables: replacements,
            });
            (materialized.logical, intent)
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
