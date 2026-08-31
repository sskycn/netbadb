//! Materialized transaction schema views and statement dependency validation.
use crate::{
    Database, DatabaseError, PreparedStatement, SchemaDependency, SchemaMutationError, Transaction,
};
use netbadb_types::{PhysicalType, ScalarValue, StorageId, TableId};
use std::rc::Rc;

impl Database {
    /// Prepare against this transaction's private schema. The returned statement
    /// is scoped to this exact transaction handle; it cannot execute globally or
    /// in another transaction, including after commit/rollback.
    pub fn prepare_statement_in(
        &self,
        transaction: &Transaction,
        source: &str,
        declared: &[Option<PhysicalType>],
    ) -> Result<PreparedStatement, DatabaseError> {
        self.validate_transaction(transaction)?;
        let compiled = netbadb_compiler::compile_statement_with_parameters(
            transaction.visible_schema(&self.committed.schema),
            source,
            declared,
        )?;
        let dependencies = self.statement_dependencies(&compiled, Some(transaction))?;
        Ok(PreparedStatement {
            compiled,
            dependencies,
            scope: Some(Rc::downgrade(&transaction.preparation_scope)),
        })
    }

    pub(crate) fn statement_dependencies(
        &self,
        compiled: &netbadb_compiler::CompiledStatement,
        transaction: Option<&Transaction>,
    ) -> Result<Vec<SchemaDependency>, DatabaseError> {
        let committed = transaction
            .and_then(|t| t.schema_mutation.as_ref())
            .filter(|m| m.staged.is_some())
            .map_or(&self.committed, |m| &m.target.committed);
        let mut dependencies = Vec::new();
        for id in compiled
            .logical_statement
            .read_tables()
            .into_iter()
            .chain(compiled.logical_statement.write_tables())
        {
            if dependencies
                .iter()
                .any(|d: &SchemaDependency| d.table_id == id)
            {
                continue;
            }
            let table = committed
                .schema
                .tables()
                .iter()
                .find(|t| t.id == id)
                .ok_or(SchemaMutationError::StalePreparedStatement)?;
            let lineage = committed
                .tables
                .iter()
                .find(|t| t.table_id == id)
                .ok_or(SchemaMutationError::StalePreparedStatement)?;
            dependencies.push(SchemaDependency {
                table_id: id,
                table_version: lineage.version,
                fingerprint: table.fingerprint()?,
            });
        }
        Ok(dependencies)
    }

    pub(crate) fn validate_prepared_dependencies(
        &self,
        prepared: &PreparedStatement,
        transaction: Option<&Transaction>,
    ) -> Result<(), DatabaseError> {
        if let Some(scope) = &prepared.scope {
            let owner = scope
                .upgrade()
                .ok_or(SchemaMutationError::StalePreparedStatement)?;
            if !transaction.is_some_and(|t| Rc::ptr_eq(&owner, &t.preparation_scope)) {
                return Err(SchemaMutationError::StalePreparedStatement.into());
            }
        }
        let current = self.statement_dependencies(&prepared.compiled, transaction)?;
        if current != prepared.dependencies {
            return Err(SchemaMutationError::StalePreparedStatement.into());
        }
        Ok(())
    }

    pub(crate) fn single_storage_in(
        &self,
        table: TableId,
        transaction: &Transaction,
    ) -> Result<Option<StorageId>, DatabaseError> {
        if let Some(id) = transaction.staged_binding(table) {
            return Ok(Some(id));
        }
        Ok(match self.bindings.placement(table)? {
            crate::TablePlacement::Single { storage_id, .. } => Some(*storage_id),
            _ => None,
        })
    }
    pub(crate) fn storage_ids_for_tables_in(
        &self,
        tables: Vec<TableId>,
        transaction: &Transaction,
    ) -> Result<Vec<StorageId>, DatabaseError> {
        let mut ids = Vec::new();
        for table in tables {
            if let Some(id) = transaction.staged_binding(table) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            } else {
                for id in self.bindings.placement(table)?.storage_ids() {
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
            }
        }
        Ok(ids)
    }
    pub(crate) fn route_storage_for_values_in(
        &self,
        table: TableId,
        values: &[ScalarValue],
        transaction: &Transaction,
    ) -> Result<StorageId, DatabaseError> {
        match transaction.staged_binding(table) {
            Some(id) => Ok(id),
            None => self.route_storage_for_values(table, values),
        }
    }
}
