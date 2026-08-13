//! Native synchronous embedded API for NetbaDB.

use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use netbadb_compiler::{CompileError, compile_statement};
use netbadb_executor::{ExecutionError, execute_statement, execute_with_storages};
use netbadb_planner::PhysicalStatement;
use netbadb_schema::{Schema, SchemaError, TableDef};
use netbadb_storage::{HeapStorage, StorageError, TransactionError};
use netbadb_types::{ColumnId, ScalarValue, TableId};

pub use netbadb_executor::{ExecutionResult, QueryResult, ResultColumn};
pub use netbadb_storage::{IndexDefinition, Transaction, TransactionState};

#[derive(Debug)]
pub enum DatabaseError {
    Compile(CompileError),
    Schema(SchemaError),
    Storage(StorageError),
    Execution(ExecutionError),
    ExpectedQuery,
    EmptyCatalog,
    TableSelectionRequired,
    DuplicateStoragePath(PathBuf),
    CreateTablesRollback {
        creation: StorageError,
        cleanup_path: PathBuf,
        cleanup: std::io::Error,
    },
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(error) => error.fmt(formatter),
            Self::Schema(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
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
            Self::Schema(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Execution(error) => Some(error),
            Self::CreateTablesRollback { creation, .. } => Some(creation),
            Self::ExpectedQuery
            | Self::EmptyCatalog
            | Self::TableSelectionRequired
            | Self::DuplicateStoragePath(_) => None,
        }
    }
}

impl From<CompileError> for DatabaseError {
    fn from(error: CompileError) -> Self {
        Self::Compile(error)
    }
}

impl From<SchemaError> for DatabaseError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<StorageError> for DatabaseError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<ExecutionError> for DatabaseError {
    fn from(error: ExecutionError) -> Self {
        Self::Execution(error)
    }
}

pub struct Database {
    schema: Schema,
    storages: Vec<HeapStorage>,
}

impl Database {
    pub fn create(path: impl AsRef<Path>, table: TableDef) -> Result<Self, DatabaseError> {
        let schema = Schema::new(vec![table.clone()])?;
        let storage = HeapStorage::create(path, table)?;
        Ok(Self {
            schema,
            storages: vec![storage],
        })
    }

    pub fn open(path: impl AsRef<Path>, table: TableDef) -> Result<Self, DatabaseError> {
        let schema = Schema::new(vec![table.clone()])?;
        let storage = HeapStorage::open(path, table)?;
        Ok(Self {
            schema,
            storages: vec![storage],
        })
    }

    /// Creates one heap file per validated table and composes them into one
    /// query catalog. Each heap persists the table's schema fingerprint.
    pub fn create_tables(tables: Vec<(PathBuf, TableDef)>) -> Result<Self, DatabaseError> {
        validate_catalog_paths(&tables)?;
        let schema = Schema::new(tables.iter().map(|(_, table)| table.clone()).collect())?;
        let mut storages = Vec::with_capacity(tables.len());
        let mut created_paths = Vec::with_capacity(tables.len());
        for (path, table) in tables {
            match HeapStorage::create(&path, table) {
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
        Ok(Self { schema, storages })
    }

    /// Opens one existing heap-format file per table as a single query catalog.
    pub fn open_tables(tables: Vec<(PathBuf, TableDef)>) -> Result<Self, DatabaseError> {
        validate_catalog_paths(&tables)?;
        let schema = Schema::new(tables.iter().map(|(_, table)| table.clone()).collect())?;
        let mut storages = Vec::with_capacity(tables.len());
        for (path, table) in tables {
            storages.push(HeapStorage::open(path, table)?);
        }
        Ok(Self { schema, storages })
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<(), DatabaseError> {
        self.primary_storage_mut()?.insert(values)?;
        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction, DatabaseError> {
        Ok(self.primary_storage_mut()?.begin_transaction()?)
    }

    /// Begins a transaction owned by the heap for `table_id`.
    ///
    /// The returned handle is valid only for writes to that same table through
    /// [`Self::insert_into_in`] or [`Self::execute_in`]. A transaction cannot
    /// span multiple table heaps; attempting to use it with another table
    /// returns a foreign-transaction error without rolling back its owner.
    pub fn begin_transaction_for(
        &mut self,
        table_id: TableId,
    ) -> Result<Transaction, DatabaseError> {
        Ok(self.storage_mut(table_id)?.begin_transaction()?)
    }

    pub fn insert_in(
        &mut self,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<(), DatabaseError> {
        self.primary_storage_mut()?.insert_in(transaction, values)?;
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
        self.storage_mut(table_id)?.insert(values)?;
        Ok(())
    }

    /// Inserts into `table_id` using a transaction created by
    /// [`Self::begin_transaction_for`] for that same table.
    ///
    /// Cross-table use is rejected as a foreign transaction. NetbaDB does not
    /// currently provide atomic write transactions spanning multiple heaps.
    pub fn insert_into_in(
        &mut self,
        table_id: TableId,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<(), DatabaseError> {
        self.storage_mut(table_id)?.insert_in(transaction, values)?;
        Ok(())
    }

    /// Flushes dirty pages and reports any write or sync failure.
    pub fn flush(&self) -> Result<(), DatabaseError> {
        for storage in &self.storages {
            storage.flush()?;
        }
        Ok(())
    }

    /// Creates a quiescent checkpoint and recycles the previous WAL history.
    pub fn checkpoint(&mut self) -> Result<(), DatabaseError> {
        for storage in &mut self.storages {
            storage.checkpoint()?;
        }
        Ok(())
    }

    /// Atomically backfills and registers one non-unique single-column index.
    /// Heap DML performed after this call is not maintained until Phase 4D2.
    pub fn create_index(
        &mut self,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Result<IndexDefinition, DatabaseError> {
        Ok(self.storage_mut(table_id)?.create_index(column_id)?)
    }

    /// Returns this table's registered indexes in persistent creation order.
    pub fn indexes(&self, table_id: TableId) -> Result<&[IndexDefinition], DatabaseError> {
        Ok(self.storage(table_id)?.indexes())
    }

    /// Explicitly closes the embedded database after flushing dirty pages.
    pub fn close(self) -> Result<(), DatabaseError> {
        for storage in self.storages {
            storage.close()?;
        }
        Ok(())
    }

    pub fn query(&mut self, source: &str) -> Result<QueryResult, DatabaseError> {
        let compiled = compile_statement(&self.schema, source)?;
        let physical = netbadb_planner::plan_statement(&compiled.logical_statement);
        let PhysicalStatement::Query(plan) = physical else {
            return Err(DatabaseError::ExpectedQuery);
        };
        Ok(execute_with_storages(&plan, &mut self.storages)?)
    }

    /// Executes SELECT or one typed DML statement. DML runs in one implicit
    /// transaction and returns an explicit affected-row count.
    pub fn execute(&mut self, source: &str) -> Result<ExecutionResult, DatabaseError> {
        let compiled = compile_statement(&self.schema, source)?;
        let physical = netbadb_planner::plan_statement(&compiled.logical_statement);
        if let PhysicalStatement::Query(plan) = &physical {
            return Ok(ExecutionResult::Query(execute_with_storages(
                plan,
                &mut self.storages,
            )?));
        }

        let table_id = statement_table_id(&physical).ok_or(DatabaseError::ExpectedQuery)?;
        let storage = self.storage_mut(table_id)?;
        let mut transaction = storage.begin_transaction()?;
        match execute_statement(&physical, storage, Some(&mut transaction)) {
            Ok(result) => {
                transaction.commit()?;
                Ok(result)
            }
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error.into()),
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    /// Executes a statement using an existing transaction. Mutating statements
    /// must target the same table passed to [`Self::begin_transaction_for`].
    /// Until savepoints exist, an execution-time DML failure rolls back the
    /// whole transaction.
    pub fn execute_in(
        &mut self,
        transaction: &mut Transaction,
        source: &str,
    ) -> Result<ExecutionResult, DatabaseError> {
        // Reject inactive or foreign handles before compilation/execution. A
        // foreign handle must never be rolled back by this database object.
        self.validate_transaction(transaction)?;
        let compiled = compile_statement(&self.schema, source)?;
        let physical = netbadb_planner::plan_statement(&compiled.logical_statement);
        if let PhysicalStatement::Query(plan) = &physical {
            return Ok(ExecutionResult::Query(execute_with_storages(
                plan,
                &mut self.storages,
            )?));
        }
        let table_id = statement_table_id(&physical).ok_or(DatabaseError::ExpectedQuery)?;
        let storage = self.storage_mut(table_id)?;
        storage.validate_transaction(transaction)?;
        match execute_statement(&physical, storage, Some(transaction)) {
            Ok(result) => Ok(result),
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error.into()),
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn primary_storage_mut(&mut self) -> Result<&mut HeapStorage, DatabaseError> {
        match self.storages.as_mut_slice() {
            [] => Err(DatabaseError::EmptyCatalog),
            [storage] => Ok(storage),
            [_, ..] => Err(DatabaseError::TableSelectionRequired),
        }
    }

    fn storage_mut(&mut self, table_id: TableId) -> Result<&mut HeapStorage, DatabaseError> {
        self.storages
            .iter_mut()
            .find(|storage| storage.table().id == table_id)
            .ok_or_else(|| ExecutionError::MissingTableStorage(table_id).into())
    }

    fn storage(&self, table_id: TableId) -> Result<&HeapStorage, DatabaseError> {
        self.storages
            .iter()
            .find(|storage| storage.table().id == table_id)
            .ok_or_else(|| ExecutionError::MissingTableStorage(table_id).into())
    }

    fn validate_transaction(&self, transaction: &Transaction) -> Result<(), DatabaseError> {
        let mut foreign = None;
        for storage in &self.storages {
            match storage.validate_transaction(transaction) {
                Ok(()) => return Ok(()),
                Err(StorageError::Transaction(TransactionError::ForeignTransaction { .. })) => {
                    foreign = Some(StorageError::Transaction(
                        TransactionError::ForeignTransaction {
                            txn_id: transaction.id(),
                        },
                    ));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(foreign.ok_or(DatabaseError::EmptyCatalog)?.into())
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

fn cleanup_created_table_files(paths: &[PathBuf]) -> Option<(PathBuf, std::io::Error)> {
    let mut first_error = None;
    for database_path in paths.iter().rev() {
        let wal_path = netbadb_storage::wal_path(database_path);
        let targets = [
            database_path.clone(),
            wal_path.clone(),
            netbadb_storage::wal_alternate_path(&wal_path),
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

#[cfg(test)]
mod tests {
    use super::{Database, TransactionState};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

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
}
