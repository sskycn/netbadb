//! Native synchronous embedded API for NetbaDB.

use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use netbadb_compiler::{CompileError, compile_statement};
use netbadb_executor::{ExecutionError, execute_statement, execute_with_storages};
use netbadb_planner::{
    IndexAccessPath, PhysicalStatement, TableAccessStatistics, plan_statement_with_statistics,
};
use netbadb_schema::{Schema, SchemaError, TableDef};
use netbadb_storage::{HeapStorage, StorageError, TransactionError};
use netbadb_types::{ColumnId, ScalarValue, TableId};

pub use netbadb_executor::{ExecutionResult, QueryResult, ResultColumn};
pub use netbadb_storage::{
    IndexDefinition, IndexStatistics, TableStatistics, Transaction, TransactionState,
};

/// Canonical table identities read or written by one successfully compiled SQL
/// statement. This exposes no syntax, compiler IR, plan, or storage details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementAccess {
    read_tables: Vec<TableId>,
    write_tables: Vec<TableId>,
}

impl StatementAccess {
    #[must_use]
    pub fn read_tables(&self) -> &[TableId] {
        &self.read_tables
    }

    #[must_use]
    pub fn write_tables(&self) -> &[TableId] {
        &self.write_tables
    }
}

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
    /// Subsequent heap and SQL DML maintain the index in the same transaction.
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

    /// Persists a fresh optimizer snapshot for one table and all of its
    /// registered indexes. DML does not maintain this snapshot automatically.
    pub fn analyze(&mut self, table_id: TableId) -> Result<(), DatabaseError> {
        self.storage_mut(table_id)?.analyze()?;
        Ok(())
    }

    /// Explicitly closes the embedded database after flushing dirty pages.
    pub fn close(self) -> Result<(), DatabaseError> {
        for storage in self.storages {
            storage.close()?;
        }
        Ok(())
    }

    pub fn query(&mut self, source: &str) -> Result<QueryResult, DatabaseError> {
        let physical = self.plan_source(source)?;
        let PhysicalStatement::Query(plan) = physical else {
            return Err(DatabaseError::ExpectedQuery);
        };
        Ok(execute_with_storages(&plan, &mut self.storages)?)
    }

    /// Compiles SQL and reports its canonical table access without planning,
    /// reading rows, starting a transaction, or touching persistent state.
    pub fn statement_access(&self, source: &str) -> Result<StatementAccess, DatabaseError> {
        let compiled = compile_statement(&self.schema, source)?;
        Ok(StatementAccess {
            read_tables: compiled.logical_statement.read_tables(),
            write_tables: compiled.logical_statement.write_tables(),
        })
    }

    /// Executes SELECT or one typed DML statement. DML runs in one implicit
    /// transaction and returns an explicit affected-row count.
    pub fn execute(&mut self, source: &str) -> Result<ExecutionResult, DatabaseError> {
        let physical = self.plan_source(source)?;
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
        let physical = self.plan_source(source)?;
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

    fn plan_source(&self, source: &str) -> Result<PhysicalStatement, DatabaseError> {
        let compiled = compile_statement(&self.schema, source)?;
        let table_statistics = self.planner_table_statistics();
        let access_paths = self.planner_access_paths();
        Ok(plan_statement_with_statistics(
            &compiled.logical_statement,
            &table_statistics,
            &access_paths,
        ))
    }

    fn planner_table_statistics(&self) -> Vec<TableAccessStatistics> {
        self.storages
            .iter()
            .map(|storage| TableAccessStatistics {
                table_id: storage.table().id,
                statistics: storage.table_statistics(),
            })
            .collect()
    }

    fn planner_access_paths(&self) -> Vec<IndexAccessPath> {
        self.storages
            .iter()
            .flat_map(|storage| {
                let table_id = storage.table().id;
                storage
                    .indexes()
                    .iter()
                    .map(move |definition| IndexAccessPath {
                        table_id,
                        column_id: definition.column_id,
                        handle: definition.handle,
                        statistics: storage.index_statistics(definition.column_id),
                    })
            })
            .collect()
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
    use super::{Database, DatabaseError, ExecutionResult, PhysicalStatement, TransactionState};
    use netbadb_planner::PhysicalPlan;
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PageId, PhysicalType, ScalarValue, TableId};

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

    fn planned_index(plan: &PhysicalPlan) -> Option<(PageId, &ScalarValue)> {
        match plan {
            PhysicalPlan::IndexScan { handle, key, .. } => Some((handle.meta_page, key)),
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Aggregate { input, .. }
            | PhysicalPlan::Limit { input, .. } => planned_index(input),
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                planned_index(left).or_else(|| planned_index(right))
            }
            PhysicalPlan::SeqScan { .. } => None,
        }
    }

    fn planned_statement_index(statement: &PhysicalStatement) -> Option<(PageId, &ScalarValue)> {
        match statement {
            PhysicalStatement::Query(plan)
            | PhysicalStatement::Update { input: plan, .. }
            | PhysicalStatement::Delete { input: plan, .. } => planned_index(plan),
            PhysicalStatement::Insert { .. } => None,
        }
    }

    fn affected(result: ExecutionResult) -> u64 {
        match result {
            ExecutionResult::AffectedRows(rows) => rows,
            ExecutionResult::Query(_) => panic!("expected affected rows"),
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
        let inserted = database
            .storage_mut(TableId(1))
            .unwrap()
            .btree()
            .lookup(definition.handle, &ScalarValue::Text("Ada".into()))
            .expect("lookup inserted index entry");
        assert_eq!(inserted.len(), 1);

        database
            .execute("UPDATE users SET name = 'Grace' WHERE id = 1")
            .expect("SQL update");
        assert!(
            database
                .storage_mut(TableId(1))
                .unwrap()
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("Ada".into()))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            database
                .storage_mut(TableId(1))
                .unwrap()
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("Grace".into()))
                .unwrap(),
            inserted
        );

        database
            .execute("DELETE FROM users WHERE id = 1")
            .expect("SQL delete");
        assert!(
            database
                .storage_mut(TableId(1))
                .unwrap()
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("Grace".into()))
                .unwrap()
                .is_empty()
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
            database
                .storage_mut(TableId(1))
                .unwrap()
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
            database
                .storage_mut(TableId(1))
                .unwrap()
                .btree()
                .lookup(definition.handle, &ScalarValue::Text("shared".into()))
                .unwrap()
                .is_empty()
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
            Some((team.handle.meta_page, &ScalarValue::Int64(10)))
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
            Some((team.handle.meta_page, &ScalarValue::Int64(10)))
        );
        assert_ne!(team.handle, name.handle);

        let is_null = database
            .plan_source("SELECT id FROM members WHERE team_id IS NULL")
            .expect("plan IS NULL");
        assert_eq!(
            planned_statement_index(&is_null),
            Some((team.handle.meta_page, &ScalarValue::Null))
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
            Some((team.handle.meta_page, &ScalarValue::Int64(10)))
        );
        assert_eq!(
            affected(
                database
                    .execute("UPDATE members SET team_id = 20 WHERE team_id = 10")
                    .expect("execute self-index update")
            ),
            3
        );
        assert!(
            database
                .storage_mut(TableId(9))
                .unwrap()
                .btree()
                .lookup(team.handle, &ScalarValue::Int64(10))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            database
                .storage_mut(TableId(9))
                .unwrap()
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
            Some((team.handle.meta_page, &ScalarValue::Int64(10)))
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
            database
                .storage_mut(TableId(9))
                .unwrap()
                .btree()
                .lookup(team.handle, &ScalarValue::Int64(10))
                .unwrap()
                .is_empty()
        );

        database.close().expect("close indexed database");
        let mut reopened = Database::open(&path, schema).expect("reopen indexed database");
        let reopened_plan = reopened
            .plan_source("SELECT id FROM members WHERE name = 'Ada'")
            .expect("plan reopened text lookup");
        assert_eq!(
            planned_statement_index(&reopened_plan),
            Some((name.handle.meta_page, &ScalarValue::Text("Ada".into())))
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
            Some((team.handle.meta_page, &ScalarValue::Int64(0)))
        );
        database.analyze(TableId(9)).expect("analyze table");
        assert_eq!(
            planned_statement_index(&database.plan_source(source).expect("costed plan")),
            Some((id.handle.meta_page, &ScalarValue::Int64(42)))
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
            Some((id.handle.meta_page, &ScalarValue::Int64(1)))
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
}
