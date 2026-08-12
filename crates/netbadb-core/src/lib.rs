//! Native synchronous embedded API for NetbaDB.

use std::error::Error;
use std::fmt;
use std::path::Path;

use netbadb_compiler::{CompileError, compile};
use netbadb_executor::{ExecutionError, QueryResult, execute};
use netbadb_schema::{Schema, TableDef};
use netbadb_storage::{HeapStorage, StorageError};
use netbadb_types::ScalarValue;

pub use netbadb_storage::{Transaction, TransactionState};

#[derive(Debug)]
pub enum DatabaseError {
    Compile(CompileError),
    Storage(StorageError),
    Execution(ExecutionError),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
        }
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Compile(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Execution(error) => Some(error),
        }
    }
}

impl From<CompileError> for DatabaseError {
    fn from(error: CompileError) -> Self {
        Self::Compile(error)
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
    storage: HeapStorage,
}

impl Database {
    pub fn create(path: impl AsRef<Path>, table: TableDef) -> Result<Self, DatabaseError> {
        let storage = HeapStorage::create(path, table.clone())?;
        Ok(Self {
            schema: Schema::new(vec![table]),
            storage,
        })
    }

    pub fn open(path: impl AsRef<Path>, table: TableDef) -> Result<Self, DatabaseError> {
        let storage = HeapStorage::open(path, table.clone())?;
        Ok(Self {
            schema: Schema::new(vec![table]),
            storage,
        })
    }

    pub fn insert(&mut self, values: &[ScalarValue]) -> Result<(), DatabaseError> {
        self.storage.insert(values)?;
        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction, DatabaseError> {
        Ok(self.storage.begin_transaction()?)
    }

    pub fn insert_in(
        &mut self,
        transaction: &mut Transaction,
        values: &[ScalarValue],
    ) -> Result<(), DatabaseError> {
        self.storage.insert_in(transaction, values)?;
        Ok(())
    }

    /// Flushes dirty pages and reports any write or sync failure.
    pub fn flush(&self) -> Result<(), DatabaseError> {
        self.storage.flush()?;
        Ok(())
    }

    /// Explicitly closes the embedded database after flushing dirty pages.
    pub fn close(self) -> Result<(), DatabaseError> {
        self.storage.close()?;
        Ok(())
    }

    pub fn query(&mut self, source: &str) -> Result<QueryResult, DatabaseError> {
        let compiled = compile(&self.schema, source)?;
        let physical = netbadb_planner::plan(&compiled.logical_plan);
        Ok(execute(&physical, &mut self.storage)?)
    }

    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
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
}
