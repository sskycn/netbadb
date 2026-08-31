//! Current external-schema lifecycle baselines for the Round 16 audit.
//! These are not assertions that a persistent runtime schema catalog exists.

use std::path::{Path, PathBuf};

use netbadb_core::{Database, DatabaseError, TableStorageCreateSpec, TableStorageOpenSpec};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_storage::{HeapStorage, LsmStorage, StorageError};
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, StorageId, TableId};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(case: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("netbadb-round16-{case}-{}", std::process::id()));
        // Never adopt or delete a pre-existing directory on setup failure.
        std::fs::create_dir(&path).expect("create exclusive test directory");
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![
            ColumnDef::new(
                ColumnId(42),
                "id",
                TypeSpec::Semantic {
                    name: "RecordId".into(),
                    physical: PhysicalType::Int64,
                },
            ),
            ColumnDef::new(ColumnId(7), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
        ],
    )
}

fn assert_heap_identity(path: &Path, table: &TableDef, storage_id: StorageId) {
    let identity = HeapStorage::inspect_identity(path).expect("read persisted identity");
    assert_eq!(identity.table_id, table.id);
    assert_eq!(identity.storage_id, storage_id);
    assert_eq!(identity.schema_fingerprint, table.fingerprint().unwrap());
}

#[test]
fn sparse_column_ids_survive_dml_index_and_reopen() {
    let root = TestDirectory::new("sparse");
    let path = root.path("physical.ndb");
    let table = table(91, "records");
    let mut database = Database::create(&path, table.clone()).unwrap();
    database
        .execute("INSERT INTO records (name, id) VALUES ('Ada', 1)")
        .unwrap();
    database.create_index(table.id, ColumnId(7)).unwrap();
    database
        .execute("UPDATE records SET name = 'Lin' WHERE name = 'Ada'")
        .unwrap();
    database.close().unwrap();

    assert_heap_identity(&path, &table, StorageId(1));
    let mut reopened = Database::open(&path, table.clone()).unwrap();
    assert_eq!(reopened.schema().tables(), &[table]);
    let catalog = reopened.inspect_catalog().unwrap();
    assert_eq!(catalog.tables[0].columns[0].column_id, ColumnId(42));
    assert_eq!(catalog.tables[0].columns[1].column_id, ColumnId(7));
    assert_eq!(catalog.tables[0].indexes[0].column_id, ColumnId(7));
    assert_eq!(
        reopened
            .query("SELECT name, id FROM records WHERE name = 'Lin'")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Text("Lin".into()), ScalarValue::Int64(1)]]
    );
    reopened.close().unwrap();
}

#[test]
fn reopen_rejects_changed_table_identity_and_column_shape() {
    let root = TestDirectory::new("mismatch");
    let path = root.path("records.ndb");
    let baseline = table(91, "records");
    Database::create(&path, baseline.clone())
        .unwrap()
        .close()
        .unwrap();

    let mut wrong_id = baseline.clone();
    wrong_id.id = TableId(92);
    assert!(matches!(
        Database::open(&path, wrong_id),
        Err(DatabaseError::Storage(StorageError::TableIdMismatch {
            expected: TableId(92),
            actual: TableId(91),
        }))
    ));

    let mut variants = Vec::new();
    let mut renamed = baseline.clone();
    renamed.name = "renamed".into();
    variants.push(renamed);
    let mut reordered = baseline.clone();
    reordered.columns.swap(0, 1);
    variants.push(reordered);
    let mut renumbered = baseline.clone();
    renumbered.columns[1].id = ColumnId(8);
    variants.push(renumbered);
    let mut nominal = baseline.clone();
    nominal.columns[0].type_spec = TypeSpec::Semantic {
        name: "OtherId".into(),
        physical: PhysicalType::Int64,
    };
    variants.push(nominal);
    let mut physical = baseline.clone();
    physical.columns[0].type_spec = TypeSpec::Physical(PhysicalType::UInt64);
    variants.push(physical);
    let mut nullable = baseline.clone();
    nullable.columns[1].nullable = false;
    variants.push(nullable);
    let mut key = baseline.clone();
    key.columns[0].primary_key = true;
    variants.push(key);
    let mut removed = baseline.clone();
    removed.columns.pop();
    variants.push(removed);

    let fingerprint = baseline.fingerprint().unwrap();
    for variant in variants {
        let changed_fingerprint = variant.fingerprint().unwrap();
        assert_ne!(changed_fingerprint, fingerprint);
        assert!(matches!(
            Database::open(&path, variant),
            Err(DatabaseError::Storage(StorageError::SchemaMismatch { expected, actual }))
                if expected == changed_fingerprint && actual == fingerprint
        ));
    }
    // Failed expectation checks must not rewrite the physical table identity.
    assert_heap_identity(&path, &baseline, StorageId(1));
    Database::open(&path, baseline).unwrap().close().unwrap();
}

#[test]
fn reordered_external_schema_rebuilds_bindings_without_reassigning_storage_ids() {
    let root = TestDirectory::new("reorder");
    let alpha = table(900, "alpha");
    let beta = table(100, "beta");
    let alpha_path = root.path("first.ndb");
    let beta_path = root.path("second.ndb");
    let mut database = Database::create_tables(vec![
        (alpha_path.clone(), alpha.clone()),
        (beta_path.clone(), beta.clone()),
    ])
    .unwrap();
    database
        .execute("INSERT INTO alpha (id, name) VALUES (1, 'alpha row')")
        .unwrap();
    database
        .execute("INSERT INTO beta (id, name) VALUES (2, 'beta row')")
        .unwrap();
    database.close().unwrap();
    assert_heap_identity(&alpha_path, &alpha, StorageId(1));
    assert_heap_identity(&beta_path, &beta, StorageId(2));

    let mut reopened = Database::open_tables(vec![
        (beta_path.clone(), beta.clone()),
        (alpha_path.clone(), alpha.clone()),
    ])
    .unwrap();
    assert_eq!(reopened.schema().tables(), &[beta.clone(), alpha.clone()]);
    assert_eq!(
        reopened
            .query("SELECT a.name, b.name FROM alpha a JOIN beta b ON a.id < b.id")
            .unwrap()
            .rows,
        vec![vec![
            ScalarValue::Text("alpha row".into()),
            ScalarValue::Text("beta row".into()),
        ]]
    );
    reopened.close().unwrap();
    assert_heap_identity(&alpha_path, &alpha, StorageId(1));
    assert_heap_identity(&beta_path, &beta, StorageId(2));
}

#[test]
fn ordinary_reopen_only_exposes_the_declared_table_subset() {
    let root = TestDirectory::new("subset");
    let alpha = table(900, "alpha");
    let beta = table(100, "beta");
    let entries = vec![
        (root.path("alpha.ndb"), alpha.clone()),
        (root.path("beta.ndb"), beta.clone()),
    ];
    Database::create_tables(entries.clone())
        .unwrap()
        .close()
        .unwrap();

    // There is no database-level persisted complete schema in this legacy API.
    let subset = Database::open_tables(vec![entries[1].clone()]).unwrap();
    assert_eq!(subset.schema().tables(), &[beta]);
    assert_eq!(subset.inspect_catalog().unwrap().tables.len(), 1);
    assert!(
        subset
            .prepare_statement("SELECT name FROM alpha", &[])
            .is_err()
    );
    subset.close().unwrap();
    assert_heap_identity(&entries[0].0, &alpha, StorageId(1));
    let complete = Database::open_tables(entries).unwrap();
    assert_eq!(complete.inspect_catalog().unwrap().tables.len(), 2);
    complete.close().unwrap();
}

#[test]
fn lsm_persists_identity_but_still_requires_external_schema() {
    let root = TestDirectory::new("lsm");
    let heap = table(900, "heap_records");
    let lsm = table(100, "lsm_records");
    let heap_path = root.path("heap.ndb");
    let lsm_path = root.path("lsm");
    let mut database = Database::create_storages(vec![
        TableStorageCreateSpec::heap(&heap_path, heap.clone()),
        TableStorageCreateSpec::lsm(&lsm_path, lsm.clone(), ColumnId(42)),
    ])
    .unwrap();
    database
        .execute("INSERT INTO lsm_records (id, name) VALUES (3, NULL)")
        .unwrap();
    database.close().unwrap();
    let identity = LsmStorage::inspect_identity(&lsm_path).unwrap();
    assert_eq!(identity.table_id, lsm.id);
    assert_eq!(identity.storage_id, StorageId(2));
    assert_eq!(identity.schema_fingerprint, lsm.fingerprint().unwrap());

    let mut changed = lsm.clone();
    changed.columns[0].type_spec = TypeSpec::Physical(PhysicalType::Int64);
    assert!(matches!(
        Database::open_storages(vec![TableStorageOpenSpec::lsm(&lsm_path, changed)]),
        Err(DatabaseError::Storage(StorageError::SchemaMismatch { .. }))
    ));
    let mut reopened = Database::open_storages(vec![
        TableStorageOpenSpec::lsm(&lsm_path, lsm),
        TableStorageOpenSpec::heap(&heap_path, heap),
    ])
    .unwrap();
    assert_eq!(
        reopened
            .query("SELECT id, name FROM lsm_records")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Int64(3), ScalarValue::Null]]
    );
    reopened.close().unwrap();
    assert_eq!(
        LsmStorage::inspect_identity(&lsm_path).unwrap().storage_id,
        StorageId(2)
    );
}

#[test]
fn catalog_generation_resets_while_index_definition_and_table_fingerprint_survive() {
    let root = TestDirectory::new("generation");
    let path = root.path("records.ndb");
    let table = table(91, "records");
    let fingerprint = table.fingerprint().unwrap();
    let mut database = Database::create(&path, table.clone()).unwrap();
    assert_eq!(database.catalog_generation(), 0);
    database.create_index(table.id, ColumnId(7)).unwrap();
    // Legacy anonymous creation does not notify catalog-generation observers.
    assert_eq!(database.catalog_generation(), 0);
    let named = database
        .prepare_ddl_statement("CREATE INDEX records_id_idx ON records (id)")
        .unwrap();
    database.execute_ddl(&named).unwrap();
    assert_eq!(database.catalog_generation(), 1);
    assert_eq!(
        database.inspect_catalog().unwrap().tables[0].fingerprint,
        fingerprint
    );
    database.close().unwrap();

    let reopened = Database::open(&path, table).unwrap();
    assert_eq!(reopened.catalog_generation(), 0);
    let inspected = reopened.inspect_catalog().unwrap();
    assert_eq!(inspected.tables[0].fingerprint, fingerprint);
    assert_eq!(inspected.tables[0].indexes.len(), 2);
    assert_eq!(inspected.tables[0].indexes[0].column_id, ColumnId(7));
    reopened.close().unwrap();
}

#[test]
fn heap_without_primary_key_accepts_duplicate_values_across_reopen() {
    let root = TestDirectory::new("no-pk");
    let path = root.path("records.ndb");
    let table = table(91, "records");
    assert!(table.columns.iter().all(|column| !column.primary_key));
    let mut database = Database::create(&path, table.clone()).unwrap();
    for _ in 0..2 {
        database
            .execute("INSERT INTO records (id, name) VALUES (1, NULL)")
            .unwrap();
    }
    database.close().unwrap();
    let mut reopened = Database::open(&path, table).unwrap();
    assert_eq!(
        reopened.query("SELECT id FROM records").unwrap().rows.len(),
        2
    );
    reopened.close().unwrap();
}
