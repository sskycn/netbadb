use std::path::{Path, PathBuf};
use std::process::Command;

use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_types::{
    ColumnId, IndexName, PhysicalType, ScalarValue, SemanticType, StorageId, TableId, TxnId,
};

use crate::{
    CreateColumnSpec, CreateTableSpec, Database, DatabaseCoordinatorConfig, ExecutionResult,
    RetiredHeapGcState, RetiredTableResource, SchemaGeneration, SchemaMutationError,
    TableSchemaVersion, TableStorageCreateSpec, TransactionState,
};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round18-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).expect("fresh isolated test directory");
    path
}
fn old_table(id: u64, name: &str) -> TableDef {
    TableDef::new(
        TableId(id),
        name,
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    )
}
fn seed(path: &Path, coordinator: bool) -> Database {
    let specs = vec![
        TableStorageCreateSpec::heap(path.join("users.heap"), old_table(1, "users")),
        TableStorageCreateSpec::heap(path.join("teams.heap"), old_table(2, "teams")),
    ];
    let config = coordinator.then(|| DatabaseCoordinatorConfig::new(path.join("coordinator")));
    let mut db = Database::create_catalog(path.join("catalog"), specs, config).unwrap();
    db.execute("INSERT INTO users (id) VALUES (1)").unwrap();
    db.execute("INSERT INTO teams (id) VALUES (2)").unwrap();
    db
}
fn spec(name: &str) -> CreateTableSpec {
    CreateTableSpec::new(
        name,
        vec![
            CreateColumnSpec::new(
                "id",
                SemanticType::named("ProjectId", PhysicalType::Int64),
                false,
            ),
            CreateColumnSpec::new(
                "name",
                SemanticType::named("ProjectName", PhysicalType::Text),
                false,
            ),
            CreateColumnSpec::new("active", SemanticType::physical(PhysicalType::Bool), false),
            CreateColumnSpec::new("score", SemanticType::physical(PhysicalType::Int64), true),
            CreateColumnSpec::new("label", SemanticType::physical(PhysicalType::Text), true),
        ],
    )
}
fn rows(result: ExecutionResult) -> Vec<Vec<ScalarValue>> {
    match result {
        ExecutionResult::Query(q) => q.rows,
        _ => panic!("expected query"),
    }
}
fn insert(db: &mut Database, txn: &mut crate::Transaction) {
    let insert = db
        .prepare_statement_in(
            txn,
            "INSERT INTO projects (id, name, active, score, label) VALUES ($1, $2, $3, $4, $5)",
            &[],
        )
        .unwrap();
    assert_eq!(
        db.execute_prepared_in(
            txn,
            &insert,
            &[
                ScalarValue::Int64(10),
                ScalarValue::Text("demo".into()),
                ScalarValue::Bool(true),
                ScalarValue::Null,
                ScalarValue::Null
            ]
        )
        .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
}
fn expected() -> Vec<Vec<ScalarValue>> {
    vec![vec![
        ScalarValue::Int64(10),
        ScalarValue::Text("demo".into()),
        ScalarValue::Bool(true),
        ScalarValue::Null,
        ScalarValue::Null,
    ]]
}

#[test]
fn core_drop_exact_overlay_prepared_invalidation_retirement_and_reopen() {
    let root = root("drop-basic");
    let mut db = seed(&root, true);
    let stale_expectation = Schema::new(db.schema().tables().to_vec()).unwrap();
    let target = db.resolve_drop_table("users").unwrap();
    let old_users = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let unaffected = db.prepare_statement("SELECT id FROM teams", &[]).unwrap();
    let before_generation = db.schema_generation();
    let before_revision = db.catalog_generation();
    let before_table = db.next_table_id();
    let before_storage = db.next_storage_id();
    let before_partition = db.next_partition_id();
    let mut txn = db.begin_transaction().unwrap();
    let transaction_users = db
        .prepare_statement_in(&txn, "SELECT id FROM users", &[])
        .unwrap();
    db.execute_in(&mut txn, "UPDATE users SET id = 9").unwrap();
    db.drop_table_in(&mut txn, target).unwrap();
    assert!(
        db.prepare_statement_in(&txn, "SELECT id FROM users", &[])
            .is_err()
    );
    assert!(
        db.execute_prepared_in(&mut txn, &transaction_users, &[])
            .is_err()
    );
    assert!(db.schema().table("users").is_some());
    assert_eq!(db.inspect_catalog().unwrap().tables.len(), 2);
    assert!(db.inspect_retired_table_resources().is_empty());
    assert_eq!(db.schema_generation(), before_generation);
    assert_eq!(db.catalog_generation(), before_revision);
    assert_eq!(db.next_table_id(), before_table);
    assert_eq!(db.next_storage_id(), before_storage);
    assert_eq!(db.next_partition_id(), before_partition);
    db.commit_transaction(&mut txn).unwrap();
    assert_eq!(txn.state(), TransactionState::Committed);
    assert!(db.schema().table("users").is_none());
    assert_eq!(db.inspect_catalog().unwrap().tables.len(), 1);
    assert!(db.execute_prepared(&old_users, &[]).is_err());
    assert_eq!(
        rows(db.execute_prepared(&unaffected, &[]).unwrap()),
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    assert_eq!(db.catalog_generation(), before_revision + 1);
    assert_eq!(db.next_table_id(), before_table);
    assert_eq!(db.next_storage_id(), before_storage);
    assert_eq!(db.next_partition_id(), before_partition);
    let retired = db.inspect_retired_table_resources();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].table_id, TableId(1));
    assert_eq!(retired[0].storage_id, StorageId(1));
    assert_eq!(retired[0].table_version, TableSchemaVersion(1));
    assert_eq!(retired[0].fingerprint, target.fingerprint);
    assert_eq!(retired[0].retired_generation, SchemaGeneration(2));
    assert!(root.join("users.heap").is_file());
    drop(txn);
    db.close().unwrap();
    assert!(
        Database::open_catalog_with_expectation(root.join("catalog"), Some(&stale_expectation))
            .is_err()
    );
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(reopened.schema().table("users").is_none());
        assert_eq!(reopened.schema_generation(), SchemaGeneration(2));
        assert_eq!(reopened.next_table_id(), before_table);
        assert_eq!(reopened.next_storage_id(), before_storage);
        assert_eq!(reopened.inspect_retired_table_resources(), retired);
        assert_eq!(
            reopened.query("SELECT id FROM teams").unwrap().rows,
            vec![vec![ScalarValue::Int64(2)]]
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_dependencies_pin_table_version_and_fingerprint_independently() {
    let root = root("prepared-dependency-audit");
    let mut db = seed(&root, true);
    let users = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let teams = db.prepare_statement("SELECT id FROM teams", &[]).unwrap();
    assert_eq!(users.schema_dependencies().len(), 1);
    assert_eq!(users.schema_dependencies()[0].table_id, TableId(1));
    assert_eq!(
        users.schema_dependencies()[0].table_version,
        TableSchemaVersion(1)
    );
    assert_eq!(
        users.schema_dependencies()[0].fingerprint,
        db.schema().table("users").unwrap().fingerprint().unwrap()
    );

    db.committed
        .tables
        .iter_mut()
        .find(|lineage| lineage.table_id == TableId(1))
        .unwrap()
        .version = TableSchemaVersion(2);
    assert!(matches!(
        db.validate_prepared_dependencies(&users, None),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    db.validate_prepared_dependencies(&teams, None)
        .expect("an unrelated table dependency remains valid");

    db.committed
        .tables
        .iter_mut()
        .find(|lineage| lineage.table_id == TableId(1))
        .unwrap()
        .version = TableSchemaVersion(1);
    let mut renamed_users = db.schema().table("users").unwrap().clone();
    renamed_users.columns[0].name = "user_id".into();
    db.committed.schema = Schema::new(vec![renamed_users, old_table(2, "teams")]).unwrap();
    assert!(matches!(
        db.validate_prepared_dependencies(&users, None),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    db.validate_prepared_dependencies(&teams, None)
        .expect("an unrelated table fingerprint remains valid");
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn core_drop_rollback_restores_exact_table_data_indexes_and_high_waters() {
    let root = root("drop-rollback");
    let mut db = seed(&root, true);
    db.create_named_index(
        IndexName::new("users_id_idx").unwrap(),
        TableId(1),
        ColumnId(1),
    )
    .unwrap();
    let target = db.resolve_drop_table("users").unwrap();
    let before = db.inspect_catalog().unwrap();
    let before_generation = db.schema_generation();
    let before_revision = db.catalog_generation();
    let high_waters = (
        db.next_table_id(),
        db.next_storage_id(),
        db.next_partition_id(),
        db.next_column_id(TableId(1)),
    );
    let mut txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut txn, target).unwrap();
    txn.rollback().unwrap();
    assert_eq!(db.inspect_catalog().unwrap(), before);
    assert_eq!(db.schema_generation(), before_generation);
    assert_eq!(db.catalog_generation(), before_revision);
    assert_eq!(
        (
            db.next_table_id(),
            db.next_storage_id(),
            db.next_partition_id(),
            db.next_column_id(TableId(1)),
        ),
        high_waters
    );
    assert!(db.inspect_retired_table_resources().is_empty());
    assert_eq!(db.indexes(TableId(1)).unwrap().len(), 1);
    assert_eq!(
        db.query("SELECT id FROM users").unwrap().rows,
        vec![vec![ScalarValue::Int64(1)]]
    );
    drop(txn);
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.resolve_drop_table("users").unwrap(), target);
    assert_eq!(reopened.inspect_catalog().unwrap(), before);
    assert!(reopened.inspect_retired_table_resources().is_empty());
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn core_drop_recreate_same_name_never_reuses_identity_data_index_or_prepared_target() {
    let root = root("drop-recreate");
    let mut db = seed(&root, true);
    db.create_named_index(
        IndexName::new("users_id_idx").unwrap(),
        TableId(1),
        ColumnId(1),
    )
    .unwrap();
    let old_prepared = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let old_target = db.resolve_drop_table("users").unwrap();
    let mut drop_txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut drop_txn, old_target).unwrap();
    db.commit_transaction(&mut drop_txn).unwrap();
    drop(drop_txn);
    let mut create_txn = db.begin_transaction().unwrap();
    let new_table = db
        .create_heap_table_in(
            &mut create_txn,
            CreateTableSpec::new(
                "users",
                vec![CreateColumnSpec::new(
                    "id",
                    SemanticType::physical(PhysicalType::Int64),
                    false,
                )],
            ),
        )
        .unwrap();
    assert_eq!(new_table, TableId(3));
    db.commit_transaction(&mut create_txn).unwrap();
    assert_eq!(db.bindings.resolve_single(new_table).unwrap(), StorageId(3));
    assert_eq!(
        db.table_schema_version(new_table),
        Some(TableSchemaVersion(1))
    );
    assert!(db.indexes(new_table).unwrap().is_empty());
    assert!(db.query("SELECT id FROM users").unwrap().rows.is_empty());
    assert!(db.execute_prepared(&old_prepared, &[]).is_err());
    assert_eq!(db.schema_generation(), SchemaGeneration(3));
    assert_eq!(
        db.inspect_retired_table_resources()[0].storage_id,
        StorageId(1)
    );
    assert!(root.join("users.heap").is_file());
    let snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let new_locator = snapshot
        .storages
        .iter()
        .find(|storage| storage.id == StorageId(3))
        .unwrap()
        .locator
        .clone();
    assert_ne!(
        db.inspect_retired_table_resources()[0].relative_locator,
        new_locator
    );
    assert!(root.join(new_locator).is_file());
    drop(create_txn);
    db.close().unwrap();
    let mut retired_heap =
        netbadb_storage::TableStorage::open_heap(root.join("users.heap"), old_table(1, "users"))
            .unwrap();
    assert_eq!(retired_heap.indexes().len(), 1);
    let view = retired_heap.read_view().unwrap();
    assert_eq!(
        retired_heap
            .scan_columns_with_view(&[ColumnId(1)], &view)
            .unwrap()
            .into_iter()
            .map(|(_, values)| values)
            .collect::<Vec<_>>(),
        vec![vec![ScalarValue::Int64(1)]]
    );
    retired_heap.close().unwrap();
    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.schema().table("users").unwrap().id, TableId(3));
    assert!(
        reopened
            .query("SELECT id FROM users")
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        reopened.inspect_retired_table_resources()[0].storage_id,
        StorageId(1)
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn core_drop_rejects_missing_stale_and_mixed_targets_without_side_effects() {
    let root = root("drop-validation");
    let mut db = seed(&root, true);
    let exact = db.resolve_drop_table("users").unwrap();
    let before_catalog = std::fs::read(root.join("catalog")).unwrap();
    let before_marker = std::fs::read(root.join("catalog.state")).unwrap();
    let before_generation = db.schema_generation();
    let high_waters = (
        db.next_table_id(),
        db.next_storage_id(),
        db.next_partition_id(),
    );
    let retained = db.begin_transaction().unwrap();
    let mut blocked = db.begin_transaction().unwrap();
    assert!(matches!(
        db.drop_table_in(&mut blocked, exact),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    assert!(!root.join("catalog.mutations").exists());
    drop(blocked);
    drop(retained);
    let mut txn = db.begin_transaction().unwrap();
    let mut missing = exact;
    missing.table_id = TableId(999);
    assert!(matches!(
        db.drop_table_in(&mut txn, missing),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::TableNotFound(TableId(999))
        ))
    ));
    let mut stale = exact;
    stale.table_version = TableSchemaVersion(2);
    assert!(matches!(
        db.drop_table_in(&mut txn, stale),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::StaleSchemaDependency
        ))
    ));
    assert!(!root.join("catalog.mutations").exists());
    assert_eq!(std::fs::read(root.join("catalog")).unwrap(), before_catalog);
    assert_eq!(
        std::fs::read(root.join("catalog.state")).unwrap(),
        before_marker
    );
    assert_eq!(db.schema_generation(), before_generation);
    assert_eq!(
        (
            db.next_table_id(),
            db.next_storage_id(),
            db.next_partition_id()
        ),
        high_waters
    );
    db.drop_table_in(&mut txn, exact).unwrap();
    let teams = db.resolve_drop_table("teams").unwrap();
    assert!(matches!(
        db.drop_table_in(&mut txn, teams),
        Err(crate::DatabaseError::UnsupportedDdlCombination)
    ));
    assert!(matches!(
        db.create_heap_table_in(&mut txn, spec("other")),
        Err(crate::DatabaseError::UnsupportedDdlCombination)
            | Err(crate::DatabaseError::SchemaMutation(
                SchemaMutationError::MultipleCreatesUnsupported
            ))
    ));
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn core_drop_rejects_lsm_and_partitioned_tables_before_persistent_intent() {
    let lsm_root = root("drop-lsm");
    let mut lsm = Database::create_catalog(
        lsm_root.join("catalog"),
        vec![TableStorageCreateSpec::lsm(
            lsm_root.join("rows.lsm"),
            old_table(1, "rows"),
            ColumnId(1),
        )],
        Some(DatabaseCoordinatorConfig::new(lsm_root.join("coordinator"))),
    )
    .unwrap();
    let target = lsm.resolve_drop_table("rows").unwrap();
    let mut txn = lsm.begin_transaction().unwrap();
    assert!(matches!(
        lsm.drop_table_in(&mut txn, target),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedPlacement
        ))
    ));
    assert!(!lsm_root.join("catalog.mutations").exists());
    txn.rollback().unwrap();
    drop(txn);
    lsm.close().unwrap();

    let partition_root = root("drop-partition");
    let mut partitioned = Database::create_catalog_with_placements(
        partition_root.join("catalog"),
        vec![crate::TablePlacementSpec::range_partitioned(
            old_table(1, "events"),
            ColumnId(1),
            vec![crate::RangePartitionSpec::new(
                netbadb_types::PartitionId(1),
                partition_root.join("events.heap"),
                None,
                None,
            )],
        )],
        crate::PartitionCatalogConfig::new(
            partition_root.join("partitions"),
            partition_root.join("coordinator"),
        ),
    )
    .unwrap();
    let target = partitioned.resolve_drop_table("events").unwrap();
    let mut txn = partitioned.begin_transaction().unwrap();
    assert!(matches!(
        partitioned.drop_table_in(&mut txn, target),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedPlacement
        ))
    ));
    assert!(!partition_root.join("catalog.mutations").exists());
    txn.rollback().unwrap();
    drop(txn);
    partitioned.close().unwrap();
    std::fs::remove_dir_all(lsm_root).unwrap();
    std::fs::remove_dir_all(partition_root).unwrap();
}

#[test]
fn core_drop_highest_identity_is_not_reused_and_missing_retained_heap_fails_open() {
    let root = root("drop-highest");
    let mut db = seed(&root, true);
    let teams = db.resolve_drop_table("teams").unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut txn, teams).unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    let mut create = db.begin_transaction().unwrap();
    let id = db
        .create_heap_table_in(&mut create, CreateTableSpec::new("replacement", vec![]))
        .unwrap();
    assert_eq!(id, TableId(3));
    db.commit_transaction(&mut create).unwrap();
    assert_eq!(db.bindings.resolve_single(id).unwrap(), StorageId(3));
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    drop(create);
    db.close().unwrap();
    let journal_before = std::fs::read(root.join("catalog.mutations")).unwrap();
    for _ in 0..3 {
        Database::open_catalog(root.join("catalog"))
            .unwrap()
            .close()
            .unwrap();
        assert_eq!(
            std::fs::read(root.join("catalog.mutations")).unwrap(),
            journal_before
        );
    }
    std::fs::remove_file(root.join("teams.heap")).unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn runtime_created_heap_can_be_dropped_and_recovered_from_retained_create_history() {
    let root = root("drop-runtime-create");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(&mut create, CreateTableSpec::new("projects", vec![]))
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    let target = db.resolve_drop_table("projects").unwrap();
    let mut drop_txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut drop_txn, target).unwrap();
    db.commit_transaction(&mut drop_txn).unwrap();
    assert_eq!(db.schema_generation(), SchemaGeneration(3));
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.inspect_retired_table_resources()[0].table_id, table);
    drop(drop_txn);
    db.close().unwrap();
    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(reopened.schema().table("projects").is_none());
        assert_eq!(reopened.schema_generation(), SchemaGeneration(3));
        assert_eq!(
            reopened.inspect_retired_table_resources()[0].table_id,
            table
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

fn create_and_drop_runtime_heap(db: &mut Database, name: &str) -> RetiredTableResource {
    let mut create = db.begin_transaction().unwrap();
    db.create_heap_table_in(
        &mut create,
        CreateTableSpec::new(
            name,
            vec![CreateColumnSpec::new(
                "id",
                SemanticType::physical(PhysicalType::Int64),
                false,
            )],
        ),
    )
    .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    let target = db.resolve_drop_table(name).unwrap();
    let mut drop_txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut drop_txn, target).unwrap();
    db.commit_transaction(&mut drop_txn).unwrap();
    drop(drop_txn);
    db.inspect_retired_table_resources()
        .into_iter()
        .max_by_key(|resource| resource.storage_id)
        .unwrap()
}

#[test]
fn retired_runtime_heap_gc_is_exact_durable_and_generation_neutral() {
    let root = root("retired-heap-gc");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    let high_waters = (
        db.schema_generation(),
        db.catalog_generation(),
        db.next_table_id(),
        db.next_storage_id(),
        db.next_partition_id(),
    );
    let inspection = db.inspect_retired_heap_gc(&retired).unwrap();
    assert_eq!(inspection.state, RetiredHeapGcState::Retained);
    assert!(inspection.eligible(), "{:?}", inspection.blockers);
    assert!(inspection.total_present_bytes > 0);
    assert!(inspection.components.iter().any(|component| {
        component.kind == crate::RetiredHeapGcComponentKind::Main && component.present
    }));
    let component_paths = inspection
        .components
        .iter()
        .map(|component| component.path.clone())
        .collect::<Vec<_>>();
    let report = db.gc_retired_heap(&retired).unwrap();
    eprintln!(
        "single retired Heap GC: files_deleted={}, bytes_deleted={}",
        report.files_deleted, report.bytes_deleted
    );
    assert_eq!(report.state, RetiredHeapGcState::Deleted);
    assert!(report.files_deleted >= 5);
    assert!(report.bytes_deleted > 0);
    assert!(component_paths.iter().all(|path| !path.exists()));
    assert_eq!(
        (
            db.schema_generation(),
            db.catalog_generation(),
            db.next_table_id(),
            db.next_storage_id(),
            db.next_partition_id(),
        ),
        high_waters
    );
    assert_eq!(
        db.query("SELECT id FROM teams").unwrap().rows,
        vec![vec![ScalarValue::Int64(2)]]
    );
    assert_eq!(
        db.inspect_retired_heap_gc(&retired).unwrap().state,
        RetiredHeapGcState::Deleted
    );
    db.close().unwrap();
    for _ in 0..3 {
        let db = Database::open_catalog(root.join("catalog")).unwrap();
        let inspection = db.inspect_retired_heap_gc(&retired).unwrap();
        assert_eq!(inspection.state, RetiredHeapGcState::Deleted);
        assert_eq!(inspection.total_present_bytes, 0);
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retired_heap_gc_never_touches_same_name_recreation_with_index() {
    let root = root("retired-heap-gc-same-name");
    let mut db = seed(&root, true);
    let mut old_create = db.begin_transaction().unwrap();
    let old_table = db
        .create_heap_table_in(
            &mut old_create,
            CreateTableSpec::new(
                "projects",
                vec![CreateColumnSpec::new(
                    "id",
                    SemanticType::physical(PhysicalType::Int64),
                    false,
                )],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut old_create).unwrap();
    drop(old_create);
    db.create_named_index(
        IndexName::new("old_projects_id_idx").unwrap(),
        old_table,
        ColumnId(1),
    )
    .unwrap();
    assert_eq!(db.indexes(old_table).unwrap().len(), 1);
    db.execute("INSERT INTO projects (id) VALUES (11)").unwrap();
    let old_target = db.resolve_drop_table("projects").unwrap();
    let mut old_drop = db.begin_transaction().unwrap();
    db.drop_table_in(&mut old_drop, old_target).unwrap();
    db.commit_transaction(&mut old_drop).unwrap();
    drop(old_drop);
    let retired = db
        .inspect_retired_table_resources()
        .into_iter()
        .max_by_key(|resource| resource.storage_id)
        .unwrap();
    let mut create = db.begin_transaction().unwrap();
    let replacement = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![CreateColumnSpec::new(
                    "id",
                    SemanticType::physical(PhysicalType::Int64),
                    false,
                )],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.create_named_index(
        IndexName::new("projects_id_idx").unwrap(),
        replacement,
        ColumnId(1),
    )
    .unwrap();
    db.execute("INSERT INTO projects (id) VALUES (44)").unwrap();
    let replacement_storage = db.bindings.resolve_single(replacement).unwrap();
    let replacement_locator = crate::schema_catalog_file::load(&root.join("catalog"))
        .unwrap()
        .storages
        .into_iter()
        .find(|storage| storage.id == replacement_storage)
        .unwrap()
        .locator;
    assert_eq!(old_table, TableId(3));
    assert_eq!(retired.storage_id, StorageId(3));
    assert_eq!(replacement, TableId(4));
    assert_eq!(replacement_storage, StorageId(4));
    assert_ne!(retired.relative_locator, replacement_locator);
    assert_ne!(replacement_storage, retired.storage_id);
    let before = db.inspect_retired_heap_gc(&retired).unwrap();
    let report = db.gc_retired_heap(&retired).unwrap();
    eprintln!(
        "indexed same-name GC: old_table={}, old_storage={}, old_locator={}, new_table={}, new_storage={}, new_locator={}, files_deleted={}, bytes_deleted={}",
        old_table.0,
        retired.storage_id.0,
        retired.relative_locator,
        replacement.0,
        replacement_storage.0,
        replacement_locator,
        report.files_deleted,
        report.bytes_deleted
    );
    assert_eq!(
        report.files_deleted,
        before.components.iter().filter(|item| item.present).count() as u64
    );
    assert_eq!(report.bytes_deleted, before.total_present_bytes);
    assert_eq!(db.indexes(replacement).unwrap().len(), 1);
    assert_eq!(
        db.query("SELECT id FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(44)]]
    );
    db.close().unwrap();
    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.indexes(replacement).unwrap().len(), 1);
    assert_eq!(
        reopened.query("SELECT id FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(44)]]
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retained_heap_missing_without_gc_intent_is_a_hard_reopen_error() {
    let root = root("retired-heap-gc-unexplained-missing");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    let inspection = db.inspect_retired_heap_gc(&retired).unwrap();
    let main = inspection
        .components
        .iter()
        .find(|component| component.kind == crate::RetiredHeapGcComponentKind::Main)
        .unwrap()
        .path
        .clone();
    db.close().unwrap();
    std::fs::remove_file(main).unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retired_heap_gc_rejects_inexact_unsupported_and_symlink_targets() {
    let safety_root = root("retired-heap-gc-safety");
    let mut db = seed(&safety_root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    let mut wrong = retired.clone();
    wrong.table_id = TableId(999);
    assert!(matches!(
        db.gc_retired_heap(&wrong),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::RetiredHeapTargetMismatch(_)
        ))
    ));
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let owner = db
            .inspect_retired_heap_gc(&retired)
            .unwrap()
            .components
            .into_iter()
            .find(|component| component.kind == crate::RetiredHeapGcComponentKind::Owner)
            .unwrap()
            .path;
        std::fs::remove_file(&owner).unwrap();
        symlink(safety_root.join("catalog"), &owner).unwrap();
        assert!(db.inspect_retired_heap_gc(&retired).is_err());
        assert!(safety_root.join("catalog").is_file());
    }
    drop(db);
    std::fs::remove_dir_all(safety_root).unwrap();

    let imported_root = root("retired-heap-gc-imported");
    let mut db = seed(&imported_root, true);
    let target = db.resolve_drop_table("users").unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut txn, target).unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    let imported = db.inspect_retired_table_resources()[0].clone();
    let inspection = db.inspect_retired_heap_gc(&imported).unwrap();
    assert_eq!(
        inspection.blockers,
        vec![crate::RetiredHeapGcBlocker::UnsupportedLocator]
    );
    assert!(db.gc_retired_heap(&imported).is_err());
    assert!(imported_root.join("users.heap").is_file());
    db.close().unwrap();
    std::fs::remove_dir_all(imported_root).unwrap();
}

#[test]
fn retired_heap_gc_requires_database_transaction_quiescence() {
    let root = root("retired-heap-gc-quiescence");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    let transaction = db.begin_transaction().unwrap();
    let inspection = db.inspect_retired_heap_gc(&retired).unwrap();
    assert!(inspection.blockers.iter().any(|blocker| matches!(
        blocker,
        crate::RetiredHeapGcBlocker::ActiveTransactionHandles { count: 1 }
    )));
    assert!(db.gc_retired_heap(&retired).is_err());
    drop(transaction);
    db.gc_retired_heap(&retired).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retired_heap_gc_waits_for_the_complete_coordinator_horizon() {
    let root = root("retired-heap-gc-horizon");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    let before = db.inspect_retired_heap_gc(&retired).unwrap();
    let paths = before
        .components
        .iter()
        .filter(|component| component.present)
        .map(|component| component.path.clone())
        .collect::<Vec<_>>();
    let horizon = db.next_transaction_id;
    db.coordinator
        .as_ref()
        .unwrap()
        .borrow_mut()
        .commit_decision(
            horizon,
            &[crate::coordinator_log::CoordinatorParticipant {
                storage_id: retired.storage_id,
                physical_txn_id: TxnId(999),
            }],
        )
        .unwrap();

    let blocked = db.inspect_retired_heap_gc(&retired).unwrap();
    assert_eq!(blocked.coordinator_horizon, Some(horizon));
    assert!(blocked.blockers.iter().any(|blocker| matches!(
        blocker,
        crate::RetiredHeapGcBlocker::CoordinatorDecisionIncomplete { transaction }
            if *transaction == horizon
    )));
    assert!(db.gc_retired_heap(&retired).is_err());
    assert!(paths.iter().all(|path| path.is_file()));

    db.coordinator
        .as_ref()
        .unwrap()
        .borrow_mut()
        .complete(horizon)
        .unwrap();
    let eligible = db.inspect_retired_heap_gc(&retired).unwrap();
    assert_eq!(eligible.coordinator_horizon, Some(horizon));
    assert!(eligible.eligible(), "{:?}", eligible.blockers);
    db.gc_retired_heap(&retired).unwrap();
    assert!(paths.iter().all(|path| !path.exists()));
    db.close().unwrap();
    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            reopened.inspect_retired_heap_gc(&retired).unwrap().state,
            RetiredHeapGcState::Deleted
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn completed_gc_rejects_reappeared_old_component() {
    let root = root("retired-heap-gc-reappeared");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    let main = db
        .inspect_retired_heap_gc(&retired)
        .unwrap()
        .components
        .into_iter()
        .find(|component| component.kind == crate::RetiredHeapGcComponentKind::Main)
        .unwrap()
        .path;
    db.gc_retired_heap(&retired).unwrap();
    db.close().unwrap();
    std::fs::write(main, b"reappeared").unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn uncertain_gc_intent_sync_is_recovered_without_unexplained_deletion() {
    let root = root("retired-heap-gc-intent-sync");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    db.mutation_journal
        .as_ref()
        .unwrap()
        .borrow_mut()
        .inject_sync_failure();
    assert!(db.gc_retired_heap(&retired).is_err());
    drop(db);
    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            reopened.inspect_retired_heap_gc(&retired).unwrap().state,
            RetiredHeapGcState::Deleted
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gc_complete_without_intent_is_rejected_as_corrupt_journal_order() {
    let root = root("retired-heap-gc-complete-without-intent");
    let mut db = seed(&root, true);
    let retired = create_and_drop_runtime_heap(&mut db, "projects");
    db.gc_retired_heap(&retired).unwrap();
    db.close().unwrap();
    let journal_path = root.join("catalog.mutations");
    let bytes = std::fs::read(&journal_path).unwrap();
    let mut reader = crate::schema_catalog::Reader(
        crate::schema_catalog::open_envelope(&bytes, b"NBSJ").unwrap(),
    );
    let incarnation = reader.take(16).unwrap().to_vec();
    let coordinator = reader.string().unwrap();
    let count = reader.u32().unwrap();
    let mut records = Vec::new();
    for _ in 0..count {
        let length = reader.u32().unwrap() as usize;
        let record = reader.take(length).unwrap();
        let payload = crate::schema_catalog::open_envelope(record, b"NBSR").unwrap();
        if payload[0] != 9 {
            records.push(record.to_vec());
        }
    }
    let mut writer = crate::schema_catalog::Writer(incarnation);
    writer.string(&coordinator).unwrap();
    writer.u32(records.len() as u32);
    for record in records {
        writer.u32(record.len() as u32);
        writer.0.extend_from_slice(&record);
    }
    let corrupt = crate::schema_catalog::envelope(b"NBSJ", &writer.0).unwrap();
    std::fs::write(journal_path, corrupt).unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn one_hundred_create_drop_gc_cycles_bound_physical_growth_and_never_reuse_ids() {
    let root = root("retired-heap-gc-100-cycles");
    let mut db = seed(&root, true);
    let mut previous_storage = StorageId(2);
    for _ in 0..100 {
        let retired = create_and_drop_runtime_heap(&mut db, "churn");
        assert!(retired.storage_id > previous_storage);
        previous_storage = retired.storage_id;
        let inspection = db.inspect_retired_heap_gc(&retired).unwrap();
        db.gc_retired_heap(&retired).unwrap();
        assert!(
            inspection
                .components
                .iter()
                .all(|component| !component.path.exists())
        );
    }
    assert_eq!(previous_storage, StorageId(102));
    assert_eq!(db.next_storage_id(), Some(StorageId(103)));
    assert_eq!(db.schema_generation(), SchemaGeneration(201));
    eprintln!(
        "100-cycle retained metadata: NBSJ={} bytes, CORD={} bytes, known retired physical bytes=0",
        std::fs::metadata(root.join("catalog.mutations"))
            .unwrap()
            .len(),
        std::fs::metadata(root.join("coordinator")).unwrap().len()
    );
    db.close().unwrap();
    for _ in 0..3 {
        Database::open_catalog(root.join("catalog"))
            .unwrap()
            .close()
            .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retired_heap_gc_crash_child() {
    let Ok(root) = std::env::var("NETBADB_GC_CHILD_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let retired = db
        .inspect_retired_table_resources()
        .into_iter()
        .max_by_key(|resource| resource.storage_id)
        .unwrap();
    db.gc_retired_heap(&retired).unwrap();
    panic!("configured retired Heap GC crash hook was not reached");
}

fn spawn_retired_heap_gc(root: &Path, point: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "schema_mutation_tests::retired_heap_gc_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_GC_CHILD_ROOT", root)
        .env("NETBADB_GC_CRASH_POINT", point)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(90),
        "{point}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn subprocess_retired_heap_gc_crash_matrix_converges_on_three_reopens() {
    for point in [
        "gc-before-intent",
        "gc-intent-durable",
        "gc-before-first-delete",
        "gc-after-owner-delete",
        "gc-after-main-delete",
        "gc-after-wal-delete",
        "gc-after-status-delete",
        "gc-after-alternate-delete",
        "gc-after-link-delete",
        "gc-after-link-shadow-delete",
        "gc-directory-synced",
        "gc-complete-durable",
    ] {
        let root = root(point);
        let mut db = seed(&root, true);
        let retired = create_and_drop_runtime_heap(&mut db, "projects");
        db.close().unwrap();
        spawn_retired_heap_gc(&root, point);
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            let state = reopened.inspect_retired_heap_gc(&retired).unwrap().state;
            if state == RetiredHeapGcState::Retained {
                reopened.gc_retired_heap(&retired).unwrap();
            }
            assert_eq!(
                reopened.inspect_retired_heap_gc(&retired).unwrap().state,
                RetiredHeapGcState::Deleted
            );
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn imported_single_heap_drop_preserves_immutable_placement_evidence() {
    let root = root("drop-imported-single");
    let mut db = Database::create_catalog_with_placements(
        root.join("catalog"),
        vec![crate::TablePlacementSpec::single(
            root.join("imported.heap"),
            old_table(9, "imported"),
        )],
        crate::PartitionCatalogConfig::new(root.join("placements"), root.join("coordinator")),
    )
    .unwrap();
    let target = db.resolve_drop_table("imported").unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut txn, target).unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(reopened.schema().tables().is_empty());
        assert_eq!(
            reopened.inspect_retired_table_resources()[0].table_id,
            TableId(9)
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn retired_inventory_explains_completed_historical_storage_only_decisions() {
    let root = root("drop-historical-decision");
    let mut db = seed(&root, true);
    let mut write = db.begin_transaction().unwrap();
    db.execute_in(&mut write, "UPDATE users SET id = 10")
        .unwrap();
    db.execute_in(&mut write, "UPDATE teams SET id = 20")
        .unwrap();
    db.commit_transaction(&mut write).unwrap();
    drop(write);
    let target = db.resolve_drop_table("users").unwrap();
    let mut drop_txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut drop_txn, target).unwrap();
    db.commit_transaction(&mut drop_txn).unwrap();
    drop(drop_txn);
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(reopened.schema().table("users").is_none());
        assert_eq!(
            reopened.query("SELECT id FROM teams").unwrap().rows,
            vec![vec![ScalarValue::Int64(20)]]
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn core_create_same_transaction_dml_publication_dependencies_and_catalog_only_reopen() {
    let root = root("basic");
    let mut db = seed(&root, true);
    let existing = db.prepare_statement("SELECT id FROM users", &[]).unwrap();
    let old_catalog = std::fs::read(root.join("catalog")).unwrap();
    let old_marker = std::fs::read(root.join("catalog.state")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "UPDATE users SET id = 7").unwrap();
    db.execute_in(&mut txn, "UPDATE teams SET id = 8").unwrap();
    assert_eq!(
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
        TableId(3)
    );
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    assert_eq!(db.next_partition_id(), Some(netbadb_types::PartitionId(1)));
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.registry.len(), 2);
    assert!(db.prepare_statement("SELECT * FROM projects", &[]).is_err());
    assert!(
        db.inspect_catalog()
            .unwrap()
            .tables
            .iter()
            .all(|t| t.name != "projects")
    );
    assert!(matches!(
        db.begin_transaction(),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    assert!(db.execute("UPDATE users SET id = 99").is_err());
    assert_eq!(std::fs::read(root.join("catalog")).unwrap(), old_catalog);
    assert_eq!(
        std::fs::read(root.join("catalog.state")).unwrap(),
        old_marker
    );
    insert(&mut db, &mut txn);
    let select = db
        .prepare_statement_in(&txn, "SELECT * FROM projects", &[])
        .unwrap();
    assert_eq!(
        rows(db.execute_prepared_in(&mut txn, &select, &[]).unwrap()),
        expected()
    );
    assert!(db.execute_prepared(&select, &[]).is_err());
    assert!(txn.commit().is_err());
    db.commit_transaction(&mut txn).unwrap();
    assert_eq!(txn.state(), TransactionState::Committed);
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    assert_eq!(db.catalog_generation(), 1);
    assert_eq!(
        db.table_schema_version(TableId(3)),
        Some(TableSchemaVersion(1))
    );
    assert_eq!(
        db.table_schema_version(TableId(1)),
        Some(TableSchemaVersion(1))
    );
    assert_eq!(db.next_column_id(TableId(3)), Some(ColumnId(6)));
    assert_eq!(
        rows(db.execute_prepared(&existing, &[]).unwrap()),
        vec![vec![ScalarValue::Int64(7)]]
    );
    assert_eq!(
        rows(db.execute("SELECT * FROM projects").unwrap()),
        expected()
    );
    assert!(db.execute_prepared(&select, &[]).is_err());
    let table = db.schema().table("projects").unwrap().clone();
    assert!(table.columns.iter().all(|c| !c.primary_key));
    assert_eq!(
        table.columns.iter().map(|c| c.id).collect::<Vec<_>>(),
        (1..=5).map(ColumnId).collect::<Vec<_>>()
    );
    drop(txn);
    db.close().unwrap();
    let bytes = std::fs::read(root.join("catalog")).unwrap();
    let marker = std::fs::read(root.join("catalog.state")).unwrap();
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema().table("projects"), Some(&table));
        assert_eq!(
            db.bindings.resolve_single(TableId(3)).unwrap(),
            StorageId(3)
        );
        assert_eq!(
            rows(db.execute("SELECT * FROM projects").unwrap()),
            expected()
        );
        assert_eq!(db.schema_generation(), SchemaGeneration(2));
        db.close().unwrap();
        assert_eq!(std::fs::read(root.join("catalog")).unwrap(), bytes);
        assert_eq!(std::fs::read(root.join("catalog.state")).unwrap(), marker);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn rollback_burns_ids_name_reuse_and_transaction_prepared_scope() {
    let root = root("rollback");
    let mut db = seed(&root, false);
    let mut txn = db.begin_transaction().unwrap();
    let rolled = db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    assert_eq!(rolled, TableId(3));
    insert(&mut db, &mut txn);
    let select = db
        .prepare_statement_in(&txn, "SELECT * FROM projects", &[])
        .unwrap();
    txn.rollback().unwrap();
    assert_eq!(txn.state(), TransactionState::RolledBack);
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert_eq!(db.catalog_generation(), 0);
    assert!(db.schema().table("projects").is_none());
    assert!(db.execute_prepared_in(&mut txn, &select, &[]).is_err());
    drop(txn);
    db.close().unwrap();
    let mut db = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
        TableId(4)
    );
    assert!(db.execute_prepared_in(&mut txn, &select, &[]).is_err());
    db.commit_transaction(&mut txn).unwrap();
    assert_eq!(
        db.bindings.resolve_single(TableId(4)).unwrap(),
        StorageId(4)
    );
    assert!(db.query("SELECT * FROM projects").unwrap().rows.is_empty());
    drop(txn);
    db.close().unwrap();
    for _ in 0..3 {
        let db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema().table("projects").unwrap().id, TableId(4));
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn exclusive_retained_handle_admission_validation_and_mixing() {
    let root = root("admission");
    let mut db = seed(&root, true);
    let retained = db.begin_transaction().unwrap();
    let mut txn = db.begin_transaction().unwrap();
    assert!(matches!(
        db.create_heap_table_in(&mut txn, spec("projects")),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    drop(retained);
    for invalid in [
        spec("users"),
        spec(""),
        CreateTableSpec::new(
            "projects",
            vec![spec("x").columns[0].clone(), spec("x").columns[0].clone()],
        ),
    ] {
        assert!(db.create_heap_table_in(&mut txn, invalid).is_err());
        assert_eq!(db.next_table_id(), Some(TableId(3)));
        assert_eq!(db.next_storage_id(), Some(StorageId(3)));
    }
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    assert!(matches!(
        db.create_heap_table_in(&mut txn, spec("other")),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::MultipleCreatesUnsupported
        ))
    ));
    let ddl = db
        .prepare_ddl_statement("CREATE INDEX user_id ON users (id)")
        .unwrap();
    assert!(db.execute_ddl_in(&mut txn, &ddl).is_err());
    assert!(db.create_index(TableId(1), ColumnId(1)).is_err());
    assert!(
        db.prepare_ddl_statement("CREATE INDEX project_id ON projects (id)")
            .is_err()
    );
    let null = db
        .prepare_statement_in(
            &txn,
            "INSERT INTO projects (id, name, active, score, label) VALUES ($1, $2, $3, $4, $5)",
            &[],
        )
        .unwrap();
    assert!(
        db.execute_prepared_in(
            &mut txn,
            &null,
            &[
                ScalarValue::Null,
                ScalarValue::Text("bad".into()),
                ScalarValue::Bool(true),
                ScalarValue::Null,
                ScalarValue::Null
            ]
        )
        .is_err()
    );
    if txn.state() == TransactionState::Active {
        txn.rollback().unwrap();
    }
    assert!(db.schema().table("projects").is_none());
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_catalog_empty_columns_and_multiple_commits_preserve_high_waters() {
    let root = root("empty");
    let mut db = Database::create_catalog(root.join("catalog"), vec![], None).unwrap();
    for n in 1..=3 {
        let mut txn = db.begin_transaction().unwrap();
        let id = db
            .create_heap_table_in(&mut txn, CreateTableSpec::new(format!("table{n}"), vec![]))
            .unwrap();
        assert_eq!(id, TableId(n));
        db.commit_transaction(&mut txn).unwrap();
        drop(txn);
    }
    assert_eq!(db.schema_generation(), SchemaGeneration(4));
    db.close().unwrap();
    let db = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(db.schema().tables().len(), 3);
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn commit_sync_failures_remain_retry_only_and_cleanup_failure_is_rollback_pending() {
    for fault in [
        "decision-append",
        "decision-sync",
        "complete-append",
        "complete-sync",
    ] {
        let root = root(fault);
        let mut db = seed(&root, true);
        let mut txn = db.begin_transaction().unwrap();
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
        insert(&mut db, &mut txn);
        {
            let mut log = db.coordinator.as_ref().unwrap().borrow_mut();
            match fault {
                "decision-append" => log.inject_decision_append_failure(),
                "decision-sync" => log.inject_decision_sync_failure(),
                "complete-append" => log.inject_complete_append_failure(),
                _ => log.inject_complete_sync_failure(),
            }
        }
        assert!(db.commit_transaction(&mut txn).is_err());
        assert!(matches!(
            txn.state(),
            TransactionState::DecisionPending | TransactionState::FinalizePending
        ));
        assert!(txn.rollback().is_err());
        assert!(db.schema().table("projects").is_none());
        db.commit_transaction(&mut txn).unwrap();
        assert_eq!(
            rows(db.execute("SELECT * FROM projects").unwrap()),
            expected()
        );
        drop(txn);
        db.close().unwrap();
        Database::open_catalog(root.join("catalog"))
            .unwrap()
            .close()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    let root = root("cleanup-pending");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    let mutation = txn.schema_mutation.as_ref().unwrap();
    let path = crate::schema_catalog_file::resolve(
        &mutation.catalog,
        &crate::schema_mutation_journal::prepared_locator(
            &mutation.catalog,
            mutation.target.incarnation,
            txn.id(),
        )
        .unwrap(),
    );
    std::fs::create_dir(&path).unwrap();
    assert!(txn.rollback().is_err());
    assert_eq!(txn.state(), TransactionState::RollbackPending);
    assert!(db.begin_transaction().is_err());
    std::fs::remove_dir(path).unwrap();
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn create_crash_child() {
    let Ok(root) = std::env::var("NETBADB_CREATE_CHILD_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "UPDATE users SET id = 7").unwrap();
    if std::env::var_os("NETBADB_CREATE_SKIP_SECOND_WRITE").is_none() {
        db.execute_in(&mut txn, "UPDATE teams SET id = 8").unwrap();
    }
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    insert(&mut db, &mut txn);
    assert_eq!(
        rows(db.execute_in(&mut txn, "SELECT * FROM projects").unwrap()),
        expected()
    );
    if std::env::var("NETBADB_CREATE_CRASH_POINT")
        .unwrap_or_default()
        .starts_with("rollback-")
    {
        txn.rollback().unwrap();
    } else {
        db.commit_transaction(&mut txn).unwrap();
    }
    panic!("configured crash hook was not reached");
}
fn spawn(root: &Path, point: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "schema_mutation_tests::create_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_CREATE_CHILD_ROOT", root)
        .env("NETBADB_CREATE_CRASH_POINT", point);
    if point == "during-decision-append" || point == "after-decision-append" {
        crate::coordinator_crash::configure_child(&mut command, "schema-create", root, point);
    }
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(if point.contains("decision-append") {
            87
        } else {
            90
        }),
        "{point}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn outcome(root: &Path, winner: bool) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            db.schema_generation(),
            SchemaGeneration(if winner { 2 } else { 1 })
        );
        assert_eq!(db.next_table_id(), Some(TableId(4)));
        assert_eq!(db.next_storage_id(), Some(StorageId(4)));
        assert_eq!(db.registry.len(), if winner { 3 } else { 2 });
        assert_eq!(
            db.query("SELECT id FROM users").unwrap().rows,
            vec![vec![ScalarValue::Int64(if winner { 7 } else { 1 })]]
        );
        assert_eq!(
            db.query("SELECT id FROM teams").unwrap().rows,
            vec![vec![ScalarValue::Int64(if winner { 8 } else { 2 })]]
        );
        if winner {
            let table = db.schema().table("projects").unwrap();
            assert_eq!(table.id, TableId(3));
            assert_eq!(
                table.columns.iter().map(|c| c.id).collect::<Vec<_>>(),
                (1..=5).map(ColumnId).collect::<Vec<_>>()
            );
            assert_eq!(
                db.table_schema_version(table.id),
                Some(TableSchemaVersion(1))
            );
            assert_eq!(db.next_column_id(table.id), Some(ColumnId(6)));
            assert_eq!(db.bindings.resolve_single(table.id).unwrap(), StorageId(3));
            assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        } else {
            assert!(db.schema().table("projects").is_none());
        }
        db.close().unwrap();
    }
}
#[test]
fn subprocess_create_crash_matrix_reopens_three_times_with_exact_outcomes() {
    for (point, winner) in [
        ("reservation-durable", false),
        ("intent-durable", false),
        ("stage-first-file", false),
        ("stage-synced", false),
        ("participants-prepared", false),
        ("prepared-catalog-written", false),
        ("prepared-catalog-durable", false),
        ("before-coordinator-decision", false),
        ("during-decision-append", false),
        ("after-decision-append", true),
        ("coordinator-durable", true),
        ("staged-heap-committed", true),
        ("promotion-partial", true),
        ("promotion-complete", true),
        ("before-nbsc-publication", true),
        ("during-nbsc-publication", true),
        ("nbsc-state-durable", true),
        ("before-memory-publish", true),
        ("after-memory-publish", true),
        ("before-api-return", true),
        ("rollback-participants-durable", false),
        ("rollback-cleanup", false),
    ] {
        let root = root(point);
        seed(&root, true).close().unwrap();
        spawn(&root, point);
        outcome(&root, winner);
        if !winner {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            let mut txn = db.begin_transaction().unwrap();
            assert_eq!(
                db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
                TableId(4)
            );
            db.commit_transaction(&mut txn).unwrap();
            assert_eq!(
                db.bindings.resolve_single(TableId(4)).unwrap(),
                StorageId(4)
            );
            drop(txn);
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn drop_crash_child() {
    let Ok(root) = std::env::var("NETBADB_DROP_CHILD_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    if std::env::var_os("NETBADB_DROP_SKIP_DML").is_none() {
        db.execute_in(&mut txn, "UPDATE users SET id = 7").unwrap();
    }
    if std::env::var_os("NETBADB_DROP_SQL").is_some() {
        assert_eq!(
            db.execute_in(&mut txn, "DROP TABLE users;").unwrap(),
            ExecutionResult::AffectedRows(0)
        );
    } else {
        let target = db.resolve_drop_table("users").unwrap();
        db.drop_table_in(&mut txn, target).unwrap();
    }
    if std::env::var("NETBADB_DROP_CRASH_POINT")
        .unwrap_or_default()
        .starts_with("drop-rollback")
    {
        txn.rollback().unwrap();
    } else {
        db.commit_transaction(&mut txn).unwrap();
    }
    panic!("configured DROP crash hook was not reached");
}

fn spawn_drop(root: &Path, point: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "schema_mutation_tests::drop_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_DROP_CHILD_ROOT", root)
        .env("NETBADB_DROP_CRASH_POINT", point);
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(90),
        "{point}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn spawn_sql_drop(root: &Path, point: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "schema_mutation_tests::drop_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_DROP_CHILD_ROOT", root)
        .env("NETBADB_DROP_CRASH_POINT", point)
        .env("NETBADB_DROP_SQL", "1")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(90),
        "{point}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn drop_outcome(root: &Path, winner: bool) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            db.schema_generation(),
            SchemaGeneration(if winner { 2 } else { 1 })
        );
        assert_eq!(db.next_table_id(), Some(TableId(3)));
        assert_eq!(db.next_storage_id(), Some(StorageId(3)));
        assert!(root.join("users.heap").is_file());
        if winner {
            assert!(db.schema().table("users").is_none());
            assert_eq!(db.inspect_retired_table_resources().len(), 1);
            assert!(db.prepare_statement("SELECT id FROM users", &[]).is_err());
        } else {
            assert_eq!(
                db.query("SELECT id FROM users").unwrap().rows,
                vec![vec![ScalarValue::Int64(1)]]
            );
            assert!(db.inspect_retired_table_resources().is_empty());
        }
        assert_eq!(
            db.query("SELECT id FROM teams").unwrap().rows,
            vec![vec![ScalarValue::Int64(2)]]
        );
        db.close().unwrap();
    }
}

#[test]
fn subprocess_drop_crash_matrix_reopens_three_times_with_exact_outcomes() {
    for (point, winner) in [
        ("drop-intent-durable", false),
        ("drop-overlay-established", false),
        ("participants-prepared", false),
        ("prepared-catalog-written", false),
        ("prepared-catalog-durable", false),
        ("before-coordinator-decision", false),
        ("coordinator-durable", true),
        ("staged-heap-committed", true),
        ("drop-retirement-durable", true),
        ("before-nbsc-publication", true),
        ("during-nbsc-publication", true),
        ("nbsc-state-durable", true),
        ("drop-nbsc-durable", true),
        ("before-memory-publish", true),
        ("after-memory-publish", true),
        ("before-api-return", true),
        ("drop-rollback-cleanup", false),
    ] {
        let root = root(&format!("drop-{point}"));
        seed(&root, true).close().unwrap();
        spawn_drop(&root, point);
        drop_outcome(&root, winner);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn sql_drop_crash_loser_and_winner_reopen_three_times() {
    for (point, winner) in [
        ("before-coordinator-decision", false),
        ("coordinator-durable", true),
    ] {
        let root = root(&format!("sql-drop-{point}"));
        seed(&root, true).close().unwrap();
        spawn_sql_drop(&root, point);
        drop_outcome(&root, winner);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn schema_only_drop_uses_zero_physical_participants_and_recovers() {
    let root = root("drop-zero-participants");
    seed(&root, true).close().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "schema_mutation_tests::drop_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_DROP_CHILD_ROOT", &root)
        .env("NETBADB_DROP_SKIP_DML", "1")
        .env("NETBADB_DROP_CRASH_POINT", "coordinator-durable")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(90));
    let log = crate::CoordinatorLog::open(root.join("coordinator")).unwrap();
    let decision = log.decisions().next().unwrap();
    assert!(decision.participants.is_empty());
    assert!(decision.schema.is_some());
    drop(log);
    drop_outcome(&root, true);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn initialized_journal_and_winner_corruption_fail_closed() {
    for case in [
        "missing-journal",
        "corrupt-journal",
        "missing-heap",
        "corrupt-owner",
        "missing-prepared",
        "bad-digest",
        "final-collision",
    ] {
        let root = root(case);
        seed(&root, true).close().unwrap();
        spawn(&root, "coordinator-durable");
        let marker = crate::schema_catalog_file::marker(&root.join("catalog"))
            .unwrap()
            .unwrap();
        let journal = crate::schema_mutation_journal::SchemaMutationJournal::open(
            &root.join("catalog"),
            marker.incarnation,
        )
        .unwrap()
        .unwrap();
        let r = journal.reservations.values().next().unwrap();
        let stage = crate::schema_catalog_file::resolve(
            &root.join("catalog"),
            &crate::schema_mutation_journal::stage_locator(
                &root.join("catalog"),
                marker.incarnation,
                r.transaction,
                r.storage,
            )
            .unwrap(),
        );
        let prepared = crate::schema_catalog_file::resolve(
            &root.join("catalog"),
            &crate::schema_mutation_journal::prepared_locator(
                &root.join("catalog"),
                marker.incarnation,
                r.transaction,
            )
            .unwrap(),
        );
        match case {
            "missing-journal" => std::fs::remove_file(root.join("catalog.mutations")).unwrap(),
            "corrupt-journal" => std::fs::write(root.join("catalog.mutations"), b"bad").unwrap(),
            "missing-heap" => std::fs::remove_file(&stage).unwrap(),
            "corrupt-owner" => {
                std::fs::write(crate::schema_catalog_file::suffix(&stage, ".owner"), b"bad")
                    .unwrap()
            }
            "missing-prepared" => std::fs::remove_file(&prepared).unwrap(),
            "bad-digest" => {
                let mut b = std::fs::read(&prepared).unwrap();
                b[20] ^= 1;
                std::fs::write(&prepared, b).unwrap();
            }
            _ => {
                let destination = crate::schema_catalog_file::resolve(
                    &root.join("catalog"),
                    &r.intent.as_ref().unwrap().fragment.storages[0].locator,
                );
                crate::schema_mutation::ensure_parent(&destination).unwrap();
                std::fs::copy(&stage, destination).unwrap();
            }
        }
        for _ in 0..3 {
            assert!(
                Database::open_catalog(root.join("catalog")).is_err(),
                "{case}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn journal_codec_roundtrip_truncation_duplicates_and_incarnation() {
    let root = root("codec");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    txn.rollback().unwrap();
    let journal = db.mutation_journal.as_ref().unwrap().borrow();
    let bytes = journal.encode().unwrap();
    let decoded = crate::schema_mutation_journal::SchemaMutationJournal::decode(&bytes).unwrap();
    assert_eq!(decoded.encode().unwrap(), bytes);
    for n in 0..bytes.len() {
        assert!(
            crate::schema_mutation_journal::SchemaMutationJournal::decode(&bytes[..n]).is_err()
        );
    }
    let mut duplicate = decoded.clone();
    let mut r = duplicate.reservations.values().next().unwrap().clone();
    r.transaction.0 += 1;
    duplicate.reservations.insert(r.transaction, r);
    assert!(duplicate.encode().is_err());
    assert!(
        crate::schema_mutation_journal::SchemaMutationJournal::open(&root.join("catalog"), [1; 16])
            .is_err()
    );
    drop(journal);
    drop(txn);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn journal_rejects_create_reservation_below_retired_allocator_floor() {
    let root = root("drop-journal-allocator-conflict");
    let mut db = seed(&root, true);
    let target = db.resolve_drop_table("users").unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.drop_table_in(&mut txn, target).unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);

    let mut journal = db.mutation_journal.as_ref().unwrap().borrow().clone();
    let retired = journal.drops.values().next_back().unwrap().clone();
    let transaction = crate::DatabaseTxnId(retired.transaction.0 + 1);
    journal.reservations.insert(
        transaction,
        crate::schema_mutation_journal::Reservation {
            transaction,
            table: retired.fragment.committed.next_table_id.unwrap(),
            storage: retired.storage(),
            base_generation: retired.target_generation,
            base_epoch: retired.target_epoch,
            intent: None,
            resolved: None,
        },
    );
    assert!(journal.encode().is_err());

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn exact_expectation_accepts_added_table_after_runtime_commit() {
    let root = root("expectation");
    let mut db = seed(&root, true);
    let expected = Schema::new(db.schema().tables().to_vec()).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    let db =
        Database::open_catalog_with_expectation(root.join("catalog"), Some(&expected)).unwrap();
    assert_eq!(db.schema().tables().len(), 3);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dropped_dirty_participant_blocks_schema_admission_without_reserving() {
    let root = root("dropped-dirty");
    let mut db = seed(&root, true);
    let mut dirty = db.begin_transaction().unwrap();
    db.execute_in(&mut dirty, "UPDATE users SET id = 4")
        .unwrap();
    drop(dirty);
    let mut txn = db.begin_transaction().unwrap();
    assert!(db.create_heap_table_in(&mut txn, spec("projects")).is_err());
    assert_eq!(db.next_table_id(), Some(TableId(3)));
    txn.rollback().unwrap();
    drop(txn);
    drop(db);
    let db = Database::open_catalog(root.join("catalog")).unwrap();
    assert!(db.schema().table("projects").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_staging_requires_rollback_and_never_decides_commit() {
    let root = root("stage-failure");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    let snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let stage = crate::schema_catalog_file::resolve(
        &root.join("catalog"),
        &crate::schema_mutation_journal::stage_locator(
            &root.join("catalog"),
            snapshot.incarnation,
            txn.id(),
            StorageId(3),
        )
        .unwrap(),
    );
    crate::schema_mutation::ensure_parent(&stage).unwrap();
    std::fs::write(
        crate::schema_catalog_file::suffix(&stage, ".owner"),
        b"injected create-new collision",
    )
    .unwrap();
    assert!(db.create_heap_table_in(&mut txn, spec("projects")).is_err());
    assert_eq!(txn.state(), TransactionState::RollbackRequired);
    assert!(db.commit_transaction(&mut txn).is_err());
    assert_eq!(
        db.coordinator
            .as_ref()
            .unwrap()
            .borrow()
            .decisions()
            .count(),
        0
    );
    txn.rollback().unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(4)));
    assert_eq!(db.next_storage_id(), Some(StorageId(4)));
    drop(txn);
    db.close().unwrap();
    Database::open_catalog(root.join("catalog"))
        .unwrap()
        .close()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_lsm_and_range_participants_commit_with_new_heap() {
    for range in [false, true] {
        let root = root(if range { "range-create" } else { "lsm-create" });
        let mut db = if range {
            Database::create_catalog_with_placements(
                root.join("catalog"),
                vec![crate::TablePlacementSpec::range_partitioned(
                    old_table(1, "users"),
                    ColumnId(1),
                    vec![
                        crate::RangePartitionSpec::new(
                            netbadb_types::PartitionId(9),
                            root.join("lower"),
                            None,
                            Some(ScalarValue::Int64(0)),
                        ),
                        crate::RangePartitionSpec::new(
                            netbadb_types::PartitionId(10),
                            root.join("upper"),
                            Some(ScalarValue::Int64(0)),
                            None,
                        ),
                    ],
                )],
                crate::PartitionCatalogConfig::new(
                    root.join("partitions"),
                    root.join("coordinator"),
                ),
            )
            .unwrap()
        } else {
            Database::create_catalog(
                root.join("catalog"),
                vec![TableStorageCreateSpec::lsm(
                    root.join("lsm"),
                    old_table(1, "users"),
                    ColumnId(1),
                )],
                Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
            )
            .unwrap()
        };
        let next_partition = db.next_partition_id();
        let next_storage = db.next_storage_id().unwrap();
        let mut txn = db.begin_transaction().unwrap();
        db.execute_in(&mut txn, "INSERT INTO users (id) VALUES (1)")
            .unwrap();
        let id = db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
        insert(&mut db, &mut txn);
        db.commit_transaction(&mut txn).unwrap();
        drop(txn);
        db.close().unwrap();
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(db.schema_generation(), SchemaGeneration(2));
            assert_eq!(db.next_partition_id(), next_partition);
            assert_eq!(db.bindings.resolve_single(id).unwrap(), next_storage);
            assert_eq!(
                db.query("SELECT id FROM users").unwrap().rows,
                vec![vec![ScalarValue::Int64(1)]]
            );
            assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn generation_and_identity_exhaustion_are_checked_before_staging() {
    for field in ["generation", "epoch", "revision", "table", "storage"] {
        let root = root(field);
        let mut db = seed(&root, true);
        let mut snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
        match field {
            "generation" => snapshot.committed.generation.0 = u64::MAX,
            "epoch" => snapshot.epoch = u64::MAX,
            "revision" => db.catalog_generation = u64::MAX,
            "table" => snapshot.committed.next_table_id = None,
            _ => snapshot.committed.next_storage_id = None,
        }
        // Test-only fixture replacement keeps the v1 marker CRC valid.
        let bytes = snapshot.encode().unwrap();
        std::fs::write(root.join("catalog"), &bytes).unwrap();
        let mut marker = std::fs::read(root.join("catalog.state")).unwrap();
        marker[36..44].copy_from_slice(&snapshot.epoch.to_le_bytes());
        marker[44..48].copy_from_slice(&crc32c::crc32c(&bytes).to_le_bytes());
        marker[12..16].fill(0);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&marker[..12]), &marker[16..]);
        marker[12..16].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(root.join("catalog.state"), marker).unwrap();
        db.committed = snapshot.committed;
        let mut txn = db.begin_transaction().unwrap();
        assert!(matches!(
            db.create_heap_table_in(&mut txn, spec("projects")),
            Err(crate::DatabaseError::SchemaMutation(
                SchemaMutationError::IdentityExhausted(_)
            ))
        ));
        assert!(!root.join("catalog.mutations").exists());
        txn.rollback().unwrap();
        drop(txn);
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[ignore = "explicit deterministic fuzz corpus generation"]
fn write_schema_mutation_fuzz_corpus() {
    let output = PathBuf::from(
        std::env::var("NETBADB_ROUND18_CORPUS").expect("explicit corpus output directory"),
    );
    std::fs::create_dir_all(output.join("schema_mutation_decode")).unwrap();
    std::fs::create_dir_all(output.join("coordinator_log_decode")).unwrap();
    let root = root("fuzz-corpus");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    let mut journal = db.mutation_journal.as_ref().unwrap().borrow().clone();
    journal.incarnation = [7; 16];
    journal.coordinator = "coordinator".into();
    for reservation in journal.reservations.values_mut() {
        let intent = reservation.intent.as_mut().unwrap();
        intent.fragment.incarnation = [7; 16];
        intent.fragment.coordinator = Some("coordinator".into());
        intent.fragment.storages[0].locator = "resources/storage/3.heap".into();
        intent.snapshot_digest = [9; 32];
    }
    for (name, intent, outcome) in [
        ("reservation-v1", false, None),
        ("intent-v1", true, None),
        ("abort-v1", true, Some(false)),
        ("commit-v1", true, Some(true)),
    ] {
        let mut sample = journal.clone();
        for r in sample.reservations.values_mut() {
            if !intent {
                r.intent = None;
            }
            r.resolved = outcome;
        }
        let bytes = sample.encode().unwrap();
        std::fs::write(output.join("schema_mutation_decode").join(name), bytes).unwrap();
    }
    txn.rollback().unwrap();
    drop(txn);
    let target = db.resolve_drop_table("users").unwrap();
    let mut drop_loser = db.begin_transaction().unwrap();
    db.drop_table_in(&mut drop_loser, target).unwrap();
    let drop_intent = db
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .encode()
        .unwrap();
    std::fs::write(
        output.join("schema_mutation_decode/drop-intent-v1"),
        &drop_intent,
    )
    .unwrap();
    std::fs::write(
        output.join("schema_mutation_decode/drop-truncated-v1"),
        &drop_intent[..drop_intent.len() - 7],
    )
    .unwrap();
    drop_loser.rollback().unwrap();
    drop(drop_loser);
    std::fs::write(
        output.join("schema_mutation_decode/drop-loser-v1"),
        db.mutation_journal
            .as_ref()
            .unwrap()
            .borrow()
            .encode()
            .unwrap(),
    )
    .unwrap();
    let mut drop_winner = db.begin_transaction().unwrap();
    db.drop_table_in(&mut drop_winner, target).unwrap();
    db.commit_transaction(&mut drop_winner).unwrap();
    drop(drop_winner);
    let winner = db.mutation_journal.as_ref().unwrap().borrow().clone();
    std::fs::write(
        output.join("schema_mutation_decode/drop-winner-v1"),
        winner.encode().unwrap(),
    )
    .unwrap();
    let mut gc_intent = winner.clone();
    let gc = crate::schema_mutation_journal::RetiredHeapGcRecord {
        coordinator_horizon: gc_intent.drops.values().next_back().unwrap().transaction,
        manifest_digest: [11; 32],
        complete: false,
    };
    gc_intent.drops.values_mut().next_back().unwrap().gc = Some(gc);
    std::fs::write(
        output.join("schema_mutation_decode/drop-gc-intent-v1"),
        gc_intent.encode().unwrap(),
    )
    .unwrap();
    gc_intent
        .drops
        .values_mut()
        .next_back()
        .unwrap()
        .gc
        .as_mut()
        .unwrap()
        .complete = true;
    std::fs::write(
        output.join("schema_mutation_decode/drop-gc-complete-v1"),
        gc_intent.encode().unwrap(),
    )
    .unwrap();
    let mut retained = winner;
    retained.drops.values_mut().next_back().unwrap().resolved = None;
    std::fs::write(
        output.join("schema_mutation_decode/drop-retained-v1"),
        retained.encode().unwrap(),
    )
    .unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-drop-zero-v2"),
        std::fs::read(root.join("coordinator")).unwrap(),
    )
    .unwrap();
    let reference = crate::coordinator_log::SchemaParticipantReference {
        incarnation: [7; 16],
        target_epoch: 2,
        digest: [9; 32],
    };
    let log_path = root.join("seed-coordinator");
    let mut log = crate::CoordinatorLog::create(&log_path).unwrap();
    log.commit_schema_decision(
        netbadb_types::DatabaseTxnId(3),
        &[
            crate::coordinator_log::CoordinatorParticipant {
                storage_id: StorageId(1),
                physical_txn_id: netbadb_types::TxnId(7),
            },
            crate::coordinator_log::CoordinatorParticipant {
                storage_id: StorageId(3),
                physical_txn_id: netbadb_types::TxnId(1),
            },
        ],
        Some(&reference),
    )
    .unwrap();
    let bytes = std::fs::read(&log_path).unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-decision-v2"),
        &bytes,
    )
    .unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-decision-v2-truncated"),
        &bytes[..bytes.len() - 5],
    )
    .unwrap();
    log.complete(netbadb_types::DatabaseTxnId(3)).unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-complete-v2"),
        std::fs::read(&log_path).unwrap(),
    )
    .unwrap();
    drop(log);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn subsequent_winner_recovers_past_prior_completed_journal_history() {
    let root = root("second-winner");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, CreateTableSpec::new("first", vec![]))
        .unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    spawn(&root, "during-nbsc-publication");
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(db.schema_generation(), SchemaGeneration(3));
        assert_eq!(db.schema().table("projects").unwrap().id, TableId(4));
        assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dropped_schema_handle_requires_recovery_and_consumes_ids() {
    let root = root("dropped-schema");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    insert(&mut db, &mut txn);
    drop(txn);
    assert!(matches!(
        db.begin_transaction(),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::RecoveryRequired
        ))
    ));
    drop(db);
    outcome(&root, false);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn automatic_coordinator_recovers_preexisting_writes_on_both_outcomes() {
    for winner in [false, true] {
        let root = root(if winner {
            "automatic-winner"
        } else {
            "automatic-loser"
        });
        seed(&root, false).close().unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "schema_mutation_tests::create_crash_child"])
            .env("NETBADB_CREATE_CHILD_ROOT", &root)
            .env("NETBADB_CREATE_SKIP_SECOND_WRITE", "1")
            .env(
                "NETBADB_CREATE_CRASH_POINT",
                if winner {
                    "coordinator-durable"
                } else {
                    "participants-prepared"
                },
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(90));
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(
                db.schema_generation(),
                SchemaGeneration(if winner { 2 } else { 1 })
            );
            assert_eq!(
                db.query("SELECT id FROM users").unwrap().rows,
                vec![vec![ScalarValue::Int64(if winner { 7 } else { 1 })]]
            );
            assert_eq!(db.next_table_id(), Some(TableId(4)));
            assert_eq!(db.schema().table("projects").is_some(), winner);
            if winner {
                assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
            }
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn uncertain_rollback_journal_sync_requires_recovery_without_publishing() {
    let root = root("rollback-sync");
    let mut db = seed(&root, true);
    let mut txn = db.begin_transaction().unwrap();
    db.create_heap_table_in(&mut txn, spec("projects")).unwrap();
    insert(&mut db, &mut txn);
    db.mutation_journal
        .as_ref()
        .unwrap()
        .borrow_mut()
        .inject_sync_failure();
    assert!(txn.rollback().is_err());
    assert_eq!(txn.state(), TransactionState::RollbackPending);
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert!(txn.rollback().is_err());
    drop(txn);
    drop(db);
    outcome(&root, false);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_journal_reopen_completes_activation_before_reservation() {
    let root = root("activation-reopen");
    let db = seed(&root, true);
    let catalog = root.join("catalog");
    let snapshot = crate::schema_catalog_file::load(&catalog).unwrap();
    crate::schema_mutation_journal::SchemaMutationJournal::initialize(
        &catalog,
        snapshot.incarnation,
        "coordinator".into(),
    )
    .unwrap();
    // Exact durable state of a crash between empty-journal and witness writes.
    std::fs::remove_file(root.join("catalog.mutations.state")).unwrap();
    db.close().unwrap();
    for _ in 0..3 {
        let db = Database::open_catalog(&catalog).unwrap();
        assert_eq!(db.next_table_id(), Some(TableId(3)));
        assert!(!root.join("catalog.mutations.state").exists());
        db.close().unwrap();
    }
    let mut db = Database::open_catalog(&catalog).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(
        db.create_heap_table_in(&mut txn, spec("projects")).unwrap(),
        TableId(3)
    );
    assert!(root.join("catalog.mutations.state").is_file());
    insert(&mut db, &mut txn);
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    db.close().unwrap();
    for _ in 0..3 {
        let mut db = Database::open_catalog(&catalog).unwrap();
        assert_eq!(db.query("SELECT * FROM projects").unwrap().rows, expected());
        db.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn journal_capacity_rejects_before_reservation_and_leaves_resolution_room() {
    let root = root("journal-capacity");
    let db = seed(&root, true);
    let catalog = root.join("catalog");
    let snapshot = crate::schema_catalog_file::load(&catalog).unwrap();
    let mut journal = crate::schema_mutation_journal::SchemaMutationJournal::initialize(
        &catalog,
        snapshot.incarnation,
        "coordinator".into(),
    )
    .unwrap();
    // 65,534 valid records leave room for reserve + intent but not resolution.
    // These are durable aborts before intent, so no physical artifacts exist.
    for id in 1..=32767 {
        let transaction = netbadb_types::DatabaseTxnId(id);
        journal.reservations.insert(
            transaction,
            crate::schema_mutation_journal::Reservation {
                transaction,
                table: TableId(id + 2),
                storage: StorageId(id + 2),
                base_generation: SchemaGeneration(1),
                base_epoch: 1,
                intent: None,
                resolved: Some(false),
            },
        );
    }
    let bytes = journal.encode().unwrap();
    std::fs::write(root.join("catalog.mutations"), &bytes).unwrap();
    db.close().unwrap();
    let mut db = Database::open_catalog(&catalog).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    assert!(db.create_heap_table_in(&mut txn, spec("projects")).is_err());
    assert_eq!(txn.state(), TransactionState::Active);
    assert_eq!(db.next_table_id(), Some(TableId(32770)));
    assert_eq!(db.next_storage_id(), Some(StorageId(32770)));
    assert_eq!(
        std::fs::read(root.join("catalog.mutations")).unwrap(),
        bytes
    );
    assert!(db.schema_writer.get().is_none());
    txn.rollback().unwrap();
    drop(txn);
    db.close().unwrap();
    let db = Database::open_catalog(&catalog).unwrap();
    assert_eq!(db.next_table_id(), Some(TableId(32770)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
