use std::path::{Path, PathBuf};
use std::process::Command;

use netbadb_inspect::{PlanNodeInspection, StatementPlanInspection};
use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
use netbadb_storage::{TableStorage, heap_resource_components};
use netbadb_types::{
    ColumnId, IndexName, PhysicalType, ScalarValue, SemanticType, StorageId, TableId, TxnId,
};

use crate::{
    AlterTableOperation, AlterTableSpec, CreateColumnSpec, CreateTableSpec, Database,
    DatabaseCoordinatorConfig, ExecutionResult, RetiredHeapGcState, RetiredTableResource,
    SchemaGeneration, SchemaMutationError, TableSchemaVersion, TableStorageCreateSpec,
    TransactionState,
};

#[test]
fn heap_schema_rewrite_add_nullable_preserves_identity_indexes_and_same_txn_dml() {
    let root = root("rewrite-add-basic");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects (id, name) VALUES (1, 'one')")
        .unwrap();
    db.execute("INSERT INTO projects (id, name) VALUES (2, NULL)")
        .unwrap();
    db.execute("INSERT INTO projects (id, name) VALUES (9, 'dead')")
        .unwrap();
    db.execute("UPDATE projects SET name = 'one-updated' WHERE id = 1")
        .unwrap();
    db.execute("DELETE FROM projects WHERE id = 9").unwrap();
    let index = db
        .create_named_index(
            IndexName::new("projects_id_idx").unwrap(),
            table,
            ColumnId(1),
        )
        .unwrap();
    db.analyze(table).unwrap();
    let analyzed = db
        .inspect_catalog()
        .unwrap()
        .tables
        .into_iter()
        .find(|entry| entry.table_id == table)
        .unwrap();
    assert!(analyzed.statistics.is_some());
    assert!(analyzed.indexes[0].statistics.is_some());
    let old_storage = db.bindings.resolve_single(table).unwrap();
    let old_fingerprint = db
        .schema()
        .table("projects")
        .unwrap()
        .fingerprint()
        .unwrap();
    let old_table = db.schema().table("projects").unwrap().clone();
    let target = db.resolve_alter_table("projects").unwrap();
    let mut alter = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut alter,
        AlterTableSpec::new(
            target,
            AlterTableOperation::AddNullableColumn {
                name: "active".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    assert_eq!(db.next_column_id(table), Some(ColumnId(4)));
    assert_eq!(
        rows(
            db.execute_in(
                &mut alter,
                "SELECT id, name, active FROM projects ORDER BY id"
            )
            .unwrap()
        ),
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("one-updated".into()),
                ScalarValue::Null
            ],
            vec![ScalarValue::Int64(2), ScalarValue::Null, ScalarValue::Null],
        ]
    );
    db.execute_in(
        &mut alter,
        "INSERT INTO projects (id, name, active) VALUES (3, 'three', true)",
    )
    .unwrap();
    db.commit_transaction(&mut alter).unwrap();
    drop(alter);
    assert_eq!(
        db.bindings.resolve_single(table).unwrap(),
        StorageId(old_storage.0 + 1)
    );
    assert_eq!(db.table_schema_version(table), Some(TableSchemaVersion(2)));
    assert_ne!(
        db.schema()
            .table("projects")
            .unwrap()
            .fingerprint()
            .unwrap(),
        old_fingerprint
    );
    assert_eq!(db.indexes(table).unwrap()[0].id, index.id);
    let active_catalog = db.inspect_catalog().unwrap();
    assert_eq!(
        active_catalog
            .tables
            .iter()
            .filter(|entry| entry.table_id == table)
            .count(),
        1
    );
    let rewritten = active_catalog
        .tables
        .into_iter()
        .find(|entry| entry.table_id == table)
        .unwrap();
    assert_eq!(rewritten.statistics, None);
    assert_eq!(rewritten.indexes[0].statistics, None);
    assert_eq!(
        db.query("SELECT id, active FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Null],
            vec![ScalarValue::Int64(2), ScalarValue::Null],
            vec![ScalarValue::Int64(3), ScalarValue::Bool(true)],
        ]
    );
    let retired = db.inspect_replacement_retired_heaps();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].old_storage_id, old_storage);
    let old_path = crate::schema_catalog_file::resolve(
        &root.join("catalog"),
        &retired[0].old_relative_locator,
    );
    let old_bundle_before_maintenance = heap_bundle_bytes(&old_path);
    let active_snapshot = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let new_storage = db.bindings.resolve_single(table).unwrap();
    let new_locator = &active_snapshot
        .storages
        .iter()
        .find(|storage| storage.id == new_storage)
        .unwrap()
        .locator;
    let new_path = crate::schema_catalog_file::resolve(&root.join("catalog"), new_locator);
    let new_bundle = heap_bundle_bytes(&new_path);
    let target_index_rows = {
        let target = db.registry.get_mut(new_storage).unwrap();
        let access_path = target.access_paths()[0].id;
        let view = target.read_view().unwrap();
        target
            .point_lookup_columns_with_view(
                access_path,
                &ScalarValue::Int64(1),
                &[ColumnId(1)],
                &view,
            )
            .unwrap()
    };
    assert_eq!(target_index_rows.len(), 1);
    assert_eq!(target_index_rows[0].0.storage_id(), new_storage);
    eprintln!(
        "round24 physical rewrite: table={} old_storage={} old_path={} old_bundle_bytes={} new_storage={} new_path={} new_bundle_bytes={} retained_total_bytes={}",
        table.0,
        old_storage.0,
        old_path.display(),
        heap_bundle_size(&old_bundle_before_maintenance),
        new_storage.0,
        new_path.display(),
        heap_bundle_size(&new_bundle),
        heap_bundle_size(&old_bundle_before_maintenance) + heap_bundle_size(&new_bundle),
    );

    // Maintenance is routed only to active S2. The retained S1 bundle remains
    // byte-for-byte recovery evidence while target statistics are repopulated.
    db.vacuum(table).unwrap();
    db.analyze(table).unwrap();
    let analyzed_target = db
        .inspect_catalog()
        .unwrap()
        .tables
        .into_iter()
        .find(|entry| entry.table_id == table)
        .unwrap();
    assert!(analyzed_target.statistics.is_some());
    assert!(analyzed_target.indexes[0].statistics.is_some());
    assert_eq!(heap_bundle_bytes(&old_path), old_bundle_before_maintenance);

    // The old schema fragment, rows, and logical index remain independently
    // openable for recovery without entering the active Core registry.
    let mut old = TableStorage::open_heap(&old_path, old_table).unwrap();
    assert_eq!(old.storage_id(), old_storage);
    assert_eq!(old.indexes()[0].id, index.id);
    let old_view = old.read_view().unwrap();
    let old_access_path = old.access_paths()[0].id;
    let old_index_rows = old
        .point_lookup_columns_with_view(
            old_access_path,
            &ScalarValue::Int64(1),
            &[ColumnId(1)],
            &old_view,
        )
        .unwrap();
    assert_eq!(old_index_rows.len(), 1);
    assert_eq!(old_index_rows[0].0.storage_id(), old_storage);
    assert_ne!(old_index_rows[0].0, target_index_rows[0].0);
    let mut old_rows = old
        .scan_columns_with_view(&[ColumnId(1), ColumnId(2)], &old_view)
        .unwrap()
        .into_iter()
        .map(|(_, values)| values)
        .collect::<Vec<_>>();
    old_rows.sort_by_key(|values| match values[0] {
        ScalarValue::Int64(value) => value,
        _ => i64::MAX,
    });
    assert_eq!(
        old_rows,
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("one-updated".into())
            ],
            vec![ScalarValue::Int64(2), ScalarValue::Null],
        ]
    );
    old.close().unwrap();
    db.close().unwrap();
    let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        reopened.table_schema_version(table),
        Some(TableSchemaVersion(2))
    );
    assert_eq!(reopened.indexes(table).unwrap()[0].id, index.id);
    assert_eq!(
        reopened
            .query("SELECT id, active FROM projects ORDER BY id")
            .unwrap()
            .rows
            .len(),
        3
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_rollback_burns_storage_and_column_ids_and_discards_target_dml() {
    let root = root("rewrite-rollback-gaps");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let old_storage = db.bindings.resolve_single(table).unwrap();
    db.flush().unwrap();
    let catalog_path = root.join("catalog");
    let snapshot = crate::schema_catalog_file::load(&catalog_path).unwrap();
    let old_locator = snapshot
        .storages
        .iter()
        .find(|storage| storage.id == old_storage)
        .unwrap()
        .locator
        .clone();
    let old_path = crate::schema_catalog_file::resolve(&catalog_path, &old_locator);
    let old_bundle = heap_bundle_bytes(&old_path);
    let old_catalog = std::fs::read(&catalog_path).unwrap();
    assert_eq!(db.next_column_id(table), Some(ColumnId(3)));
    let first_storage = db.next_storage_id().unwrap();
    let mut loser = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut loser,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::AddNullableColumn {
                name: "loser".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    db.execute_in(
        &mut loser,
        "INSERT INTO projects (id, name, loser) VALUES (2, 'private', true)",
    )
    .unwrap();
    db.execute_in(
        &mut loser,
        "UPDATE projects SET name = 'private-update' WHERE id = 1",
    )
    .unwrap();
    loser.rollback().unwrap();
    drop(loser);
    db.flush().unwrap();
    assert_eq!(db.bindings.resolve_single(table).unwrap(), old_storage);
    assert_eq!(std::fs::read(&catalog_path).unwrap(), old_catalog);
    assert_eq!(heap_bundle_bytes(&old_path), old_bundle);
    assert_eq!(db.next_storage_id(), Some(StorageId(first_storage.0 + 1)));
    assert_eq!(db.next_column_id(table), Some(ColumnId(4)));
    assert_eq!(
        db.query("SELECT id, name FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())]]
    );

    let mut winner = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut winner,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::AddNullableColumn {
                name: "winner".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut winner).unwrap();
    drop(winner);
    assert_eq!(
        db.bindings.resolve_single(table).unwrap(),
        StorageId(first_storage.0 + 1)
    );
    assert_eq!(
        db.schema().table("projects").unwrap().columns[2].id,
        ColumnId(4)
    );
    assert_eq!(db.next_column_id(table), Some(ColumnId(5)));
    assert_eq!(db.next_storage_id(), Some(StorageId(first_storage.0 + 2)));
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.next_column_id(table), Some(ColumnId(5)));
    assert_eq!(
        reopened.next_storage_id(),
        Some(StorageId(first_storage.0 + 2))
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_rename_is_private_and_rollback_restores_exact_committed_state() {
    let root = root("rewrite-rename-rollback");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
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
    db.execute("INSERT INTO projects VALUES (1)").unwrap();
    db.flush().unwrap();

    let old_storage = db.bindings.resolve_single(table).unwrap();
    let reserved_storage = db.next_storage_id().unwrap();
    let old_table = db.schema().table("projects").unwrap().clone();
    let old_fingerprint = old_table.fingerprint().unwrap();
    let old_catalog = std::fs::read(root.join("catalog")).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut transaction,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::RenameTable {
                new_name: "work".into(),
            },
        ),
    )
    .unwrap();

    assert_eq!(db.schema().table("projects").unwrap().id, table);
    assert!(db.schema().table("work").is_none());
    assert!(
        db.execute_in(&mut transaction, "SELECT id FROM projects")
            .is_err()
    );
    let prepared = db
        .prepare_statement_in(&transaction, "SELECT id FROM work", &[])
        .unwrap();
    assert_eq!(
        rows(
            db.execute_prepared_in(&mut transaction, &prepared, &[])
                .unwrap()
        ),
        vec![vec![ScalarValue::Int64(1)]]
    );
    transaction.rollback().unwrap();
    drop(transaction);

    assert_eq!(db.schema().table("projects").unwrap(), &old_table);
    assert!(db.schema().table("work").is_none());
    assert_eq!(db.bindings.resolve_single(table).unwrap(), old_storage);
    assert_eq!(db.table_schema_version(table), Some(TableSchemaVersion(1)));
    assert_eq!(
        db.schema()
            .table("projects")
            .unwrap()
            .fingerprint()
            .unwrap(),
        old_fingerprint
    );
    assert_eq!(std::fs::read(root.join("catalog")).unwrap(), old_catalog);
    assert_eq!(
        db.next_storage_id(),
        Some(StorageId(reserved_storage.0 + 1))
    );
    assert!(db.execute_prepared(&prepared, &[]).is_err());
    assert_eq!(
        db.query("SELECT id FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(1)]]
    );
    assert!(db.inspect_replacement_retired_heaps().is_empty());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_invalidates_stale_manifest_and_sdk_schema_expectations() {
    let root = root("rewrite-stale-expectation");
    let catalog = root.join("catalog");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    let old_table = db.schema().table("projects").unwrap().clone();
    let old_fingerprint = old_table.fingerprint().unwrap();
    let stale_expectation = Schema::new(vec![old_table]).unwrap();

    let mut rewrite = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut rewrite,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::AddNullableColumn {
                name: "active".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut rewrite).unwrap();
    drop(rewrite);
    let new_fingerprint = db
        .schema()
        .table("projects")
        .unwrap()
        .fingerprint()
        .unwrap();
    assert_ne!(new_fingerprint, old_fingerprint);
    assert_eq!(db.schema().table("projects").unwrap().id, table);
    db.close().unwrap();

    assert!(Database::open_catalog_with_expectation(&catalog, Some(&stale_expectation)).is_err());
    let reopened = Database::open_catalog(&catalog).unwrap();
    assert_eq!(
        reopened
            .schema()
            .table("projects")
            .unwrap()
            .fingerprint()
            .unwrap(),
        new_fingerprint
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_preserves_sparse_index_identity_and_high_water() {
    let root = root("rewrite-index-high-water");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("a", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("b", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("c", SemanticType::physical(PhysicalType::Int64), false),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects VALUES (1, 2, 3)").unwrap();
    let first = db.create_index(table, ColumnId(1)).unwrap();
    db.drop_index(table, first.id).unwrap();
    let second = db.create_index(table, ColumnId(2)).unwrap();
    assert_eq!(first.id.0 + 1, second.id.0);
    let mut rewrite = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut rewrite,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::RenameColumn {
                column_id: ColumnId(3),
                new_name: "renamed_c".into(),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut rewrite).unwrap();
    drop(rewrite);
    assert_eq!(db.indexes(table).unwrap()[0].id, second.id);
    let third = db.create_index(table, ColumnId(1)).unwrap();
    assert_eq!(second.id.0 + 1, third.id.0);
    assert_eq!(
        db.query("SELECT a FROM projects WHERE b = 2").unwrap().rows,
        vec![vec![ScalarValue::Int64(1)]]
    );
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        reopened
            .indexes(table)
            .unwrap()
            .iter()
            .map(|index| index.id)
            .collect::<Vec<_>>(),
        vec![second.id, third.id]
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_preserves_index_and_join_planner_paths() {
    let root = root("rewrite-planner-paths");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new(
                        "name",
                        SemanticType::physical(PhysicalType::Text),
                        false,
                    ),
                    CreateColumnSpec::new(
                        "code",
                        SemanticType::physical(PhysicalType::Int64),
                        false,
                    ),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    let payload = "x".repeat(500);
    for id in 0..128_i64 {
        db.execute(&format!(
            "INSERT INTO projects VALUES ({id}, 'name-{id}-{payload}', {id})"
        ))
        .unwrap();
    }
    let id_index = db.create_index(table, ColumnId(1)).unwrap();
    let name_index = db.create_index(table, ColumnId(2)).unwrap();

    let mut rewrite = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut rewrite,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::RenameColumn {
                column_id: ColumnId(2),
                new_name: "title".into(),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut rewrite).unwrap();
    drop(rewrite);
    assert_eq!(
        db.indexes(table)
            .unwrap()
            .iter()
            .map(|index| (index.id, index.column_id))
            .collect::<Vec<_>>(),
        vec![(id_index.id, ColumnId(1)), (name_index.id, ColumnId(2))]
    );
    db.analyze(table).unwrap();
    db.analyze(TableId(2)).unwrap();

    let point = db
        .inspect_statement(&format!(
            "SELECT id FROM projects WHERE title = 'name-42-{payload}'"
        ))
        .unwrap();
    assert!(plan_contains(query_plan(&point.plan), PlanKind::IndexScan));
    assert_eq!(
        db.query(&format!(
            "SELECT id FROM projects WHERE title = 'name-42-{payload}'"
        ))
        .unwrap()
        .rows,
        vec![vec![ScalarValue::Int64(42)]]
    );

    let range = db
        .inspect_statement("SELECT id FROM projects WHERE id >= 10 AND id < 20")
        .unwrap();
    assert!(plan_contains(
        query_plan(&range.plan),
        PlanKind::RangeIndexScan
    ));
    assert_eq!(
        db.query("SELECT id FROM projects WHERE id >= 10 AND id < 20")
            .unwrap()
            .rows
            .len(),
        10
    );

    let indexed_join = db
        .inspect_statement("SELECT p.id FROM teams t JOIN projects p ON t.id = p.id WHERE t.id = 2")
        .unwrap();
    assert!(plan_contains(
        query_plan(&indexed_join.plan),
        PlanKind::IndexNestedLoopJoin
    ));
    assert_eq!(
        db.query("SELECT p.id FROM teams t JOIN projects p ON t.id = p.id WHERE t.id = 2")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Int64(2)]]
    );

    for id in 0..128_i64 {
        db.execute(&format!("INSERT INTO teams VALUES ({})", id + 1_000))
            .unwrap();
    }
    db.analyze(TableId(2)).unwrap();
    let hash_join = db
        .inspect_statement("SELECT p.id FROM teams t JOIN projects p ON t.id = p.code")
        .unwrap();
    assert!(plan_contains(
        query_plan(&hash_join.plan),
        PlanKind::HashJoin
    ));
    assert_eq!(
        db.query("SELECT p.id FROM teams t JOIN projects p ON t.id = p.code")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Int64(2)]]
    );

    let retired = db.inspect_replacement_retired_heaps()[0].clone();
    let active_storage = db.bindings.resolve_single(table).unwrap();
    let active = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let active_locator = &active
        .storages
        .iter()
        .find(|storage| storage.id == active_storage)
        .unwrap()
        .locator;
    let active_path = crate::schema_catalog_file::resolve(&root.join("catalog"), active_locator);
    let active_bundle = heap_bundle_bytes(&active_path);
    let before_gc = db.inspect_replacement_retired_heap_gc(&retired).unwrap();
    let report = db.gc_replacement_retired_heap(&retired).unwrap();
    eprintln!(
        "index-heavy replacement GC: old_storage={} active_storage={} files_deleted={} bytes_deleted={}",
        retired.old_storage_id.0, active_storage.0, report.files_deleted, report.bytes_deleted
    );
    assert_eq!(report.bytes_deleted, before_gc.total_present_bytes);
    assert!(
        before_gc
            .components
            .iter()
            .all(|component| !component.path.exists())
    );
    assert_eq!(heap_bundle_bytes(&active_path), active_bundle);
    assert_eq!(
        db.indexes(table)
            .unwrap()
            .iter()
            .map(|index| (index.id, index.column_id))
            .collect::<Vec<_>>(),
        vec![(id_index.id, ColumnId(1)), (name_index.id, ColumnId(2))]
    );
    assert_eq!(
        db.query(&format!(
            "SELECT id FROM projects WHERE title = 'name-42-{payload}'"
        ))
        .unwrap()
        .rows,
        vec![vec![ScalarValue::Int64(42)]]
    );

    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert!(plan_contains(
        query_plan(
            &reopened
                .inspect_statement(&format!(
                    "SELECT id FROM projects WHERE title = 'name-42-{payload}'"
                ))
                .unwrap()
                .plan
        ),
        PlanKind::IndexScan
    ));
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_all_operations_preserve_logical_id_and_advance_physical_identity() {
    let root = root("rewrite-all-operations");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                    CreateColumnSpec::new("flag", SemanticType::physical(PhysicalType::Bool), true),
                    CreateColumnSpec::new(
                        "code",
                        SemanticType::named("UserId", PhysicalType::Int64),
                        true,
                    ),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects VALUES (1, 'one', true, 11)")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two', false, 22)")
        .unwrap();
    let index = db
        .create_named_index(
            IndexName::new("projects_id_idx").unwrap(),
            table,
            ColumnId(1),
        )
        .unwrap();
    let name_index = db
        .create_named_index(
            IndexName::new("projects_name_idx").unwrap(),
            table,
            ColumnId(2),
        )
        .unwrap();
    let code_index = db
        .create_named_index(
            IndexName::new("projects_code_idx").unwrap(),
            table,
            ColumnId(4),
        )
        .unwrap();
    let index_identity = vec![
        (index.id, index.name.clone(), index.column_id),
        (name_index.id, name_index.name.clone(), name_index.column_id),
        (code_index.id, code_index.name.clone(), code_index.column_id),
    ];
    let unrelated = db.prepare_statement("SELECT id FROM teams", &[]).unwrap();
    let stale = db
        .prepare_statement("SELECT id FROM projects", &[])
        .unwrap();
    let initial_storage = db.bindings.resolve_single(table).unwrap();
    let mut current_name = "projects".to_owned();
    let mut expected_rows = 2;
    let operations = vec![
        AlterTableOperation::RenameTable {
            new_name: "work".into(),
        },
        AlterTableOperation::RenameColumn {
            column_id: ColumnId(2),
            new_name: "title".into(),
        },
        AlterTableOperation::AddNullableColumn {
            name: "extra".into(),
            data_type: SemanticType::physical(PhysicalType::Bool),
        },
        AlterTableOperation::DropColumn {
            column_id: ColumnId(3),
        },
        AlterTableOperation::SetNotNull {
            column_id: ColumnId(2),
        },
        AlterTableOperation::DropNotNull {
            column_id: ColumnId(1),
        },
        AlterTableOperation::ChangeNominalType {
            column_id: ColumnId(4),
            target_type: SemanticType::named("AccountId", PhysicalType::Int64),
        },
    ];
    for (position, operation) in operations.into_iter().enumerate() {
        let before = db.bindings.resolve_single(table).unwrap();
        let target = db.resolve_alter_table(&current_name).unwrap();
        let old_prepared = db
            .prepare_statement(&format!("SELECT id FROM {current_name}"), &[])
            .unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        db.rewrite_heap_table_schema_in(
            &mut transaction,
            AlterTableSpec::new(target, operation.clone()),
        )
        .unwrap();
        if matches!(operation, AlterTableOperation::RenameTable { .. }) {
            current_name = "work".into();
            assert!(
                db.prepare_statement_in(&transaction, "SELECT id FROM projects", &[])
                    .is_err()
            );
        }
        assert!(
            db.execute_prepared_in(&mut transaction, &old_prepared, &[])
                .is_err()
        );
        assert_eq!(
            rows(
                db.execute_in(
                    &mut transaction,
                    &format!("SELECT id FROM {current_name} ORDER BY id")
                )
                .unwrap()
            )
            .len(),
            expected_rows
        );
        if matches!(operation, AlterTableOperation::SetNotNull { .. }) {
            assert!(
                db.execute_in(
                    &mut transaction,
                    "INSERT INTO work (id, title, code, extra) VALUES (3, NULL, 33, NULL)",
                )
                .is_err()
            );
        }
        if matches!(operation, AlterTableOperation::DropNotNull { .. }) {
            db.execute_in(
                &mut transaction,
                "INSERT INTO work (id, title, code, extra) VALUES (NULL, 'null-id', 33, NULL)",
            )
            .unwrap();
            expected_rows += 1;
        }
        db.commit_transaction(&mut transaction).unwrap();
        drop(transaction);
        let after = db.bindings.resolve_single(table).unwrap();
        assert_ne!(before, after);
        assert_eq!(after.0, initial_storage.0 + position as u64 + 1);
        assert_eq!(
            db.table_schema_version(table),
            Some(TableSchemaVersion(position as u64 + 2))
        );
        assert_eq!(
            db.indexes(table)
                .unwrap()
                .iter()
                .map(|index| (index.id, index.name.clone(), index.column_id))
                .collect::<Vec<_>>(),
            index_identity
        );
    }
    assert!(db.execute_prepared(&stale, &[]).is_err());
    assert_eq!(
        rows(db.execute_prepared(&unrelated, &[]).unwrap()),
        vec![vec![ScalarValue::Int64(2)]]
    );
    let final_table = db.schema().table("work").unwrap();
    assert_eq!(final_table.id, table);
    assert_eq!(final_table.column_by_id(ColumnId(2)).unwrap().name, "title");
    assert!(final_table.column_by_id(ColumnId(3)).is_none());
    assert_eq!(final_table.column_by_id(ColumnId(5)).unwrap().name, "extra");
    assert!(final_table.column_by_id(ColumnId(1)).unwrap().nullable);
    assert!(!final_table.column_by_id(ColumnId(2)).unwrap().nullable);
    assert_eq!(
        final_table
            .column_by_id(ColumnId(4))
            .unwrap()
            .semantic_type(),
        SemanticType::named("AccountId", PhysicalType::Int64)
    );
    assert_eq!(db.inspect_replacement_retired_heaps().len(), 7);
    let final_storage = db.bindings.resolve_single(table).unwrap();
    let final_fingerprint = final_table.fingerprint().unwrap();
    let final_next_storage = db.next_storage_id();
    let final_next_column = db.next_column_id(table);
    let retained = db.inspect_replacement_retired_heaps()[0].clone();
    let retained_path =
        crate::schema_catalog_file::resolve(&root.join("catalog"), &retained.old_relative_locator);
    let retained_bundle = heap_bundle_bytes(&retained_path);
    let inspection = db.inspect_replacement_retired_heap_gc(&retained).unwrap();
    assert!(inspection.eligible(), "{:?}", inspection.blockers);
    let report = db.gc_replacement_retired_heap(&retained).unwrap();
    assert_eq!(report.state, RetiredHeapGcState::Deleted);
    assert!(retained_bundle.iter().any(|(_, bytes)| bytes.is_some()));
    assert!(
        heap_bundle_bytes(&retained_path)
            .iter()
            .all(|(_, bytes)| bytes.is_none())
    );
    assert_eq!(db.inspect_replacement_retired_heaps().len(), 7);
    assert_eq!(
        db.query("SELECT id FROM work WHERE id = 2").unwrap().rows,
        vec![vec![ScalarValue::Int64(2)]]
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.schema().table("work").unwrap().id, table);
        assert_eq!(
            reopened.table_schema_version(table),
            Some(TableSchemaVersion(8))
        );
        assert_eq!(
            reopened.bindings.resolve_single(table).unwrap(),
            final_storage
        );
        assert_eq!(
            reopened
                .schema()
                .table("work")
                .unwrap()
                .fingerprint()
                .unwrap(),
            final_fingerprint
        );
        assert_eq!(reopened.next_storage_id(), final_next_storage);
        assert_eq!(reopened.next_column_id(table), final_next_column);
        assert_eq!(
            reopened
                .indexes(table)
                .unwrap()
                .iter()
                .map(|index| (index.id, index.name.clone(), index.column_id))
                .collect::<Vec<_>>(),
            index_identity
        );
        assert_eq!(reopened.inspect_replacement_retired_heaps().len(), 7);
        assert_eq!(reopened.query("SELECT id FROM work").unwrap().rows.len(), 3);
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_rejects_prior_access_static_dependencies_and_failed_not_null() {
    let root = root("rewrite-rejections");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects VALUES (1, NULL)").unwrap();
    db.create_index(table, ColumnId(1)).unwrap();

    let before_storage = db.next_storage_id();
    let before_column = db.next_column_id(table);
    let target = db.resolve_alter_table("projects").unwrap();
    let mut stale = target.clone();
    stale.table_version = TableSchemaVersion(stale.table_version.0 + 1);
    let mut stale_txn = db.begin_transaction().unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut stale_txn,
            AlterTableSpec::new(
                stale,
                AlterTableOperation::RenameTable {
                    new_name: "work".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::StaleSchemaDependency
        ))
    ));
    stale_txn.rollback().unwrap();
    drop(stale_txn);

    let mut no_op = db.begin_transaction().unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut no_op,
            AlterTableSpec::new(
                target.clone(),
                AlterTableOperation::RenameTable {
                    new_name: "projects".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::InvalidSchemaEvolution(_)
        ))
    ));
    no_op.rollback().unwrap();
    drop(no_op);

    for operation in [
        AlterTableOperation::SetNotNull {
            column_id: ColumnId(1),
        },
        AlterTableOperation::DropNotNull {
            column_id: ColumnId(2),
        },
        AlterTableOperation::ChangeNominalType {
            column_id: ColumnId(1),
            target_type: SemanticType::physical(PhysicalType::Int64),
        },
    ] {
        let mut no_op = db.begin_transaction().unwrap();
        assert!(matches!(
            db.rewrite_heap_table_schema_in(
                &mut no_op,
                AlterTableSpec::new(target.clone(), operation)
            ),
            Err(crate::DatabaseError::SchemaMutation(
                SchemaMutationError::InvalidSchemaEvolution(_)
            ))
        ));
        no_op.rollback().unwrap();
        drop(no_op);
    }

    let mut conversion = db.begin_transaction().unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut conversion,
            AlterTableSpec::new(
                target.clone(),
                AlterTableOperation::ChangeNominalType {
                    column_id: ColumnId(1),
                    target_type: SemanticType::physical(PhysicalType::Text),
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedSchemaEvolution
        ))
    ));
    conversion.rollback().unwrap();
    drop(conversion);
    assert_eq!(db.next_storage_id(), before_storage);
    assert_eq!(db.next_column_id(table), before_column);

    let mut read_first = db.begin_transaction().unwrap();
    db.execute_in(&mut read_first, "SELECT id FROM projects")
        .unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut read_first,
            AlterTableSpec::new(
                target.clone(),
                AlterTableOperation::RenameTable {
                    new_name: "work".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));
    read_first.rollback().unwrap();
    drop(read_first);
    assert_eq!(db.next_storage_id(), before_storage);
    assert_eq!(db.next_column_id(table), before_column);

    let mut write_first = db.begin_transaction().unwrap();
    db.execute_in(
        &mut write_first,
        "UPDATE projects SET name = 'private' WHERE id = 1",
    )
    .unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut write_first,
            AlterTableSpec::new(
                target.clone(),
                AlterTableOperation::RenameTable {
                    new_name: "work".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));
    write_first.rollback().unwrap();
    drop(write_first);
    assert_eq!(db.next_storage_id(), before_storage);

    let executed = db
        .prepare_statement("SELECT id FROM projects", &[])
        .unwrap();
    let mut prepared_first = db.begin_transaction().unwrap();
    db.execute_prepared_in(&mut prepared_first, &executed, &[])
        .unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut prepared_first,
            AlterTableSpec::new(
                target.clone(),
                AlterTableOperation::RenameTable {
                    new_name: "work".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));
    prepared_first.rollback().unwrap();
    drop(prepared_first);
    assert_eq!(db.next_storage_id(), before_storage);

    let mut indexed_drop = db.begin_transaction().unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut indexed_drop,
            AlterTableSpec::new(
                target.clone(),
                AlterTableOperation::DropColumn {
                    column_id: ColumnId(1)
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::IndexedColumn(ColumnId(1))
        ))
    ));
    indexed_drop.rollback().unwrap();
    drop(indexed_drop);
    assert_eq!(db.next_storage_id(), before_storage);

    let failed_storage = db.next_storage_id().unwrap();
    let mut not_null = db.begin_transaction().unwrap();
    assert!(matches!(
        db.rewrite_heap_table_schema_in(
            &mut not_null,
            AlterTableSpec::new(
                target,
                AlterTableOperation::SetNotNull {
                    column_id: ColumnId(2)
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(ColumnId(2))
        ))
    ));
    not_null.rollback().unwrap();
    assert_eq!(db.next_storage_id(), Some(StorageId(failed_storage.0 + 1)));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column_by_id(ColumnId(2))
            .unwrap()
            .nullable
    );
    assert_eq!(
        db.query("SELECT id, name FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(1), ScalarValue::Null]]
    );
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        reopened.next_storage_id(),
        Some(StorageId(failed_storage.0 + 1))
    );
    assert_eq!(
        reopened.table_schema_version(table),
        Some(TableSchemaVersion(1))
    );
    assert!(reopened.inspect_replacement_retired_heaps().is_empty());
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn heap_schema_rewrite_rejects_bootstrap_lsm_and_range_placements_before_reservation() {
    let bootstrap_root = root("rewrite-bootstrap-rejection");
    let mut bootstrap = seed(&bootstrap_root, true);
    let target = bootstrap.resolve_alter_table("users").unwrap();
    let before_storage = bootstrap.next_storage_id();
    let mut transaction = bootstrap.begin_transaction().unwrap();
    assert!(matches!(
        bootstrap.rewrite_heap_table_schema_in(
            &mut transaction,
            AlterTableSpec::new(
                target,
                AlterTableOperation::RenameTable {
                    new_name: "people".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedPlacement
        ))
    ));
    assert_eq!(bootstrap.next_storage_id(), before_storage);
    assert!(!bootstrap_root.join("catalog.mutations").exists());
    transaction.rollback().unwrap();
    drop(transaction);
    bootstrap.close().unwrap();

    let lsm_root = root("rewrite-lsm-rejection");
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
    let target = lsm.resolve_alter_table("rows").unwrap();
    let before_storage = lsm.next_storage_id();
    let mut transaction = lsm.begin_transaction().unwrap();
    assert!(matches!(
        lsm.rewrite_heap_table_schema_in(
            &mut transaction,
            AlterTableSpec::new(
                target,
                AlterTableOperation::RenameTable {
                    new_name: "records".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedPlacement
        ))
    ));
    assert_eq!(lsm.next_storage_id(), before_storage);
    assert!(!lsm_root.join("catalog.mutations").exists());
    transaction.rollback().unwrap();
    drop(transaction);
    lsm.close().unwrap();

    let range_root = root("rewrite-range-rejection");
    let mut range = Database::create_catalog_with_placements(
        range_root.join("catalog"),
        vec![crate::TablePlacementSpec::range_partitioned(
            old_table(1, "events"),
            ColumnId(1),
            vec![crate::RangePartitionSpec::new(
                netbadb_types::PartitionId(1),
                range_root.join("events.heap"),
                None,
                None,
            )],
        )],
        crate::PartitionCatalogConfig::new(
            range_root.join("partitions"),
            range_root.join("coordinator"),
        ),
    )
    .unwrap();
    let target = range.resolve_alter_table("events").unwrap();
    let before_storage = range.next_storage_id();
    let mut transaction = range.begin_transaction().unwrap();
    assert!(matches!(
        range.rewrite_heap_table_schema_in(
            &mut transaction,
            AlterTableSpec::new(
                target,
                AlterTableOperation::RenameTable {
                    new_name: "history".into()
                }
            )
        ),
        Err(crate::DatabaseError::SchemaMutation(
            SchemaMutationError::UnsupportedPlacement
        ))
    ));
    assert_eq!(range.next_storage_id(), before_storage);
    assert!(!range_root.join("catalog.mutations").exists());
    transaction.rollback().unwrap();
    drop(transaction);
    range.close().unwrap();

    std::fs::remove_dir_all(bootstrap_root).unwrap();
    std::fs::remove_dir_all(lsm_root).unwrap();
    std::fs::remove_dir_all(range_root).unwrap();
}

#[test]
fn one_hundred_heap_schema_rewrites_preserve_identity_and_report_growth() {
    let root = root("rewrite-one-hundred");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![CreateColumnSpec::new(
                    "value_a",
                    SemanticType::physical(PhysicalType::Int64),
                    false,
                )],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects VALUES (7)").unwrap();
    let initial_storage = db.bindings.resolve_single(table).unwrap();
    let journal_before = std::fs::metadata(root.join("catalog.mutations"))
        .unwrap()
        .len();
    let coordinator_before = std::fs::metadata(root.join("coordinator")).unwrap().len();

    let mut current_name = "value_a";
    for iteration in 0..100 {
        let next_name = if current_name == "value_a" {
            "value_b"
        } else {
            "value_a"
        };
        let target = db.resolve_alter_table("projects").unwrap();
        let column_id = db.resolve_alter_column(&target, current_name).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        db.rewrite_heap_table_schema_in(
            &mut transaction,
            AlterTableSpec::new(
                target,
                AlterTableOperation::RenameColumn {
                    column_id,
                    new_name: next_name.into(),
                },
            ),
        )
        .unwrap();
        db.commit_transaction(&mut transaction).unwrap();
        drop(transaction);
        current_name = next_name;
        assert_eq!(
            db.table_schema_version(table),
            Some(TableSchemaVersion(iteration + 2))
        );
    }

    assert_eq!(table, TableId(3));
    assert_eq!(
        db.bindings.resolve_single(table).unwrap(),
        StorageId(initial_storage.0 + 100)
    );
    assert_eq!(db.inspect_replacement_retired_heaps().len(), 100);
    assert_eq!(
        db.query(&format!("SELECT {current_name} FROM projects"))
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Int64(7)]]
    );

    let retained_bytes = db
        .inspect_replacement_retired_heaps()
        .iter()
        .flat_map(|retired| {
            netbadb_storage::heap_resource_components(crate::schema_catalog_file::resolve(
                &root.join("catalog"),
                &retired.old_relative_locator,
            ))
        })
        .filter_map(|component| std::fs::metadata(component.path).ok())
        .map(|metadata| metadata.len())
        .sum::<u64>();
    let journal_after = std::fs::metadata(root.join("catalog.mutations"))
        .unwrap()
        .len();
    let coordinator_after = std::fs::metadata(root.join("coordinator")).unwrap().len();
    eprintln!(
        "round24 rewrite stress: cycles=100 table={} initial_storage={} final_storage={} retained_heaps=100 retained_bytes={} nbsj_before={} nbsj_after={} nbsj_growth={} cord_before={} cord_after={} cord_growth={}",
        table.0,
        initial_storage.0,
        initial_storage.0 + 100,
        retained_bytes,
        journal_before,
        journal_after,
        journal_after - journal_before,
        coordinator_before,
        coordinator_after,
        coordinator_after - coordinator_before,
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            reopened.table_schema_version(table),
            Some(TableSchemaVersion(101))
        );
        assert_eq!(reopened.inspect_replacement_retired_heaps().len(), 100);
        assert_eq!(
            reopened
                .query(&format!("SELECT {current_name} FROM projects"))
                .unwrap()
                .rows,
            vec![vec![ScalarValue::Int64(7)]]
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn replacement_gc_supports_chained_rewrites_reverse_order_and_later_alter() {
    let root = root("replacement-gc-chain");
    let mut db = seed(&root, true);
    let table = create_rewrite_gc_table(&mut db, "projects");
    db.execute("INSERT INTO projects VALUES (7)").unwrap();
    let index = db
        .create_named_index(
            IndexName::new("projects_value_idx").unwrap(),
            table,
            ColumnId(1),
        )
        .unwrap();
    let first = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
    let second = rename_rewrite_gc_column(&mut db, "projects", "value_b", "value_c");
    let third = rename_rewrite_gc_column(&mut db, "projects", "value_c", "value_d");
    let retired = vec![first, second, third];
    let active_storage = db.bindings.resolve_single(table).unwrap();
    let active = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let active_locator = &active
        .storages
        .iter()
        .find(|storage| storage.id == active_storage)
        .unwrap()
        .locator;
    let active_path = crate::schema_catalog_file::resolve(&root.join("catalog"), active_locator);
    let active_bytes = heap_bundle_bytes(&active_path);
    let invariants = (
        db.schema_generation(),
        db.catalog_generation(),
        db.next_table_id(),
        db.next_storage_id(),
        db.next_partition_id(),
        db.next_column_id(table),
    );

    for resource in retired.iter().rev() {
        let inspection = db.inspect_replacement_retired_heap_gc(resource).unwrap();
        assert!(inspection.eligible(), "{:?}", inspection.blockers);
        assert_eq!(inspection.target.storage_id(), resource.old_storage_id);
        db.gc_replacement_retired_heap(resource).unwrap();
    }
    assert_eq!(heap_bundle_bytes(&active_path), active_bytes);
    assert_eq!(
        (
            db.schema_generation(),
            db.catalog_generation(),
            db.next_table_id(),
            db.next_storage_id(),
            db.next_partition_id(),
            db.next_column_id(table),
        ),
        invariants
    );
    assert_eq!(db.indexes(table).unwrap()[0].id, index.id);
    assert_eq!(
        db.query("SELECT value_d FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(7)]]
    );
    for resource in &retired {
        assert_eq!(
            db.inspect_replacement_retired_heap_gc(resource)
                .unwrap()
                .state,
            RetiredHeapGcState::Deleted
        );
    }

    let fourth = rename_rewrite_gc_column(&mut db, "projects", "value_d", "value_e");
    assert_eq!(fourth.old_storage_id, active_storage);
    assert_eq!(db.schema().table("projects").unwrap().id, table);
    assert_eq!(
        db.query("SELECT value_e FROM projects").unwrap().rows,
        vec![vec![ScalarValue::Int64(7)]]
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.schema().table("projects").unwrap().id, table);
        assert_eq!(
            reopened.query("SELECT value_e FROM projects").unwrap().rows,
            vec![vec![ScalarValue::Int64(7)]]
        );
        for resource in &retired {
            assert_eq!(
                reopened
                    .inspect_replacement_retired_heap_gc(resource)
                    .unwrap()
                    .state,
                RetiredHeapGcState::Deleted
            );
        }
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn replacement_and_drop_retirements_gc_independently_in_both_orders() {
    for drop_first in [false, true] {
        let root = root(if drop_first {
            "replacement-gc-drop-first"
        } else {
            "replacement-gc-rewrite-first"
        });
        let mut db = seed(&root, true);
        let table = create_rewrite_gc_table(&mut db, "projects");
        db.execute("INSERT INTO projects VALUES (11)").unwrap();
        let replacement = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
        let active_storage = db.bindings.resolve_single(table).unwrap();
        let target = db.resolve_drop_table("projects").unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        db.drop_table_in(&mut transaction, target).unwrap();
        db.commit_transaction(&mut transaction).unwrap();
        drop(transaction);
        let dropped = db
            .inspect_retired_table_resources()
            .into_iter()
            .find(|resource| resource.storage_id == active_storage)
            .unwrap();
        if drop_first {
            db.gc_retired_heap(&dropped).unwrap();
            db.gc_replacement_retired_heap(&replacement).unwrap();
        } else {
            db.gc_replacement_retired_heap(&replacement).unwrap();
            db.gc_retired_heap(&dropped).unwrap();
        }
        assert_eq!(
            db.inspect_replacement_retired_heap_gc(&replacement)
                .unwrap()
                .state,
            RetiredHeapGcState::Deleted
        );
        assert_eq!(
            db.inspect_retired_heap_gc(&dropped).unwrap().state,
            RetiredHeapGcState::Deleted
        );
        assert!(db.schema().tables().iter().all(|active| active.id != table));
        assert_eq!(
            db.query("SELECT id FROM teams").unwrap().rows,
            vec![vec![ScalarValue::Int64(2)]]
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
}

#[test]
fn one_hundred_rewrite_gc_cycles_reclaim_physical_history_without_reusing_storage() {
    let root = root("replacement-gc-one-hundred");
    let mut db = seed(&root, true);
    let table = create_rewrite_gc_table(&mut db, "projects");
    db.execute("INSERT INTO projects VALUES (25)").unwrap();
    let initial_storage = db.bindings.resolve_single(table).unwrap();
    let journal_before = std::fs::metadata(root.join("catalog.mutations"))
        .unwrap()
        .len();
    let coordinator_before = std::fs::metadata(root.join("coordinator")).unwrap().len();
    let mut current_name = "value_a";
    let mut deleted_bytes = 0_u64;
    let mut first_before = 0_u64;
    for iteration in 0..100 {
        let next_name = if current_name == "value_a" {
            "value_b"
        } else {
            "value_a"
        };
        let retired = rename_rewrite_gc_column(&mut db, "projects", current_name, next_name);
        let before = db.inspect_replacement_retired_heap_gc(&retired).unwrap();
        assert!(before.eligible(), "{:?}", before.blockers);
        if iteration == 0 {
            first_before = before.total_present_bytes;
        }
        let invariants = (
            db.schema_generation(),
            db.catalog_generation(),
            db.next_storage_id(),
            db.next_column_id(table),
        );
        let report = db.gc_replacement_retired_heap(&retired).unwrap();
        deleted_bytes = deleted_bytes.checked_add(report.bytes_deleted).unwrap();
        assert_eq!(
            (
                db.schema_generation(),
                db.catalog_generation(),
                db.next_storage_id(),
                db.next_column_id(table),
            ),
            invariants
        );
        current_name = next_name;
    }
    let retained_bytes = db
        .inspect_replacement_retired_heaps()
        .iter()
        .map(|retired| {
            db.inspect_replacement_retired_heap_gc(retired)
                .unwrap()
                .total_present_bytes
        })
        .sum::<u64>();
    let active_storage = db.bindings.resolve_single(table).unwrap();
    let active = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let active_locator = &active
        .storages
        .iter()
        .find(|storage| storage.id == active_storage)
        .unwrap()
        .locator;
    let active_bytes = heap_bundle_size(&heap_bundle_bytes(&crate::schema_catalog_file::resolve(
        &root.join("catalog"),
        active_locator,
    )));
    let journal_after = std::fs::metadata(root.join("catalog.mutations"))
        .unwrap()
        .len();
    let coordinator_after = std::fs::metadata(root.join("coordinator")).unwrap().len();
    eprintln!(
        "round25 rewrite+GC stress: cycles=100 table={} initial_storage={} active_storage={} first_retired_before={} retained_replacement_bytes={} deleted_replacement_bytes={} active_heap_bytes={} nbsj_before={} nbsj_after={} cord_before={} cord_after={}",
        table.0,
        initial_storage.0,
        active_storage.0,
        first_before,
        retained_bytes,
        deleted_bytes,
        active_bytes,
        journal_before,
        journal_after,
        coordinator_before,
        coordinator_after,
    );
    assert_eq!(db.schema().table("projects").unwrap().id, table);
    assert_eq!(
        db.table_schema_version(table),
        Some(TableSchemaVersion(101))
    );
    assert_eq!(active_storage, StorageId(initial_storage.0 + 100));
    assert_eq!(retained_bytes, 0);
    assert_eq!(db.inspect_replacement_retired_heaps().len(), 100);
    assert_eq!(
        db.query(&format!("SELECT {current_name} FROM projects"))
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Int64(25)]]
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            reopened.bindings.resolve_single(table).unwrap(),
            active_storage
        );
        assert_eq!(
            reopened
                .inspect_replacement_retired_heaps()
                .iter()
                .map(|retired| {
                    reopened
                        .inspect_replacement_retired_heap_gc(retired)
                        .unwrap()
                        .total_present_bytes
                })
                .sum::<u64>(),
            0
        );
        assert_eq!(
            reopened
                .query(&format!("SELECT {current_name} FROM projects"))
                .unwrap()
                .rows,
            vec![vec![ScalarValue::Int64(25)]]
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn rewrite_journal_rejects_invalid_identity_version_fingerprint_and_ordering() {
    let root = root("rewrite-journal-invalid");
    let mut db = seed(&root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    let mut rewrite = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut rewrite,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::AddNullableColumn {
                name: "active".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut rewrite).unwrap();
    drop(rewrite);

    let mut valid = db.mutation_journal.as_ref().unwrap().borrow().clone();
    let transaction = *valid.rewrites.keys().next_back().unwrap();
    valid.reservations.clear();
    valid.drops.clear();
    valid
        .rewrite_reservations
        .retain(|candidate, _| *candidate == transaction);
    valid
        .rewrites
        .retain(|candidate, _| *candidate == transaction);
    valid.rewrite_losers.clear();
    assert!(valid.encode().is_ok());

    let mut same_storage = valid.clone();
    let old_storage = same_storage.rewrites[&transaction].old_storage();
    same_storage
        .rewrite_reservations
        .get_mut(&transaction)
        .unwrap()
        .storage = old_storage;
    let intent = same_storage.rewrites.get_mut(&transaction).unwrap();
    intent.reservation.storage = old_storage;
    intent.target.storages[0].id = old_storage;
    intent.target.placements.tables[0].placement = crate::TablePlacement::Single {
        table_id: table,
        storage_id: old_storage,
    };
    intent.target.committed.next_storage_id = Some(StorageId(old_storage.0 + 1));
    assert!(same_storage.encode().is_err());

    let mut base_version = valid.clone();
    base_version
        .rewrites
        .get_mut(&transaction)
        .unwrap()
        .base
        .committed
        .tables[0]
        .version = TableSchemaVersion(2);
    assert!(base_version.encode().is_err());

    let mut target_version = valid.clone();
    target_version
        .rewrites
        .get_mut(&transaction)
        .unwrap()
        .target
        .committed
        .tables[0]
        .version = TableSchemaVersion(1);
    assert!(target_version.encode().is_err());

    let mut target_fingerprint = valid.clone();
    let base_fingerprint = target_fingerprint.rewrites[&transaction]
        .base
        .placements
        .tables[0]
        .schema_fingerprint;
    target_fingerprint
        .rewrites
        .get_mut(&transaction)
        .unwrap()
        .target
        .placements
        .tables[0]
        .schema_fingerprint = base_fingerprint;
    assert!(target_fingerprint.encode().is_err());

    let mut reserved_column = valid.clone();
    reserved_column
        .rewrite_reservations
        .get_mut(&transaction)
        .unwrap()
        .column = Some(ColumnId(4));
    reserved_column
        .rewrites
        .get_mut(&transaction)
        .unwrap()
        .reservation
        .column = Some(ColumnId(4));
    assert!(reserved_column.encode().is_err());

    let mut gc_intent = valid.clone();
    gc_intent.rewrites.get_mut(&transaction).unwrap().gc =
        Some(crate::schema_mutation_journal::RetiredHeapGcRecord {
            coordinator_horizon: transaction,
            manifest_digest: [25; 32],
            complete: false,
        });
    assert!(gc_intent.encode().is_ok());
    gc_intent
        .rewrites
        .get_mut(&transaction)
        .unwrap()
        .gc
        .as_mut()
        .unwrap()
        .complete = true;
    assert!(gc_intent.encode().is_ok());

    let mut gc_before_winner = valid.clone();
    let rewrite = gc_before_winner.rewrites.get_mut(&transaction).unwrap();
    rewrite.resolved = None;
    rewrite.gc = Some(crate::schema_mutation_journal::RetiredHeapGcRecord {
        coordinator_horizon: transaction,
        manifest_digest: [25; 32],
        complete: false,
    });
    assert!(gc_before_winner.encode().is_err());

    let mut stale_horizon = valid.clone();
    stale_horizon.rewrites.get_mut(&transaction).unwrap().gc =
        Some(crate::schema_mutation_journal::RetiredHeapGcRecord {
            coordinator_horizon: netbadb_types::DatabaseTxnId(transaction.0 - 1),
            manifest_digest: [25; 32],
            complete: false,
        });
    assert!(stale_horizon.encode().is_err());

    let mut winner_without_retirement = valid;
    winner_without_retirement
        .rewrites
        .get_mut(&transaction)
        .unwrap()
        .retired = false;
    assert!(winner_without_retirement.encode().is_err());

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round18-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).expect("fresh isolated test directory");
    path
}

fn create_rewrite_gc_table(db: &mut Database, name: &str) -> TableId {
    let mut transaction = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut transaction,
            CreateTableSpec::new(
                name,
                vec![CreateColumnSpec::new(
                    "value_a",
                    SemanticType::physical(PhysicalType::Int64),
                    false,
                )],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    table
}

fn rename_rewrite_gc_column(
    db: &mut Database,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> crate::ReplacementRetiredHeap {
    let target = db.resolve_alter_table(table_name).unwrap();
    let column_id = db.resolve_alter_column(&target, old_name).unwrap();
    let old_storage = db.bindings.resolve_single(target.table_id).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut transaction,
        AlterTableSpec::new(
            target,
            AlterTableOperation::RenameColumn {
                column_id,
                new_name: new_name.into(),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    drop(transaction);
    db.inspect_replacement_retired_heaps()
        .into_iter()
        .find(|retired| retired.old_storage_id == old_storage)
        .unwrap()
}

fn heap_bundle_bytes(path: &Path) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    heap_resource_components(path)
        .into_iter()
        .map(|component| {
            let bytes = match std::fs::read(&component.path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("read Heap component {}: {error}", component.path.display()),
            };
            (component.path, bytes)
        })
        .collect()
}

fn heap_bundle_size(bundle: &[(PathBuf, Option<Vec<u8>>)]) -> usize {
    bundle
        .iter()
        .map(|(_, bytes)| bytes.as_ref().map_or(0, Vec::len))
        .sum()
}

#[derive(Clone, Copy)]
enum PlanKind {
    IndexScan,
    RangeIndexScan,
    IndexNestedLoopJoin,
    HashJoin,
}

fn query_plan(plan: &StatementPlanInspection) -> &PlanNodeInspection {
    match plan {
        StatementPlanInspection::Query { root } => root,
        _ => panic!("expected query plan"),
    }
}

fn plan_contains(plan: &PlanNodeInspection, expected: PlanKind) -> bool {
    if matches!(
        (plan, expected),
        (PlanNodeInspection::IndexScan { .. }, PlanKind::IndexScan)
            | (
                PlanNodeInspection::RangeIndexScan { .. },
                PlanKind::RangeIndexScan
            )
            | (
                PlanNodeInspection::IndexNestedLoopJoin { .. },
                PlanKind::IndexNestedLoopJoin
            )
            | (PlanNodeInspection::HashJoin { .. }, PlanKind::HashJoin)
    ) {
        return true;
    }
    match plan {
        PlanNodeInspection::NestedLoopJoin { left, right, .. }
        | PlanNodeInspection::HashJoin { left, right, .. } => {
            plan_contains(left, expected) || plan_contains(right, expected)
        }
        PlanNodeInspection::IndexNestedLoopJoin { left, .. }
        | PlanNodeInspection::Filter { input: left, .. }
        | PlanNodeInspection::Sort { input: left, .. }
        | PlanNodeInspection::Project { input: left, .. }
        | PlanNodeInspection::ScalarProject { input: left, .. }
        | PlanNodeInspection::Aggregate { input: left, .. }
        | PlanNodeInspection::Limit { input: left, .. } => plan_contains(left, expected),
        PlanNodeInspection::OneRow
        | PlanNodeInspection::SeqScan { .. }
        | PlanNodeInspection::IndexScan { .. }
        | PlanNodeInspection::RangeIndexScan { .. }
        | PlanNodeInspection::PartitionedScan { .. } => false,
    }
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
        "gc-before-api-return",
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
fn replacement_retired_heap_gc_waits_for_complete_coordinator_horizon() {
    let root = root("replacement-gc-horizon");
    let mut db = seed(&root, true);
    create_rewrite_gc_table(&mut db, "projects");
    let retired = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
    let horizon = db.next_transaction_id;
    db.coordinator
        .as_ref()
        .unwrap()
        .borrow_mut()
        .commit_decision(
            horizon,
            &[crate::coordinator_log::CoordinatorParticipant {
                storage_id: retired.old_storage_id,
                physical_txn_id: TxnId(991),
            }],
        )
        .unwrap();
    let blocked = db.inspect_replacement_retired_heap_gc(&retired).unwrap();
    assert_eq!(blocked.coordinator_horizon, Some(horizon));
    assert!(blocked.blockers.iter().any(|blocker| matches!(
        blocker,
        crate::RetiredHeapGcBlocker::CoordinatorDecisionIncomplete { transaction }
            if *transaction == horizon
    )));
    assert!(db.gc_replacement_retired_heap(&retired).is_err());
    db.coordinator
        .as_ref()
        .unwrap()
        .borrow_mut()
        .complete(horizon)
        .unwrap();
    assert!(
        db.inspect_replacement_retired_heap_gc(&retired)
            .unwrap()
            .eligible()
    );
    db.gc_replacement_retired_heap(&retired).unwrap();
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn replacement_retired_heap_missing_without_gc_intent_is_hard_corruption() {
    let root = root("replacement-gc-missing-retained");
    let mut db = seed(&root, true);
    create_rewrite_gc_table(&mut db, "projects");
    let retired = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
    let main = db
        .inspect_replacement_retired_heap_gc(&retired)
        .unwrap()
        .components
        .into_iter()
        .find(|component| component.kind == crate::RetiredHeapGcComponentKind::Main)
        .unwrap()
        .path;
    db.close().unwrap();
    std::fs::remove_file(main).unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn replacement_gc_refuses_symlinks_and_completed_component_reappearance() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let root = root("replacement-gc-symlink");
        let mut db = seed(&root, true);
        create_rewrite_gc_table(&mut db, "projects");
        let retired = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
        let owner = db
            .inspect_replacement_retired_heap_gc(&retired)
            .unwrap()
            .components
            .into_iter()
            .find(|component| component.kind == crate::RetiredHeapGcComponentKind::Owner)
            .unwrap()
            .path;
        let outside = root.join("outside");
        std::fs::write(&outside, b"outside").unwrap();
        std::fs::remove_file(&owner).unwrap();
        symlink(&outside, &owner).unwrap();
        assert!(db.inspect_replacement_retired_heap_gc(&retired).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    let root = root("replacement-gc-reappeared");
    let mut db = seed(&root, true);
    create_rewrite_gc_table(&mut db, "projects");
    let retired = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
    let main = db
        .inspect_replacement_retired_heap_gc(&retired)
        .unwrap()
        .components
        .into_iter()
        .find(|component| component.kind == crate::RetiredHeapGcComponentKind::Main)
        .unwrap()
        .path;
    db.gc_replacement_retired_heap(&retired).unwrap();
    db.close().unwrap();
    std::fs::write(main, b"reappeared").unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn replacement_retired_heap_gc_crash_child() {
    let Ok(root) = std::env::var("NETBADB_REPLACEMENT_GC_CHILD_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let retired = db
        .inspect_replacement_retired_heaps()
        .into_iter()
        .max_by_key(|resource| resource.replacement_transaction)
        .unwrap();
    db.gc_replacement_retired_heap(&retired).unwrap();
    panic!("configured replacement-retired Heap GC crash hook was not reached");
}

fn spawn_replacement_retired_heap_gc(root: &Path, point: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "schema_mutation_tests::replacement_retired_heap_gc_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_REPLACEMENT_GC_CHILD_ROOT", root)
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
fn subprocess_replacement_retired_heap_gc_crash_matrix_converges_on_three_reopens() {
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
        "gc-before-api-return",
    ] {
        let root = root(&format!("replacement-{point}"));
        let mut db = seed(&root, true);
        create_rewrite_gc_table(&mut db, "projects");
        let retired = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
        db.close().unwrap();
        spawn_replacement_retired_heap_gc(&root, point);
        for _ in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            let state = reopened
                .inspect_replacement_retired_heap_gc(&retired)
                .unwrap()
                .state;
            if state == RetiredHeapGcState::Retained {
                reopened.gc_replacement_retired_heap(&retired).unwrap();
            }
            assert_eq!(
                reopened
                    .inspect_replacement_retired_heap_gc(&retired)
                    .unwrap()
                    .state,
                RetiredHeapGcState::Deleted
            );
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn replacement_gc_intent_refuses_a_different_heap_at_the_old_path() {
    let root = root("replacement-gc-wrong-heap");
    let mut db = seed(&root, true);
    create_rewrite_gc_table(&mut db, "projects");
    let retired = rename_rewrite_gc_column(&mut db, "projects", "value_a", "value_b");
    let active_storage = db.bindings.resolve_single(retired.table_id).unwrap();
    let active = crate::schema_catalog_file::load(&root.join("catalog")).unwrap();
    let active_locator = &active
        .storages
        .iter()
        .find(|storage| storage.id == active_storage)
        .unwrap()
        .locator;
    let active_path = crate::schema_catalog_file::resolve(&root.join("catalog"), active_locator);
    let old_path =
        crate::schema_catalog_file::resolve(&root.join("catalog"), &retired.old_relative_locator);
    let active_before = heap_bundle_bytes(&active_path);
    db.close().unwrap();
    spawn_replacement_retired_heap_gc(&root, "gc-intent-durable");
    std::fs::copy(&active_path, &old_path).unwrap();
    for _ in 0..3 {
        assert!(Database::open_catalog(root.join("catalog")).is_err());
    }
    assert_eq!(heap_bundle_bytes(&active_path), active_before);
    std::fs::remove_dir_all(root).unwrap();
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

fn seed_rewrite_crash(root: &Path) {
    let mut db = seed(root, true);
    let mut create = db.begin_transaction().unwrap();
    let table = db
        .create_heap_table_in(
            &mut create,
            CreateTableSpec::new(
                "projects",
                vec![
                    CreateColumnSpec::new("id", SemanticType::physical(PhysicalType::Int64), false),
                    CreateColumnSpec::new("name", SemanticType::physical(PhysicalType::Text), true),
                ],
            ),
        )
        .unwrap();
    db.commit_transaction(&mut create).unwrap();
    drop(create);
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    db.create_named_index(
        IndexName::new("projects_id_idx").unwrap(),
        table,
        ColumnId(1),
    )
    .unwrap();
    db.close().unwrap();
}

#[test]
fn rewrite_crash_child() {
    let Ok(root) = std::env::var("NETBADB_REWRITE_CHILD_ROOT") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut transaction,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::AddNullableColumn {
                name: "active".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    if std::env::var("NETBADB_REWRITE_CRASH_POINT")
        .unwrap_or_default()
        .starts_with("rollback-")
    {
        transaction.rollback().unwrap();
    } else {
        db.commit_transaction(&mut transaction).unwrap();
    }
    panic!("configured rewrite crash hook was not reached");
}

fn spawn_rewrite(root: &Path, point: &str) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "schema_mutation_tests::rewrite_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_REWRITE_CHILD_ROOT", root)
        .env("NETBADB_REWRITE_CRASH_POINT", point);
    if point == "during-decision-append" || point == "after-decision-append" {
        crate::coordinator_crash::configure_child(&mut command, "schema-rewrite", root, point);
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

fn rewrite_outcome(root: &Path, winner: bool) {
    for _ in 0..3 {
        let mut db = Database::open_catalog(root.join("catalog")).unwrap();
        let table = db.schema().table("projects").unwrap();
        assert_eq!(table.id, TableId(3));
        assert_eq!(
            db.schema_generation(),
            SchemaGeneration(if winner { 3 } else { 2 })
        );
        assert_eq!(
            db.table_schema_version(table.id),
            Some(TableSchemaVersion(if winner { 2 } else { 1 }))
        );
        assert_eq!(
            db.bindings.resolve_single(table.id).unwrap(),
            StorageId(if winner { 4 } else { 3 })
        );
        assert_eq!(db.next_storage_id(), Some(StorageId(5)));
        assert_eq!(db.next_column_id(table.id), Some(ColumnId(4)));
        assert_eq!(db.indexes(table.id).unwrap()[0].id.0, 1);
        if winner {
            assert_eq!(table.columns[2].id, ColumnId(3));
            assert_eq!(db.inspect_replacement_retired_heaps().len(), 1);
            assert_eq!(
                db.query("SELECT id, active FROM projects ORDER BY id")
                    .unwrap()
                    .rows,
                vec![
                    vec![ScalarValue::Int64(1), ScalarValue::Null],
                    vec![ScalarValue::Int64(2), ScalarValue::Null],
                ]
            );
        } else {
            assert_eq!(table.columns.len(), 2);
            assert!(db.inspect_replacement_retired_heaps().is_empty());
            assert_eq!(
                db.query("SELECT id FROM projects ORDER BY id")
                    .unwrap()
                    .rows
                    .len(),
                2
            );
        }
        db.close().unwrap();
    }
}

fn assert_rewrite_target_requires_prepared_recovery(root: &Path) {
    let catalog = root.join("catalog");
    let snapshot = crate::schema_catalog_file::load(&catalog).unwrap();
    let journal =
        crate::schema_mutation_journal::SchemaMutationJournal::open(&catalog, snapshot.incarnation)
            .unwrap()
            .unwrap();
    let rewrite = journal.rewrites.values().next_back().unwrap();
    let stage = crate::schema_catalog_file::resolve(
        &catalog,
        &crate::schema_mutation_journal::stage_locator(
            &catalog,
            snapshot.incarnation,
            rewrite.reservation.transaction,
            rewrite.new_storage(),
        )
        .unwrap(),
    );
    let recovery =
        TableStorage::inspect_heap_recovery(stage, &rewrite.target.committed.schema.tables()[0])
            .unwrap();
    assert!(recovery.prepared_transactions.iter().any(|prepared| {
        prepared.database_txn_id == rewrite.reservation.transaction
            && prepared.state == netbadb_storage::PreparedTransactionState::Prepared
    }));
}

#[test]
fn subprocess_rewrite_crash_matrix_reopens_three_times_without_recopy() {
    for (point, winner) in [
        ("rewrite-reservation-durable", false),
        ("rewrite-intent-durable", false),
        ("rewrite-stage-first-file", false),
        ("rewrite-before-index-install", false),
        ("rewrite-stage-synced", false),
        ("rewrite-first-row", false),
        ("rewrite-mid-copy", false),
        ("rewrite-copy-complete", false),
        ("rewrite-indexes-complete", false),
        ("participants-prepared", false),
        ("prepared-catalog-written", false),
        ("prepared-catalog-durable", false),
        ("before-coordinator-decision", false),
        ("during-decision-append", false),
        ("after-decision-append", true),
        ("coordinator-durable", true),
        ("staged-heap-committed", true),
        ("promotion-partial", true),
        ("rewrite-promotion-complete", true),
        ("rewrite-final-heap-synced", true),
        ("rewrite-retirement-durable", true),
        ("before-nbsc-publication", true),
        ("during-nbsc-publication", true),
        ("nbsc-state-durable", true),
        ("rewrite-nbsc-durable", true),
        ("rewrite-before-coordinator-complete", true),
        ("rewrite-coordinator-complete", true),
        ("rewrite-before-winner-resolution", true),
        ("rewrite-winner-resolved", true),
        ("before-memory-publish", true),
        ("after-memory-publish", true),
        ("before-api-return", true),
        ("rollback-participants-durable", false),
        ("rollback-cleanup", false),
    ] {
        eprintln!("rewrite crash point: {point}");
        let root = root(&format!("rewrite-{point}"));
        seed_rewrite_crash(&root);
        spawn_rewrite(&root, point);
        if point == "after-decision-append" {
            assert_rewrite_target_requires_prepared_recovery(&root);
        }
        rewrite_outcome(&root, winner);
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
    let schema_drop_zero = std::fs::read(root.join("coordinator")).unwrap();

    let mut rewrite_create = db.begin_transaction().unwrap();
    db.create_heap_table_in(
        &mut rewrite_create,
        CreateTableSpec::new(
            "rewrite_rows",
            vec![CreateColumnSpec::new(
                "id",
                SemanticType::physical(PhysicalType::Int64),
                false,
            )],
        ),
    )
    .unwrap();
    db.commit_transaction(&mut rewrite_create).unwrap();
    drop(rewrite_create);
    let rewrite_target = db.resolve_alter_table("rewrite_rows").unwrap();
    let mut rewrite_loser = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut rewrite_loser,
        AlterTableSpec::new(
            rewrite_target.clone(),
            AlterTableOperation::AddNullableColumn {
                name: "note".into(),
                data_type: SemanticType::physical(PhysicalType::Text),
            },
        ),
    )
    .unwrap();
    let mut rewrite_intent = db.mutation_journal.as_ref().unwrap().borrow().clone();
    rewrite_intent.reservations.clear();
    rewrite_intent.drops.clear();
    normalize_rewrite_fuzz_journal(&mut rewrite_intent);
    let rewrite_transaction = *rewrite_intent.rewrite_reservations.keys().next().unwrap();
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-intent-v1"),
        rewrite_intent.encode().unwrap(),
    )
    .unwrap();
    let mut rewrite_reservation = rewrite_intent.clone();
    rewrite_reservation.rewrites.clear();
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-reservation-v1"),
        rewrite_reservation.encode().unwrap(),
    )
    .unwrap();
    let rewrite_bytes = rewrite_intent.encode().unwrap();
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-truncated-v1"),
        &rewrite_bytes[..rewrite_bytes.len() - 11],
    )
    .unwrap();
    rewrite_loser.rollback().unwrap();
    drop(rewrite_loser);
    let mut rewrite_loser_history = rewrite_reservation.clone();
    rewrite_loser_history
        .rewrite_losers
        .insert(rewrite_transaction);
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-loser-v1"),
        rewrite_loser_history.encode().unwrap(),
    )
    .unwrap();

    let mut rewrite_winner = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_in(
        &mut rewrite_winner,
        AlterTableSpec::new(
            rewrite_target,
            AlterTableOperation::RenameColumn {
                column_id: ColumnId(1),
                new_name: "row_id".into(),
            },
        ),
    )
    .unwrap();
    db.commit_transaction(&mut rewrite_winner).unwrap();
    drop(rewrite_winner);
    let mut rewrite_winner_history = db.mutation_journal.as_ref().unwrap().borrow().clone();
    rewrite_winner_history.reservations.clear();
    rewrite_winner_history.drops.clear();
    let winner_transaction = *rewrite_winner_history.rewrites.keys().next_back().unwrap();
    rewrite_winner_history
        .rewrite_reservations
        .retain(|transaction, _| *transaction == winner_transaction);
    rewrite_winner_history
        .rewrites
        .retain(|transaction, _| *transaction == winner_transaction);
    rewrite_winner_history.rewrite_losers.clear();
    normalize_rewrite_fuzz_journal(&mut rewrite_winner_history);
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-winner-v1"),
        rewrite_winner_history.encode().unwrap(),
    )
    .unwrap();
    let mut rewrite_gc_intent = rewrite_winner_history.clone();
    rewrite_gc_intent
        .rewrites
        .get_mut(&winner_transaction)
        .unwrap()
        .gc = Some(crate::schema_mutation_journal::RetiredHeapGcRecord {
        coordinator_horizon: winner_transaction,
        manifest_digest: [25; 32],
        complete: false,
    });
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-gc-intent-v1"),
        rewrite_gc_intent.encode().unwrap(),
    )
    .unwrap();
    rewrite_gc_intent
        .rewrites
        .get_mut(&winner_transaction)
        .unwrap()
        .gc
        .as_mut()
        .unwrap()
        .complete = true;
    std::fs::write(
        output.join("schema_mutation_decode/rewrite-gc-complete-v1"),
        rewrite_gc_intent.encode().unwrap(),
    )
    .unwrap();
    std::fs::write(
        output.join("coordinator_log_decode/schema-drop-zero-v2"),
        schema_drop_zero,
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

fn normalize_rewrite_fuzz_journal(
    journal: &mut crate::schema_mutation_journal::SchemaMutationJournal,
) {
    journal.incarnation = [24; 16];
    journal.coordinator = "coordinator".into();
    for rewrite in journal.rewrites.values_mut() {
        rewrite.snapshot_digest = [24; 32];
        for fragment in [&mut rewrite.base, &mut rewrite.target] {
            fragment.incarnation = [24; 16];
            fragment.coordinator = Some("coordinator".into());
            for storage in &mut fragment.storages {
                storage.locator = format!("resources/storage/{}.heap", storage.id.0);
            }
        }
    }
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
