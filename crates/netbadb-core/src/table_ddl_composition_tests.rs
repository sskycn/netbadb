use super::*;
use netbadb_types::{IndexId, TableId, TableSchemaVersion};
use std::path::{Path, PathBuf};
use std::process::Command;

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round30-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db
}

#[test]
fn create_alter_index_materializes_one_final_heap_and_reopens() {
    let root = root("create-alter-index");
    let mut db = seed(&root);
    let initial_storage = db.next_storage_id().unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(
        &mut txn,
        "CREATE TABLE work (id BIGINT NOT NULL, name TEXT)",
    )
    .unwrap();
    let table = txn.visible_schema(db.schema()).table("work").unwrap().id;
    assert_eq!(db.next_storage_id(), Some(initial_storage));
    assert!(txn.staged_binding(table).is_none());
    db.execute_in(&mut txn, "ALTER TABLE work ADD COLUMN active BOOLEAN")
        .unwrap();
    db.execute_in(&mut txn, "ALTER TABLE work RENAME COLUMN name TO title")
        .unwrap();
    db.execute_in(&mut txn, "CREATE INDEX work_title_idx ON work(title)")
        .unwrap();
    assert_eq!(txn.participant_count(), 0);
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    let committed = db.schema().table("work").unwrap().clone();
    assert_eq!(committed.id, table);
    assert_eq!(committed.columns.len(), 3);
    assert_eq!(
        db.committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == table)
            .unwrap()
            .version,
        TableSchemaVersion(1)
    );
    let storage = db.bindings.resolve_single(table).unwrap();
    assert_eq!(storage, initial_storage);
    let indexes = db.indexes(table).unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].id, IndexId(1));
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.schema().table("work").unwrap(), &committed);
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn create_drop_burns_only_table_identity() {
    let root = root("create-drop");
    let mut db = seed(&root);
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let storage = db.next_storage_id();
    let table = db.next_table_id().unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "CREATE TABLE scratch (id BIGINT)")
        .unwrap();
    db.execute_in(&mut txn, "DROP TABLE scratch").unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    assert!(db.schema().table("scratch").is_none());
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.catalog_generation(), revision);
    assert_eq!(db.next_storage_id(), storage);
    assert_eq!(db.next_table_id(), table.0.checked_add(1).map(TableId));
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.next_storage_id(), storage);
    assert_eq!(
        reopened.next_table_id(),
        table.0.checked_add(1).map(TableId)
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn alter_then_drop_elides_rewrite_and_same_name_recreate_keeps_identity() {
    let root = root("alter-drop-recreate");
    let mut db = seed(&root);
    let old_table = db.schema().table("projects").unwrap().id;
    let old_storage = db.bindings.resolve_single(old_table).unwrap();
    let next_storage = db.next_storage_id();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(
        &mut txn,
        "ALTER TABLE projects ADD COLUMN temporary BOOLEAN",
    )
    .unwrap();
    db.execute_in(
        &mut txn,
        "CREATE INDEX projects_temporary_idx ON projects(temporary)",
    )
    .unwrap();
    db.execute_in(&mut txn, "DROP TABLE projects").unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    assert!(db.schema().table("projects").is_none());
    assert_eq!(db.next_storage_id(), next_storage);
    let retired = db.inspect_retired_table_resources();
    let retired = retired
        .iter()
        .find(|entry| entry.table_id == old_table && entry.storage_id == old_storage)
        .unwrap()
        .clone();
    assert_eq!(
        db.gc_retired_heap(&retired).unwrap().state,
        RetiredHeapGcState::Deleted
    );
    assert_eq!(
        db.inspect_retired_heap_gc(&retired).unwrap().state,
        RetiredHeapGcState::Deleted
    );

    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "CREATE TABLE projects (id BIGINT)")
        .unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    let new_table = db.schema().table("projects").unwrap().id;
    assert_ne!(new_table, old_table);
    assert_eq!(
        db.bindings.resolve_single(new_table).unwrap(),
        next_storage.unwrap()
    );
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.schema().table("projects").unwrap().id, new_table);
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn mixed_table_objects_use_one_schema_decision_and_only_final_participants() {
    let root = root("mixed-table-objects");
    let mut db = seed(&root);
    db.execute("CREATE TABLE retired (id BIGINT)").unwrap();
    db.execute("CREATE TABLE indexed (id BIGINT)").unwrap();
    let projects = db.schema().table("projects").unwrap().id;
    let retired = db.schema().table("retired").unwrap().id;
    let indexed = db.schema().table("indexed").unwrap().id;
    let retired_storage = db.bindings.resolve_single(retired).unwrap();
    let indexed_storage = db.bindings.resolve_single(indexed).unwrap();
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "CREATE TABLE fresh (id BIGINT)")
        .unwrap();
    let fresh = txn.visible_schema(db.schema()).table("fresh").unwrap().id;
    db.execute_in(&mut txn, "ALTER TABLE fresh ADD COLUMN note TEXT")
        .unwrap();
    db.execute_in(&mut txn, "ALTER TABLE projects RENAME COLUMN name TO title")
        .unwrap();
    db.execute_in(&mut txn, "CREATE INDEX indexed_id_idx ON indexed(id)")
        .unwrap();
    db.execute_in(&mut txn, "DROP TABLE retired").unwrap();
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);

    assert_eq!(db.schema_generation().0, generation.0 + 1);
    assert_eq!(db.catalog_generation(), revision + 1);
    assert!(db.schema().table("retired").is_none());
    assert_eq!(db.schema().table("fresh").unwrap().id, fresh);
    assert_eq!(db.schema().table("fresh").unwrap().columns.len(), 2);
    assert_eq!(db.schema().table("projects").unwrap().id, projects);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("title")
            .is_some()
    );
    assert_eq!(
        db.bindings.resolve_single(indexed).unwrap(),
        indexed_storage
    );
    assert_eq!(
        db.indexes(indexed).unwrap()[0]
            .name
            .as_ref()
            .unwrap()
            .as_str(),
        "indexed_id_idx"
    );
    assert!(db.inspect_retired_table_resources().iter().any(|resource| {
        resource.table_id == retired && resource.storage_id == retired_storage
    }));
    assert!(
        db.inspect_replacement_retired_heaps()
            .iter()
            .any(|resource| {
                resource.table_id == projects && resource.old_storage_id != resource.new_storage_id
            })
    );
    let decisions = db
        .coordinator
        .as_ref()
        .unwrap()
        .borrow()
        .decisions()
        .cloned()
        .collect::<Vec<_>>();
    let decision = decisions.last().unwrap();
    assert_eq!(decision.participants.len(), 3);
    assert!(decision.schema.is_some());
    assert!(decision.complete);
    db.close().unwrap();
    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(reopened.schema().table("retired").is_none());
        assert_eq!(reopened.schema().table("fresh").unwrap().id, fresh);
        assert!(
            reopened
                .schema()
                .table("projects")
                .unwrap()
                .column("title")
                .is_some()
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn private_prepared_targets_do_not_rebind_after_drop_or_same_name_recreate() {
    let root = root("prepared-private");
    let mut db = seed(&root);
    let unrelated = db
        .prepare_statement("SELECT id FROM projects", &[])
        .unwrap();
    let mut txn = db.begin_transaction().unwrap();
    db.execute_in(&mut txn, "CREATE TABLE scratch (id BIGINT)")
        .unwrap();
    let first = txn.visible_schema(db.schema()).table("scratch").unwrap().id;
    let prepared = db
        .prepare_statement_in(&txn, "INSERT INTO scratch VALUES (1)", &[])
        .unwrap();
    db.execute_in(&mut txn, "DROP TABLE scratch").unwrap();
    assert!(db.execute_prepared_in(&mut txn, &prepared, &[]).is_err());
    db.execute_in(&mut txn, "CREATE TABLE scratch (id BIGINT)")
        .unwrap();
    let second = txn.visible_schema(db.schema()).table("scratch").unwrap().id;
    assert_ne!(first, second);
    assert!(db.execute_prepared_in(&mut txn, &prepared, &[]).is_err());
    db.commit_transaction(&mut txn).unwrap();
    drop(txn);
    assert_eq!(db.schema().table("scratch").unwrap().id, second);
    assert!(db.execute_prepared(&prepared, &[]).is_err());
    assert!(db.execute_prepared(&unrelated, &[]).is_ok());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn table_ddl_crash_child() {
    let Ok(root) = std::env::var("NETBADB_TABLE_DDL_CHILD") else {
        return;
    };
    let scenario = std::env::var("NETBADB_TABLE_DDL_SCENARIO").unwrap();
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    match scenario.as_str() {
        "create" => {
            db.execute_in(&mut txn, "CREATE TABLE created (id BIGINT)")
                .unwrap();
        }
        "create-two" => {
            db.execute_in(&mut txn, "CREATE TABLE created_a (id BIGINT)")
                .unwrap();
            db.execute_in(&mut txn, "CREATE TABLE created_b (id BIGINT)")
                .unwrap();
        }
        "drop" => {
            db.execute_in(&mut txn, "DROP TABLE projects").unwrap();
        }
        "same-name" => {
            db.execute_in(&mut txn, "DROP TABLE projects").unwrap();
            db.execute_in(&mut txn, "CREATE TABLE projects (id BIGINT)")
                .unwrap();
        }
        "alter-drop" => {
            db.execute_in(
                &mut txn,
                "ALTER TABLE projects ADD COLUMN temporary BOOLEAN",
            )
            .unwrap();
            db.execute_in(
                &mut txn,
                "CREATE INDEX temporary_idx ON projects(temporary)",
            )
            .unwrap();
            db.execute_in(&mut txn, "DROP TABLE projects").unwrap();
        }
        _ => panic!("unknown table DDL crash scenario"),
    }
    db.commit_transaction(&mut txn).unwrap();
    panic!("configured table DDL crash point was not reached");
}

fn spawn_table_ddl_crash(root: &Path, scenario: &str, point: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "table_ddl_composition_tests::table_ddl_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_TABLE_DDL_CHILD", root)
        .env("NETBADB_TABLE_DDL_SCENARIO", scenario)
        .env("NETBADB_CREATE_CRASH_POINT", point)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(90),
        "{scenario}/{point}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn reopen_table_ddl_outcome(root: &Path, verify: impl Fn(&Database)) {
    for _ in 0..3 {
        let db = Database::open_catalog(root.join("catalog"))
            .unwrap_or_else(|error| panic!("reopen {}: {error}", root.display()));
        verify(&db);
        db.close().unwrap();
    }
}

#[test]
fn table_id_and_materialization_loser_crash_matrix_burns_only_durable_floors() {
    for (scenario, point, expected_next_table, expected_next_storage) in [
        (
            "create",
            "composition-table-reservation-durable",
            TableId(3),
            StorageId(2),
        ),
        (
            "create-two",
            "composition-before-intent",
            TableId(4),
            StorageId(2),
        ),
        (
            "create",
            "composition-intent-durable",
            TableId(3),
            StorageId(3),
        ),
        (
            "create",
            "composition-stage-first-file",
            TableId(3),
            StorageId(3),
        ),
        (
            "create",
            "composition-all-targets-staged",
            TableId(3),
            StorageId(3),
        ),
        (
            "create",
            "composition-prepared-catalog-durable",
            TableId(3),
            StorageId(3),
        ),
    ] {
        let root = root(&format!("loser-{scenario}-{point}"));
        seed(&root).close().unwrap();
        spawn_table_ddl_crash(&root, scenario, point);
        reopen_table_ddl_outcome(&root, |db| {
            assert!(db.schema().table("created").is_none());
            assert!(db.schema().table("created_a").is_none());
            assert!(db.schema().table("created_b").is_none());
            assert_eq!(db.next_table_id(), Some(expected_next_table));
            assert_eq!(db.next_storage_id(), Some(expected_next_storage));
        });
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn table_object_winner_crash_matrix_converges_without_sql_replay() {
    for point in [
        "coordinator-durable",
        "composition-final-heaps-synced",
        "composition-retirements-durable",
        "composition-before-nbsc-publication",
        "composition-nbsc-durable",
        "composition-after-cord-complete",
        "composition-before-winner-resolution",
        "composition-after-winner-resolution",
        "composition-before-memory-publication",
    ] {
        let root = root(&format!("winner-{point}"));
        seed(&root).close().unwrap();
        spawn_table_ddl_crash(&root, "create", point);
        reopen_table_ddl_outcome(&root, |db| {
            let created = db.schema().table("created").unwrap();
            assert_eq!(created.id, TableId(2));
            assert_eq!(
                db.bindings.resolve_single(created.id).unwrap(),
                StorageId(2)
            );
        });
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn drop_same_name_and_alter_drop_crashes_preserve_final_identity_classification() {
    for (scenario, point, winner) in [
        ("drop", "composition-intent-durable", false),
        ("drop", "coordinator-durable", true),
        ("same-name", "composition-intent-durable", false),
        ("same-name", "coordinator-durable", true),
        ("alter-drop", "composition-intent-durable", false),
        ("alter-drop", "coordinator-durable", true),
    ] {
        let root = root(&format!("classification-{scenario}-{point}"));
        seed(&root).close().unwrap();
        spawn_table_ddl_crash(&root, scenario, point);
        reopen_table_ddl_outcome(&root, |db| match scenario {
            "drop" | "alter-drop" => {
                assert_eq!(db.schema().table("projects").is_none(), winner);
                assert_eq!(db.next_storage_id(), Some(StorageId(2)));
                assert!(db.inspect_replacement_retired_heaps().is_empty());
            }
            "same-name" if winner => {
                let table = db.schema().table("projects").unwrap();
                assert_eq!(table.id, TableId(2));
                assert_eq!(db.bindings.resolve_single(table.id).unwrap(), StorageId(2));
            }
            "same-name" => {
                let table = db.schema().table("projects").unwrap();
                assert_eq!(table.id, TableId(1));
                assert_eq!(db.bindings.resolve_single(table.id).unwrap(), StorageId(1));
            }
            _ => unreachable!(),
        });
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn composition_drop_gc_crash_matrix_converges_with_new_gc_records() {
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
        let root = root(&format!("composition-drop-gc-{point}"));
        let mut db = seed(&root);
        db.execute("DROP TABLE projects").unwrap();
        let retired = db.inspect_retired_table_resources()[0].clone();
        db.close().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "schema_mutation_tests::retired_heap_gc_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_GC_CHILD_ROOT", &root)
            .env("NETBADB_GC_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for reopen in 0..3 {
            let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
            let inspection = reopened.inspect_retired_heap_gc(&retired).unwrap();
            if point == "gc-before-intent" && reopen == 0 {
                assert_eq!(inspection.state, RetiredHeapGcState::Retained);
                reopened.gc_retired_heap(&retired).unwrap();
            } else {
                assert_eq!(inspection.state, RetiredHeapGcState::Deleted);
            }
            reopened.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
