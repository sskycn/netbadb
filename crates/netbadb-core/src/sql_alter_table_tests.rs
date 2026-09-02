use super::*;
use netbadb_schema::{ColumnDef, TypeSpec};
use netbadb_types::{IndexName, SemanticType};
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round26-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn seed(root: &Path) -> Database {
    let mut db = Database::create_catalog(
        root.join("catalog"),
        vec![TableStorageCreateSpec::heap(
            root.join("seed.heap"),
            TableDef::new(
                TableId(1),
                "seed",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "id",
                    TypeSpec::Physical(PhysicalType::Int64),
                )],
            ),
        )],
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator"))),
    )
    .unwrap();
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db
}

fn files(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(files(&path));
        } else {
            result.push((path.clone(), std::fs::read(path).unwrap()));
        }
    }
    result.sort();
    result
}

#[test]
fn composed_multi_alter_delays_and_performs_one_final_rewrite_per_table() {
    let root = root("round28-multi-alter");
    let mut db = seed(&root);
    db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    db.execute("INSERT INTO teams VALUES (1, 'red')").unwrap();
    let projects = db.schema().table("projects").unwrap().id;
    let teams = db.schema().table("teams").unwrap().id;
    let base_generation = db.schema_generation();
    let base_revision = db.catalog_generation();
    let project_version = db.table_schema_version(projects).unwrap();
    let team_version = db.table_schema_version(teams).unwrap();
    let project_storage = db.bindings.resolve_single(projects).unwrap();
    let team_storage = db.bindings.resolve_single(teams).unwrap();
    let next_storage = db.next_storage_id().unwrap();
    let next_column = db.next_column_id(projects).unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
    )
    .unwrap();
    assert_eq!(db.next_storage_id(), Some(next_storage));
    assert_eq!(transaction.participant_count(), 0);
    assert_eq!(
        db.next_column_id(projects),
        next_column.0.checked_add(1).map(ColumnId)
    );
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects RENAME COLUMN active TO enabled",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects RENAME COLUMN name TO title",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE teams ADD COLUMN archived BOOLEAN",
    )
    .unwrap();
    assert_eq!(transaction.participant_count(), 0);
    assert_eq!(db.next_storage_id(), Some(next_storage));
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(
        db.schema_generation(),
        SchemaGeneration(base_generation.0 + 1)
    );
    assert_eq!(db.catalog_generation(), base_revision + 1);
    assert_eq!(
        db.table_schema_version(projects),
        Some(TableSchemaVersion(project_version.0 + 1))
    );
    assert_eq!(
        db.table_schema_version(teams),
        Some(TableSchemaVersion(team_version.0 + 1))
    );
    assert_eq!(db.bindings.resolve_single(projects).unwrap(), next_storage);
    assert_eq!(
        db.bindings.resolve_single(teams).unwrap(),
        StorageId(next_storage.0 + 1)
    );
    assert_ne!(
        db.bindings.resolve_single(projects).unwrap(),
        project_storage
    );
    assert_ne!(db.bindings.resolve_single(teams).unwrap(), team_storage);
    assert_eq!(db.next_storage_id(), Some(StorageId(next_storage.0 + 2)));
    assert_eq!(
        db.query("SELECT id, title, enabled FROM projects")
            .unwrap()
            .rows,
        vec![vec![
            ScalarValue::Int64(1),
            ScalarValue::Text("one".into()),
            ScalarValue::Null,
        ]]
    );
    {
        let journal = db.mutation_journal.as_ref().unwrap().borrow().clone();
        let bytes = journal.encode().unwrap();
        let decoded =
            crate::schema_mutation_journal::SchemaMutationJournal::decode(&bytes).unwrap();
        assert_eq!(decoded.encode().unwrap(), bytes);
        let composition_txn = *decoded.compositions.keys().next_back().unwrap();
        let mut duplicate_storage = decoded.clone();
        let plans = &mut duplicate_storage
            .compositions
            .get_mut(&composition_txn)
            .unwrap()
            .intent
            .as_mut()
            .unwrap()
            .tables;
        let duplicate = plans[0].new_storage();
        plans[1].target.storages[0].id = duplicate;
        plans[1].target.placements.tables[0].placement = TablePlacement::Single {
            table_id: plans[1].table(),
            storage_id: duplicate,
        };
        assert!(duplicate_storage.encode().is_err());

        let mut missing_retirement = decoded;
        missing_retirement
            .compositions
            .get_mut(&composition_txn)
            .unwrap()
            .intent
            .as_mut()
            .unwrap()
            .tables[0]
            .retired = false;
        assert!(missing_retirement.encode().is_err());
    }
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert!(
        reopened
            .schema()
            .table("projects")
            .unwrap()
            .column("title")
            .is_some()
    );
    assert!(
        reopened
            .schema()
            .table("teams")
            .unwrap()
            .column("archived")
            .is_some()
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn typed_core_api_resolves_and_composes_against_its_overlay() {
    let root = root("round28-typed-api");
    let mut db = seed(&root);
    let table = db.schema().table("projects").unwrap().id;
    let next_storage = db.next_storage_id();
    let mut transaction = db.begin_transaction().unwrap();
    let first = db.resolve_alter_table_in(&transaction, "projects").unwrap();
    db.rewrite_heap_table_schema_in(
        &mut transaction,
        AlterTableSpec::new(
            first,
            AlterTableOperation::RenameColumn {
                column_id: ColumnId(2),
                new_name: "title".into(),
            },
        ),
    )
    .unwrap();
    let second = db.resolve_alter_table_in(&transaction, "projects").unwrap();
    assert_eq!(second.table_version, TableSchemaVersion(2));
    db.rewrite_heap_table_schema_in(
        &mut transaction,
        AlterTableSpec::new(
            second,
            AlterTableOperation::AddNullableColumn {
                name: "active".into(),
                data_type: SemanticType::physical(PhysicalType::Bool),
            },
        ),
    )
    .unwrap();
    assert_eq!(db.next_storage_id(), next_storage);
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.table_schema_version(table), Some(TableSchemaVersion(2)));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("title")
            .is_some()
    );
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_some()
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn composed_noop_and_rollback_burn_columns_without_storage_or_generation() {
    let root = root("round28-noop-allocation");
    let mut db = seed(&root);
    let table = db.schema().table("projects").unwrap().id;
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let version = db.table_schema_version(table);
    let storage = db.bindings.resolve_single(table).unwrap();
    let next_storage = db.next_storage_id();
    let first_column = db.next_column_id(table).unwrap();

    let mut rollback = db.begin_transaction().unwrap();
    db.execute_in(
        &mut rollback,
        "ALTER TABLE projects ADD COLUMN temporary BIGINT",
    )
    .unwrap();
    db.execute_in(&mut rollback, "ALTER TABLE projects DROP COLUMN temporary")
        .unwrap();
    rollback.rollback().unwrap();
    assert_eq!(db.next_storage_id(), next_storage);
    assert_eq!(db.next_column_id(table), Some(ColumnId(first_column.0 + 1)));
    drop(rollback);

    let mut no_change = db.begin_transaction().unwrap();
    db.execute_in(
        &mut no_change,
        "ALTER TABLE projects ADD COLUMN transient BIGINT",
    )
    .unwrap();
    db.execute_in(&mut no_change, "ALTER TABLE projects DROP COLUMN transient")
        .unwrap();
    db.commit_transaction(&mut no_change).unwrap();
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.catalog_generation(), revision);
    assert_eq!(db.table_schema_version(table), version);
    assert_eq!(db.bindings.resolve_single(table).unwrap(), storage);
    assert_eq!(db.next_storage_id(), next_storage);
    assert_eq!(db.next_column_id(table), Some(ColumnId(first_column.0 + 2)));

    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.schema_generation(), generation);
    assert_eq!(
        reopened.next_column_id(table),
        Some(ColumnId(first_column.0 + 2))
    );
    assert_eq!(reopened.next_storage_id(), next_storage);
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn composed_action_bound_is_atomic_and_pure_rename_cycle_has_no_durable_effect() {
    let root = root("round28-action-bound");
    let mut db = seed(&root);
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let storage = db.next_storage_id();
    let before = files(&root);
    let mut transaction = db.begin_transaction().unwrap();
    for action in 0..128 {
        let sql = if action % 2 == 0 {
            "ALTER TABLE projects RENAME TO work"
        } else {
            "ALTER TABLE work RENAME TO projects"
        };
        db.execute_in(&mut transaction, sql).unwrap();
    }
    let error = db
        .execute_in(&mut transaction, "ALTER TABLE projects RENAME TO overflow")
        .unwrap_err();
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::CompositionLimitExceeded(
            "schema actions"
        ))
    ));
    assert!(
        transaction
            .visible_schema(db.schema())
            .table("projects")
            .is_some()
    );
    assert!(
        transaction
            .visible_schema(db.schema())
            .table("overflow")
            .is_none()
    );
    db.commit_transaction(&mut transaction).unwrap();
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.catalog_generation(), revision);
    assert_eq!(db.next_storage_id(), storage);
    assert_eq!(files(&root), before);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn composed_one_hundred_actions_produce_one_physical_rewrite() {
    let root = root("round28-hundred-actions");
    let mut db = seed(&root);
    let table = db.schema().table("projects").unwrap().id;
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let version = db.table_schema_version(table).unwrap();
    let old_storage = db.bindings.resolve_single(table).unwrap();
    let next_storage = db.next_storage_id().unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    for action in 0..99 {
        let sql = if action % 2 == 0 {
            "ALTER TABLE projects RENAME TO work"
        } else {
            "ALTER TABLE work RENAME TO projects"
        };
        db.execute_in(&mut transaction, sql).unwrap();
    }
    db.execute_in(
        &mut transaction,
        "ALTER TABLE work RENAME TO final_projects",
    )
    .unwrap();
    assert_eq!(transaction.participant_count(), 0);
    assert_eq!(db.next_storage_id(), Some(next_storage));
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(db.schema_generation(), SchemaGeneration(generation.0 + 1));
    assert_eq!(db.catalog_generation(), revision + 1);
    assert_eq!(
        db.table_schema_version(table),
        Some(TableSchemaVersion(version.0 + 1))
    );
    assert_eq!(db.bindings.resolve_single(table), Ok(next_storage));
    assert_ne!(db.bindings.resolve_single(table), Ok(old_storage));
    assert_eq!(db.next_storage_id(), Some(StorageId(next_storage.0 + 1)));
    let composition = db
        .mutation_journal
        .as_ref()
        .unwrap()
        .borrow()
        .compositions
        .values()
        .next_back()
        .unwrap()
        .clone();
    let intent = composition.intent.unwrap();
    assert_eq!(intent.tables.len(), 1);
    assert!(intent.tables[0].retired);

    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(reopened.schema().table("final_projects").unwrap().id, table);
    assert!(reopened.schema().table("projects").is_none());
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn composed_noop_table_is_not_rewritten_beside_an_effective_table() {
    let root = root("round28-mixed-noop-effective");
    let mut db = seed(&root);
    db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    let projects = db.schema().table("projects").unwrap().id;
    let teams = db.schema().table("teams").unwrap().id;
    let generation = db.schema_generation();
    let revision = db.catalog_generation();
    let project_version = db.table_schema_version(projects).unwrap();
    let team_version = db.table_schema_version(teams).unwrap();
    let project_storage = db.bindings.resolve_single(projects).unwrap();
    let team_storage = db.bindings.resolve_single(teams).unwrap();
    let next_storage = db.next_storage_id().unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE projects RENAME TO work")
        .unwrap();
    db.execute_in(&mut transaction, "ALTER TABLE work RENAME TO projects")
        .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE teams ADD COLUMN archived BOOLEAN",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();

    assert_eq!(db.schema_generation(), SchemaGeneration(generation.0 + 1));
    assert_eq!(db.catalog_generation(), revision + 1);
    assert_eq!(db.table_schema_version(projects), Some(project_version));
    assert_eq!(
        db.table_schema_version(teams),
        Some(TableSchemaVersion(team_version.0 + 1))
    );
    assert_eq!(db.bindings.resolve_single(projects), Ok(project_storage));
    assert_eq!(db.bindings.resolve_single(teams), Ok(next_storage));
    assert_ne!(db.bindings.resolve_single(teams), Ok(team_storage));
    assert_eq!(db.next_storage_id(), Some(StorageId(next_storage.0 + 1)));

    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        reopened.bindings.resolve_single(projects),
        Ok(project_storage)
    );
    assert!(
        reopened
            .schema()
            .table("teams")
            .unwrap()
            .column("archived")
            .is_some()
    );
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn composed_dml_materializes_all_and_globally_seals_schema() {
    let root = root("round28-dml-seal");
    let mut db = seed(&root);
    db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    let projects = db.schema().table("projects").unwrap().id;
    let teams = db.schema().table("teams").unwrap().id;
    let base_storage = db.next_storage_id().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE teams ADD COLUMN archived BOOLEAN",
    )
    .unwrap();
    assert_eq!(transaction.participant_count(), 0);
    db.execute_in(
        &mut transaction,
        "INSERT INTO projects VALUES (1, 'one', true)",
    )
    .unwrap();
    assert_eq!(transaction.participant_count(), 2);
    assert_eq!(transaction.staged_binding(projects), Some(base_storage));
    assert_eq!(
        transaction.staged_binding(teams),
        Some(StorageId(base_storage.0 + 1))
    );
    let error = db
        .execute_in(
            &mut transaction,
            "ALTER TABLE projects RENAME COLUMN name TO title",
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::SchemaMutationAfterMaterialization)
    ));
    transaction.rollback().unwrap();
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );
    assert!(
        db.schema()
            .table("teams")
            .unwrap()
            .column("archived")
            .is_none()
    );
    assert_eq!(db.next_storage_id(), Some(StorageId(base_storage.0 + 2)));
    assert!(db.query("SELECT * FROM projects").unwrap().rows.is_empty());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn composed_finalize_error_preserves_retryable_materialized_state() {
    let root = root("round28-finalize-retry");
    let mut db = seed(&root);
    let table = db.schema().table("projects").unwrap().id;
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
    )
    .unwrap();
    db.ensure_schema_materialized(&mut transaction).unwrap();
    transaction.commit_with_schema_mutations().unwrap();
    assert_eq!(transaction.state(), TransactionState::FinalizePending);

    let coordinator = transaction.shared_coordinator().unwrap();
    let busy = coordinator.borrow_mut();
    let error = db.finish_composition_commit(&mut transaction).unwrap_err();
    assert!(matches!(error, DatabaseError::Transaction(_)));
    assert_eq!(transaction.state(), TransactionState::FinalizePending);
    assert!(transaction.schema_composition.materialized().is_some());
    drop(busy);

    db.finish_composition_commit(&mut transaction).unwrap();
    assert_eq!(transaction.state(), TransactionState::Committed);
    assert!(
        db.schema()
            .tables()
            .iter()
            .find(|candidate| candidate.id == table)
            .unwrap()
            .column("active")
            .is_some()
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_alter_prepare_is_pure_exact_and_schema_write_only() {
    let root = root("prepare-pure");
    let db = seed(&root);
    let expected = db.resolve_alter_table("projects").unwrap();
    let before = files(&root);
    let prepared = db
        .prepare_ddl_statement("ALTER TABLE projects ADD COLUMN active BOOL")
        .unwrap();
    assert!(prepared.is_table_alter());
    assert_eq!(prepared.alter_table_target(), Some(expected.into()));
    assert!(prepared.access().schema_write());
    assert_eq!(prepared.access().schema_tables(), [TableId(2)]);
    assert!(prepared.access().read_tables().is_empty());
    assert!(prepared.access().write_tables().is_empty());
    assert_eq!(
        prepared.created_column_types().cloned().collect::<Vec<_>>(),
        [SemanticType::physical(PhysicalType::Bool)]
    );
    assert_eq!(files(&root), before);
    assert_eq!(db.next_storage_id(), Some(StorageId(3)));
    assert_eq!(db.next_column_id(TableId(2)), Some(ColumnId(3)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn current_schema_transaction_materializes_first_alter_and_rejects_a_second() {
    let root = root("single-mutation-boundary");
    let mut db = seed(&root);
    db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let projects = db.schema().table("projects").unwrap().id;
    let teams = db.schema().table("teams").unwrap().id;
    let project_name = db
        .schema()
        .table("projects")
        .unwrap()
        .column("name")
        .unwrap()
        .id;
    let base_generation = db.schema_generation();
    let base_revision = db.catalog_generation();
    let reserved_storage = db.next_storage_id().unwrap();
    let reserved_column = db.next_column_id(projects).unwrap();
    let old_storage = db.bindings.resolve_single(projects).unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    db.rewrite_heap_table_schema_legacy_in(
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

    let mutation = transaction.schema_mutation.as_ref().unwrap();
    assert_eq!(mutation.reservation.storage, reserved_storage);
    assert_eq!(
        mutation.rewrite.as_ref().unwrap().old_storage(),
        old_storage
    );
    assert_eq!(
        mutation.rewrite.as_ref().unwrap().new_storage(),
        reserved_storage
    );
    assert_eq!(
        mutation.staged.as_ref().unwrap().storage_id(),
        reserved_storage
    );
    assert_eq!(transaction.participant_count(), 1);
    assert_eq!(transaction.write_participant(), Some(reserved_storage));
    assert_eq!(
        transaction.participant_mode(reserved_storage),
        Some(crate::ParticipantMode::Write)
    );
    assert_eq!(
        mutation.target.committed.generation,
        SchemaGeneration(base_generation.0 + 1)
    );
    assert_eq!(
        mutation
            .target
            .committed
            .tables
            .iter()
            .find(|lineage| lineage.table_id == projects)
            .unwrap()
            .version,
        TableSchemaVersion(2)
    );
    assert!(
        mutation
            .target
            .committed
            .schema
            .table("projects")
            .unwrap()
            .column("active")
            .is_some()
    );
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );
    assert_eq!(db.schema_generation(), base_generation);
    assert_eq!(db.catalog_generation(), base_revision);
    assert!(matches!(
        db.begin_transaction(),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));
    assert_eq!(
        db.next_storage_id(),
        Some(StorageId(reserved_storage.0 + 1))
    );
    assert_eq!(
        db.next_column_id(projects),
        Some(ColumnId(reserved_column.0 + 1))
    );
    let journal = db.mutation_journal.as_ref().unwrap().borrow();
    assert_eq!(journal.rewrite_reservations.len(), 1);
    assert_eq!(journal.rewrites.len(), 1);
    assert!(journal.rewrites.contains_key(&transaction.id()));
    drop(journal);

    let same_table = db.rewrite_heap_table_schema_legacy_in(
        &mut transaction,
        AlterTableSpec::new(
            db.resolve_alter_table("projects").unwrap(),
            AlterTableOperation::RenameColumn {
                column_id: project_name,
                new_name: "title".into(),
            },
        ),
    );
    assert!(matches!(
        same_table,
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));
    let different_table = db.rewrite_heap_table_schema_legacy_in(
        &mut transaction,
        AlterTableSpec::new(
            db.resolve_alter_table("teams").unwrap(),
            AlterTableOperation::RenameTable {
                new_name: "groups".into(),
            },
        ),
    );
    assert!(matches!(
        different_table,
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::TransactionNotPristine
        ))
    ));

    assert!(matches!(
        db.prepare_sql_statement_in(
            &transaction,
            "ALTER TABLE projects RENAME COLUMN active TO enabled",
            &[],
        ),
        Err(DatabaseError::UnsupportedDdlCombination)
    ));
    transaction.rollback().unwrap();
    drop(transaction);

    assert_eq!(db.schema_generation(), base_generation);
    assert_eq!(db.catalog_generation(), base_revision);
    assert_eq!(db.bindings.resolve_single(projects).unwrap(), old_storage);
    assert_eq!(
        db.next_storage_id(),
        Some(StorageId(reserved_storage.0 + 1))
    );
    assert_eq!(
        db.next_column_id(projects),
        Some(ColumnId(reserved_column.0 + 1))
    );
    assert_eq!(db.schema().table("teams").unwrap().id, teams);
    let mut resumed = db.begin_transaction().unwrap();
    resumed.rollback().unwrap();
    drop(resumed);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn provisional_version_and_fingerprint_invalidate_prepared_across_overlay_changes() {
    let root = root("provisional-dependency");
    let mut db = seed(&root);
    let table_id = db.schema().table("projects").unwrap().id;
    let base_fingerprint = db
        .schema()
        .table("projects")
        .unwrap()
        .fingerprint()
        .unwrap();
    let global = db
        .prepare_statement("SELECT name FROM projects", &[])
        .unwrap();
    assert_eq!(
        global.schema_dependencies()[0].table_version,
        TableSchemaVersion(1)
    );

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects RENAME COLUMN name TO title",
    )
    .unwrap();
    let provisional = db
        .prepare_statement_in(&transaction, "SELECT title FROM projects", &[])
        .unwrap();
    let first_dependency = &provisional.schema_dependencies()[0];
    assert_eq!(first_dependency.table_version, TableSchemaVersion(2));
    assert_ne!(first_dependency.fingerprint, base_fingerprint);

    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects RENAME COLUMN title TO name",
    )
    .unwrap();
    let cycled_table = transaction
        .visible_schema(db.schema())
        .table("projects")
        .unwrap();
    assert_eq!(cycled_table.fingerprint().unwrap(), base_fingerprint);
    assert_eq!(
        transaction
            .schema_composition
            .plan()
            .unwrap()
            .overlay
            .tables
            .iter()
            .find(|lineage| lineage.table_id == table_id)
            .unwrap()
            .version,
        TableSchemaVersion(2)
    );
    assert!(matches!(
        db.validate_prepared_dependencies(&provisional, Some(&transaction)),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));
    assert!(matches!(
        db.validate_prepared_dependencies(&global, Some(&transaction)),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::StalePreparedStatement
        ))
    ));

    transaction.rollback().unwrap();
    drop(transaction);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn six_sql_alters_use_one_rewrite_lifecycle_and_preserve_logical_identities() {
    let root = root("six-operations");
    let mut db = seed(&root);
    let table_id = db.schema().table("projects").unwrap().id;
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    let index = db
        .create_named_index(
            IndexName::new("projects_name_idx").unwrap(),
            table_id,
            ColumnId(2),
        )
        .unwrap();
    let initial_storage = db.bindings.resolve_single(table_id).unwrap();
    let initial_generation = db.schema_generation();

    let mut add = db.begin_transaction().unwrap();
    assert_eq!(
        db.execute_in(&mut add, "ALTER TABLE projects ADD COLUMN active BOOLEAN")
            .unwrap(),
        ExecutionResult::AffectedRows(0)
    );
    assert_eq!(
        db.execute_in(
            &mut add,
            "SELECT id, name, active FROM projects ORDER BY id"
        )
        .unwrap(),
        ExecutionResult::Query(QueryResult {
            columns: vec![
                ResultColumn {
                    name: "id".into(),
                    data_type: SemanticType::physical(PhysicalType::Int64),
                    nullable: false,
                },
                ResultColumn {
                    name: "name".into(),
                    data_type: SemanticType::physical(PhysicalType::Text),
                    nullable: true,
                },
                ResultColumn {
                    name: "active".into(),
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: true,
                },
            ],
            rows: vec![
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("one".into()),
                    ScalarValue::Null
                ],
                vec![
                    ScalarValue::Int64(2),
                    ScalarValue::Text("two".into()),
                    ScalarValue::Null
                ],
            ],
        })
    );
    db.execute_in(&mut add, "INSERT INTO projects VALUES (3, 'three', true)")
        .unwrap();
    db.commit_transaction(&mut add).unwrap();
    drop(add);

    for sql in [
        "ALTER TABLE projects RENAME COLUMN name TO title",
        "ALTER TABLE projects ALTER COLUMN title SET NOT NULL",
        "ALTER TABLE projects ALTER COLUMN title DROP NOT NULL",
        "ALTER TABLE projects DROP COLUMN active",
        "ALTER TABLE projects RENAME TO work",
    ] {
        assert_eq!(db.execute(sql).unwrap(), ExecutionResult::AffectedRows(0));
    }

    let table = db.schema().table("work").unwrap();
    assert_eq!(table.id, table_id);
    assert_eq!(table.columns[0].id, ColumnId(1));
    assert_eq!(table.columns[1].id, ColumnId(2));
    assert_eq!(table.columns[1].name, "title");
    assert!(table.columns[1].nullable);
    assert_eq!(db.indexes(table_id).unwrap()[0].id, index.id);
    assert_eq!(db.indexes(table_id).unwrap()[0].name, index.name);
    assert_eq!(db.indexes(table_id).unwrap()[0].column_id, ColumnId(2));
    assert_eq!(
        db.bindings.resolve_single(table_id).unwrap(),
        StorageId(initial_storage.0 + 6)
    );
    assert_eq!(
        db.table_schema_version(table_id),
        Some(TableSchemaVersion(7))
    );
    assert_eq!(
        db.schema_generation(),
        SchemaGeneration(initial_generation.0 + 6)
    );
    assert_eq!(
        db.query("SELECT id, title FROM work ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
            vec![ScalarValue::Int64(2), ScalarValue::Text("two".into())],
            vec![ScalarValue::Int64(3), ScalarValue::Text("three".into())],
        ]
    );
    let retired = db.inspect_replacement_retired_heaps();
    assert_eq!(retired.len(), 6);
    assert_eq!(retired[0].old_storage_id, initial_storage);
    assert_eq!(
        db.gc_replacement_retired_heap(&retired[0]).unwrap().state,
        RetiredHeapGcState::Deleted
    );
    db.close().unwrap();
    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.schema().table("work").unwrap().id, table_id);
        assert_eq!(
            reopened.table_schema_version(table_id),
            Some(TableSchemaVersion(7))
        );
        assert_eq!(reopened.indexes(table_id).unwrap()[0].id, index.id);
        assert_eq!(
            reopened
                .query("SELECT id, title FROM work ORDER BY id")
                .unwrap()
                .rows
                .len(),
            3
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_alter_is_exact_but_ignores_data_and_index_only_revisions() {
    let root = root("prepared-exact");
    let mut db = seed(&root);
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let add = db
        .prepare_ddl_statement("ALTER TABLE projects ADD COLUMN active BOOL")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    db.create_named_index(
        IndexName::new("projects_name_idx").unwrap(),
        TableId(2),
        ColumnId(2),
    )
    .unwrap();
    assert_eq!(db.execute_ddl(&add).unwrap(), DdlOutcome::Altered);
    assert_eq!(
        db.query("SELECT id, active FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Null],
            vec![ScalarValue::Int64(2), ScalarValue::Null],
        ]
    );
    let storage_after = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&add).unwrap_err().kind(),
        DatabaseErrorKind::TransactionState
    );
    assert_eq!(db.next_storage_id(), storage_after);

    let stale = db
        .prepare_ddl_statement("ALTER TABLE projects RENAME TO work")
        .unwrap();
    db.execute("ALTER TABLE projects RENAME COLUMN name TO title")
        .unwrap();
    let before = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&stale).unwrap_err().kind(),
        DatabaseErrorKind::TransactionState
    );
    assert_eq!(db.next_storage_id(), before);
    assert!(db.schema().table("projects").is_some());
    assert!(db.schema().table("work").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_alter_never_rebinds_after_drop_and_same_name_recreate() {
    let root = root("drop-recreate-exact");
    let mut db = seed(&root);
    let prepared = db
        .prepare_ddl_statement("ALTER TABLE projects RENAME TO work")
        .unwrap();
    let original = prepared.alter_table_target().unwrap();
    db.execute("DROP TABLE projects").unwrap();
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, replacement TEXT)")
        .unwrap();
    let replacement = db.schema().table("projects").unwrap().id;
    assert_ne!(replacement, original.table_id);
    let storage = db.bindings.resolve_single(replacement).unwrap();
    let next_storage = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&prepared).unwrap_err().kind(),
        DatabaseErrorKind::UndefinedTable
    );
    assert_eq!(db.schema().table("projects").unwrap().id, replacement);
    assert!(db.schema().table("work").is_none());
    assert_eq!(db.bindings.resolve_single(replacement).unwrap(), storage);
    assert_eq!(db.next_storage_id(), next_storage);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_drop_column_revalidates_new_index_and_pristine_admission() {
    let root = root("late-index-pristine");
    let mut db = seed(&root);
    let drop_name = db
        .prepare_ddl_statement("ALTER TABLE projects DROP COLUMN name")
        .unwrap();
    db.create_named_index(
        IndexName::new("projects_name_idx").unwrap(),
        TableId(2),
        ColumnId(2),
    )
    .unwrap();
    let before = db.next_storage_id();
    assert_eq!(
        db.execute_ddl(&drop_name).unwrap_err().kind(),
        DatabaseErrorKind::DependentObjects
    );
    assert_eq!(db.next_storage_id(), before);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .is_some()
    );

    let prepared = db
        .prepare_ddl_statement("ALTER TABLE projects ADD COLUMN active BOOL")
        .unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(&mut transaction, "SELECT * FROM seed")
        .unwrap();
    assert_eq!(
        db.execute_ddl_in(&mut transaction, &prepared)
            .unwrap_err()
            .kind(),
        DatabaseErrorKind::TransactionState
    );
    assert_eq!(db.next_storage_id(), before);
    transaction.rollback().unwrap();
    drop(transaction);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_alter_rollback_burns_ids_but_restores_schema_and_rows() {
    let root = root("rollback");
    let mut db = seed(&root);
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let generation = db.schema_generation();
    let storage = db.bindings.resolve_single(TableId(2)).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "INSERT INTO projects VALUES (2, 'two', true)",
    )
    .unwrap();
    transaction.rollback().unwrap();
    drop(transaction);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );
    assert_eq!(db.query("SELECT * FROM projects").unwrap().rows.len(), 1);
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.bindings.resolve_single(TableId(2)).unwrap(), storage);
    assert_eq!(db.next_storage_id(), Some(StorageId(storage.0 + 2)));
    assert_eq!(db.next_column_id(TableId(2)), Some(ColumnId(4)));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_alter_crash_child() {
    let Ok(root) = std::env::var("NETBADB_SQL_ALTER_CHILD") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    db.execute("ALTER TABLE projects ADD COLUMN active BOOLEAN")
        .unwrap();
    panic!("configured SQL ALTER crash hook was not reached");
}

#[test]
fn multi_alter_crash_child() {
    let Ok(root) = std::env::var("NETBADB_MULTI_ALTER_CHILD") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects RENAME COLUMN name TO title",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE teams RENAME COLUMN name TO label",
    )
    .unwrap();
    db.commit_transaction(&mut transaction).unwrap();
    panic!("configured multi-ALTER crash hook was not reached");
}

#[test]
fn composed_cross_table_crash_matrix_converges_on_three_reopens() {
    for (point, winner) in [
        ("composition-before-intent", false),
        ("composition-intent-durable", false),
        ("composition-after-target-create-1", false),
        ("composition-after-table-copy-1", false),
        ("composition-all-targets-staged", false),
        ("composition-prepared-catalog-durable", false),
        ("composition-before-coordinator-decision", false),
        ("coordinator-durable", true),
        ("composition-after-promotion-1", true),
        ("composition-after-retirement-1", true),
        ("composition-before-nbsc-publication", true),
        ("composition-after-cord-complete", true),
        ("composition-after-winner-resolution", true),
        ("composition-before-memory-publication", true),
    ] {
        let root = root(&format!("round28-multi-crash-{point}"));
        let mut db = seed(&root);
        db.execute("CREATE TABLE teams (id BIGINT NOT NULL, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO projects VALUES (1, 'one')")
            .unwrap();
        db.execute("INSERT INTO teams VALUES (1, 'red')").unwrap();
        let base_generation = db.schema_generation();
        let projects = db.schema().table("projects").unwrap().id;
        let teams = db.schema().table("teams").unwrap().id;
        let project_storage = db.bindings.resolve_single(projects).unwrap();
        let team_storage = db.bindings.resolve_single(teams).unwrap();
        db.close().unwrap();

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sql_alter_table_tests::multi_alter_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_MULTI_ALTER_CHILD", &root)
            .env("NETBADB_REWRITE_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let db = Database::open_catalog(root.join("catalog")).unwrap();
            assert_eq!(
                db.schema_generation(),
                SchemaGeneration(base_generation.0 + u64::from(winner))
            );
            if winner {
                assert!(
                    db.schema()
                        .table("projects")
                        .unwrap()
                        .column("title")
                        .is_some()
                );
                assert!(
                    db.schema()
                        .table("teams")
                        .unwrap()
                        .column("label")
                        .is_some()
                );
                assert_ne!(
                    db.bindings.resolve_single(projects).unwrap(),
                    project_storage
                );
                assert_ne!(db.bindings.resolve_single(teams).unwrap(), team_storage);
                assert_eq!(db.inspect_replacement_retired_heaps().len(), 2);
            } else {
                assert!(
                    db.schema()
                        .table("projects")
                        .unwrap()
                        .column("name")
                        .is_some()
                );
                assert!(db.schema().table("teams").unwrap().column("name").is_some());
                assert_eq!(
                    db.bindings.resolve_single(projects).unwrap(),
                    project_storage
                );
                assert_eq!(db.bindings.resolve_single(teams).unwrap(), team_storage);
                assert!(db.inspect_replacement_retired_heaps().is_empty());
            }
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn sql_driven_alter_loser_and_winner_recover_three_times_without_reparse() {
    for (point, winner) in [
        ("before-coordinator-decision", false),
        ("coordinator-durable", true),
    ] {
        let root = root(&format!("crash-{point}"));
        let mut seeded = seed(&root);
        seeded
            .execute("INSERT INTO projects VALUES (1, 'one')")
            .unwrap();
        seeded.close().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sql_alter_table_tests::sql_alter_crash_child",
                "--nocapture",
            ])
            .env("NETBADB_SQL_ALTER_CHILD", &root)
            .env("NETBADB_REWRITE_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(90),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for _ in 0..3 {
            let mut db = Database::open_catalog(root.join("catalog")).unwrap();
            let table = db.schema().table("projects").unwrap();
            assert_eq!(table.id, TableId(2));
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
                StorageId(if winner { 3 } else { 2 })
            );
            assert_eq!(db.next_storage_id(), Some(StorageId(4)));
            assert_eq!(db.next_column_id(table.id), Some(ColumnId(4)));
            assert_eq!(table.column("active").is_some(), winner);
            if winner {
                assert_eq!(
                    db.query("SELECT id, active FROM projects").unwrap().rows,
                    [vec![ScalarValue::Int64(1), ScalarValue::Null]]
                );
                assert_eq!(db.inspect_replacement_retired_heaps().len(), 1);
            } else {
                assert!(db.inspect_replacement_retired_heaps().is_empty());
            }
            db.close().unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
