use super::*;
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use std::path::{Path, PathBuf};

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "netbadb-round31-{name}-{}-{:?}",
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
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    db
}

fn rows(result: ExecutionResult) -> Vec<Vec<ScalarValue>> {
    match result {
        ExecutionResult::Query(result) => result.rows,
        ExecutionResult::AffectedRows(_) => panic!("expected query"),
    }
}

#[test]
fn staged_backfill_is_read_your_writes_but_production_refinement_is_sealed() {
    let root = root("staged-backfill");
    let mut db = seed(&root);
    let projects = db.schema().table("projects").unwrap().id;
    let base_generation = db.schema_generation();
    let base_storage = db.bindings.resolve_single(projects).unwrap();

    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN normalized_name TEXT",
    )
    .unwrap();
    let provisional = transaction
        .schema_composition
        .plan()
        .unwrap()
        .overlay
        .schema
        .table("projects")
        .unwrap()
        .clone();
    let normalized = provisional.column("normalized_name").unwrap().id;
    assert!(provisional.column("normalized_name").unwrap().nullable);
    let prepared = match db
        .prepare_sql_statement_in(
            &transaction,
            "SELECT normalized_name FROM projects ORDER BY id",
            &[],
        )
        .unwrap()
    {
        PreparedSqlStatement::Relational(prepared) => prepared,
        PreparedSqlStatement::Ddl(_) => panic!("expected relational statement"),
    };
    assert_eq!(
        prepared.schema_dependencies()[0].fingerprint,
        provisional.fingerprint().unwrap()
    );

    assert_eq!(
        db.execute_in(
            &mut transaction,
            "UPDATE projects SET normalized_name = 'filled-one' WHERE id = 1",
        )
        .unwrap(),
        ExecutionResult::AffectedRows(1)
    );
    assert!(transaction.schema_composition.materialized().is_some());
    assert_ne!(transaction.staged_binding(projects), Some(base_storage));
    assert_eq!(
        rows(
            db.execute_in(
                &mut transaction,
                "SELECT id, normalized_name FROM projects ORDER BY id",
            )
            .unwrap()
        ),
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled-one".into())
            ],
            vec![ScalarValue::Int64(2), ScalarValue::Null],
        ]
    );
    assert!(matches!(
        db.audit_validate_staged_not_null(&mut transaction, projects, normalized),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::NotNullViolation(column)
        )) if column == normalized
    ));

    db.execute_in(
        &mut transaction,
        "UPDATE projects SET normalized_name = 'filled-two' WHERE id = 2",
    )
    .unwrap();
    db.audit_validate_staged_not_null(&mut transaction, projects, normalized)
        .unwrap();
    assert_eq!(
        rows(
            db.execute_in(
                &mut transaction,
                "SELECT id, normalized_name FROM projects ORDER BY id",
            )
            .unwrap()
        ),
        vec![
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Text("filled-one".into())
            ],
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Text("filled-two".into())
            ],
        ]
    );

    let materialized = transaction.schema_composition.materialized().unwrap();
    assert_eq!(
        materialized.target.committed.schema,
        materialized.logical.overlay.schema
    );
    assert_eq!(
        materialized.intent.target_generation,
        materialized.target.committed.generation
    );
    assert_eq!(
        materialized.intent.snapshot_digest,
        crate::schema_mutation::digest(&materialized.target.encode().unwrap())
    );
    let mut refined = provisional.clone();
    refined
        .columns
        .iter_mut()
        .find(|column| column.id == normalized)
        .unwrap()
        .nullable = false;
    assert_ne!(
        provisional.fingerprint().unwrap(),
        refined.fingerprint().unwrap()
    );
    assert_ne!(
        prepared.schema_dependencies()[0].fingerprint,
        refined.fingerprint().unwrap()
    );

    let error = db
        .execute_in(
            &mut transaction,
            "ALTER TABLE projects ALTER COLUMN normalized_name SET NOT NULL",
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::SchemaMutationAfterMaterialization)
    ));
    assert_eq!(transaction.state(), TransactionState::Active);
    assert!(matches!(
        db.begin_transaction(),
        Err(DatabaseError::SchemaMutation(
            SchemaMutationError::SchemaBusy
        ))
    ));

    transaction.rollback().unwrap();
    assert_eq!(db.schema_generation(), base_generation);
    assert_eq!(db.bindings.resolve_single(projects), Ok(base_storage));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("normalized_name")
            .is_none()
    );
    assert_eq!(
        db.query("SELECT id, name FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
            vec![ScalarValue::Int64(2), ScalarValue::Text("two".into())],
        ]
    );
    db.close().unwrap();

    for _ in 0..3 {
        let reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert!(
            reopened
                .schema()
                .table("projects")
                .unwrap()
                .column("normalized_name")
                .is_none()
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn transaction_created_heap_reads_own_rows_before_sealed_refinement_rejection() {
    let root = root("created-backfill");
    let mut db = seed(&root);
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "CREATE TABLE imported (id BIGINT NOT NULL, label TEXT)",
    )
    .unwrap();
    db.execute_in(&mut transaction, "INSERT INTO imported VALUES (7, 'seven')")
        .unwrap();
    assert_eq!(
        rows(
            db.execute_in(&mut transaction, "SELECT id, label FROM imported")
                .unwrap()
        ),
        vec![vec![
            ScalarValue::Int64(7),
            ScalarValue::Text("seven".into())
        ]]
    );
    let error = db
        .execute_in(
            &mut transaction,
            "ALTER TABLE imported ALTER COLUMN label SET NOT NULL",
        )
        .unwrap_err();
    assert!(matches!(
        error,
        DatabaseError::SchemaMutation(SchemaMutationError::SchemaMutationAfterMaterialization)
    ));
    transaction.rollback().unwrap();
    assert!(db.schema().table("imported").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn staged_backfill_predecision_crash_child() {
    let Ok(root) = std::env::var("NETBADB_ROUND31_BACKFILL_CHILD") else {
        return;
    };
    let mut db = Database::open_catalog(Path::new(&root).join("catalog")).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    db.execute_in(
        &mut transaction,
        "ALTER TABLE projects ADD COLUMN normalized_name TEXT",
    )
    .unwrap();
    db.execute_in(
        &mut transaction,
        "UPDATE projects SET normalized_name = 'filled'",
    )
    .unwrap();
    std::process::exit(90);
}

#[test]
fn predecision_crash_discards_private_backfill_and_preserves_base_winner() {
    if std::env::var_os("NETBADB_ROUND31_BACKFILL_CHILD").is_some() {
        return;
    }
    let root = root("predecision-crash");
    let seeded = seed(&root);
    let projects = seeded.schema().table("projects").unwrap().id;
    let base_storage = seeded.bindings.resolve_single(projects).unwrap();
    let reserved_storage = seeded.next_storage_id().unwrap();
    let reserved_column = seeded.next_column_id(projects).unwrap();
    seeded.close().unwrap();

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "migration_backfill_audit_tests::staged_backfill_predecision_crash_child",
            "--nocapture",
        ])
        .env("NETBADB_ROUND31_BACKFILL_CHILD", &root)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(90),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    for _ in 0..3 {
        let mut reopened = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(reopened.bindings.resolve_single(projects), Ok(base_storage));
        assert!(
            reopened
                .schema()
                .table("projects")
                .unwrap()
                .column("normalized_name")
                .is_none()
        );
        assert_eq!(
            reopened
                .query("SELECT id, name FROM projects ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Text("one".into())],
                vec![ScalarValue::Int64(2), ScalarValue::Text("two".into())],
            ]
        );
        assert_eq!(
            reopened.next_storage_id(),
            Some(StorageId(reserved_storage.0 + 1))
        );
        assert_eq!(
            reopened.next_column_id(projects),
            Some(ColumnId(reserved_column.0 + 1))
        );
        reopened.close().unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}
