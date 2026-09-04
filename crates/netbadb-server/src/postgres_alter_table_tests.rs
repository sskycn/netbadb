use super::*;
use crate::authorization::{
    AuthorizationPolicy, PrincipalAuthorization, PrincipalGrants, TablePermissions,
};
use crate::sql_create_table_test_support::{files, seed};
use netbadb_types::{ColumnId, IndexName, TableId};

fn principal(schema_admin: bool) -> PrincipalAuthorization {
    AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(PrincipalGrants {
            schema_admin,
            tables: vec![TablePermissions::new(TableId(2), true, true, true, false)],
        }),
        vec![],
        &[TableId(1), TableId(2)],
    )
    .unwrap()
    .admit(&ClientIdentity::LocalPlaintext)
    .unwrap()
}

fn session(db: &Database, schema_admin: bool) -> PgWorkerSession {
    PgWorkerSession::new(
        db,
        SessionPolicy::default(),
        principal(schema_admin),
        StartupMessage {
            parameters: Default::default(),
        },
        1,
    )
    .unwrap()
    .0
}

fn sql(session: &mut PgWorkerSession, db: &mut Database, source: &str) -> Vec<BackendMessage> {
    session.handle(db, FrontendMessage::Query(source.into()))
}

fn ok(messages: &[BackendMessage]) {
    assert!(
        !messages
            .iter()
            .any(|message| matches!(message, BackendMessage::ErrorResponse(_))),
        "{messages:?}"
    );
}

fn state(messages: &[BackendMessage], expected: &str) {
    assert!(
        messages.iter().any(
            |message| matches!(message, BackendMessage::ErrorResponse(error) if error.sqlstate == expected)
        ),
        "{messages:?}"
    );
}

fn project(name: &str) -> (std::path::PathBuf, Database) {
    let (root, mut db) = seed(name);
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    (root, db)
}

#[test]
fn pg_simple_query_covers_all_six_alter_actions_and_transactional_dml() {
    let (root, mut db) = project("pg-alter-simple");
    let mut session = session(&db, true);
    ok(&sql(&mut session, &mut db, "BEGIN"));
    assert_eq!(
        sql(
            &mut session,
            &mut db,
            "ALTER TABLE projects ADD COLUMN active BOOLEAN"
        ),
        [
            BackendMessage::CommandComplete("ALTER TABLE".into()),
            BackendMessage::ReadyForQuery(b'T')
        ]
    );
    ok(&sql(
        &mut session,
        &mut db,
        "ALTER TABLE projects RENAME COLUMN name TO title",
    ));
    ok(&sql(
        &mut session,
        &mut db,
        "SELECT id, title, active FROM projects",
    ));
    ok(&sql(
        &mut session,
        &mut db,
        "INSERT INTO projects VALUES (2, 'two', true)",
    ));
    ok(&sql(
        &mut session,
        &mut db,
        "ALTER TABLE projects RENAME COLUMN title TO name",
    ));
    ok(&sql(&mut session, &mut db, "ROLLBACK"));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );

    ok(&sql(&mut session, &mut db, "BEGIN"));
    for source in [
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
        "ALTER TABLE projects RENAME COLUMN name TO title",
        "ALTER TABLE projects ALTER COLUMN title SET NOT NULL",
        "ALTER TABLE projects ALTER COLUMN title DROP NOT NULL",
        "ALTER TABLE projects DROP COLUMN active",
        "ALTER TABLE projects RENAME TO work",
    ] {
        assert_eq!(
            sql(&mut session, &mut db, source),
            [
                BackendMessage::CommandComplete("ALTER TABLE".into()),
                BackendMessage::ReadyForQuery(b'T')
            ],
            "{source}"
        );
    }
    ok(&sql(&mut session, &mut db, "COMMIT"));
    assert_eq!(db.schema().table("work").unwrap().id, TableId(2));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_drop_first_source_backfill_publishes_one_fresh_replacement() {
    let (root, mut db) = project("pg-round39-drop-first");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let base_version = db.table_schema_version(TableId(2)).unwrap();
    let base_generation = db.schema_generation();
    let target_storage = db.next_storage_id().unwrap();
    let mut admin = session(&db, true);

    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        ("DROP INDEX projects_name_idx", "DROP INDEX"),
        (
            "UPDATE projects SET name = 'filled' WHERE name IS NULL",
            "UPDATE 1",
        ),
        (
            "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
            "ALTER TABLE",
        ),
        (
            "CREATE INDEX projects_name_idx ON projects(name)",
            "CREATE INDEX",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' })
            ],
            "{source}"
        );
    }

    let table = db.schema().table("projects").unwrap();
    assert_eq!(table.id, TableId(2));
    assert!(!table.column("name").unwrap().nullable);
    assert_eq!(
        db.table_schema_version(TableId(2)).unwrap().0,
        base_version.0 + 1
    );
    assert_eq!(db.schema_generation().0, base_generation.0 + 1);
    assert_eq!(db.next_storage_id().unwrap().0, target_storage.0 + 1);
    let replacement = db.indexes(TableId(2)).unwrap();
    assert_eq!(replacement.len(), 1);
    assert_ne!(replacement[0].id, old.id);
    assert_eq!(replacement[0].name.as_ref(), old.name.as_ref());
    assert_eq!(replacement[0].column_id, old.column_id);
    assert_eq!(
        db.query("SELECT id, name FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                netbadb_types::ScalarValue::Int64(1),
                netbadb_types::ScalarValue::Text("one".into()),
            ],
            vec![
                netbadb_types::ScalarValue::Int64(2),
                netbadb_types::ScalarValue::Text("filled".into()),
            ],
        ]
    );
    assert_eq!(db.inspect_replacement_retired_heaps().len(), 1);
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
fn pg_drop_first_partial_backfill_and_post_refinement_dml_enter_e() {
    let (root, mut db) = project("pg-round39-errors");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let mut admin = session(&db, true);

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(&mut admin, &mut db, "DROP INDEX projects_name_idx"));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET name = 'one-updated' WHERE id = 1",
    ));
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
        ),
        "23502",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM projects"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));

    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );
    assert_eq!(db.indexes(TableId(2)).unwrap().len(), 1);
    assert_eq!(db.indexes(TableId(2)).unwrap(), std::slice::from_ref(&old));
    assert_eq!(
        db.query("SELECT name FROM projects WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec![netbadb_types::ScalarValue::Null]]
    );

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    for source in [
        "DROP INDEX projects_name_idx",
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE projects SET name = 'late' WHERE id = 1",
        ),
        "25000",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM projects"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    assert_eq!(db.indexes(TableId(2)).unwrap(), [old]);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_drop_update_commit_remains_the_ordinary_source_path() {
    let (root, mut db) = project("pg-round39-no-alter");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let version = db.table_schema_version(TableId(2)).unwrap();
    let generation = db.schema_generation();
    let storage_floor = db.next_storage_id();
    let mut admin = session(&db, true);

    for source in [
        "BEGIN",
        "DROP INDEX projects_name_idx",
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
        "COMMIT",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    assert_eq!(db.table_schema_version(TableId(2)), Some(version));
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.next_storage_id(), storage_floor);
    assert!(db.indexes(TableId(2)).unwrap().is_empty());
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );
    assert!(db.inspect_replacement_retired_heaps().is_empty());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_drop_first_alter_is_pure_until_execute() {
    let (root, mut db) = project("pg-round39-extended-alter");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let mut admin = session(&db, true);
    let before_prepare = files(&root);

    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "round39-alter".into(),
                query: "ALTER TABLE projects ALTER COLUMN name SET NOT NULL".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(files(&root), before_prepare);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "round39-alter".into(),
                statement: "round39-alter".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Portal,
                name: "round39-alter".into(),
            },
        ),
        [BackendMessage::NoData]
    );
    assert_eq!(files(&root), before_prepare);

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(&mut admin, &mut db, "DROP INDEX projects_name_idx"));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
    ));
    ok(&sql(&mut admin, &mut db, "SELECT id FROM projects"));
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "round39-alter".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("ALTER TABLE".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    ok(&sql(
        &mut admin,
        &mut db,
        "CREATE INDEX projects_name_idx ON projects(name)",
    ));
    ok(&sql(&mut admin, &mut db, "COMMIT"));

    let replacement = db.indexes(TableId(2)).unwrap();
    assert_eq!(replacement.len(), 1);
    assert_ne!(replacement[0].id, old.id);
    assert!(
        !db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_create_prepared_under_f1_stales_before_reservation() {
    let (root, mut db) = project("pg-round39-extended-stale-create");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let mut admin = session(&db, true);

    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "old-create".into(),
                query: "CREATE INDEX projects_name_idx ON projects(name)".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "old-create".into(),
                statement: "old-create".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    for source in [
        "DROP INDEX projects_name_idx",
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "old-create".into(),
                max_rows: 0,
            },
        ),
        "25000",
    );
    assert_eq!(
        admin.handle(&mut db, FrontendMessage::Sync),
        [BackendMessage::ReadyForQuery(b'E')]
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM projects"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    assert_eq!(db.indexes(TableId(2)).unwrap(), [old]);

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_staged_index_evacuation_closes_dml_and_publishes_a_fresh_replacement() {
    let (root, mut db) = project("pg-round36-staged-indexed-nullability");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let mut admin = session(&db, true);

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(
        &mut admin,
        &mut db,
        "ALTER TABLE projects ADD COLUMN migration_marker TEXT",
    ));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
    ));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET migration_marker = 'done'",
    ));
    assert_eq!(
        sql(&mut admin, &mut db, "DROP INDEX projects_name_idx"),
        [
            BackendMessage::CommandComplete("DROP INDEX".into()),
            BackendMessage::ReadyForQuery(b'T')
        ]
    );
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE projects SET migration_marker = 'late'",
        ),
        "25000",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    for source in [
        "ALTER TABLE projects ADD COLUMN migration_marker TEXT",
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
        "UPDATE projects SET migration_marker = 'done'",
        "DROP INDEX projects_name_idx",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
        "CREATE INDEX projects_name_idx ON projects(name)",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    ok(&sql(&mut admin, &mut db, "COMMIT"));

    let table = db.schema().table("projects").unwrap();
    assert!(!table.column("name").unwrap().nullable);
    assert!(table.column("migration_marker").is_some());
    let replacement = db.indexes(TableId(2)).unwrap();
    assert_eq!(replacement.len(), 1);
    assert_eq!(replacement[0].name.as_ref(), old.name.as_ref());
    assert_ne!(replacement[0].id, old.id);
    assert_eq!(replacement[0].column_id, old.column_id);
    assert_eq!(
        db.query("SELECT id, name, migration_marker FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                netbadb_types::ScalarValue::Int64(1),
                netbadb_types::ScalarValue::Text("one".into()),
                netbadb_types::ScalarValue::Text("done".into()),
            ],
            vec![
                netbadb_types::ScalarValue::Int64(2),
                netbadb_types::ScalarValue::Text("filled".into()),
                netbadb_types::ScalarValue::Text("done".into()),
            ],
        ]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_staged_index_evacuation_reports_partial_backfill_and_allows_no_replacement() {
    let (root, mut db) = project("pg-round36-partial-and-no-replacement");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let mut admin = session(&db, true);

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    for source in [
        "ALTER TABLE projects ADD COLUMN migration_marker TEXT",
        "UPDATE projects SET migration_marker = 'done'",
        "DROP INDEX projects_name_idx",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
        ),
        "23502",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM projects"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );
    assert_eq!(db.indexes(TableId(2)).unwrap(), [old]);

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    for source in [
        "ALTER TABLE projects ADD COLUMN migration_marker TEXT",
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
        "UPDATE projects SET migration_marker = 'done'",
        "DROP INDEX projects_name_idx",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert!(
        !db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );
    assert!(db.indexes(TableId(2)).unwrap().is_empty());

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_parse_bind_describe_are_pure_and_execute_is_exact() {
    let (root, mut db) = project("pg-alter-extended");
    let mut first = session(&db, true);
    let before = files(&root);
    assert_eq!(
        first.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "alter".into(),
                query: "ALTER TABLE projects ADD COLUMN active BOOLEAN".into(),
                parameter_types: vec![],
            }
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        first.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "alter".into(),
                statement: "alter".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            }
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        first.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Statement,
                name: "alter".into(),
            }
        ),
        [
            BackendMessage::ParameterDescription(vec![]),
            BackendMessage::NoData
        ]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        first.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "alter".into(),
                max_rows: 0,
            }
        ),
        [BackendMessage::CommandComplete("ALTER TABLE".into())]
    );
    ok(&first.handle(&mut db, FrontendMessage::Sync));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_some()
    );
    assert_eq!(
        first.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "alter-again".into(),
                statement: "alter".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            }
        ),
        [BackendMessage::BindComplete]
    );
    state(
        &first.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "alter-again".into(),
                max_rows: 0,
            },
        ),
        "25000",
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_staged_drop_evacuation_occurs_only_on_execute() {
    let (root, mut db) = project("pg-round36-extended-drop");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let mut admin = session(&db, true);
    let before_parse = files(&root);

    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "drop".into(),
                query: "DROP INDEX projects_name_idx".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(files(&root), before_parse);
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    for source in [
        "ALTER TABLE projects ADD COLUMN migration_marker TEXT",
        "UPDATE projects SET name = 'filled' WHERE name IS NULL",
        "UPDATE projects SET migration_marker = 'done'",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    let before_bind = files(&root);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "drop".into(),
                statement: "drop".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Portal,
                name: "drop".into(),
            },
        ),
        [BackendMessage::NoData]
    );
    assert_eq!(files(&root), before_bind);
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET migration_marker = 'still-open'",
    ));

    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "drop".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("DROP INDEX".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE projects SET migration_marker = 'closed'",
        ),
        "25000",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    assert_eq!(db.indexes(TableId(2)).unwrap().len(), 1);
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("migration_marker")
            .is_none()
    );

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_alter_never_rebinds_after_drop_and_recreate() {
    let (root, mut db) = project("pg-alter-drop-recreate");
    let mut prepared = session(&db, true);
    assert_eq!(
        prepared.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "alter".into(),
                query: "ALTER TABLE projects RENAME TO work".into(),
                parameter_types: vec![],
            }
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        prepared.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "alter".into(),
                statement: "alter".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            }
        ),
        [BackendMessage::BindComplete]
    );

    let mut concurrent = session(&db, true);
    ok(&sql(&mut concurrent, &mut db, "DROP TABLE projects"));
    ok(&sql(
        &mut concurrent,
        &mut db,
        "CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)",
    ));
    assert_eq!(db.schema().table("projects").unwrap().id, TableId(3));
    let next_storage = db.next_storage_id();

    state(
        &prepared.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "alter".into(),
                max_rows: 0,
            },
        ),
        "42P01",
    );
    assert_eq!(db.schema().table("projects").unwrap().id, TableId(3));
    assert_eq!(db.next_storage_id(), next_storage);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_alter_permission_errors_unsupported_forms_and_sqlstates_fail_closed() {
    let (root, mut db) = project("pg-alter-errors");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    let before = files(&root);
    let mut denied = session(&db, false);
    state(
        &sql(&mut denied, &mut db, "ALTER TABLE projects RENAME TO work"),
        "42501",
    );
    assert_eq!(files(&root), before);

    let mut admin = session(&db, true);
    let next_storage = db.next_storage_id();
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ADD COLUMN native_only UINT64",
        ),
        "0A000",
    );
    assert_eq!(db.next_storage_id(), next_storage);
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE missing ADD COLUMN active BOOL",
        ),
        "42P01",
    );
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ADD COLUMN name TEXT",
        ),
        "42701",
    );
    for unsupported in [
        "ALTER TABLE projects ADD COLUMN bad BIGINT NOT NULL",
        "ALTER TABLE projects ALTER COLUMN name TYPE TEXT",
        "ALTER TABLE projects DROP COLUMN name CASCADE",
        "ALTER TABLE projects ADD COLUMN a BIGINT, ADD COLUMN b BIGINT",
    ] {
        state(&sql(&mut admin, &mut db, unsupported), "0A000");
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
        ),
        "23502",
    );
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .unwrap()
            .nullable
    );

    db.create_named_index(
        IndexName::new("projects_name_idx").unwrap(),
        TableId(2),
        ColumnId(2),
    )
    .unwrap();
    state(
        &sql(&mut admin, &mut db, "ALTER TABLE projects DROP COLUMN name"),
        "2BP01",
    );
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ALTER COLUMN name TYPE TEXT",
        ),
        "0A000",
    );
    state(&sql(&mut admin, &mut db, "SELECT * FROM projects"), "25P02");
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
