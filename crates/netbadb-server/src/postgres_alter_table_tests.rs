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
        "SELECT id, name, active FROM projects",
    ));
    ok(&sql(
        &mut session,
        &mut db,
        "INSERT INTO projects VALUES (2, 'two', true)",
    ));
    ok(&sql(&mut session, &mut db, "ROLLBACK"));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );

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
                BackendMessage::ReadyForQuery(b'I')
            ],
            "{source}"
        );
    }
    assert_eq!(db.schema().table("work").unwrap().id, TableId(2));
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
