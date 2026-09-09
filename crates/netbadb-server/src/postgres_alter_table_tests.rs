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

fn layout_project(name: &str) -> (std::path::PathBuf, Database) {
    layout_project_with_email_nullability(name, false)
}

fn terminal_layout_project(name: &str) -> (std::path::PathBuf, Database) {
    let (root, mut db) = seed(name);
    db.execute("CREATE TABLE accounts (id BIGINT NOT NULL, legacy TEXT, flag BOOLEAN)")
        .unwrap();
    for statement in [
        "INSERT INTO accounts VALUES (1, 'old1', true)",
        "INSERT INTO accounts VALUES (2, 'old2', false)",
        "INSERT INTO accounts VALUES (3, NULL, NULL)",
    ] {
        db.execute(statement).unwrap();
    }
    (root, db)
}

fn indexed_terminal_layout_project(name: &str) -> (std::path::PathBuf, Database) {
    let (root, mut db) = terminal_layout_project(name);
    db.execute("CREATE INDEX accounts_legacy_idx ON accounts(legacy)")
        .unwrap();
    (root, db)
}

fn prepare_indexed_terminal_swap(admin: &mut PgWorkerSession, db: &mut Database) {
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 999",
        "ALTER TABLE accounts ADD COLUMN shadow TEXT",
        "UPDATE accounts SET shadow = legacy WHERE legacy IS NOT NULL",
        "UPDATE accounts SET shadow = 'missing' WHERE shadow IS NULL",
        "ALTER TABLE accounts ALTER COLUMN shadow SET NOT NULL",
    ] {
        ok(&sql(admin, db, source));
    }
}

fn layout_project_with_email_nullability(
    name: &str,
    email_not_null: bool,
) -> (std::path::PathBuf, Database) {
    let (root, mut db) = seed(name);
    let nullability = if email_not_null { " NOT NULL" } else { "" };
    db.execute(&format!(
        "CREATE TABLE accounts (id BIGINT NOT NULL, legacy TEXT, email TEXT{nullability})"
    ))
    .unwrap();
    let first_email = if email_not_null {
        "'one@example.test'"
    } else {
        "NULL"
    };
    db.execute(&format!(
        "INSERT INTO accounts VALUES (1, 'old-one', {first_email})"
    ))
    .unwrap();
    db.execute("INSERT INTO accounts VALUES (2, 'old-two', 'two@example.test')")
        .unwrap();
    db.execute("INSERT INTO accounts VALUES (3, 'old-three', 'three@example.test')")
        .unwrap();
    db.execute("CREATE INDEX accounts_legacy_idx ON accounts(legacy)")
        .unwrap();
    db.execute("CREATE INDEX accounts_email_idx ON accounts(email)")
        .unwrap();
    (root, db)
}

#[test]
fn pg_simple_post_dml_adoption_supports_bounded_alter_and_exact_tags() {
    let (root, mut db) = layout_project("pg-round44-simple");
    let table = db.schema().table("accounts").unwrap().id;
    let email = db
        .schema()
        .table("accounts")
        .unwrap()
        .column("email")
        .unwrap()
        .id;
    let index = db
        .indexes(table)
        .unwrap()
        .iter()
        .find(|index| index.column_id == email)
        .unwrap()
        .id;
    let mut admin = session(&db, true);
    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        (
            "UPDATE accounts SET email = 'filled@example.test' WHERE email IS NULL",
            "UPDATE 1",
        ),
        ("ALTER TABLE accounts ADD COLUMN marker TEXT", "ALTER TABLE"),
        (
            "ALTER TABLE accounts RENAME COLUMN email TO contact",
            "ALTER TABLE",
        ),
        (
            "ALTER TABLE accounts RENAME TO customer_accounts",
            "ALTER TABLE",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
            ],
            "{source}"
        );
    }
    let users = db.schema().table("customer_accounts").unwrap();
    assert_eq!(users.id, table);
    assert_eq!(users.column("contact").unwrap().id, email);
    assert_eq!(db.indexes(table).unwrap()[1].id, index);
    assert_eq!(
        db.query("SELECT contact, marker FROM customer_accounts WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![
            netbadb_types::ScalarValue::Text("filled@example.test".into()),
            netbadb_types::ScalarValue::Null,
        ]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();

    let (root, mut db) = project("pg-round44-delete-drop");
    db.execute("INSERT INTO projects VALUES (2, 'two')")
        .unwrap();
    let mut admin = session(&db, true);
    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        ("DELETE FROM projects WHERE id = 2", "DELETE 1"),
        ("ALTER TABLE projects DROP COLUMN name", "ALTER TABLE"),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
            ],
            "{source}"
        );
    }
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("name")
            .is_none()
    );
    assert_eq!(
        db.query("SELECT id FROM projects ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![netbadb_types::ScalarValue::Int64(1)]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_post_dml_add_is_pure_until_execute() {
    let (root, mut db) = project("pg-round44-extended");
    let table = db.schema().table("projects").unwrap().id;
    let column_floor = db.next_column_id(table).unwrap();
    let storage_floor = db.next_storage_id();
    let before = files(&root);
    let mut admin = session(&db, true);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "post-dml-add".into(),
                query: "ALTER TABLE projects ADD COLUMN marker TEXT".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "post-dml-add".into(),
                statement: "post-dml-add".into(),
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
                target: DescribeTarget::Statement,
                name: "post-dml-add".into(),
            },
        ),
        [
            BackendMessage::ParameterDescription(vec![]),
            BackendMessage::NoData,
        ]
    );
    assert_eq!(db.next_column_id(table), Some(column_floor));
    assert_eq!(db.next_storage_id(), storage_floor);
    assert_eq!(files(&root), before);
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET name = name WHERE id = 1",
    ));
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "post-dml-add".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("ALTER TABLE".into())]
    );
    assert_eq!(db.next_column_id(table), Some(ColumnId(column_floor.0 + 1)));
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("marker")
            .is_some()
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_post_dml_adoption_error_boundaries_enter_failed_transaction() {
    let (root, mut db) = project("pg-round44-errors");
    db.execute("INSERT INTO projects VALUES (2, NULL)").unwrap();
    let mut admin = session(&db, true);
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET name = name WHERE id = 1",
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

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE projects SET name = name WHERE id = 1",
    ));
    ok(&sql(
        &mut admin,
        &mut db,
        "ALTER TABLE projects ADD COLUMN marker TEXT",
    ));
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE projects SET name = name WHERE id = 1",
        ),
        "25000",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM projects"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_simple_drop_first_layout_sql_publishes_one_projected_replacement() {
    let (root, mut db) = layout_project("pg-round42-simple");
    let table = db.schema().table("accounts").unwrap().id;
    let old_legacy = db
        .schema()
        .table("accounts")
        .unwrap()
        .column("legacy")
        .unwrap()
        .id;
    let version = db.table_schema_version(table).unwrap();
    let generation = db.schema_generation();
    let target_storage = db.next_storage_id().unwrap();
    let mut admin = session(&db, true);

    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        ("DROP INDEX accounts_legacy_idx", "DROP INDEX"),
        ("DROP INDEX accounts_email_idx", "DROP INDEX"),
        (
            "UPDATE accounts SET email = 'filled@example.test' WHERE email IS NULL",
            "UPDATE 1",
        ),
        (
            "INSERT INTO accounts VALUES (4, 'old-four', 'four@example.test')",
            "INSERT 0 1",
        ),
        ("DELETE FROM accounts WHERE id = 2", "DELETE 1"),
        ("ALTER TABLE accounts DROP COLUMN legacy", "ALTER TABLE"),
        ("ALTER TABLE accounts ADD COLUMN legacy TEXT", "ALTER TABLE"),
        (
            "ALTER TABLE accounts ALTER COLUMN email SET NOT NULL",
            "ALTER TABLE",
        ),
        (
            "ALTER TABLE accounts RENAME COLUMN email TO contact",
            "ALTER TABLE",
        ),
        (
            "CREATE INDEX accounts_contact_idx ON accounts(contact)",
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

    let accounts = db.schema().table("accounts").unwrap();
    let replacement = accounts.column("legacy").unwrap();
    assert_eq!(replacement.id, ColumnId(4));
    assert_ne!(replacement.id, old_legacy);
    assert!(!accounts.column("contact").unwrap().nullable);
    assert_eq!(db.table_schema_version(table).unwrap().0, version.0 + 1);
    assert_eq!(db.schema_generation().0, generation.0 + 1);
    assert_eq!(db.next_storage_id().unwrap().0, target_storage.0 + 1);
    assert_eq!(db.indexes(table).unwrap().len(), 1);
    assert_eq!(db.indexes(table).unwrap()[0].column_id, ColumnId(3));
    assert_eq!(
        db.query("SELECT id, contact, legacy FROM accounts ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                netbadb_types::ScalarValue::Int64(1),
                netbadb_types::ScalarValue::Text("filled@example.test".into()),
                netbadb_types::ScalarValue::Null,
            ],
            vec![
                netbadb_types::ScalarValue::Int64(3),
                netbadb_types::ScalarValue::Text("three@example.test".into()),
                netbadb_types::ScalarValue::Null,
            ],
            vec![
                netbadb_types::ScalarValue::Int64(4),
                netbadb_types::ScalarValue::Text("four@example.test".into()),
                netbadb_types::ScalarValue::Null,
            ],
        ]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_add_is_pure_until_execute_and_reserves_once() {
    let (root, mut db) = layout_project("pg-round42-extended-add");
    let table = db.schema().table("accounts").unwrap().id;
    let column_floor = db.next_column_id(table).unwrap();
    let storage_floor = db.next_storage_id();
    let before = files(&root);
    let mut admin = session(&db, true);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "add-marker".into(),
                query: "ALTER TABLE accounts ADD COLUMN marker TEXT".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "add-marker".into(),
                statement: "add-marker".into(),
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
                target: DescribeTarget::Statement,
                name: "add-marker".into(),
            },
        ),
        [
            BackendMessage::ParameterDescription(vec![]),
            BackendMessage::NoData
        ]
    );
    assert_eq!(files(&root), before);
    assert_eq!(db.next_column_id(table), Some(column_floor));
    assert_eq!(db.next_storage_id(), storage_floor);
    for source in [
        "BEGIN",
        "DROP INDEX accounts_legacy_idx",
        "DROP INDEX accounts_email_idx",
        "UPDATE accounts SET email = email WHERE id = 1",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "add-marker".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("ALTER TABLE".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    assert_eq!(db.next_column_id(table), Some(ColumnId(column_floor.0 + 1)));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    let next_column = db.next_column_id(table);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "add-again".into(),
                statement: "add-marker".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    state(
        &admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "add-again".into(),
                max_rows: 0,
            },
        ),
        "25000",
    );
    assert_eq!(db.next_column_id(table), next_column);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_extended_drop_keeps_exact_column_identity_after_same_name_add() {
    let (root, mut db) = layout_project("pg-round42-extended-drop");
    let table = db.schema().table("accounts").unwrap().id;
    let column_floor = db.next_column_id(table).unwrap();
    let before = files(&root);
    let mut admin = session(&db, true);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "drop-legacy".into(),
                query: "ALTER TABLE accounts DROP COLUMN legacy".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "drop-legacy".into(),
                statement: "drop-legacy".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(files(&root), before);
    for source in [
        "BEGIN",
        "DROP INDEX accounts_legacy_idx",
        "DROP INDEX accounts_email_idx",
        "UPDATE accounts SET email = email WHERE id = 1",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "drop-legacy".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("ALTER TABLE".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    ok(&sql(
        &mut admin,
        &mut db,
        "ALTER TABLE accounts ADD COLUMN legacy TEXT",
    ));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert_eq!(
        db.schema()
            .table("accounts")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        column_floor
    );
    let next_column = db.next_column_id(table);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "drop-again".into(),
                statement: "drop-legacy".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    state(
        &admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "drop-again".into(),
                max_rows: 0,
            },
        ),
        "25000",
    );
    assert_eq!(db.next_column_id(table), next_column);
    assert_eq!(
        db.schema()
            .table("accounts")
            .unwrap()
            .column("legacy")
            .unwrap()
            .id,
        column_floor
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_post_dml_layout_rejections_enter_failed_transaction_state() {
    let (root, mut db) = layout_project("post-dml-not-null");
    let mut admin = session(&db, true);
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(
        &mut admin,
        &mut db,
        "UPDATE accounts SET email = email WHERE id = 1",
    ));
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts ALTER COLUMN email SET NOT NULL",
        ),
        "23502",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();

    let (root, mut db) = layout_project("new-column-not-null");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "DROP INDEX accounts_legacy_idx",
        "DROP INDEX accounts_email_idx",
        "UPDATE accounts SET email = email WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts ALTER COLUMN marker SET NOT NULL",
        ),
        "0A000",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();

    let (root, mut db) = layout_project("indexed-drop");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "DROP INDEX accounts_email_idx",
        "UPDATE accounts SET email = email WHERE id = 1",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts DROP COLUMN legacy",
        ),
        "2BP01",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();

    let (root, mut db) = layout_project("post-refinement-dml");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "DROP INDEX accounts_legacy_idx",
        "DROP INDEX accounts_email_idx",
        "UPDATE accounts SET email = email WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "INSERT INTO accounts VALUES (4, 'old-four', 'four@example.test', NULL)",
        ),
        "25000",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round46_surviving_nullability_is_supported_and_round45_boundaries_remain() {
    for (name, base_not_null, setup, statement) in [
        (
            "set-not-null",
            false,
            "UPDATE accounts SET email = 'filled' WHERE email IS NULL",
            "ALTER TABLE accounts ALTER COLUMN email SET NOT NULL",
        ),
        (
            "drop-not-null",
            true,
            "UPDATE accounts SET email = email WHERE id = 1",
            "ALTER TABLE accounts ALTER COLUMN email DROP NOT NULL",
        ),
    ] {
        let (root, mut db) =
            layout_project_with_email_nullability(&format!("round46-{name}"), base_not_null);
        let table = db.schema().table("accounts").unwrap().id;
        let old_index = db.indexes(table).unwrap()[1].clone();
        let mut admin = session(&db, true);
        for (source, tag) in [
            ("BEGIN", "BEGIN"),
            (setup, "UPDATE 1"),
            (statement, "ALTER TABLE"),
            ("COMMIT", "COMMIT"),
        ] {
            assert_eq!(
                sql(&mut admin, &mut db, source),
                [
                    BackendMessage::CommandComplete(tag.into()),
                    BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
                ],
                "{source}"
            );
        }
        let final_column = db
            .schema()
            .table("accounts")
            .unwrap()
            .column("email")
            .unwrap();
        assert_eq!(final_column.nullable, base_not_null);
        let final_index = &db.indexes(table).unwrap()[1];
        assert_eq!(
            (final_index.id, final_index.column_id, &final_index.name),
            (old_index.id, old_index.column_id, &old_index.name)
        );
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    for (name, statement) in [
        (
            "terminal-alter",
            "ALTER TABLE accounts RENAME COLUMN email TO contact",
        ),
        ("select", "SELECT id, marker FROM accounts"),
        (
            "insert",
            "INSERT INTO accounts VALUES (4, 'old-four', 'four@example.test', NULL)",
        ),
        ("update", "UPDATE accounts SET email = email WHERE id = 1"),
        ("delete", "DELETE FROM accounts WHERE id = 1"),
    ] {
        let (root, mut db) = layout_project(&format!("round45-{name}"));
        let mut admin = session(&db, true);
        ok(&sql(&mut admin, &mut db, "BEGIN"));
        ok(&sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET email = email WHERE id = 1",
        ));
        ok(&sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts ADD COLUMN marker TEXT",
        ));
        ok(&sql(
            &mut admin,
            &mut db,
            "CREATE INDEX accounts_marker_idx ON accounts(marker)",
        ));
        state(&sql(&mut admin, &mut db, statement), "25000");
        state(
            &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
            "25P02",
        );
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round46_extended_set_is_pure_until_execute_and_uses_transaction_view() {
    let (root, mut db) = layout_project("round46-extended-set");
    let table = db.schema().table("accounts").unwrap().id;
    let version = db.table_schema_version(table);
    let generation = db.schema_generation();
    let storage_floor = db.next_storage_id();
    let before = files(&root);
    let mut admin = session(&db, true);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "round46-set".into(),
                query: "ALTER TABLE accounts ALTER COLUMN email SET NOT NULL".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "round46-set".into(),
                statement: "round46-set".into(),
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
                target: DescribeTarget::Statement,
                name: "round46-set".into(),
            },
        ),
        [
            BackendMessage::ParameterDescription(vec![]),
            BackendMessage::NoData,
        ]
    );
    assert_eq!(files(&root), before);
    assert_eq!(db.table_schema_version(table), version);
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.next_storage_id(), storage_floor);

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    assert_eq!(
        sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET email = 'filled' WHERE email IS NULL"
        ),
        [
            BackendMessage::CommandComplete("UPDATE 1".into()),
            BackendMessage::ReadyForQuery(b'T'),
        ]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "round46-set".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("ALTER TABLE".into())]
    );
    assert_eq!(db.table_schema_version(table), version);
    assert_eq!(db.schema_generation(), generation);
    assert_eq!(db.next_storage_id(), storage_floor);
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert!(
        !db.schema()
            .table("accounts")
            .unwrap()
            .column("email")
            .unwrap()
            .nullable
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round46_sqlstate_boundaries_for_new_columns_and_closed_dml_are_stable() {
    for (name, operation, expected) in [
        ("cnew-set", "SET NOT NULL", "25000"),
        ("cnew-drop", "DROP NOT NULL", "58000"),
    ] {
        let (root, mut db) = layout_project(&format!("round46-{name}"));
        let mut admin = session(&db, true);
        for statement in [
            "BEGIN",
            "UPDATE accounts SET legacy = legacy WHERE id = 1",
            "ALTER TABLE accounts ADD COLUMN marker TEXT",
        ] {
            ok(&sql(&mut admin, &mut db, statement));
        }
        state(
            &sql(
                &mut admin,
                &mut db,
                &format!("ALTER TABLE accounts ALTER COLUMN marker {operation}"),
            ),
            expected,
        );
        state(
            &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
            "25P02",
        );
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    let (root, mut db) = layout_project("round46-dml-closed");
    let mut admin = session(&db, true);
    for statement in [
        "BEGIN",
        "UPDATE accounts SET email = 'filled' WHERE email IS NULL",
        "ALTER TABLE accounts ALTER COLUMN email SET NOT NULL",
    ] {
        ok(&sql(&mut admin, &mut db, statement));
    }
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
        "25000",
    );
    state(
        &sql(&mut admin, &mut db, "DELETE FROM accounts WHERE id = 1"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
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

#[test]
fn pg_round60_production_casts_and_sqlstates_keep_alter_type_closed() {
    let (root, mut db) = project("pg-round60-production-cast");
    let mut admin = session(&db, true);

    for supported in [
        "SELECT 42::BIGINT",
        "SELECT '42'::BIGINT",
        "SELECT 42::TEXT",
        "SELECT true::TEXT",
        "SELECT 'false'::BOOL",
    ] {
        ok(&sql(&mut admin, &mut db, supported));
    }
    state(&sql(&mut admin, &mut db, "SELECT 'bad'::BIGINT"), "22P02");
    state(&sql(&mut admin, &mut db, "SELECT 256::UINT8"), "22003");
    state(&sql(&mut admin, &mut db, "SELECT true::BIGINT"), "42846");
    state(
        &sql(&mut admin, &mut db, "SELECT '18446744073709551615'::UINT64"),
        "0A000",
    );

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    state(
        &sql(
            &mut admin,
            &mut db,
            "SELECT name::BIGINT FROM projects WHERE name IS NOT NULL",
        ),
        "22P02",
    );
    state(&sql(&mut admin, &mut db, "SELECT * FROM projects"), "25P02");
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));

    ok(&sql(&mut admin, &mut db, "BEGIN"));
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT",
        ),
        "0A000",
    );
    state(&sql(&mut admin, &mut db, "SELECT * FROM projects"), "25P02");
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));

    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round60_extended_declared_text_parameter_casts_to_bigint() {
    let (root, mut db) = project("pg-round60-extended-cast");
    let mut admin = session(&db, true);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "cast-text".into(),
                query: "SELECT $1::BIGINT".into(),
                parameter_types: vec![PostgresType::Text.oid()],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    let description = admin.handle(
        &mut db,
        FrontendMessage::Describe {
            target: DescribeTarget::Statement,
            name: "cast-text".into(),
        },
    );
    assert_eq!(
        description[0],
        BackendMessage::ParameterDescription(vec![PostgresType::Text.oid()])
    );
    assert!(matches!(
        &description[1],
        BackendMessage::RowDescription(fields)
            if fields.len() == 1 && fields[0].data_type == PostgresType::Int8
    ));
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "cast-text".into(),
                statement: "cast-text".into(),
                parameter_formats: vec![],
                parameters: vec![Some(b"42".to_vec())],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    let executed = admin.handle(
        &mut db,
        FrontendMessage::Execute {
            portal: "cast-text".into(),
            max_rows: 0,
        },
    );
    ok(&executed);
    assert!(executed.contains(&BackendMessage::DataRow(vec![Some(b"42".to_vec())])));
    ok(&admin.handle(&mut db, FrontendMessage::Sync));

    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "cast-bad".into(),
                statement: "cast-text".into(),
                parameter_formats: vec![],
                parameters: vec![Some(b"bad".to_vec())],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    state(
        &admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "cast-bad".into(),
                max_rows: 0,
            },
        ),
        "22P02",
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round48_extended_final_create_is_pure_until_execute_and_terminal() {
    for (case, alters, create) in [
        (
            "cnew",
            vec!["ALTER TABLE projects ADD COLUMN marker TEXT"],
            "CREATE INDEX marker_idx ON projects(marker)",
        ),
        (
            "index-only",
            vec![
                "ALTER TABLE projects RENAME COLUMN name TO tmp",
                "ALTER TABLE projects RENAME COLUMN tmp TO name",
            ],
            "CREATE INDEX name_idx ON projects(name)",
        ),
    ] {
        let (root, mut db) = project(&format!("round48-extended-{case}"));
        let base_version = db.table_schema_version(TableId(2));
        let floor = db.next_storage_id();
        let mut admin = session(&db, true);
        ok(&sql(&mut admin, &mut db, "BEGIN"));
        ok(&sql(
            &mut admin,
            &mut db,
            "UPDATE projects SET name = 'updated'",
        ));
        for alter in alters {
            ok(&sql(&mut admin, &mut db, alter));
        }
        let before = files(&root);
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Parse {
                    statement: "create".into(),
                    query: create.into(),
                    parameter_types: vec![],
                }
            ),
            [BackendMessage::ParseComplete]
        );
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Bind {
                    portal: "create".into(),
                    statement: "create".into(),
                    parameter_formats: vec![],
                    parameters: vec![],
                    result_formats: vec![],
                }
            ),
            [BackendMessage::BindComplete]
        );
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Describe {
                    target: DescribeTarget::Statement,
                    name: "create".into(),
                }
            ),
            [
                BackendMessage::ParameterDescription(vec![]),
                BackendMessage::NoData
            ]
        );
        assert_eq!(files(&root), before);
        assert!(db.indexes(TableId(2)).unwrap().is_empty());
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Execute {
                    portal: "create".into(),
                    max_rows: 0,
                }
            ),
            [BackendMessage::CommandComplete("CREATE INDEX".into())]
        );
        ok(&admin.handle(&mut db, FrontendMessage::Sync));
        assert!(db.indexes(TableId(2)).unwrap().is_empty());
        ok(&sql(&mut admin, &mut db, "COMMIT"));
        assert_eq!(db.indexes(TableId(2)).unwrap().len(), 1);
        if case == "index-only" {
            assert_eq!(db.table_schema_version(TableId(2)), base_version);
            assert_eq!(db.next_storage_id(), floor);
        }
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round48_extended_stale_create_and_exact_old_drop_never_rebind() {
    for is_drop in [false, true] {
        let (root, mut db) = project(&format!("round48-prepared-{is_drop}"));
        if is_drop {
            db.execute("CREATE INDEX name_idx ON projects(name)")
                .unwrap();
        }
        let mut admin = session(&db, true);
        ok(&sql(&mut admin, &mut db, "BEGIN"));
        ok(&sql(
            &mut admin,
            &mut db,
            "UPDATE projects SET name = 'updated'",
        ));
        ok(&sql(
            &mut admin,
            &mut db,
            "ALTER TABLE projects RENAME COLUMN name TO tmp",
        ));
        let query = if is_drop {
            "DROP INDEX name_idx"
        } else {
            "CREATE INDEX IF NOT EXISTS name_idx ON projects(tmp)"
        };
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Parse {
                    statement: "old".into(),
                    query: query.into(),
                    parameter_types: vec![],
                }
            ),
            [BackendMessage::ParseComplete]
        );
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Bind {
                    portal: "old".into(),
                    statement: "old".into(),
                    parameter_formats: vec![],
                    parameters: vec![],
                    result_formats: vec![],
                }
            ),
            [BackendMessage::BindComplete]
        );
        if is_drop {
            ok(&sql(&mut admin, &mut db, "DROP INDEX name_idx"));
            ok(&sql(
                &mut admin,
                &mut db,
                "CREATE INDEX name_idx ON projects(tmp)",
            ));
        } else {
            ok(&sql(
                &mut admin,
                &mut db,
                "ALTER TABLE projects RENAME COLUMN tmp TO name",
            ));
        }
        let before = files(&root);
        state(
            &admin.handle(
                &mut db,
                FrontendMessage::Execute {
                    portal: "old".into(),
                    max_rows: 0,
                },
            ),
            if is_drop { "42704" } else { "25000" },
        );
        assert_eq!(files(&root), before);
        ok(&admin.handle(&mut db, FrontendMessage::Sync));
        state(
            &sql(&mut admin, &mut db, "SELECT id FROM projects"),
            "25P02",
        );
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round48_terminal_alter_and_relational_errors_fail_the_transaction() {
    for statement in [
        "ALTER TABLE projects ADD COLUMN extra TEXT",
        "ALTER TABLE projects DROP COLUMN name",
        "ALTER TABLE projects RENAME TO work",
        "ALTER TABLE projects RENAME COLUMN name TO contact",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
        "ALTER TABLE projects ALTER COLUMN name DROP NOT NULL",
        "SELECT id FROM projects",
        "INSERT INTO projects VALUES (2, 'two')",
        "UPDATE projects SET name = name",
        "DELETE FROM projects WHERE id = 1",
    ] {
        let (root, mut db) = project("round48-terminal");
        let mut admin = session(&db, true);
        for source in [
            "BEGIN",
            "UPDATE projects SET name = 'updated'",
            "ALTER TABLE projects RENAME COLUMN name TO tmp",
            "ALTER TABLE projects RENAME COLUMN tmp TO name",
            "CREATE INDEX name_idx ON projects(name)",
        ] {
            ok(&sql(&mut admin, &mut db, source));
        }
        state(&sql(&mut admin, &mut db, statement), "25000");
        state(
            &sql(&mut admin, &mut db, "SELECT id FROM projects"),
            "25P02",
        );
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round50_simple_deferred_update_not_null_index_and_commit() {
    let (root, mut db) = layout_project("round50-simple");
    let mut admin = session(&db, true);
    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        (
            "UPDATE accounts SET legacy = 'updated' WHERE id = 1",
            "UPDATE 1",
        ),
        ("ALTER TABLE accounts ADD COLUMN marker TEXT", "ALTER TABLE"),
        (
            "UPDATE accounts SET marker = legacy WHERE legacy IS NOT NULL",
            "UPDATE 3",
        ),
        (
            "ALTER TABLE accounts ALTER COLUMN marker SET NOT NULL",
            "ALTER TABLE",
        ),
        (
            "CREATE INDEX accounts_marker_idx ON accounts(marker)",
            "CREATE INDEX",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
            ],
            "{source}"
        );
    }
    assert_eq!(
        db.query("SELECT marker FROM accounts ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![ScalarValue::Text("updated".into())],
            vec![ScalarValue::Text("old-two".into())],
            vec![ScalarValue::Text("old-three".into())],
        ]
    );
    assert_eq!(
        db.indexes(TableId(2))
            .unwrap()
            .iter()
            .filter(|index| {
                index
                    .name
                    .as_ref()
                    .is_some_and(|name| name.as_str() == "accounts_marker_idx")
            })
            .count(),
        1
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round50_projected_not_null_failure_aborts_explicit_transaction() {
    let (root, mut db) = layout_project("round50-not-null-failure");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
        "UPDATE accounts SET marker = legacy WHERE id = 1",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts ALTER COLUMN marker SET NOT NULL",
        ),
        "23502",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    assert!(
        db.schema()
            .table("accounts")
            .unwrap()
            .column("marker")
            .is_none()
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round50_extended_bound_update_is_pure_until_execute() {
    let (root, mut db) = layout_project("round50-extended");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    let before = files(&root);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "backfill".into(),
                query: "UPDATE accounts SET marker = $1 WHERE id = $2".into(),
                parameter_types: vec![PostgresOid(25), PostgresOid(20)],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "backfill".into(),
                statement: "backfill".into(),
                parameter_formats: vec![],
                parameters: vec![Some(b"bound".to_vec()), Some(b"2".to_vec())],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "backfill".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("UPDATE 1".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert_eq!(
        db.query("SELECT marker FROM accounts WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Text("bound".into())]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round54_simple_virtual_row_repair_copy_index_and_reopen() {
    let (root, mut db) = layout_project("round54-simple");
    let mut admin = session(&db, true);
    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        (
            "UPDATE accounts SET legacy = 'updated1' WHERE id = 1",
            "UPDATE 1",
        ),
        (
            "INSERT INTO accounts VALUES (4, 'inserted4', NULL)",
            "INSERT 0 1",
        ),
        ("DELETE FROM accounts WHERE id = 2", "DELETE 1"),
        ("UPDATE accounts SET legacy = NULL WHERE id = 3", "UPDATE 1"),
        ("ALTER TABLE accounts ADD COLUMN marker TEXT", "ALTER TABLE"),
        (
            "ALTER TABLE accounts ADD COLUMN normalized TEXT",
            "ALTER TABLE",
        ),
        (
            "UPDATE accounts SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL",
            "UPDATE 2",
        ),
        (
            "UPDATE accounts SET marker = 'missing' WHERE marker IS NULL",
            "UPDATE 1",
        ),
        (
            "UPDATE accounts SET normalized = marker WHERE marker IS NOT NULL",
            "UPDATE 3",
        ),
        (
            "ALTER TABLE accounts ALTER COLUMN marker SET NOT NULL",
            "ALTER TABLE",
        ),
        (
            "ALTER TABLE accounts ALTER COLUMN normalized SET NOT NULL",
            "ALTER TABLE",
        ),
        (
            "CREATE INDEX accounts_normalized_idx ON accounts(normalized)",
            "CREATE INDEX",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
            ],
            "{source}"
        );
    }
    for _ in 0..3 {
        assert_eq!(
            db.query("SELECT id, marker, normalized FROM accounts ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Text("updated1".into()),
                    ScalarValue::Text("updated1".into()),
                ],
                vec![
                    ScalarValue::Int64(3),
                    ScalarValue::Text("missing".into()),
                    ScalarValue::Text("missing".into()),
                ],
                vec![
                    ScalarValue::Int64(4),
                    ScalarValue::Text("inserted4".into()),
                    ScalarValue::Text("inserted4".into()),
                ],
            ]
        );
        db.close().unwrap();
        db = Database::open_catalog(root.join("catalog")).unwrap();
    }
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round54_extended_prepared_consumer_reads_current_prefix_and_bound_values() {
    let (root, mut db) = layout_project("round54-extended");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
        "ALTER TABLE accounts ADD COLUMN normalized TEXT",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "consumer".into(),
                query: "UPDATE accounts SET normalized = marker WHERE marker IS NOT NULL".into(),
                parameter_types: vec![],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL"
        ),
        [
            BackendMessage::CommandComplete("UPDATE 3".into()),
            BackendMessage::ReadyForQuery(b'T'),
        ]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "consumer".into(),
                statement: "consumer".into(),
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
            FrontendMessage::Execute {
                portal: "consumer".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("UPDATE 3".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));

    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "repair".into(),
                query: "UPDATE accounts SET marker = $1 WHERE marker IS NULL AND legacy = $2"
                    .into(),
                parameter_types: vec![PostgresOid(25), PostgresOid(25)],
            },
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "repair".into(),
                statement: "repair".into(),
                parameter_formats: vec![],
                parameters: vec![Some(b"bound".to_vec()), Some(b"never".to_vec())],
                result_formats: vec![],
            },
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(
        admin.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "repair".into(),
                max_rows: 0,
            },
        ),
        [BackendMessage::CommandComplete("UPDATE 0".into())]
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert_eq!(
        db.query("SELECT normalized FROM accounts WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![ScalarValue::Text("old-one".into())]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round54_true_unsupported_access_still_aborts_the_transaction() {
    for statement in [
        "UPDATE accounts SET legacy = marker",
        "UPDATE accounts SET legacy = marker, normalized = marker",
        "SELECT marker FROM accounts",
        "INSERT INTO accounts (id, marker) VALUES (9, 'x')",
        "DELETE FROM accounts WHERE marker IS NULL",
    ] {
        let (root, mut db) = layout_project("round54-unsupported");
        let mut admin = session(&db, true);
        for source in [
            "BEGIN",
            "UPDATE accounts SET legacy = legacy WHERE id = 1",
            "ALTER TABLE accounts ADD COLUMN marker TEXT",
            "ALTER TABLE accounts ADD COLUMN normalized TEXT",
        ] {
            ok(&sql(&mut admin, &mut db, source));
        }
        state(&sql(&mut admin, &mut db, statement), "25000");
        state(
            &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
            "25P02",
        );
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round54_terminal_index_phase_closes_late_read_updates() {
    let (root, mut db) = layout_project("round54-terminal");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
        "ALTER TABLE accounts ADD COLUMN normalized TEXT",
        "UPDATE accounts SET marker = legacy WHERE marker IS NULL",
        "CREATE INDEX accounts_marker_round54_idx ON accounts(marker)",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET normalized = marker WHERE marker IS NOT NULL",
        ),
        "25000",
    );
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
        "25P02",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round56_simple_atomic_shadow_swap_uses_exact_tags_and_reopens() {
    let (root, mut db) = terminal_layout_project("round56-shadow-swap");
    let table_id = db.schema().table("accounts").unwrap().id;
    let mut admin = session(&db, true);
    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        (
            "UPDATE accounts SET legacy = 'updated1' WHERE id = 1",
            "UPDATE 1",
        ),
        (
            "INSERT INTO accounts VALUES (4, 'inserted4', true)",
            "INSERT 0 1",
        ),
        ("DELETE FROM accounts WHERE id = 2", "DELETE 1"),
        ("ALTER TABLE accounts ADD COLUMN shadow TEXT", "ALTER TABLE"),
        (
            "UPDATE accounts SET shadow = legacy WHERE legacy IS NOT NULL",
            "UPDATE 2",
        ),
        (
            "UPDATE accounts SET shadow = 'missing' WHERE shadow IS NULL",
            "UPDATE 1",
        ),
        (
            "ALTER TABLE accounts ALTER COLUMN shadow SET NOT NULL",
            "ALTER TABLE",
        ),
        ("ALTER TABLE accounts DROP COLUMN legacy", "ALTER TABLE"),
        (
            "ALTER TABLE accounts RENAME COLUMN shadow TO legacy",
            "ALTER TABLE",
        ),
        (
            "CREATE INDEX accounts_legacy_idx ON accounts(legacy)",
            "CREATE INDEX",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
            ],
            "{source}"
        );
    }
    for _ in 0..3 {
        let table = db.schema().table("accounts").unwrap();
        assert_eq!(table.column("legacy").unwrap().id, ColumnId(4));
        assert!(table.column_by_id(ColumnId(2)).is_none());
        assert_eq!(table.columns.len(), 3);
        assert_eq!(db.indexes(table_id).unwrap()[0].column_id, ColumnId(4));
        assert_eq!(
            db.query("SELECT id, legacy FROM accounts ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
                vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
                vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
            ]
        );
        db.close().unwrap();
        db = Database::open_catalog(root.join("catalog")).unwrap();
    }
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round58_simple_indexed_shadow_swap_reuses_name_and_reopens() {
    let (root, mut db) = indexed_terminal_layout_project("round58-indexed-swap");
    let table_id = db.schema().table("accounts").unwrap().id;
    let old_index = db.indexes(table_id).unwrap()[0].id;
    let mut admin = session(&db, true);
    for (source, tag) in [
        ("BEGIN", "BEGIN"),
        (
            "UPDATE accounts SET legacy = 'updated1' WHERE id = 1",
            "UPDATE 1",
        ),
        (
            "INSERT INTO accounts VALUES (4, 'inserted4', true)",
            "INSERT 0 1",
        ),
        ("DELETE FROM accounts WHERE id = 2", "DELETE 1"),
        ("ALTER TABLE accounts ADD COLUMN shadow TEXT", "ALTER TABLE"),
        (
            "UPDATE accounts SET shadow = legacy WHERE legacy IS NOT NULL",
            "UPDATE 2",
        ),
        (
            "UPDATE accounts SET shadow = 'missing' WHERE shadow IS NULL",
            "UPDATE 1",
        ),
        (
            "ALTER TABLE accounts ALTER COLUMN shadow SET NOT NULL",
            "ALTER TABLE",
        ),
        ("DROP INDEX accounts_legacy_idx", "DROP INDEX"),
        ("ALTER TABLE accounts DROP COLUMN legacy", "ALTER TABLE"),
        (
            "ALTER TABLE accounts RENAME COLUMN shadow TO legacy",
            "ALTER TABLE",
        ),
        (
            "CREATE INDEX accounts_legacy_idx ON accounts(legacy)",
            "CREATE INDEX",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        assert_eq!(
            sql(&mut admin, &mut db, source),
            [
                BackendMessage::CommandComplete(tag.into()),
                BackendMessage::ReadyForQuery(if source == "COMMIT" { b'I' } else { b'T' }),
            ],
            "{source}"
        );
    }

    for _ in 0..3 {
        let table = db.schema().table("accounts").unwrap();
        assert_eq!(table.column("legacy").unwrap().id, ColumnId(4));
        assert!(table.column_by_id(ColumnId(2)).is_none());
        let indexes = db.indexes(table_id).unwrap();
        assert_eq!(indexes.len(), 1);
        assert_ne!(indexes[0].id, old_index);
        assert_eq!(indexes[0].column_id, ColumnId(4));
        assert_eq!(
            db.query("SELECT id, legacy FROM accounts ORDER BY id")
                .unwrap()
                .rows,
            vec![
                vec![ScalarValue::Int64(1), ScalarValue::Text("updated1".into())],
                vec![ScalarValue::Int64(3), ScalarValue::Text("missing".into())],
                vec![ScalarValue::Int64(4), ScalarValue::Text("inserted4".into())],
            ]
        );
        db.close().unwrap();
        db = Database::open_catalog(root.join("catalog")).unwrap();
    }
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round58_unchanged_drop_does_not_seal_but_effective_drop_does() {
    let (root, mut db) = indexed_terminal_layout_project("round58-drop-phases");
    let mut admin = session(&db, true);
    prepare_indexed_terminal_swap(&mut admin, &mut db);
    assert_eq!(
        sql(
            &mut admin,
            &mut db,
            "DROP INDEX IF EXISTS index_that_is_not_here"
        ),
        [
            BackendMessage::CommandComplete("DROP INDEX".into()),
            BackendMessage::ReadyForQuery(b'T'),
        ]
    );
    assert_eq!(
        sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET shadow = shadow WHERE id = 999"
        ),
        [
            BackendMessage::CommandComplete("UPDATE 0".into()),
            BackendMessage::ReadyForQuery(b'T'),
        ]
    );
    ok(&sql(&mut admin, &mut db, "DROP INDEX accounts_legacy_idx"));
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET shadow = shadow WHERE id = 999",
        ),
        "25000",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();

    for (name, statement) in [
        (
            "round58-nullability-closed",
            "ALTER TABLE accounts ALTER COLUMN shadow DROP NOT NULL",
        ),
        (
            "round58-add-closed",
            "ALTER TABLE accounts ADD COLUMN later TEXT",
        ),
    ] {
        let (root, mut db) = indexed_terminal_layout_project(name);
        let mut admin = session(&db, true);
        prepare_indexed_terminal_swap(&mut admin, &mut db);
        ok(&sql(&mut admin, &mut db, "DROP INDEX accounts_legacy_idx"));
        state(&sql(&mut admin, &mut db, statement), "25000");
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round58_rollback_and_active_stream_keep_the_old_indexed_source() {
    for enabled in [false, true] {
        let (root, mut db) = indexed_terminal_layout_project(if enabled {
            "round58-stream-block"
        } else {
            "round58-rollback"
        });
        let table_id = db.schema().table("accounts").unwrap().id;
        let old_index = db.indexes(table_id).unwrap()[0].clone();
        if enabled {
            db.enable_change_stream(table_id).unwrap();
        }
        let storage_floor = db.next_storage_id();
        let mut admin = session(&db, true);
        prepare_indexed_terminal_swap(&mut admin, &mut db);
        for statement in [
            "DROP INDEX accounts_legacy_idx",
            "ALTER TABLE accounts DROP COLUMN legacy",
            "ALTER TABLE accounts RENAME COLUMN shadow TO legacy",
            "CREATE INDEX accounts_legacy_idx ON accounts(legacy)",
        ] {
            ok(&sql(&mut admin, &mut db, statement));
        }
        if enabled {
            state(&sql(&mut admin, &mut db, "COMMIT"), "0A000");
            state(
                &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
                "25000",
            );
        }
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        assert_eq!(db.next_storage_id(), storage_floor);
        let table = db.schema().table("accounts").unwrap();
        assert_eq!(table.column("legacy").unwrap().id, ColumnId(2));
        assert!(table.column_by_id(ColumnId(4)).is_none());
        assert_eq!(db.indexes(table_id).unwrap(), &[old_index]);
        db.close().unwrap();
        db = Database::open_catalog(root.join("catalog")).unwrap();
        assert_eq!(
            db.schema()
                .table("accounts")
                .unwrap()
                .column("legacy")
                .unwrap()
                .id,
            ColumnId(2)
        );
        assert_eq!(db.indexes(table_id).unwrap().len(), 1);
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round56_prepared_update_and_ddl_stale_before_final_phase_guard() {
    for (name, query, statement_name, portal_name) in [
        (
            "round56-stale-update",
            "UPDATE accounts SET shadow = 'late' WHERE id = 1",
            "old-update",
            "old-update-portal",
        ),
        (
            "round56-stale-rename",
            "ALTER TABLE accounts RENAME COLUMN shadow TO replacement",
            "old-rename",
            "old-rename-portal",
        ),
    ] {
        let (root, mut db) = terminal_layout_project(name);
        let mut admin = session(&db, true);
        for source in [
            "BEGIN",
            "UPDATE accounts SET legacy = legacy WHERE id = 1",
            "ALTER TABLE accounts ADD COLUMN shadow TEXT",
            "UPDATE accounts SET shadow = legacy WHERE legacy IS NOT NULL",
        ] {
            ok(&sql(&mut admin, &mut db, source));
        }
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Parse {
                    statement: statement_name.into(),
                    query: query.into(),
                    parameter_types: vec![],
                },
            ),
            [BackendMessage::ParseComplete]
        );
        assert_eq!(
            admin.handle(
                &mut db,
                FrontendMessage::Bind {
                    portal: portal_name.into(),
                    statement: statement_name.into(),
                    parameter_formats: vec![],
                    parameters: vec![],
                    result_formats: vec![],
                },
            ),
            [BackendMessage::BindComplete]
        );
        ok(&admin.handle(&mut db, FrontendMessage::Sync));
        ok(&sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts DROP COLUMN legacy",
        ));
        state(
            &admin.handle(
                &mut db,
                FrontendMessage::Execute {
                    portal: portal_name.into(),
                    max_rows: 0,
                },
            ),
            "25000",
        );
        assert_eq!(
            admin.handle(&mut db, FrontendMessage::Sync),
            [BackendMessage::ReadyForQuery(b'E')]
        );
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
        db.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn pg_round56_final_phase_and_indexed_drop_keep_existing_sqlstates() {
    let (root, mut db) = terminal_layout_project("round56-final-guard");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN shadow TEXT",
        "UPDATE accounts SET shadow = legacy WHERE legacy IS NOT NULL",
        "ALTER TABLE accounts DROP COLUMN legacy",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET shadow = 'late' WHERE id = 1",
        ),
        "25000",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();

    let (root, mut db) = layout_project("round56-indexed-drop");
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN shadow TEXT",
        "UPDATE accounts SET shadow = legacy WHERE legacy IS NOT NULL",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    state(
        &sql(
            &mut admin,
            &mut db,
            "ALTER TABLE accounts DROP COLUMN legacy",
        ),
        "2BP01",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_round54_enabled_stream_accepts_updates_but_blocks_commit() {
    let (root, mut db) = layout_project("round54-enabled-stream");
    let table = db.schema().table("accounts").unwrap().id;
    let frontier = db.enable_change_stream(table).unwrap().frontier;
    let mut admin = session(&db, true);
    for source in [
        "BEGIN",
        "UPDATE accounts SET legacy = legacy WHERE id = 1",
        "ALTER TABLE accounts ADD COLUMN marker TEXT",
        "ALTER TABLE accounts ADD COLUMN normalized TEXT",
    ] {
        ok(&sql(&mut admin, &mut db, source));
    }
    assert_eq!(
        sql(
            &mut admin,
            &mut db,
            "UPDATE accounts SET marker = legacy WHERE marker IS NULL AND legacy IS NOT NULL"
        ),
        [
            BackendMessage::CommandComplete("UPDATE 3".into()),
            BackendMessage::ReadyForQuery(b'T'),
        ]
    );
    state(&sql(&mut admin, &mut db, "COMMIT"), "0A000");
    state(
        &sql(&mut admin, &mut db, "SELECT id FROM accounts"),
        "25000",
    );
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    let stream = db.inspect_change_stream(table).unwrap();
    assert_eq!(stream.current_data_version, frontier);
    assert_eq!(stream.committed_batch_count, 0);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
