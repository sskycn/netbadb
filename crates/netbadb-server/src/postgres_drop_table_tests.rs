use super::*;
use crate::authorization::{AuthorizationPolicy, PrincipalGrants};
use crate::sql_create_table_test_support::{files, principal, seed};
use netbadb_types::TableId;

fn session(db: &Database, authorization: PrincipalAuthorization) -> PgWorkerSession {
    PgWorkerSession::new(
        db,
        SessionPolicy::default(),
        authorization,
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

#[test]
fn pg_extended_drop_parse_bind_describe_are_pure_and_execute_has_exact_tag() {
    let (root, mut db) = seed("pg-drop-extended");
    let mut session = session(&db, principal(true));
    let before = files(&root);
    assert_eq!(
        session.handle(
            &mut db,
            FrontendMessage::Parse {
                statement: "drop".into(),
                query: "DROP TABLE users".into(),
                parameter_types: vec![],
            }
        ),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        session.handle(
            &mut db,
            FrontendMessage::Bind {
                portal: "drop".into(),
                statement: "drop".into(),
                parameter_formats: vec![],
                parameters: vec![],
                result_formats: vec![],
            }
        ),
        [BackendMessage::BindComplete]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        session.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Statement,
                name: "drop".into(),
            }
        ),
        [
            BackendMessage::ParameterDescription(vec![]),
            BackendMessage::NoData
        ]
    );
    assert_eq!(
        session.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Portal,
                name: "drop".into(),
            }
        ),
        [BackendMessage::NoData]
    );
    assert_eq!(files(&root), before);
    assert_eq!(
        session.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "drop".into(),
                max_rows: 0,
            }
        ),
        [BackendMessage::CommandComplete("DROP TABLE".into())]
    );
    ok(&session.handle(&mut db, FrontendMessage::Sync));
    assert!(db.schema().table("users").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_prepared_drop_does_not_rebind_after_other_session_recreates_name() {
    let (root, mut db) = seed("pg-drop-stale");
    let mut first = session(&db, principal(true));
    let mut second = session(&db, principal(true));
    ok(&first.handle(
        &mut db,
        FrontendMessage::Parse {
            statement: "old".into(),
            query: "DROP TABLE users".into(),
            parameter_types: vec![],
        },
    ));
    ok(&first.handle(
        &mut db,
        FrontendMessage::Bind {
            portal: "old".into(),
            statement: "old".into(),
            parameter_formats: vec![],
            parameters: vec![],
            result_formats: vec![],
        },
    ));
    ok(&sql(&mut second, &mut db, "DROP TABLE users"));
    ok(&sql(&mut second, &mut db, "CREATE TABLE users (id BIGINT)"));
    let replacement = db.schema().table("users").unwrap().id;
    assert_ne!(replacement, TableId(1));
    state(
        &first.handle(
            &mut db,
            FrontendMessage::Execute {
                portal: "old".into(),
                max_rows: 0,
            },
        ),
        "42P01",
    );
    ok(&first.handle(&mut db, FrontendMessage::Sync));
    assert_eq!(db.schema().table("users").unwrap().id, replacement);
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_drop_authorization_errors_and_transaction_state_are_side_effect_free() {
    let (root, mut db) = seed("pg-drop-auth-txn");
    let before = files(&root);
    let mut denied = session(&db, principal(false));
    state(&sql(&mut denied, &mut db, "DROP TABLE users"), "42501");
    assert_eq!(files(&root), before);
    assert!(db.schema().table("users").is_some());

    let mut admin = session(&db, principal(true));
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    assert_eq!(
        sql(&mut admin, &mut db, "DROP TABLE users"),
        [
            BackendMessage::CommandComplete("DROP TABLE".into()),
            BackendMessage::ReadyForQuery(b'T')
        ]
    );
    state(&sql(&mut admin, &mut db, "SELECT * FROM users"), "42P01");
    assert_eq!(admin.status, PgTransactionStatus::Failed);
    state(&sql(&mut admin, &mut db, "COMMIT"), "25P02");
    ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    assert!(db.schema().table("users").is_some());

    state(&sql(&mut admin, &mut db, "DROP TABLE missing"), "42P01");
    for unsupported in [
        "DROP TABLE IF EXISTS users",
        "DROP TABLE users CASCADE",
        "DROP TABLE users RESTRICT",
    ] {
        state(&sql(&mut admin, &mut db, unsupported), "0A000");
    }
    ok(&sql(&mut admin, &mut db, "BEGIN"));
    ok(&sql(&mut admin, &mut db, "DROP TABLE users"));
    ok(&sql(&mut admin, &mut db, "COMMIT"));
    assert!(db.schema().table("users").is_none());
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert!(reopened.schema().table("users").is_none());
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_schema_admin_can_drop_without_any_table_data_grant() {
    let (root, mut db) = seed("pg-drop-schema-only");
    let authorization = AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(PrincipalGrants {
            schema_admin: true,
            tables: vec![],
        }),
        vec![],
        &[TableId(1)],
    )
    .unwrap()
    .admit(&ClientIdentity::LocalPlaintext)
    .unwrap();
    let mut admin = session(&db, authorization);
    assert_eq!(
        sql(&mut admin, &mut db, "DROP TABLE users"),
        [
            BackendMessage::CommandComplete("DROP TABLE".into()),
            BackendMessage::ReadyForQuery(b'I')
        ]
    );
    assert!(db.schema().table("users").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
