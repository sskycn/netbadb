use super::*;
use crate::authorization::{AuthorizationPolicy, PrincipalGrants};
use crate::sql_create_table_test_support::{files, principal, seed};
use netbadb_protocol::ClientMessage;
use netbadb_types::TableId;

fn session(
    db: &mut Database,
    authorization: crate::authorization::PrincipalAuthorization,
) -> WorkerSession {
    let mut session = WorkerSession::new(
        SessionPolicy::default(),
        ClientIdentity::LocalPlaintext,
        authorization,
    );
    session.handle(db, 1, ClientMessage::Hello);
    session
}

#[test]
fn native_sql_drop_authorization_rollback_commit_and_reopen_use_protocol_v1() {
    let (root, mut db) = seed("native-drop");
    let before = files(&root);
    let mut denied = session(&mut db, principal(false));
    let response = denied.handle(
        &mut db,
        2,
        ClientMessage::Execute {
            sql: "DROP TABLE users".into(),
        },
    );
    assert!(response.authorization_denied);
    assert_eq!(files(&root), before);

    let mut admin = session(&mut db, principal(true));
    let begin = admin.handle(
        &mut db,
        3,
        ClientMessage::Begin {
            table_id: TableId(1),
        },
    );
    assert!(!begin.authorization_denied);
    let dropped = admin.handle(
        &mut db,
        4,
        ClientMessage::Execute {
            sql: "DROP TABLE users;".into(),
        },
    );
    assert_eq!(
        dropped.batch.messages,
        [ServerMessage::AffectedRows { count: 0 }]
    );
    assert!(db.schema().table("users").is_some());
    let rollback = admin.handle(&mut db, 5, ClientMessage::Rollback);
    assert_eq!(
        rollback.batch.messages,
        [ServerMessage::TransactionRolledBack]
    );
    assert!(db.schema().table("users").is_some());

    let dropped = admin.handle(
        &mut db,
        6,
        ClientMessage::Execute {
            sql: "DROP TABLE users".into(),
        },
    );
    assert_eq!(
        dropped.batch.messages,
        [ServerMessage::AffectedRows { count: 0 }]
    );
    assert!(db.schema().table("users").is_none());
    db.close().unwrap();
    let reopened = Database::open_catalog(root.join("catalog")).unwrap();
    assert!(reopened.schema().table("users").is_none());
    reopened.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn native_schema_admin_needs_no_table_data_grant_to_drop() {
    let (root, mut db) = seed("native-drop-schema-only");
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
    let mut admin = session(&mut db, authorization);
    let response = admin.handle(
        &mut db,
        2,
        ClientMessage::Execute {
            sql: "DROP TABLE users".into(),
        },
    );
    assert!(!response.authorization_denied);
    assert_eq!(
        response.batch.messages,
        [ServerMessage::AffectedRows { count: 0 }]
    );
    assert!(db.schema().table("users").is_none());
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
