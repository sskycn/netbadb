use super::*;
use crate::authorization::{AuthorizationPolicy, PrincipalGrants, TablePermissions};
use crate::sql_create_table_test_support::{files, seed};
use netbadb_protocol::ClientMessage;
use netbadb_types::TableId;

fn authorization(schema_admin: bool) -> crate::authorization::PrincipalAuthorization {
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

fn session(db: &mut Database, schema_admin: bool) -> WorkerSession {
    let mut session = WorkerSession::new(
        SessionPolicy::default(),
        ClientIdentity::LocalPlaintext,
        authorization(schema_admin),
    );
    session.handle(db, 1, ClientMessage::Hello);
    session
}

#[test]
fn native_protocol_v1_alter_authorization_transaction_and_autocommit() {
    let (root, mut db) = seed("native-alter");
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (1, 'one')")
        .unwrap();
    let before = files(&root);
    let mut denied = session(&mut db, false);
    let response = denied.handle(
        &mut db,
        2,
        ClientMessage::Execute {
            sql: "ALTER TABLE projects RENAME TO work".into(),
        },
    );
    assert!(response.authorization_denied);
    assert_eq!(files(&root), before);

    let mut admin = session(&mut db, true);
    assert_eq!(
        admin
            .handle(
                &mut db,
                3,
                ClientMessage::Begin {
                    table_id: TableId(2),
                },
            )
            .batch
            .messages,
        [ServerMessage::TransactionStarted]
    );
    assert_eq!(
        admin
            .handle(
                &mut db,
                4,
                ClientMessage::Execute {
                    sql: "ALTER TABLE projects ADD COLUMN active BOOLEAN".into(),
                },
            )
            .batch
            .messages,
        [ServerMessage::AffectedRows { count: 0 }]
    );
    assert_eq!(
        admin
            .handle(
                &mut db,
                5,
                ClientMessage::Execute {
                    sql: "INSERT INTO projects VALUES (2, 'two', true)".into(),
                },
            )
            .batch
            .messages,
        [ServerMessage::AffectedRows { count: 1 }]
    );
    assert_eq!(
        admin
            .handle(&mut db, 6, ClientMessage::Rollback)
            .batch
            .messages,
        [ServerMessage::TransactionRolledBack]
    );
    assert!(
        db.schema()
            .table("projects")
            .unwrap()
            .column("active")
            .is_none()
    );

    assert_eq!(
        admin
            .handle(
                &mut db,
                7,
                ClientMessage::Execute {
                    sql: "ALTER TABLE projects RENAME TO work".into(),
                },
            )
            .batch
            .messages,
        [ServerMessage::AffectedRows { count: 0 }]
    );
    assert_eq!(db.schema().table("work").unwrap().id, TableId(2));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
