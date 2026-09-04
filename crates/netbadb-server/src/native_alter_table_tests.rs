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

fn execute(
    session: &mut WorkerSession,
    db: &mut Database,
    request_id: u64,
    sql: &str,
) -> Vec<ServerMessage> {
    session
        .handle(db, request_id, ClientMessage::Execute { sql: sql.into() })
        .batch
        .messages
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

#[test]
fn native_protocol_v1_routes_staged_drop_without_changing_ddl_results() {
    let (root, mut db) = seed("native-round36-staged-drop");
    db.execute("CREATE TABLE projects (id BIGINT NOT NULL, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO projects VALUES (1, NULL)").unwrap();
    db.execute("CREATE INDEX projects_name_idx ON projects(name)")
        .unwrap();
    let old = db.indexes(TableId(2)).unwrap()[0].clone();
    let mut admin = session(&mut db, true);
    assert_eq!(
        admin
            .handle(
                &mut db,
                2,
                ClientMessage::Begin {
                    table_id: TableId(2),
                },
            )
            .batch
            .messages,
        [ServerMessage::TransactionStarted]
    );
    for (request_id, sql, count) in [
        (
            3,
            "ALTER TABLE projects ADD COLUMN migration_marker TEXT",
            0,
        ),
        (4, "UPDATE projects SET name = 'filled'", 1),
        (5, "UPDATE projects SET migration_marker = 'done'", 1),
        (6, "DROP INDEX projects_name_idx", 0),
        (7, "ALTER TABLE projects ALTER COLUMN name SET NOT NULL", 0),
        (8, "CREATE INDEX projects_name_idx ON projects(name)", 0),
    ] {
        assert_eq!(
            execute(&mut admin, &mut db, request_id, sql),
            [ServerMessage::AffectedRows { count }],
            "{sql}"
        );
    }
    assert_eq!(
        admin
            .handle(&mut db, 9, ClientMessage::Commit)
            .batch
            .messages,
        [ServerMessage::TransactionCommitted]
    );
    let replacement = db.indexes(TableId(2)).unwrap();
    assert_eq!(replacement.len(), 1);
    assert_eq!(replacement[0].name, old.name);
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
