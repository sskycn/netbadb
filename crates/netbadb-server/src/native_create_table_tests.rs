use super::*;
use crate::sql_create_table_test_support::{CREATE, files, principal, seed};
use netbadb_types::{ScalarValue, TableId};

fn session(admin: bool, db: &mut Database) -> WorkerSession {
    let mut s = WorkerSession::new(
        SessionPolicy::default(),
        ClientIdentity::LocalPlaintext,
        principal(admin),
    );
    s.handle(db, 1, ClientMessage::Hello);
    s
}
fn run(s: &mut WorkerSession, db: &mut Database, request: ClientMessage) -> Vec<ServerMessage> {
    let result = s.handle(db, 2, request).batch.messages;
    assert!(
        !result
            .iter()
            .any(|m| matches!(m, ServerMessage::Error { .. })),
        "{result:?}"
    );
    result
}
fn sql(sql: &str) -> ClientMessage {
    ClientMessage::Execute { sql: sql.into() }
}

#[test]
fn native_worker_authorizes_typed_ddl_and_transaction_local_dml() {
    let (root, mut db) = seed("native");
    let mut admin = session(true, &mut db);
    let mut denied = session(false, &mut db);
    let before = files(&root);
    let response = denied.handle(&mut db, 2, sql(CREATE));
    assert!(response.authorization_denied);
    assert_eq!(files(&root), before);
    for commit in [false, true] {
        run(
            &mut admin,
            &mut db,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );
        assert_eq!(
            run(&mut admin, &mut db, sql(CREATE)),
            [ServerMessage::AffectedRows { count: 0 }]
        );
        run(
            &mut admin,
            &mut db,
            sql("INSERT INTO projects VALUES (10, 'demo', true)"),
        );
        let result = run(&mut admin, &mut db, sql("SELECT * FROM projects"));
        assert!(result.iter().any(|m| matches!(m, ServerMessage::QueryRow { values } if values == &vec![ScalarValue::Int64(10), ScalarValue::Text("demo".into()), ScalarValue::Bool(true)])));
        run(
            &mut admin,
            &mut db,
            if commit {
                ClientMessage::Commit
            } else {
                ClientMessage::Rollback
            },
        );
        let result = admin.handle(&mut db, 2, sql("SELECT * FROM projects"));
        if commit {
            assert!(result.authorization_denied);
        } else {
            assert!(db.schema().table("projects").is_none());
        }
    }
    run(
        &mut admin,
        &mut db,
        sql("CREATE TABLE native_u64 (value UINT64 NOT NULL)"),
    );
    assert_eq!(
        db.schema().table("native_u64").unwrap().columns[0]
            .semantic_type()
            .physical,
        netbadb_types::PhysicalType::UInt64
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
