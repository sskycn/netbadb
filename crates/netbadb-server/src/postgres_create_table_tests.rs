use super::*;
use crate::sql_create_table_test_support::{CREATE, files, principal, seed};
use netbadb_core::{SchemaGeneration, TableSchemaVersion};
use netbadb_types::StorageId;

fn session(db: &Database, admin: bool) -> PgWorkerSession {
    PgWorkerSession::new(
        db,
        SessionPolicy::default(),
        principal(admin),
        StartupMessage {
            parameters: Default::default(),
        },
        1,
    )
    .unwrap()
    .0
}
fn ok(messages: &[BackendMessage]) {
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse(_))),
        "{messages:?}"
    );
}
fn sql(session: &mut PgWorkerSession, db: &mut Database, sql: &str) -> Vec<BackendMessage> {
    session.handle(db, FrontendMessage::Query(sql.into()))
}
fn state(messages: &[BackendMessage], code: &str) {
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse(e) if e.sqlstate == code)),
        "{messages:?}"
    );
}
fn parse(
    session: &mut PgWorkerSession,
    db: &mut Database,
    name: &str,
    sql: &str,
) -> Vec<BackendMessage> {
    session.handle(
        db,
        FrontendMessage::Parse {
            statement: name.into(),
            query: sql.into(),
            parameter_types: vec![],
        },
    )
}
fn bind(
    session: &mut PgWorkerSession,
    db: &mut Database,
    portal: &str,
    name: &str,
    values: Vec<Option<Vec<u8>>>,
) -> Vec<BackendMessage> {
    session.handle(
        db,
        FrontendMessage::Bind {
            portal: portal.into(),
            statement: name.into(),
            parameter_formats: vec![],
            parameters: values,
            result_formats: vec![],
        },
    )
}
fn execute(session: &mut PgWorkerSession, db: &mut Database, portal: &str) -> Vec<BackendMessage> {
    session.handle(
        db,
        FrontendMessage::Execute {
            portal: portal.into(),
            max_rows: 0,
        },
    )
}

#[test]
fn pg_parse_bind_describe_are_pure_execute_checks_current_schema() {
    let (root, mut db) = seed("extended");
    let mut s = session(&db, true);
    let before = files(&root);
    assert_eq!(
        parse(&mut s, &mut db, "create", CREATE),
        [BackendMessage::ParseComplete]
    );
    assert_eq!(
        bind(&mut s, &mut db, "one", "create", vec![]),
        [BackendMessage::BindComplete]
    );
    assert_eq!(
        s.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Statement,
                name: "create".into()
            }
        ),
        [
            BackendMessage::ParameterDescription(vec![]),
            BackendMessage::NoData
        ]
    );
    assert_eq!(
        s.handle(
            &mut db,
            FrontendMessage::Describe {
                target: DescribeTarget::Portal,
                name: "one".into()
            }
        ),
        [BackendMessage::NoData]
    );
    assert_eq!(files(&root), before);
    assert_eq!(db.next_table_id(), Some(TableId(2)));
    assert_eq!(db.next_storage_id(), Some(StorageId(2)));
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    assert_eq!(
        execute(&mut s, &mut db, "one"),
        [BackendMessage::CommandComplete("CREATE TABLE".into())]
    );
    ok(&s.handle(&mut db, FrontendMessage::Sync));
    ok(&bind(&mut s, &mut db, "two", "create", vec![]));
    state(&execute(&mut s, &mut db, "two"), "42P07");
    ok(&s.handle(&mut db, FrontendMessage::Sync));
    assert_eq!(db.next_table_id(), Some(TableId(3)));
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    state(&sql(&mut s, &mut db, "SELECT * FROM projects"), "42501");
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_preflight_errors_have_no_storage_effects_and_poison_explicit_transactions() {
    let (root, mut db) = seed("errors");
    let mut admin = session(&db, true);
    let mut denied = session(&db, false);
    let before = files(&root);
    for (statement, code) in [
        ("CREATE TABLE t (a BIGINT PRIMARY KEY)", "0A000"),
        ("CREATE TABLE t (a TEXT, a TEXT)", "42701"),
        ("CREATE TABLE t (a MAGIC_TYPE)", "42704"),
        ("CREATE TABLE t (a NUMERIC)", "0A000"),
        ("CREATE TABLE t (a UINT64)", "0A000"),
        ("CREATE TABLE t (a VARCHAR(20))", "0A000"),
        ("CREATE TABLE t (a TEXT,)", "42601"),
    ] {
        let result = sql(&mut admin, &mut db, statement);
        state(&result, code);
        if statement.contains("MAGIC_TYPE") {
            assert!(result.iter().any(|m| matches!(m, BackendMessage::ErrorResponse(e) if e.position == Some(statement.find("MAGIC_TYPE").unwrap() as u32 + 1))));
        }
    }
    state(&sql(&mut denied, &mut db, CREATE), "42501");
    ok(&parse(&mut denied, &mut db, "denied", CREATE));
    ok(&bind(&mut denied, &mut db, "denied", "denied", vec![]));
    state(&execute(&mut denied, &mut db, "denied"), "42501");
    ok(&denied.handle(&mut db, FrontendMessage::Sync));
    state(
        &parse(&mut admin, &mut db, "uint", "CREATE TABLE t (a UINT64)"),
        "0A000",
    );
    ok(&admin.handle(&mut db, FrontendMessage::Sync));
    assert_eq!(files(&root), before);
    assert_eq!(db.next_table_id(), Some(TableId(2)));
    assert_eq!(db.next_storage_id(), Some(StorageId(2)));
    assert_eq!(db.schema_generation(), SchemaGeneration(1));
    for (statement, code) in [
        ("CREATE TABLE users (id BIGINT)", "42P07"),
        ("CREATE TABLE t (id BIGINT PRIMARY KEY)", "0A000"),
    ] {
        ok(&sql(&mut admin, &mut db, "BEGIN"));
        state(&sql(&mut admin, &mut db, statement), code);
        assert_eq!(admin.status, PgTransactionStatus::Failed);
        state(&sql(&mut admin, &mut db, "SELECT 1"), "25P02");
        ok(&sql(&mut admin, &mut db, "ROLLBACK"));
    }
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn pg_simple_and_extended_overlay_creator_access_commit_and_rollback() {
    let (root, mut db) = seed("overlay");
    let mut s = session(&db, true);
    let mut observer = session(&db, true);
    for commit in [false, true] {
        ok(&sql(&mut s, &mut db, "BEGIN"));
        ok(&sql(&mut s, &mut db, CREATE));
        state(
            &sql(&mut observer, &mut db, "SELECT * FROM projects"),
            "42P01",
        );
        ok(&parse(
            &mut s,
            &mut db,
            "insert",
            "INSERT INTO projects VALUES ($1, $2, $3)",
        ));
        ok(&bind(
            &mut s,
            &mut db,
            "insert",
            "insert",
            vec![
                Some(b"10".to_vec()),
                Some(b"demo".to_vec()),
                Some(b"true".to_vec()),
            ],
        ));
        ok(&execute(&mut s, &mut db, "insert"));
        ok(&s.handle(&mut db, FrontendMessage::Sync));
        let result = sql(&mut s, &mut db, "SELECT * FROM projects WHERE id = 10");
        ok(&result);
        assert!(result.contains(&BackendMessage::DataRow(vec![
            Some(b"10".to_vec()),
            Some(b"demo".to_vec()),
            Some(b"t".to_vec())
        ])));
        ok(&sql(
            &mut s,
            &mut db,
            if commit { "COMMIT" } else { "ROLLBACK" },
        ));
        assert!(s.portals.is_empty());
        // Staged prepared statements cannot escape the creating transaction.
        ok(&bind(
            &mut s,
            &mut db,
            "stale",
            "insert",
            vec![
                Some(b"11".to_vec()),
                Some(b"bad".to_vec()),
                Some(b"true".to_vec()),
            ],
        ));
        state(&execute(&mut s, &mut db, "stale"), "42501");
        ok(&s.handle(&mut db, FrontendMessage::Sync));
        state(
            &sql(&mut s, &mut db, "SELECT * FROM projects"),
            if commit { "42501" } else { "42P01" },
        );
        if !commit {
            assert_eq!(db.schema_generation(), SchemaGeneration(1));
            assert_eq!(db.next_table_id(), Some(TableId(3)));
            s.prepared.clear();
        }
    }
    assert_eq!(db.schema().table("projects").unwrap().id, TableId(3));
    assert_eq!(db.schema_generation(), SchemaGeneration(2));
    assert_eq!(
        db.table_schema_version(TableId(3)),
        Some(TableSchemaVersion(1))
    );
    // Existing observer refreshes metadata but its grants still hide the new table.
    let list = "SELECT ns.nspname AS \"Schema\", rel.relname AS \"Name\", CASE rel.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' END AS \"Type\", pg_catalog.pg_get_userbyid(rel.relowner) AS \"Owner\" FROM pg_catalog.pg_class AS rel LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace LEFT JOIN pg_catalog.pg_am AS method ON method.oid = rel.relam WHERE rel.relkind IN ('r', 'p', '') AND pg_catalog.pg_table_is_visible(rel.oid)";
    let result = sql(&mut observer, &mut db, list);
    ok(&result);
    assert!(observer.catalog.table("projects").is_some());
    assert!(!result.iter().any(|m| matches!(m, BackendMessage::DataRow(row) if row.iter().flatten().any(|v| v == b"projects"))));
    db.close().unwrap();
    let mut db = Database::open_catalog(root.join("catalog")).unwrap();
    assert_eq!(
        db.query("SELECT * FROM projects").unwrap().rows,
        vec![vec![
            ScalarValue::Int64(10),
            ScalarValue::Text("demo".into()),
            ScalarValue::Bool(true)
        ]]
    );
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn schema_only_admin_can_begin_without_durable_table_grants() {
    let (root, mut db) = seed("schema-only");
    let auth = AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(crate::authorization::PrincipalGrants {
            schema_admin: true,
            tables: vec![],
        }),
        vec![],
        &[TableId(1)],
    )
    .unwrap()
    .admit(&ClientIdentity::LocalPlaintext)
    .unwrap();
    let mut s = PgWorkerSession::new(
        &db,
        SessionPolicy::default(),
        auth,
        StartupMessage {
            parameters: Default::default(),
        },
        1,
    )
    .unwrap()
    .0;
    ok(&sql(&mut s, &mut db, "BEGIN"));
    ok(&sql(&mut s, &mut db, CREATE));
    ok(&sql(
        &mut s,
        &mut db,
        "INSERT INTO projects VALUES (1, NULL, true)",
    ));
    ok(&sql(&mut s, &mut db, "COMMIT"));
    state(&sql(&mut s, &mut db, "SELECT * FROM projects"), "42501");
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn prepared_existing_table_survives_transaction_end_and_unrelated_create() {
    let (root, mut db) = seed("prepared-existing");
    let mut s = session(&db, true);
    ok(&sql(&mut s, &mut db, "INSERT INTO users VALUES (7)"));
    ok(&sql(&mut s, &mut db, "BEGIN"));
    ok(&parse(&mut s, &mut db, "existing", "SELECT * FROM users"));
    ok(&sql(&mut s, &mut db, CREATE));
    ok(&sql(&mut s, &mut db, "COMMIT"));
    ok(&bind(&mut s, &mut db, "existing", "existing", vec![]));
    let result = execute(&mut s, &mut db, "existing");
    ok(&result);
    assert!(result.contains(&BackendMessage::DataRow(vec![Some(b"7".to_vec())])));
    ok(&s.handle(&mut db, FrontendMessage::Sync));
    db.close().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
