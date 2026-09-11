use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_core::{
    AdaptiveEvidencePoolLimits, Database, DatabaseCoordinatorConfig, TableStorageCreateSpec,
};
use netbadb_protocol::{ClientMessage, ProtocolErrorCode, ServerMessage};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};

use super::*;
use crate::adaptive_feedback::{ServerAdaptiveFeedbackConfig, ServerAdaptiveFeedbackRuntime};
use crate::authorization::{PrincipalGrants, TablePermissions};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
const TABLE_ID: TableId = TableId(51_510);

struct Fixture {
    root: PathBuf,
    database: Database,
}

impl Fixture {
    fn create(name: &str) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-native-feedback-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create native feedback root");
        let table = TableDef::new(
            TABLE_ID,
            "events",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(
                    ColumnId(2),
                    "category",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
            ],
        );
        let mut database = Database::create_catalog(
            root.join("catalog"),
            vec![TableStorageCreateSpec::heap(root.join("events"), table)],
            Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
        )
        .expect("create native feedback database");
        database
            .execute("INSERT INTO events (id, category) VALUES (1, 7)")
            .expect("seed first row");
        database
            .execute("INSERT INTO events (id, category) VALUES (2, 7)")
            .expect("seed second row");
        Self { root, database }
    }

    fn close(self) {
        self.database.close().expect("close native database");
        fs::remove_dir_all(self.root).expect("remove native feedback root");
    }
}

fn principal(read: bool, write: bool, transaction: bool) -> PrincipalAuthorization {
    AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(PrincipalGrants {
            schema_admin: false,
            tables: vec![TablePermissions::new(
                TABLE_ID,
                read,
                write,
                transaction,
                false,
            )],
        }),
        Vec::new(),
        &[TABLE_ID],
    )
    .expect("authorization policy")
    .admit(&ClientIdentity::LocalPlaintext)
    .expect("principal")
}

fn session(policy: SessionPolicy, principal: PrincipalAuthorization) -> WorkerSession {
    WorkerSession::new(policy, ClientIdentity::LocalPlaintext, principal)
}

fn handshake(session: &mut WorkerSession, database: &mut Database) {
    let response = session.handle(database, 1, ClientMessage::Hello);
    assert!(matches!(
        response.batch.messages.as_slice(),
        [ServerMessage::HelloAck { .. }]
    ));
}

fn runtime() -> ServerAdaptiveFeedbackRuntime {
    ServerAdaptiveFeedbackRuntime::new(ServerAdaptiveFeedbackConfig::new(
        AdaptiveEvidencePoolLimits::default(),
    ))
}

#[test]
fn native_worker_runtime_is_shared_and_excludes_transactions_and_dml() {
    let mut fixture = Fixture::create("shared-worker");
    let mut runtime = runtime();
    let mut first = session(SessionPolicy::default(), principal(true, true, true));
    let mut second = session(SessionPolicy::default(), principal(true, true, true));
    handshake(&mut first, &mut fixture.database);
    handshake(&mut second, &mut fixture.database);

    let ordinary = second.handle(
        &mut fixture.database,
        2,
        ClientMessage::Execute {
            sql: "SELECT id FROM events WHERE category = 7".into(),
        },
    );
    let response = first.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        2,
        ClientMessage::Execute {
            sql: "SELECT id FROM events WHERE category = 7".into(),
        },
    );
    assert!(matches!(
        response.batch.messages.first(),
        Some(ServerMessage::QueryStart { .. })
    ));
    assert_eq!(response.batch.messages, ordinary.batch.messages);
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);

    second.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        3,
        ClientMessage::Execute {
            sql: "SELECT id FROM events".into(),
        },
    );
    assert_eq!(runtime.inspection().progress.recorded_reports, 2);

    first.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        4,
        ClientMessage::Begin { table_id: TABLE_ID },
    );
    let response = first.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        5,
        ClientMessage::Execute {
            sql: "SELECT id FROM events".into(),
        },
    );
    assert!(matches!(
        response.batch.messages.first(),
        Some(ServerMessage::QueryStart { .. })
    ));
    first.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        6,
        ClientMessage::Rollback,
    );
    assert_eq!(runtime.inspection().progress.recorded_reports, 2);

    let before_dml = fixture
        .database
        .current_database_snapshot()
        .expect("inspect before DML")
        .expect("global snapshot before DML")
        .commit_seq();
    let response = second.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        7,
        ClientMessage::Execute {
            sql: "INSERT INTO events (id, category) VALUES (3, 8)".into(),
        },
    );
    assert_eq!(
        response.batch.messages,
        vec![ServerMessage::AffectedRows { count: 1 }]
    );
    assert_eq!(runtime.inspection().progress.recorded_reports, 2);
    let after_dml = fixture
        .database
        .current_database_snapshot()
        .expect("inspect after DML")
        .expect("global snapshot after DML")
        .commit_seq();
    assert_eq!(after_dml.0, before_dml.0 + 1);

    second.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        8,
        ClientMessage::Execute {
            sql: "SELECT id FROM events".into(),
        },
    );
    let inspected = runtime.inspection();
    assert_eq!(inspected.progress.recorded_reports, 3);
    assert_eq!(inspected.diagnostics.eligible_query_count, 3);
    assert_eq!(inspected.diagnostics.record_success_count, 3);
    fixture.close();
}

#[test]
fn native_authorization_denial_has_no_adaptive_side_effect() {
    let mut fixture = Fixture::create("denied");
    let mut runtime = runtime();
    let mut denied = session(SessionPolicy::default(), principal(false, false, true));
    handshake(&mut denied, &mut fixture.database);
    let before = runtime.inspection();

    let response = denied.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        2,
        ClientMessage::Execute {
            sql: "SELECT id FROM events".into(),
        },
    );
    assert!(response.authorization_denied);
    assert!(matches!(
        response.batch.messages.as_slice(),
        [ServerMessage::Error {
            code: ProtocolErrorCode::Database,
            ..
        }]
    ));
    assert_eq!(runtime.inspection(), before);
    fixture.close();
}

#[test]
fn native_row_limit_failure_occurs_after_core_success_capture() {
    let mut fixture = Fixture::create("row-limit");
    let policy = SessionPolicy::new(1).expect("one row policy");
    let mut session = session(policy, principal(true, false, false));
    let mut runtime = runtime();
    handshake(&mut session, &mut fixture.database);

    let response = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        2,
        ClientMessage::Execute {
            sql: "SELECT id FROM events".into(),
        },
    );
    assert!(response.result_row_limit_exceeded);
    assert!(matches!(
        response.batch.messages.as_slice(),
        [ServerMessage::Error { .. }]
    ));
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);
    fixture.close();
}

#[test]
fn native_server_builder_is_default_disabled_and_explicitly_enabled() {
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-native-feedback-config-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create config root");
    let table = root.join("events.ndb");
    fs::write(&table, []).expect("create manifest table path");
    let manifest = root.join("server.json");
    let source = format!(
        r#"{{
            "version": 5,
            "listen": "127.0.0.1:0",
            "authorization": {{
                "local_plaintext": {{
                    "tables": [{{"table_id": 51510, "read": true}}]
                }},
                "clients": []
            }},
            "tables": [{{
                "path": "{}",
                "id": 51510,
                "name": "events",
                "columns": [{{
                    "id": 1,
                    "name": "id",
                    "physical_type": "int64",
                    "nullable": false,
                    "primary_key": false
                }}]
            }}]
        }}"#,
        Path::new("events.ndb").display()
    );
    fs::write(&manifest, source).expect("write manifest");
    let config = ServerConfig::from_manifest_path(&manifest).expect("parse manifest");

    let disabled = TcpServer::new(config);
    assert!(disabled.adaptive_override.is_none());
    let limits = AdaptiveEvidencePoolLimits::default();
    let enabled = disabled.with_adaptive_feedback(ServerAdaptiveFeedbackConfig::new(limits));
    assert!(matches!(
        enabled.adaptive_override,
        Some(crate::adaptive_driver::ServerAdaptiveStartupMode::FeedbackOnly(config))
            if config.limits() == limits
    ));

    fs::remove_dir_all(root).expect("remove config root");
}
