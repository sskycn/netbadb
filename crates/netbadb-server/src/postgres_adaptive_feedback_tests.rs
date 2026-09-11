use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use netbadb_core::{
    AdaptiveEvidencePoolLimits, AdaptiveEvidenceRecordError, AdaptiveEvidenceRecordOutcome,
    DatabaseCoordinatorConfig, PhysicalDesignAdvisorPolicy, PhysicalDesignEvidenceLimits,
    PhysicalDesignEvidenceRecordError, PhysicalDesignRecommendationPolicy, TableStorageCreateSpec,
};
use netbadb_schema::{ColumnDef, TableDef, TypeSpec};

use super::*;
use crate::adaptive_feedback::{ServerAdaptiveFeedbackConfig, ServerAdaptiveFeedbackRuntime};
use crate::authorization::{PrincipalGrants, TablePermissions};
use crate::physical_design::{ServerPhysicalDesignAdvisorConfig, ServerPhysicalDesignRuntime};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
const TABLE_ID: TableId = TableId(51_520);

struct Fixture {
    root: PathBuf,
    database: Database,
}

impl Fixture {
    fn create(name: &str, global: bool) -> Self {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-postgres-feedback-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create PostgreSQL feedback root");
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
        let mut database = if global {
            Database::create_catalog(
                root.join("catalog"),
                vec![TableStorageCreateSpec::heap(root.join("events"), table)],
                Some(
                    DatabaseCoordinatorConfig::new(root.join("coordinator"))
                        .with_global_visibility(),
                ),
            )
            .expect("create global PostgreSQL feedback database")
        } else {
            Database::create_tables(vec![(root.join("events"), table)])
                .expect("create local PostgreSQL feedback database")
        };
        for id in 1..=3_i64 {
            database
                .execute(&format!(
                    "INSERT INTO events (id, category) VALUES ({id}, 7)"
                ))
                .expect("seed PostgreSQL feedback row");
        }
        Self { root, database }
    }

    fn close(self) {
        self.database
            .close()
            .expect("close PostgreSQL feedback database");
        fs::remove_dir_all(self.root).expect("remove PostgreSQL feedback root");
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

fn new_session(
    database: &Database,
    policy: SessionPolicy,
    principal: PrincipalAuthorization,
) -> PgWorkerSession {
    PgWorkerSession::new(
        database,
        policy,
        principal,
        StartupMessage {
            parameters: Default::default(),
        },
        1,
    )
    .expect("PostgreSQL worker session")
    .0
}

fn runtime() -> ServerAdaptiveFeedbackRuntime {
    ServerAdaptiveFeedbackRuntime::new(ServerAdaptiveFeedbackConfig::new(
        AdaptiveEvidencePoolLimits::default(),
    ))
}

fn design_runtime() -> ServerPhysicalDesignRuntime {
    let recommendation = PhysicalDesignRecommendationPolicy {
        minimum_reports: 1,
        minimum_distinct_query_shapes: 1,
        minimum_actual_scan_work_units: 0,
        max_recommendations: 8,
    };
    ServerPhysicalDesignRuntime::new(ServerPhysicalDesignAdvisorConfig::new(
        PhysicalDesignEvidenceLimits::default(),
        PhysicalDesignAdvisorPolicy {
            index: recommendation,
            columnar: recommendation,
        },
    ))
}

fn assert_successful_query(messages: &[BackendMessage]) {
    assert!(
        messages
            .iter()
            .any(|message| matches!(message, BackendMessage::DataRow(_)))
    );
    assert!(
        messages
            .iter()
            .any(|message| matches!(message, BackendMessage::CommandComplete(_)))
    );
    assert!(matches!(
        messages.last(),
        Some(BackendMessage::ReadyForQuery(b'I'))
    ));
    assert!(
        !messages
            .iter()
            .any(|message| matches!(message, BackendMessage::ErrorResponse(_)))
    );
}

#[test]
fn postgres_simple_capture_excludes_compatibility_transactions_and_dml() {
    let mut fixture = Fixture::create("simple", true);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, true, true),
    );
    let mut runtime = runtime();

    let mut ordinary_session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, false, false),
    );
    let ordinary = ordinary_session.handle(
        &mut fixture.database,
        FrontendMessage::Query("SELECT id FROM events WHERE category = 7".into()),
    );
    let messages = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events WHERE category = 7".into()),
    );
    assert_successful_query(&messages);
    assert_eq!(messages, ordinary);
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);

    let compatibility = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT version()".into()),
    );
    assert_successful_query(&compatibility);
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);

    session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("BEGIN".into()),
    );
    session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("COMMIT".into()),
    );
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);

    let dml = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("INSERT INTO events (id, category) VALUES (4, 8)".into()),
    );
    assert!(dml.iter().any(
        |message| matches!(message, BackendMessage::CommandComplete(tag) if tag == "INSERT 0 1")
    ));
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);

    let mut second = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, false, false),
    );
    let second_messages = second.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events WHERE id = 1".into()),
    );
    assert_successful_query(&second_messages);
    assert_eq!(runtime.inspection().progress.recorded_reports, 2);
    fixture.close();
}

#[test]
fn postgres_default_session_path_remains_ordinary_execution() {
    let mut fixture = Fixture::create("disabled", true);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, false, false),
    );
    let messages = session.handle(
        &mut fixture.database,
        FrontendMessage::Query("SELECT id FROM events WHERE id = 1".into()),
    );
    assert_successful_query(&messages);
    fixture.close();
}

#[test]
fn postgres_server_builder_is_default_disabled_and_explicitly_enabled() {
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-postgres-feedback-config-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create PostgreSQL config root");
    fs::write(root.join("events.ndb"), []).expect("create manifest table path");
    let manifest = root.join("server.json");
    fs::write(
        &manifest,
        r#"{
            "version": 7,
            "listen": "127.0.0.1:0",
            "authorization": {
                "local_plaintext": {
                    "tables": [{"table_id": 51520, "read": true}]
                },
                "clients": []
            },
            "tables": [{
                "path": "events.ndb",
                "id": 51520,
                "name": "events",
                "columns": [{
                    "id": 1,
                    "name": "id",
                    "physical_type": "int64",
                    "nullable": false,
                    "primary_key": false
                }]
            }]
        }"#,
    )
    .expect("write PostgreSQL manifest");
    let config = ServerConfig::from_manifest_path(&manifest).expect("parse PostgreSQL manifest");

    let disabled = PostgresTcpServer::new(config.clone());
    assert!(disabled.adaptive_override.is_none());
    assert!(disabled.physical_design_override.is_none());
    let limits = AdaptiveEvidencePoolLimits::default();
    let design = design_runtime().status().evidence.limits;
    let recommendation = PhysicalDesignRecommendationPolicy {
        minimum_reports: 1,
        minimum_distinct_query_shapes: 1,
        minimum_actual_scan_work_units: 0,
        max_recommendations: 8,
    };
    let design = ServerPhysicalDesignAdvisorConfig::new(
        design,
        PhysicalDesignAdvisorPolicy {
            index: recommendation,
            columnar: recommendation,
        },
    );
    let enabled = disabled
        .with_adaptive_feedback(ServerAdaptiveFeedbackConfig::new(limits))
        .with_physical_design_advisor(design);
    assert!(matches!(
        enabled.adaptive_override,
        Some(crate::adaptive_driver::ServerAdaptiveStartupMode::FeedbackOnly(config))
            if config.limits() == limits
    ));
    assert_eq!(enabled.physical_design_override, Some(design));
    let reverse = PostgresTcpServer::new(config)
        .with_physical_design_advisor(design)
        .with_adaptive_feedback(ServerAdaptiveFeedbackConfig::new(limits));
    assert!(matches!(
        reverse.adaptive_override,
        Some(crate::adaptive_driver::ServerAdaptiveStartupMode::FeedbackOnly(config))
            if config.limits() == limits
    ));
    assert_eq!(reverse.physical_design_override, Some(design));
    fs::remove_dir_all(root).expect("remove PostgreSQL config root");
}

#[test]
fn postgres_extended_portal_captures_only_first_real_execution() {
    let mut fixture = Fixture::create("extended", true);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, false, false),
    );
    let mut runtime = runtime();

    assert_eq!(
        session.handle_with_adaptive_feedback(
            &mut fixture.database,
            Some(&mut runtime),
            FrontendMessage::Parse {
                statement: "by_category".into(),
                query: "SELECT id FROM events WHERE category = $1".into(),
                parameter_types: vec![PostgresType::Int8.oid()],
            },
        ),
        vec![BackendMessage::ParseComplete]
    );
    assert_eq!(
        session.handle_with_adaptive_feedback(
            &mut fixture.database,
            Some(&mut runtime),
            FrontendMessage::Bind {
                portal: "events_portal".into(),
                statement: "by_category".into(),
                parameter_formats: Vec::new(),
                parameters: vec![Some(b"7".to_vec())],
                result_formats: Vec::new(),
            },
        ),
        vec![BackendMessage::BindComplete]
    );
    assert_eq!(runtime.inspection().progress.recorded_reports, 0);

    let first = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Execute {
            portal: "events_portal".into(),
            max_rows: 1,
        },
    );
    assert!(matches!(
        first.last(),
        Some(BackendMessage::PortalSuspended)
    ));
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);

    for _ in 0..3 {
        session.handle_with_adaptive_feedback(
            &mut fixture.database,
            Some(&mut runtime),
            FrontendMessage::Execute {
                portal: "events_portal".into(),
                max_rows: 1,
            },
        );
        assert_eq!(runtime.inspection().progress.recorded_reports, 1);
    }

    session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Parse {
            statement: "never_executed".into(),
            query: "SELECT id FROM events".into(),
            parameter_types: Vec::new(),
        },
    );
    session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Bind {
            portal: "closed_portal".into(),
            statement: "never_executed".into(),
            parameter_formats: Vec::new(),
            parameters: Vec::new(),
            result_formats: Vec::new(),
        },
    );
    session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Close {
            target: CloseTarget::Portal,
            name: "closed_portal".into(),
        },
    );
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);
    fixture.close();
}

#[test]
fn postgres_telemetry_error_does_not_poison_protocol_state() {
    let mut fixture = Fixture::create("legacy-local", false);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, false, false),
    );
    let mut runtime = runtime();

    let messages = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    assert_successful_query(&messages);
    assert_eq!(session.status, PgTransactionStatus::Idle);
    assert!(!session.awaiting_sync);
    let inspected = runtime.inspection();
    assert_eq!(inspected.progress.recorded_reports, 0);
    assert_eq!(inspected.diagnostics.record_error_count, 1);
    assert_eq!(
        inspected.diagnostics.last_record_error,
        Some(AdaptiveEvidenceRecordError::GlobalVisibilityRequired)
    );

    let next = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events WHERE id = 1".into()),
    );
    assert_successful_query(&next);
    assert_eq!(runtime.inspection().diagnostics.record_error_count, 2);
    fixture.close();
}

#[test]
fn postgres_denied_query_never_reaches_adaptive_capture() {
    let mut fixture = Fixture::create("denied", true);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(false, false, true),
    );
    let mut runtime = runtime();
    let before = runtime.inspection();

    let messages = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    assert!(messages.iter().any(
        |message| matches!(message, BackendMessage::ErrorResponse(error) if error.sqlstate == "42501")
    ));
    assert!(matches!(
        messages.last(),
        Some(BackendMessage::ReadyForQuery(b'I'))
    ));
    assert_eq!(session.status, PgTransactionStatus::Idle);
    assert!(!session.awaiting_sync);
    assert_eq!(runtime.inspection(), before);
    fixture.close();
}

#[test]
fn postgres_core_success_is_captured_before_result_row_policy() {
    let mut fixture = Fixture::create("row-limit", true);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::new(1).expect("one row policy"),
        principal(true, false, false),
    );
    let mut runtime = runtime();
    let messages = session.handle_with_adaptive_feedback(
        &mut fixture.database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    assert!(messages.iter().any(
        |message| matches!(message, BackendMessage::ErrorResponse(error) if error.sqlstate == "54000")
    ));
    assert_eq!(runtime.inspection().progress.recorded_reports, 1);
    fixture.close();
}

#[test]
fn schema_advance_rotates_naturally_on_the_next_query_report() {
    let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "netbadb-postgres-feedback-schema-rotation-{}-{suffix}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create schema rotation root");
    let mut database = Database::create_catalog(
        root.join("catalog"),
        Vec::new(),
        Some(DatabaseCoordinatorConfig::new(root.join("coordinator")).with_global_visibility()),
    )
    .expect("create runtime-schema database");
    database
        .execute("CREATE TABLE events (id BIGINT, category BIGINT)")
        .expect("create runtime table");
    database
        .enable_global_visibility()
        .expect("test fixture enables global visibility explicitly");
    database
        .execute("INSERT INTO events (id, category) VALUES (1, 7)")
        .expect("seed runtime table");
    let runtime_table_id = database.schema().tables()[0].id;
    let runtime_principal = AuthorizationPolicy::new(
        TransportKind::PlaintextLoopback,
        Some(PrincipalGrants {
            schema_admin: false,
            tables: vec![TablePermissions::new(
                runtime_table_id,
                true,
                false,
                false,
                false,
            )],
        }),
        Vec::new(),
        &[runtime_table_id],
    )
    .expect("runtime authorization policy")
    .admit(&ClientIdentity::LocalPlaintext)
    .expect("runtime principal");
    let mut session = new_session(&database, SessionPolicy::default(), runtime_principal);
    let mut runtime = runtime();
    let first = session.handle_with_adaptive_feedback(
        &mut database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    let first_inspection = runtime.inspection();
    assert_eq!(
        first_inspection.diagnostics.last_record_outcome,
        Some(AdaptiveEvidenceRecordOutcome::Recorded),
        "messages={first:?}, inspection={first_inspection:?}"
    );

    database
        .execute("ALTER TABLE events ADD COLUMN note TEXT")
        .expect("advance schema");
    session.handle_with_adaptive_feedback(
        &mut database,
        Some(&mut runtime),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    let inspected = runtime.inspection();
    assert_eq!(inspected.progress.recorded_reports, 1);
    assert_eq!(inspected.diagnostics.schema_rotation_count, 1);
    assert_eq!(
        inspected.diagnostics.last_record_outcome,
        Some(AdaptiveEvidenceRecordOutcome::SchemaRotated)
    );
    database.close().expect("close schema rotation database");
    fs::remove_dir_all(root).expect("remove schema rotation root");
}

#[test]
fn postgres_design_only_extended_portal_records_once_and_keeps_eligibility() {
    let mut fixture = Fixture::create("design-extended", true);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, true, true),
    );
    let mut design = design_runtime();

    assert_eq!(
        session.handle_with_server_observation(
            &mut fixture.database,
            None,
            Some(&mut design),
            FrontendMessage::Parse {
                statement: "by_category".into(),
                query: "SELECT id FROM events WHERE category = $1".into(),
                parameter_types: vec![PostgresType::Int8.oid()],
            },
        ),
        vec![BackendMessage::ParseComplete]
    );
    assert_eq!(
        session.handle_with_server_observation(
            &mut fixture.database,
            None,
            Some(&mut design),
            FrontendMessage::Bind {
                portal: "events_portal".into(),
                statement: "by_category".into(),
                parameter_formats: Vec::new(),
                parameters: vec![Some(b"7".to_vec())],
                result_formats: Vec::new(),
            },
        ),
        vec![BackendMessage::BindComplete]
    );
    let first = session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Execute {
            portal: "events_portal".into(),
            max_rows: 1,
        },
    );
    assert!(matches!(
        first.last(),
        Some(BackendMessage::PortalSuspended)
    ));
    assert_eq!(design.status().evidence.recorded_reports, 1);

    for _ in 0..3 {
        session.handle_with_server_observation(
            &mut fixture.database,
            None,
            Some(&mut design),
            FrontendMessage::Execute {
                portal: "events_portal".into(),
                max_rows: 1,
            },
        );
        assert_eq!(design.status().evidence.recorded_reports, 1);
    }

    session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Query("SELECT version()".into()),
    );
    session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Query("BEGIN".into()),
    );
    session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Query("COMMIT".into()),
    );
    session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Query("INSERT INTO events (id, category) VALUES (4, 8)".into()),
    );
    assert_eq!(design.status().evidence.recorded_reports, 1);
    assert_eq!(design.status().diagnostics.eligible_query_count, 1);
    fixture.close();
}

#[test]
fn postgres_design_only_legacy_local_rejection_preserves_success() {
    let mut fixture = Fixture::create("design-legacy-local", false);
    let mut session = new_session(
        &fixture.database,
        SessionPolicy::default(),
        principal(true, false, false),
    );
    let mut design = design_runtime();
    let messages = session.handle_with_server_observation(
        &mut fixture.database,
        None,
        Some(&mut design),
        FrontendMessage::Query("SELECT id FROM events".into()),
    );
    assert_successful_query(&messages);
    let status = design.status();
    assert_eq!(status.evidence.recorded_reports, 0);
    assert_eq!(status.diagnostics.record_error_count, 1);
    assert_eq!(
        status.diagnostics.last_record_error,
        Some(PhysicalDesignEvidenceRecordError::GlobalVisibilityRequired)
    );
    fixture.close();
}
