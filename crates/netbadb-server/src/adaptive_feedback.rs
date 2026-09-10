use netbadb_core::{
    AdaptiveEvidencePool, AdaptiveEvidencePoolLimits, AdaptiveEvidenceRecordError,
    AdaptiveEvidenceRecordOutcome, Database, DatabaseError, ExecutionResult, PreparedStatement,
};
#[cfg(test)]
use netbadb_core::{
    AdaptiveEvidencePoolHealth, AdaptiveEvidencePoolInspection, AdaptiveEvidenceProgressToken,
};
use netbadb_types::ScalarValue;

use crate::DatabaseSession;

/// Explicit, bounded Server-side capture configuration.
///
/// This value enables workload feedback capture for eligible queries. It does
/// not define a clock, scheduler cadence, maintenance budget, or automatic
/// action policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerAdaptiveFeedbackConfig {
    limits: AdaptiveEvidencePoolLimits,
}

impl ServerAdaptiveFeedbackConfig {
    #[must_use]
    pub const fn new(limits: AdaptiveEvidencePoolLimits) -> Self {
        Self { limits }
    }

    #[must_use]
    pub const fn limits(self) -> AdaptiveEvidencePoolLimits {
        self.limits
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ServerAdaptiveFeedbackDiagnostics {
    pub(crate) eligible_query_count: u64,
    pub(crate) record_success_count: u64,
    pub(crate) record_error_count: u64,
    pub(crate) capacity_rejection_count: u64,
    pub(crate) schema_rotation_count: u64,
    pub(crate) incomplete_report_count: u64,
    pub(crate) counter_overflowed: bool,
    pub(crate) last_record_outcome: Option<AdaptiveEvidenceRecordOutcome>,
    pub(crate) last_record_error: Option<AdaptiveEvidenceRecordError>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServerAdaptiveFeedbackInspection {
    pub(crate) diagnostics: ServerAdaptiveFeedbackDiagnostics,
    pub(crate) progress: AdaptiveEvidenceProgressToken,
    pub(crate) health: AdaptiveEvidencePoolHealth,
    pub(crate) pool: AdaptiveEvidencePoolInspection,
}

/// Runtime aggregation owned by exactly one current Database execution owner.
pub(crate) struct ServerAdaptiveFeedbackRuntime {
    pool: AdaptiveEvidencePool,
    diagnostics: ServerAdaptiveFeedbackDiagnostics,
}

impl ServerAdaptiveFeedbackRuntime {
    pub(crate) const fn new(config: ServerAdaptiveFeedbackConfig) -> Self {
        Self {
            pool: AdaptiveEvidencePool::new(config.limits()),
            diagnostics: ServerAdaptiveFeedbackDiagnostics {
                eligible_query_count: 0,
                record_success_count: 0,
                record_error_count: 0,
                capacity_rejection_count: 0,
                schema_rotation_count: 0,
                incomplete_report_count: 0,
                counter_overflowed: false,
                last_record_outcome: None,
                last_record_error: None,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn inspection(&self) -> ServerAdaptiveFeedbackInspection {
        let pool = self.pool.inspection();
        ServerAdaptiveFeedbackInspection {
            diagnostics: self.diagnostics,
            progress: self.pool.progress_token(),
            health: pool.health,
            pool,
        }
    }

    fn increment(&mut self, counter: fn(&mut ServerAdaptiveFeedbackDiagnostics) -> &mut u64) {
        let value = counter(&mut self.diagnostics);
        match value.checked_add(1) {
            Some(next) => *value = next,
            None => self.diagnostics.counter_overflowed = true,
        }
    }

    fn record_successful_query(&mut self, report: &netbadb_core::ExecutionFeedbackReport) {
        self.increment(|diagnostics| &mut diagnostics.eligible_query_count);
        if report.incomplete || report.overflowed {
            self.increment(|diagnostics| &mut diagnostics.incomplete_report_count);
        }
        match self.pool.record_execution_feedback(report) {
            Ok(recorded) => {
                self.increment(|diagnostics| &mut diagnostics.record_success_count);
                self.diagnostics.last_record_outcome = Some(recorded.outcome);
                self.diagnostics.last_record_error = None;
                if matches!(
                    recorded.outcome,
                    AdaptiveEvidenceRecordOutcome::SchemaRotated
                        | AdaptiveEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection
                ) {
                    self.increment(|diagnostics| &mut diagnostics.schema_rotation_count);
                }
                if matches!(
                    recorded.outcome,
                    AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection
                        | AdaptiveEvidenceRecordOutcome::SchemaRotatedWithCapacityRejection
                ) {
                    self.increment(|diagnostics| &mut diagnostics.capacity_rejection_count);
                }
            }
            Err(error) => {
                // Admission is telemetry-only. A failed call is deliberately
                // neither retried nor followed by clear/rotation.
                self.increment(|diagnostics| &mut diagnostics.record_error_count);
                self.diagnostics.last_record_outcome = None;
                self.diagnostics.last_record_error = Some(error);
            }
        }
    }
}

/// Executes one already-authorized prepared Core statement, optionally
/// capturing the exact successful autocommit query execution.
pub(crate) fn execute_prepared_with_optional_server_feedback(
    runtime: Option<&mut ServerAdaptiveFeedbackRuntime>,
    session: &mut DatabaseSession,
    database: &mut Database,
    prepared: &PreparedStatement,
    values: &[ScalarValue],
) -> Result<ExecutionResult, DatabaseError> {
    let Some(runtime) = runtime else {
        return session.execute_prepared(database, prepared, values);
    };
    if session.has_explicit_transaction() || !prepared.description().is_query {
        return session.execute_prepared(database, prepared, values);
    }

    let executed = database.execute_prepared_with_feedback(prepared, values)?;
    if let Some(report) = executed.feedback.query_report() {
        runtime.record_successful_query(report);
    } else {
        // The typed description and Core result should agree. If they do not,
        // preserve the successful user result and classify the missing report
        // as telemetry failure rather than inventing evidence.
        runtime.increment(|diagnostics| &mut diagnostics.record_error_count);
    }
    Ok(executed.result)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use netbadb_core::{ColumnarProjectionSpec, DatabaseCoordinatorConfig, TableStorageCreateSpec};
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    use super::*;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn fixture(name: &str, global: bool) -> (PathBuf, Database) {
        let suffix = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "netbadb-server-feedback-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create feedback fixture root");
        let source = root.join("source");
        let table = TableDef::new(
            TableId(51_501),
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
        let coordinator = DatabaseCoordinatorConfig::new(root.join("coordinator"));
        let coordinator = if global {
            coordinator.with_global_visibility()
        } else {
            coordinator
        };
        let mut database = Database::create_catalog(
            root.join("catalog"),
            vec![TableStorageCreateSpec::heap(&source, table)],
            Some(coordinator),
        )
        .expect("create feedback database");
        database
            .execute("INSERT INTO events (id, category) VALUES (1, 7)")
            .expect("seed feedback database");
        (root, database)
    }

    fn cleanup(root: PathBuf, database: Database) {
        database.close().expect("close feedback database");
        fs::remove_dir_all(root).expect("remove feedback fixture root");
    }

    #[test]
    fn shared_bridge_captures_only_successful_autocommit_queries() {
        let (root, mut database) = fixture("eligibility", true);
        let mut session = DatabaseSession::default();
        let mut runtime = ServerAdaptiveFeedbackRuntime::new(ServerAdaptiveFeedbackConfig::new(
            AdaptiveEvidencePoolLimits::default(),
        ));
        let query = database
            .prepare_statement(
                "SELECT id FROM events WHERE category = $1",
                &[Some(PhysicalType::Int64)],
            )
            .expect("prepare query");
        let mutation = database
            .prepare_statement(
                "INSERT INTO events (id, category) VALUES ($1, $2)",
                &[Some(PhysicalType::Int64), Some(PhysicalType::Int64)],
            )
            .expect("prepare mutation");

        let ordinary = session
            .execute_prepared(&mut database, &query, &[ScalarValue::Int64(7)])
            .expect("execute ordinary comparison query");

        let result = execute_prepared_with_optional_server_feedback(
            Some(&mut runtime),
            &mut session,
            &mut database,
            &query,
            &[ScalarValue::Int64(7)],
        )
        .expect("execute captured query");
        assert_eq!(result, ordinary);
        assert_eq!(runtime.inspection().progress.recorded_reports, 1);

        execute_prepared_with_optional_server_feedback(
            Some(&mut runtime),
            &mut session,
            &mut database,
            &mutation,
            &[ScalarValue::Int64(2), ScalarValue::Int64(8)],
        )
        .expect("execute ordinary mutation");
        assert_eq!(runtime.inspection().progress.recorded_reports, 1);

        session
            .begin(&mut database, None)
            .expect("begin transaction");
        execute_prepared_with_optional_server_feedback(
            Some(&mut runtime),
            &mut session,
            &mut database,
            &query,
            &[ScalarValue::Int64(7)],
        )
        .expect("execute transaction query");
        session.rollback().expect("rollback transaction");
        assert_eq!(runtime.inspection().progress.recorded_reports, 1);

        assert!(
            execute_prepared_with_optional_server_feedback(
                Some(&mut runtime),
                &mut session,
                &mut database,
                &query,
                &[ScalarValue::Text("wrong".into())],
            )
            .is_err()
        );
        let inspected = runtime.inspection();
        assert_eq!(inspected.diagnostics.eligible_query_count, 1);
        assert_eq!(inspected.diagnostics.record_success_count, 1);
        assert_eq!(inspected.diagnostics.record_error_count, 0);
        cleanup(root, database);
    }

    #[test]
    fn telemetry_rejection_preserves_query_success_and_worker_runtime() {
        let (root, mut database) = fixture("legacy-local", false);
        let mut session = DatabaseSession::default();
        let mut runtime = ServerAdaptiveFeedbackRuntime::new(ServerAdaptiveFeedbackConfig::new(
            AdaptiveEvidencePoolLimits::default(),
        ));
        let query = database
            .prepare_statement("SELECT id FROM events", &[])
            .expect("prepare query");
        assert!(
            database
                .current_database_snapshot()
                .expect("inspect local visibility")
                .is_none()
        );

        for _ in 0..2 {
            let result = execute_prepared_with_optional_server_feedback(
                Some(&mut runtime),
                &mut session,
                &mut database,
                &query,
                &[],
            )
            .expect("query survives telemetry rejection");
            assert!(matches!(result, ExecutionResult::Query(_)));
        }
        let inspected = runtime.inspection();
        assert_eq!(inspected.progress.recorded_reports, 0);
        assert_eq!(inspected.diagnostics.eligible_query_count, 2);
        assert_eq!(inspected.diagnostics.record_success_count, 0);
        assert_eq!(inspected.diagnostics.record_error_count, 2);
        assert_eq!(inspected.health, AdaptiveEvidencePoolHealth::Healthy);
        assert!(
            database
                .current_database_snapshot()
                .expect("capture does not enable global visibility")
                .is_none()
        );
        cleanup(root, database);
    }

    #[test]
    fn disabled_bridge_uses_ordinary_execution_without_a_runtime() {
        let (root, mut database) = fixture("disabled", true);
        let mut session = DatabaseSession::default();
        let query = database
            .prepare_statement("SELECT id FROM events", &[])
            .expect("prepare query");
        let result = execute_prepared_with_optional_server_feedback(
            None,
            &mut session,
            &mut database,
            &query,
            &[],
        )
        .expect("ordinary query");
        assert!(matches!(result, ExecutionResult::Query(_)));
        cleanup(root, database);
    }

    #[test]
    fn prepared_ddl_bypasses_feedback_capture() {
        let (root, mut database) = fixture("ddl", true);
        let mut session = DatabaseSession::default();
        let mut runtime = ServerAdaptiveFeedbackRuntime::new(ServerAdaptiveFeedbackConfig::new(
            AdaptiveEvidencePoolLimits::default(),
        ));
        let ddl = session
            .prepare(&database, "CREATE INDEX events_id_idx ON events (id)", &[])
            .expect("prepare DDL");
        let result = session
            .execute_sql_prepared_with_optional_server_feedback(
                Some(&mut runtime),
                &mut database,
                &ddl,
            )
            .expect("execute ordinary DDL");
        assert_eq!(result, ExecutionResult::AffectedRows(0));
        assert_eq!(runtime.inspection().progress.recorded_reports, 0);
        assert_eq!(
            runtime.inspection().diagnostics,
            ServerAdaptiveFeedbackDiagnostics::default()
        );
        cleanup(root, database);
    }

    #[test]
    fn target_capacity_rejection_is_observable_but_not_query_fatal() {
        let (root, mut database) = fixture("capacity", true);
        let mut transaction = database.begin_transaction().expect("begin capacity seed");
        for id in 2..=512_i64 {
            database
                .insert_into_in(
                    TableId(51_501),
                    &mut transaction,
                    &[ScalarValue::Int64(id), ScalarValue::Int64(id % 8)],
                )
                .expect("insert capacity seed row");
        }
        transaction.commit().expect("commit capacity seed");
        database
            .build_columnar_projection(ColumnarProjectionSpec::new(
                TableId(51_501),
                root.join("projection"),
                vec![ColumnId(1)],
            ))
            .expect("build columnar projection");
        let limits = AdaptiveEvidencePoolLimits {
            max_target_windows: 0,
            ..AdaptiveEvidencePoolLimits::default()
        };
        let mut runtime =
            ServerAdaptiveFeedbackRuntime::new(ServerAdaptiveFeedbackConfig::new(limits));
        let mut session = DatabaseSession::default();
        let query = database
            .prepare_statement("SELECT id FROM events", &[])
            .expect("prepare columnar query");

        let result = execute_prepared_with_optional_server_feedback(
            Some(&mut runtime),
            &mut session,
            &mut database,
            &query,
            &[],
        )
        .expect("capacity rejection cannot fail query");
        assert!(matches!(result, ExecutionResult::Query(_)));
        let inspected = runtime.inspection();
        assert_eq!(inspected.progress.recorded_reports, 1);
        assert_eq!(inspected.diagnostics.record_success_count, 1);
        assert_eq!(inspected.diagnostics.capacity_rejection_count, 1);
        assert_eq!(
            inspected.diagnostics.last_record_outcome,
            Some(AdaptiveEvidenceRecordOutcome::RecordedWithCapacityRejection)
        );
        assert_eq!(
            inspected.health,
            AdaptiveEvidencePoolHealth::RotationRecommended
        );
        assert_eq!(inspected.pool.target_capacity_rejections, 1);
        assert_eq!(inspected.pool.window_epoch.0, 0);
        cleanup(root, database);
    }

    #[test]
    fn runtime_restart_is_empty_and_diagnostic_overflow_saturates() {
        let config = ServerAdaptiveFeedbackConfig::new(AdaptiveEvidencePoolLimits::default());
        let mut previous = ServerAdaptiveFeedbackRuntime::new(config);
        previous.diagnostics.eligible_query_count = u64::MAX;
        previous.increment(|diagnostics| &mut diagnostics.eligible_query_count);
        assert_eq!(previous.diagnostics.eligible_query_count, u64::MAX);
        assert!(previous.diagnostics.counter_overflowed);

        let reopened = ServerAdaptiveFeedbackRuntime::new(config).inspection();
        assert_eq!(reopened.progress.recorded_reports, 0);
        assert_eq!(reopened.progress.window_epoch.0, 0);
        assert_eq!(
            reopened.diagnostics,
            ServerAdaptiveFeedbackDiagnostics::default()
        );
    }
}
