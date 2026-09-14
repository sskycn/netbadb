//! Offline local catalog and statement inspection CLI.

mod json;

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

use netbadb_sdk::inspection::{render_catalog, render_statement};
use netbadb_sdk::{Database, DatabaseError};
use netbadb_server::{
    ManifestError, OperatorAdaptiveModeV4, OperatorClientError, OperatorErrorCodeV4,
    OperatorPhysicalColumnarApplyOutcomeV4, OperatorPhysicalColumnarDesignModeV4,
    OperatorPhysicalDesignDecisionV4, OperatorPhysicalDesignNoActionReasonV4,
    OperatorPhysicalDesignRecommendationsV4, OperatorPhysicalIndexApplyOutcomeV4,
    OperatorRemoteErrorV4, OperatorSchedulerDelayClassV4, OperatorSchedulerFaultV4,
    OperatorSchedulerGateV4, OperatorStatusV4, ServerConfig, ServerOperatorClient,
};

const ROOT_HELP: &str = "Usage:\n  netbadb inspect <catalog|statement> [options]\n  netbadb operator <status|rotate-evidence|reset-faulted-scheduler|physical-design> [options]\n\nUse `netbadb inspect --help` or `netbadb operator --help` for commands.\n";
const INSPECT_HELP: &str = "Usage:\n  netbadb inspect catalog --manifest <server.json> [--format text|json]\n  netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n";
const CATALOG_HELP: &str = "Usage: netbadb inspect catalog --manifest <server.json> [--format text|json]\n\nInspects the complete offline local catalog.\n";
const STATEMENT_HELP: &str = "Usage: netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n\nCompiles and inspects one statement without executing it.\n";
const OPERATOR_HELP: &str = "Usage:\n  netbadb operator status --manifest <server.json>\n  netbadb operator rotate-evidence --manifest <server.json> --expected-window-epoch <epoch>\n  netbadb operator reset-faulted-scheduler --manifest <server.json>\n  netbadb operator physical-design recommendations --manifest <server.json>\n  netbadb operator physical-design rotate-evidence --manifest <server.json> --expected-evidence-epoch <epoch>\n  netbadb operator physical-design apply-index --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id> --index-name <name>\n  netbadb operator physical-design apply-columnar --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id>... --mode <snapshot|incremental> --placement-key <key>\n";
const OPERATOR_STATUS_HELP: &str = "Usage: netbadb operator status --manifest <server.json>\n\nReads bounded live Adaptive and Physical Design status over NBOP v4.\n";
const OPERATOR_ROTATE_HELP: &str = "Usage: netbadb operator rotate-evidence --manifest <server.json> --expected-window-epoch <epoch>\n\nConditionally rotates the live evidence window. The expected epoch is required and is never inferred.\n";
const OPERATOR_RESET_HELP: &str = "Usage: netbadb operator reset-faulted-scheduler --manifest <server.json>\n\nAcknowledges and resets only a genuinely faulted scheduler.\n";
const OPERATOR_PHYSICAL_DESIGN_HELP: &str = "Usage:\n  netbadb operator physical-design recommendations --manifest <server.json>\n  netbadb operator physical-design rotate-evidence --manifest <server.json> --expected-evidence-epoch <epoch>\n  netbadb operator physical-design apply-index --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id> --index-name <name>\n  netbadb operator physical-design apply-columnar --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id>... --mode <snapshot|incremental> --placement-key <key>\n";
const OPERATOR_PHYSICAL_DESIGN_RECOMMENDATIONS_HELP: &str = "Usage: netbadb operator physical-design recommendations --manifest <server.json>\n\nReads current-inventory physical-design advice without applying it.\n";
const OPERATOR_PHYSICAL_DESIGN_ROTATE_HELP: &str = "Usage: netbadb operator physical-design rotate-evidence --manifest <server.json> --expected-evidence-epoch <epoch>\n\nConditionally rotates design evidence. The expected epoch is required and is never inferred.\n";
const OPERATOR_PHYSICAL_DESIGN_APPLY_HELP: &str = "Usage: netbadb operator physical-design apply-index --manifest <server.json> --expected-runtime-token <32-lowercase-hex> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id> --index-name <name>\n\nExplicitly approves one exact current physical-index candidate. No value is inferred or refreshed and the mutation is never retried automatically.\n";
const OPERATOR_PHYSICAL_DESIGN_APPLY_COLUMNAR_HELP: &str = "Usage: netbadb operator physical-design apply-columnar --manifest <server.json> --expected-runtime-token <32-lowercase-hex> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id>... --mode <snapshot|incremental> --placement-key <key>\n\nExplicitly approves one exact ordered Columnar candidate and logical placement. No path, token, epoch, mode, or candidate is inferred.\n";

/// Parses and runs one CLI invocation and returns its complete stdout after
/// the requested operation reaches a definitive outcome.
pub fn run_cli(arguments: impl IntoIterator<Item = OsString>) -> Result<String, CliError> {
    match parse_args(arguments)? {
        Action::Help(topic) => Ok(topic.text().to_owned()),
        Action::Version => Ok(format!("netbadb {}\n", env!("CARGO_PKG_VERSION"))),
        Action::Catalog { manifest, format } => inspect(manifest, |database| {
            let catalog = database
                .inspect_catalog()
                .map_err(InspectionFailure::Database)?;
            match format {
                OutputFormat::Text => Ok(render_catalog(&catalog)),
                OutputFormat::Json => {
                    json::render_catalog(&catalog).map_err(InspectionFailure::Json)
                }
            }
        }),
        Action::Statement {
            manifest,
            source,
            format,
        } => {
            // Read SQL input before manifest validation or database recovery.
            let source = source.read()?;
            inspect(manifest, |database| {
                let statement = database
                    .inspect_statement(&source)
                    .map_err(InspectionFailure::Database)?;
                match format {
                    OutputFormat::Text => Ok(render_statement(&statement)),
                    OutputFormat::Json => {
                        json::render_statement(&statement).map_err(InspectionFailure::Json)
                    }
                }
            })
        }
        Action::OperatorStatus { manifest } => {
            let status = run_operator(manifest, |client| client.status())?;
            Ok(render_operator_status(&status))
        }
        Action::OperatorRotate {
            manifest,
            expected_window_epoch,
        } => {
            let rotation = run_operator(manifest, |client| {
                client.rotate_evidence(expected_window_epoch)
            })?;
            Ok(format!(
                "evidence window rotated: previous epoch {}, new epoch {}\n",
                rotation.previous_window_epoch, rotation.new_window_epoch
            ))
        }
        Action::OperatorReset { manifest } => {
            run_operator(manifest, |client| client.reset_faulted_scheduler())?;
            Ok("adaptive scheduler reset\n".into())
        }
        Action::OperatorPhysicalDesignRecommendations { manifest } => {
            let recommendations =
                run_operator(manifest, |client| client.physical_design_recommendations())?;
            Ok(render_physical_design_recommendations(&recommendations))
        }
        Action::OperatorPhysicalDesignRotate {
            manifest,
            expected_evidence_epoch,
        } => {
            let rotation = run_operator(manifest, |client| {
                client.rotate_physical_design_evidence(expected_evidence_epoch)
            })?;
            Ok(format!(
                "physical-design evidence rotated: previous epoch {}, new epoch {}\n",
                rotation.previous_epoch, rotation.new_epoch
            ))
        }
        Action::OperatorPhysicalDesignApply {
            manifest,
            expected_runtime_token,
            expected_evidence_epoch,
            table_id,
            column_id,
            index_name,
        } => {
            let apply = run_operator_apply(manifest, |client| {
                client.apply_physical_index(
                    expected_runtime_token,
                    expected_evidence_epoch,
                    table_id,
                    column_id,
                    index_name,
                )
            })?;
            Ok(render_physical_index_apply(&apply))
        }
        Action::OperatorPhysicalDesignApplyColumnar {
            manifest,
            expected_runtime_token,
            expected_evidence_epoch,
            table_id,
            columns,
            mode,
            placement_key,
        } => {
            let apply = run_operator_apply(manifest, |client| {
                client.apply_physical_columnar(
                    expected_runtime_token,
                    expected_evidence_epoch,
                    table_id,
                    columns,
                    mode,
                    placement_key,
                )
            })?;
            Ok(render_physical_columnar_apply(&apply))
        }
    }
}

fn run_operator_apply<T>(
    manifest: PathBuf,
    operation: impl FnOnce(&ServerOperatorClient<'_>) -> Result<T, OperatorClientError>,
) -> Result<T, CliError> {
    let config = ServerConfig::from_manifest_path(manifest).map_err(OperationalError::Manifest)?;
    let operator = config.operator_config().ok_or(OperationalError::Operator(
        OperatorClientError::OperatorNotConfigured,
    ))?;
    operation(&ServerOperatorClient::new(operator))
        .map_err(classify_operator_apply_error)
        .map_err(Into::into)
}

fn classify_operator_apply_error(error: OperatorClientError) -> OperationalError {
    match &error {
        OperatorClientError::MutationOutcomeUncertain { .. }
        | OperatorClientError::Protocol(_)
        | OperatorClientError::RequestIdMismatch { .. }
        | OperatorClientError::UnexpectedResult => {
            OperationalError::OperatorApplyOutcomeUncertain(error)
        }
        _ => OperationalError::Operator(error),
    }
}

fn run_operator<T>(
    manifest: PathBuf,
    operation: impl FnOnce(&ServerOperatorClient<'_>) -> Result<T, OperatorClientError>,
) -> Result<T, CliError> {
    let config = ServerConfig::from_manifest_path(manifest).map_err(OperationalError::Manifest)?;
    let operator = config.operator_config().ok_or(OperationalError::Operator(
        OperatorClientError::OperatorNotConfigured,
    ))?;
    operation(&ServerOperatorClient::new(operator))
        .map_err(OperationalError::Operator)
        .map_err(Into::into)
}

fn render_operator_status(status: &OperatorStatusV4) -> String {
    let mut output = String::new();
    match &status.adaptive {
        None => output.push_str("Adaptive: disabled\n"),
        Some(adaptive) => {
            let mode = match adaptive.mode {
                OperatorAdaptiveModeV4::FeedbackOnly => "feedback-only",
                OperatorAdaptiveModeV4::Driven => "driven",
            };
            let feedback = &adaptive.feedback;
            output.push_str(&format!(
                "Adaptive: {mode}\nadaptive evidence window epoch: {}\nadaptive schema generation: {}\nadaptive recorded reports: {}\nadaptive eligible queries: {}\nadaptive record successes: {}\nadaptive record errors: {}\n",
                feedback.window_epoch,
                feedback
                    .schema_generation
                    .map_or_else(|| "none".into(), |generation| generation.to_string()),
                feedback.recorded_reports,
                feedback.eligible_query_count,
                feedback.record_success_count,
                feedback.record_error_count,
            ));
            if let Some(driver) = &adaptive.driver {
                output.push_str("scheduler gate: ");
                output.push_str(render_scheduler_gate(driver.scheduler_gate));
                output.push('\n');
                output.push_str(&format!(
                    "scheduler ticks: {}\nscheduler runs: {}\nscheduler errors: {}\ntick pending: {}\n",
                    driver.scheduler_tick_count,
                    driver.scheduler_ran_count,
                    driver.scheduler_error_count,
                    driver.tick_pending,
                ));
            }
        }
    }
    match &status.physical_design {
        None => output.push_str("Physical Design: disabled\n"),
        Some(design) => output.push_str(&format!(
            "Physical Design: enabled\nPhysical index apply: {}\nPhysical Columnar apply: {}\nAllowed Columnar modes: {}\nRuntime token: {}\nRuntime token purpose: stale-request guard, not a credential\ndesign evidence epoch: {}\ndesign recorded reports: {}\nindex candidate count: {}\ncolumnar candidate count: {}\ndesign evidence truncated: {}\ndesign evidence incomplete: {}\n",
            if design.physical_index_apply.enabled { "enabled" } else { "disabled" },
            if design.physical_columnar_apply.enabled { "enabled" } else { "disabled" },
            render_columnar_modes(
                design.physical_columnar_apply.allow_snapshot,
                design.physical_columnar_apply.allow_incremental,
            ),
            design
                .physical_index_apply
                .runtime_token
                .as_deref()
                .or(design.physical_columnar_apply.runtime_token.as_deref())
                .unwrap_or("none"),
            design.evidence.epoch,
            design.evidence.recorded_reports,
            design.evidence.index_candidate_count,
            design.evidence.columnar_candidate_count,
            design.evidence.truncated,
            design.evidence.incomplete,
        )),
    }
    output
}

const fn render_columnar_modes(allow_snapshot: bool, allow_incremental: bool) -> &'static str {
    match (allow_snapshot, allow_incremental) {
        (true, true) => "snapshot, incremental",
        (true, false) => "snapshot",
        (false, true) => "incremental",
        (false, false) => "none",
    }
}

fn render_physical_design_recommendations(
    recommendations: &OperatorPhysicalDesignRecommendationsV4,
) -> String {
    let report = &recommendations.report;
    let mut output = format!(
        "Physical-design approval token: {}\nRuntime token: {}\nEvidence epoch: {}\nphysical-design evidence epoch: {}\nschema generation: {}\nG range: {}..={}\nrecorded reports: {}\ndiscarded incomplete reports: {}\noverflowed: {}\nincomplete: {}\n",
        if recommendations.runtime_token.is_some() {
            "present"
        } else {
            "absent"
        },
        recommendations.runtime_token.as_deref().unwrap_or("none"),
        report.evidence_epoch,
        report.evidence_epoch,
        report.schema_generation,
        report.first_global_commit_seq,
        report.last_global_commit_seq,
        report.recorded_reports,
        report.discarded_incomplete_reports,
        report.overflowed,
        report.incomplete,
    );
    for candidate in &report.index_candidates {
        output.push_str(&format!(
            "Index candidate: TableId({}), ColumnId({})\n  observed reports: {}\n  distinct shapes: {}\n  observed actual scan work: {}\n  rows examined: {}\n  point reports: {}\n  range reports: {}\n  decision: {}\n",
            candidate.table_id,
            candidate.column_id,
            candidate.evidence.report_count,
            candidate.evidence.distinct_query_shapes,
            candidate.evidence.total_actual_scan_work_units,
            candidate.evidence.total_rows_examined,
            candidate.point_report_count,
            candidate.range_report_count,
            render_design_decision(candidate.decision),
        ));
    }
    for candidate in &report.columnar_candidates {
        let columns = candidate
            .columns
            .iter()
            .map(|column| format!("ColumnId({column})"))
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!(
            "Columnar candidate: TableId({}), columns [{}]\n  observed reports: {}\n  distinct shapes: {}\n  observed actual scan work: {}\n  rows examined: {}\n  decision: {}\n",
            candidate.table_id,
            columns,
            candidate.evidence.report_count,
            candidate.evidence.distinct_query_shapes,
            candidate.evidence.total_actual_scan_work_units,
            candidate.evidence.total_rows_examined,
            render_design_decision(candidate.decision),
        ));
    }
    output
}

fn render_physical_index_apply(
    apply: &netbadb_server::OperatorPhysicalIndexApplyResultV4,
) -> String {
    match apply.outcome {
        OperatorPhysicalIndexApplyOutcomeV4::Created { index_id } => format!(
            "physical index created: IndexId({index_id}), TableId({}), ColumnId({}), name {}\n",
            apply.table_id, apply.column_id, apply.index_name
        ),
        OperatorPhysicalIndexApplyOutcomeV4::AlreadyApplied { index_id } => format!(
            "physical index already applied: IndexId({index_id}), TableId({}), ColumnId({}), name {}\n",
            apply.table_id, apply.column_id, apply.index_name
        ),
        OperatorPhysicalIndexApplyOutcomeV4::AlreadyCovered => format!(
            "physical index already covered: TableId({}), ColumnId({}), name {}; no new index was created because current physical state already covers the candidate.\n",
            apply.table_id, apply.column_id, apply.index_name
        ),
    }
}

fn render_physical_columnar_apply(
    apply: &netbadb_server::OperatorPhysicalColumnarApplyResultV4,
) -> String {
    let columns = apply
        .columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mode = match apply.mode {
        OperatorPhysicalColumnarDesignModeV4::Snapshot => "snapshot",
        OperatorPhysicalColumnarDesignModeV4::Incremental => "incremental",
    };
    match apply.outcome {
        OperatorPhysicalColumnarApplyOutcomeV4::Created { projection_id } => format!(
            "created projection {projection_id} (TableId({}), columns [{columns}], mode {mode}, placement key {})\n",
            apply.table_id, apply.placement_key
        ),
        OperatorPhysicalColumnarApplyOutcomeV4::AlreadyApplied { projection_id } => format!(
            "already applied as projection {projection_id} (TableId({}), columns [{columns}], mode {mode}, placement key {})\n",
            apply.table_id, apply.placement_key
        ),
        OperatorPhysicalColumnarApplyOutcomeV4::AlreadyCovered => format!(
            "already covered; no projection created (TableId({}), columns [{columns}], mode {mode}, placement key {})\n",
            apply.table_id, apply.placement_key
        ),
    }
}

const fn render_design_decision(decision: OperatorPhysicalDesignDecisionV4) -> &'static str {
    match decision {
        OperatorPhysicalDesignDecisionV4::Recommend {} => "recommend",
        OperatorPhysicalDesignDecisionV4::NoAction { reason } => match reason {
            OperatorPhysicalDesignNoActionReasonV4::BelowMinimumReports => {
                "no_action: below_minimum_reports"
            }
            OperatorPhysicalDesignNoActionReasonV4::BelowMinimumShapeDiversity => {
                "no_action: below_minimum_shape_diversity"
            }
            OperatorPhysicalDesignNoActionReasonV4::BelowMinimumActualWork => {
                "no_action: below_minimum_actual_work"
            }
            OperatorPhysicalDesignNoActionReasonV4::ExistingDesignCovers => {
                "no_action: existing_design_covers"
            }
            OperatorPhysicalDesignNoActionReasonV4::UnsupportedCurrentLayout => {
                "no_action: unsupported_current_layout"
            }
            OperatorPhysicalDesignNoActionReasonV4::IncompleteEvidence => {
                "no_action: incomplete_evidence"
            }
            OperatorPhysicalDesignNoActionReasonV4::CurrentProjectionUnavailable => {
                "no_action: current_projection_unavailable"
            }
            OperatorPhysicalDesignNoActionReasonV4::RecommendationLimitReached => {
                "no_action: recommendation_limit_reached"
            }
        },
    }
}

const fn render_scheduler_gate(gate: OperatorSchedulerGateV4) -> &'static str {
    match gate {
        OperatorSchedulerGateV4::Open {
            delay_class: OperatorSchedulerDelayClassV4::Normal,
        } => "open (normal)",
        OperatorSchedulerGateV4::Open {
            delay_class: OperatorSchedulerDelayClassV4::Idle,
        } => "open (idle)",
        OperatorSchedulerGateV4::Open {
            delay_class: OperatorSchedulerDelayClassV4::NoProgress,
        } => "open (no_progress)",
        OperatorSchedulerGateV4::AwaitingTrialProgress { .. } => "awaiting_trial_progress",
        OperatorSchedulerGateV4::AwaitingEvidenceRenewal { .. } => "awaiting_evidence_renewal",
        OperatorSchedulerGateV4::Faulted {
            fault: OperatorSchedulerFaultV4::MaintenanceEnvelopeExceeded,
        } => "faulted (maintenance_envelope_exceeded)",
        OperatorSchedulerGateV4::Faulted {
            fault: OperatorSchedulerFaultV4::StepFailed,
        } => "faulted (step_failed)",
        OperatorSchedulerGateV4::Faulted {
            fault: OperatorSchedulerFaultV4::ConsumptionOverflow,
        } => "faulted (consumption_overflow)",
    }
}

fn inspect(
    manifest: PathBuf,
    operation: impl FnOnce(&Database) -> Result<String, InspectionFailure>,
) -> Result<String, CliError> {
    let config = ServerConfig::from_manifest_path(manifest).map_err(OperationalError::Manifest)?;
    let tables = config
        .tables()
        .iter()
        .map(|entry| (entry.path.clone(), entry.table.clone()))
        .collect();
    let database = Database::open_tables(tables).map_err(OperationalError::Open)?;
    let result = operation(&database);
    let close = database.close();
    match (result, close) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(primary), Ok(())) => Err(OperationalError::Inspection(primary).into()),
        (Ok(_), Err(close)) => Err(OperationalError::Close(close).into()),
        (Err(primary), Err(close)) => {
            Err(OperationalError::InspectionAndClose { primary, close }.into())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Help(HelpTopic),
    Version,
    Catalog {
        manifest: PathBuf,
        format: OutputFormat,
    },
    Statement {
        manifest: PathBuf,
        source: SqlSource,
        format: OutputFormat,
    },
    OperatorStatus {
        manifest: PathBuf,
    },
    OperatorRotate {
        manifest: PathBuf,
        expected_window_epoch: u64,
    },
    OperatorReset {
        manifest: PathBuf,
    },
    OperatorPhysicalDesignRecommendations {
        manifest: PathBuf,
    },
    OperatorPhysicalDesignRotate {
        manifest: PathBuf,
        expected_evidence_epoch: u64,
    },
    OperatorPhysicalDesignApply {
        manifest: PathBuf,
        expected_runtime_token: String,
        expected_evidence_epoch: u64,
        table_id: u64,
        column_id: u32,
        index_name: String,
    },
    OperatorPhysicalDesignApplyColumnar {
        manifest: PathBuf,
        expected_runtime_token: String,
        expected_evidence_epoch: u64,
        table_id: u64,
        columns: Vec<u32>,
        mode: OperatorPhysicalColumnarDesignModeV4,
        placement_key: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelpTopic {
    Root,
    Inspect,
    Catalog,
    Statement,
    Operator,
    OperatorStatus,
    OperatorRotate,
    OperatorReset,
    OperatorPhysicalDesign,
    OperatorPhysicalDesignRecommendations,
    OperatorPhysicalDesignRotate,
    OperatorPhysicalDesignApply,
    OperatorPhysicalDesignApplyColumnar,
}

impl HelpTopic {
    const fn text(self) -> &'static str {
        match self {
            Self::Root => ROOT_HELP,
            Self::Inspect => INSPECT_HELP,
            Self::Catalog => CATALOG_HELP,
            Self::Statement => STATEMENT_HELP,
            Self::Operator => OPERATOR_HELP,
            Self::OperatorStatus => OPERATOR_STATUS_HELP,
            Self::OperatorRotate => OPERATOR_ROTATE_HELP,
            Self::OperatorReset => OPERATOR_RESET_HELP,
            Self::OperatorPhysicalDesign => OPERATOR_PHYSICAL_DESIGN_HELP,
            Self::OperatorPhysicalDesignRecommendations => {
                OPERATOR_PHYSICAL_DESIGN_RECOMMENDATIONS_HELP
            }
            Self::OperatorPhysicalDesignRotate => OPERATOR_PHYSICAL_DESIGN_ROTATE_HELP,
            Self::OperatorPhysicalDesignApply => OPERATOR_PHYSICAL_DESIGN_APPLY_HELP,
            Self::OperatorPhysicalDesignApplyColumnar => {
                OPERATOR_PHYSICAL_DESIGN_APPLY_COLUMNAR_HELP
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlSource {
    Inline(String),
    File(PathBuf),
}

impl SqlSource {
    fn read(self) -> Result<String, CliError> {
        match self {
            Self::Inline(source) => Ok(source),
            Self::File(path) => std::fs::read_to_string(&path)
                .map_err(|source| OperationalError::ReadSql { path, source }.into()),
        }
    }
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Action, UsageError> {
    let mut arguments = arguments.into_iter();
    let first = arguments.next().ok_or(UsageError::CommandRequired)?;
    if first == "--help" || first == "-h" {
        return no_extra(arguments, Action::Help(HelpTopic::Root));
    }
    if first == "--version" || first == "-V" {
        return no_extra(arguments, Action::Version);
    }
    if first == "operator" {
        return parse_operator(arguments);
    }
    if first != "inspect" {
        return Err(UsageError::UnknownArgument(first));
    }

    let subcommand = arguments.next().ok_or(UsageError::InspectCommandRequired)?;
    if subcommand == "--help" || subcommand == "-h" {
        return no_extra(arguments, Action::Help(HelpTopic::Inspect));
    }
    if subcommand == "catalog" {
        return parse_catalog(arguments);
    }
    if subcommand == "statement" {
        return parse_statement(arguments);
    }
    Err(UsageError::UnknownInspectCommand(subcommand))
}

fn parse_operator(mut arguments: impl Iterator<Item = OsString>) -> Result<Action, UsageError> {
    let subcommand = arguments
        .next()
        .ok_or(UsageError::OperatorCommandRequired)?;
    if subcommand == "--help" || subcommand == "-h" {
        return no_extra(arguments, Action::Help(HelpTopic::Operator));
    }
    match subcommand.to_str() {
        Some("status") => parse_operator_simple(arguments, HelpTopic::OperatorStatus, |manifest| {
            Action::OperatorStatus { manifest }
        }),
        Some("rotate-evidence") => parse_operator_rotate(arguments),
        Some("reset-faulted-scheduler") => {
            parse_operator_simple(arguments, HelpTopic::OperatorReset, |manifest| {
                Action::OperatorReset { manifest }
            })
        }
        Some("physical-design") => parse_operator_physical_design(arguments),
        _ => Err(UsageError::UnknownOperatorCommand(subcommand)),
    }
}

fn parse_operator_physical_design(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let subcommand = arguments
        .next()
        .ok_or(UsageError::PhysicalDesignCommandRequired)?;
    if subcommand == "--help" || subcommand == "-h" {
        return no_extra(arguments, Action::Help(HelpTopic::OperatorPhysicalDesign));
    }
    match subcommand.to_str() {
        Some("recommendations") => parse_operator_simple(
            arguments,
            HelpTopic::OperatorPhysicalDesignRecommendations,
            |manifest| Action::OperatorPhysicalDesignRecommendations { manifest },
        ),
        Some("rotate-evidence") => parse_operator_physical_design_rotate(arguments),
        Some("apply-index") => parse_operator_physical_design_apply(arguments),
        Some("apply-columnar") => parse_operator_physical_design_apply_columnar(arguments),
        _ => Err(UsageError::UnknownPhysicalDesignCommand(subcommand)),
    }
}

fn parse_operator_physical_design_apply_columnar(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let mut manifest = None;
    let mut runtime_token = None;
    let mut evidence_epoch = None;
    let mut table_id = None;
    let mut columns = Vec::new();
    let mut mode = None;
    let mut placement_key = None;
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            if manifest.is_none()
                && runtime_token.is_none()
                && evidence_epoch.is_none()
                && table_id.is_none()
                && columns.is_empty()
                && mode.is_none()
                && placement_key.is_none()
            {
                return no_extra(
                    arguments,
                    Action::Help(HelpTopic::OperatorPhysicalDesignApplyColumnar),
                );
            }
            return Err(UsageError::UnexpectedArgument(argument));
        }
        match argument.to_str() {
            Some("--manifest") => set_once(
                &mut manifest,
                PathBuf::from(required_value(&mut arguments, "--manifest")?),
                "--manifest",
            )?,
            Some("--expected-runtime-token") => {
                let value = required_utf8(&mut arguments, "--expected-runtime-token")?;
                set_once(&mut runtime_token, value, "--expected-runtime-token")?;
            }
            Some("--expected-evidence-epoch") => {
                let raw = required_value(&mut arguments, "--expected-evidence-epoch")?;
                set_once(
                    &mut evidence_epoch,
                    parse_u64(raw, UsageError::InvalidEvidenceEpoch)?,
                    "--expected-evidence-epoch",
                )?;
            }
            Some("--table-id") => {
                let raw = required_value(&mut arguments, "--table-id")?;
                set_once(
                    &mut table_id,
                    parse_u64(raw, UsageError::InvalidTableId)?,
                    "--table-id",
                )?;
            }
            Some("--column-id") => {
                let raw = required_value(&mut arguments, "--column-id")?;
                let value = raw
                    .to_str()
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or(UsageError::InvalidColumnId(raw))?;
                columns.push(value);
            }
            Some("--mode") => {
                let value = required_utf8(&mut arguments, "--mode")?;
                let parsed = match value.as_str() {
                    "snapshot" => OperatorPhysicalColumnarDesignModeV4::Snapshot,
                    "incremental" => OperatorPhysicalColumnarDesignModeV4::Incremental,
                    _ => return Err(UsageError::InvalidColumnarMode(value)),
                };
                set_once(&mut mode, parsed, "--mode")?;
            }
            Some("--placement-key") => {
                let value = required_utf8(&mut arguments, "--placement-key")?;
                set_once(&mut placement_key, value, "--placement-key")?;
            }
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    if columns.is_empty() {
        return Err(UsageError::ColumnIdsRequired);
    }
    Ok(Action::OperatorPhysicalDesignApplyColumnar {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        expected_runtime_token: runtime_token.ok_or(UsageError::ExpectedRuntimeTokenRequired)?,
        expected_evidence_epoch: evidence_epoch.ok_or(UsageError::ExpectedEvidenceEpochRequired)?,
        table_id: table_id.ok_or(UsageError::TableIdRequired)?,
        columns,
        mode: mode.ok_or(UsageError::ColumnarModeRequired)?,
        placement_key: placement_key.ok_or(UsageError::PlacementKeyRequired)?,
    })
}

fn parse_operator_physical_design_apply(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let mut manifest = None;
    let mut runtime_token = None;
    let mut evidence_epoch = None;
    let mut table_id = None;
    let mut column_id = None;
    let mut index_name = None;
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            if manifest.is_none()
                && runtime_token.is_none()
                && evidence_epoch.is_none()
                && table_id.is_none()
                && column_id.is_none()
                && index_name.is_none()
            {
                return no_extra(
                    arguments,
                    Action::Help(HelpTopic::OperatorPhysicalDesignApply),
                );
            }
            return Err(UsageError::UnexpectedArgument(argument));
        }
        match argument.to_str() {
            Some("--manifest") => set_once(
                &mut manifest,
                PathBuf::from(required_value(&mut arguments, "--manifest")?),
                "--manifest",
            )?,
            Some("--expected-runtime-token") => {
                let value = required_utf8(&mut arguments, "--expected-runtime-token")?;
                set_once(&mut runtime_token, value, "--expected-runtime-token")?;
            }
            Some("--expected-evidence-epoch") => {
                let raw = required_value(&mut arguments, "--expected-evidence-epoch")?;
                let value = parse_u64(raw, UsageError::InvalidEvidenceEpoch)?;
                set_once(&mut evidence_epoch, value, "--expected-evidence-epoch")?;
            }
            Some("--table-id") => {
                let raw = required_value(&mut arguments, "--table-id")?;
                let value = parse_u64(raw, UsageError::InvalidTableId)?;
                set_once(&mut table_id, value, "--table-id")?;
            }
            Some("--column-id") => {
                let raw = required_value(&mut arguments, "--column-id")?;
                let value = raw
                    .to_str()
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or(UsageError::InvalidColumnId(raw))?;
                set_once(&mut column_id, value, "--column-id")?;
            }
            Some("--index-name") => {
                let value = required_utf8(&mut arguments, "--index-name")?;
                set_once(&mut index_name, value, "--index-name")?;
            }
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    Ok(Action::OperatorPhysicalDesignApply {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        expected_runtime_token: runtime_token.ok_or(UsageError::ExpectedRuntimeTokenRequired)?,
        expected_evidence_epoch: evidence_epoch.ok_or(UsageError::ExpectedEvidenceEpochRequired)?,
        table_id: table_id.ok_or(UsageError::TableIdRequired)?,
        column_id: column_id.ok_or(UsageError::ColumnIdRequired)?,
        index_name: index_name.ok_or(UsageError::IndexNameRequired)?,
    })
}

fn required_utf8(
    arguments: &mut impl Iterator<Item = OsString>,
    option: &'static str,
) -> Result<String, UsageError> {
    required_value(arguments, option)?
        .into_string()
        .map_err(|_| UsageError::ValueMustBeUtf8(option))
}

fn parse_u64(raw: OsString, invalid: fn(OsString) -> UsageError) -> Result<u64, UsageError> {
    raw.to_str()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| invalid(raw))
}

fn parse_operator_physical_design_rotate(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let mut manifest = None;
    let mut expected = None;
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            if manifest.is_none() && expected.is_none() {
                return no_extra(
                    arguments,
                    Action::Help(HelpTopic::OperatorPhysicalDesignRotate),
                );
            }
            return Err(UsageError::UnexpectedArgument(argument));
        }
        match argument.to_str() {
            Some("--manifest") => set_once(
                &mut manifest,
                PathBuf::from(required_value(&mut arguments, "--manifest")?),
                "--manifest",
            )?,
            Some("--expected-evidence-epoch") => {
                let raw = required_value(&mut arguments, "--expected-evidence-epoch")?;
                let parsed = raw
                    .to_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or(UsageError::InvalidEvidenceEpoch(raw))?;
                set_once(&mut expected, parsed, "--expected-evidence-epoch")?;
            }
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    Ok(Action::OperatorPhysicalDesignRotate {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        expected_evidence_epoch: expected.ok_or(UsageError::ExpectedEvidenceEpochRequired)?,
    })
}

fn parse_operator_simple(
    mut arguments: impl Iterator<Item = OsString>,
    help: HelpTopic,
    action: impl FnOnce(PathBuf) -> Action,
) -> Result<Action, UsageError> {
    let first = arguments.next().ok_or(UsageError::ManifestRequired)?;
    if first == "--help" || first == "-h" {
        return no_extra(arguments, Action::Help(help));
    }
    if first != "--manifest" {
        return Err(UsageError::UnknownArgument(first));
    }
    let manifest = PathBuf::from(required_value(&mut arguments, "--manifest")?);
    no_extra(arguments, action(manifest))
}

fn parse_operator_rotate(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let mut manifest = None;
    let mut expected = None;
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            if manifest.is_none() && expected.is_none() {
                return no_extra(arguments, Action::Help(HelpTopic::OperatorRotate));
            }
            return Err(UsageError::UnexpectedArgument(argument));
        }
        match argument.to_str() {
            Some("--manifest") => set_once(
                &mut manifest,
                PathBuf::from(required_value(&mut arguments, "--manifest")?),
                "--manifest",
            )?,
            Some("--expected-window-epoch") => {
                let raw = required_value(&mut arguments, "--expected-window-epoch")?;
                let parsed = raw
                    .to_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or(UsageError::InvalidWindowEpoch(raw))?;
                set_once(&mut expected, parsed, "--expected-window-epoch")?;
            }
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    Ok(Action::OperatorRotate {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        expected_window_epoch: expected.ok_or(UsageError::ExpectedWindowEpochRequired)?,
    })
}

fn parse_catalog(arguments: impl Iterator<Item = OsString>) -> Result<Action, UsageError> {
    let mut arguments = arguments.peekable();
    if matches!(arguments.peek(), Some(argument) if argument == "--help" || argument == "-h") {
        arguments.next();
        return no_extra(arguments, Action::Help(HelpTopic::Catalog));
    }
    let mut manifest = None;
    let mut format = None;
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--manifest") => set_once(
                &mut manifest,
                PathBuf::from(required_value(&mut arguments, "--manifest")?),
                "--manifest",
            )?,
            Some("--format") => set_once(
                &mut format,
                parse_format(required_value(&mut arguments, "--format")?)?,
                "--format",
            )?,
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    Ok(Action::Catalog {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        format: format.unwrap_or(OutputFormat::Text),
    })
}

fn parse_statement(arguments: impl Iterator<Item = OsString>) -> Result<Action, UsageError> {
    let mut arguments = arguments.peekable();
    if matches!(arguments.peek(), Some(argument) if argument == "--help" || argument == "-h") {
        arguments.next();
        return no_extra(arguments, Action::Help(HelpTopic::Statement));
    }
    let mut manifest = None;
    let mut format = None;
    let mut inline_sql = None;
    let mut sql_file = None;
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--manifest") => set_once(
                &mut manifest,
                PathBuf::from(required_value(&mut arguments, "--manifest")?),
                "--manifest",
            )?,
            Some("--format") => set_once(
                &mut format,
                parse_format(required_value(&mut arguments, "--format")?)?,
                "--format",
            )?,
            Some("--sql") => {
                let value = required_value(&mut arguments, "--sql")?;
                let value = value.into_string().map_err(|_| UsageError::SqlMustBeUtf8)?;
                set_once(&mut inline_sql, value, "--sql")?;
            }
            Some("--sql-file") => set_once(
                &mut sql_file,
                PathBuf::from(required_value(&mut arguments, "--sql-file")?),
                "--sql-file",
            )?,
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    let source = match (inline_sql, sql_file) {
        (Some(source), None) => SqlSource::Inline(source),
        (None, Some(path)) => SqlSource::File(path),
        (None, None) => return Err(UsageError::SqlSourceRequired),
        (Some(_), Some(_)) => return Err(UsageError::SqlSourceConflict),
    };
    Ok(Action::Statement {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        source,
        format: format.unwrap_or(OutputFormat::Text),
    })
}

fn required_value(
    arguments: &mut impl Iterator<Item = OsString>,
    option: &'static str,
) -> Result<OsString, UsageError> {
    arguments.next().ok_or(UsageError::ValueRequired(option))
}

fn parse_format(value: OsString) -> Result<OutputFormat, UsageError> {
    match value.to_str() {
        Some("text") => Ok(OutputFormat::Text),
        Some("json") => Ok(OutputFormat::Json),
        _ => Err(UsageError::UnknownFormat(value)),
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, option: &'static str) -> Result<(), UsageError> {
    if slot.replace(value).is_some() {
        Err(UsageError::DuplicateOption(option))
    } else {
        Ok(())
    }
}

fn no_extra(
    mut arguments: impl Iterator<Item = OsString>,
    action: Action,
) -> Result<Action, UsageError> {
    match arguments.next() {
        Some(argument) => Err(UsageError::UnexpectedArgument(argument)),
        None => Ok(action),
    }
}

#[derive(Debug)]
enum InspectionFailure {
    Database(DatabaseError),
    Json(serde_json::Error),
}

impl fmt::Display for InspectionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::Json(error) => write!(formatter, "failed to render Inspection JSON v1: {error}"),
        }
    }
}

#[derive(Debug)]
enum OperationalError {
    ReadSql {
        path: PathBuf,
        source: std::io::Error,
    },
    Manifest(ManifestError),
    Open(DatabaseError),
    Inspection(InspectionFailure),
    Close(DatabaseError),
    InspectionAndClose {
        primary: InspectionFailure,
        close: DatabaseError,
    },
    Operator(OperatorClientError),
    OperatorApplyOutcomeUncertain(OperatorClientError),
}

impl fmt::Display for OperationalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadSql { path, source } => {
                write!(
                    formatter,
                    "failed to read SQL file `{}`: {source}",
                    path.display()
                )
            }
            Self::Manifest(error) => error.fmt(formatter),
            Self::Open(error) => write!(formatter, "failed to open database: {error}"),
            Self::Inspection(error) => write!(formatter, "inspection failed: {error}"),
            Self::Close(error) => write!(formatter, "failed to close database: {error}"),
            Self::InspectionAndClose { primary, close } => write!(
                formatter,
                "inspection failed: {primary}; additionally failed to close database: {close}"
            ),
            Self::Operator(OperatorClientError::Remote(OperatorRemoteErrorV4 {
                code: OperatorErrorCodeV4::ResponseTooLarge,
                ..
            })) => formatter.write_str(
                "operator recommendation response exceeds NBOP v4 payload limit; reduce physical-design evidence/recommendation cardinality in Manifest and restart",
            ),
            Self::Operator(OperatorClientError::Remote(OperatorRemoteErrorV4 {
                code: OperatorErrorCodeV4::PhysicalDesignRuntimeChanged,
                ..
            })) => formatter.write_str(
                "recommendation approval belonged to a previous daemon/operator runtime; fetch fresh recommendations before authorizing a new mutation",
            ),
            Self::Operator(error) => error.fmt(formatter),
            Self::OperatorApplyOutcomeUncertain(
                error @ OperatorClientError::MutationOutcomeUncertain {
                    recovery_required: true,
                    ..
                },
            ) => write!(
                formatter,
                "operator apply outcome is uncertain ({error}); the CLI did not retry or refresh its token or evidence epoch"
            ),
            Self::OperatorApplyOutcomeUncertain(error) => write!(
                formatter,
                "operator apply outcome is uncertain because no definitive NBOP result was received ({error}); re-run the same exact approval; the CLI did not retry or refresh its token or evidence epoch"
            ),
        }
    }
}

impl Error for OperationalError {}

#[derive(Debug)]
enum UsageError {
    CommandRequired,
    InspectCommandRequired,
    UnknownInspectCommand(OsString),
    OperatorCommandRequired,
    UnknownOperatorCommand(OsString),
    PhysicalDesignCommandRequired,
    UnknownPhysicalDesignCommand(OsString),
    ManifestRequired,
    SqlSourceRequired,
    SqlSourceConflict,
    SqlMustBeUtf8,
    ValueRequired(&'static str),
    DuplicateOption(&'static str),
    UnknownFormat(OsString),
    UnknownArgument(OsString),
    UnexpectedArgument(OsString),
    ExpectedWindowEpochRequired,
    InvalidWindowEpoch(OsString),
    ExpectedEvidenceEpochRequired,
    InvalidEvidenceEpoch(OsString),
    ExpectedRuntimeTokenRequired,
    TableIdRequired,
    ColumnIdRequired,
    IndexNameRequired,
    ColumnIdsRequired,
    ColumnarModeRequired,
    InvalidColumnarMode(String),
    PlacementKeyRequired,
    InvalidTableId(OsString),
    InvalidColumnId(OsString),
    ValueMustBeUtf8(&'static str),
}

impl fmt::Display for UsageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandRequired => formatter.write_str("a command is required"),
            Self::InspectCommandRequired => {
                formatter.write_str("inspect requires `catalog` or `statement`")
            }
            Self::UnknownInspectCommand(command) => write!(
                formatter,
                "unknown inspect command `{}`",
                command.to_string_lossy()
            ),
            Self::OperatorCommandRequired => formatter.write_str(
                "operator requires `status`, `rotate-evidence`, `reset-faulted-scheduler`, or `physical-design`",
            ),
            Self::UnknownOperatorCommand(command) => write!(
                formatter,
                "unknown operator command `{}`",
                command.to_string_lossy()
            ),
            Self::PhysicalDesignCommandRequired => formatter.write_str(
                "physical-design requires `recommendations`, `rotate-evidence`, `apply-index`, or `apply-columnar`",
            ),
            Self::UnknownPhysicalDesignCommand(command) => write!(
                formatter,
                "unknown physical-design command `{}`",
                command.to_string_lossy()
            ),
            Self::ManifestRequired => formatter.write_str("--manifest is required"),
            Self::SqlSourceRequired => {
                formatter.write_str("exactly one of --sql or --sql-file is required")
            }
            Self::SqlSourceConflict => {
                formatter.write_str("--sql and --sql-file cannot be used together")
            }
            Self::SqlMustBeUtf8 => formatter.write_str("--sql must be valid UTF-8"),
            Self::ValueRequired(option) => write!(formatter, "{option} requires a value"),
            Self::DuplicateOption(option) => {
                write!(formatter, "{option} may be specified only once")
            }
            Self::UnknownFormat(format) => write!(
                formatter,
                "unknown format `{}`; expected `text` or `json`",
                format.to_string_lossy()
            ),
            Self::UnknownArgument(argument) => write!(
                formatter,
                "unknown argument `{}`",
                argument.to_string_lossy()
            ),
            Self::UnexpectedArgument(argument) => write!(
                formatter,
                "unexpected additional argument `{}`",
                argument.to_string_lossy()
            ),
            Self::ExpectedWindowEpochRequired => {
                formatter.write_str("--expected-window-epoch is required")
            }
            Self::InvalidWindowEpoch(value) => write!(
                formatter,
                "invalid window epoch `{}`; expected an unsigned integer",
                value.to_string_lossy()
            ),
            Self::ExpectedEvidenceEpochRequired => {
                formatter.write_str("--expected-evidence-epoch is required")
            }
            Self::InvalidEvidenceEpoch(value) => write!(
                formatter,
                "invalid evidence epoch `{}`; expected an unsigned integer",
                value.to_string_lossy()
            ),
            Self::ExpectedRuntimeTokenRequired => {
                formatter.write_str("--expected-runtime-token is required")
            }
            Self::TableIdRequired => formatter.write_str("--table-id is required"),
            Self::ColumnIdRequired => formatter.write_str("--column-id is required"),
            Self::IndexNameRequired => formatter.write_str("--index-name is required"),
            Self::ColumnIdsRequired => formatter.write_str("at least one --column-id is required"),
            Self::ColumnarModeRequired => formatter.write_str("--mode is required"),
            Self::InvalidColumnarMode(value) => write!(
                formatter,
                "invalid Columnar mode `{value}`; expected `snapshot` or `incremental`"
            ),
            Self::PlacementKeyRequired => formatter.write_str("--placement-key is required"),
            Self::InvalidTableId(value) => write!(
                formatter,
                "invalid table ID `{}`; expected an unsigned integer",
                value.to_string_lossy()
            ),
            Self::InvalidColumnId(value) => write!(
                formatter,
                "invalid column ID `{}`; expected an unsigned 32-bit integer",
                value.to_string_lossy()
            ),
            Self::ValueMustBeUtf8(option) => write!(formatter, "{option} must be valid UTF-8"),
        }
    }
}

impl Error for UsageError {}

/// One CLI failure with a stable coarse exit-code classification.
#[derive(Debug)]
pub struct CliError(Box<CliErrorKind>);

#[derive(Debug)]
enum CliErrorKind {
    Usage(UsageError),
    Operational(OperationalError),
}

impl CliError {
    /// Returns 2 for usage failures and 1 for operational failures.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self.0.as_ref() {
            CliErrorKind::Usage(_) => 2,
            CliErrorKind::Operational(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            CliErrorKind::Usage(error) => error.fmt(formatter),
            CliErrorKind::Operational(error) => error.fmt(formatter),
        }
    }
}

impl Error for CliError {}

impl From<UsageError> for CliError {
    fn from(error: UsageError) -> Self {
        Self(Box::new(CliErrorKind::Usage(error)))
    }
}

impl From<OperationalError> for CliError {
    fn from(error: OperationalError) -> Self {
        Self(Box::new(CliErrorKind::Operational(error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbadb_server::{
        OperatorPhysicalDesignAdvisorReportV4, OperatorPhysicalDesignEvidenceSummaryV4,
        OperatorPhysicalIndexCandidateV4,
    };

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_help_version_and_catalog_options() {
        assert_eq!(
            parse_args(args(&["--help"])).unwrap(),
            Action::Help(HelpTopic::Root)
        );
        assert_eq!(
            parse_args(args(&["inspect", "--help"])).unwrap(),
            Action::Help(HelpTopic::Inspect)
        );
        assert_eq!(
            parse_args(args(&["inspect", "catalog", "--help"])).unwrap(),
            Action::Help(HelpTopic::Catalog)
        );
        assert_eq!(
            parse_args(args(&["inspect", "statement", "--help"])).unwrap(),
            Action::Help(HelpTopic::Statement)
        );
        assert_eq!(
            parse_args(args(&["operator", "--help"])).unwrap(),
            Action::Help(HelpTopic::Operator)
        );
        assert_eq!(
            parse_args(args(&["operator", "physical-design", "--help"])).unwrap(),
            Action::Help(HelpTopic::OperatorPhysicalDesign)
        );
        assert!(OPERATOR_HELP.contains("physical-design apply-columnar"));
        assert!(OPERATOR_PHYSICAL_DESIGN_HELP.contains("physical-design apply-columnar"));
        assert_eq!(parse_args(args(&["--version"])).unwrap(), Action::Version);
        assert_eq!(
            parse_args(args(&[
                "inspect",
                "catalog",
                "--format",
                "json",
                "--manifest",
                "server.json",
            ]))
            .unwrap(),
            Action::Catalog {
                manifest: PathBuf::from("server.json"),
                format: OutputFormat::Json,
            }
        );
    }

    #[test]
    fn statement_requires_exactly_one_sql_source() {
        assert!(matches!(
            parse_args(args(&["inspect", "statement", "--manifest", "server.json"])),
            Err(UsageError::SqlSourceRequired)
        ));
        assert!(matches!(
            parse_args(args(&[
                "inspect",
                "statement",
                "--manifest",
                "server.json",
                "--sql",
                "SELECT 1",
                "--sql-file",
                "query.sql",
            ])),
            Err(UsageError::SqlSourceConflict)
        ));
        assert_eq!(
            parse_args(args(&[
                "inspect",
                "statement",
                "--sql-file",
                "query.sql",
                "--manifest",
                "server.json",
            ]))
            .unwrap(),
            Action::Statement {
                manifest: PathBuf::from("server.json"),
                source: SqlSource::File(PathBuf::from("query.sql")),
                format: OutputFormat::Text,
            }
        );
    }

    #[test]
    fn rejects_unknown_duplicate_and_missing_option_values() {
        assert!(matches!(
            parse_args(args(&[
                "inspect",
                "catalog",
                "--format",
                "yaml",
                "--manifest",
                "m"
            ])),
            Err(UsageError::UnknownFormat(_))
        ));
        assert!(matches!(
            parse_args(args(&[
                "inspect",
                "catalog",
                "--manifest",
                "a",
                "--manifest",
                "b",
            ])),
            Err(UsageError::DuplicateOption("--manifest"))
        ));
        assert!(matches!(
            parse_args(args(&["inspect", "catalog", "--manifest"])),
            Err(UsageError::ValueRequired("--manifest"))
        ));
    }

    #[test]
    fn parses_operator_commands_and_requires_explicit_rotation_epoch() {
        assert_eq!(
            parse_args(args(&["operator", "status", "--manifest", "server.json"])).unwrap(),
            Action::OperatorStatus {
                manifest: PathBuf::from("server.json")
            }
        );
        assert_eq!(
            parse_args(args(&[
                "operator",
                "rotate-evidence",
                "--expected-window-epoch",
                "7",
                "--manifest",
                "server.json"
            ]))
            .unwrap(),
            Action::OperatorRotate {
                manifest: PathBuf::from("server.json"),
                expected_window_epoch: 7,
            }
        );
        assert_eq!(
            parse_args(args(&[
                "operator",
                "reset-faulted-scheduler",
                "--manifest",
                "server.json"
            ]))
            .unwrap(),
            Action::OperatorReset {
                manifest: PathBuf::from("server.json")
            }
        );
        assert!(matches!(
            parse_args(args(&[
                "operator",
                "rotate-evidence",
                "--manifest",
                "server.json"
            ])),
            Err(UsageError::ExpectedWindowEpochRequired)
        ));
        assert_eq!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "recommendations",
                "--manifest",
                "server.json"
            ]))
            .unwrap(),
            Action::OperatorPhysicalDesignRecommendations {
                manifest: PathBuf::from("server.json")
            }
        );
        assert_eq!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "rotate-evidence",
                "--expected-evidence-epoch",
                "9",
                "--manifest",
                "server.json"
            ]))
            .unwrap(),
            Action::OperatorPhysicalDesignRotate {
                manifest: PathBuf::from("server.json"),
                expected_evidence_epoch: 9,
            }
        );
        assert!(matches!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "rotate-evidence",
                "--manifest",
                "server.json"
            ])),
            Err(UsageError::ExpectedEvidenceEpochRequired)
        ));
        assert_eq!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "apply-index",
                "--manifest",
                "server.json",
                "--expected-runtime-token",
                "00112233445566778899aabbccddeeff",
                "--expected-evidence-epoch",
                "7",
                "--table-id",
                "1",
                "--column-id",
                "3",
                "--index-name",
                "idx_users_email",
            ]))
            .unwrap(),
            Action::OperatorPhysicalDesignApply {
                manifest: PathBuf::from("server.json"),
                expected_runtime_token: "00112233445566778899aabbccddeeff".into(),
                expected_evidence_epoch: 7,
                table_id: 1,
                column_id: 3,
                index_name: "idx_users_email".into(),
            }
        );
        assert!(matches!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "apply-index",
                "--manifest",
                "server.json",
            ])),
            Err(UsageError::ExpectedRuntimeTokenRequired)
        ));
    }

    #[test]
    fn recommendation_output_uses_canonical_ids_and_observed_work_only() {
        let output =
            render_physical_design_recommendations(&OperatorPhysicalDesignRecommendationsV4 {
                runtime_token: Some("00".repeat(16)),
                report: OperatorPhysicalDesignAdvisorReportV4 {
                    evidence_epoch: 7,
                    schema_generation: 8,
                    first_global_commit_seq: 9,
                    last_global_commit_seq: 10,
                    recorded_reports: 11,
                    discarded_incomplete_reports: 0,
                    overflowed: false,
                    incomplete: false,
                    index_candidates: vec![OperatorPhysicalIndexCandidateV4 {
                        table_id: 12,
                        column_id: 13,
                        point_report_count: 14,
                        range_report_count: 15,
                        evidence: OperatorPhysicalDesignEvidenceSummaryV4 {
                            report_count: 16,
                            distinct_query_shapes: 17,
                            total_actual_scan_work_units: 18,
                            total_rows_examined: 19,
                            overflowed: false,
                            incomplete: false,
                            truncated: false,
                        },
                        decision: OperatorPhysicalDesignDecisionV4::NoAction {
                            reason: OperatorPhysicalDesignNoActionReasonV4::ExistingDesignCovers,
                        },
                    }],
                    columnar_candidates: Vec::new(),
                },
            });
        assert!(output.contains("TableId(12), ColumnId(13)"));
        assert!(output.contains("observed actual scan work: 18"));
        assert!(output.contains("no_action: existing_design_covers"));
        for forbidden in ["CREATE INDEX", "estimated_savings", "speedup", "roi"] {
            assert!(!output.contains(forbidden));
        }
    }

    #[test]
    fn recommendation_output_reports_only_token_presence() {
        let render = |token_present: bool| {
            render_physical_design_recommendations(&OperatorPhysicalDesignRecommendationsV4 {
                runtime_token: token_present.then(|| "11".repeat(16)),
                report: OperatorPhysicalDesignAdvisorReportV4 {
                    evidence_epoch: 1,
                    schema_generation: 1,
                    first_global_commit_seq: 1,
                    last_global_commit_seq: 1,
                    recorded_reports: 1,
                    discarded_incomplete_reports: 0,
                    overflowed: false,
                    incomplete: false,
                    index_candidates: Vec::new(),
                    columnar_candidates: Vec::new(),
                },
            })
        };

        for (present, expected) in [(true, "present"), (false, "absent")] {
            let output = render(present);
            assert!(output.contains(&format!("Physical-design approval token: {expected}\n")));
            assert!(!output.contains("Physical index apply:"));
            assert!(!output.contains("Physical Columnar apply:"));
            assert_eq!(output.contains("Runtime token: none"), !present);
        }
    }

    #[test]
    fn apply_error_classification_is_conservative_after_dispatch() {
        let uncertain = classify_operator_apply_error(OperatorClientError::RequestIdMismatch {
            expected: 1,
            received: 2,
        });
        assert!(matches!(
            uncertain,
            OperationalError::OperatorApplyOutcomeUncertain(_)
        ));
        let rendered = uncertain.to_string();
        assert!(rendered.contains("same exact approval"));
        assert!(!rendered.contains("without creating"));

        let definite =
            classify_operator_apply_error(OperatorClientError::Remote(OperatorRemoteErrorV4 {
                code: OperatorErrorCodeV4::ServerStopped,
                message: "command was not sent".into(),
            }));
        assert!(matches!(definite, OperationalError::Operator(_)));

        let transport = classify_operator_apply_error(OperatorClientError::RequestIdMismatch {
            expected: 1,
            received: 2,
        });
        assert!(matches!(
            transport,
            OperationalError::OperatorApplyOutcomeUncertain(_)
        ));
    }

    #[test]
    fn apply_output_distinguishes_all_stable_success_outcomes() {
        for (outcome, expected) in [
            (
                OperatorPhysicalIndexApplyOutcomeV4::Created { index_id: 7 },
                "physical index created",
            ),
            (
                OperatorPhysicalIndexApplyOutcomeV4::AlreadyApplied { index_id: 7 },
                "physical index already applied",
            ),
            (
                OperatorPhysicalIndexApplyOutcomeV4::AlreadyCovered,
                "no new index was created because current physical state already covers",
            ),
        ] {
            let output =
                render_physical_index_apply(&netbadb_server::OperatorPhysicalIndexApplyResultV4 {
                    table_id: 1,
                    column_id: 3,
                    index_name: "idx_users_email".into(),
                    outcome,
                });
            assert!(output.contains(expected));
            assert!(!output.contains("CREATE INDEX"));
        }
    }
}
