//! Offline local catalog and statement inspection CLI.

use netbadb_server::{
    OperatorPhysicalDesignMutationAdmissionConstraintV7,
    OperatorPhysicalDesignMutationAdmissionDimensionV7,
    OperatorPhysicalDesignMutationAdmissionModeV7,
    OperatorPhysicalDesignMutationAdmissionRejectionV7,
};

mod json;

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

use netbadb_sdk::inspection::{render_catalog, render_statement};
use netbadb_sdk::{Database, DatabaseError};
use netbadb_server::{
    ManifestError, OperatorAdaptiveModeV7, OperatorClientError, OperatorErrorCodeV7,
    OperatorPhysicalColumnarApplyOutcomeV7, OperatorPhysicalColumnarDesignModeV7,
    OperatorPhysicalDesignDecisionV7, OperatorPhysicalDesignMutationReceiptCursorV7,
    OperatorPhysicalDesignMutationReceiptOutcomeV7, OperatorPhysicalDesignMutationReceiptPageV7,
    OperatorPhysicalDesignMutationReceiptRefV7, OperatorPhysicalDesignMutationReceiptSourceV7,
    OperatorPhysicalDesignMutationReceiptStatusV7, OperatorPhysicalDesignMutationReceiptTargetV7,
    OperatorPhysicalDesignNoActionReasonV7, OperatorPhysicalDesignRecommendationsV7,
    OperatorPhysicalIndexApplyOutcomeV7, OperatorRemoteErrorV7, OperatorSchedulerDelayClassV7,
    OperatorSchedulerFaultV7, OperatorSchedulerGateV7, OperatorStatusV7, ServerConfig,
    ServerOperatorClient,
};

const ROOT_HELP: &str = "Usage:\n  netbadb inspect <catalog|statement> [options]\n  netbadb operator <status|rotate-evidence|reset-faulted-scheduler|physical-design> [options]\n\nUse `netbadb inspect --help` or `netbadb operator --help` for commands.\n";
const INSPECT_HELP: &str = "Usage:\n  netbadb inspect catalog --manifest <server.json> [--format text|json]\n  netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n";
const CATALOG_HELP: &str = "Usage: netbadb inspect catalog --manifest <server.json> [--format text|json]\n\nInspects the complete offline local catalog.\n";
const STATEMENT_HELP: &str = "Usage: netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n\nCompiles and inspects one statement without executing it.\n";
const OPERATOR_HELP: &str = "Usage:\n  netbadb operator status --manifest <server.json>\n  netbadb operator rotate-evidence --manifest <server.json> --expected-window-epoch <epoch>\n  netbadb operator reset-faulted-scheduler --manifest <server.json>\n  netbadb operator physical-design recommendations --manifest <server.json>\n  netbadb operator physical-design rotate-evidence --manifest <server.json> --expected-evidence-epoch <epoch>\n  netbadb operator physical-design receipts status --manifest <server.json>\n  netbadb operator physical-design receipts list --manifest <server.json> [--limit <1..128>] [--after-journal-incarnation <hex> --after-receipt-id <id>]\n  netbadb operator physical-design apply-index --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id> --index-name <name>\n  netbadb operator physical-design apply-columnar --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id>... --mode <snapshot|incremental> --placement-key <key>\n";
const OPERATOR_STATUS_HELP: &str = "Usage: netbadb operator status --manifest <server.json>\n\nReads bounded live Adaptive and Physical Design status over NBOP v7.\n";
const OPERATOR_ROTATE_HELP: &str = "Usage: netbadb operator rotate-evidence --manifest <server.json> --expected-window-epoch <epoch>\n\nConditionally rotates the live evidence window. The expected epoch is required and is never inferred.\n";
const OPERATOR_RESET_HELP: &str = "Usage: netbadb operator reset-faulted-scheduler --manifest <server.json>\n\nAcknowledges and resets only a genuinely faulted scheduler.\n";
const OPERATOR_PHYSICAL_DESIGN_HELP: &str = "Usage:\n  netbadb operator physical-design recommendations --manifest <server.json>\n  netbadb operator physical-design rotate-evidence --manifest <server.json> --expected-evidence-epoch <epoch>\n  netbadb operator physical-design receipts <status|list> [options]\n  netbadb operator physical-design apply-index --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id> --index-name <name>\n  netbadb operator physical-design apply-columnar --manifest <server.json> --expected-runtime-token <token> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id>... --mode <snapshot|incremental> --placement-key <key>\n";
const OPERATOR_PHYSICAL_DESIGN_RECOMMENDATIONS_HELP: &str = "Usage: netbadb operator physical-design recommendations --manifest <server.json>\n\nReads current-inventory physical-design advice without applying it.\n";
const OPERATOR_PHYSICAL_DESIGN_ROTATE_HELP: &str = "Usage: netbadb operator physical-design rotate-evidence --manifest <server.json> --expected-evidence-epoch <epoch>\n\nConditionally rotates design evidence. The expected epoch is required and is never inferred.\n";
const OPERATOR_PHYSICAL_DESIGN_APPLY_HELP: &str = "Usage: netbadb operator physical-design apply-index --manifest <server.json> --expected-runtime-token <32-lowercase-hex> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id> --index-name <name>\n\nExplicitly approves one exact current physical-index candidate. No value is inferred or refreshed and the mutation is never retried automatically.\n";
const OPERATOR_PHYSICAL_DESIGN_APPLY_COLUMNAR_HELP: &str = "Usage: netbadb operator physical-design apply-columnar --manifest <server.json> --expected-runtime-token <32-lowercase-hex> --expected-evidence-epoch <epoch> --table-id <id> --column-id <id>... --mode <snapshot|incremental> --placement-key <key>\n\nExplicitly approves one exact ordered Columnar candidate and logical placement. No path, token, epoch, mode, or candidate is inferred.\n";
const OPERATOR_PHYSICAL_DESIGN_RECEIPTS_HELP: &str = "Usage:\n  netbadb operator physical-design receipts status --manifest <server.json>\n  netbadb operator physical-design receipts list --manifest <server.json> [--limit <1..128>] [--after-journal-incarnation <32-lowercase-hex> --after-receipt-id <nonzero-id>]\n";
const OPERATOR_PHYSICAL_DESIGN_RECEIPTS_STATUS_HELP: &str =
    "Usage: netbadb operator physical-design receipts status --manifest <server.json>\n";
const OPERATOR_PHYSICAL_DESIGN_RECEIPTS_LIST_HELP: &str = "Usage: netbadb operator physical-design receipts list --manifest <server.json> [--limit <1..128>] [--after-journal-incarnation <32-lowercase-hex> --after-receipt-id <nonzero-id>]\n";

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
        Action::OperatorPhysicalDesignReceiptStatus { manifest } => {
            let status = run_operator(manifest, |client| {
                client.physical_design_mutation_receipt_status()
            })?;
            Ok(render_physical_design_receipt_status(&status))
        }
        Action::OperatorPhysicalDesignReceipts {
            manifest,
            after,
            limit,
        } => {
            let page = run_operator(manifest, |client| {
                client.physical_design_mutation_receipts(after, limit)
            })?;
            Ok(render_physical_design_receipts(&page))
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

fn render_operator_status(status: &OperatorStatusV7) -> String {
    let mut output = String::new();
    match &status.adaptive {
        None => output.push_str("Adaptive: disabled\n"),
        Some(adaptive) => {
            let mode = match adaptive.mode {
                OperatorAdaptiveModeV7::FeedbackOnly => "feedback-only",
                OperatorAdaptiveModeV7::Driven => "driven",
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
            "Physical Design: enabled\nPhysical index apply: {}\nPhysical Columnar apply: {}\nPhysical design receipt read: {}\nAllowed Columnar modes: {}\nRuntime token: {}\nRuntime token purpose: stale-request guard, not a credential\ndesign evidence epoch: {}\ndesign recorded reports: {}\nindex candidate count: {}\ncolumnar candidate count: {}\ndesign evidence truncated: {}\ndesign evidence incomplete: {}\n",
            if design.physical_index_apply.enabled { "enabled" } else { "disabled" },
            if design.physical_columnar_apply.enabled { "enabled" } else { "disabled" },
            if design.physical_design_mutation_receipts.read_enabled { "enabled" } else { "disabled" },
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
    if let Some(design) = &status.physical_design {
        render_admission_mode(&mut output, "Index", design.physical_index_apply.admission);
        render_admission_mode(
            &mut output,
            "Columnar Snapshot",
            design.physical_columnar_apply.snapshot_admission,
        );
        render_admission_mode(
            &mut output,
            "Columnar Incremental",
            design.physical_columnar_apply.incremental_admission,
        );
    }
    output
}

fn render_admission_mode(
    output: &mut String,
    label: &str,
    mode: OperatorPhysicalDesignMutationAdmissionModeV7,
) {
    match mode {
        OperatorPhysicalDesignMutationAdmissionModeV7::Unadmitted {} => {
            output.push_str(&format!("{label} admission: unadmitted\n"));
        }
        OperatorPhysicalDesignMutationAdmissionModeV7::ComponentLimits { policy } => {
            output.push_str(&format!("{label} admission: component limits\n"));
            for (dimension, constraint) in [
                ("source work units", policy.source_work_units),
                ("source read bytes", policy.source_read_bytes),
                ("prerequisite work units", policy.prerequisite_work_units),
                ("prerequisite read bytes", policy.prerequisite_read_bytes),
                ("prerequisite write bytes", policy.prerequisite_write_bytes),
                ("output write bytes", policy.output_write_bytes),
            ] {
                let value = match constraint {
                    OperatorPhysicalDesignMutationAdmissionConstraintV7::Unconstrained {} => {
                        "unconstrained".into()
                    }
                    OperatorPhysicalDesignMutationAdmissionConstraintV7::AtMost { maximum } => {
                        format!("at most {maximum}")
                    }
                };
                output.push_str(&format!("  {dimension}: {value}\n"));
            }
        }
    }
}

const fn render_admission_dimension(
    dimension: OperatorPhysicalDesignMutationAdmissionDimensionV7,
) -> &'static str {
    match dimension {
        OperatorPhysicalDesignMutationAdmissionDimensionV7::SourceWorkUnits => "source_work_units",
        OperatorPhysicalDesignMutationAdmissionDimensionV7::SourceReadBytes => "source_read_bytes",
        OperatorPhysicalDesignMutationAdmissionDimensionV7::PrerequisiteWorkUnits => {
            "prerequisite_work_units"
        }
        OperatorPhysicalDesignMutationAdmissionDimensionV7::PrerequisiteReadBytes => {
            "prerequisite_read_bytes"
        }
        OperatorPhysicalDesignMutationAdmissionDimensionV7::PrerequisiteWriteBytes => {
            "prerequisite_write_bytes"
        }
        OperatorPhysicalDesignMutationAdmissionDimensionV7::OutputWriteBytes => {
            "output_write_bytes"
        }
    }
}

fn render_admission_rejection(
    rejection: &OperatorPhysicalDesignMutationAdmissionRejectionV7,
) -> String {
    match rejection {
        OperatorPhysicalDesignMutationAdmissionRejectionV7::RequiredBoundNotProven {
            dimension,
        } => format!(
            "admission rejected: {} has no proven current conservative bound",
            render_admission_dimension(*dimension)
        ),
        OperatorPhysicalDesignMutationAdmissionRejectionV7::LimitExceeded {
            dimension,
            conservative_bound,
            maximum,
        } => format!(
            "admission rejected: {} conservative bound {conservative_bound} exceeds configured maximum {maximum}",
            render_admission_dimension(*dimension)
        ),
        OperatorPhysicalDesignMutationAdmissionRejectionV7::InspectionFailed {} => {
            "admission rejected: current mutation-work inspection failed before mutation".into()
        }
        OperatorPhysicalDesignMutationAdmissionRejectionV7::RecoveryRequired {} => {
            "admission rejected: current mutation-work inspection requires restart/reopen before retry"
                .into()
        }
    }
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
    recommendations: &OperatorPhysicalDesignRecommendationsV7,
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
    apply: &netbadb_server::OperatorPhysicalIndexApplyResultV7,
) -> String {
    let mut output = match apply.outcome {
        OperatorPhysicalIndexApplyOutcomeV7::Created { index_id } => format!(
            "physical index created: IndexId({index_id}), TableId({}), ColumnId({}), name {}\n",
            apply.table_id, apply.column_id, apply.index_name
        ),
        OperatorPhysicalIndexApplyOutcomeV7::AlreadyApplied { index_id } => format!(
            "physical index already applied: IndexId({index_id}), TableId({}), ColumnId({}), name {}\n",
            apply.table_id, apply.column_id, apply.index_name
        ),
        OperatorPhysicalIndexApplyOutcomeV7::AlreadyCovered => format!(
            "physical index already covered: TableId({}), ColumnId({}), name {}; no new index was created because current physical state already covers the candidate.\n",
            apply.table_id, apply.column_id, apply.index_name
        ),
    };
    if let Some(receipt) = &apply.receipt {
        output.push_str(&format!("receipt: {}\n", render_receipt_ref(receipt)));
    }
    output
}

fn render_physical_columnar_apply(
    apply: &netbadb_server::OperatorPhysicalColumnarApplyResultV7,
) -> String {
    let columns = apply
        .columns
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mode = match apply.mode {
        OperatorPhysicalColumnarDesignModeV7::Snapshot => "snapshot",
        OperatorPhysicalColumnarDesignModeV7::Incremental => "incremental",
    };
    let mut output = match apply.outcome {
        OperatorPhysicalColumnarApplyOutcomeV7::Created { projection_id } => format!(
            "created projection {projection_id} (TableId({}), columns [{columns}], mode {mode}, placement key {})\n",
            apply.table_id, apply.placement_key
        ),
        OperatorPhysicalColumnarApplyOutcomeV7::AlreadyApplied { projection_id } => format!(
            "already applied as projection {projection_id} (TableId({}), columns [{columns}], mode {mode}, placement key {})\n",
            apply.table_id, apply.placement_key
        ),
        OperatorPhysicalColumnarApplyOutcomeV7::AlreadyCovered => format!(
            "already covered; no projection created (TableId({}), columns [{columns}], mode {mode}, placement key {})\n",
            apply.table_id, apply.placement_key
        ),
    };
    if let Some(receipt) = &apply.receipt {
        output.push_str(&format!("receipt: {}\n", render_receipt_ref(receipt)));
    }
    output
}

fn render_receipt_ref(receipt: &OperatorPhysicalDesignMutationReceiptRefV7) -> String {
    format!("{}/{}", receipt.journal_incarnation, receipt.receipt_id)
}

fn render_physical_design_receipt_status(
    status: &OperatorPhysicalDesignMutationReceiptStatusV7,
) -> String {
    format!(
        "Journal incarnation: {}\nRecovery required: {}\nLatest receipt ID: {}\nMax page size: {}\n",
        status.journal_incarnation,
        status.recovery_required,
        status
            .latest_receipt_id
            .map_or_else(|| "none".into(), |id| id.to_string()),
        status.max_receipts_per_read,
    )
}

fn render_physical_design_receipts(page: &OperatorPhysicalDesignMutationReceiptPageV7) -> String {
    let mut output = String::new();
    for receipt in &page.receipts {
        let source = match receipt.source {
            OperatorPhysicalDesignMutationReceiptSourceV7::Programmatic => "programmatic",
            OperatorPhysicalDesignMutationReceiptSourceV7::LocalOperator => "local_operator",
        };
        let target = match &receipt.target {
            OperatorPhysicalDesignMutationReceiptTargetV7::Index {
                table_id,
                column_id,
                index_name,
            } => format!("index TableId({table_id}) ColumnId({column_id}) name {index_name}"),
            OperatorPhysicalDesignMutationReceiptTargetV7::Columnar {
                table_id,
                columns,
                mode,
                placement_key,
            } => format!(
                "columnar TableId({table_id}) columns [{}] mode {} placement key {placement_key}",
                columns
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                match mode {
                    OperatorPhysicalColumnarDesignModeV7::Snapshot => "snapshot",
                    OperatorPhysicalColumnarDesignModeV7::Incremental => "incremental",
                }
            ),
        };
        output.push_str(&format!(
            "Receipt: {}\n  source: {source}\n  evidence epoch: {}\n  target: {target}\n  outcome: {}\n",
            render_receipt_ref(&receipt.receipt),
            receipt.evidence_epoch,
            render_receipt_outcome(receipt.outcome),
        ));
    }
    output.push_str(&format!(
        "Next after: {}\n",
        page.next_after.as_ref().map_or_else(
            || "none".into(),
            |cursor| format!("{}/{}", cursor.journal_incarnation, cursor.receipt_id),
        )
    ));
    output
}

fn render_receipt_outcome(outcome: OperatorPhysicalDesignMutationReceiptOutcomeV7) -> String {
    match outcome {
        OperatorPhysicalDesignMutationReceiptOutcomeV7::Pending => "pending".into(),
        OperatorPhysicalDesignMutationReceiptOutcomeV7::CreatedIndex { index_id } => {
            format!("created_index IndexId({index_id})")
        }
        OperatorPhysicalDesignMutationReceiptOutcomeV7::CreatedColumnar { projection_id } => {
            format!("created_columnar ProjectionId({projection_id})")
        }
        OperatorPhysicalDesignMutationReceiptOutcomeV7::AlreadyAppliedIndex { index_id } => {
            format!("already_applied_index IndexId({index_id})")
        }
        OperatorPhysicalDesignMutationReceiptOutcomeV7::AlreadyAppliedColumnar {
            projection_id,
        } => format!("already_applied_columnar ProjectionId({projection_id})"),
        OperatorPhysicalDesignMutationReceiptOutcomeV7::AlreadyCovered => "already_covered".into(),
        OperatorPhysicalDesignMutationReceiptOutcomeV7::Rejected => "rejected".into(),
        OperatorPhysicalDesignMutationReceiptOutcomeV7::Failed => "failed".into(),
        OperatorPhysicalDesignMutationReceiptOutcomeV7::RecoveredAppliedIndex { index_id } => {
            format!("recovered_applied_index IndexId({index_id})")
        }
        OperatorPhysicalDesignMutationReceiptOutcomeV7::RecoveredAppliedColumnar {
            projection_id,
        } => format!("recovered_applied_columnar ProjectionId({projection_id})"),
        OperatorPhysicalDesignMutationReceiptOutcomeV7::RecoveredNotApplied => {
            "recovered_not_applied".into()
        }
        OperatorPhysicalDesignMutationReceiptOutcomeV7::RecoveredConflict => {
            "recovered_conflict".into()
        }
    }
}

const fn render_design_decision(decision: OperatorPhysicalDesignDecisionV7) -> &'static str {
    match decision {
        OperatorPhysicalDesignDecisionV7::Recommend {} => "recommend",
        OperatorPhysicalDesignDecisionV7::NoAction { reason } => match reason {
            OperatorPhysicalDesignNoActionReasonV7::BelowMinimumReports => {
                "no_action: below_minimum_reports"
            }
            OperatorPhysicalDesignNoActionReasonV7::BelowMinimumShapeDiversity => {
                "no_action: below_minimum_shape_diversity"
            }
            OperatorPhysicalDesignNoActionReasonV7::BelowMinimumActualWork => {
                "no_action: below_minimum_actual_work"
            }
            OperatorPhysicalDesignNoActionReasonV7::ExistingDesignCovers => {
                "no_action: existing_design_covers"
            }
            OperatorPhysicalDesignNoActionReasonV7::UnsupportedCurrentLayout => {
                "no_action: unsupported_current_layout"
            }
            OperatorPhysicalDesignNoActionReasonV7::IncompleteEvidence => {
                "no_action: incomplete_evidence"
            }
            OperatorPhysicalDesignNoActionReasonV7::CurrentProjectionUnavailable => {
                "no_action: current_projection_unavailable"
            }
            OperatorPhysicalDesignNoActionReasonV7::RecommendationLimitReached => {
                "no_action: recommendation_limit_reached"
            }
        },
    }
}

const fn render_scheduler_gate(gate: OperatorSchedulerGateV7) -> &'static str {
    match gate {
        OperatorSchedulerGateV7::Open {
            delay_class: OperatorSchedulerDelayClassV7::Normal,
        } => "open (normal)",
        OperatorSchedulerGateV7::Open {
            delay_class: OperatorSchedulerDelayClassV7::Idle,
        } => "open (idle)",
        OperatorSchedulerGateV7::Open {
            delay_class: OperatorSchedulerDelayClassV7::NoProgress,
        } => "open (no_progress)",
        OperatorSchedulerGateV7::AwaitingTrialProgress { .. } => "awaiting_trial_progress",
        OperatorSchedulerGateV7::AwaitingEvidenceRenewal { .. } => "awaiting_evidence_renewal",
        OperatorSchedulerGateV7::Faulted {
            fault: OperatorSchedulerFaultV7::MaintenanceEnvelopeExceeded,
        } => "faulted (maintenance_envelope_exceeded)",
        OperatorSchedulerGateV7::Faulted {
            fault: OperatorSchedulerFaultV7::StepFailed,
        } => "faulted (step_failed)",
        OperatorSchedulerGateV7::Faulted {
            fault: OperatorSchedulerFaultV7::ConsumptionOverflow,
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
    OperatorPhysicalDesignReceiptStatus {
        manifest: PathBuf,
    },
    OperatorPhysicalDesignReceipts {
        manifest: PathBuf,
        after: Option<OperatorPhysicalDesignMutationReceiptCursorV7>,
        limit: u32,
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
        mode: OperatorPhysicalColumnarDesignModeV7,
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
    OperatorPhysicalDesignReceipts,
    OperatorPhysicalDesignReceiptsStatus,
    OperatorPhysicalDesignReceiptsList,
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
            Self::OperatorPhysicalDesignReceipts => OPERATOR_PHYSICAL_DESIGN_RECEIPTS_HELP,
            Self::OperatorPhysicalDesignReceiptsStatus => {
                OPERATOR_PHYSICAL_DESIGN_RECEIPTS_STATUS_HELP
            }
            Self::OperatorPhysicalDesignReceiptsList => OPERATOR_PHYSICAL_DESIGN_RECEIPTS_LIST_HELP,
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
        Some("receipts") => parse_operator_physical_design_receipts(arguments),
        Some("apply-index") => parse_operator_physical_design_apply(arguments),
        Some("apply-columnar") => parse_operator_physical_design_apply_columnar(arguments),
        _ => Err(UsageError::UnknownPhysicalDesignCommand(subcommand)),
    }
}

fn parse_operator_physical_design_receipts(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let subcommand = arguments.next().ok_or(UsageError::ReceiptCommandRequired)?;
    if subcommand == "--help" || subcommand == "-h" {
        return no_extra(
            arguments,
            Action::Help(HelpTopic::OperatorPhysicalDesignReceipts),
        );
    }
    match subcommand.to_str() {
        Some("status") => parse_operator_simple(
            arguments,
            HelpTopic::OperatorPhysicalDesignReceiptsStatus,
            |manifest| Action::OperatorPhysicalDesignReceiptStatus { manifest },
        ),
        Some("list") => parse_operator_physical_design_receipt_list(arguments),
        _ => Err(UsageError::UnknownReceiptCommand(subcommand)),
    }
}

fn parse_operator_physical_design_receipt_list(
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<Action, UsageError> {
    let mut manifest = None;
    let mut limit = None;
    let mut incarnation = None;
    let mut receipt_id = None;
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            if manifest.is_none()
                && limit.is_none()
                && incarnation.is_none()
                && receipt_id.is_none()
            {
                return no_extra(
                    arguments,
                    Action::Help(HelpTopic::OperatorPhysicalDesignReceiptsList),
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
            Some("--limit") => {
                let value = required_value(&mut arguments, "--limit")?;
                set_once(
                    &mut limit,
                    parse_u32(value, UsageError::InvalidReceiptLimit)?,
                    "--limit",
                )?;
            }
            Some("--after-journal-incarnation") => set_once(
                &mut incarnation,
                required_utf8(&mut arguments, "--after-journal-incarnation")?,
                "--after-journal-incarnation",
            )?,
            Some("--after-receipt-id") => {
                let value = required_value(&mut arguments, "--after-receipt-id")?;
                set_once(
                    &mut receipt_id,
                    parse_u64(value, UsageError::InvalidReceiptId)?,
                    "--after-receipt-id",
                )?;
            }
            _ => return Err(UsageError::UnknownArgument(argument)),
        }
    }
    let after = match (incarnation, receipt_id) {
        (None, None) => None,
        (Some(journal_incarnation), Some(receipt_id)) if receipt_id != 0 => {
            Some(OperatorPhysicalDesignMutationReceiptCursorV7 {
                journal_incarnation,
                receipt_id,
            })
        }
        (Some(_), Some(_)) => return Err(UsageError::ZeroReceiptId),
        _ => return Err(UsageError::IncompleteReceiptCursor),
    };
    Ok(Action::OperatorPhysicalDesignReceipts {
        manifest: manifest.ok_or(UsageError::ManifestRequired)?,
        after,
        limit: limit.unwrap_or(32),
    })
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
                    "snapshot" => OperatorPhysicalColumnarDesignModeV7::Snapshot,
                    "incremental" => OperatorPhysicalColumnarDesignModeV7::Incremental,
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

fn parse_u32(raw: OsString, invalid: fn(OsString) -> UsageError) -> Result<u32, UsageError> {
    raw.to_str()
        .and_then(|value| value.parse::<u32>().ok())
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
            Self::Operator(OperatorClientError::Remote(OperatorRemoteErrorV7 {
                code: OperatorErrorCodeV7::PhysicalDesignMutationAdmissionRejected,
                admission: Some(admission), receipt, ..
            })) => {
                formatter.write_str(&render_admission_rejection(admission))?;
                if let Some(receipt) = receipt {
                    write!(formatter, "; receipt {}", render_receipt_ref(receipt))?;
                }
                Ok(())
            }
            Self::Operator(OperatorClientError::Remote(OperatorRemoteErrorV7 {
                code: OperatorErrorCodeV7::ResponseTooLarge,
                ..
            })) => formatter.write_str(
                "operator response exceeds the NBOP v7 payload limit; retry a receipt list with a smaller limit or reduce recommendation cardinality",
            ),
            Self::Operator(OperatorClientError::Remote(OperatorRemoteErrorV7 {
                message,
                receipt: Some(receipt),
                ..
            })) => {
                write!(
                    formatter,
                    "operator request failed: {}; receipt {}",
                    message,
                    render_receipt_ref(receipt)
                )
            }
            Self::Operator(OperatorClientError::Remote(OperatorRemoteErrorV7 {
                code: OperatorErrorCodeV7::PhysicalDesignRuntimeChanged,
                ..
            })) => formatter.write_str(
                "recommendation approval belonged to a previous daemon/operator runtime; fetch fresh recommendations before authorizing a new mutation",
            ),
            Self::Operator(error) => error.fmt(formatter),
            Self::OperatorApplyOutcomeUncertain(
                OperatorClientError::MutationOutcomeUncertain {
                    recovery_required: true,
                    receipt: Some(receipt),
                    ..
                },
            ) => write!(formatter, "mutation outcome is uncertain; receipt {}; restart/reopen the daemon, wait for startup reconciliation, then inspect that receipt; the CLI did not retry", render_receipt_ref(receipt)),
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
    ReceiptCommandRequired,
    UnknownReceiptCommand(OsString),
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
    InvalidReceiptLimit(OsString),
    InvalidReceiptId(OsString),
    ZeroReceiptId,
    IncompleteReceiptCursor,
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
            Self::ReceiptCommandRequired => {
                formatter.write_str("physical-design receipts requires `status` or `list`")
            }
            Self::UnknownReceiptCommand(command) => write!(
                formatter,
                "unknown physical-design receipts command `{}`",
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
            Self::InvalidReceiptLimit(value) => write!(
                formatter,
                "invalid receipt limit `{}`; expected an unsigned integer",
                value.to_string_lossy()
            ),
            Self::InvalidReceiptId(value) => write!(
                formatter,
                "invalid receipt ID `{}`; expected an unsigned integer",
                value.to_string_lossy()
            ),
            Self::ZeroReceiptId => formatter.write_str("--after-receipt-id must be nonzero"),
            Self::IncompleteReceiptCursor => formatter.write_str(
                "--after-journal-incarnation and --after-receipt-id must be provided together",
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
        OperatorPhysicalDesignAdvisorReportV7, OperatorPhysicalDesignEvidenceSummaryV7,
        OperatorPhysicalIndexCandidateV7,
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
            render_physical_design_recommendations(&OperatorPhysicalDesignRecommendationsV7 {
                runtime_token: Some("00".repeat(16)),
                report: OperatorPhysicalDesignAdvisorReportV7 {
                    evidence_epoch: 7,
                    schema_generation: 8,
                    first_global_commit_seq: 9,
                    last_global_commit_seq: 10,
                    recorded_reports: 11,
                    discarded_incomplete_reports: 0,
                    overflowed: false,
                    incomplete: false,
                    index_candidates: vec![OperatorPhysicalIndexCandidateV7 {
                        table_id: 12,
                        column_id: 13,
                        point_report_count: 14,
                        range_report_count: 15,
                        evidence: OperatorPhysicalDesignEvidenceSummaryV7 {
                            report_count: 16,
                            distinct_query_shapes: 17,
                            total_actual_scan_work_units: 18,
                            total_rows_examined: 19,
                            overflowed: false,
                            incomplete: false,
                            truncated: false,
                        },
                        decision: OperatorPhysicalDesignDecisionV7::NoAction {
                            reason: OperatorPhysicalDesignNoActionReasonV7::ExistingDesignCovers,
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
            render_physical_design_recommendations(&OperatorPhysicalDesignRecommendationsV7 {
                runtime_token: token_present.then(|| "11".repeat(16)),
                report: OperatorPhysicalDesignAdvisorReportV7 {
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
            classify_operator_apply_error(OperatorClientError::Remote(OperatorRemoteErrorV7 {
                admission: None,
                code: OperatorErrorCodeV7::ServerStopped,
                message: "command was not sent".into(),
                receipt: None,
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
                OperatorPhysicalIndexApplyOutcomeV7::Created { index_id: 7 },
                "physical index created",
            ),
            (
                OperatorPhysicalIndexApplyOutcomeV7::AlreadyApplied { index_id: 7 },
                "physical index already applied",
            ),
            (
                OperatorPhysicalIndexApplyOutcomeV7::AlreadyCovered,
                "no new index was created because current physical state already covers",
            ),
        ] {
            let output =
                render_physical_index_apply(&netbadb_server::OperatorPhysicalIndexApplyResultV7 {
                    table_id: 1,
                    column_id: 3,
                    index_name: "idx_users_email".into(),
                    outcome,
                    receipt: None,
                });
            assert!(output.contains(expected));
            assert!(!output.contains("CREATE INDEX"));
        }
    }

    #[test]
    fn parses_receipt_status_list_and_requires_a_paired_scoped_cursor() {
        assert_eq!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "receipts",
                "status",
                "--manifest",
                "server.json",
            ]))
            .unwrap(),
            Action::OperatorPhysicalDesignReceiptStatus {
                manifest: PathBuf::from("server.json"),
            }
        );
        assert_eq!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "receipts",
                "list",
                "--manifest",
                "server.json",
            ]))
            .unwrap(),
            Action::OperatorPhysicalDesignReceipts {
                manifest: PathBuf::from("server.json"),
                after: None,
                limit: 32,
            }
        );
        assert_eq!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "receipts",
                "list",
                "--manifest",
                "server.json",
                "--limit",
                "7",
                "--after-journal-incarnation",
                "00112233445566778899aabbccddeeff",
                "--after-receipt-id",
                "41",
            ]))
            .unwrap(),
            Action::OperatorPhysicalDesignReceipts {
                manifest: PathBuf::from("server.json"),
                after: Some(OperatorPhysicalDesignMutationReceiptCursorV7 {
                    journal_incarnation: "00112233445566778899aabbccddeeff".into(),
                    receipt_id: 41,
                }),
                limit: 7,
            }
        );
        for incomplete in [
            vec![
                "operator",
                "physical-design",
                "receipts",
                "list",
                "--manifest",
                "server.json",
                "--after-receipt-id",
                "41",
            ],
            vec![
                "operator",
                "physical-design",
                "receipts",
                "list",
                "--manifest",
                "server.json",
                "--after-journal-incarnation",
                "00112233445566778899aabbccddeeff",
            ],
        ] {
            assert!(matches!(
                parse_args(args(&incomplete)),
                Err(UsageError::IncompleteReceiptCursor)
            ));
        }
        assert!(matches!(
            parse_args(args(&[
                "operator",
                "physical-design",
                "receipts",
                "list",
                "--manifest",
                "server.json",
                "--after-journal-incarnation",
                "00112233445566778899aabbccddeeff",
                "--after-receipt-id",
                "0",
            ])),
            Err(UsageError::ZeroReceiptId)
        ));
        assert!(OPERATOR_PHYSICAL_DESIGN_HELP.contains("receipts"));
    }

    #[test]
    fn receipt_status_page_and_apply_render_only_logical_identity() {
        let reference = OperatorPhysicalDesignMutationReceiptRefV7 {
            journal_incarnation: "00112233445566778899aabbccddeeff".into(),
            receipt_id: 41,
        };
        let status =
            render_physical_design_receipt_status(&OperatorPhysicalDesignMutationReceiptStatusV7 {
                journal_incarnation: reference.journal_incarnation.clone(),
                recovery_required: false,
                latest_receipt_id: Some(41),
                max_receipts_per_read: 128,
            });
        for expected in [
            "Journal incarnation: 00112233445566778899aabbccddeeff",
            "Recovery required: false",
            "Latest receipt ID: 41",
            "Max page size: 128",
        ] {
            assert!(status.contains(expected));
        }

        let page = render_physical_design_receipts(&OperatorPhysicalDesignMutationReceiptPageV7 {
            journal_incarnation: reference.journal_incarnation.clone(),
            receipts: vec![netbadb_server::OperatorPhysicalDesignMutationReceiptV7 {
                receipt: reference.clone(),
                source: OperatorPhysicalDesignMutationReceiptSourceV7::LocalOperator,
                evidence_epoch: 7,
                target: OperatorPhysicalDesignMutationReceiptTargetV7::Columnar {
                    table_id: 1,
                    columns: vec![2, 3, 4],
                    mode: OperatorPhysicalColumnarDesignModeV7::Incremental,
                    placement_key: "users-analytics-v1".into(),
                },
                outcome: OperatorPhysicalDesignMutationReceiptOutcomeV7::RecoveredAppliedColumnar {
                    projection_id: 9,
                },
            }],
            next_after: Some(OperatorPhysicalDesignMutationReceiptCursorV7 {
                journal_incarnation: reference.journal_incarnation.clone(),
                receipt_id: 41,
            }),
        });
        for expected in [
            "00112233445566778899aabbccddeeff/41",
            "source: local_operator",
            "evidence epoch: 7",
            "placement key users-analytics-v1",
            "recovered_applied_columnar ProjectionId(9)",
        ] {
            assert!(page.contains(expected));
        }

        let with_receipt =
            render_physical_index_apply(&netbadb_server::OperatorPhysicalIndexApplyResultV7 {
                table_id: 1,
                column_id: 3,
                index_name: "idx_users_email".into(),
                outcome: OperatorPhysicalIndexApplyOutcomeV7::Created { index_id: 7 },
                receipt: Some(reference),
            });
        assert!(with_receipt.contains("receipt: 00112233445566778899aabbccddeeff/41"));
        for forbidden in [
            ".nbmr",
            "/private/",
            "database incarnation",
            "runtime token",
        ] {
            assert!(!status.contains(forbidden));
            assert!(!page.contains(forbidden));
            assert!(!with_receipt.contains(forbidden));
        }
    }

    #[test]
    fn explicit_remote_and_network_uncertainty_render_distinct_guidance() {
        let reference = OperatorPhysicalDesignMutationReceiptRefV7 {
            journal_incarnation: "00112233445566778899aabbccddeeff".into(),
            receipt_id: 41,
        };
        let remote = OperationalError::OperatorApplyOutcomeUncertain(
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: true,
                receipt: Some(reference),
                source: Box::new(OperatorClientError::Remote(OperatorRemoteErrorV7 {
                    admission: None,
                    code: OperatorErrorCodeV7::PhysicalDesignMutationOutcomeUncertain,
                    message: "bounded".into(),
                    receipt: None,
                })),
            },
        )
        .to_string();
        assert!(remote.contains("mutation outcome is uncertain"));
        assert!(remote.contains("00112233445566778899aabbccddeeff/41"));
        assert!(remote.contains("restart/reopen"));
        assert!(remote.contains("startup reconciliation"));
        assert!(!remote.contains("re-run the same exact approval"));

        let network = OperationalError::OperatorApplyOutcomeUncertain(
            OperatorClientError::MutationOutcomeUncertain {
                recovery_required: false,
                receipt: None,
                source: Box::new(OperatorClientError::UnexpectedResult),
            },
        )
        .to_string();
        assert!(network.contains("no definitive NBOP result"));
        assert!(network.contains("re-run the same exact approval"));
        assert!(!network.contains("receipt 001122"));
        assert!(!network.contains("restart/reopen"));
    }
    include!("operator_admission_tests.rs");
}
