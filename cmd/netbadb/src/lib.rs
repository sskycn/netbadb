//! Offline local catalog and statement inspection CLI.

mod json;

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

use netbadb_sdk::inspection::{render_catalog, render_statement};
use netbadb_sdk::{Database, DatabaseError};
use netbadb_server::{
    ManifestError, OperatorAdaptiveModeV1, OperatorClientError, OperatorSchedulerDelayClassV1,
    OperatorSchedulerFaultV1, OperatorSchedulerGateV1, OperatorStatusV1, ServerConfig,
    ServerOperatorClient,
};

const ROOT_HELP: &str = "Usage:\n  netbadb inspect <catalog|statement> [options]\n  netbadb operator <status|rotate-evidence|reset-faulted-scheduler> [options]\n\nUse `netbadb inspect --help` or `netbadb operator --help` for commands.\n";
const INSPECT_HELP: &str = "Usage:\n  netbadb inspect catalog --manifest <server.json> [--format text|json]\n  netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n";
const CATALOG_HELP: &str = "Usage: netbadb inspect catalog --manifest <server.json> [--format text|json]\n\nInspects the complete offline local catalog.\n";
const STATEMENT_HELP: &str = "Usage: netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n\nCompiles and inspects one statement without executing it.\n";
const OPERATOR_HELP: &str = "Usage:\n  netbadb operator status --manifest <server.json>\n  netbadb operator rotate-evidence --manifest <server.json> --expected-window-epoch <epoch>\n  netbadb operator reset-faulted-scheduler --manifest <server.json>\n";
const OPERATOR_STATUS_HELP: &str = "Usage: netbadb operator status --manifest <server.json>\n\nReads bounded live Adaptive status over NBOP v1.\n";
const OPERATOR_ROTATE_HELP: &str = "Usage: netbadb operator rotate-evidence --manifest <server.json> --expected-window-epoch <epoch>\n\nConditionally rotates the live evidence window. The expected epoch is required and is never inferred.\n";
const OPERATOR_RESET_HELP: &str = "Usage: netbadb operator reset-faulted-scheduler --manifest <server.json>\n\nAcknowledges and resets only a genuinely faulted scheduler.\n";

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

fn render_operator_status(status: &OperatorStatusV1) -> String {
    let mode = match status.mode {
        OperatorAdaptiveModeV1::FeedbackOnly => "feedback_only",
        OperatorAdaptiveModeV1::Driven => "driven",
    };
    let feedback = &status.feedback;
    let mut output = format!(
        "adaptive mode: {mode}\nevidence window epoch: {}\nschema generation: {}\nrecorded reports: {}\neligible queries: {}\nrecord successes: {}\nrecord errors: {}\n",
        feedback.window_epoch,
        feedback
            .schema_generation
            .map_or_else(|| "none".into(), |generation| generation.to_string()),
        feedback.recorded_reports,
        feedback.eligible_query_count,
        feedback.record_success_count,
        feedback.record_error_count,
    );
    if let Some(driver) = &status.driver {
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
    output
}

const fn render_scheduler_gate(gate: OperatorSchedulerGateV1) -> &'static str {
    match gate {
        OperatorSchedulerGateV1::Open {
            delay_class: OperatorSchedulerDelayClassV1::Normal,
        } => "open (normal)",
        OperatorSchedulerGateV1::Open {
            delay_class: OperatorSchedulerDelayClassV1::Idle,
        } => "open (idle)",
        OperatorSchedulerGateV1::Open {
            delay_class: OperatorSchedulerDelayClassV1::NoProgress,
        } => "open (no_progress)",
        OperatorSchedulerGateV1::AwaitingTrialProgress { .. } => "awaiting_trial_progress",
        OperatorSchedulerGateV1::AwaitingEvidenceRenewal { .. } => "awaiting_evidence_renewal",
        OperatorSchedulerGateV1::Faulted {
            fault: OperatorSchedulerFaultV1::MaintenanceEnvelopeExceeded,
        } => "faulted (maintenance_envelope_exceeded)",
        OperatorSchedulerGateV1::Faulted {
            fault: OperatorSchedulerFaultV1::StepFailed,
        } => "faulted (step_failed)",
        OperatorSchedulerGateV1::Faulted {
            fault: OperatorSchedulerFaultV1::ConsumptionOverflow,
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
        _ => Err(UsageError::UnknownOperatorCommand(subcommand)),
    }
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
            Self::Operator(error) => error.fmt(formatter),
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
                "operator requires `status`, `rotate-evidence`, or `reset-faulted-scheduler`",
            ),
            Self::UnknownOperatorCommand(command) => write!(
                formatter,
                "unknown operator command `{}`",
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
    }
}
