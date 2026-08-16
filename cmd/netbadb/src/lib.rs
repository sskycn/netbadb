//! Offline local catalog and statement inspection CLI.

mod json;

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

use netbadb_sdk::inspection::{render_catalog, render_statement};
use netbadb_sdk::{Database, DatabaseError};
use netbadb_server::{ManifestError, ServerConfig};

const ROOT_HELP: &str = "Usage: netbadb inspect <catalog|statement> [options]\n\nOffline local NetbaDB inspection. Use `netbadb inspect --help` for commands.\n";
const INSPECT_HELP: &str = "Usage:\n  netbadb inspect catalog --manifest <server.json> [--format text|json]\n  netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n";
const CATALOG_HELP: &str = "Usage: netbadb inspect catalog --manifest <server.json> [--format text|json]\n\nInspects the complete offline local catalog.\n";
const STATEMENT_HELP: &str = "Usage: netbadb inspect statement --manifest <server.json> (--sql <SQL>|--sql-file <path>) [--format text|json]\n\nCompiles and inspects one statement without executing it.\n";

/// Parses and runs one CLI invocation, returning complete stdout only after a
/// successfully closed database.
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelpTopic {
    Root,
    Inspect,
    Catalog,
    Statement,
}

impl HelpTopic {
    const fn text(self) -> &'static str {
        match self {
            Self::Root => ROOT_HELP,
            Self::Inspect => INSPECT_HELP,
            Self::Catalog => CATALOG_HELP,
            Self::Statement => STATEMENT_HELP,
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
        }
    }
}

impl Error for OperationalError {}

#[derive(Debug)]
enum UsageError {
    CommandRequired,
    InspectCommandRequired,
    UnknownInspectCommand(OsString),
    ManifestRequired,
    SqlSourceRequired,
    SqlSourceConflict,
    SqlMustBeUtf8,
    ValueRequired(&'static str),
    DuplicateOption(&'static str),
    UnknownFormat(OsString),
    UnknownArgument(OsString),
    UnexpectedArgument(OsString),
}

impl fmt::Display for UsageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandRequired => formatter.write_str("an inspect command is required"),
            Self::InspectCommandRequired => {
                formatter.write_str("inspect requires `catalog` or `statement`")
            }
            Self::UnknownInspectCommand(command) => write!(
                formatter,
                "unknown inspect command `{}`",
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
}
