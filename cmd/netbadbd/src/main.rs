use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::process::ExitCode;

use netbadb_server::{ServerConfig, TcpServer};

const HELP: &str =
    "Usage: netbadbd --manifest <path>\n\nStarts the loopback-only NetbaDB TCP server.";

fn main() -> ExitCode {
    match parse_args(env::args_os().skip(1)) {
        Ok(Action::Help) => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        Ok(Action::Version) => {
            println!("netbadbd {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Ok(Action::Run { manifest }) => match run_server(manifest) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("netbadbd: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("netbadbd: {error}\n\n{HELP}");
            ExitCode::FAILURE
        }
    }
}

fn run_server(manifest: PathBuf) -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::from_manifest_path(manifest)?;
    let max_connections = config.limits().max_connections();
    let server = TcpServer::new(config).start()?;
    eprintln!(
        "netbadbd listening on {} with {} table(s), max {} connections",
        server.local_addr(),
        server.table_count(),
        max_connections
    );
    server.wait()?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Run { manifest: PathBuf },
    Help,
    Version,
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Action, CliError> {
    let mut arguments = arguments.into_iter();
    let Some(first) = arguments.next() else {
        return Err(CliError::ManifestRequired);
    };
    if first == "--help" || first == "-h" {
        return no_extra_arguments(arguments, Action::Help);
    }
    if first == "--version" || first == "-V" {
        return no_extra_arguments(arguments, Action::Version);
    }
    if first != "--manifest" {
        return Err(CliError::UnknownArgument(first));
    }
    let manifest = arguments.next().ok_or(CliError::ManifestPathRequired)?;
    no_extra_arguments(
        arguments,
        Action::Run {
            manifest: PathBuf::from(manifest),
        },
    )
}

fn no_extra_arguments(
    mut arguments: impl Iterator<Item = OsString>,
    action: Action,
) -> Result<Action, CliError> {
    match arguments.next() {
        Some(argument) => Err(CliError::UnexpectedArgument(argument)),
        None => Ok(action),
    }
}

#[derive(Debug)]
enum CliError {
    ManifestRequired,
    ManifestPathRequired,
    UnknownArgument(OsString),
    UnexpectedArgument(OsString),
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManifestRequired => formatter.write_str("--manifest is required"),
            Self::ManifestPathRequired => formatter.write_str("--manifest requires a path"),
            Self::UnknownArgument(argument) => {
                write!(
                    formatter,
                    "unknown argument `{}`",
                    argument.to_string_lossy()
                )
            }
            Self::UnexpectedArgument(argument) => write!(
                formatter,
                "unexpected additional argument `{}`",
                argument.to_string_lossy()
            ),
        }
    }
}

impl Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_the_single_manifest_argument() {
        assert_eq!(
            parse_args(args(&["--manifest", "server.json"])).unwrap(),
            Action::Run {
                manifest: PathBuf::from("server.json")
            }
        );
    }

    #[test]
    fn supports_help_and_version_but_rejects_ambiguous_invocations() {
        assert_eq!(parse_args(args(&["--help"])).unwrap(), Action::Help);
        assert_eq!(parse_args(args(&["--version"])).unwrap(), Action::Version);
        assert!(matches!(
            parse_args(args(&[])),
            Err(CliError::ManifestRequired)
        ));
        assert!(matches!(
            parse_args(args(&["--manifest"])),
            Err(CliError::ManifestPathRequired)
        ));
        assert!(matches!(
            parse_args(args(&["--manifest", "a", "b"])),
            Err(CliError::UnexpectedArgument(_))
        ));
    }
}
