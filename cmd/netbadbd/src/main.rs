use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::process::ExitCode;

use netbadb_server::{PostgresTcpServer, ServerAdaptiveMode, ServerConfig, TcpServer};

const HELP: &str = "Usage: netbadbd --manifest <path> [--postgres]\n\nStarts the manifest-configured native server, or the experimental PostgreSQL wire listener with --postgres.";

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
        Ok(Action::Run { manifest, postgres }) => match run_server(manifest, postgres) {
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

fn run_server(manifest: PathBuf, postgres: bool) -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::from_manifest_path(manifest)?;
    let max_connections = config.limits().max_connections();
    let adaptive = adaptive_label(config.adaptive_mode());
    if postgres {
        let server = PostgresTcpServer::new(config).start()?;
        eprintln!(
            "netbadbd experimental PostgreSQL listener on {}, max {} connections, transport plaintext-loopback, adaptive {adaptive}",
            server.local_addr(),
            max_connections,
        );
        server.wait()?;
        return Ok(());
    }
    let server = TcpServer::new(config).start()?;
    eprintln!(
        "netbadbd listening on {} with {} table(s), max {} connections, transport {}, adaptive {adaptive}",
        server.local_addr(),
        server.table_count(),
        max_connections,
        server.transport_kind()
    );
    server.wait()?;
    Ok(())
}

const fn adaptive_label(mode: ServerAdaptiveMode) -> &'static str {
    match mode {
        ServerAdaptiveMode::Disabled => "disabled",
        ServerAdaptiveMode::FeedbackOnly => "feedback-only",
        ServerAdaptiveMode::Driven => "driven",
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Run { manifest: PathBuf, postgres: bool },
    Help,
    Version,
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Action, CliError> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    let Some(first) = arguments.first() else {
        return Err(CliError::ManifestRequired);
    };
    if first == "--help" || first == "-h" {
        return no_extra_arguments(arguments.into_iter().skip(1), Action::Help);
    }
    if first == "--version" || first == "-V" {
        return no_extra_arguments(arguments.into_iter().skip(1), Action::Version);
    }
    let mut manifest = None;
    let mut postgres = false;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].to_str() {
            Some("--manifest") => {
                if manifest.is_some() {
                    return Err(CliError::UnexpectedArgument(arguments[index].clone()));
                }
                index += 1;
                let value = arguments.get(index).ok_or(CliError::ManifestPathRequired)?;
                manifest = Some(PathBuf::from(value));
            }
            Some("--postgres") if !postgres => postgres = true,
            _ => return Err(CliError::UnknownArgument(arguments[index].clone())),
        }
        index += 1;
    }
    Ok(Action::Run {
        manifest: manifest.ok_or(CliError::ManifestRequired)?,
        postgres,
    })
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
                manifest: PathBuf::from("server.json"),
                postgres: false,
            }
        );
        assert_eq!(
            parse_args(args(&["--postgres", "--manifest", "server.json"])).unwrap(),
            Action::Run {
                manifest: PathBuf::from("server.json"),
                postgres: true,
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
            Err(CliError::UnknownArgument(_))
        ));
    }

    #[test]
    fn startup_adaptive_labels_expose_only_the_stable_mode_keyword() {
        assert_eq!(adaptive_label(ServerAdaptiveMode::Disabled), "disabled");
        assert_eq!(
            adaptive_label(ServerAdaptiveMode::FeedbackOnly),
            "feedback-only"
        );
        assert_eq!(adaptive_label(ServerAdaptiveMode::Driven), "driven");
    }
}
