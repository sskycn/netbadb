use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use netbadb_server::{
    PostgresServerHandle, PostgresTcpServer, PostgresTcpServerError, ServerAdaptiveMode,
    ServerConfig, ServerHandle, TcpServer, TcpServerError,
};

const HELP: &str = "Usage: netbadbd --manifest <path> [--postgres]\n\nStarts the manifest-configured native server, or the experimental PostgreSQL wire listener with --postgres.";
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(10);

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
        Ok(Action::Run { manifest, postgres }) => match run_daemon(manifest, postgres) {
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

fn run_daemon(manifest: PathBuf, postgres: bool) -> Result<(), Box<dyn Error>> {
    // Registration deliberately precedes manifest parsing and every daemon-owned
    // database, listener, and operator resource.
    let signals = ShutdownSignals::install()?;
    let mut stderr = io::stderr().lock();
    run_server(manifest, postgres, &signals, &mut stderr)
}

fn run_server(
    manifest: PathBuf,
    postgres: bool,
    signals: &ShutdownSignals,
    readiness: &mut impl Write,
) -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::from_manifest_path(manifest)?;
    let max_connections = config.limits().max_connections();
    let adaptive = adaptive_label(config.adaptive_mode());
    let server = if postgres {
        RunningServer::Postgres(PostgresTcpServer::new(config).start()?)
    } else {
        RunningServer::Native(TcpServer::new(config).start()?)
    };

    match lifecycle_action(false, signals.requested(), server.is_finished()) {
        LifecycleAction::Shutdown => return server.shutdown().map_err(Into::into),
        LifecycleAction::Wait => return server.wait().map_err(Into::into),
        LifecycleAction::PublishReady => {}
        LifecycleAction::Continue => unreachable!("a server cannot continue before readiness"),
    }
    if let Err(readiness_error) = server.publish_readiness(readiness, max_connections, adaptive) {
        return match server.shutdown() {
            Ok(()) => Err(Box::new(ReadinessError::Publish(readiness_error))),
            Err(shutdown) => Err(Box::new(ReadinessError::PublishAndShutdown {
                publish: readiness_error,
                shutdown,
            })),
        };
    }

    loop {
        match lifecycle_action(true, signals.requested(), server.is_finished()) {
            LifecycleAction::Shutdown => return server.shutdown().map_err(Into::into),
            LifecycleAction::Wait => return server.wait().map_err(Into::into),
            LifecycleAction::Continue => thread::sleep(LIFECYCLE_POLL_INTERVAL),
            LifecycleAction::PublishReady => {
                unreachable!("readiness is published exactly once")
            }
        }
    }
}

enum RunningServer {
    Native(ServerHandle),
    Postgres(PostgresServerHandle),
}

impl RunningServer {
    fn is_finished(&self) -> bool {
        match self {
            Self::Native(server) => server.is_finished(),
            Self::Postgres(server) => server.is_finished(),
        }
    }

    fn publish_readiness(
        &self,
        writer: &mut impl Write,
        max_connections: usize,
        adaptive: &str,
    ) -> io::Result<()> {
        match self {
            Self::Native(server) => writeln!(
                writer,
                "netbadbd ready: native listener on {}, {} table(s), max {} connections, transport {}, adaptive {adaptive}",
                server.local_addr(),
                server.table_count(),
                max_connections,
                server.transport_kind(),
            )?,
            Self::Postgres(server) => writeln!(
                writer,
                "netbadbd ready: PostgreSQL listener on {}, max {} connections, transport plaintext-loopback, adaptive {adaptive}",
                server.local_addr(),
                max_connections,
            )?,
        }
        writer.flush()
    }

    fn shutdown(self) -> Result<(), RunningServerError> {
        match self {
            Self::Native(server) => server.shutdown().map_err(RunningServerError::Native),
            Self::Postgres(server) => server.shutdown().map_err(RunningServerError::Postgres),
        }
    }

    fn wait(self) -> Result<(), RunningServerError> {
        match self {
            Self::Native(server) => server.wait().map_err(RunningServerError::Native),
            Self::Postgres(server) => server.wait().map_err(RunningServerError::Postgres),
        }
    }
}

#[derive(Debug)]
enum RunningServerError {
    Native(TcpServerError),
    Postgres(PostgresTcpServerError),
}

impl fmt::Display for RunningServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => error.fmt(formatter),
            Self::Postgres(error) => error.fmt(formatter),
        }
    }
}

impl Error for RunningServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Postgres(error) => Some(error),
        }
    }
}

#[derive(Debug)]
enum ReadinessError {
    Publish(io::Error),
    PublishAndShutdown {
        publish: io::Error,
        shutdown: RunningServerError,
    },
}

impl fmt::Display for ReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publish(error) => write!(formatter, "failed to publish readiness: {error}"),
            Self::PublishAndShutdown { publish, shutdown } => write!(
                formatter,
                "failed to publish readiness: {publish}; graceful shutdown also failed: {shutdown}"
            ),
        }
    }
}

impl Error for ReadinessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Publish(error) | Self::PublishAndShutdown { publish: error, .. } => Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecycleAction {
    PublishReady,
    Shutdown,
    Wait,
    Continue,
}

const fn lifecycle_action(
    ready_published: bool,
    shutdown_requested: bool,
    server_finished: bool,
) -> LifecycleAction {
    if shutdown_requested {
        LifecycleAction::Shutdown
    } else if server_finished {
        LifecycleAction::Wait
    } else if ready_published {
        LifecycleAction::Continue
    } else {
        LifecycleAction::PublishReady
    }
}

struct ShutdownSignals {
    requested: Arc<AtomicBool>,
    #[cfg(unix)]
    registrations: [signal_hook::SigId; 2],
}

impl ShutdownSignals {
    fn install() -> io::Result<Self> {
        let requested = Arc::new(AtomicBool::new(false));
        #[cfg(unix)]
        {
            use signal_hook::consts::signal::{SIGINT, SIGTERM};

            let interrupt = signal_hook::flag::register(SIGINT, Arc::clone(&requested))?;
            let terminate = match signal_hook::flag::register(SIGTERM, Arc::clone(&requested)) {
                Ok(registration) => registration,
                Err(error) => {
                    signal_hook::low_level::unregister(interrupt);
                    return Err(error);
                }
            };
            Ok(Self {
                requested,
                registrations: [interrupt, terminate],
            })
        }
        #[cfg(not(unix))]
        Ok(Self { requested })
    }

    fn requested(&self) -> bool {
        self.requested.load(Ordering::Relaxed)
    }
}

#[cfg(unix)]
impl Drop for ShutdownSignals {
    fn drop(&mut self) {
        for registration in self.registrations {
            signal_hook::low_level::unregister(registration);
        }
    }
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

    #[test]
    fn lifecycle_shuts_down_before_readiness_when_a_signal_is_pending() {
        assert_eq!(
            lifecycle_action(false, true, false),
            LifecycleAction::Shutdown
        );
    }

    #[test]
    fn lifecycle_waits_when_the_server_finishes_before_readiness() {
        assert_eq!(lifecycle_action(false, false, true), LifecycleAction::Wait);
    }

    #[test]
    fn lifecycle_publishes_readiness_only_for_a_running_unsignalled_server() {
        assert_eq!(
            lifecycle_action(false, false, false),
            LifecycleAction::PublishReady
        );
    }

    #[test]
    fn lifecycle_shuts_down_when_a_signal_arrives_after_readiness() {
        assert_eq!(
            lifecycle_action(true, true, false),
            LifecycleAction::Shutdown
        );
    }
}
