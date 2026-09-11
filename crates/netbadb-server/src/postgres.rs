use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use netbadb_core::{
    Database, DatabaseError, DatabaseErrorKind, DdlOutcome, ExecutionResult, IndexKindInspection,
    ParameterTypeHint, PreparedDdlStatement as CorePreparedDdl, PreparedSqlStatement,
    PreparedStatement as CorePrepared, QueryResult, StatementAccess, StatementDescription,
    TablePlacementInspection,
};
use netbadb_pgwire::{
    BackendMessage, CloseTarget, DescribeTarget, ErrorResponse, FieldDescription, FormatCode,
    FrontendMessage, PostgresOid, PostgresType, StartupMessage, StartupPacket, TypeMappingError,
    WireError, decode_binary_parameter_as, decode_text_parameter_as, encode_binary_value,
    encode_text_value, read_frontend_message, read_startup_packet, write_backend_message,
};
use netbadb_protocol::WireTransactionState;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};
use sha2::{Digest, Sha256};

use crate::adaptive_driver::{
    ServerAdaptiveControlHandle, ServerAdaptiveDriverConfig, ServerAdaptiveDriverConfigError,
    ServerAdaptiveHostConfig, ServerAdaptiveHostDriver, ServerAdaptiveStartupMode,
    ServerAdaptiveWorkerCommand, ServerAdaptiveWorkerRuntime, forward_control_requests,
    handle_disabled_worker_command,
};
use crate::adaptive_feedback::{
    ServerAdaptiveFeedbackRuntime, execute_prepared_with_optional_server_feedback,
};
use crate::authorization::{AuthorizationPolicy, PrincipalAuthorization};
use crate::operator::ServerOperatorPlane;
use crate::{
    ClientIdentity, DatabaseSession, ServerAdaptiveFeedbackConfig, ServerConfig, ServerLimits,
    ServerOperatorError, SessionPolicy, TableBootstrap, TransportKind,
};

const MAX_PREPARED_STATEMENTS: usize = 1_024;
const MAX_PORTALS: usize = 1_024;
const MAX_ERROR_BYTES: usize = 8 * 1024;
const MAX_COMPATIBILITY_TOKENS: usize = 4_096;
const MAX_COMPATIBILITY_NESTING: usize = 32;
const MAX_CATALOG_PATTERN_BYTES: usize = 1_024;
const MAX_CATALOG_PATTERN_ATOMS: usize = 1_024;

pub struct PostgresTcpServer {
    config: ServerConfig,
    adaptive_override: Option<ServerAdaptiveStartupMode>,
}

impl PostgresTcpServer {
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            adaptive_override: None,
        }
    }

    /// Enables bounded autocommit Core-query feedback capture in this
    /// PostgreSQL server's database worker. The wire contract is unchanged.
    #[must_use]
    pub fn with_adaptive_feedback(mut self, config: ServerAdaptiveFeedbackConfig) -> Self {
        self.adaptive_override = Some(ServerAdaptiveStartupMode::FeedbackOnly(config));
        self
    }

    /// Enables feedback capture plus host-time logical scheduling in the
    /// existing PostgreSQL database worker. The wire contract is unchanged.
    #[must_use]
    pub fn with_adaptive_driver(mut self, config: ServerAdaptiveDriverConfig) -> Self {
        self.adaptive_override = Some(ServerAdaptiveStartupMode::Driven(Box::new(config)));
        self
    }

    pub fn start(self) -> Result<PostgresServerHandle, PostgresTcpServerError> {
        let (
            listen,
            tables,
            limits,
            security,
            authorization,
            manifest_adaptive_mode,
            operator_config,
        ) = self.config.into_parts();
        let adaptive_mode = self.adaptive_override.unwrap_or(manifest_adaptive_mode);
        if security.kind() != TransportKind::PlaintextLoopback {
            return Err(PostgresTcpServerError::TlsManifestUnsupported);
        }
        let tick_interval = adaptive_mode.tick_interval();
        let worker = PgDatabaseWorker::start(
            tables,
            limits.session_policy(),
            authorization,
            adaptive_mode,
        )?;
        let listener = match TcpListener::bind(listen) {
            Ok(listener) => listener,
            Err(source) => {
                return Err(finish_pg_startup_failure(
                    worker,
                    PostgresTcpServerError::Bind {
                        address: listen,
                        source,
                    },
                ));
            }
        };
        if let Err(error) = listener.set_nonblocking(true) {
            return Err(finish_pg_startup_failure(
                worker,
                PostgresTcpServerError::ListenerConfiguration(error),
            ));
        }
        let local_addr = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                return Err(finish_pg_startup_failure(
                    worker,
                    PostgresTcpServerError::ListenerConfiguration(error),
                ));
            }
        };
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let (adaptive_control_tx, adaptive_control_rx) = mpsc::channel();
        let (operator_failure_tx, operator_failure_rx) = mpsc::channel();
        let (worker_tx, worker_rx) = mpsc::sync_channel(0);
        let join = match thread::Builder::new()
            .name("netbadb-postgres-server".into())
            .spawn(move || {
                let worker = worker_rx
                    .recv()
                    .map_err(|_| PostgresTcpServerError::WorkerStopped)?;
                run_pg_accept_loop(
                    listener,
                    shutdown_rx,
                    worker,
                    limits,
                    ServerAdaptiveHostConfig {
                        tick_interval,
                        controls: adaptive_control_rx,
                        operator_failures: operator_failure_rx,
                    },
                )
            }) {
            Ok(join) => join,
            Err(error) => {
                return Err(finish_pg_startup_failure(
                    worker,
                    PostgresTcpServerError::ThreadSpawn(error),
                ));
            }
        };
        if let Err(error) = worker_tx.send(worker) {
            let startup = match join.join() {
                Ok(Ok(())) => PostgresTcpServerError::WorkerStopped,
                Ok(Err(error)) => error,
                Err(_) => PostgresTcpServerError::ThreadPanicked,
            };
            return Err(finish_pg_startup_failure(error.0, startup));
        }
        let adaptive_control = ServerAdaptiveControlHandle::new(adaptive_control_tx);
        let operator = match operator_config {
            Some(config) => match ServerOperatorPlane::start(
                config,
                adaptive_control.clone(),
                operator_failure_tx,
            ) {
                Ok(operator) => Some(operator),
                Err(error) => {
                    let _ = shutdown_tx.send(());
                    let cleanup = join
                        .join()
                        .map_err(|_| PostgresTcpServerError::ThreadPanicked)
                        .and_then(|result| result);
                    return Err(match cleanup {
                        Ok(()) => PostgresTcpServerError::Operator(error),
                        Err(server) => PostgresTcpServerError::OperatorAndServerCleanup {
                            operator: Box::new(error),
                            server: Box::new(server),
                        },
                    });
                }
            },
            None => None,
        };
        Ok(PostgresServerHandle {
            local_addr,
            shutdown_tx,
            adaptive_control,
            operator,
            join: Some(join),
        })
    }

    pub fn run(self) -> Result<(), PostgresTcpServerError> {
        self.start()?.wait()
    }
}

pub struct PostgresServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: Sender<()>,
    adaptive_control: ServerAdaptiveControlHandle,
    operator: Option<ServerOperatorPlane>,
    join: Option<JoinHandle<Result<(), PostgresTcpServerError>>>,
}

impl PostgresServerHandle {
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub fn adaptive_control(&self) -> ServerAdaptiveControlHandle {
        self.adaptive_control.clone()
    }

    /// Returns whether the main server thread or configured operator listener
    /// has terminated. This is a lifecycle observation, not a health check.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.join.as_ref().is_none_or(JoinHandle::is_finished)
            || self
                .operator
                .as_ref()
                .is_some_and(ServerOperatorPlane::is_finished)
    }

    pub fn shutdown(mut self) -> Result<(), PostgresTcpServerError> {
        let operator = self.operator.take().map(ServerOperatorPlane::shutdown);
        let _ = self.shutdown_tx.send(());
        combine_operator_and_server(operator, self.join_server())
    }

    pub fn wait(mut self) -> Result<(), PostgresTcpServerError> {
        let server = self.join_server();
        let operator = self.operator.take().map(ServerOperatorPlane::shutdown);
        combine_operator_and_server(operator, server)
    }

    fn join_server(&mut self) -> Result<(), PostgresTcpServerError> {
        let join = self
            .join
            .take()
            .ok_or(PostgresTcpServerError::WorkerStopped)?;
        join.join()
            .map_err(|_| PostgresTcpServerError::ThreadPanicked)?
    }
}

fn combine_operator_and_server(
    operator: Option<Result<(), ServerOperatorError>>,
    server: Result<(), PostgresTcpServerError>,
) -> Result<(), PostgresTcpServerError> {
    match (operator.transpose(), server) {
        (Ok(_), Ok(())) => Ok(()),
        (Ok(_), Err(server)) => Err(server),
        (Err(operator), Ok(())) => Err(PostgresTcpServerError::Operator(operator)),
        (Err(operator), Err(server)) => Err(PostgresTcpServerError::OperatorAndServerCleanup {
            operator: Box::new(operator),
            server: Box::new(server),
        }),
    }
}

#[derive(Debug)]
pub enum PostgresTcpServerError {
    TlsManifestUnsupported,
    Bind {
        address: SocketAddr,
        source: io::Error,
    },
    ListenerConfiguration(io::Error),
    Accept(io::Error),
    ThreadSpawn(io::Error),
    Database(DatabaseError),
    AdaptiveConfig(ServerAdaptiveDriverConfigError),
    Operator(ServerOperatorError),
    OperatorAndServerCleanup {
        operator: Box<ServerOperatorError>,
        server: Box<PostgresTcpServerError>,
    },
    StartupCleanup {
        startup: Box<PostgresTcpServerError>,
        cleanup: Box<PostgresTcpServerError>,
    },
    WorkerStopped,
    WorkerClose(String),
    ThreadPanicked,
    SessionIdExhausted,
}

impl fmt::Display for PostgresTcpServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TlsManifestUnsupported => formatter.write_str(
                "experimental PostgreSQL listener currently supports loopback plaintext only",
            ),
            Self::Bind { address, source } => {
                write!(
                    formatter,
                    "failed to bind PostgreSQL listener `{address}`: {source}"
                )
            }
            Self::ListenerConfiguration(error) => error.fmt(formatter),
            Self::Accept(error) => write!(formatter, "PostgreSQL accept failed: {error}"),
            Self::ThreadSpawn(error) => write!(
                formatter,
                "failed to spawn PostgreSQL server thread: {error}"
            ),
            Self::Database(error) => write!(
                formatter,
                "PostgreSQL database worker startup failed: {error}"
            ),
            Self::AdaptiveConfig(error) => {
                write!(formatter, "adaptive driver startup failed: {error}")
            }
            Self::Operator(error) => error.fmt(formatter),
            Self::OperatorAndServerCleanup { operator, server } => write!(
                formatter,
                "operator shutdown failed: {operator}; PostgreSQL server cleanup also failed: {server}"
            ),
            Self::StartupCleanup { startup, cleanup } => write!(
                formatter,
                "PostgreSQL server startup failed: {startup}; worker cleanup also failed: {cleanup}"
            ),
            Self::WorkerStopped => {
                formatter.write_str("PostgreSQL database worker stopped unexpectedly")
            }
            Self::WorkerClose(message) => write!(
                formatter,
                "PostgreSQL database worker cleanup failed: {message}"
            ),
            Self::ThreadPanicked => formatter.write_str("PostgreSQL server thread panicked"),
            Self::SessionIdExhausted => formatter.write_str("PostgreSQL session IDs are exhausted"),
        }
    }
}

impl Error for PostgresTcpServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind { source, .. }
            | Self::ListenerConfiguration(source)
            | Self::Accept(source)
            | Self::ThreadSpawn(source) => Some(source),
            Self::Database(error) => Some(error),
            Self::AdaptiveConfig(error) => Some(error),
            Self::Operator(error) => Some(error),
            Self::OperatorAndServerCleanup { server, .. } => Some(server.as_ref()),
            Self::StartupCleanup { cleanup, .. } => Some(cleanup.as_ref()),
            _ => None,
        }
    }
}

fn finish_pg_startup_failure(
    worker: PgDatabaseWorker,
    startup: PostgresTcpServerError,
) -> PostgresTcpServerError {
    match worker.shutdown() {
        Ok(()) => startup,
        Err(cleanup) => PostgresTcpServerError::StartupCleanup {
            startup: Box::new(startup),
            cleanup: Box::new(cleanup),
        },
    }
}

struct PgConnection {
    stream: TcpStream,
    join: JoinHandle<()>,
}

fn run_pg_accept_loop(
    listener: TcpListener,
    shutdown: Receiver<()>,
    worker: PgDatabaseWorker,
    limits: ServerLimits,
    adaptive_host_config: ServerAdaptiveHostConfig,
) -> Result<(), PostgresTcpServerError> {
    let mut connections = Vec::new();
    let mut next_session_id = 1_u64;
    let mut adaptive_host = adaptive_host_config
        .tick_interval
        .map(ServerAdaptiveHostDriver::new);
    loop {
        match shutdown.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {}
        }
        if adaptive_host_config.operator_failures.try_recv().is_ok() {
            break;
        }
        reap_pg_connections(&mut connections);
        if let Some(host) = adaptive_host.as_mut() {
            host.poll(|command| {
                worker
                    .client
                    .commands
                    .send(PgWorkerCommand::Adaptive(command))
                    .map_err(|_| ())
            });
        }
        let host_snapshot = adaptive_host
            .as_ref()
            .map(ServerAdaptiveHostDriver::snapshot);
        forward_control_requests(&adaptive_host_config.controls, host_snapshot, |command| {
            worker
                .client
                .commands
                .send(PgWorkerCommand::Adaptive(command))
                .map_err(|_| ())
        });
        match listener.accept() {
            Ok((stream, _)) => {
                if connections.len() >= limits.max_connections() {
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                configure_stream(&stream, limits)
                    .map_err(PostgresTcpServerError::ListenerConfiguration)?;
                let control = stream
                    .try_clone()
                    .map_err(PostgresTcpServerError::ListenerConfiguration)?;
                let client = worker.client.clone();
                let session_id = next_session_id;
                next_session_id = next_session_id
                    .checked_add(1)
                    .filter(|next| *next != 0)
                    .ok_or(PostgresTcpServerError::SessionIdExhausted)?;
                let join = thread::Builder::new()
                    .name(format!("netbadb-postgres-connection-{session_id}"))
                    .spawn(move || {
                        if let Err(error) = run_pg_connection(stream, session_id, client) {
                            eprintln!("netbadb PostgreSQL connection {session_id} failed: {error}");
                        }
                    })
                    .map_err(PostgresTcpServerError::ThreadSpawn)?;
                connections.push(PgConnection {
                    stream: control,
                    join,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(PostgresTcpServerError::Accept(error)),
        }
    }
    for connection in &connections {
        let _ = connection.stream.shutdown(Shutdown::Both);
    }
    for connection in connections {
        let _ = connection.join.join();
    }
    worker.shutdown()
}

fn reap_pg_connections(connections: &mut Vec<PgConnection>) {
    let mut index = 0;
    while index < connections.len() {
        if connections[index].join.is_finished() {
            let connection = connections.swap_remove(index);
            let _ = connection.join.join();
        } else {
            index += 1;
        }
    }
}

fn configure_stream(stream: &TcpStream, limits: ServerLimits) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(limits.idle_timeout()))?;
    stream.set_write_timeout(Some(limits.write_timeout()))
}

#[derive(Debug)]
enum PgConnectionError {
    Wire(WireError),
    Io(io::Error),
    WorkerStopped,
}

impl fmt::Display for PgConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
            Self::WorkerStopped => formatter.write_str("database worker stopped"),
        }
    }
}

impl From<WireError> for PgConnectionError {
    fn from(error: WireError) -> Self {
        Self::Wire(error)
    }
}

fn run_pg_connection(
    mut stream: TcpStream,
    session_id: u64,
    worker: PgWorkerClient,
) -> Result<(), PgConnectionError> {
    let startup = loop {
        match read_startup_packet(&mut stream)? {
            Some(StartupPacket::SslRequest) => {
                stream.write_all(b"N").map_err(PgConnectionError::Io)?;
                stream.flush().map_err(PgConnectionError::Io)?;
            }
            Some(StartupPacket::CancelRequest { .. }) => return Ok(()),
            Some(StartupPacket::Startup(startup)) => break startup,
            None => return Ok(()),
        }
    };
    let startup_messages = worker.open(session_id, startup)?;
    if let Err(error) = write_messages(&mut stream, &startup_messages) {
        let _ = worker.close(session_id);
        return Err(error);
    }
    if startup_messages
        .iter()
        .any(|message| matches!(message, BackendMessage::ErrorResponse(_)))
    {
        return Ok(());
    }

    let request_result = (|| {
        while let Some(message) = read_frontend_message(&mut stream)? {
            if matches!(message, FrontendMessage::Terminate) {
                break;
            }
            let messages = worker.request(session_id, message)?;
            write_messages(&mut stream, &messages)?;
        }
        Ok(())
    })();
    let close_result = worker.close(session_id);
    let _ = stream.shutdown(Shutdown::Both);
    close_result?;
    request_result
}

fn write_messages(
    stream: &mut TcpStream,
    messages: &[BackendMessage],
) -> Result<(), PgConnectionError> {
    for message in messages {
        write_backend_message(stream, message)?;
    }
    stream.flush().map_err(PgConnectionError::Io)
}

#[derive(Clone)]
struct PgWorkerClient {
    commands: Sender<PgWorkerCommand>,
}

impl PgWorkerClient {
    fn open(
        &self,
        session_id: u64,
        startup: StartupMessage,
    ) -> Result<Vec<BackendMessage>, PgConnectionError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.commands
            .send(PgWorkerCommand::Open {
                session_id,
                startup,
                reply,
            })
            .map_err(|_| PgConnectionError::WorkerStopped)?;
        result
            .recv()
            .map_err(|_| PgConnectionError::WorkerStopped)?
    }

    fn request(
        &self,
        session_id: u64,
        message: FrontendMessage,
    ) -> Result<Vec<BackendMessage>, PgConnectionError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.commands
            .send(PgWorkerCommand::Request {
                session_id,
                message,
                reply,
            })
            .map_err(|_| PgConnectionError::WorkerStopped)?;
        result
            .recv()
            .map_err(|_| PgConnectionError::WorkerStopped)?
    }

    fn close(&self, session_id: u64) -> Result<(), PgConnectionError> {
        let (reply, result) = mpsc::sync_channel(1);
        self.commands
            .send(PgWorkerCommand::Close { session_id, reply })
            .map_err(|_| PgConnectionError::WorkerStopped)?;
        result
            .recv()
            .map_err(|_| PgConnectionError::WorkerStopped)?
    }
}

struct PgDatabaseWorker {
    client: PgWorkerClient,
    join: JoinHandle<Result<(), String>>,
}

impl PgDatabaseWorker {
    fn start(
        tables: Vec<TableBootstrap>,
        policy: SessionPolicy,
        authorization: AuthorizationPolicy,
        adaptive_mode: ServerAdaptiveStartupMode,
    ) -> Result<Self, PostgresTcpServerError> {
        let (commands, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("netbadb-postgres-database-worker".into())
            .spawn(move || {
                let entries = tables
                    .into_iter()
                    .map(|table| (table.path, table.table))
                    .collect();
                let database = match Database::open_tables(entries) {
                    Ok(database) => database,
                    Err(error) => {
                        let _ = ready_tx.send(Err(PgWorkerStartupError::Database(error)));
                        return Ok(());
                    }
                };
                let adaptive = match ServerAdaptiveWorkerRuntime::new(adaptive_mode, &database) {
                    Ok(adaptive) => adaptive,
                    Err(error) => {
                        let _ = ready_tx.send(Err(PgWorkerStartupError::Adaptive(error)));
                        return database.close().map_err(|error| error.to_string());
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return database.close().map_err(|error| error.to_string());
                }
                run_pg_worker(database, policy, authorization, adaptive, receiver)
            })
            .map_err(PostgresTcpServerError::ThreadSpawn)?;
        match ready_rx
            .recv()
            .map_err(|_| PostgresTcpServerError::WorkerStopped)?
        {
            Ok(()) => Ok(Self {
                client: PgWorkerClient { commands },
                join,
            }),
            Err(PgWorkerStartupError::Database(error)) => {
                let _ = join.join();
                Err(PostgresTcpServerError::Database(error))
            }
            Err(PgWorkerStartupError::Adaptive(error)) => {
                let _ = join.join();
                Err(PostgresTcpServerError::AdaptiveConfig(error))
            }
        }
    }

    fn shutdown(self) -> Result<(), PostgresTcpServerError> {
        let (reply, result) = mpsc::sync_channel(1);
        if self
            .client
            .commands
            .send(PgWorkerCommand::Shutdown { reply })
            .is_ok()
        {
            let _ = result.recv();
        }
        match self.join.join() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(PostgresTcpServerError::WorkerClose(message)),
            Err(_) => Err(PostgresTcpServerError::ThreadPanicked),
        }
    }
}

enum PgWorkerCommand {
    Open {
        session_id: u64,
        startup: StartupMessage,
        reply: SyncSender<Result<Vec<BackendMessage>, PgConnectionError>>,
    },
    Request {
        session_id: u64,
        message: FrontendMessage,
        reply: SyncSender<Result<Vec<BackendMessage>, PgConnectionError>>,
    },
    Close {
        session_id: u64,
        reply: SyncSender<Result<(), PgConnectionError>>,
    },
    Adaptive(ServerAdaptiveWorkerCommand),
    Shutdown {
        reply: SyncSender<()>,
    },
}

enum PgWorkerStartupError {
    Database(DatabaseError),
    Adaptive(ServerAdaptiveDriverConfigError),
}

fn run_pg_worker(
    mut database: Database,
    policy: SessionPolicy,
    authorization: AuthorizationPolicy,
    mut adaptive: Option<ServerAdaptiveWorkerRuntime>,
    commands: Receiver<PgWorkerCommand>,
) -> Result<(), String> {
    let mut sessions: HashMap<u64, PgWorkerSession> = HashMap::new();
    while let Ok(command) = commands.recv() {
        match command {
            PgWorkerCommand::Open {
                session_id,
                startup,
                reply,
            } => {
                if sessions.contains_key(&session_id) {
                    let _ = reply.send(Err(PgConnectionError::WorkerStopped));
                    continue;
                }
                let principal = match authorization.admit(&ClientIdentity::LocalPlaintext) {
                    Ok(principal) => principal,
                    Err(_) => {
                        let _ = reply.send(Ok(vec![BackendMessage::ErrorResponse(fixed_error(
                            "28000",
                            "authorization denied for PostgreSQL connection",
                        ))]));
                        continue;
                    }
                };
                let (session, messages) =
                    match PgWorkerSession::new(&database, policy, principal, startup, session_id) {
                        Ok(opened) => opened,
                        Err(error) => {
                            let _ = reply.send(Ok(vec![BackendMessage::ErrorResponse(
                                map_database_error(&error),
                            )]));
                            continue;
                        }
                    };
                sessions.insert(session_id, session);
                let _ = reply.send(Ok(messages));
            }
            PgWorkerCommand::Request {
                session_id,
                message,
                reply,
            } => {
                let Some(session) = sessions.get_mut(&session_id) else {
                    let _ = reply.send(Err(PgConnectionError::WorkerStopped));
                    continue;
                };
                let messages = session.handle_with_adaptive_feedback(
                    &mut database,
                    adaptive
                        .as_mut()
                        .map(ServerAdaptiveWorkerRuntime::feedback_mut),
                    message,
                );
                let _ = reply.send(Ok(messages));
            }
            PgWorkerCommand::Close { session_id, reply } => {
                let result = match sessions.get_mut(&session_id) {
                    Some(session) => session
                        .execution
                        .close()
                        .map_err(|_| PgConnectionError::WorkerStopped),
                    None => Ok(()),
                };
                if result.is_ok() {
                    sessions.remove(&session_id);
                }
                let _ = reply.send(result);
            }
            PgWorkerCommand::Adaptive(command) => match adaptive.as_mut() {
                Some(adaptive) => adaptive.handle(&mut database, command),
                None => handle_disabled_worker_command(command),
            },
            PgWorkerCommand::Shutdown { reply } => {
                for session in sessions.values_mut() {
                    session
                        .execution
                        .close()
                        .map_err(|error| error.to_string())?;
                }
                let _ = reply.send(());
                return database.close().map_err(|error| error.to_string());
            }
        }
    }
    for session in sessions.values_mut() {
        session
            .execution
            .close()
            .map_err(|error| error.to_string())?;
    }
    database.close().map_err(|error| error.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgTransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

impl PgTransactionStatus {
    const fn ready_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

struct PreparedStatement {
    sql: String,
    execution: PreparedExecution,
    parameters: Vec<PostgresOid>,
    /// Exact NetbaDB targets inferred from SQL context. PostgreSQL OIDs in
    /// `parameters` remain transport carriers and are never stored here.
    parameter_targets: Vec<PhysicalType>,
    fields: Vec<FieldDescription>,
    is_query: bool,
}

#[derive(Clone)]
enum PreparedExecution {
    Core(Box<CorePrepared>),
    Ddl(Box<CorePreparedDdl>),
    Compatibility(CompatibilityStatement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompatibilityStatement {
    PsqlRelationLookup,
    PsqlRelationLookupQualified,
    PsqlTableList,
    PsqlIndexList,
    PsqlRelationProperties,
    PsqlColumns,
    PsqlIndexes,
    PsqlPolicies,
    PsqlExtendedStatistics,
    PsqlPublications,
    PsqlInheritanceParents,
    PsqlInheritanceChildren,
    TypeLookup,
    SchemaNames,
    TableNames,
    HasTableVisible,
    HasTableQualified,
    ColumnsVisible,
    ColumnsQualified,
    Domains,
    Enums,
    TableOidsVisible,
    TableOidsQualified,
    PrimaryKeys,
    ForeignKeysVisible,
    ForeignKeysQualified,
    Indexes,
    TableCommentsVisible,
    TableCommentsQualified,
    CheckConstraintsVisible,
    CheckConstraintsQualified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgCompatibilityOperator {
    RegexMatch,
}

#[derive(Debug)]
struct SimpleCompatibilityQuery {
    statement: CompatibilityStatement,
    values: Vec<ScalarValue>,
}

struct Portal {
    statement: String,
    sql: String,
    execution: PreparedExecution,
    values: Vec<ScalarValue>,
    fields: Vec<FieldDescription>,
    result: Option<PortalResult>,
}

struct ReadOnlySavepoint {
    name: String,
    mutation_generation: u64,
}

const SYNTHETIC_OID_BASE: u32 = 0x8000_0000;
const SYNTHETIC_OID_MASK: u32 = 0x1fff_ffff;
const POSTGRES_IDENTIFIER_MAX_BYTES: usize = 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum PgCatalogObjectKey {
    Table(TableId),
    Index {
        table_id: TableId,
        column_id: ColumnId,
    },
}

#[derive(Clone)]
struct PgCompatibilityCatalog {
    tables: Vec<PgCatalogTable>,
}

#[derive(Clone)]
struct PgCatalogTable {
    table_id: TableId,
    oid: u32,
    name: String,
    columns: Vec<PgCatalogColumn>,
    indexes: Vec<PgCatalogIndex>,
    index_reflection_supported: bool,
}

#[derive(Clone)]
struct PgCatalogColumn {
    column_id: ColumnId,
    name: String,
    physical: PhysicalType,
    nullable: bool,
    primary_key: bool,
}

#[derive(Clone)]
struct PgCatalogIndex {
    oid: u32,
    name: String,
    column_id: ColumnId,
    column_name: String,
    column_physical: PhysicalType,
    unique: bool,
    access_method: &'static str,
}

impl PgCompatibilityCatalog {
    fn derive(database: &Database) -> Result<Self, DatabaseError> {
        let inspected = database.inspect_catalog()?;
        let mut identities = Vec::new();
        let mut index_name_inputs = Vec::new();
        let explicit_index_names = inspected
            .tables
            .iter()
            .flat_map(|table| &table.indexes)
            .filter_map(|index| index.name.as_ref().map(|name| name.as_str().to_owned()))
            .collect::<HashSet<_>>();
        for table in &inspected.tables {
            let mut table_hash = Sha256::new();
            table_hash.update(b"netbadb-pg-table-object-oid-v2");
            table_hash.update(table.table_id.0.to_be_bytes());
            table_hash.update(table.fingerprint.as_bytes());
            identities.push((
                PgCatalogObjectKey::Table(table.table_id),
                <[u8; 32]>::from(table_hash.finalize()),
            ));
            for index in &table.indexes {
                let mut index_hash = Sha256::new();
                index_hash.update(b"netbadb-pg-index-object-oid-v1");
                index_hash.update(table.table_id.0.to_be_bytes());
                index_hash.update(table.fingerprint.as_bytes());
                index_hash.update(index.column_id.0.to_be_bytes());
                index_hash.update([match index.kind {
                    IndexKindInspection::BTree => 1,
                }]);
                index_hash.update([u8::from(index.unique)]);
                let digest = <[u8; 32]>::from(index_hash.finalize());
                let key = PgCatalogObjectKey::Index {
                    table_id: table.table_id,
                    column_id: index.column_id,
                };
                identities.push((key, digest));
                if index.name.is_none() {
                    index_name_inputs.push((
                        key,
                        table.name.as_str(),
                        index.column_name.as_str(),
                        digest,
                    ));
                }
            }
        }
        identities.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        let object_oids = assign_synthetic_oids(&identities, SYNTHETIC_OID_BASE);
        let index_names =
            assign_compatibility_index_names_avoiding(&index_name_inputs, explicit_index_names);
        let tables = inspected
            .tables
            .into_iter()
            .map(|table| {
                let columns = table
                    .columns
                    .iter()
                    .map(|column| PgCatalogColumn {
                        column_id: column.column_id,
                        name: column.name.clone(),
                        physical: column.data_type.physical,
                        nullable: column.nullable,
                        primary_key: column.primary_key,
                    })
                    .collect::<Vec<_>>();
                let mut indexes = table
                    .indexes
                    .iter()
                    .map(|index| {
                        let key = PgCatalogObjectKey::Index {
                            table_id: table.table_id,
                            column_id: index.column_id,
                        };
                        let column = columns
                            .iter()
                            .find(|column| column.column_id == index.column_id)
                            .ok_or(DatabaseError::InspectionIndexColumnMissing {
                                table_id: table.table_id,
                                column_id: index.column_id,
                            })?;
                        Ok(PgCatalogIndex {
                            oid: object_oids[&key],
                            name: index.name.as_ref().map_or_else(
                                || index_names[&key].clone(),
                                |name| name.as_str().to_owned(),
                            ),
                            column_id: index.column_id,
                            column_name: index.column_name.clone(),
                            column_physical: column.physical,
                            unique: index.unique,
                            access_method: match index.kind {
                                IndexKindInspection::BTree => "btree",
                            },
                        })
                    })
                    .collect::<Result<Vec<_>, DatabaseError>>()?;
                indexes.sort_by(|left, right| {
                    left.name
                        .cmp(&right.name)
                        .then(left.column_id.cmp(&right.column_id))
                });
                Ok(PgCatalogTable {
                    table_id: table.table_id,
                    oid: object_oids[&PgCatalogObjectKey::Table(table.table_id)],
                    name: table.name,
                    columns,
                    indexes,
                    index_reflection_supported: matches!(
                        table.placement,
                        TablePlacementInspection::Single
                    ),
                })
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        Ok(Self { tables })
    }

    fn table(&self, name: &str) -> Option<&PgCatalogTable> {
        self.tables.iter().find(|table| table.name == name)
    }
}

fn assign_synthetic_oids(
    identities: &[(PgCatalogObjectKey, [u8; 32])],
    base: u32,
) -> HashMap<PgCatalogObjectKey, u32> {
    let mut assigned = HashMap::with_capacity(identities.len());
    let mut used = HashSet::with_capacity(identities.len());
    for (object, digest) in identities {
        let mut candidate = base
            | (u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]])
                & SYNTHETIC_OID_MASK);
        while !used.insert(candidate) {
            candidate = base | (candidate.wrapping_add(1) & SYNTHETIC_OID_MASK);
        }
        assigned.insert(*object, candidate);
    }
    assigned
}

#[cfg(test)]
fn assign_compatibility_index_names(
    inputs: &[(PgCatalogObjectKey, &str, &str, [u8; 32])],
) -> HashMap<PgCatalogObjectKey, String> {
    assign_compatibility_index_names_avoiding(inputs, HashSet::new())
}

fn assign_compatibility_index_names_avoiding(
    inputs: &[(PgCatalogObjectKey, &str, &str, [u8; 32])],
    mut used: HashSet<String>,
) -> HashMap<PgCatalogObjectKey, String> {
    let mut inputs = inputs.to_vec();
    inputs.sort_by(|left, right| left.3.cmp(&right.3).then(left.0.cmp(&right.0)));
    let mut assigned = HashMap::with_capacity(inputs.len());
    for (object, table_name, column_name, digest) in inputs {
        let readable = sanitized_index_name_prefix(table_name, column_name);
        let hash_suffix = digest[..6]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let fixed_bytes = "nb__".len() + hash_suffix.len() + "_idx".len();
        let readable_limit = POSTGRES_IDENTIFIER_MAX_BYTES - fixed_bytes;
        let readable = &readable[..readable.len().min(readable_limit)];
        let base = format!("nb_{readable}_{hash_suffix}_idx");
        let mut candidate = base.clone();
        let mut collision = 0_u32;
        while !used.insert(candidate.clone()) {
            collision = collision.wrapping_add(1);
            let discriminator = format!("_{collision:x}");
            let keep = base
                .len()
                .saturating_sub(discriminator.len())
                .min(POSTGRES_IDENTIFIER_MAX_BYTES - discriminator.len());
            candidate = format!("{}{}", &base[..keep], discriminator);
        }
        assigned.insert(object, candidate);
    }
    assigned
}

fn sanitized_index_name_prefix(table_name: &str, column_name: &str) -> String {
    let mut output = String::new();
    for (position, name) in [table_name, column_name].into_iter().enumerate() {
        if position != 0 && !output.ends_with('_') {
            output.push('_');
        }
        let before = output.len();
        for byte in name.bytes() {
            let normalized = match byte {
                b'a'..=b'z' | b'0'..=b'9' => char::from(byte),
                b'A'..=b'Z' => char::from(byte.to_ascii_lowercase()),
                _ => '_',
            };
            if normalized != '_' || !output.ends_with('_') {
                output.push(normalized);
            }
        }
        if output.len() == before {
            output.push_str("index");
        }
    }
    while output.ends_with('_') {
        output.pop();
    }
    if output.is_empty() {
        output.push_str("index");
    }
    output
}

enum PortalResult {
    Query {
        rows: Vec<Vec<Option<Vec<u8>>>>,
        position: usize,
    },
    Command {
        tag: String,
    },
}

struct PgWorkerSession {
    execution: DatabaseSession,
    authorization: PrincipalAuthorization,
    user: String,
    database_name: String,
    status: PgTransactionStatus,
    prepared: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    awaiting_sync: bool,
    mutation_generation: u64,
    savepoints: Vec<ReadOnlySavepoint>,
    catalog: PgCompatibilityCatalog,
    catalog_generation: u64,
    trace_enabled: bool,
}

impl PgWorkerSession {
    fn new(
        database: &Database,
        policy: SessionPolicy,
        authorization: PrincipalAuthorization,
        startup: StartupMessage,
        session_id: u64,
    ) -> Result<(Self, Vec<BackendMessage>), DatabaseError> {
        let trace_enabled = std::env::var_os("NETBADB_POSTGRES_TRACE").is_some();
        if trace_enabled {
            let parameter_names = startup.parameters.keys().cloned().collect::<Vec<_>>();
            eprintln!(
                "netbadb postgres trace: startup session={session_id} parameter_names={parameter_names:?}"
            );
        }
        let user = startup.parameter("user").unwrap_or("netbadb").to_owned();
        let database_name = startup.parameter("database").unwrap_or(&user).to_owned();
        let process_id = i32::try_from(session_id).unwrap_or(i32::MAX);
        let secret_key = process_id.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        let messages = vec![
            BackendMessage::AuthenticationOk,
            parameter_status("server_version", "16.0 (NetbaDB experimental)"),
            parameter_status("server_encoding", "UTF8"),
            parameter_status("client_encoding", "UTF8"),
            parameter_status("DateStyle", "ISO, MDY"),
            parameter_status("TimeZone", "UTC"),
            parameter_status("integer_datetimes", "on"),
            parameter_status("standard_conforming_strings", "on"),
            BackendMessage::BackendKeyData {
                process_id,
                secret_key,
            },
            BackendMessage::ReadyForQuery(b'I'),
        ];
        Ok((
            Self {
                execution: DatabaseSession::with_policy(policy),
                authorization,
                user,
                database_name,
                status: PgTransactionStatus::Idle,
                prepared: HashMap::new(),
                portals: HashMap::new(),
                awaiting_sync: false,
                mutation_generation: 0,
                savepoints: Vec::new(),
                catalog: PgCompatibilityCatalog::derive(database)?,
                catalog_generation: database.catalog_generation(),
                trace_enabled,
            },
            messages,
        ))
    }

    #[cfg(test)]
    fn handle(&mut self, database: &mut Database, message: FrontendMessage) -> Vec<BackendMessage> {
        self.handle_with_adaptive_feedback(database, None, message)
    }

    fn handle_with_adaptive_feedback(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        message: FrontendMessage,
    ) -> Vec<BackendMessage> {
        self.trace_frontend(&message);
        let messages = if self.awaiting_sync {
            match message {
                FrontendMessage::Sync => {
                    self.awaiting_sync = false;
                    vec![BackendMessage::ReadyForQuery(self.status.ready_byte())]
                }
                _ => Vec::new(),
            }
        } else {
            match message {
                FrontendMessage::Query(sql) => self.simple_query(database, adaptive_feedback, &sql),
                FrontendMessage::Parse {
                    statement,
                    query,
                    parameter_types,
                } => self.parse(database, statement, query, parameter_types),
                FrontendMessage::Bind {
                    portal,
                    statement,
                    parameter_formats,
                    parameters,
                    result_formats,
                } => self.bind(
                    portal,
                    statement,
                    parameter_formats,
                    parameters,
                    result_formats,
                ),
                FrontendMessage::Describe { target, name } => self.describe(target, &name),
                FrontendMessage::Execute { portal, max_rows } => {
                    self.execute_portal(database, adaptive_feedback, &portal, max_rows)
                }
                FrontendMessage::Close { target, name } => self.close_object(target, &name),
                FrontendMessage::Sync => {
                    vec![BackendMessage::ReadyForQuery(self.status.ready_byte())]
                }
                FrontendMessage::Flush => Vec::new(),
                FrontendMessage::Password(_) => {
                    self.extended_error(fixed_error("08P01", "unexpected PasswordMessage"))
                }
                FrontendMessage::Terminate => Vec::new(),
            }
        };
        self.trace_backend_errors(&messages);
        messages
    }

    fn trace_frontend(&self, message: &FrontendMessage) {
        if !self.trace_enabled {
            return;
        }
        match message {
            FrontendMessage::Query(sql) => {
                eprintln!("netbadb postgres trace: Query sql={sql:?}");
            }
            FrontendMessage::Parse {
                statement,
                query,
                parameter_types,
            } => {
                let parameter_oids = parameter_types.iter().map(|oid| oid.0).collect::<Vec<_>>();
                eprintln!(
                    "netbadb postgres trace: Parse statement={statement:?} parameter_oids={parameter_oids:?} sql={query:?}"
                );
            }
            FrontendMessage::Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            } => {
                eprintln!(
                    "netbadb postgres trace: Bind portal={portal:?} statement={statement:?} parameter_count={} parameter_formats={parameter_formats:?} result_formats={result_formats:?}",
                    parameters.len()
                );
            }
            FrontendMessage::Describe { target, name } => {
                eprintln!("netbadb postgres trace: Describe target={target:?} name={name:?}");
            }
            FrontendMessage::Execute { portal, max_rows } => {
                eprintln!("netbadb postgres trace: Execute portal={portal:?} max_rows={max_rows}");
            }
            FrontendMessage::Close { target, name } => {
                eprintln!("netbadb postgres trace: Close target={target:?} name={name:?}");
            }
            FrontendMessage::Sync => eprintln!("netbadb postgres trace: Sync"),
            FrontendMessage::Flush => eprintln!("netbadb postgres trace: Flush"),
            FrontendMessage::Terminate => eprintln!("netbadb postgres trace: Terminate"),
            FrontendMessage::Password(_) => {
                eprintln!("netbadb postgres trace: Password payload=<redacted>");
            }
        }
    }

    fn trace_backend_errors(&self, messages: &[BackendMessage]) {
        if !self.trace_enabled {
            return;
        }
        for message in messages {
            if let BackendMessage::ErrorResponse(error) = message {
                eprintln!(
                    "netbadb postgres trace: ErrorResponse sqlstate={} message={:?}",
                    error.sqlstate, error.message
                );
            }
        }
    }

    fn simple_query(
        &mut self,
        database: &mut Database,
        mut adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        sql: &str,
    ) -> Vec<BackendMessage> {
        self.prepared.remove("");
        self.portals.remove("");
        let statements = split_statements(sql);
        if statements.is_empty() {
            return vec![
                BackendMessage::EmptyQueryResponse,
                BackendMessage::ReadyForQuery(self.status.ready_byte()),
            ];
        }
        let mut messages = Vec::new();
        for statement in statements {
            match self.execute_statement(database, adaptive_feedback.as_deref_mut(), statement) {
                Ok(mut statement_messages) => messages.append(&mut statement_messages),
                Err(error) => {
                    messages.push(BackendMessage::ErrorResponse(error));
                    break;
                }
            }
        }
        messages.push(BackendMessage::ReadyForQuery(self.status.ready_byte()));
        messages
    }

    fn parse(
        &mut self,
        database: &Database,
        statement: String,
        query: String,
        parameter_types: Vec<PostgresOid>,
    ) -> Vec<BackendMessage> {
        if !statement.is_empty() && self.prepared.contains_key(&statement) {
            return self.extended_error(fixed_error("42P05", "prepared statement already exists"));
        }
        if !self.prepared.contains_key(&statement) && self.prepared.len() >= MAX_PREPARED_STATEMENTS
        {
            return self.extended_error(fixed_error(
                "54000",
                "prepared statement session limit reached",
            ));
        }
        let normalized = normalize_sql(&query);
        if is_index_ddl(&normalized) {
            if !normalized.starts_with("create index ") && !normalized.starts_with("drop index ") {
                return self.extended_error(unsupported_index_ddl());
            }
            let prepared = match self.prepare_index_ddl(database, &query) {
                Ok(prepared) => prepared,
                Err(error) => return self.extended_error(error),
            };
            if !parameter_types.is_empty() {
                return self.extended_error(fixed_error(
                    "08P01",
                    "index DDL does not accept bind parameters",
                ));
            }
            if statement.is_empty() {
                self.prepared.remove("");
                self.portals
                    .retain(|_, portal| !portal.statement.is_empty());
            }
            self.prepared.insert(
                statement,
                PreparedStatement {
                    sql: query,
                    execution: PreparedExecution::Ddl(Box::new(prepared)),
                    parameters: Vec::new(),
                    parameter_targets: Vec::new(),
                    fields: Vec::new(),
                    is_query: false,
                },
            );
            return vec![BackendMessage::ParseComplete];
        }
        if is_unsupported_schema_ddl(&normalized) {
            return self.extended_error(unsupported_schema_ddl());
        }
        if let Some(compatibility) = classify_compatibility_statement(&query) {
            if self.trace_enabled {
                eprintln!("netbadb postgres trace: compatibility classification={compatibility:?}");
            }
            return self.parse_compatibility(statement, query, parameter_types, compatibility);
        }
        let declared = match parameter_types
            .iter()
            .map(|oid| parameter_hint(*oid))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(declared) => declared,
            Err(error) => return self.extended_error(error),
        };
        let prepared = match self.execution.prepare(database, &query, &declared) {
            Ok(PreparedSqlStatement::Relational(prepared)) => *prepared,
            Ok(PreparedSqlStatement::Ddl(prepared)) => {
                if let Err(error) = preflight_ddl_types(&prepared) {
                    return self.extended_error(error);
                }
                if statement.is_empty() {
                    self.prepared.remove("");
                    self.portals
                        .retain(|_, portal| !portal.statement.is_empty());
                }
                self.prepared.insert(
                    statement,
                    PreparedStatement {
                        sql: query,
                        execution: PreparedExecution::Ddl(Box::new(prepared)),
                        parameters: Vec::new(),
                        parameter_targets: Vec::new(),
                        fields: Vec::new(),
                        is_query: false,
                    },
                );
                return vec![BackendMessage::ParseComplete];
            }
            Err(error) => return self.extended_error(map_database_error(&error)),
        };
        let description = prepared.description();
        let parameter_targets = prepared
            .parameters()
            .iter()
            .map(|parameter| parameter.data_type.physical)
            .collect::<Vec<_>>();
        let inferred_oids = match prepared
            .parameters()
            .iter()
            .enumerate()
            .map(
                |(index, parameter)| match parameter_types.get(index).copied() {
                    Some(oid) if oid.0 != 0 => {
                        let carrier = PostgresType::from_oid(oid).ok_or_else(|| {
                            map_type_error(TypeMappingError::UnsupportedOid(oid))
                        })?;
                        if !carrier.is_parameter_carrier_for(parameter.data_type.physical) {
                            return Err(fixed_error(
                                "42804",
                                "PostgreSQL parameter type is not a valid carrier for the SQL target type",
                            ));
                        }
                        Ok(oid)
                    }
                    _ => Ok(PostgresType::for_parameter(parameter.data_type.physical).oid()),
                },
            )
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(oids) => oids,
            Err(error) => return self.extended_error(error),
        };
        let fields = match fields_from_description(&description) {
            Ok(fields) => fields,
            Err(error) => return self.extended_error(error),
        };
        if statement.is_empty() {
            self.prepared.remove("");
            self.portals
                .retain(|_, portal| !portal.statement.is_empty());
        }
        self.prepared.insert(
            statement,
            PreparedStatement {
                sql: query,
                execution: PreparedExecution::Core(Box::new(prepared)),
                parameters: inferred_oids,
                parameter_targets,
                fields,
                is_query: description.is_query,
            },
        );
        vec![BackendMessage::ParseComplete]
    }

    fn parse_compatibility(
        &mut self,
        statement: String,
        query: String,
        parameter_types: Vec<PostgresOid>,
        compatibility: CompatibilityStatement,
    ) -> Vec<BackendMessage> {
        const THREE_KIND_TABLE_OID_PARAMETERS: &[PostgresType] = &[
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ];
        let expected_parameters = if matches!(
            compatibility,
            CompatibilityStatement::TableOidsVisible | CompatibilityStatement::TableOidsQualified
        ) && highest_postgres_parameter(&query) == 5
        {
            THREE_KIND_TABLE_OID_PARAMETERS
        } else {
            compatibility_parameter_types(compatibility)
        };
        if parameter_types.len() > expected_parameters.len() {
            return self.extended_error(fixed_error(
                "08P01",
                "Parse declares more parameters than the compatibility query uses",
            ));
        }
        let parameters = expected_parameters
            .iter()
            .enumerate()
            .map(
                |(index, expected)| match parameter_types.get(index).copied() {
                    None | Some(PostgresOid(0)) => Ok(expected.oid()),
                    Some(oid) => {
                        let Some(supplied) = PostgresType::from_oid(oid) else {
                            return Err(map_type_error(TypeMappingError::UnsupportedOid(oid)));
                        };
                        let Some(target) = expected.parameter_fallback() else {
                            return Err(fixed_error(
                                "0A000",
                                "compatibility parameter type has no scalar carrier",
                            ));
                        };
                        if supplied.is_parameter_carrier_for(target) {
                            Ok(oid)
                        } else {
                            Err(fixed_error(
                                "42804",
                                "compatibility query parameter has an incompatible type",
                            ))
                        }
                    }
                },
            )
            .collect::<Result<Vec<_>, _>>();
        let parameters = match parameters {
            Ok(parameters) => parameters,
            Err(error) => return self.extended_error(error),
        };
        let parameter_targets = match expected_parameters
            .iter()
            .map(|data_type| {
                data_type.parameter_fallback().ok_or_else(|| {
                    fixed_error(
                        "0A000",
                        "compatibility parameter type has no scalar carrier",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(targets) => targets,
            Err(error) => return self.extended_error(error),
        };
        if statement.is_empty() {
            self.prepared.remove("");
            self.portals
                .retain(|_, portal| !portal.statement.is_empty());
        }
        self.prepared.insert(
            statement,
            PreparedStatement {
                sql: query,
                execution: PreparedExecution::Compatibility(compatibility),
                parameters,
                parameter_targets,
                fields: compatibility_fields(compatibility),
                is_query: true,
            },
        );
        vec![BackendMessage::ParseComplete]
    }

    fn bind(
        &mut self,
        portal: String,
        statement: String,
        parameter_formats: Vec<FormatCode>,
        parameters: Vec<Option<Vec<u8>>>,
        result_formats: Vec<FormatCode>,
    ) -> Vec<BackendMessage> {
        let Some(prepared) = self.prepared.get(&statement) else {
            return self.extended_error(fixed_error("26000", "prepared statement does not exist"));
        };
        if !portal.is_empty() && self.portals.contains_key(&portal) {
            return self.extended_error(fixed_error("42P03", "portal already exists"));
        }
        if !self.portals.contains_key(&portal) && self.portals.len() >= MAX_PORTALS {
            return self.extended_error(fixed_error("54000", "portal session limit reached"));
        }
        if parameters.len() != prepared.parameters.len() {
            return self.extended_error(fixed_error(
                "08P01",
                "Bind parameter count does not match Parse",
            ));
        }
        let parameter_formats = match expand_formats(
            &parameter_formats,
            parameters.len(),
            "parameter format count does not match parameters",
        ) {
            Ok(formats) => formats,
            Err(error) => return self.extended_error(error),
        };
        let values = match parameters
            .iter()
            .zip(&prepared.parameters)
            .zip(&prepared.parameter_targets)
            .zip(&parameter_formats)
            .map(|(((bytes, oid), target), format)| match format {
                FormatCode::Text => decode_text_parameter_as(bytes.as_deref(), *oid, *target),
                FormatCode::Binary => decode_binary_parameter_as(bytes.as_deref(), *oid, *target),
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(values) => values,
            Err(error) => return self.extended_error(map_type_error(error)),
        };
        let result_formats = match expand_formats(
            &result_formats,
            prepared.fields.len(),
            "result format count does not match result columns",
        ) {
            Ok(formats) => formats,
            Err(error) => return self.extended_error(error),
        };
        let fields = prepared
            .fields
            .iter()
            .cloned()
            .zip(result_formats)
            .map(|(mut field, format)| {
                field.format = format;
                field
            })
            .collect();
        let sql = prepared.sql.clone();
        let execution = prepared.execution.clone();
        if portal.is_empty() {
            self.portals.remove("");
        }
        self.portals.insert(
            portal,
            Portal {
                statement,
                sql,
                execution,
                values,
                fields,
                result: None,
            },
        );
        vec![BackendMessage::BindComplete]
    }

    fn describe(&mut self, target: DescribeTarget, name: &str) -> Vec<BackendMessage> {
        match target {
            DescribeTarget::Statement => {
                let Some(statement) = self.prepared.get(name) else {
                    return self
                        .extended_error(fixed_error("26000", "prepared statement does not exist"));
                };
                let mut messages = vec![BackendMessage::ParameterDescription(
                    statement.parameters.clone(),
                )];
                if statement.is_query {
                    messages.push(BackendMessage::RowDescription(statement.fields.clone()));
                } else {
                    messages.push(BackendMessage::NoData);
                }
                messages
            }
            DescribeTarget::Portal => {
                let Some(portal) = self.portals.get(name) else {
                    return self.extended_error(fixed_error("34000", "portal does not exist"));
                };
                if portal.fields.is_empty() {
                    vec![BackendMessage::NoData]
                } else {
                    vec![BackendMessage::RowDescription(portal.fields.clone())]
                }
            }
        }
    }

    fn execute_portal(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        name: &str,
        max_rows: u32,
    ) -> Vec<BackendMessage> {
        if self.status == PgTransactionStatus::Failed {
            return self.extended_error(fixed_error(
                "25P02",
                "current transaction is aborted; ROLLBACK is required",
            ));
        }
        let Some(mut portal) = self.portals.remove(name) else {
            return self.extended_error(fixed_error("34000", "portal does not exist"));
        };
        if portal.result.is_none() {
            let result = match self.execute_to_portal(
                database,
                adaptive_feedback,
                &portal.sql,
                &portal.execution,
                &portal.values,
                &portal.fields,
            ) {
                Ok(result) => result,
                Err(error) => {
                    self.portals.insert(name.to_owned(), portal);
                    return self.extended_error(error);
                }
            };
            portal.result = Some(result);
        }
        let Some(result) = portal.result.as_mut() else {
            self.portals.insert(name.to_owned(), portal);
            return self.extended_error(fixed_error("XX000", "portal result was not initialized"));
        };
        let messages = portal_messages(result, max_rows);
        self.portals.insert(name.to_owned(), portal);
        messages
    }

    fn execute_to_portal(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        sql: &str,
        execution: &PreparedExecution,
        values: &[ScalarValue],
        fields: &[FieldDescription],
    ) -> Result<PortalResult, ErrorResponse> {
        let result = match execution {
            PreparedExecution::Core(prepared) => {
                self.execute_prepared_core(database, adaptive_feedback, prepared, values)?
            }
            PreparedExecution::Ddl(prepared) => {
                if !values.is_empty() {
                    return Err(fixed_error("08P01", "DDL does not accept parameters"));
                }
                preflight_ddl_types(prepared)?;
                self.authorize_access(&prepared.access())?;
                let outcome = self
                    .execution
                    .execute_ddl(database, prepared)
                    .map_err(|error| self.record_error(&error))?;
                if outcome != DdlOutcome::Unchanged {
                    self.mutation_generation = self.mutation_generation.saturating_add(1);
                }
                self.refresh_catalog(database)?;
                return Ok(PortalResult::Command {
                    tag: ddl_command_tag(prepared).into(),
                });
            }
            PreparedExecution::Compatibility(statement) => {
                self.refresh_catalog(database)?;
                ExecutionResult::Query(execute_compatibility_statement(
                    &self.catalog,
                    &self.authorization,
                    *statement,
                    values,
                )?)
            }
        };
        match result {
            ExecutionResult::Query(query) => Ok(PortalResult::Query {
                rows: encode_query_rows(&query, fields, self.execution.policy.max_result_rows())?,
                position: 0,
            }),
            ExecutionResult::AffectedRows(count) => {
                self.mutation_generation = self.mutation_generation.saturating_add(1);
                Ok(PortalResult::Command {
                    tag: command_tag(sql, count),
                })
            }
        }
    }

    fn prepare_index_ddl(
        &mut self,
        database: &Database,
        sql: &str,
    ) -> Result<CorePreparedDdl, ErrorResponse> {
        let prepared = match self
            .execution
            .prepare(database, sql, &[])
            .map_err(|error| map_create_index_error(&error))?
        {
            PreparedSqlStatement::Ddl(prepared) => prepared,
            PreparedSqlStatement::Relational(_) => return Err(unsupported_index_ddl()),
        };
        if let Some(name) = prepared.unresolved_index_name() {
            self.refresh_catalog(database)?;
            // Compatibility aliases are resolved only within visible tables.
            // A guessed alias for a denied table behaves exactly like absence.
            for table in self
                .catalog
                .tables
                .iter()
                .filter(|table| self.authorization.can_see(table.table_id))
            {
                if let Some(index) = table
                    .indexes
                    .iter()
                    .find(|index| index.name == name.as_str())
                {
                    let definition = database
                        .indexes(table.table_id)
                        .map_err(|error| map_database_error(&error))?
                        .iter()
                        .find(|definition| {
                            definition.column_id == index.column_id && definition.name.is_none()
                        })
                        .ok_or_else(|| fixed_error("42704", "index does not exist"))?;
                    return database
                        .prepare_drop_index(
                            table.table_id,
                            definition.id,
                            prepared.index_drop_if_exists(),
                        )
                        .map_err(|error| map_database_error(&error));
                }
            }
        }
        Ok(prepared)
    }

    fn refresh_catalog(&mut self, database: &Database) -> Result<(), ErrorResponse> {
        let generation = database.catalog_generation();
        if generation != self.catalog_generation {
            self.catalog = PgCompatibilityCatalog::derive(database)
                .map_err(|error| map_database_error(&error))?;
            self.catalog_generation = generation;
        }
        Ok(())
    }

    fn close_object(&mut self, target: CloseTarget, name: &str) -> Vec<BackendMessage> {
        match target {
            CloseTarget::Statement => {
                self.prepared.remove(name);
                self.portals.retain(|_, portal| portal.statement != name);
            }
            CloseTarget::Portal => {
                self.portals.remove(name);
            }
        }
        vec![BackendMessage::CloseComplete]
    }

    fn execute_statement(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        sql: &str,
    ) -> Result<Vec<BackendMessage>, ErrorResponse> {
        let normalized = normalize_sql(sql);
        match normalized.as_str() {
            "begin" | "begin transaction" | "start transaction" => return self.begin(database),
            "commit" | "commit transaction" => return self.commit(database),
            "rollback" | "rollback transaction" => return self.rollback(),
            "deallocate all" => {
                self.prepared.clear();
                self.portals.clear();
                return Ok(vec![BackendMessage::CommandComplete("DEALLOCATE".into())]);
            }
            _ => {}
        }
        if let Some(name) = transaction_control_name(&normalized, "deallocate prepare ")
            .or_else(|| transaction_control_name(&normalized, "deallocate "))
        {
            self.prepared.remove(name);
            self.portals.retain(|_, portal| portal.statement != name);
            return Ok(vec![BackendMessage::CommandComplete("DEALLOCATE".into())]);
        }
        if let Some(name) = transaction_control_name(&normalized, "savepoint ") {
            return self.savepoint(name);
        }
        if let Some(name) = transaction_control_name(&normalized, "release savepoint ")
            .or_else(|| transaction_control_name(&normalized, "release "))
        {
            return self.release_savepoint(name);
        }
        if let Some(name) = transaction_control_name(&normalized, "rollback to savepoint ")
            .or_else(|| transaction_control_name(&normalized, "rollback to "))
        {
            return self.rollback_to_savepoint(name);
        }
        if self.status == PgTransactionStatus::Failed {
            return Err(fixed_error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        self.refresh_catalog(database)?;
        if let Some(messages) = self.compatibility_query(&normalized) {
            if self.trace_enabled {
                eprintln!("netbadb postgres trace: compatibility classification=ScalarCommand");
            }
            return Ok(messages);
        }
        if normalized.contains("pg_catalog.")
            && ["insert ", "update ", "delete "]
                .into_iter()
                .any(|prefix| normalized.starts_with(prefix))
        {
            return Err(fixed_error(
                "0A000",
                "the PostgreSQL compatibility catalog is read-only",
            ));
        }
        if let Some(query) = classify_simple_compatibility_query(sql)
            .map_err(|error| self.record_protocol_error(error))?
        {
            if self.trace_enabled {
                eprintln!(
                    "netbadb postgres trace: compatibility classification={:?}",
                    query.statement
                );
            }
            let fields = compatibility_fields(query.statement);
            let result = execute_compatibility_statement_for_owner(
                &self.catalog,
                &self.authorization,
                &self.user,
                query.statement,
                &query.values,
            )
            .map_err(|error| self.record_protocol_error(error))?;
            return query_messages_with_fields(
                result,
                fields,
                self.execution.policy.max_result_rows(),
            )
            .map_err(|error| self.record_protocol_error(error));
        }
        if classify_compatibility_statement(sql).is_some() {
            return Err(fixed_error(
                "0A000",
                "parameterized catalog reflection requires Extended Query",
            ));
        }
        if is_index_ddl(&normalized) {
            if !normalized.starts_with("create index ") && !normalized.starts_with("drop index ") {
                return Err(self.record_protocol_error(unsupported_index_ddl()));
            }
            let prepared = self
                .prepare_index_ddl(database, sql)
                .map_err(|error| self.record_protocol_error(error))?;
            self.authorize_access(&prepared.access())?;
            let outcome = self
                .execution
                .execute_ddl(database, &prepared)
                .map_err(|error| self.record_error(&error))?;
            if outcome != DdlOutcome::Unchanged {
                self.mutation_generation = self.mutation_generation.saturating_add(1);
            }
            self.refresh_catalog(database)?;
            return Ok(vec![BackendMessage::CommandComplete(
                if prepared.is_index_drop() {
                    "DROP INDEX"
                } else {
                    "CREATE INDEX"
                }
                .into(),
            )]);
        }
        if is_unsupported_schema_ddl(&normalized) {
            return Err(self.record_protocol_error(unsupported_schema_ddl()));
        }
        let prepared = self
            .execution
            .prepare(database, sql, &[])
            .map_err(|error| self.record_error(&error))?;
        let result = match prepared {
            PreparedSqlStatement::Relational(prepared) => {
                self.execute_prepared_core(database, adaptive_feedback, &prepared, &[])?
            }
            PreparedSqlStatement::Ddl(prepared) => {
                preflight_ddl_types(&prepared).map_err(|e| self.record_protocol_error(e))?;
                self.authorize_access(&prepared.access())?;
                self.execution
                    .execute_ddl(database, &prepared)
                    .map_err(|e| self.record_error(&e))?;
                self.mutation_generation = self.mutation_generation.saturating_add(1);
                self.refresh_catalog(database)?;
                return Ok(vec![BackendMessage::CommandComplete(
                    ddl_command_tag(&prepared).into(),
                )]);
            }
        };
        match result {
            ExecutionResult::Query(query) => {
                query_messages(query, self.execution.policy.max_result_rows())
                    .map_err(|error| self.record_protocol_error(error))
            }
            ExecutionResult::AffectedRows(count) => {
                self.mutation_generation = self.mutation_generation.saturating_add(1);
                Ok(vec![BackendMessage::CommandComplete(command_tag(
                    sql, count,
                ))])
            }
        }
    }

    fn execute_prepared_core(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        prepared: &CorePrepared,
        values: &[ScalarValue],
    ) -> Result<ExecutionResult, ErrorResponse> {
        if self.status == PgTransactionStatus::Failed {
            return Err(fixed_error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        self.authorize_access(&prepared.access())?;
        execute_prepared_with_optional_server_feedback(
            adaptive_feedback,
            &mut self.execution,
            database,
            prepared,
            values,
        )
        .map_err(|error| self.record_error(&error))
    }

    fn authorize_access(&mut self, access: &StatementAccess) -> Result<(), ErrorResponse> {
        self.authorization
            .authorize_statement(access, &self.execution)
            .map_err(|_| {
                self.record_protocol_error(fixed_error(
                    "42501",
                    "permission denied for schema or relation",
                ))
            })
    }

    fn record_failure(&mut self) {
        // An uncertain schema commit retains its handle for COMMIT retry. It
        // must not be converted into an abort-only PG transaction at Sync.
        if self.execution.transaction_state() == WireTransactionState::CommitPending {
            self.status = PgTransactionStatus::InTransaction;
        } else if self.status == PgTransactionStatus::InTransaction
            || self.execution.transaction_state() == WireTransactionState::RollbackPending
        {
            self.status = PgTransactionStatus::Failed;
        }
    }

    fn record_error(&mut self, error: &DatabaseError) -> ErrorResponse {
        self.record_failure();
        map_database_error(error)
    }

    fn record_protocol_error(&mut self, error: ErrorResponse) -> ErrorResponse {
        self.record_failure();
        error
    }

    fn begin(&mut self, database: &mut Database) -> Result<Vec<BackendMessage>, ErrorResponse> {
        if self.status != PgTransactionStatus::Idle {
            return Err(fixed_error("25001", "transaction is already active"));
        }
        if !self.authorization.can_start_transaction() {
            return Err(fixed_error(
                "42501",
                "permission denied to start transaction",
            ));
        }
        let transaction_anchor = self
            .catalog
            .tables
            .iter()
            .find(|t| self.authorization.can_see(t.table_id))
            .map(|t| t.table_id);
        if transaction_anchor.is_none() && !self.authorization.schema_admin() {
            return Err(fixed_error("42501", "no authorized transaction anchor"));
        }
        self.execution
            .begin(database, transaction_anchor)
            .map_err(|error| map_database_error(&error))?;
        self.status = PgTransactionStatus::InTransaction;
        self.savepoints.clear();
        Ok(vec![BackendMessage::CommandComplete("BEGIN".into())])
    }

    fn commit(&mut self, database: &mut Database) -> Result<Vec<BackendMessage>, ErrorResponse> {
        match self.status {
            PgTransactionStatus::Idle => Ok(vec![BackendMessage::CommandComplete("COMMIT".into())]),
            PgTransactionStatus::Failed => Err(fixed_error(
                "25P02",
                "current transaction is aborted; ROLLBACK is required",
            )),
            PgTransactionStatus::InTransaction => {
                self.execution
                    .commit(database)
                    .map_err(|error| map_database_error(&error))?;
                self.status = PgTransactionStatus::Idle;
                self.portals.clear();
                self.savepoints.clear();
                Ok(vec![BackendMessage::CommandComplete("COMMIT".into())])
            }
        }
    }

    fn rollback(&mut self) -> Result<Vec<BackendMessage>, ErrorResponse> {
        if self.execution.transaction_state() != WireTransactionState::None {
            self.execution
                .rollback()
                .map_err(|error| map_database_error(&error))?;
        }
        self.status = PgTransactionStatus::Idle;
        self.portals.clear();
        self.savepoints.clear();
        Ok(vec![BackendMessage::CommandComplete("ROLLBACK".into())])
    }

    fn savepoint(&mut self, name: &str) -> Result<Vec<BackendMessage>, ErrorResponse> {
        if self.status != PgTransactionStatus::InTransaction {
            return Err(fixed_error(
                "25P01",
                "SAVEPOINT can only be used in transaction blocks",
            ));
        }
        self.savepoints.push(ReadOnlySavepoint {
            name: name.to_owned(),
            mutation_generation: self.mutation_generation,
        });
        Ok(vec![BackendMessage::CommandComplete("SAVEPOINT".into())])
    }

    fn release_savepoint(&mut self, name: &str) -> Result<Vec<BackendMessage>, ErrorResponse> {
        if self.status == PgTransactionStatus::Failed {
            return Err(fixed_error(
                "25P02",
                "current transaction is aborted, commands ignored until rollback",
            ));
        }
        let Some(position) = self
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)
        else {
            return Err(fixed_error("3B001", "savepoint does not exist"));
        };
        self.savepoints.truncate(position);
        Ok(vec![BackendMessage::CommandComplete("RELEASE".into())])
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<Vec<BackendMessage>, ErrorResponse> {
        let Some(position) = self
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)
        else {
            return Err(fixed_error("3B001", "savepoint does not exist"));
        };
        if self.savepoints[position].mutation_generation != self.mutation_generation {
            return Err(fixed_error(
                "0A000",
                "rollback to a savepoint after a write is not supported",
            ));
        }
        self.savepoints.truncate(position + 1);
        self.status = PgTransactionStatus::InTransaction;
        Ok(vec![BackendMessage::CommandComplete("ROLLBACK".into())])
    }

    fn compatibility_query(&self, normalized: &str) -> Option<Vec<BackendMessage>> {
        let (name, data_type, value, tag) = match normalized {
            "show client_encoding" => (
                "client_encoding",
                PostgresType::Text,
                "UTF8".to_owned(),
                "SHOW",
            ),
            "show datestyle" => (
                "DateStyle",
                PostgresType::Text,
                "ISO, MDY".to_owned(),
                "SHOW",
            ),
            "show timezone" => ("TimeZone", PostgresType::Text, "UTC".to_owned(), "SHOW"),
            "show transaction isolation level" => (
                "transaction_isolation",
                PostgresType::Text,
                "read committed".to_owned(),
                "SHOW",
            ),
            "show standard_conforming_strings" => (
                "standard_conforming_strings",
                PostgresType::Text,
                "on".to_owned(),
                "SHOW",
            ),
            "show search_path" => (
                "search_path",
                PostgresType::Text,
                "public".to_owned(),
                "SHOW",
            ),
            "select version()" | "select pg_catalog.version()" => (
                "version",
                PostgresType::Text,
                "PostgreSQL 16.0 (NetbaDB experimental compatibility profile)".to_owned(),
                "SELECT 1",
            ),
            "select current_database()" => (
                "current_database",
                PostgresType::Text,
                self.database_name.clone(),
                "SELECT 1",
            ),
            "select current_schema()" => (
                "current_schema",
                PostgresType::Text,
                "public".to_owned(),
                "SELECT 1",
            ),
            "select current_user" | "select current_user()" => (
                "current_user",
                PostgresType::Text,
                self.user.clone(),
                "SELECT 1",
            ),
            _ => return None,
        };
        Some(vec![
            BackendMessage::RowDescription(vec![FieldDescription {
                name: name.into(),
                table_oid: 0,
                column_attribute: 0,
                data_type,
                type_modifier: -1,
                format: FormatCode::Text,
            }]),
            BackendMessage::DataRow(vec![Some(value.into_bytes())]),
            BackendMessage::CommandComplete(tag.into()),
        ])
    }

    fn extended_error(&mut self, error: ErrorResponse) -> Vec<BackendMessage> {
        self.record_failure();
        self.awaiting_sync = true;
        vec![BackendMessage::ErrorResponse(error)]
    }
}

fn highest_postgres_parameter(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut highest = 0_usize;
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'$' {
            cursor += 1;
            continue;
        }
        cursor += 1;
        let start = cursor;
        let mut value = 0_usize;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            value = value
                .saturating_mul(10)
                .saturating_add(usize::from(bytes[cursor] - b'0'));
            cursor += 1;
        }
        if cursor != start {
            highest = highest.max(value);
        }
    }
    highest
}

#[derive(Debug, PartialEq, Eq)]
enum PgCompatibilityToken<'a> {
    Word(&'a str),
    Identifier(String),
    String(String),
    Symbol(u8),
}

fn classify_simple_compatibility_query(
    sql: &str,
) -> Result<Option<SimpleCompatibilityQuery>, ErrorResponse> {
    let normalized = normalize_sql(sql);
    if !normalized.starts_with("select ")
        || !(normalized.contains("pg_catalog.pg_class")
            || normalized.contains("pg_catalog.pg_attribute")
            || normalized.contains("pg_catalog.pg_index")
            || normalized.contains("pg_catalog.pg_policy")
            || normalized.contains("pg_catalog.pg_statistic_ext"))
    {
        return Ok(None);
    }
    let tokens = lex_compatibility_query(sql)?;
    if let Some(class_alias) = relation_alias(&tokens, "pg_catalog", "pg_class") {
        if let Some(namespace_alias) = relation_alias(&tokens, "pg_catalog", "pg_namespace") {
            if !normalized.contains("pg_catalog.pg_get_userbyid(")
                && has_column_reference(&tokens, class_alias, "oid")
                && has_column_reference(&tokens, class_alias, "relname")
                && has_column_reference(&tokens, class_alias, "relnamespace")
                && has_column_reference(&tokens, namespace_alias, "oid")
                && has_column_reference(&tokens, namespace_alias, "nspname")
            {
                if let Some((PgCompatibilityOperator::RegexMatch, pattern)) =
                    column_pattern(&tokens, class_alias, "relname")
                {
                    PgCatalogPattern::compile(&pattern)?;
                    if has_visible_table_predicate(&tokens, class_alias) {
                        return Ok(Some(SimpleCompatibilityQuery {
                            statement: CompatibilityStatement::PsqlRelationLookup,
                            values: vec![ScalarValue::Text(pattern)],
                        }));
                    }
                    if let Some((PgCompatibilityOperator::RegexMatch, schema_pattern)) =
                        column_pattern(&tokens, namespace_alias, "nspname")
                    {
                        PgCatalogPattern::compile(&schema_pattern)?;
                        return Ok(Some(SimpleCompatibilityQuery {
                            statement: CompatibilityStatement::PsqlRelationLookupQualified,
                            values: vec![
                                ScalarValue::Text(schema_pattern),
                                ScalarValue::Text(pattern),
                            ],
                        }));
                    }
                }
            }
        }
        if let Some(namespace_alias) = relation_alias(&tokens, "pg_catalog", "pg_namespace") {
            if relation_alias(&tokens, "pg_catalog", "pg_am").is_some()
                && normalized.contains("pg_catalog.pg_get_userbyid(")
                && ["relname", "relkind", "relowner", "relnamespace"]
                    .into_iter()
                    .all(|column| has_column_reference(&tokens, class_alias, column))
                && has_column_reference(&tokens, namespace_alias, "oid")
                && has_column_reference(&tokens, namespace_alias, "nspname")
            {
                let schema_pattern = column_pattern(&tokens, namespace_alias, "nspname")
                    .map(|(_, pattern)| PgCatalogPattern::compile(&pattern).map(|_| pattern))
                    .transpose()?;
                let name_pattern = column_pattern(&tokens, class_alias, "relname")
                    .map(|(_, pattern)| PgCatalogPattern::compile(&pattern).map(|_| pattern))
                    .transpose()?;
                let values = vec![
                    schema_pattern.map_or(ScalarValue::Null, ScalarValue::Text),
                    name_pattern.map_or(ScalarValue::Null, ScalarValue::Text),
                ];
                if relation_alias(&tokens, "pg_catalog", "pg_index").is_some()
                    && normalized.contains("indexrelid")
                    && normalized.contains("indrelid")
                {
                    return Ok(Some(SimpleCompatibilityQuery {
                        statement: CompatibilityStatement::PsqlIndexList,
                        values,
                    }));
                }
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlTableList,
                    values,
                }));
            }
        }
        if relation_alias(&tokens, "pg_catalog", "pg_am").is_some()
            && [
                "relchecks",
                "relkind",
                "relhasindex",
                "relhasrules",
                "relhastriggers",
                "relrowsecurity",
                "relforcerowsecurity",
                "relispartition",
                "reltablespace",
                "reloftype",
                "relpersistence",
                "relreplident",
            ]
            .into_iter()
            .all(|column| has_column_reference(&tokens, class_alias, column))
        {
            if let Some(oid) = relation_oid_literal(&tokens, class_alias, "oid")? {
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlRelationProperties,
                    values: vec![ScalarValue::Int64(i64::from(oid))],
                }));
            }
        }
        if let Some(index_alias) = relation_alias(&tokens, "pg_catalog", "pg_index") {
            if relation_alias(&tokens, "pg_catalog", "pg_constraint").is_some()
                && normalized.contains("pg_catalog.pg_get_indexdef(")
                && normalized.contains("pg_catalog.pg_get_constraintdef(")
                && [
                    "indrelid",
                    "indexrelid",
                    "indisprimary",
                    "indisunique",
                    "indisclustered",
                    "indisvalid",
                    "indisreplident",
                ]
                .into_iter()
                .all(|column| has_column_reference(&tokens, index_alias, column))
            {
                if let Some(oid) = relation_oid_literal(&tokens, class_alias, "oid")? {
                    return Ok(Some(SimpleCompatibilityQuery {
                        statement: CompatibilityStatement::PsqlIndexes,
                        values: vec![ScalarValue::Int64(i64::from(oid))],
                    }));
                }
            }
        }
    }
    if let Some(attribute_alias) = relation_alias(&tokens, "pg_catalog", "pg_attribute") {
        if relation_alias(&tokens, "pg_catalog", "pg_attrdef").is_some()
            && relation_alias(&tokens, "pg_catalog", "pg_collation").is_some()
            && relation_alias(&tokens, "pg_catalog", "pg_type").is_some()
            && normalized.contains("pg_catalog.format_type(")
            && normalized.contains("pg_catalog.pg_get_expr(")
            && [
                "attname",
                "atttypid",
                "atttypmod",
                "attrelid",
                "attnum",
                "attnotnull",
                "attcollation",
                "attidentity",
                "attgenerated",
                "attisdropped",
            ]
            .into_iter()
            .all(|column| has_column_reference(&tokens, attribute_alias, column))
        {
            if let Some(oid) = relation_oid_literal(&tokens, attribute_alias, "attrelid")? {
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlColumns,
                    values: vec![ScalarValue::Int64(i64::from(oid))],
                }));
            }
        }
    }
    if let Some(policy_alias) = relation_alias(&tokens, "pg_catalog", "pg_policy") {
        if relation_alias(&tokens, "pg_catalog", "pg_roles").is_some()
            && normalized.contains("pg_catalog.pg_get_expr(")
            && [
                "polname",
                "polpermissive",
                "polroles",
                "polqual",
                "polrelid",
                "polwithcheck",
                "polcmd",
            ]
            .into_iter()
            .all(|column| has_column_reference(&tokens, policy_alias, column))
        {
            if let Some(oid) = relation_oid_literal(&tokens, policy_alias, "polrelid")? {
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlPolicies,
                    values: vec![ScalarValue::Int64(i64::from(oid))],
                }));
            }
        }
    }
    if normalized.contains(" from pg_catalog.pg_statistic_ext ")
        && normalized.contains("pg_catalog.pg_get_statisticsobjdef_columns(")
        && [
            "oid",
            "stxrelid",
            "stxnamespace",
            "stxname",
            "stxkind",
            "stxstattarget",
        ]
        .into_iter()
        .all(|column| normalized.contains(column))
    {
        if let Some(oid) = unqualified_oid_literal(&tokens, "stxrelid")? {
            return Ok(Some(SimpleCompatibilityQuery {
                statement: CompatibilityStatement::PsqlExtendedStatistics,
                values: vec![ScalarValue::Int64(i64::from(oid))],
            }));
        }
    }
    if relation_alias(&tokens, "pg_catalog", "pg_publication").is_some()
        && relation_alias(&tokens, "pg_catalog", "pg_publication_namespace").is_some()
        && relation_alias(&tokens, "pg_catalog", "pg_publication_rel").is_some()
        && normalized.contains("pg_catalog.pg_relation_is_publishable(")
        && normalized.contains(" union ")
        && normalized.contains("pubname")
    {
        if let Some(class_alias) = relation_alias(&tokens, "pg_catalog", "pg_class") {
            if let Some(oid) = relation_oid_literal(&tokens, class_alias, "oid")? {
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlPublications,
                    values: vec![ScalarValue::Int64(i64::from(oid))],
                }));
            }
        }
    }
    if let Some(inherits_alias) = relation_alias(&tokens, "pg_catalog", "pg_inherits") {
        if normalized.contains("::pg_catalog.regclass")
            && ["inhparent", "inhrelid"]
                .into_iter()
                .all(|column| has_column_reference(&tokens, inherits_alias, column))
            && normalized.contains("inhseqno")
        {
            if let Some(oid) = relation_oid_literal(&tokens, inherits_alias, "inhrelid")? {
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlInheritanceParents,
                    values: vec![ScalarValue::Int64(i64::from(oid))],
                }));
            }
        }
    }
    if let Some(inherits_alias) = relation_alias(&tokens, "pg_catalog", "pg_inherits") {
        if normalized.contains("::pg_catalog.regclass")
            && normalized.contains("pg_catalog.pg_get_expr(")
            && normalized.contains("relpartbound")
            && normalized.contains("inhdetachpending")
            && ["inhparent", "inhrelid"]
                .into_iter()
                .all(|column| has_column_reference(&tokens, inherits_alias, column))
        {
            if let Some(oid) = relation_oid_literal(&tokens, inherits_alias, "inhparent")? {
                return Ok(Some(SimpleCompatibilityQuery {
                    statement: CompatibilityStatement::PsqlInheritanceChildren,
                    values: vec![ScalarValue::Int64(i64::from(oid))],
                }));
            }
        }
    }
    Ok(None)
}

fn lex_compatibility_query(sql: &str) -> Result<Vec<PgCompatibilityToken<'_>>, ErrorResponse> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    let mut nesting = 0_usize;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }
        let token = match bytes[index] {
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'$'))
                {
                    index += 1;
                }
                PgCompatibilityToken::Word(&sql[start..index])
            }
            b'0'..=b'9' => {
                let start = index;
                index += 1;
                while index < bytes.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
                PgCompatibilityToken::Word(&sql[start..index])
            }
            b'\'' => {
                index += 1;
                let mut value = String::new();
                let mut segment = index;
                loop {
                    let Some(relative) = bytes[index..].iter().position(|byte| *byte == b'\'')
                    else {
                        return Err(fixed_error(
                            "42601",
                            "unterminated string in PostgreSQL compatibility query",
                        ));
                    };
                    let quote = index + relative;
                    value.push_str(&sql[segment..quote]);
                    if bytes.get(quote + 1) == Some(&b'\'') {
                        value.push('\'');
                        index = quote + 2;
                        segment = index;
                    } else {
                        index = quote + 1;
                        break;
                    }
                    if value.len() > MAX_CATALOG_PATTERN_BYTES {
                        return Err(fixed_error(
                            "54000",
                            "PostgreSQL compatibility literal exceeds the pattern limit",
                        ));
                    }
                }
                if value.len() > MAX_CATALOG_PATTERN_BYTES {
                    return Err(fixed_error(
                        "54000",
                        "PostgreSQL compatibility literal exceeds the pattern limit",
                    ));
                }
                PgCompatibilityToken::String(value)
            }
            b'"' => {
                index += 1;
                let mut value = String::new();
                loop {
                    let Some(relative) = bytes[index..].iter().position(|byte| *byte == b'"')
                    else {
                        return Err(fixed_error(
                            "42601",
                            "unterminated quoted identifier in PostgreSQL compatibility query",
                        ));
                    };
                    let quote = index + relative;
                    value.push_str(&sql[index..quote]);
                    if bytes.get(quote + 1) == Some(&b'"') {
                        value.push('"');
                        index = quote + 2;
                    } else {
                        index = quote + 1;
                        break;
                    }
                    if value.len() > MAX_CATALOG_PATTERN_BYTES {
                        return Err(fixed_error(
                            "54000",
                            "PostgreSQL compatibility identifier exceeds the configured limit",
                        ));
                    }
                }
                if value.len() > MAX_CATALOG_PATTERN_BYTES {
                    return Err(fixed_error(
                        "54000",
                        "PostgreSQL compatibility identifier exceeds the configured limit",
                    ));
                }
                PgCompatibilityToken::Identifier(value)
            }
            byte @ (b'.' | b',' | b'(' | b')' | b'=' | b'~' | b'*' | b';' | b':' | b'<' | b'>'
            | b'[' | b']' | b'!') => {
                index += 1;
                if byte == b'(' {
                    nesting = nesting.checked_add(1).ok_or_else(|| {
                        fixed_error("54000", "PostgreSQL compatibility nesting is too deep")
                    })?;
                    if nesting > MAX_COMPATIBILITY_NESTING {
                        return Err(fixed_error(
                            "54000",
                            "PostgreSQL compatibility nesting is too deep",
                        ));
                    }
                } else if byte == b')' {
                    nesting = nesting.checked_sub(1).ok_or_else(|| {
                        fixed_error("42601", "unbalanced PostgreSQL compatibility parentheses")
                    })?;
                }
                PgCompatibilityToken::Symbol(byte)
            }
            _ => {
                return Err(fixed_error(
                    "42601",
                    "unsupported token in PostgreSQL compatibility query",
                ));
            }
        };
        tokens.push(token);
        if tokens.len() > MAX_COMPATIBILITY_TOKENS {
            return Err(fixed_error(
                "54000",
                "PostgreSQL compatibility query has too many tokens",
            ));
        }
    }
    if nesting != 0 {
        return Err(fixed_error(
            "42601",
            "unbalanced PostgreSQL compatibility parentheses",
        ));
    }
    Ok(tokens)
}

fn relation_alias<'a>(
    tokens: &'a [PgCompatibilityToken<'a>],
    schema: &str,
    relation: &str,
) -> Option<&'a str> {
    for index in 0..tokens.len() {
        if !qualified_name_at(tokens, index, schema, relation) {
            continue;
        }
        let mut alias_index = index + 3;
        if token_word_eq(tokens.get(alias_index), "as") {
            alias_index += 1;
        }
        if let Some(PgCompatibilityToken::Word(alias)) = tokens.get(alias_index) {
            return Some(alias);
        }
        if let Some(PgCompatibilityToken::Identifier(alias)) = tokens.get(alias_index) {
            return Some(alias);
        }
    }
    None
}

fn qualified_name_at(
    tokens: &[PgCompatibilityToken<'_>],
    index: usize,
    qualifier: &str,
    name: &str,
) -> bool {
    token_word_eq(tokens.get(index), qualifier)
        && tokens.get(index + 1) == Some(&PgCompatibilityToken::Symbol(b'.'))
        && token_word_eq(tokens.get(index + 2), name)
}

fn token_word_eq(token: Option<&PgCompatibilityToken<'_>>, expected: &str) -> bool {
    match token {
        Some(PgCompatibilityToken::Word(actual)) => actual.eq_ignore_ascii_case(expected),
        Some(PgCompatibilityToken::Identifier(actual)) => actual == expected,
        _ => false,
    }
}

fn column_reference_at(
    tokens: &[PgCompatibilityToken<'_>],
    index: usize,
    alias: &str,
    column: &str,
) -> bool {
    token_word_eq(tokens.get(index), alias)
        && tokens.get(index + 1) == Some(&PgCompatibilityToken::Symbol(b'.'))
        && token_word_eq(tokens.get(index + 2), column)
}

fn has_column_reference(tokens: &[PgCompatibilityToken<'_>], alias: &str, column: &str) -> bool {
    (0..tokens.len()).any(|index| column_reference_at(tokens, index, alias, column))
}

fn has_visible_table_predicate(tokens: &[PgCompatibilityToken<'_>], class_alias: &str) -> bool {
    (0..tokens.len()).any(|index| {
        qualified_name_at(tokens, index, "pg_catalog", "pg_table_is_visible")
            && tokens.get(index + 3) == Some(&PgCompatibilityToken::Symbol(b'('))
            && column_reference_at(tokens, index + 4, class_alias, "oid")
            && tokens.get(index + 7) == Some(&PgCompatibilityToken::Symbol(b')'))
    })
}

fn column_pattern(
    tokens: &[PgCompatibilityToken<'_>],
    relation_alias: &str,
    column: &str,
) -> Option<(PgCompatibilityOperator, String)> {
    for index in 0..tokens.len() {
        if !column_reference_at(tokens, index, relation_alias, column)
            || !token_word_eq(tokens.get(index + 3), "operator")
            || tokens.get(index + 4) != Some(&PgCompatibilityToken::Symbol(b'('))
            || !token_word_eq(tokens.get(index + 5), "pg_catalog")
            || tokens.get(index + 6) != Some(&PgCompatibilityToken::Symbol(b'.'))
            || tokens.get(index + 7) != Some(&PgCompatibilityToken::Symbol(b'~'))
            || tokens.get(index + 8) != Some(&PgCompatibilityToken::Symbol(b')'))
        {
            continue;
        }
        let PgCompatibilityToken::String(pattern) = tokens.get(index + 9)? else {
            return None;
        };
        if !token_word_eq(tokens.get(index + 10), "collate")
            || !qualified_name_at(tokens, index + 11, "pg_catalog", "default")
        {
            return None;
        }
        return Some((PgCompatibilityOperator::RegexMatch, pattern.clone()));
    }
    None
}

fn relation_oid_literal(
    tokens: &[PgCompatibilityToken<'_>],
    relation_alias: &str,
    column: &str,
) -> Result<Option<u32>, ErrorResponse> {
    for index in 0..tokens.len() {
        if !column_reference_at(tokens, index, relation_alias, column)
            || tokens.get(index + 3) != Some(&PgCompatibilityToken::Symbol(b'='))
        {
            continue;
        }
        let Some(PgCompatibilityToken::String(oid)) = tokens.get(index + 4) else {
            return Err(fixed_error(
                "42601",
                "psql relation OID predicate must use one literal",
            ));
        };
        let oid = oid
            .parse::<u32>()
            .map_err(|_| fixed_error("22P02", "invalid psql relation OID literal"))?;
        return Ok(Some(oid));
    }
    Ok(None)
}

fn unqualified_oid_literal(
    tokens: &[PgCompatibilityToken<'_>],
    column: &str,
) -> Result<Option<u32>, ErrorResponse> {
    for index in 0..tokens.len() {
        if !token_word_eq(tokens.get(index), column)
            || tokens.get(index + 1) != Some(&PgCompatibilityToken::Symbol(b'='))
        {
            continue;
        }
        let Some(PgCompatibilityToken::String(oid)) = tokens.get(index + 2) else {
            return Err(fixed_error(
                "42601",
                "psql relation OID predicate must use one literal",
            ));
        };
        return oid
            .parse::<u32>()
            .map(Some)
            .map_err(|_| fixed_error("22P02", "invalid psql relation OID literal"));
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgCatalogPatternAtom {
    Literal(char),
    AnyCharacter,
    AnySequence,
}

#[derive(Debug)]
struct PgCatalogPattern {
    atoms: Vec<PgCatalogPatternAtom>,
}

impl PgCatalogPattern {
    fn compile(pattern: &str) -> Result<Self, ErrorResponse> {
        if pattern.len() > MAX_CATALOG_PATTERN_BYTES {
            return Err(fixed_error(
                "54000",
                "PostgreSQL catalog pattern exceeds the configured limit",
            ));
        }
        let Some(body) = pattern
            .strip_prefix("^(")
            .and_then(|pattern| pattern.strip_suffix(")$"))
        else {
            return Err(fixed_error(
                "0A000",
                "only anchored psql catalog patterns are supported",
            ));
        };
        let mut atoms = Vec::new();
        let mut characters = body.chars();
        while let Some(character) = characters.next() {
            let atom = match character {
                '\\' => {
                    PgCatalogPatternAtom::Literal(characters.next().ok_or_else(|| {
                        fixed_error("42601", "catalog pattern ends with an escape")
                    })?)
                }
                '.' if characters.clone().next() == Some('*') => {
                    characters.next();
                    PgCatalogPatternAtom::AnySequence
                }
                '.' => PgCatalogPatternAtom::AnyCharacter,
                '^' | '$' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '+' | '*' | '?' => {
                    return Err(fixed_error(
                        "0A000",
                        "catalog pattern uses an unsupported PostgreSQL regular expression feature",
                    ));
                }
                literal => PgCatalogPatternAtom::Literal(literal),
            };
            atoms.push(atom);
            if atoms.len() > MAX_CATALOG_PATTERN_ATOMS {
                return Err(fixed_error(
                    "54000",
                    "PostgreSQL catalog pattern has too many atoms",
                ));
            }
        }
        Ok(Self { atoms })
    }

    fn matches(&self, value: &str) -> bool {
        let value = value.chars().collect::<Vec<_>>();
        let mut previous = vec![false; value.len() + 1];
        previous[0] = true;
        for atom in &self.atoms {
            let mut current = vec![false; value.len() + 1];
            if *atom == PgCatalogPatternAtom::AnySequence {
                current[0] = previous[0];
            }
            for index in 1..=value.len() {
                current[index] = match atom {
                    PgCatalogPatternAtom::Literal(expected) => {
                        previous[index - 1] && value[index - 1] == *expected
                    }
                    PgCatalogPatternAtom::AnyCharacter => previous[index - 1],
                    PgCatalogPatternAtom::AnySequence => previous[index] || current[index - 1],
                };
            }
            previous = current;
        }
        previous[value.len()]
    }
}

fn compile_optional_catalog_pattern(
    value: &ScalarValue,
) -> Result<Option<PgCatalogPattern>, ErrorResponse> {
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Text(pattern) => PgCatalogPattern::compile(pattern).map(Some),
        _ => Err(fixed_error(
            "42804",
            "psql catalog pattern must be text or NULL",
        )),
    }
}

fn classify_compatibility_statement(sql: &str) -> Option<CompatibilityStatement> {
    let normalized = normalize_sql(sql);
    if !normalized.starts_with("select ") {
        return None;
    }
    let has_type =
        normalized.contains("pg_catalog.pg_type") || normalized.contains(" from pg_type ");
    let has_class = normalized.contains("pg_catalog.pg_class");
    let has_attribute = normalized.contains("pg_catalog.pg_attribute");
    let has_constraint = normalized.contains("pg_catalog.pg_constraint");
    let has_description = normalized.contains("pg_catalog.pg_description");
    let is_visible = normalized.contains("pg_table_is_visible(");
    let is_qualified = normalized.contains(".nspname = ");
    if has_type && normalized.contains("to_regtype(") {
        return Some(CompatibilityStatement::TypeLookup);
    }
    if has_type && has_constraint && normalized.contains(".typtype = ") {
        return Some(CompatibilityStatement::Domains);
    }
    if has_type && normalized.contains("pg_catalog.pg_enum") && normalized.contains(".typtype = ") {
        return Some(CompatibilityStatement::Enums);
    }
    if has_class
        && normalized.contains("pg_catalog.pg_index")
        && normalized.contains("pg_catalog.pg_opclass")
        && normalized.contains("not pg_catalog.pg_index.indisprimary")
    {
        return Some(CompatibilityStatement::Indexes);
    }
    if has_attribute
        && has_constraint
        && normalized.contains(".contype = ")
        && normalized.contains("array_agg(")
    {
        return Some(CompatibilityStatement::PrimaryKeys);
    }
    if has_class
        && has_constraint
        && normalized.contains(".confrelid")
        && normalized.contains(".contype = ")
        && is_visible
    {
        return Some(CompatibilityStatement::ForeignKeysVisible);
    }
    if has_class
        && has_constraint
        && normalized.contains(".confrelid")
        && normalized.contains(".contype = ")
        && is_qualified
    {
        return Some(CompatibilityStatement::ForeignKeysQualified);
    }
    if has_class
        && has_description
        && !has_attribute
        && !has_constraint
        && normalized.contains(".description")
        && is_visible
    {
        return Some(CompatibilityStatement::TableCommentsVisible);
    }
    if has_class
        && has_description
        && !has_attribute
        && !has_constraint
        && normalized.contains(".description")
        && is_qualified
    {
        return Some(CompatibilityStatement::TableCommentsQualified);
    }
    if has_class
        && has_constraint
        && !normalized.contains(".confrelid")
        && normalized.contains("pg_get_constraintdef(")
        && is_visible
    {
        return Some(CompatibilityStatement::CheckConstraintsVisible);
    }
    if has_class
        && has_constraint
        && !normalized.contains(".confrelid")
        && normalized.contains("pg_get_constraintdef(")
        && is_qualified
    {
        return Some(CompatibilityStatement::CheckConstraintsQualified);
    }
    if normalized.contains("pg_catalog.pg_namespace")
        && !has_class
        && normalized.contains(".nspname")
    {
        return Some(CompatibilityStatement::SchemaNames);
    }
    if has_class
        && has_attribute
        && normalized.contains("format_type(")
        && normalized.contains(".attnotnull")
        && normalized.contains(".relname in ")
        && is_visible
    {
        return Some(CompatibilityStatement::ColumnsVisible);
    }
    if has_class
        && has_attribute
        && normalized.contains("format_type(")
        && normalized.contains(".attnotnull")
        && normalized.contains(".relname in ")
        && is_qualified
    {
        return Some(CompatibilityStatement::ColumnsQualified);
    }
    if has_class
        && normalized.contains("pg_catalog.pg_class.oid")
        && normalized.contains(".relname in ")
        && is_visible
    {
        return Some(CompatibilityStatement::TableOidsVisible);
    }
    if has_class
        && normalized.contains("pg_catalog.pg_class.oid")
        && normalized.contains(".relname in ")
        && is_qualified
    {
        return Some(CompatibilityStatement::TableOidsQualified);
    }
    if has_class
        && normalized.contains(".relname = ")
        && normalized.contains(".relkind = any ")
        && is_visible
    {
        return Some(CompatibilityStatement::HasTableVisible);
    }
    if has_class
        && normalized.contains(".relname = ")
        && normalized.contains(".relkind = any ")
        && is_qualified
    {
        return Some(CompatibilityStatement::HasTableQualified);
    }
    if has_class && normalized.contains(".relkind = any ") && is_visible {
        return Some(CompatibilityStatement::TableNames);
    }
    None
}

fn preflight_ddl_types(prepared: &CorePreparedDdl) -> Result<(), ErrorResponse> {
    for data_type in prepared.created_column_types() {
        PostgresType::from_netbadb(data_type.physical).map_err(map_type_error)?;
    }
    Ok(())
}

fn ddl_command_tag(prepared: &CorePreparedDdl) -> &'static str {
    if prepared.is_table_create() {
        "CREATE TABLE"
    } else if prepared.is_table_drop() {
        "DROP TABLE"
    } else if prepared.is_table_alter() {
        "ALTER TABLE"
    } else if prepared.is_index_drop() {
        "DROP INDEX"
    } else {
        "CREATE INDEX"
    }
}

fn is_index_ddl(normalized: &str) -> bool {
    normalized.starts_with("create index ")
        || normalized.starts_with("create unique index ")
        || normalized.starts_with("drop index ")
}

fn is_unsupported_schema_ddl(normalized: &str) -> bool {
    [
        "create schema ",
        "alter schema ",
        "drop schema ",
        "create type ",
        "alter type ",
        "drop type ",
        "create sequence ",
        "alter sequence ",
        "drop sequence ",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix))
}

fn unsupported_schema_ddl() -> ErrorResponse {
    fixed_error(
        "0A000",
        "this table/schema/type/sequence mutation is not supported",
    )
}

fn unsupported_index_ddl() -> ErrorResponse {
    fixed_error(
        "0A000",
        "only single-column non-unique non-concurrent BTree CREATE/DROP INDEX is supported",
    )
}

fn map_create_index_error(error: &DatabaseError) -> ErrorResponse {
    match error.kind() {
        DatabaseErrorKind::UndefinedObject
        | DatabaseErrorKind::UndefinedTable
        | DatabaseErrorKind::UndefinedColumn
        | DatabaseErrorKind::DuplicateObject
        | DatabaseErrorKind::TransactionState
        | DatabaseErrorKind::Operational
        | DatabaseErrorKind::Internal
        | DatabaseErrorKind::SchemaBusy
        | DatabaseErrorKind::DuplicateColumn
        | DatabaseErrorKind::DependentObjects => map_database_error(error),
        DatabaseErrorKind::Syntax
        | DatabaseErrorKind::AmbiguousColumn
        | DatabaseErrorKind::DatatypeMismatch
        | DatabaseErrorKind::InvalidTextRepresentation
        | DatabaseErrorKind::NumericValueOutOfRange
        | DatabaseErrorKind::CannotCoerce
        | DatabaseErrorKind::IndeterminateDatatype
        | DatabaseErrorKind::ParameterCount
        | DatabaseErrorKind::NotNullViolation
        | DatabaseErrorKind::FeatureNotSupported => unsupported_index_ddl(),
    }
}

fn compatibility_parameter_types(statement: CompatibilityStatement) -> &'static [PostgresType] {
    match statement {
        CompatibilityStatement::PsqlRelationLookup
        | CompatibilityStatement::TypeLookup
        | CompatibilityStatement::SchemaNames => &[PostgresType::Text],
        CompatibilityStatement::PsqlRelationLookupQualified
        | CompatibilityStatement::PsqlTableList
        | CompatibilityStatement::PsqlIndexList => &[PostgresType::Text, PostgresType::Text],
        CompatibilityStatement::PsqlRelationProperties
        | CompatibilityStatement::PsqlColumns
        | CompatibilityStatement::PsqlIndexes
        | CompatibilityStatement::PsqlPolicies
        | CompatibilityStatement::PsqlExtendedStatistics
        | CompatibilityStatement::PsqlPublications
        | CompatibilityStatement::PsqlInheritanceParents
        | CompatibilityStatement::PsqlInheritanceChildren => &[PostgresType::Int8],
        CompatibilityStatement::TableNames => &[
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::HasTableVisible | CompatibilityStatement::HasTableQualified => &[
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::ColumnsVisible | CompatibilityStatement::ColumnsQualified => &[
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Int8,
            PostgresType::Int8,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::Domains => {
            &[PostgresType::Bool, PostgresType::Int8, PostgresType::Text]
        }
        CompatibilityStatement::Enums => &[PostgresType::Text],
        CompatibilityStatement::TableOidsVisible | CompatibilityStatement::TableOidsQualified => &[
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::PrimaryKeys => {
            &[PostgresType::Int8, PostgresType::Text, PostgresType::Int8]
        }
        CompatibilityStatement::ForeignKeysVisible
        | CompatibilityStatement::ForeignKeysQualified => &[
            PostgresType::Bool,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::Indexes => &[
            PostgresType::Int8,
            PostgresType::Int8,
            PostgresType::Bool,
            PostgresType::Int8,
            PostgresType::Int8,
            PostgresType::Int8,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::TableCommentsVisible
        | CompatibilityStatement::TableCommentsQualified => &[
            PostgresType::Int8,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
        CompatibilityStatement::CheckConstraintsVisible
        | CompatibilityStatement::CheckConstraintsQualified => &[
            PostgresType::Bool,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
            PostgresType::Text,
        ],
    }
}

fn compatibility_fields(statement: CompatibilityStatement) -> Vec<FieldDescription> {
    let fields = match statement {
        CompatibilityStatement::PsqlRelationLookup => [
            ("oid", PostgresType::Int8),
            ("nspname", PostgresType::Text),
            ("relname", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlRelationLookupQualified => [
            ("oid", PostgresType::Int8),
            ("nspname", PostgresType::Text),
            ("relname", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlTableList => [
            ("Schema", PostgresType::Text),
            ("Name", PostgresType::Text),
            ("Type", PostgresType::Text),
            ("Owner", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlIndexList => [
            ("Schema", PostgresType::Text),
            ("Name", PostgresType::Text),
            ("Type", PostgresType::Text),
            ("Owner", PostgresType::Text),
            ("Table", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlRelationProperties => [
            ("relchecks", PostgresType::Int8),
            ("relkind", PostgresType::Text),
            ("relhasindex", PostgresType::Bool),
            ("relhasrules", PostgresType::Bool),
            ("relhastriggers", PostgresType::Bool),
            ("relrowsecurity", PostgresType::Bool),
            ("relforcerowsecurity", PostgresType::Bool),
            ("relhasoids", PostgresType::Bool),
            ("relispartition", PostgresType::Bool),
            ("reloptions", PostgresType::Text),
            ("reltablespace", PostgresType::Int8),
            ("reloftype", PostgresType::Text),
            ("relpersistence", PostgresType::Text),
            ("relreplident", PostgresType::Text),
            ("amname", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlColumns => [
            ("attname", PostgresType::Text),
            ("format_type", PostgresType::Text),
            ("default", PostgresType::Text),
            ("attnotnull", PostgresType::Bool),
            ("attcollation", PostgresType::Text),
            ("attidentity", PostgresType::Text),
            ("attgenerated", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlIndexes => [
            ("relname", PostgresType::Text),
            ("indisprimary", PostgresType::Bool),
            ("indisunique", PostgresType::Bool),
            ("indisclustered", PostgresType::Bool),
            ("indisvalid", PostgresType::Bool),
            ("indexdef", PostgresType::Text),
            ("constraintdef", PostgresType::Text),
            ("contype", PostgresType::Text),
            ("condeferrable", PostgresType::Bool),
            ("condeferred", PostgresType::Bool),
            ("indisreplident", PostgresType::Bool),
            ("reltablespace", PostgresType::Int8),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlPolicies => [
            ("polname", PostgresType::Text),
            ("polpermissive", PostgresType::Bool),
            ("roles", PostgresType::Text),
            ("qual", PostgresType::Text),
            ("withcheck", PostgresType::Text),
            ("cmd", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlExtendedStatistics => [
            ("oid", PostgresType::Int8),
            ("stxrelid", PostgresType::Text),
            ("nsp", PostgresType::Text),
            ("stxname", PostgresType::Text),
            ("columns", PostgresType::Text),
            ("ndist_enabled", PostgresType::Bool),
            ("deps_enabled", PostgresType::Bool),
            ("mcv_enabled", PostgresType::Bool),
            ("stxstattarget", PostgresType::Int8),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlPublications => [
            ("pubname", PostgresType::Text),
            ("qual", PostgresType::Text),
            ("columns", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::PsqlInheritanceParents => {
            [("parent", PostgresType::Text)].as_slice()
        }
        CompatibilityStatement::PsqlInheritanceChildren => [
            ("child", PostgresType::Text),
            ("relkind", PostgresType::Text),
            ("inhdetachpending", PostgresType::Bool),
            ("relpartbound", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::TypeLookup => [
            ("name", PostgresType::Text),
            ("oid", PostgresType::Int8),
            ("array_oid", PostgresType::Int8),
            ("regtype", PostgresType::Text),
            ("delimiter", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::SchemaNames => [("nspname", PostgresType::Text)].as_slice(),
        CompatibilityStatement::TableNames => [("relname", PostgresType::Text)].as_slice(),
        CompatibilityStatement::HasTableVisible | CompatibilityStatement::HasTableQualified => {
            [("relname", PostgresType::Text)].as_slice()
        }
        CompatibilityStatement::ColumnsVisible | CompatibilityStatement::ColumnsQualified => [
            ("name", PostgresType::Text),
            ("format_type", PostgresType::Text),
            ("default", PostgresType::Text),
            ("not_null", PostgresType::Bool),
            ("table_name", PostgresType::Text),
            ("comment", PostgresType::Text),
            ("generated", PostgresType::Text),
            ("identity_options", PostgresType::Text),
            ("collation", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::Domains => [
            ("name", PostgresType::Text),
            ("attype", PostgresType::Text),
            ("nullable", PostgresType::Bool),
            ("default", PostgresType::Text),
            ("visible", PostgresType::Bool),
            ("schema", PostgresType::Text),
            ("condefs", PostgresType::Text),
            ("connames", PostgresType::Text),
            ("collname", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::Enums => [
            ("name", PostgresType::Text),
            ("visible", PostgresType::Bool),
            ("schema", PostgresType::Text),
            ("labels", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::TableOidsVisible | CompatibilityStatement::TableOidsQualified => {
            [("oid", PostgresType::Int8), ("relname", PostgresType::Text)].as_slice()
        }
        CompatibilityStatement::PrimaryKeys => [
            ("conrelid", PostgresType::Int8),
            ("cols", PostgresType::TextArray),
            ("conname", PostgresType::Text),
            ("description", PostgresType::Text),
            ("indnkeyatts", PostgresType::Int8),
            ("indnullsnotdistinct", PostgresType::Bool),
        ]
        .as_slice(),
        CompatibilityStatement::ForeignKeysVisible
        | CompatibilityStatement::ForeignKeysQualified => [
            ("relname", PostgresType::Text),
            ("conname", PostgresType::Text),
            ("anon_1", PostgresType::Text),
            ("nspname", PostgresType::Text),
            ("description", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::Indexes => [
            ("indrelid", PostgresType::Int8),
            ("relname", PostgresType::Text),
            ("indisunique", PostgresType::Bool),
            ("has_constraint", PostgresType::Bool),
            ("indoption", PostgresType::Text),
            ("reloptions", PostgresType::TextArray),
            ("amname", PostgresType::Text),
            ("filter_definition", PostgresType::Text),
            ("indnkeyatts", PostgresType::Int8),
            ("indnullsnotdistinct", PostgresType::Bool),
            ("elements", PostgresType::TextArray),
            ("elements_is_expr", PostgresType::BoolArray),
            ("elements_opclass", PostgresType::TextArray),
            ("elements_opdefault", PostgresType::BoolArray),
        ]
        .as_slice(),
        CompatibilityStatement::TableCommentsVisible
        | CompatibilityStatement::TableCommentsQualified => [
            ("relname", PostgresType::Text),
            ("description", PostgresType::Text),
        ]
        .as_slice(),
        CompatibilityStatement::CheckConstraintsVisible
        | CompatibilityStatement::CheckConstraintsQualified => [
            ("relname", PostgresType::Text),
            ("conname", PostgresType::Text),
            ("anon_1", PostgresType::Text),
            ("description", PostgresType::Text),
        ]
        .as_slice(),
    };
    fields
        .iter()
        .map(|(name, data_type)| FieldDescription {
            name: (*name).to_owned(),
            table_oid: 0,
            column_attribute: 0,
            data_type: *data_type,
            type_modifier: -1,
            format: FormatCode::Text,
        })
        .collect()
}

fn execute_compatibility_statement(
    catalog: &PgCompatibilityCatalog,
    authorization: &PrincipalAuthorization,
    statement: CompatibilityStatement,
    values: &[ScalarValue],
) -> Result<QueryResult, ErrorResponse> {
    execute_compatibility_statement_for_owner(catalog, authorization, "netbadb", statement, values)
}

fn execute_compatibility_statement_for_owner(
    catalog: &PgCompatibilityCatalog,
    authorization: &PrincipalAuthorization,
    compatibility_owner: &str,
    statement: CompatibilityStatement,
    values: &[ScalarValue],
) -> Result<QueryResult, ErrorResponse> {
    match statement {
        CompatibilityStatement::PsqlRelationLookup => {
            let [ScalarValue::Text(pattern)] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql relation lookup requires one text pattern",
                ));
            };
            let pattern = PgCatalogPattern::compile(pattern)?;
            let mut tables = catalog
                .tables
                .iter()
                .filter(|table| {
                    authorization.can_see(table.table_id) && pattern.matches(&table.name)
                })
                .collect::<Vec<_>>();
            tables.sort_by(|left, right| left.name.cmp(&right.name).then(left.oid.cmp(&right.oid)));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("oid", PhysicalType::Int64, false),
                    compatibility_result_column("nspname", PhysicalType::Text, false),
                    compatibility_result_column("relname", PhysicalType::Text, false),
                ],
                rows: tables
                    .into_iter()
                    .map(|table| {
                        vec![
                            ScalarValue::Int64(i64::from(table.oid)),
                            ScalarValue::Text("public".into()),
                            ScalarValue::Text(table.name.clone()),
                        ]
                    })
                    .collect(),
            })
        }
        CompatibilityStatement::PsqlRelationLookupQualified => {
            let [schema_pattern, relation_pattern] = values else {
                return Err(fixed_error(
                    "42804",
                    "qualified psql relation lookup requires schema and relation patterns",
                ));
            };
            let Some(schema_pattern) = compile_optional_catalog_pattern(schema_pattern)? else {
                return Err(fixed_error("42804", "psql schema pattern cannot be NULL"));
            };
            let Some(relation_pattern) = compile_optional_catalog_pattern(relation_pattern)? else {
                return Err(fixed_error("42804", "psql relation pattern cannot be NULL"));
            };
            let mut tables = catalog
                .tables
                .iter()
                .filter(|table| {
                    authorization.can_see(table.table_id)
                        && schema_pattern.matches("public")
                        && relation_pattern.matches(&table.name)
                })
                .collect::<Vec<_>>();
            tables.sort_by(|left, right| left.name.cmp(&right.name).then(left.oid.cmp(&right.oid)));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("oid", PhysicalType::Int64, false),
                    compatibility_result_column("nspname", PhysicalType::Text, false),
                    compatibility_result_column("relname", PhysicalType::Text, false),
                ],
                rows: tables
                    .into_iter()
                    .map(|table| {
                        vec![
                            ScalarValue::Int64(i64::from(table.oid)),
                            ScalarValue::Text("public".into()),
                            ScalarValue::Text(table.name.clone()),
                        ]
                    })
                    .collect(),
            })
        }
        CompatibilityStatement::PsqlTableList => {
            let [schema_pattern, name_pattern] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql table listing requires schema and name pattern slots",
                ));
            };
            let schema_pattern = compile_optional_catalog_pattern(schema_pattern)?;
            let name_pattern = compile_optional_catalog_pattern(name_pattern)?;
            let mut tables = catalog
                .tables
                .iter()
                .filter(|table| authorization.can_see(table.table_id))
                .filter(|_| {
                    schema_pattern
                        .as_ref()
                        .is_none_or(|pattern| pattern.matches("public"))
                })
                .filter(|table| {
                    name_pattern
                        .as_ref()
                        .is_none_or(|pattern| pattern.matches(&table.name))
                })
                .collect::<Vec<_>>();
            tables.sort_by(|left, right| left.name.cmp(&right.name).then(left.oid.cmp(&right.oid)));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("Schema", PhysicalType::Text, false),
                    compatibility_result_column("Name", PhysicalType::Text, false),
                    compatibility_result_column("Type", PhysicalType::Text, false),
                    compatibility_result_column("Owner", PhysicalType::Text, false),
                ],
                rows: tables
                    .into_iter()
                    .map(|table| {
                        vec![
                            ScalarValue::Text("public".into()),
                            ScalarValue::Text(table.name.clone()),
                            ScalarValue::Text("table".into()),
                            ScalarValue::Text(compatibility_owner.into()),
                        ]
                    })
                    .collect(),
            })
        }
        CompatibilityStatement::PsqlIndexList => {
            let [schema_pattern, name_pattern] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql index listing requires schema and name pattern slots",
                ));
            };
            let schema_pattern = compile_optional_catalog_pattern(schema_pattern)?;
            let name_pattern = compile_optional_catalog_pattern(name_pattern)?;
            let schema_matches = schema_pattern
                .as_ref()
                .is_none_or(|pattern| pattern.matches("public"));
            let mut rows = Vec::new();
            if schema_matches {
                for table in catalog
                    .tables
                    .iter()
                    .filter(|table| authorization.can_see(table.table_id))
                {
                    let primary_name = compatibility_primary_key_name(&table.name, table.oid);
                    let pattern_selects_table = name_pattern.as_ref().is_none_or(|pattern| {
                        (table.columns.iter().any(|column| column.primary_key)
                            && pattern.matches(&primary_name))
                            || table
                                .indexes
                                .iter()
                                .any(|index| pattern.matches(&index.name))
                    });
                    if !pattern_selects_table {
                        continue;
                    }
                    if !table.index_reflection_supported {
                        return Err(fixed_error(
                            "0A000",
                            "logical index reflection is unsupported for partitioned tables",
                        ));
                    }
                    if table.columns.iter().any(|column| column.primary_key)
                        && name_pattern
                            .as_ref()
                            .is_none_or(|pattern| pattern.matches(&primary_name))
                    {
                        rows.push(vec![
                            ScalarValue::Text("public".into()),
                            ScalarValue::Text(primary_name),
                            ScalarValue::Text("index".into()),
                            ScalarValue::Text(compatibility_owner.into()),
                            ScalarValue::Text(table.name.clone()),
                        ]);
                    }
                    for index in &table.indexes {
                        if name_pattern
                            .as_ref()
                            .is_some_and(|pattern| !pattern.matches(&index.name))
                        {
                            continue;
                        }
                        rows.push(vec![
                            ScalarValue::Text("public".into()),
                            ScalarValue::Text(index.name.clone()),
                            ScalarValue::Text("index".into()),
                            ScalarValue::Text(compatibility_owner.into()),
                            ScalarValue::Text(table.name.clone()),
                        ]);
                    }
                }
            }
            rows.sort_by(|left, right| match (&left[1], &right[1]) {
                (ScalarValue::Text(left), ScalarValue::Text(right)) => left.cmp(right),
                _ => std::cmp::Ordering::Equal,
            });
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("Schema", PhysicalType::Text, false),
                    compatibility_result_column("Name", PhysicalType::Text, false),
                    compatibility_result_column("Type", PhysicalType::Text, false),
                    compatibility_result_column("Owner", PhysicalType::Text, false),
                    compatibility_result_column("Table", PhysicalType::Text, false),
                ],
                rows,
            })
        }
        CompatibilityStatement::PsqlRelationProperties => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql relation properties require one OID",
                ));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("relchecks", PhysicalType::Int64, false),
                    compatibility_result_column("relkind", PhysicalType::Text, false),
                    compatibility_result_column("relhasindex", PhysicalType::Bool, false),
                    compatibility_result_column("relhasrules", PhysicalType::Bool, false),
                    compatibility_result_column("relhastriggers", PhysicalType::Bool, false),
                    compatibility_result_column("relrowsecurity", PhysicalType::Bool, false),
                    compatibility_result_column("relforcerowsecurity", PhysicalType::Bool, false),
                    compatibility_result_column("relhasoids", PhysicalType::Bool, false),
                    compatibility_result_column("relispartition", PhysicalType::Bool, false),
                    compatibility_result_column("reloptions", PhysicalType::Text, false),
                    compatibility_result_column("reltablespace", PhysicalType::Int64, false),
                    compatibility_result_column("reloftype", PhysicalType::Text, false),
                    compatibility_result_column("relpersistence", PhysicalType::Text, false),
                    compatibility_result_column("relreplident", PhysicalType::Text, false),
                    compatibility_result_column("amname", PhysicalType::Text, true),
                ],
                rows: table
                    .map(|table| {
                        let has_index = !table.indexes.is_empty()
                            || table.columns.iter().any(|column| column.primary_key);
                        vec![vec![
                            ScalarValue::Int64(0),
                            ScalarValue::Text("r".into()),
                            ScalarValue::Bool(has_index),
                            ScalarValue::Bool(false),
                            ScalarValue::Bool(false),
                            ScalarValue::Bool(false),
                            ScalarValue::Bool(false),
                            ScalarValue::Bool(false),
                            ScalarValue::Bool(false),
                            ScalarValue::Text(String::new()),
                            ScalarValue::Int64(0),
                            ScalarValue::Text(String::new()),
                            ScalarValue::Text("p".into()),
                            ScalarValue::Text("n".into()),
                            ScalarValue::Null,
                        ]]
                    })
                    .unwrap_or_default(),
            })
        }
        CompatibilityStatement::PsqlColumns => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error("42804", "psql columns require one table OID"));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            let rows = table
                .into_iter()
                .flat_map(|table| {
                    table.columns.iter().map(|column| {
                        Ok(vec![
                            ScalarValue::Text(column.name.clone()),
                            ScalarValue::Text(postgres_reflection_type(column.physical)?.into()),
                            ScalarValue::Null,
                            ScalarValue::Bool(!column.nullable),
                            ScalarValue::Null,
                            ScalarValue::Text(String::new()),
                            ScalarValue::Text(String::new()),
                        ])
                    })
                })
                .collect::<Result<Vec<_>, ErrorResponse>>()?;
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("attname", PhysicalType::Text, false),
                    compatibility_result_column("format_type", PhysicalType::Text, false),
                    compatibility_result_column("default", PhysicalType::Text, true),
                    compatibility_result_column("attnotnull", PhysicalType::Bool, false),
                    compatibility_result_column("attcollation", PhysicalType::Text, true),
                    compatibility_result_column("attidentity", PhysicalType::Text, false),
                    compatibility_result_column("attgenerated", PhysicalType::Text, false),
                ],
                rows,
            })
        }
        CompatibilityStatement::PsqlIndexes => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error("42804", "psql indexes require one table OID"));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            if table.is_some_and(|table| !table.index_reflection_supported) {
                return Err(fixed_error(
                    "0A000",
                    "logical index reflection is unsupported for partitioned tables",
                ));
            }
            let mut rows = Vec::new();
            if let Some(table) = table {
                let primary_columns = table
                    .columns
                    .iter()
                    .filter(|column| column.primary_key)
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>();
                if !primary_columns.is_empty() {
                    let name = compatibility_primary_key_name(&table.name, table.oid);
                    let columns = postgres_identifier_list(&primary_columns);
                    rows.push(vec![
                        ScalarValue::Text(name.clone()),
                        ScalarValue::Bool(true),
                        ScalarValue::Bool(true),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(true),
                        ScalarValue::Text(format!(
                            "CREATE UNIQUE INDEX {} ON {}.{} USING btree ({columns})",
                            quote_postgres_identifier(&name),
                            quote_postgres_identifier("public"),
                            quote_postgres_identifier(&table.name),
                        )),
                        ScalarValue::Text(format!("PRIMARY KEY ({columns})")),
                        ScalarValue::Text("p".into()),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(false),
                        ScalarValue::Int64(0),
                    ]);
                }
                for index in &table.indexes {
                    let column = quote_postgres_identifier(&index.column_name);
                    rows.push(vec![
                        ScalarValue::Text(index.name.clone()),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(index.unique),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(true),
                        ScalarValue::Text(format!(
                            "CREATE INDEX {} ON {}.{} USING {} ({column})",
                            quote_postgres_identifier(&index.name),
                            quote_postgres_identifier("public"),
                            quote_postgres_identifier(&table.name),
                            index.access_method,
                        )),
                        ScalarValue::Null,
                        ScalarValue::Null,
                        ScalarValue::Null,
                        ScalarValue::Null,
                        ScalarValue::Bool(false),
                        ScalarValue::Int64(0),
                    ]);
                }
                rows.sort_by(|left, right| {
                    let left_primary = matches!(left.get(1), Some(ScalarValue::Bool(true)));
                    let right_primary = matches!(right.get(1), Some(ScalarValue::Bool(true)));
                    right_primary.cmp(&left_primary).then_with(|| {
                        let left_name = match left.first() {
                            Some(ScalarValue::Text(name)) => name.as_str(),
                            _ => "",
                        };
                        let right_name = match right.first() {
                            Some(ScalarValue::Text(name)) => name.as_str(),
                            _ => "",
                        };
                        left_name.cmp(right_name)
                    })
                });
            }
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("relname", PhysicalType::Text, false),
                    compatibility_result_column("indisprimary", PhysicalType::Bool, false),
                    compatibility_result_column("indisunique", PhysicalType::Bool, false),
                    compatibility_result_column("indisclustered", PhysicalType::Bool, false),
                    compatibility_result_column("indisvalid", PhysicalType::Bool, false),
                    compatibility_result_column("indexdef", PhysicalType::Text, false),
                    compatibility_result_column("constraintdef", PhysicalType::Text, true),
                    compatibility_result_column("contype", PhysicalType::Text, true),
                    compatibility_result_column("condeferrable", PhysicalType::Bool, true),
                    compatibility_result_column("condeferred", PhysicalType::Bool, true),
                    compatibility_result_column("indisreplident", PhysicalType::Bool, false),
                    compatibility_result_column("reltablespace", PhysicalType::Int64, false),
                ],
                rows,
            })
        }
        CompatibilityStatement::PsqlPolicies => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error("42804", "psql policies require one table OID"));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let _visible_table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("polname", PhysicalType::Text, false),
                    compatibility_result_column("polpermissive", PhysicalType::Bool, false),
                    compatibility_result_column("roles", PhysicalType::Text, true),
                    compatibility_result_column("qual", PhysicalType::Text, true),
                    compatibility_result_column("withcheck", PhysicalType::Text, true),
                    compatibility_result_column("cmd", PhysicalType::Text, false),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::PsqlExtendedStatistics => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql extended statistics require one table OID",
                ));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let _visible_table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("oid", PhysicalType::Int64, false),
                    compatibility_result_column("stxrelid", PhysicalType::Text, false),
                    compatibility_result_column("nsp", PhysicalType::Text, false),
                    compatibility_result_column("stxname", PhysicalType::Text, false),
                    compatibility_result_column("columns", PhysicalType::Text, false),
                    compatibility_result_column("ndist_enabled", PhysicalType::Bool, false),
                    compatibility_result_column("deps_enabled", PhysicalType::Bool, false),
                    compatibility_result_column("mcv_enabled", PhysicalType::Bool, false),
                    compatibility_result_column("stxstattarget", PhysicalType::Int64, false),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::PsqlPublications => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql publications require one table OID",
                ));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let _visible_table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("pubname", PhysicalType::Text, false),
                    compatibility_result_column("qual", PhysicalType::Text, true),
                    compatibility_result_column("columns", PhysicalType::Text, true),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::PsqlInheritanceParents => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql inheritance lookup requires one table OID",
                ));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let _visible_table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![compatibility_result_column(
                    "parent",
                    PhysicalType::Text,
                    false,
                )],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::PsqlInheritanceChildren => {
            let [ScalarValue::Int64(oid)] = values else {
                return Err(fixed_error(
                    "42804",
                    "psql inheritance lookup requires one table OID",
                ));
            };
            let oid = u32::try_from(*oid)
                .map_err(|_| fixed_error("22003", "psql relation OID is out of range"))?;
            let _visible_table = catalog
                .tables
                .iter()
                .find(|table| table.oid == oid)
                .filter(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("child", PhysicalType::Text, false),
                    compatibility_result_column("relkind", PhysicalType::Text, false),
                    compatibility_result_column("inhdetachpending", PhysicalType::Bool, false),
                    compatibility_result_column("relpartbound", PhysicalType::Text, true),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::TypeLookup => {
            if !matches!(values, [ScalarValue::Text(_) | ScalarValue::Null]) {
                return Err(fixed_error(
                    "42804",
                    "type lookup requires one text parameter",
                ));
            }
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("name", PhysicalType::Text, false),
                    compatibility_result_column("oid", PhysicalType::Int64, false),
                    compatibility_result_column("array_oid", PhysicalType::Int64, false),
                    compatibility_result_column("regtype", PhysicalType::Text, false),
                    compatibility_result_column("delimiter", PhysicalType::Text, false),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::SchemaNames => {
            let [ScalarValue::Text(excluded_pattern)] = values else {
                return Err(fixed_error(
                    "42804",
                    "schema-name lookup requires one text pattern",
                ));
            };
            let rows = ["pg_catalog", "public"]
                .into_iter()
                .filter(|name| !sql_like(name, excluded_pattern))
                .map(|name| vec![ScalarValue::Text(name.to_owned())])
                .collect();
            Ok(QueryResult {
                columns: vec![compatibility_result_column(
                    "nspname",
                    PhysicalType::Text,
                    false,
                )],
                rows,
            })
        }
        CompatibilityStatement::TableNames => {
            let [
                ScalarValue::Text(first_kind),
                ScalarValue::Text(second_kind),
                ScalarValue::Text(excluded_persistence),
                ScalarValue::Text(excluded_namespace),
            ] = values
            else {
                return Err(fixed_error(
                    "42804",
                    "table-name lookup requires four text parameters",
                ));
            };
            let exposes_tables = first_kind == "r" || second_kind == "r";
            let namespace_allowed = excluded_namespace != "public";
            let persistence_allowed = excluded_persistence != "p";
            let mut names = if exposes_tables && namespace_allowed && persistence_allowed {
                catalog
                    .tables
                    .iter()
                    .filter(|table| authorization.can_see(table.table_id))
                    .map(|table| table.name.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            names.sort();
            Ok(QueryResult {
                columns: vec![compatibility_result_column(
                    "relname",
                    PhysicalType::Text,
                    false,
                )],
                rows: names
                    .into_iter()
                    .map(|name| vec![ScalarValue::Text(name)])
                    .collect(),
            })
        }
        CompatibilityStatement::HasTableVisible | CompatibilityStatement::HasTableQualified => {
            let [
                ScalarValue::Text(table_name),
                ScalarValue::Text(kind_1),
                ScalarValue::Text(kind_2),
                ScalarValue::Text(kind_3),
                ScalarValue::Text(kind_4),
                ScalarValue::Text(kind_5),
                ScalarValue::Text(namespace),
            ] = values
            else {
                return Err(fixed_error(
                    "42804",
                    "has-table lookup requires seven text parameters",
                ));
            };
            let exposes_tables = [kind_1, kind_2, kind_3, kind_4, kind_5]
                .into_iter()
                .any(|kind| kind == "r");
            let namespace_matches = match statement {
                CompatibilityStatement::HasTableVisible => namespace != "public",
                CompatibilityStatement::HasTableQualified => namespace == "public",
                _ => false,
            };
            let exists = exposes_tables
                && namespace_matches
                && catalog
                    .table(table_name)
                    .is_some_and(|table| authorization.can_see(table.table_id));
            Ok(QueryResult {
                columns: vec![compatibility_result_column(
                    "relname",
                    PhysicalType::Text,
                    false,
                )],
                rows: if exists {
                    vec![vec![ScalarValue::Text(table_name.clone())]]
                } else {
                    Vec::new()
                },
            })
        }
        CompatibilityStatement::ColumnsVisible | CompatibilityStatement::ColumnsQualified => {
            if values.len() != 18 {
                return Err(fixed_error(
                    "42804",
                    "column reflection requires eighteen typed parameters",
                ));
            }
            let Some(ScalarValue::Text(namespace)) = values.get(16) else {
                return Err(fixed_error("42804", "column namespace must be text"));
            };
            let Some(ScalarValue::Text(table_name)) = values.get(17) else {
                return Err(fixed_error("42804", "column table name must be text"));
            };
            let namespace_matches = match statement {
                CompatibilityStatement::ColumnsVisible => namespace != "public",
                CompatibilityStatement::ColumnsQualified => namespace == "public",
                _ => false,
            };
            let table = catalog
                .table(table_name)
                .filter(|table| namespace_matches && authorization.can_see(table.table_id));
            let rows = match table {
                None => Vec::new(),
                Some(table) => table
                    .columns
                    .iter()
                    .map(|column| {
                        let format_type = postgres_reflection_type(column.physical)?;
                        Ok(vec![
                            ScalarValue::Text(column.name.clone()),
                            ScalarValue::Text(format_type.to_owned()),
                            ScalarValue::Null,
                            ScalarValue::Bool(!column.nullable),
                            ScalarValue::Text(table.name.clone()),
                            ScalarValue::Null,
                            ScalarValue::Text(String::new()),
                            ScalarValue::Null,
                            ScalarValue::Null,
                        ])
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            };
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("name", PhysicalType::Text, false),
                    compatibility_result_column("format_type", PhysicalType::Text, false),
                    compatibility_result_column("default", PhysicalType::Text, true),
                    compatibility_result_column("not_null", PhysicalType::Bool, false),
                    compatibility_result_column("table_name", PhysicalType::Text, false),
                    compatibility_result_column("comment", PhysicalType::Text, true),
                    compatibility_result_column("generated", PhysicalType::Text, false),
                    compatibility_result_column("identity_options", PhysicalType::Text, true),
                    compatibility_result_column("collation", PhysicalType::Text, true),
                ],
                rows,
            })
        }
        CompatibilityStatement::Domains => {
            if !matches!(
                values,
                [
                    ScalarValue::Bool(_),
                    ScalarValue::Int64(_),
                    ScalarValue::Text(_)
                ]
            ) {
                return Err(fixed_error(
                    "42804",
                    "domain reflection requires bool, integer, and text parameters",
                ));
            }
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("name", PhysicalType::Text, false),
                    compatibility_result_column("attype", PhysicalType::Text, false),
                    compatibility_result_column("nullable", PhysicalType::Bool, false),
                    compatibility_result_column("default", PhysicalType::Text, true),
                    compatibility_result_column("visible", PhysicalType::Bool, false),
                    compatibility_result_column("schema", PhysicalType::Text, false),
                    compatibility_result_column("condefs", PhysicalType::Text, true),
                    compatibility_result_column("connames", PhysicalType::Text, true),
                    compatibility_result_column("collname", PhysicalType::Text, true),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::Enums => {
            if !matches!(values, [ScalarValue::Text(_)]) {
                return Err(fixed_error(
                    "42804",
                    "enum reflection requires one text parameter",
                ));
            }
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("name", PhysicalType::Text, false),
                    compatibility_result_column("visible", PhysicalType::Bool, false),
                    compatibility_result_column("schema", PhysicalType::Text, false),
                    compatibility_result_column("labels", PhysicalType::Text, true),
                ],
                rows: Vec::new(),
            })
        }
        CompatibilityStatement::TableOidsVisible | CompatibilityStatement::TableOidsQualified => {
            let (exposes_tables, namespace, table_name) = match values {
                [
                    ScalarValue::Text(kind_1),
                    ScalarValue::Text(kind_2),
                    ScalarValue::Text(kind_3),
                    ScalarValue::Text(namespace),
                    ScalarValue::Text(table_name),
                ] => (
                    [kind_1, kind_2, kind_3].into_iter().any(|kind| kind == "r"),
                    namespace,
                    table_name,
                ),
                [
                    ScalarValue::Text(kind_1),
                    ScalarValue::Text(kind_2),
                    ScalarValue::Text(kind_3),
                    ScalarValue::Text(kind_4),
                    ScalarValue::Text(kind_5),
                    ScalarValue::Text(namespace),
                    ScalarValue::Text(table_name),
                ] => (
                    [kind_1, kind_2, kind_3, kind_4, kind_5]
                        .into_iter()
                        .any(|kind| kind == "r"),
                    namespace,
                    table_name,
                ),
                _ => {
                    return Err(fixed_error(
                        "42804",
                        "table OID lookup requires five or seven text parameters",
                    ));
                }
            };
            let namespace_matches = match statement {
                CompatibilityStatement::TableOidsVisible => namespace != "public",
                CompatibilityStatement::TableOidsQualified => namespace == "public",
                _ => false,
            };
            let table = catalog.table(table_name).filter(|table| {
                exposes_tables && namespace_matches && authorization.can_see(table.table_id)
            });
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("oid", PhysicalType::Int64, false),
                    compatibility_result_column("relname", PhysicalType::Text, false),
                ],
                rows: table
                    .map(|table| {
                        vec![vec![
                            ScalarValue::Int64(i64::from(table.oid)),
                            ScalarValue::Text(table.name.clone()),
                        ]]
                    })
                    .unwrap_or_default(),
            })
        }
        CompatibilityStatement::PrimaryKeys => {
            let [
                ScalarValue::Int64(_subscript_start),
                ScalarValue::Text(constraint_kind),
                ScalarValue::Int64(table_oid),
            ] = values
            else {
                return Err(fixed_error(
                    "42804",
                    "primary-key reflection requires integer, text, and OID parameters",
                ));
            };
            let table = u32::try_from(*table_oid)
                .ok()
                .and_then(|oid| catalog.tables.iter().find(|table| table.oid == oid))
                .filter(|table| constraint_kind == "p" && authorization.can_see(table.table_id));
            let rows = table
                .and_then(|table| {
                    let columns = table
                        .columns
                        .iter()
                        .filter(|column| column.primary_key)
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>();
                    (!columns.is_empty()).then(|| {
                        vec![
                            ScalarValue::Int64(i64::from(table.oid)),
                            ScalarValue::Text(postgres_text_array(&columns)),
                            ScalarValue::Text(format!("{}_pkey", table.name)),
                            ScalarValue::Null,
                            ScalarValue::Int64(columns.len() as i64),
                            ScalarValue::Bool(false),
                        ]
                    })
                })
                .into_iter()
                .collect();
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("conrelid", PhysicalType::Int64, false),
                    compatibility_result_column("cols", PhysicalType::Text, false),
                    compatibility_result_column("conname", PhysicalType::Text, false),
                    compatibility_result_column("description", PhysicalType::Text, true),
                    compatibility_result_column("indnkeyatts", PhysicalType::Int64, false),
                    compatibility_result_column("indnullsnotdistinct", PhysicalType::Bool, false),
                ],
                rows,
            })
        }
        CompatibilityStatement::ForeignKeysVisible
        | CompatibilityStatement::ForeignKeysQualified => {
            if values.len() != 9 {
                return Err(fixed_error(
                    "42804",
                    "foreign-key reflection requires nine typed parameters",
                ));
            }
            let Some(ScalarValue::Text(constraint_kind)) = values.get(1) else {
                return Err(fixed_error("42804", "foreign-key kind must be text"));
            };
            let Some(ScalarValue::Text(namespace)) = values.get(7) else {
                return Err(fixed_error("42804", "foreign-key namespace must be text"));
            };
            let Some(ScalarValue::Text(table_name)) = values.get(8) else {
                return Err(fixed_error("42804", "foreign-key table name must be text"));
            };
            let namespace_matches = match statement {
                CompatibilityStatement::ForeignKeysVisible => namespace != "public",
                CompatibilityStatement::ForeignKeysQualified => namespace == "public",
                _ => false,
            };
            let table = catalog.table(table_name).filter(|table| {
                constraint_kind == "f" && namespace_matches && authorization.can_see(table.table_id)
            });
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("relname", PhysicalType::Text, false),
                    compatibility_result_column("conname", PhysicalType::Text, true),
                    compatibility_result_column("anon_1", PhysicalType::Text, true),
                    compatibility_result_column("nspname", PhysicalType::Text, true),
                    compatibility_result_column("description", PhysicalType::Text, true),
                ],
                rows: table
                    .map(|table| {
                        vec![vec![
                            ScalarValue::Text(table.name.clone()),
                            ScalarValue::Null,
                            ScalarValue::Null,
                            ScalarValue::Null,
                            ScalarValue::Null,
                        ]]
                    })
                    .unwrap_or_default(),
            })
        }
        CompatibilityStatement::Indexes => {
            let [
                ScalarValue::Int64(_expression_attnum),
                ScalarValue::Int64(_subscript_offset),
                ScalarValue::Bool(_pretty),
                ScalarValue::Int64(_expression_attnum_again),
                ScalarValue::Int64(_subscript_dimension),
                ScalarValue::Int64(table_oid),
                ScalarValue::Text(_primary_kind),
                ScalarValue::Text(_unique_kind),
                ScalarValue::Text(_exclusion_kind),
            ] = values
            else {
                return Err(fixed_error(
                    "42804",
                    "index reflection requires nine typed parameters",
                ));
            };
            let table = u32::try_from(*table_oid)
                .ok()
                .and_then(|oid| catalog.tables.iter().find(|table| table.oid == oid))
                .filter(|table| authorization.can_see(table.table_id));
            if table.is_some_and(|table| !table.index_reflection_supported) {
                return Err(fixed_error(
                    "0A000",
                    "logical index reflection is unsupported for partitioned tables",
                ));
            }
            let rows = table
                .into_iter()
                .flat_map(|table| {
                    table.indexes.iter().map(move |index| {
                        debug_assert_ne!(index.oid, table.oid);
                        let opclass = match index.column_physical {
                            PhysicalType::Bool => Ok("bool_ops"),
                            PhysicalType::Int8 | PhysicalType::Int16 | PhysicalType::UInt8 => {
                                Ok("int2_ops")
                            }
                            PhysicalType::Int32 | PhysicalType::UInt16 => Ok("int4_ops"),
                            PhysicalType::Int64 => Ok("int8_ops"),
                            PhysicalType::UInt32 => Ok("int8_ops"),
                            PhysicalType::Float32 => Ok("float4_ops"),
                            PhysicalType::Float64 => Ok("float8_ops"),
                            PhysicalType::Text => Ok("text_ops"),
                            PhysicalType::Bytes => Ok("bytea_ops"),
                            PhysicalType::Int128 | PhysicalType::UInt64 | PhysicalType::UInt128 => {
                                Err(fixed_error(
                                    "0A000",
                                    "index type has no lossless PostgreSQL reflection",
                                ))
                            }
                        }?;
                        Ok(vec![
                            ScalarValue::Int64(i64::from(table.oid)),
                            ScalarValue::Text(index.name.clone()),
                            ScalarValue::Bool(index.unique),
                            ScalarValue::Bool(false),
                            ScalarValue::Text("0".into()),
                            ScalarValue::Null,
                            ScalarValue::Text(index.access_method.into()),
                            ScalarValue::Null,
                            ScalarValue::Int64(1),
                            ScalarValue::Bool(false),
                            ScalarValue::Text(postgres_text_array(&[&index.column_name])),
                            ScalarValue::Text("{f}".into()),
                            ScalarValue::Text(postgres_text_array(&[opclass])),
                            ScalarValue::Text("{t}".into()),
                        ])
                    })
                })
                .collect::<Result<Vec<_>, ErrorResponse>>()?;
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("indrelid", PhysicalType::Int64, false),
                    compatibility_result_column("relname", PhysicalType::Text, false),
                    compatibility_result_column("indisunique", PhysicalType::Bool, false),
                    compatibility_result_column("has_constraint", PhysicalType::Bool, false),
                    compatibility_result_column("indoption", PhysicalType::Text, true),
                    compatibility_result_column("reloptions", PhysicalType::Text, true),
                    compatibility_result_column("amname", PhysicalType::Text, false),
                    compatibility_result_column("filter_definition", PhysicalType::Text, true),
                    compatibility_result_column("indnkeyatts", PhysicalType::Int64, false),
                    compatibility_result_column("indnullsnotdistinct", PhysicalType::Bool, false),
                    compatibility_result_column("elements", PhysicalType::Text, true),
                    compatibility_result_column("elements_is_expr", PhysicalType::Text, true),
                    compatibility_result_column("elements_opclass", PhysicalType::Text, true),
                    compatibility_result_column("elements_opdefault", PhysicalType::Text, true),
                ],
                rows,
            })
        }
        CompatibilityStatement::TableCommentsVisible
        | CompatibilityStatement::TableCommentsQualified => {
            let [
                ScalarValue::Int64(_),
                ScalarValue::Text(_),
                ScalarValue::Text(kind_1),
                ScalarValue::Text(kind_2),
                ScalarValue::Text(kind_3),
                ScalarValue::Text(kind_4),
                ScalarValue::Text(kind_5),
                ScalarValue::Text(namespace),
                ScalarValue::Text(table_name),
            ] = values
            else {
                return Err(fixed_error(
                    "42804",
                    "table-comment reflection requires nine typed parameters",
                ));
            };
            let exposes_tables = [kind_1, kind_2, kind_3, kind_4, kind_5]
                .into_iter()
                .any(|kind| kind == "r");
            let namespace_matches = match statement {
                CompatibilityStatement::TableCommentsVisible => namespace != "public",
                CompatibilityStatement::TableCommentsQualified => namespace == "public",
                _ => false,
            };
            let table = catalog.table(table_name).filter(|table| {
                exposes_tables && namespace_matches && authorization.can_see(table.table_id)
            });
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("relname", PhysicalType::Text, false),
                    compatibility_result_column("description", PhysicalType::Text, true),
                ],
                rows: table
                    .map(|table| {
                        vec![vec![
                            ScalarValue::Text(table.name.clone()),
                            ScalarValue::Null,
                        ]]
                    })
                    .unwrap_or_default(),
            })
        }
        CompatibilityStatement::CheckConstraintsVisible
        | CompatibilityStatement::CheckConstraintsQualified => {
            let [
                ScalarValue::Bool(_),
                ScalarValue::Text(constraint_kind),
                ScalarValue::Text(kind_1),
                ScalarValue::Text(kind_2),
                ScalarValue::Text(kind_3),
                ScalarValue::Text(kind_4),
                ScalarValue::Text(kind_5),
                ScalarValue::Text(namespace),
                ScalarValue::Text(table_name),
            ] = values
            else {
                return Err(fixed_error(
                    "42804",
                    "check-constraint reflection requires nine typed parameters",
                ));
            };
            let exposes_tables = [kind_1, kind_2, kind_3, kind_4, kind_5]
                .into_iter()
                .any(|kind| kind == "r");
            let namespace_matches = match statement {
                CompatibilityStatement::CheckConstraintsVisible => namespace != "public",
                CompatibilityStatement::CheckConstraintsQualified => namespace == "public",
                _ => false,
            };
            let table = catalog.table(table_name).filter(|table| {
                constraint_kind == "c"
                    && exposes_tables
                    && namespace_matches
                    && authorization.can_see(table.table_id)
            });
            Ok(QueryResult {
                columns: vec![
                    compatibility_result_column("relname", PhysicalType::Text, false),
                    compatibility_result_column("conname", PhysicalType::Text, true),
                    compatibility_result_column("anon_1", PhysicalType::Text, true),
                    compatibility_result_column("description", PhysicalType::Text, true),
                ],
                rows: table
                    .map(|table| {
                        vec![vec![
                            ScalarValue::Text(table.name.clone()),
                            ScalarValue::Null,
                            ScalarValue::Null,
                            ScalarValue::Null,
                        ]]
                    })
                    .unwrap_or_default(),
            })
        }
    }
}

fn postgres_reflection_type(physical: PhysicalType) -> Result<&'static str, ErrorResponse> {
    match physical {
        PhysicalType::Bool => Ok("boolean"),
        PhysicalType::Int8 | PhysicalType::Int16 => Ok("smallint"),
        PhysicalType::Int32 => Ok("integer"),
        PhysicalType::Int64 => Ok("bigint"),
        PhysicalType::UInt8 => Ok("smallint"),
        PhysicalType::UInt16 => Ok("integer"),
        PhysicalType::UInt32 => Ok("bigint"),
        PhysicalType::Float32 => Ok("real"),
        PhysicalType::Float64 => Ok("double precision"),
        PhysicalType::Text => Ok("text"),
        PhysicalType::Bytes => Ok("bytea"),
        PhysicalType::Int128 | PhysicalType::UInt64 | PhysicalType::UInt128 => Err(fixed_error(
            "0A000",
            "type has no lossless PostgreSQL reflection type",
        )),
    }
}

fn compatibility_primary_key_name(table_name: &str, table_oid: u32) -> String {
    let simple = format!("{table_name}_pkey");
    if simple.len() <= POSTGRES_IDENTIFIER_MAX_BYTES
        && simple.bytes().enumerate().all(|(index, byte)| {
            matches!(byte, b'a'..=b'z' | b'_') || (index != 0 && byte.is_ascii_digit())
        })
    {
        return simple;
    }
    let readable = sanitized_index_name_prefix(table_name, "pkey");
    let suffix = format!("_{table_oid:08x}_pkey");
    let keep = readable
        .len()
        .min(POSTGRES_IDENTIFIER_MAX_BYTES.saturating_sub("nb_".len() + suffix.len()));
    format!("nb_{}{suffix}", &readable[..keep])
}

fn quote_postgres_identifier(identifier: &str) -> String {
    let mut quoted = String::with_capacity(identifier.len().saturating_add(2));
    quoted.push('"');
    for character in identifier.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

fn postgres_identifier_list(identifiers: &[&str]) -> String {
    identifiers
        .iter()
        .map(|identifier| quote_postgres_identifier(identifier))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sql_like(value: &str, pattern: &str) -> bool {
    fn matches(value: &[u8], pattern: &[u8]) -> bool {
        match pattern.first().copied() {
            None => value.is_empty(),
            Some(b'%') => {
                matches(value, &pattern[1..])
                    || (!value.is_empty() && matches(&value[1..], pattern))
            }
            Some(b'_') => !value.is_empty() && matches(&value[1..], &pattern[1..]),
            Some(expected) => {
                value.first() == Some(&expected) && matches(&value[1..], &pattern[1..])
            }
        }
    }
    matches(value.as_bytes(), pattern.as_bytes())
}

fn postgres_text_array(values: &[&str]) -> String {
    let mut output = String::from("{");
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push('"');
        for character in value.chars() {
            if matches!(character, '\\' | '"') {
                output.push('\\');
            }
            output.push(character);
        }
        output.push('"');
    }
    output.push('}');
    output
}

fn compatibility_result_column(
    name: &str,
    physical: PhysicalType,
    nullable: bool,
) -> netbadb_core::ResultColumn {
    netbadb_core::ResultColumn {
        name: name.to_owned(),
        data_type: SemanticType::physical(physical),
        nullable,
    }
}

fn parameter_status(name: &str, value: &str) -> BackendMessage {
    BackendMessage::ParameterStatus {
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

fn parameter_hint(oid: PostgresOid) -> Result<Option<ParameterTypeHint>, ErrorResponse> {
    if oid.0 == 0 {
        return Ok(None);
    }
    PostgresType::from_oid(oid)
        .and_then(PostgresType::parameter_fallback)
        .map(|physical| Some(ParameterTypeHint::Fallback(physical)))
        .ok_or_else(|| map_type_error(TypeMappingError::UnsupportedOid(oid)))
}

fn expand_formats(
    formats: &[FormatCode],
    count: usize,
    message: &'static str,
) -> Result<Vec<FormatCode>, ErrorResponse> {
    match formats {
        [] => Ok(vec![FormatCode::Text; count]),
        [format] => Ok(vec![*format; count]),
        formats if formats.len() == count => Ok(formats.to_vec()),
        _ => Err(fixed_error("08P01", message)),
    }
}

fn fields_from_description(
    description: &StatementDescription,
) -> Result<Vec<FieldDescription>, ErrorResponse> {
    description
        .columns
        .iter()
        .map(|column| {
            let data_type =
                PostgresType::from_netbadb(column.data_type.physical).map_err(map_type_error)?;
            Ok(FieldDescription {
                name: column.name.clone(),
                table_oid: 0,
                column_attribute: 0,
                data_type,
                type_modifier: -1,
                format: FormatCode::Text,
            })
        })
        .collect()
}

fn query_messages(query: QueryResult, limit: usize) -> Result<Vec<BackendMessage>, ErrorResponse> {
    let description = StatementDescription {
        columns: query.columns.clone(),
        is_query: true,
    };
    let fields = fields_from_description(&description)?;
    let rows = encode_query_rows(&query, &fields, limit)?;
    let row_count = rows.len();
    let mut messages = Vec::with_capacity(row_count.saturating_add(2));
    messages.push(BackendMessage::RowDescription(fields));
    messages.extend(rows.into_iter().map(BackendMessage::DataRow));
    messages.push(BackendMessage::CommandComplete(format!(
        "SELECT {row_count}"
    )));
    Ok(messages)
}

fn query_messages_with_fields(
    query: QueryResult,
    fields: Vec<FieldDescription>,
    limit: usize,
) -> Result<Vec<BackendMessage>, ErrorResponse> {
    let rows = encode_query_rows(&query, &fields, limit)?;
    let row_count = rows.len();
    let mut messages = Vec::with_capacity(row_count.saturating_add(2));
    messages.push(BackendMessage::RowDescription(fields));
    messages.extend(rows.into_iter().map(BackendMessage::DataRow));
    messages.push(BackendMessage::CommandComplete(format!(
        "SELECT {row_count}"
    )));
    Ok(messages)
}

fn encode_query_rows(
    query: &QueryResult,
    fields: &[FieldDescription],
    limit: usize,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, ErrorResponse> {
    if query.rows.len() > limit {
        return Err(fixed_error(
            "54000",
            "query result exceeds configured row limit",
        ));
    }
    if fields.len() != query.columns.len() {
        return Err(fixed_error("XX000", "query result metadata mismatch"));
    }
    query
        .rows
        .iter()
        .map(|row| {
            if row.len() != fields.len() {
                return Err(fixed_error("XX000", "query result row width mismatch"));
            }
            row.iter()
                .zip(fields)
                .map(|(value, field)| {
                    match field.format {
                        FormatCode::Text => encode_text_value(value, field.data_type),
                        FormatCode::Binary => encode_binary_value(value, field.data_type),
                    }
                    .map_err(map_type_error)
                })
                .collect()
        })
        .collect()
}

fn portal_messages(result: &mut PortalResult, max_rows: u32) -> Vec<BackendMessage> {
    match result {
        PortalResult::Command { tag } => vec![BackendMessage::CommandComplete(tag.clone())],
        PortalResult::Query { rows, position } => {
            let remaining = rows.len().saturating_sub(*position);
            let requested = if max_rows == 0 {
                remaining
            } else {
                usize::try_from(max_rows)
                    .unwrap_or(remaining)
                    .min(remaining)
            };
            let end = position.saturating_add(requested);
            let mut messages = rows[*position..end]
                .iter()
                .cloned()
                .map(BackendMessage::DataRow)
                .collect::<Vec<_>>();
            *position = end;
            if *position < rows.len() {
                messages.push(BackendMessage::PortalSuspended);
            } else {
                messages.push(BackendMessage::CommandComplete(format!(
                    "SELECT {}",
                    rows.len()
                )));
            }
            messages
        }
    }
}

fn command_tag(sql: &str, count: u64) -> String {
    match sql
        .trim_start()
        .split_ascii_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
        .as_str()
    {
        "INSERT" => format!("INSERT 0 {count}"),
        "UPDATE" => format!("UPDATE {count}"),
        "DELETE" => format!("DELETE {count}"),
        other if !other.is_empty() => format!("{other} {count}"),
        _ => count.to_string(),
    }
}

fn map_database_error(error: &DatabaseError) -> ErrorResponse {
    let (sqlstate, safe_message) = match error.kind() {
        DatabaseErrorKind::DuplicateColumn => ("42701", Some(error.to_string())),
        DatabaseErrorKind::DependentObjects => ("2BP01", Some(error.to_string())),
        DatabaseErrorKind::SchemaBusy => ("55P03", Some("schema writer is busy".into())),
        DatabaseErrorKind::Syntax => ("42601", Some(error.to_string())),
        DatabaseErrorKind::UndefinedTable => ("42P01", Some(error.to_string())),
        DatabaseErrorKind::UndefinedObject => ("42704", Some(error.to_string())),
        DatabaseErrorKind::UndefinedColumn => ("42703", Some(error.to_string())),
        DatabaseErrorKind::AmbiguousColumn => ("42702", Some(error.to_string())),
        DatabaseErrorKind::DatatypeMismatch => ("42804", Some(error.to_string())),
        DatabaseErrorKind::InvalidTextRepresentation => ("22P02", Some(error.to_string())),
        DatabaseErrorKind::NumericValueOutOfRange => ("22003", Some(error.to_string())),
        DatabaseErrorKind::CannotCoerce => ("42846", Some(error.to_string())),
        DatabaseErrorKind::IndeterminateDatatype => ("42P18", Some(error.to_string())),
        DatabaseErrorKind::ParameterCount => ("08P01", Some(error.to_string())),
        DatabaseErrorKind::NotNullViolation => ("23502", Some(error.to_string())),
        DatabaseErrorKind::FeatureNotSupported => ("0A000", Some(error.to_string())),
        DatabaseErrorKind::DuplicateObject => ("42P07", Some(error.to_string())),
        DatabaseErrorKind::TransactionState => ("25000", Some("invalid transaction state".into())),
        DatabaseErrorKind::Operational => ("58000", Some("database operation failed".into())),
        DatabaseErrorKind::Internal => ("XX000", Some("internal database error".into())),
    };
    ErrorResponse {
        severity: "ERROR",
        sqlstate,
        message: bounded_message(&safe_message.unwrap_or_else(|| "database error".into())),
        detail: None,
        hint: None,
        position: error.source_position(),
    }
}

fn map_type_error(error: TypeMappingError) -> ErrorResponse {
    let state = match error {
        TypeMappingError::UnsupportedUInt64
        | TypeMappingError::UnsupportedPhysicalType(_)
        | TypeMappingError::UnsupportedOid(_)
        | TypeMappingError::BinaryFormatUnsupported(_) => "0A000",
        TypeMappingError::InvalidTextValue(_) => "22P02",
        TypeMappingError::InvalidBinaryValue(_) => "22P03",
        TypeMappingError::ValueOutOfRange(_) => "22003",
        TypeMappingError::TypeMismatch => "42804",
    };
    fixed_error(state, &error.to_string())
}

fn fixed_error(sqlstate: &'static str, message: &str) -> ErrorResponse {
    ErrorResponse {
        severity: "ERROR",
        sqlstate,
        message: bounded_message(message),
        detail: None,
        hint: None,
        position: None,
    }
}

fn bounded_message(message: &str) -> String {
    if message.len() <= MAX_ERROR_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_ERROR_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn transaction_control_name<'a>(sql: &'a str, prefix: &str) -> Option<&'a str> {
    let name = sql.strip_prefix(prefix)?.trim();
    if name.is_empty() {
        return None;
    }
    Some(
        name.strip_prefix('"')
            .and_then(|name| name.strip_suffix('"'))
            .unwrap_or(name),
    )
}

fn split_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' if quoted && bytes.get(index + 1) == Some(&b'\'') => index += 1,
            b'\'' => quoted = !quoted,
            b';' if !quoted => {
                let statement = sql[start..index].trim();
                if !statement.is_empty() {
                    statements.push(statement);
                }
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    let statement = sql[start..].trim();
    if !statement.is_empty() {
        statements.push(statement);
    }
    statements
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;

    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::ColumnId;

    use super::*;
    use crate::authorization::TablePermissions;

    #[test]
    fn postgres_handle_is_finished_observes_main_thread_completion() {
        let (shutdown_tx, _shutdown_rx) = mpsc::channel();
        let (adaptive_tx, _adaptive_rx) = mpsc::channel();
        let join = thread::spawn(|| Ok(()));
        while !join.is_finished() {
            thread::yield_now();
        }
        let server = PostgresServerHandle {
            local_addr: "127.0.0.1:1".parse().unwrap(),
            shutdown_tx,
            adaptive_control: ServerAdaptiveControlHandle::new(adaptive_tx),
            operator: None,
            join: Some(join),
        };

        assert!(server.is_finished());
        server.shutdown().unwrap();
    }

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "netbadb-postgres-catalog-{name}-{}",
            std::process::id()
        ))
    }

    fn cleanup(paths: &[&Path]) {
        for path in paths {
            let _ = std::fs::remove_file(path);
        }
    }

    fn catalog_tables() -> Vec<TableDef> {
        vec![
            TableDef::new(
                TableId(1),
                "users",
                vec![
                    ColumnDef::new(
                        ColumnId(1),
                        "id",
                        TypeSpec::Semantic {
                            name: "UserId".into(),
                            physical: PhysicalType::Int64,
                        },
                    )
                    .primary_key(true),
                    ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                        .nullable(true),
                    ColumnDef::new(
                        ColumnId(3),
                        "active",
                        TypeSpec::Physical(PhysicalType::Bool),
                    ),
                ],
            ),
            TableDef::new(
                TableId(2),
                "teams",
                vec![
                    ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64))
                        .primary_key(true),
                ],
            ),
            TableDef::new(
                TableId(3),
                "unsigned_values",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "value",
                    TypeSpec::Physical(PhysicalType::UInt64),
                )],
            ),
        ]
    }

    fn principal(known: &[TableId], visible: &[TableId]) -> PrincipalAuthorization {
        let permissions = visible
            .iter()
            .map(|table_id| TablePermissions::new(*table_id, true, false, false, false))
            .collect();
        AuthorizationPolicy::new(
            TransportKind::PlaintextLoopback,
            Some(crate::authorization::PrincipalGrants {
                schema_admin: false,
                tables: permissions,
            }),
            Vec::new(),
            known,
        )
        .expect("authorization policy")
        .admit(&ClientIdentity::LocalPlaintext)
        .expect("principal")
    }

    #[test]
    fn extended_query_keeps_postgres_carriers_distinct_from_exact_targets() {
        let root = test_path("physical-parameter-carriers");
        std::fs::create_dir(&root).unwrap();
        let physical = [
            ("int8_value", PhysicalType::Int8),
            ("int16_value", PhysicalType::Int16),
            ("int32_value", PhysicalType::Int32),
            ("uint8_value", PhysicalType::UInt8),
            ("uint16_value", PhysicalType::UInt16),
            ("uint32_value", PhysicalType::UInt32),
            ("uint64_value", PhysicalType::UInt64),
            ("int128_value", PhysicalType::Int128),
            ("uint128_value", PhysicalType::UInt128),
            ("float32_value", PhysicalType::Float32),
            ("float64_value", PhysicalType::Float64),
            ("bytes_value", PhysicalType::Bytes),
        ];
        let table = TableDef::new(
            TableId(1),
            "typed_values",
            physical
                .iter()
                .enumerate()
                .map(|(index, (name, physical))| {
                    ColumnDef::new(
                        ColumnId(u32::try_from(index + 1).unwrap()),
                        *name,
                        TypeSpec::Physical(*physical),
                    )
                    .nullable(true)
                })
                .collect(),
        );
        let mut database =
            Database::create_tables(vec![(root.join("typed_values"), table)]).unwrap();
        let authorization = AuthorizationPolicy::new(
            TransportKind::PlaintextLoopback,
            Some(crate::authorization::PrincipalGrants {
                schema_admin: false,
                tables: vec![TablePermissions::new(TableId(1), true, true, true, false)],
            }),
            Vec::new(),
            &[TableId(1)],
        )
        .unwrap()
        .admit(&ClientIdentity::LocalPlaintext)
        .unwrap();
        let (mut session, _) = PgWorkerSession::new(
            &database,
            SessionPolicy::default(),
            authorization,
            StartupMessage {
                parameters: Default::default(),
            },
            1,
        )
        .unwrap();

        let cases = [
            (
                "int8_value",
                PostgresType::Int2,
                b"-7".to_vec(),
                (-7_i16).to_be_bytes().to_vec(),
            ),
            (
                "int16_value",
                PostgresType::Int2,
                b"-8".to_vec(),
                (-8_i16).to_be_bytes().to_vec(),
            ),
            (
                "int32_value",
                PostgresType::Int4,
                b"-9".to_vec(),
                (-9_i32).to_be_bytes().to_vec(),
            ),
            (
                "uint8_value",
                PostgresType::Int2,
                b"255".to_vec(),
                255_i16.to_be_bytes().to_vec(),
            ),
            (
                "uint16_value",
                PostgresType::Int4,
                b"65535".to_vec(),
                65_535_i32.to_be_bytes().to_vec(),
            ),
            (
                "uint32_value",
                PostgresType::Int8,
                b"4294967295".to_vec(),
                4_294_967_295_i64.to_be_bytes().to_vec(),
            ),
            (
                "uint64_value",
                PostgresType::Int8,
                b"7".to_vec(),
                7_i64.to_be_bytes().to_vec(),
            ),
            (
                "int128_value",
                PostgresType::Int8,
                b"-10".to_vec(),
                (-10_i64).to_be_bytes().to_vec(),
            ),
            (
                "uint128_value",
                PostgresType::Int8,
                b"11".to_vec(),
                11_i64.to_be_bytes().to_vec(),
            ),
            (
                "float32_value",
                PostgresType::Float4,
                b"1.5".to_vec(),
                1.5_f32.to_be_bytes().to_vec(),
            ),
            (
                "float64_value",
                PostgresType::Float8,
                b"2.5".to_vec(),
                2.5_f64.to_be_bytes().to_vec(),
            ),
            (
                "bytes_value",
                PostgresType::Bytea,
                b"\\x00ff80".to_vec(),
                vec![0, 0xff, 0x80],
            ),
        ];
        for (index, (column, carrier, text, binary)) in cases.into_iter().enumerate() {
            for (suffix, format, bytes) in [
                ("text", FormatCode::Text, text),
                ("binary", FormatCode::Binary, binary),
            ] {
                let name = format!("s{index}_{suffix}");
                let parse = session.handle(
                    &mut database,
                    FrontendMessage::Parse {
                        statement: name.clone(),
                        query: format!("INSERT INTO typed_values ({column}) VALUES ($1)"),
                        parameter_types: vec![carrier.oid()],
                    },
                );
                assert_eq!(
                    parse,
                    vec![BackendMessage::ParseComplete],
                    "{column} {suffix}"
                );
                let bind = session.handle(
                    &mut database,
                    FrontendMessage::Bind {
                        portal: name.clone(),
                        statement: name,
                        parameter_formats: vec![format],
                        parameters: vec![Some(bytes)],
                        result_formats: Vec::new(),
                    },
                );
                assert_eq!(
                    bind,
                    vec![BackendMessage::BindComplete],
                    "{column} {suffix}"
                );
            }
        }

        let parse = session.handle(
            &mut database,
            FrontendMessage::Parse {
                statement: "fallback".into(),
                query: "SELECT $1".into(),
                parameter_types: vec![PostgresType::Int2.oid()],
            },
        );
        assert_eq!(parse, vec![BackendMessage::ParseComplete]);
        assert_eq!(
            session.prepared["fallback"].parameter_targets,
            vec![PhysicalType::Int16]
        );

        for name in ["uint8_below", "uint8_above"] {
            let parse = session.handle(
                &mut database,
                FrontendMessage::Parse {
                    statement: name.into(),
                    query: "INSERT INTO typed_values (uint8_value) VALUES ($1)".into(),
                    parameter_types: vec![PostgresType::Int2.oid()],
                },
            );
            assert_eq!(parse, vec![BackendMessage::ParseComplete]);
        }
        for (name, format, bytes) in [
            ("uint8_below", FormatCode::Text, b"-1".to_vec()),
            (
                "uint8_above",
                FormatCode::Binary,
                256_i16.to_be_bytes().to_vec(),
            ),
        ] {
            let bind = session.handle(
                &mut database,
                FrontendMessage::Bind {
                    portal: name.into(),
                    statement: name.into(),
                    parameter_formats: vec![format],
                    parameters: vec![Some(bytes)],
                    result_formats: Vec::new(),
                },
            );
            assert!(matches!(
                bind.as_slice(),
                [BackendMessage::ErrorResponse(error)] if error.sqlstate == "22003"
            ));
            assert!(matches!(
                session
                    .handle(&mut database, FrontendMessage::Sync)
                    .as_slice(),
                [BackendMessage::ReadyForQuery(_)]
            ));
        }

        drop(session);
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn existing_pg_session_refreshes_core_created_tables_without_granting_access() {
        let root = test_path("core-created-table");
        std::fs::create_dir(&root).unwrap();
        let mut database =
            Database::create_tables(vec![(root.join("users"), catalog_tables().remove(0))])
                .unwrap();
        let (mut session, _) = PgWorkerSession::new(
            &database,
            SessionPolicy::default(),
            principal(&[TableId(1)], &[TableId(1)]),
            StartupMessage {
                parameters: Default::default(),
            },
            1,
        )
        .unwrap();
        let mut txn = database.begin_transaction().unwrap();
        let id = database
            .create_heap_table_in(
                &mut txn,
                netbadb_core::CreateTableSpec::new(
                    "projects",
                    vec![netbadb_core::CreateColumnSpec::new(
                        "id",
                        netbadb_types::SemanticType::physical(PhysicalType::Int64),
                        false,
                    )],
                ),
            )
            .unwrap();
        session.refresh_catalog(&database).unwrap();
        assert!(session.catalog.table("projects").is_none());
        database.commit_transaction(&mut txn).unwrap();
        drop(txn);
        // The very same session refreshes through its normal next-command path.
        let query = "SELECT ns.nspname AS \"Schema\", rel.relname AS \"Name\", CASE rel.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' END AS \"Type\", pg_catalog.pg_get_userbyid(rel.relowner) AS \"Owner\" FROM pg_catalog.pg_class AS rel LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace LEFT JOIN pg_catalog.pg_am AS method ON method.oid = rel.relam WHERE rel.relkind IN ('r', 'p', '') AND pg_catalog.pg_table_is_visible(rel.oid)";
        let messages = session.handle(&mut database, FrontendMessage::Query(query.into()));
        assert!(session.catalog.table("projects").is_some());
        assert!(
            !messages
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
        );
        let visible = execute_compatibility_statement_for_owner(
            &session.catalog,
            &session.authorization,
            "reader",
            CompatibilityStatement::PsqlTableList,
            &[ScalarValue::Null, ScalarValue::Null],
        )
        .unwrap();
        assert!(
            visible
                .rows
                .iter()
                .all(|row| row[1] != ScalarValue::Text("projects".into()))
        );
        // A separately configured fixture authorization can observe the generic
        // projection; production's preexisting grants remain default-deny.
        let admin = principal(&[TableId(1), id], &[TableId(1), id]);
        let visible = execute_compatibility_statement_for_owner(
            &session.catalog,
            &admin,
            "admin",
            CompatibilityStatement::PsqlTableList,
            &[ScalarValue::Null, ScalarValue::Null],
        )
        .unwrap();
        assert!(
            visible
                .rows
                .iter()
                .any(|row| row[1] == ScalarValue::Text("projects".into()))
        );
        assert!(
            session
                .catalog
                .table("projects")
                .unwrap()
                .columns
                .iter()
                .all(|c| !c.primary_key)
        );
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn guessed_legacy_alias_for_hidden_table_is_not_resolved() {
        let root = test_path("drop-hidden-alias");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let tables = catalog_tables()
            .into_iter()
            .map(|table| (root.join(&table.name), table))
            .collect::<Vec<_>>();
        let mut database = Database::create_tables(tables).unwrap();
        database.create_index(TableId(1), ColumnId(2)).unwrap();
        let catalog = PgCompatibilityCatalog::derive(&database).unwrap();
        let alias = catalog
            .tables
            .iter()
            .find(|table| table.table_id == TableId(1))
            .unwrap()
            .indexes[0]
            .name
            .clone();
        let (mut session, _) = PgWorkerSession::new(
            &database,
            SessionPolicy::default(),
            principal(&[TableId(1), TableId(2), TableId(3)], &[TableId(2)]),
            StartupMessage {
                parameters: Default::default(),
            },
            1,
        )
        .unwrap();
        let hidden = session
            .prepare_index_ddl(&database, &format!("DROP INDEX {alias}"))
            .unwrap();
        assert!(hidden.access().write_tables().is_empty());
        assert_eq!(
            database.execute_ddl(&hidden).unwrap_err().kind(),
            DatabaseErrorKind::UndefinedObject
        );
        let no_op = session
            .prepare_index_ddl(&database, &format!("DROP INDEX IF EXISTS {alias}"))
            .unwrap();
        assert_eq!(database.execute_ddl(&no_op).unwrap(), DdlOutcome::Unchanged);
        assert_eq!(database.indexes(TableId(1)).unwrap().len(), 1);
        database.close().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn statement_splitter_preserves_quoted_semicolons() {
        assert_eq!(
            split_statements("BEGIN; INSERT INTO users (id, name) VALUES (1, 'a;b'); COMMIT;"),
            [
                "BEGIN",
                "INSERT INTO users (id, name) VALUES (1, 'a;b')",
                "COMMIT"
            ]
        );
        assert!(split_statements(" ; ").is_empty());
    }

    #[test]
    fn unsupported_schema_ddl_is_classified_without_entering_the_generic_parser() {
        for sql in [
            "create schema private",
            "create type mood as enum ('ok')",
            "create sequence users_id_seq",
        ] {
            assert!(is_unsupported_schema_ddl(sql), "{sql}");
        }
        assert!(!is_unsupported_schema_ddl(
            "alter table users add column name text"
        ));
        assert!(!is_unsupported_schema_ddl(
            "create index users_id_idx on users (id)"
        ));
        assert_eq!(unsupported_schema_ddl().sqlstate, "0A000");
    }

    #[test]
    fn database_error_mapping_uses_stable_sqlstate_and_position() {
        use netbadb_compiler::CompileError;
        use netbadb_parser::{ParseError, Span};

        let error = DatabaseError::Compile(CompileError::Parse(ParseError {
            kind: netbadb_parser::ParseErrorKind::Syntax,
            message: "expected SELECT".into(),
            span: Span { start: 4, end: 5 },
        }));
        let mapped = map_database_error(&error);
        assert_eq!(mapped.sqlstate, "42601");
        assert_eq!(mapped.position, Some(5));
        assert!(!mapped.message.contains("ParseError"));
    }

    #[test]
    fn bind_format_cardinality_follows_protocol_rules() {
        assert_eq!(
            expand_formats(&[], 2, "bad").unwrap(),
            [FormatCode::Text, FormatCode::Text]
        );
        assert_eq!(
            expand_formats(&[FormatCode::Binary], 2, "bad").unwrap(),
            [FormatCode::Binary, FormatCode::Binary]
        );
        assert_eq!(
            expand_formats(&[FormatCode::Text, FormatCode::Binary], 2, "bad").unwrap(),
            [FormatCode::Text, FormatCode::Binary]
        );
        assert_eq!(expand_formats(&[FormatCode::Binary], 0, "bad").unwrap(), []);
        assert_eq!(
            expand_formats(&[FormatCode::Text, FormatCode::Binary], 1, "bad")
                .unwrap_err()
                .sqlstate,
            "08P01"
        );
    }

    #[test]
    fn failed_transaction_ready_state_is_explicit() {
        assert_eq!(PgTransactionStatus::Idle.ready_byte(), b'I');
        assert_eq!(PgTransactionStatus::InTransaction.ready_byte(), b'T');
        assert_eq!(PgTransactionStatus::Failed.ready_byte(), b'E');
    }

    #[test]
    fn synthetic_oids_are_deterministic_separated_and_collision_checked() {
        let first = [0_u8; 32];
        let mut second = [0_u8; 32];
        second[31] = 1;
        let table = PgCatalogObjectKey::Table(TableId(1));
        let index = PgCatalogObjectKey::Index {
            table_id: TableId(1),
            column_id: ColumnId(2),
        };
        let assigned =
            assign_synthetic_oids(&[(table, first), (index, second)], SYNTHETIC_OID_BASE);
        assert_eq!(assigned[&table], SYNTHETIC_OID_BASE);
        assert_eq!(assigned[&index], SYNTHETIC_OID_BASE + 1);
        assert_ne!(assigned[&table], assigned[&index]);
        assert!(assigned.values().all(|oid| oid & 0x8000_0000 != 0));
        assert_eq!(
            assigned,
            assign_synthetic_oids(&[(table, first), (index, second)], SYNTHETIC_OID_BASE)
        );
    }

    #[test]
    fn compatibility_index_names_are_bounded_readable_stable_and_collision_checked() {
        let first = PgCatalogObjectKey::Index {
            table_id: TableId(1),
            column_id: ColumnId(1),
        };
        let second = PgCatalogObjectKey::Index {
            table_id: TableId(1),
            column_id: ColumnId(2),
        };
        let digest = [0xab; 32];
        let inputs = [
            (
                first,
                "Very Long Users Table Name With Spaces And Punctuation !!!",
                "E-mail Address With More Punctuation ???",
                digest,
            ),
            (
                second,
                "Very Long Users Table Name With Spaces And Punctuation !!!",
                "E-mail Address With More Punctuation ???",
                digest,
            ),
        ];
        let names = assign_compatibility_index_names(&inputs);
        let repeated = assign_compatibility_index_names(&inputs);
        assert_eq!(names, repeated);
        assert_ne!(names[&first], names[&second]);
        assert!(names.values().all(|name| {
            name.len() <= POSTGRES_IDENTIFIER_MAX_BYTES
                && name.starts_with("nb_very_long_users_table")
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        }));
    }

    #[test]
    fn partitioned_index_reflection_is_explicitly_unsupported() {
        let catalog = PgCompatibilityCatalog {
            tables: vec![PgCatalogTable {
                table_id: TableId(41),
                oid: SYNTHETIC_OID_BASE,
                name: "events".into(),
                columns: vec![PgCatalogColumn {
                    column_id: ColumnId(1),
                    name: "id".into(),
                    physical: PhysicalType::Int64,
                    nullable: false,
                    primary_key: true,
                }],
                indexes: Vec::new(),
                index_reflection_supported: false,
            }],
        };
        let authorized = principal(&[TableId(41)], &[TableId(41)]);
        let error = execute_compatibility_statement(
            &catalog,
            &authorized,
            CompatibilityStatement::Indexes,
            &[
                ScalarValue::Int64(0),
                ScalarValue::Int64(1),
                ScalarValue::Bool(true),
                ScalarValue::Int64(0),
                ScalarValue::Int64(1),
                ScalarValue::Int64(i64::from(SYNTHETIC_OID_BASE)),
                ScalarValue::Text("p".into()),
                ScalarValue::Text("u".into()),
                ScalarValue::Text("x".into()),
            ],
        )
        .expect_err("partition-local indexes cannot become one logical index");
        assert_eq!(error.sqlstate, "0A000");
        let unrelated = execute_compatibility_statement(
            &catalog,
            &authorized,
            CompatibilityStatement::PsqlIndexList,
            &[ScalarValue::Null, ScalarValue::Text("^(users.*)$".into())],
        )
        .expect("an unrelated psql index pattern does not select the partitioned table");
        assert!(unrelated.rows.is_empty());
        assert_eq!(
            execute_compatibility_statement(
                &catalog,
                &authorized,
                CompatibilityStatement::PsqlIndexList,
                &[ScalarValue::Null, ScalarValue::Text("^(events.*)$".into()),],
            )
            .expect_err("a selected partitioned logical index remains unsupported")
            .sqlstate,
            "0A000"
        );
    }

    #[test]
    fn derived_catalog_preserves_schema_metadata_and_authorization() {
        let paths = [
            test_path("users"),
            test_path("teams"),
            test_path("unsigned"),
        ];
        cleanup(&[&paths[0], &paths[1], &paths[2]]);
        let tables = catalog_tables();
        let mut database = Database::create_tables(
            paths
                .iter()
                .cloned()
                .zip(tables.clone())
                .collect::<Vec<_>>(),
        )
        .expect("create catalog fixture");
        database
            .create_index(TableId(1), ColumnId(2))
            .expect("create nullable text index");
        database
            .create_index(TableId(1), ColumnId(3))
            .expect("create bool index");
        let catalog = PgCompatibilityCatalog::derive(&database).expect("derive catalog");
        let repeated = PgCompatibilityCatalog::derive(&database).expect("repeat catalog");
        assert_eq!(
            catalog
                .tables
                .iter()
                .map(|table| table.oid)
                .collect::<Vec<_>>(),
            repeated
                .tables
                .iter()
                .map(|table| table.oid)
                .collect::<Vec<_>>()
        );
        let users = catalog.table("users").expect("users metadata");
        assert_eq!(
            users
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name", "active"]
        );
        assert!(users.columns[0].primary_key);
        assert!(!users.columns[0].nullable);
        assert!(users.columns[1].nullable);
        assert_eq!(users.columns[2].physical, PhysicalType::Bool);
        assert_eq!(users.indexes.len(), 2);
        assert!(users.indexes.iter().all(|index| !index.unique));
        assert!(
            users
                .indexes
                .iter()
                .all(|index| index.access_method == "btree")
        );
        assert!(users.indexes.iter().all(|index| index.oid != users.oid));
        assert!(
            catalog
                .table("teams")
                .expect("teams metadata")
                .indexes
                .is_empty()
        );

        let restricted = principal(&[TableId(1), TableId(2), TableId(3)], &[TableId(1)]);
        let names = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::TableNames,
            &[
                ScalarValue::Text("r".into()),
                ScalarValue::Text("p".into()),
                ScalarValue::Text("t".into()),
                ScalarValue::Text("pg_catalog".into()),
            ],
        )
        .expect("table names");
        assert_eq!(names.rows, [vec![ScalarValue::Text("users".into())]]);

        let psql_tables = execute_compatibility_statement_for_owner(
            &catalog,
            &restricted,
            "restricted_user",
            CompatibilityStatement::PsqlTableList,
            &[ScalarValue::Null, ScalarValue::Null],
        )
        .expect("psql table list");
        assert_eq!(
            psql_tables.rows,
            [vec![
                ScalarValue::Text("public".into()),
                ScalarValue::Text("users".into()),
                ScalarValue::Text("table".into()),
                ScalarValue::Text("restricted_user".into()),
            ]]
        );

        let qualified_users = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::PsqlRelationLookupQualified,
            &[
                ScalarValue::Text("^(public)$".into()),
                ScalarValue::Text("^(user.*)$".into()),
            ],
        )
        .expect("qualified psql relation lookup");
        assert_eq!(qualified_users.rows.len(), 1);
        let wrong_schema = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::PsqlRelationLookupQualified,
            &[
                ScalarValue::Text("^(private)$".into()),
                ScalarValue::Text("^(user.*)$".into()),
            ],
        )
        .expect("non-matching schema");
        assert!(wrong_schema.rows.is_empty());

        let psql_indexes = execute_compatibility_statement_for_owner(
            &catalog,
            &restricted,
            "restricted_user",
            CompatibilityStatement::PsqlIndexList,
            &[ScalarValue::Null, ScalarValue::Text("^(.*users.*)$".into())],
        )
        .expect("psql index list");
        assert_eq!(psql_indexes.rows.len(), 3);
        assert!(
            psql_indexes
                .rows
                .iter()
                .all(|row| row[3] == ScalarValue::Text("restricted_user".into()))
        );
        assert!(
            psql_indexes
                .rows
                .iter()
                .all(|row| { matches!(&row[4], ScalarValue::Text(table) if table == "users") })
        );

        let missing = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::HasTableQualified,
            &[
                ScalarValue::Text("missing".into()),
                ScalarValue::Text("r".into()),
                ScalarValue::Text("p".into()),
                ScalarValue::Text("f".into()),
                ScalarValue::Text("v".into()),
                ScalarValue::Text("m".into()),
                ScalarValue::Text("public".into()),
            ],
        )
        .expect("missing table");
        assert!(missing.rows.is_empty());

        let mut column_parameters = vec![ScalarValue::Null; 18];
        column_parameters[16] = ScalarValue::Text("public".into());
        column_parameters[17] = ScalarValue::Text("users".into());
        let columns = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::ColumnsQualified,
            &column_parameters,
        )
        .expect("columns");
        assert_eq!(columns.rows.len(), 3);
        assert_eq!(columns.rows[0][0], ScalarValue::Text("id".into()));
        assert_eq!(columns.rows[0][1], ScalarValue::Text("bigint".into()));
        assert_eq!(columns.rows[1][3], ScalarValue::Bool(false));

        column_parameters[17] = ScalarValue::Text("unsigned_values".into());
        let unrestricted = principal(
            &[TableId(1), TableId(2), TableId(3)],
            &[TableId(1), TableId(2), TableId(3)],
        );
        assert_eq!(
            execute_compatibility_statement(
                &catalog,
                &unrestricted,
                CompatibilityStatement::ColumnsQualified,
                &column_parameters,
            )
            .expect_err("UINT64 reflection must be explicit")
            .sqlstate,
            "0A000"
        );

        let primary_key = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::PrimaryKeys,
            &[
                ScalarValue::Int64(1),
                ScalarValue::Text("p".into()),
                ScalarValue::Int64(i64::from(users.oid)),
            ],
        )
        .expect("primary key");
        assert_eq!(primary_key.rows.len(), 1);
        assert_eq!(primary_key.rows[0][1], ScalarValue::Text("{\"id\"}".into()));

        let index_parameters = [
            ScalarValue::Int64(0),
            ScalarValue::Int64(1),
            ScalarValue::Bool(true),
            ScalarValue::Int64(0),
            ScalarValue::Int64(1),
            ScalarValue::Int64(i64::from(users.oid)),
            ScalarValue::Text("p".into()),
            ScalarValue::Text("u".into()),
            ScalarValue::Text("x".into()),
        ];
        let reflected_indexes = execute_compatibility_statement(
            &catalog,
            &restricted,
            CompatibilityStatement::Indexes,
            &index_parameters,
        )
        .expect("reflect indexes");
        assert_eq!(reflected_indexes.rows.len(), 2);
        assert_eq!(
            reflected_indexes
                .rows
                .iter()
                .map(|row| (&row[10], &row[2], &row[6]))
                .collect::<Vec<_>>(),
            vec![
                (
                    &ScalarValue::Text("{\"active\"}".into()),
                    &ScalarValue::Bool(false),
                    &ScalarValue::Text("btree".into()),
                ),
                (
                    &ScalarValue::Text("{\"name\"}".into()),
                    &ScalarValue::Bool(false),
                    &ScalarValue::Text("btree".into()),
                ),
            ]
        );
        let denied = principal(&[TableId(1), TableId(2), TableId(3)], &[TableId(2)]);
        assert!(
            execute_compatibility_statement(
                &catalog,
                &denied,
                CompatibilityStatement::Indexes,
                &index_parameters,
            )
            .expect("deny index metadata")
            .rows
            .is_empty()
        );

        let stable_indexes = users
            .indexes
            .iter()
            .map(|index| (index.oid, index.name.clone(), index.column_id))
            .collect::<Vec<_>>();

        // Exercise the Core-only maintenance boundary; no new PG SQL surface.
        let retired = database.create_index(TableId(1), ColumnId(1)).unwrap();
        database.drop_index(TableId(1), retired.id).unwrap();
        let named = database
            .prepare_ddl_statement("CREATE INDEX teams_id_named ON teams (id)")
            .unwrap();
        database.execute_ddl(&named).unwrap();
        let named_catalog = PgCompatibilityCatalog::derive(&database).unwrap();
        let named_identity = named_catalog
            .table("teams")
            .unwrap()
            .indexes
            .iter()
            .map(|index| (index.oid, index.name.clone(), index.column_id))
            .collect::<Vec<_>>();
        let inspection = database.inspect_catalog().unwrap();
        let generation = database.catalog_generation();
        let report = database.compact_index_catalog(TableId(1)).unwrap();
        assert_eq!(report.retired_indexes_removed, 1);
        assert_eq!(database.inspect_catalog().unwrap(), inspection);
        assert_eq!(database.catalog_generation(), generation);
        let compacted = PgCompatibilityCatalog::derive(&database).unwrap();
        assert_eq!(
            compacted
                .table("users")
                .unwrap()
                .indexes
                .iter()
                .map(|index| (index.oid, index.name.clone(), index.column_id))
                .collect::<Vec<_>>(),
            stable_indexes
        );

        database.compact_index_catalog(TableId(2)).unwrap();
        let compacted = PgCompatibilityCatalog::derive(&database).unwrap();
        assert_eq!(
            compacted
                .table("teams")
                .unwrap()
                .indexes
                .iter()
                .map(|index| (index.oid, index.name.clone(), index.column_id))
                .collect::<Vec<_>>(),
            named_identity
        );

        database.close().expect("close catalog fixture");
        let mut root = paths[0].as_os_str().to_os_string();
        root.push(".schema");
        let reopened = Database::open_catalog(std::path::PathBuf::from(root))
            .expect("reopen catalog fixture without external schema");
        let reopened_catalog =
            PgCompatibilityCatalog::derive(&reopened).expect("derive reopened catalog");
        assert_eq!(
            reopened_catalog
                .tables
                .iter()
                .map(|table| table.oid)
                .collect::<Vec<_>>(),
            catalog
                .tables
                .iter()
                .map(|table| table.oid)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            reopened_catalog
                .table("users")
                .expect("reopened users")
                .indexes
                .iter()
                .map(|index| (index.oid, index.name.clone(), index.column_id))
                .collect::<Vec<_>>(),
            stable_indexes
        );
        assert_eq!(
            reopened_catalog
                .table("teams")
                .unwrap()
                .indexes
                .iter()
                .map(|index| (index.oid, index.name.clone(), index.column_id))
                .collect::<Vec<_>>(),
            named_identity
        );
        reopened.close().expect("close reopened catalog fixture");
        cleanup(&[&paths[0], &paths[1], &paths[2]]);
    }

    #[test]
    fn catalog_classification_is_structural_and_sql_like_is_bounded() {
        assert!(is_index_ddl("create index users_name on users (name)"));
        assert!(is_index_ddl(
            "create unique index users_name on users (name)"
        ));
        assert!(is_index_ddl("drop index users_name"));
        assert!(!is_index_ddl("select name from users"));
        assert_eq!(
            classify_compatibility_statement(
                "SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relkind = ANY ($1) AND pg_catalog.pg_table_is_visible(c.oid)"
            ),
            Some(CompatibilityStatement::TableNames)
        );
        assert_eq!(
            classify_compatibility_statement(
                "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull, d.description \
                 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON true \
                 LEFT JOIN pg_catalog.pg_description d ON true JOIN pg_catalog.pg_namespace n ON true \
                 WHERE pg_catalog.pg_table_is_visible(c.oid) AND c.relname IN ($1)"
            ),
            Some(CompatibilityStatement::ColumnsVisible)
        );
        assert_eq!(
            classify_compatibility_statement(
                "SELECT c.relname, d.description FROM pg_catalog.pg_class c \
                 LEFT JOIN pg_catalog.pg_description d ON true \
                 WHERE pg_catalog.pg_table_is_visible(c.oid)"
            ),
            Some(CompatibilityStatement::TableCommentsVisible)
        );
        assert_eq!(
            classify_compatibility_statement(
                "SELECT a.conrelid, array_agg(a.attname) FROM pg_catalog.pg_attribute a \
                 JOIN pg_catalog.pg_constraint c ON true JOIN pg_catalog.pg_index i ON true \
                 WHERE c.contype = $1"
            ),
            Some(CompatibilityStatement::PrimaryKeys)
        );
        assert_eq!(
            classify_compatibility_statement(
                "SELECT i.indrelid FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON true \
                 JOIN pg_catalog.pg_opclass o ON true WHERE NOT pg_catalog.pg_index.indisprimary"
            ),
            Some(CompatibilityStatement::Indexes)
        );
        assert!(sql_like("pg_catalog", "pg_%"));
        assert!(sql_like("public", "pub_ic"));
        assert!(!sql_like("public", "pg_%"));
    }

    #[test]
    fn psql_relation_lookup_is_structural_bounded_and_non_backtracking() {
        let query = "SELECT klass.oid, space.nspname, klass.relname \
                     FROM pg_catalog.pg_class AS klass \
                     LEFT JOIN pg_catalog.pg_namespace AS space \
                       ON space.oid = klass.relnamespace \
                     WHERE klass.relname OPERATOR ( pg_catalog.~ ) '^(user.*)$' \
                       COLLATE pg_catalog.default \
                       AND pg_catalog.pg_table_is_visible(klass.oid) \
                     ORDER BY 2, 3";
        let classified = classify_simple_compatibility_query(query)
            .expect("classify psql relation lookup")
            .expect("recognized psql relation lookup");
        assert_eq!(
            classified.statement,
            CompatibilityStatement::PsqlRelationLookup
        );
        assert_eq!(
            classified.values,
            vec![ScalarValue::Text("^(user.*)$".into())]
        );

        let qualified = "SELECT rel.oid, ns.nspname, rel.relname \
                         FROM pg_catalog.pg_class rel \
                         LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = rel.relnamespace \
                         WHERE rel.relname OPERATOR(pg_catalog.~) '^(users)$' COLLATE pg_catalog.default \
                           AND ns.nspname OPERATOR(pg_catalog.~) '^(public)$' COLLATE pg_catalog.default";
        let classified = classify_simple_compatibility_query(qualified)
            .expect("classify qualified relation lookup")
            .expect("recognize qualified relation lookup");
        assert_eq!(
            classified.statement,
            CompatibilityStatement::PsqlRelationLookupQualified
        );
        assert_eq!(
            classified.values,
            [
                ScalarValue::Text("^(public)$".into()),
                ScalarValue::Text("^(users)$".into()),
            ]
        );

        let table_list = "SELECT ns.nspname AS \"Schema\", rel.relname AS \"Name\", \
                          CASE rel.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' END AS \"Type\", \
                          pg_catalog.pg_get_userbyid(rel.relowner) AS \"Owner\" \
                          FROM pg_catalog.pg_class AS rel \
                          LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace \
                          LEFT JOIN pg_catalog.pg_am AS method ON method.oid = rel.relam \
                          WHERE rel.relkind IN ('r', 'p', '') \
                            AND rel.relname OPERATOR(pg_catalog.~) '^(user.*)$' COLLATE pg_catalog.default \
                            AND pg_catalog.pg_table_is_visible(rel.oid)";
        let classified = classify_simple_compatibility_query(table_list)
            .expect("classify table listing")
            .expect("recognize table listing");
        assert_eq!(classified.statement, CompatibilityStatement::PsqlTableList);
        assert_eq!(
            classified.values,
            [ScalarValue::Null, ScalarValue::Text("^(user.*)$".into())]
        );

        let index_list = "SELECT ns.nspname AS \"Schema\", idx_class.relname AS \"Name\", \
                          pg_catalog.pg_get_userbyid(idx_class.relowner) AS \"Owner\", table_class.relname AS \"Table\" \
                          FROM pg_catalog.pg_class AS idx_class \
                          LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = idx_class.relnamespace \
                          LEFT JOIN pg_catalog.pg_am AS method ON method.oid = idx_class.relam \
                          LEFT JOIN pg_catalog.pg_index AS idx ON idx.indexrelid = idx_class.oid \
                          LEFT JOIN pg_catalog.pg_class AS table_class ON idx.indrelid = table_class.oid \
                          WHERE idx_class.relkind IN ('i', 'I', '') \
                            AND idx_class.relname OPERATOR(pg_catalog.~) '^(.*users.*)$' COLLATE pg_catalog.default \
                            AND pg_catalog.pg_table_is_visible(idx_class.oid)";
        let classified = classify_simple_compatibility_query(index_list)
            .expect("classify index listing")
            .expect("recognize index listing");
        assert_eq!(classified.statement, CompatibilityStatement::PsqlIndexList);
        assert_eq!(
            classified.values,
            [ScalarValue::Null, ScalarValue::Text("^(.*users.*)$".into()),]
        );

        let policies = "SELECT pol.polname, pol.polpermissive, pol.polroles, \
                        pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), \
                        pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid), pol.polcmd \
                        FROM pg_catalog.pg_policy pol, pg_catalog.pg_roles role \
                        WHERE pol.polrelid = '2591984850'";
        assert_eq!(
            classify_simple_compatibility_query(policies)
                .expect("classify policies")
                .expect("recognize policies")
                .statement,
            CompatibilityStatement::PsqlPolicies
        );
        let statistics = "SELECT oid, stxrelid, stxnamespace, stxname, stxkind, stxstattarget, \
                          pg_catalog.pg_get_statisticsobjdef_columns(oid) \
                          FROM pg_catalog.pg_statistic_ext WHERE stxrelid = '2591984850'";
        assert_eq!(
            classify_simple_compatibility_query(statistics)
                .expect("classify statistics")
                .expect("recognize statistics")
                .statement,
            CompatibilityStatement::PsqlExtendedStatistics
        );
        let publications = "SELECT pub.pubname FROM pg_catalog.pg_publication pub \
                            JOIN pg_catalog.pg_publication_namespace pubns ON true \
                            JOIN pg_catalog.pg_publication_rel pubrel ON true \
                            JOIN pg_catalog.pg_class rel ON true \
                            WHERE rel.oid = '2591984850' \
                              AND pg_catalog.pg_relation_is_publishable(rel.oid) \
                            UNION SELECT pub.pubname FROM pg_catalog.pg_publication pub";
        assert_eq!(
            classify_simple_compatibility_query(publications)
                .expect("classify publications")
                .expect("recognize publications")
                .statement,
            CompatibilityStatement::PsqlPublications
        );
        let parents = "SELECT rel.oid::pg_catalog.regclass \
                       FROM pg_catalog.pg_class rel, pg_catalog.pg_inherits inh \
                       WHERE rel.oid = inh.inhparent AND inh.inhrelid = '2591984850' \
                       ORDER BY inhseqno";
        assert_eq!(
            classify_simple_compatibility_query(parents)
                .expect("classify inheritance parents")
                .expect("recognize inheritance parents")
                .statement,
            CompatibilityStatement::PsqlInheritanceParents
        );
        let children = "SELECT rel.oid::pg_catalog.regclass, rel.relkind, inhdetachpending, \
                        pg_catalog.pg_get_expr(rel.relpartbound, rel.oid) \
                        FROM pg_catalog.pg_class rel, pg_catalog.pg_inherits inh \
                        WHERE rel.oid = inh.inhrelid AND inh.inhparent = '2591984850'";
        assert_eq!(
            classify_simple_compatibility_query(children)
                .expect("classify inheritance children")
                .expect("recognize inheritance children")
                .statement,
            CompatibilityStatement::PsqlInheritanceChildren
        );

        let properties = "SELECT k.relchecks, k.relkind, k.relhasindex, k.relhasrules, \
                          k.relhastriggers, k.relrowsecurity, k.relforcerowsecurity, false, \
                          k.relispartition, '', k.reltablespace, \
                          CASE WHEN k.reloftype = 0 THEN '' ELSE k.reloftype::pg_catalog.regtype::pg_catalog.text END, \
                          k.relpersistence, k.relreplident, method.amname \
                          FROM pg_catalog.pg_class k \
                          LEFT JOIN pg_catalog.pg_class toast_class ON (k.reltoastrelid = toast_class.oid) \
                          LEFT JOIN pg_catalog.pg_am method ON (k.relam = method.oid) \
                          WHERE k.oid = '2591984850'";
        let classified = classify_simple_compatibility_query(properties)
            .expect("classify psql relation properties")
            .expect("recognized psql relation properties");
        assert_eq!(
            classified.statement,
            CompatibilityStatement::PsqlRelationProperties
        );
        assert_eq!(classified.values, vec![ScalarValue::Int64(2_591_984_850)]);

        let columns = "SELECT attr.attname, \
                       pg_catalog.format_type(attr.atttypid, attr.atttypmod), \
                       (SELECT pg_catalog.pg_get_expr(def.adbin, def.adrelid, true) \
                        FROM pg_catalog.pg_attrdef def \
                        WHERE def.adrelid = attr.attrelid AND def.adnum = attr.attnum AND attr.atthasdef), \
                       attr.attnotnull, \
                       (SELECT coll.collname FROM pg_catalog.pg_collation coll, pg_catalog.pg_type typ \
                        WHERE coll.oid = attr.attcollation AND typ.oid = attr.atttypid) AS attcollation, \
                       attr.attidentity, attr.attgenerated \
                       FROM pg_catalog.pg_attribute attr \
                       WHERE attr.attrelid = '2591984850' AND attr.attnum > 0 AND NOT attr.attisdropped \
                       ORDER BY attr.attnum";
        let tokens = lex_compatibility_query(columns).expect("lex psql columns");
        let attribute_alias =
            relation_alias(&tokens, "pg_catalog", "pg_attribute").expect("attribute alias");
        assert!(relation_alias(&tokens, "pg_catalog", "pg_attrdef").is_some());
        assert!(relation_alias(&tokens, "pg_catalog", "pg_collation").is_some());
        assert!(relation_alias(&tokens, "pg_catalog", "pg_type").is_some());
        let normalized_columns = normalize_sql(columns);
        assert!(normalized_columns.contains("pg_catalog.format_type("));
        assert!(normalized_columns.contains("pg_catalog.pg_get_expr("));
        for column in [
            "attname",
            "atttypid",
            "atttypmod",
            "attrelid",
            "attnum",
            "attnotnull",
            "attcollation",
            "attidentity",
            "attgenerated",
            "attisdropped",
        ] {
            assert!(
                has_column_reference(&tokens, attribute_alias, column),
                "missing column marker {column}"
            );
        }
        assert_eq!(
            relation_oid_literal(&tokens, attribute_alias, "attrelid")
                .expect("column OID predicate"),
            Some(2_591_984_850)
        );
        let classified = classify_simple_compatibility_query(columns)
            .expect("classify psql columns")
            .expect("recognized psql columns");
        assert_eq!(classified.statement, CompatibilityStatement::PsqlColumns);
        assert_eq!(classified.values, vec![ScalarValue::Int64(2_591_984_850)]);

        let indexes = "SELECT idx_class.relname, idx.indisprimary, idx.indisunique, \
                       idx.indisclustered, idx.indisvalid, \
                       pg_catalog.pg_get_indexdef(idx.indexrelid, 0, true), \
                       pg_catalog.pg_get_constraintdef(con.oid, true), contype, \
                       condeferrable, condeferred, idx.indisreplident, idx_class.reltablespace \
                       FROM pg_catalog.pg_class table_class, pg_catalog.pg_class idx_class, \
                            pg_catalog.pg_index idx \
                       LEFT JOIN pg_catalog.pg_constraint con ON \
                         (conrelid = idx.indrelid AND conindid = idx.indexrelid AND contype IN ('p','u','x')) \
                       WHERE table_class.oid = '2591984850' \
                         AND table_class.oid = idx.indrelid AND idx.indexrelid = idx_class.oid \
                       ORDER BY idx.indisprimary DESC, idx_class.relname";
        let classified = classify_simple_compatibility_query(indexes)
            .expect("classify psql indexes")
            .expect("recognized psql indexes");
        assert_eq!(classified.statement, CompatibilityStatement::PsqlIndexes);
        assert_eq!(classified.values, vec![ScalarValue::Int64(2_591_984_850)]);

        let wildcard = PgCatalogPattern::compile("^(user.*)$").expect("compile wildcard");
        assert!(wildcard.matches("users"));
        assert!(wildcard.matches("user_profiles"));
        assert!(!wildcard.matches("teams"));
        let utf8 = PgCatalogPattern::compile("^(用户.)$").expect("compile UTF-8 pattern");
        assert!(utf8.matches("用户表"));
        let escaped = PgCatalogPattern::compile(r"^(user\.name)$").expect("compile escape");
        assert!(escaped.matches("user.name"));
        assert!(!escaped.matches("userXname"));
        assert_eq!(
            PgCatalogPattern::compile("^(user[0-9])$")
                .expect_err("reject unsupported character class")
                .sqlstate,
            "0A000"
        );

        let unterminated = query.replace("'^(user.*)$'", "'^(user.*)$");
        assert_eq!(
            classify_simple_compatibility_query(&unterminated)
                .expect_err("reject unterminated literal")
                .sqlstate,
            "42601"
        );
        let oversized = format!("^({})$", "x".repeat(MAX_CATALOG_PATTERN_BYTES));
        assert_eq!(
            PgCatalogPattern::compile(&oversized)
                .expect_err("reject oversized pattern")
                .sqlstate,
            "54000"
        );

        let truncated_operator = query.replace(
            "OPERATOR ( pg_catalog.~ ) '^(user.*)$'",
            "OPERATOR ( pg_catalog.",
        );
        assert_eq!(
            classify_simple_compatibility_query(&truncated_operator)
                .expect_err("reject truncated OPERATOR")
                .sqlstate,
            "42601"
        );
        let invalid_operator = query.replace("pg_catalog.~", "pg_catalog.=");
        assert!(
            classify_simple_compatibility_query(&invalid_operator)
                .expect("invalid operator is not a supported compatibility query")
                .is_none()
        );
        let invalid_schema = query.replace("pg_catalog.~", "other.~");
        assert!(
            classify_simple_compatibility_query(&invalid_schema)
                .expect("invalid operator schema is not a supported compatibility query")
                .is_none()
        );
        assert_eq!(
            lex_compatibility_query("SELECT \"unterminated FROM pg_catalog.pg_class c")
                .expect_err("reject unterminated quoted identifier")
                .sqlstate,
            "42601"
        );
        let oversized_identifier = format!(
            "SELECT \"{}\" FROM pg_catalog.pg_class c",
            "x".repeat(MAX_CATALOG_PATTERN_BYTES + 1),
        );
        assert_eq!(
            lex_compatibility_query(&oversized_identifier)
                .expect_err("reject oversized quoted identifier")
                .sqlstate,
            "54000"
        );
        assert_eq!(
            lex_compatibility_query("SELECT (c.oid FROM pg_catalog.pg_class c")
                .expect_err("reject unclosed parenthesis")
                .sqlstate,
            "42601"
        );
        let deeply_nested = format!(
            "SELECT {}c.oid{} FROM pg_catalog.pg_class c",
            "(".repeat(MAX_COMPATIBILITY_NESTING + 1),
            ")".repeat(MAX_COMPATIBILITY_NESTING + 1),
        );
        assert_eq!(
            lex_compatibility_query(&deeply_nested)
                .expect_err("reject excessive nesting")
                .sqlstate,
            "54000"
        );
    }
}

#[cfg(test)]
#[path = "postgres_adaptive_feedback_tests.rs"]
mod adaptive_feedback_tests;
#[cfg(test)]
#[path = "postgres_alter_table_tests.rs"]
mod alter_table_tests;
#[cfg(test)]
#[path = "postgres_create_table_tests.rs"]
mod create_table_tests;
#[cfg(test)]
#[path = "postgres_drop_table_tests.rs"]
mod drop_table_tests;
