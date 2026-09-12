use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use netbadb_core::{Database, DatabaseError};
use netbadb_protocol::{
    ClientMessage, Frame, ProtocolError, ProtocolErrorCode, ServerMessage, read_client_frame,
    write_server_frame,
};

use crate::adaptive_driver::{
    ServerAdaptiveControlHandle, ServerAdaptiveDriverConfig, ServerAdaptiveDriverConfigError,
    ServerAdaptiveHostConfig, ServerAdaptiveHostDriver, ServerAdaptiveStartupMode,
    ServerAdaptiveWorkerCommand, ServerAdaptiveWorkerRuntime, forward_control_requests,
    handle_disabled_worker_command,
};
use crate::adaptive_feedback::ServerAdaptiveFeedbackRuntime;
use crate::authorization::{
    AuthorizationAction, AuthorizationDenied, AuthorizationPolicy, PrincipalAuthorization,
};
use crate::manifest::validate_listener_security;
use crate::operator::ServerOperatorPlane;
use crate::physical_design::{
    ServerHostObservationConfig, ServerPhysicalDesignAdvisorConfig,
    ServerPhysicalDesignControlHandle, ServerPhysicalDesignRuntime,
    ServerPhysicalDesignWorkerCommand, forward_physical_design_control_requests,
};
use crate::tls::{ConnectionStream, TlsHandshakeError, TransportSecurity};
use crate::{
    ClientIdentity, ManifestError, ResponseBatch, ServerAdaptiveFeedbackConfig, ServerConfig,
    ServerLimits, ServerMetricsHandle, ServerOperatorError, SessionPolicy, SessionResponse,
    SessionState, TableBootstrap, TransportKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerFatalError {
    SessionCloseFailed {
        session_id: SessionId,
        message: String,
    },
    DatabaseCloseFailed {
        message: String,
    },
}

impl fmt::Display for WorkerFatalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SessionCloseFailed {
                session_id,
                message,
            } => write!(
                formatter,
                "failed to close session {} safely: {message}",
                session_id.get()
            ),
            Self::DatabaseCloseFailed { message } => {
                write!(formatter, "failed to close database worker: {message}")
            }
        }
    }
}

impl Error for WorkerFatalError {}

#[derive(Debug)]
pub enum TcpServerError {
    Manifest(ManifestError),
    Database(DatabaseError),
    AdaptiveConfig(ServerAdaptiveDriverConfigError),
    Operator(ServerOperatorError),
    Bind {
        address: SocketAddr,
        source: io::Error,
    },
    ListenerConfiguration(io::Error),
    Accept(io::Error),
    ThreadSpawn(io::Error),
    StartupCleanup {
        startup: Box<TcpServerError>,
        cleanup: Box<TcpServerError>,
    },
    OperatorAndServerCleanup {
        operator: Box<ServerOperatorError>,
        server: Box<TcpServerError>,
    },
    WorkerStopped,
    WorkerPanicked,
    WorkerFatal(WorkerFatalError),
    SessionIdExhausted,
    ServerThreadPanicked,
}

impl fmt::Display for TcpServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Manifest(error) => error.fmt(formatter),
            Self::Database(error) => write!(formatter, "database worker startup failed: {error}"),
            Self::AdaptiveConfig(error) => {
                write!(formatter, "adaptive driver startup failed: {error}")
            }
            Self::Operator(error) => error.fmt(formatter),
            Self::Bind { address, source } => {
                write!(
                    formatter,
                    "failed to bind TCP listener `{address}`: {source}"
                )
            }
            Self::ListenerConfiguration(error) => {
                write!(formatter, "failed to configure TCP listener: {error}")
            }
            Self::Accept(error) => write!(formatter, "TCP accept failed: {error}"),
            Self::ThreadSpawn(error) => write!(formatter, "failed to spawn server thread: {error}"),
            Self::StartupCleanup { startup, cleanup } => write!(
                formatter,
                "server startup failed: {startup}; database worker cleanup also failed: {cleanup}"
            ),
            Self::OperatorAndServerCleanup { operator, server } => write!(
                formatter,
                "operator shutdown failed: {operator}; server cleanup also failed: {server}"
            ),
            Self::WorkerStopped => formatter.write_str("database worker stopped unexpectedly"),
            Self::WorkerPanicked => formatter.write_str("database worker thread panicked"),
            Self::WorkerFatal(error) => error.fmt(formatter),
            Self::SessionIdExhausted => formatter.write_str("server session IDs are exhausted"),
            Self::ServerThreadPanicked => formatter.write_str("TCP server thread panicked"),
        }
    }
}

impl Error for TcpServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Manifest(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::AdaptiveConfig(error) => Some(error),
            Self::Operator(error) => Some(error),
            Self::Bind { source, .. }
            | Self::ListenerConfiguration(source)
            | Self::Accept(source)
            | Self::ThreadSpawn(source) => Some(source),
            Self::StartupCleanup { cleanup, .. } => Some(cleanup.as_ref()),
            Self::OperatorAndServerCleanup { server, .. } => Some(server.as_ref()),
            Self::WorkerFatal(error) => Some(error),
            Self::WorkerStopped
            | Self::WorkerPanicked
            | Self::SessionIdExhausted
            | Self::ServerThreadPanicked => None,
        }
    }
}

impl From<ManifestError> for TcpServerError {
    fn from(error: ManifestError) -> Self {
        Self::Manifest(error)
    }
}

pub struct TcpServer {
    config: ServerConfig,
    adaptive_override: Option<ServerAdaptiveStartupMode>,
    physical_design_override: Option<ServerPhysicalDesignAdvisorConfig>,
}

impl TcpServer {
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            adaptive_override: None,
            physical_design_override: None,
        }
    }

    /// Enables bounded autocommit Core-query feedback capture in this
    /// server's database worker. This explicitly replaces any manifest mode.
    #[must_use]
    pub fn with_adaptive_feedback(mut self, config: ServerAdaptiveFeedbackConfig) -> Self {
        self.adaptive_override = Some(ServerAdaptiveStartupMode::FeedbackOnly(config));
        self
    }

    /// Enables feedback capture plus host-time logical scheduling in the
    /// existing database worker. This explicitly replaces any manifest mode.
    #[must_use]
    pub fn with_adaptive_driver(mut self, config: ServerAdaptiveDriverConfig) -> Self {
        self.adaptive_override = Some(ServerAdaptiveStartupMode::Driven(Box::new(config)));
        self
    }

    /// Enables worker-owned physical-design evidence and read-only, on-demand
    /// recommendations. This setting is independent of the adaptive mode.
    #[must_use]
    pub fn with_physical_design_advisor(
        mut self,
        config: ServerPhysicalDesignAdvisorConfig,
    ) -> Self {
        self.physical_design_override = Some(config);
        self
    }

    pub fn start(self) -> Result<ServerHandle, TcpServerError> {
        let (
            listen,
            tables,
            limits,
            security,
            authorization,
            manifest_adaptive_mode,
            manifest_physical_design,
            operator_config,
        ) = self.config.into_parts();
        let adaptive_mode = self.adaptive_override.unwrap_or(manifest_adaptive_mode);
        let physical_design = self.physical_design_override.or(manifest_physical_design);
        validate_listener_security(listen, security.kind() == TransportKind::MutualTls)?;
        let table_count = tables.len();
        let transport_kind = security.kind();
        let tick_interval = adaptive_mode.tick_interval();
        let worker = DatabaseWorker::start(
            tables,
            limits.session_policy(),
            authorization,
            adaptive_mode,
            physical_design,
        )?;
        let metrics = ServerMetricsHandle::new();
        let listener = match TcpListener::bind(listen) {
            Ok(listener) => listener,
            Err(source) => {
                return Err(finish_startup_failure(
                    worker,
                    TcpServerError::Bind {
                        address: listen,
                        source,
                    },
                ));
            }
        };
        if let Err(error) = listener.set_nonblocking(true) {
            return Err(finish_startup_failure(
                worker,
                TcpServerError::ListenerConfiguration(error),
            ));
        }
        let local_addr = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                return Err(finish_startup_failure(
                    worker,
                    TcpServerError::ListenerConfiguration(error),
                ));
            }
        };
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let (adaptive_control_tx, adaptive_control_rx) = mpsc::channel();
        let (physical_design_control_tx, physical_design_control_rx) = mpsc::channel();
        let (operator_failure_tx, operator_failure_rx) = mpsc::channel();
        let (worker_tx, worker_rx) = mpsc::sync_channel(0);
        let server_metrics = metrics.clone();
        let join = match thread::Builder::new()
            .name("netbadb-tcp-server".into())
            .spawn(move || {
                let worker = worker_rx
                    .recv()
                    .map_err(|_| TcpServerError::WorkerStopped)?;
                run_accept_loop(
                    listener,
                    shutdown_rx,
                    worker,
                    limits,
                    security,
                    server_metrics,
                    ServerHostObservationConfig {
                        adaptive: ServerAdaptiveHostConfig {
                            tick_interval,
                            controls: adaptive_control_rx,
                            operator_failures: operator_failure_rx,
                        },
                        physical_design_controls: physical_design_control_rx,
                    },
                )
            }) {
            Ok(join) => join,
            Err(error) => {
                return Err(finish_startup_failure(
                    worker,
                    TcpServerError::ThreadSpawn(error),
                ));
            }
        };
        if let Err(error) = worker_tx.send(worker) {
            let startup = match join.join() {
                Ok(Ok(())) => TcpServerError::WorkerStopped,
                Ok(Err(error)) => error,
                Err(_) => TcpServerError::ServerThreadPanicked,
            };
            return Err(finish_startup_failure(error.0, startup));
        }
        let adaptive_control = ServerAdaptiveControlHandle::new(adaptive_control_tx);
        let physical_design_control =
            ServerPhysicalDesignControlHandle::new(physical_design_control_tx);
        let operator = match operator_config {
            Some(config) => match ServerOperatorPlane::start(
                config,
                adaptive_control.clone(),
                physical_design_control.clone(),
                operator_failure_tx,
            ) {
                Ok(operator) => Some(operator),
                Err(error) => {
                    let _ = shutdown_tx.send(());
                    let cleanup = join
                        .join()
                        .map_err(|_| TcpServerError::ServerThreadPanicked)
                        .and_then(|result| result);
                    return Err(match cleanup {
                        Ok(()) => TcpServerError::Operator(error),
                        Err(server) => TcpServerError::OperatorAndServerCleanup {
                            operator: Box::new(error),
                            server: Box::new(server),
                        },
                    });
                }
            },
            None => None,
        };
        Ok(ServerHandle {
            local_addr,
            table_count,
            transport_kind,
            metrics,
            shutdown_tx,
            adaptive_control,
            physical_design_control,
            operator,
            join: Some(join),
        })
    }

    pub fn run(self) -> Result<(), TcpServerError> {
        self.start()?.wait()
    }
}

pub struct ServerHandle {
    local_addr: SocketAddr,
    table_count: usize,
    transport_kind: TransportKind,
    metrics: ServerMetricsHandle,
    shutdown_tx: Sender<()>,
    adaptive_control: ServerAdaptiveControlHandle,
    physical_design_control: ServerPhysicalDesignControlHandle,
    operator: Option<ServerOperatorPlane>,
    join: Option<JoinHandle<Result<(), TcpServerError>>>,
}

impl ServerHandle {
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub fn table_count(&self) -> usize {
        self.table_count
    }

    #[must_use]
    pub const fn transport_kind(&self) -> TransportKind {
        self.transport_kind
    }

    #[must_use]
    pub fn metrics(&self) -> crate::ServerMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn metrics_handle(&self) -> ServerMetricsHandle {
        self.metrics.clone()
    }

    #[must_use]
    pub fn adaptive_control(&self) -> ServerAdaptiveControlHandle {
        self.adaptive_control.clone()
    }

    #[must_use]
    pub fn physical_design_control(&self) -> ServerPhysicalDesignControlHandle {
        self.physical_design_control.clone()
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

    pub fn shutdown(mut self) -> Result<(), TcpServerError> {
        let operator = self.operator.take().map(ServerOperatorPlane::shutdown);
        let _ = self.shutdown_tx.send(());
        combine_operator_and_server(operator, self.join_server())
    }

    pub fn wait(mut self) -> Result<(), TcpServerError> {
        let server = self.join_server();
        let operator = self.operator.take().map(ServerOperatorPlane::shutdown);
        combine_operator_and_server(operator, server)
    }

    fn join_server(&mut self) -> Result<(), TcpServerError> {
        let join = self
            .join
            .take()
            .ok_or(TcpServerError::ServerThreadPanicked)?;
        join.join()
            .map_err(|_| TcpServerError::ServerThreadPanicked)?
    }
}

fn combine_operator_and_server(
    operator: Option<Result<(), ServerOperatorError>>,
    server: Result<(), TcpServerError>,
) -> Result<(), TcpServerError> {
    match (operator.transpose(), server) {
        (Ok(_), Ok(())) => Ok(()),
        (Ok(_), Err(server)) => Err(server),
        (Err(operator), Ok(())) => Err(TcpServerError::Operator(operator)),
        (Err(operator), Err(server)) => Err(TcpServerError::OperatorAndServerCleanup {
            operator: Box::new(operator),
            server: Box::new(server),
        }),
    }
}

#[derive(Clone)]
struct WorkerClient {
    commands: Sender<WorkerCommand>,
}

impl WorkerClient {
    fn open_session(
        &self,
        session_id: SessionId,
        identity: ClientIdentity,
    ) -> Result<bool, WorkerRequestError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.commands
            .send(WorkerCommand::OpenSession {
                session_id,
                identity,
                reply,
            })
            .map_err(|_| WorkerRequestError::Stopped)?;
        response.recv().map_err(|_| WorkerRequestError::Stopped)?
    }

    fn request(
        &self,
        session_id: SessionId,
        frame: Frame<ClientMessage>,
        metrics: &ServerMetricsHandle,
    ) -> Result<SessionResponse, WorkerRequestError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.commands
            .send(WorkerCommand::Request {
                session_id,
                frame,
                reply,
            })
            .map_err(|_| WorkerRequestError::Stopped)?;
        metrics.worker_request();
        response.recv().map_err(|_| WorkerRequestError::Stopped)?
    }

    fn close_session(&self, session_id: SessionId) -> Result<(), WorkerRequestError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.commands
            .send(WorkerCommand::CloseSession { session_id, reply })
            .map_err(|_| WorkerRequestError::Stopped)?;
        response.recv().map_err(|_| WorkerRequestError::Stopped)?
    }
}

struct DatabaseWorker {
    client: WorkerClient,
    events: Receiver<WorkerFatalError>,
    join: Option<JoinHandle<Result<(), WorkerFatalError>>>,
}

impl DatabaseWorker {
    fn start(
        tables: Vec<TableBootstrap>,
        session_policy: SessionPolicy,
        authorization: AuthorizationPolicy,
        adaptive_mode: ServerAdaptiveStartupMode,
        physical_design_config: Option<ServerPhysicalDesignAdvisorConfig>,
    ) -> Result<Self, TcpServerError> {
        let (commands, command_rx) = mpsc::channel();
        let (events_tx, events) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("netbadb-database-worker".into())
            .spawn(move || {
                let entries = tables
                    .into_iter()
                    .map(|entry| (entry.path, entry.table))
                    .collect();
                let database = match Database::open_tables_with_expectation(entries) {
                    Ok(database) => database,
                    Err(error) => {
                        let _ = ready_tx.send(Err(WorkerStartupError::Database(error)));
                        return Ok(());
                    }
                };
                let adaptive = match ServerAdaptiveWorkerRuntime::new(adaptive_mode, &database) {
                    Ok(adaptive) => adaptive,
                    Err(error) => {
                        let _ = ready_tx.send(Err(WorkerStartupError::Adaptive(error)));
                        return database.close().map_err(|error| {
                            WorkerFatalError::DatabaseCloseFailed {
                                message: error.to_string(),
                            }
                        });
                    }
                };
                let physical_design = physical_design_config.map(ServerPhysicalDesignRuntime::new);
                if ready_tx.send(Ok(())).is_err() {
                    return database.close().map_err(|error| {
                        WorkerFatalError::DatabaseCloseFailed {
                            message: error.to_string(),
                        }
                    });
                }
                run_database_worker(
                    database,
                    session_policy,
                    authorization,
                    adaptive,
                    physical_design,
                    command_rx,
                    events_tx,
                )
            })
            .map_err(TcpServerError::ThreadSpawn)?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                client: WorkerClient { commands },
                events,
                join: Some(join),
            }),
            Ok(Err(WorkerStartupError::Database(error))) => {
                let _ = join.join();
                Err(TcpServerError::Database(error))
            }
            Ok(Err(WorkerStartupError::Adaptive(error))) => {
                let _ = join.join();
                Err(TcpServerError::AdaptiveConfig(error))
            }
            Err(_) => match join.join() {
                Ok(Err(error)) => Err(TcpServerError::WorkerFatal(error)),
                Ok(Ok(())) => Err(TcpServerError::WorkerStopped),
                Err(_) => Err(TcpServerError::WorkerPanicked),
            },
        }
    }

    fn client(&self) -> WorkerClient {
        self.client.clone()
    }

    fn poll_fatal(&self) -> Result<Option<WorkerFatalError>, TcpServerError> {
        match self.events.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(TcpServerError::WorkerStopped),
        }
    }

    fn shutdown(mut self) -> Result<(), TcpServerError> {
        let (reply, response) = mpsc::sync_channel(1);
        if self
            .client
            .commands
            .send(WorkerCommand::Shutdown { reply })
            .is_err()
        {
            return self.join_worker(None);
        }
        let result = match response.recv() {
            Ok(result) => result,
            Err(_) => {
                return match self.join_worker(None) {
                    Ok(()) => Err(TcpServerError::WorkerStopped),
                    Err(error) => Err(error),
                };
            }
        };
        match result {
            Ok(()) => self.join_worker(None),
            Err(error) => self.join_worker(Some(error)),
        }
    }

    fn finish_fatal(mut self, error: WorkerFatalError) -> Result<(), TcpServerError> {
        self.join_worker(Some(error))
    }

    fn join_worker(
        &mut self,
        expected_error: Option<WorkerFatalError>,
    ) -> Result<(), TcpServerError> {
        let join = self.join.take().ok_or(TcpServerError::WorkerStopped)?;
        match join.join() {
            Ok(Ok(())) => match expected_error {
                Some(error) => Err(TcpServerError::WorkerFatal(error)),
                None => Ok(()),
            },
            Ok(Err(error)) => Err(TcpServerError::WorkerFatal(expected_error.unwrap_or(error))),
            Err(_) => Err(TcpServerError::WorkerPanicked),
        }
    }
}

fn finish_startup_failure(worker: DatabaseWorker, startup: TcpServerError) -> TcpServerError {
    match worker.shutdown() {
        Ok(()) => startup,
        Err(cleanup) => TcpServerError::StartupCleanup {
            startup: Box::new(startup),
            cleanup: Box::new(cleanup),
        },
    }
}

enum WorkerCommand {
    OpenSession {
        session_id: SessionId,
        identity: ClientIdentity,
        reply: SyncSender<Result<bool, WorkerRequestError>>,
    },
    Request {
        session_id: SessionId,
        frame: Frame<ClientMessage>,
        reply: SyncSender<Result<SessionResponse, WorkerRequestError>>,
    },
    CloseSession {
        session_id: SessionId,
        reply: SyncSender<Result<(), WorkerRequestError>>,
    },
    Adaptive(ServerAdaptiveWorkerCommand),
    PhysicalDesign(ServerPhysicalDesignWorkerCommand),
    Shutdown {
        reply: SyncSender<Result<(), WorkerFatalError>>,
    },
}

enum WorkerStartupError {
    Database(DatabaseError),
    Adaptive(ServerAdaptiveDriverConfigError),
}

#[derive(Debug, Clone)]
enum WorkerRequestError {
    DuplicateSession(SessionId),
    MissingSession(SessionId),
    AuthorizationDenied(AuthorizationDenied),
    Stopped,
    Fatal(WorkerFatalError),
}

impl fmt::Display for WorkerRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateSession(session_id) => {
                write!(
                    formatter,
                    "session {} is already registered",
                    session_id.get()
                )
            }
            Self::MissingSession(session_id) => {
                write!(formatter, "session {} is not registered", session_id.get())
            }
            Self::AuthorizationDenied(error) => error.fmt(formatter),
            Self::Stopped => formatter.write_str("database worker stopped"),
            Self::Fatal(error) => error.fmt(formatter),
        }
    }
}

struct DatabaseWorkerState {
    database: Option<Database>,
    adaptive: Option<ServerAdaptiveWorkerRuntime>,
    physical_design: Option<ServerPhysicalDesignRuntime>,
    sessions: HashMap<SessionId, WorkerSession>,
    session_policy: SessionPolicy,
    authorization: AuthorizationPolicy,
}

struct WorkerSession {
    state: SessionState,
    identity: ClientIdentity,
    authorization: PrincipalAuthorization,
}

impl WorkerSession {
    fn new(
        policy: SessionPolicy,
        identity: ClientIdentity,
        authorization: PrincipalAuthorization,
    ) -> Self {
        Self {
            state: SessionState::with_policy(policy),
            identity,
            authorization,
        }
    }

    fn is_authenticated(&self) -> bool {
        self.identity.is_authenticated()
    }

    #[cfg(test)]
    fn handle(
        &mut self,
        database: &mut Database,
        request_id: u64,
        request: ClientMessage,
    ) -> SessionResponse {
        self.handle_with_server_observation(database, None, None, request_id, request)
    }

    #[cfg(test)]
    fn handle_with_adaptive_feedback(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        request_id: u64,
        request: ClientMessage,
    ) -> SessionResponse {
        self.handle_with_server_observation(database, adaptive_feedback, None, request_id, request)
    }

    fn handle_with_server_observation(
        &mut self,
        database: &mut Database,
        adaptive_feedback: Option<&mut ServerAdaptiveFeedbackRuntime>,
        physical_design: Option<&mut ServerPhysicalDesignRuntime>,
        request_id: u64,
        request: ClientMessage,
    ) -> SessionResponse {
        if !self.state.is_handshaken() || matches!(request, ClientMessage::Hello) {
            let first_hello =
                !self.state.is_handshaken() && matches!(request, ClientMessage::Hello);
            let mut response = self
                .state
                .handle_with_metadata(database, request_id, request);
            if first_hello && self.state.is_handshaken() {
                for message in &mut response.batch.messages {
                    if let netbadb_protocol::ServerMessage::HelloAck { tables, .. } = message {
                        tables.retain(|table| self.authorization.can_see(table.table_id));
                    }
                }
            }
            return response;
        }

        if let ClientMessage::Execute { sql } = &request {
            let prepared = match self.state.execution.prepare(database, sql, &[]) {
                Ok(prepared) => prepared,
                Err(error) => {
                    return SessionResponse::standard(
                        self.state.database_error_batch(request_id, error),
                    );
                }
            };
            if let Err(denial) = self
                .authorization
                .authorize_statement(&prepared.access(), &self.state.execution)
            {
                return authorization_denied_response(&self.state, request_id, denial);
            }
            return self.state.handle_prepared(
                database,
                adaptive_feedback,
                physical_design,
                request_id,
                &prepared,
            );
        }

        let denial = match &request {
            ClientMessage::Hello
            | ClientMessage::Ping
            | ClientMessage::Commit
            | ClientMessage::Rollback => None,
            ClientMessage::Begin { table_id } => {
                if self.state.transaction_state() != netbadb_protocol::WireTransactionState::None
                    || !database
                        .schema()
                        .tables()
                        .iter()
                        .any(|table| table.id == *table_id)
                {
                    None
                } else {
                    self.authorization
                        .authorize(AuthorizationAction::Transaction, *table_id)
                        .err()
                }
            }
            ClientMessage::Analyze { table_id } => {
                if self.state.transaction_state() != netbadb_protocol::WireTransactionState::None
                    || !database
                        .schema()
                        .tables()
                        .iter()
                        .any(|table| table.id == *table_id)
                {
                    None
                } else {
                    self.authorization
                        .authorize(AuthorizationAction::Analyze, *table_id)
                        .err()
                }
            }
            ClientMessage::Execute { .. } => None,
        };
        match denial {
            Some(denial) => authorization_denied_response(&self.state, request_id, denial),
            None => self
                .state
                .handle_with_metadata(database, request_id, request),
        }
    }
}

fn authorization_denied_response(
    state: &SessionState,
    request_id: u64,
    denial: AuthorizationDenied,
) -> SessionResponse {
    SessionResponse {
        batch: ResponseBatch {
            request_id,
            messages: vec![ServerMessage::Error {
                code: ProtocolErrorCode::Database,
                transaction_state: state.transaction_state(),
                message: crate::bounded_error_message(&denial.to_string()),
            }],
        },
        result_row_limit_exceeded: false,
        authorization_denied: true,
    }
}

impl DatabaseWorkerState {
    fn new(
        database: Database,
        session_policy: SessionPolicy,
        authorization: AuthorizationPolicy,
        adaptive: Option<ServerAdaptiveWorkerRuntime>,
        physical_design: Option<ServerPhysicalDesignRuntime>,
    ) -> Self {
        Self {
            database: Some(database),
            adaptive,
            physical_design,
            sessions: HashMap::new(),
            session_policy,
            authorization,
        }
    }

    fn close_session(&mut self, session_id: SessionId) -> Result<(), WorkerFatalError> {
        let session = self.sessions.get_mut(&session_id).ok_or_else(|| {
            WorkerFatalError::SessionCloseFailed {
                session_id,
                message: "session is not registered".into(),
            }
        })?;
        session
            .state
            .close()
            .map_err(|error| WorkerFatalError::SessionCloseFailed {
                session_id,
                message: error.to_string(),
            })?;
        self.sessions.remove(&session_id);
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), WorkerFatalError> {
        let session_ids = self.sessions.keys().copied().collect::<Vec<_>>();
        for session_id in session_ids {
            self.close_session(session_id)?;
        }
        let database =
            self.database
                .take()
                .ok_or_else(|| WorkerFatalError::DatabaseCloseFailed {
                    message: "database was already closed".into(),
                })?;
        database
            .close()
            .map_err(|error| WorkerFatalError::DatabaseCloseFailed {
                message: error.to_string(),
            })
    }
}

fn run_database_worker(
    database: Database,
    session_policy: SessionPolicy,
    authorization: AuthorizationPolicy,
    adaptive: Option<ServerAdaptiveWorkerRuntime>,
    physical_design: Option<ServerPhysicalDesignRuntime>,
    commands: Receiver<WorkerCommand>,
    events: Sender<WorkerFatalError>,
) -> Result<(), WorkerFatalError> {
    let mut state = DatabaseWorkerState::new(
        database,
        session_policy,
        authorization,
        adaptive,
        physical_design,
    );
    while let Ok(command) = commands.recv() {
        match command {
            WorkerCommand::OpenSession {
                session_id,
                identity,
                reply,
            } => {
                if state.sessions.contains_key(&session_id) {
                    let _ = reply.send(Err(WorkerRequestError::DuplicateSession(session_id)));
                    continue;
                }
                let principal = match state.authorization.admit(&identity) {
                    Ok(principal) => principal,
                    Err(error) => {
                        let _ = reply.send(Err(WorkerRequestError::AuthorizationDenied(error)));
                        continue;
                    }
                };
                let session = WorkerSession::new(state.session_policy, identity, principal);
                let authenticated = session.is_authenticated();
                state.sessions.insert(session_id, session);
                if reply.send(Ok(authenticated)).is_err() {
                    if let Err(error) = state.close_session(session_id) {
                        let _ = events.send(error.clone());
                        return Err(error);
                    }
                }
            }
            WorkerCommand::Request {
                session_id,
                frame,
                reply,
            } => {
                let Some(session) = state.sessions.get_mut(&session_id) else {
                    let _ = reply.send(Err(WorkerRequestError::MissingSession(session_id)));
                    continue;
                };
                let Some(database) = state.database.as_mut() else {
                    let error = WorkerFatalError::DatabaseCloseFailed {
                        message: "database is unavailable before worker shutdown".into(),
                    };
                    let _ = reply.send(Err(WorkerRequestError::Fatal(error.clone())));
                    let _ = events.send(error.clone());
                    return Err(error);
                };
                let response = session.handle_with_server_observation(
                    database,
                    state
                        .adaptive
                        .as_mut()
                        .map(ServerAdaptiveWorkerRuntime::feedback_mut),
                    state.physical_design.as_mut(),
                    frame.request_id,
                    frame.message,
                );
                let _ = reply.send(Ok(response));
            }
            WorkerCommand::CloseSession { session_id, reply } => {
                if !state.sessions.contains_key(&session_id) {
                    let _ = reply.send(Err(WorkerRequestError::MissingSession(session_id)));
                    continue;
                }
                match state.close_session(session_id) {
                    Ok(()) => {
                        let _ = reply.send(Ok(()));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(WorkerRequestError::Fatal(error.clone())));
                        let _ = events.send(error.clone());
                        return Err(error);
                    }
                }
            }
            WorkerCommand::Adaptive(command) => {
                let Some(database) = state.database.as_mut() else {
                    handle_disabled_worker_command(command);
                    continue;
                };
                match state.adaptive.as_mut() {
                    Some(adaptive) => adaptive.handle(database, command),
                    None => handle_disabled_worker_command(command),
                }
            }
            WorkerCommand::PhysicalDesign(command) => {
                let Some(database) = state.database.as_mut() else {
                    crate::physical_design::handle_disabled_physical_design_worker_command(command);
                    continue;
                };
                match state.physical_design.as_mut() {
                    Some(runtime) => runtime.handle(database, command),
                    None => crate::physical_design::handle_disabled_physical_design_worker_command(
                        command,
                    ),
                }
            }
            WorkerCommand::Shutdown { reply } => {
                let result = state.shutdown();
                let _ = reply.send(result.clone());
                if let Err(error) = &result {
                    let _ = events.send(error.clone());
                }
                return result;
            }
        }
    }
    let result = state.shutdown();
    if let Err(error) = &result {
        let _ = events.send(error.clone());
    }
    result
}

struct ConnectionThread {
    session_id: SessionId,
    control: TcpStream,
    join: JoinHandle<Result<(), ConnectionError>>,
}

fn run_accept_loop(
    listener: TcpListener,
    shutdown: Receiver<()>,
    worker: DatabaseWorker,
    limits: ServerLimits,
    security: TransportSecurity,
    metrics: ServerMetricsHandle,
    host_config: ServerHostObservationConfig,
) -> Result<(), TcpServerError> {
    let client = worker.client();
    let mut connections = Vec::new();
    let mut next_session_id = 1_u64;
    let mut fatal = None;
    let mut adaptive_host = host_config
        .adaptive
        .tick_interval
        .map(ServerAdaptiveHostDriver::new);

    loop {
        match shutdown.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {}
        }
        if host_config.adaptive.operator_failures.try_recv().is_ok() {
            break;
        }
        match worker.poll_fatal() {
            Ok(Some(error)) => {
                fatal = Some(error);
                break;
            }
            Ok(None) => {}
            Err(error) => {
                return finish_accept_loop(connections, worker, &metrics, Err(error));
            }
        }
        reap_connections(&mut connections, &client, &metrics);
        if let Some(host) = adaptive_host.as_mut() {
            host.poll(|command| {
                client
                    .commands
                    .send(WorkerCommand::Adaptive(command))
                    .map_err(|_| ())
            });
        }
        let host_snapshot = adaptive_host
            .as_ref()
            .map(ServerAdaptiveHostDriver::snapshot);
        forward_control_requests(&host_config.adaptive.controls, host_snapshot, |command| {
            client
                .commands
                .send(WorkerCommand::Adaptive(command))
                .map_err(|_| ())
        });
        forward_physical_design_control_requests(
            &host_config.physical_design_controls,
            |command| {
                client
                    .commands
                    .send(WorkerCommand::PhysicalDesign(command))
                    .map_err(|_| ())
            },
        );

        match listener.accept() {
            Ok((stream, _peer)) => {
                if connections.len() >= limits.max_connections() {
                    metrics.rejected();
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
                let session_id = SessionId(next_session_id);
                next_session_id = match next_session_id.checked_add(1) {
                    Some(0) | None => {
                        return finish_accept_loop(
                            connections,
                            worker,
                            &metrics,
                            Err(TcpServerError::SessionIdExhausted),
                        );
                    }
                    Some(next) => next,
                };
                match register_connection(
                    stream,
                    session_id,
                    &client,
                    limits,
                    security.clone(),
                    &metrics,
                    &mut connections,
                ) {
                    Ok(()) => metrics.accepted(),
                    Err(error) => {
                        eprintln!(
                            "netbadb connection {} setup failed: {error}",
                            session_id.get()
                        );
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                return finish_accept_loop(
                    connections,
                    worker,
                    &metrics,
                    Err(TcpServerError::Accept(error)),
                );
            }
        }
    }

    for connection in &connections {
        let _ = connection.control.shutdown(Shutdown::Both);
    }
    join_connections(connections, &client, &metrics);
    if fatal.is_none() {
        if let Ok(Some(error)) = worker.poll_fatal() {
            fatal = Some(error);
        }
    }
    match fatal {
        Some(error) => worker.finish_fatal(error),
        None => worker.shutdown(),
    }
}

fn finish_accept_loop(
    connections: Vec<ConnectionThread>,
    worker: DatabaseWorker,
    metrics: &ServerMetricsHandle,
    result: Result<(), TcpServerError>,
) -> Result<(), TcpServerError> {
    for connection in &connections {
        let _ = connection.control.shutdown(Shutdown::Both);
    }
    let client = worker.client();
    join_connections(connections, &client, metrics);
    match worker.shutdown() {
        Ok(()) => result,
        Err(error) => Err(error),
    }
}

fn register_connection(
    stream: TcpStream,
    session_id: SessionId,
    worker: &WorkerClient,
    limits: ServerLimits,
    security: TransportSecurity,
    metrics: &ServerMetricsHandle,
    connections: &mut Vec<ConnectionThread>,
) -> Result<(), ConnectionError> {
    configure_connection_stream(&stream, limits).map_err(ConnectionError::Configure)?;
    let control = match stream.try_clone() {
        Ok(control) => control,
        Err(error) => return Err(ConnectionError::Configure(error)),
    };
    let connection_worker = worker.clone();
    let connection_metrics = metrics.clone();
    let join = match thread::Builder::new()
        .name(format!("netbadb-connection-{}", session_id.get()))
        .spawn(move || {
            run_connection(
                stream,
                session_id,
                connection_worker,
                security,
                connection_metrics,
            )
        }) {
        Ok(join) => join,
        Err(error) => return Err(ConnectionError::Spawn(error)),
    };
    connections.push(ConnectionThread {
        session_id,
        control,
        join,
    });
    Ok(())
}

fn configure_connection_stream(stream: &TcpStream, limits: ServerLimits) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(limits.idle_timeout()))?;
    stream.set_write_timeout(Some(limits.write_timeout()))
}

fn run_connection(
    stream: TcpStream,
    session_id: SessionId,
    worker: WorkerClient,
    security: TransportSecurity,
    metrics: ServerMetricsHandle,
) -> Result<(), ConnectionError> {
    let tls_enabled = security.kind() == TransportKind::MutualTls;
    let (mut stream, identity) = security
        .establish(stream)
        .map_err(ConnectionError::TlsHandshake)?;
    if tls_enabled {
        metrics.tls_handshake();
    }
    let authenticated = worker
        .open_session(session_id, identity)
        .map_err(ConnectionError::Worker)?;
    if authenticated {
        metrics.authenticated_connection();
    }
    let request_result = run_connection_requests(&mut stream, session_id, &worker, &metrics);
    let close_result = worker
        .close_session(session_id)
        .map_err(ConnectionError::Worker);
    stream.close();
    close_result?;
    request_result
}

fn run_connection_requests(
    stream: &mut ConnectionStream,
    session_id: SessionId,
    worker: &WorkerClient,
    metrics: &ServerMetricsHandle,
) -> Result<(), ConnectionError> {
    loop {
        let frame = match read_client_frame(stream) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => return Err(ConnectionError::Read(error)),
        };
        let response = worker
            .request(session_id, frame, metrics)
            .map_err(ConnectionError::Worker)?;
        if response.result_row_limit_exceeded {
            metrics.query_response_limit_error();
        }
        if response.authorization_denied {
            metrics.authorization_denial();
        }
        let response = response.batch;
        for message in response.messages {
            write_server_frame(
                stream,
                &Frame {
                    request_id: response.request_id,
                    message,
                },
            )
            .map_err(ConnectionError::WriteProtocol)?;
        }
        stream.flush().map_err(ConnectionError::Flush)?;
    }
}

fn reap_connections(
    connections: &mut Vec<ConnectionThread>,
    worker: &WorkerClient,
    metrics: &ServerMetricsHandle,
) {
    let mut index = 0;
    while index < connections.len() {
        if connections[index].join.is_finished() {
            let connection = connections.swap_remove(index);
            join_connection(connection, worker, metrics);
        } else {
            index += 1;
        }
    }
}

fn join_connections(
    connections: Vec<ConnectionThread>,
    worker: &WorkerClient,
    metrics: &ServerMetricsHandle,
) {
    for connection in connections {
        join_connection(connection, worker, metrics);
    }
}

fn join_connection(
    connection: ConnectionThread,
    worker: &WorkerClient,
    metrics: &ServerMetricsHandle,
) {
    match connection.join.join() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            error.record(metrics);
            eprintln!(
                "netbadb connection {} closed: {error}",
                connection.session_id.get()
            );
        }
        Err(_) => {
            eprintln!(
                "netbadb connection {} thread panicked",
                connection.session_id.get()
            );
            let _ = worker.close_session(connection.session_id);
        }
    }
    metrics.closed();
}

#[derive(Debug)]
enum ConnectionError {
    Configure(io::Error),
    Spawn(io::Error),
    TlsHandshake(TlsHandshakeError),
    Read(ProtocolError),
    WriteProtocol(ProtocolError),
    Flush(io::Error),
    Worker(WorkerRequestError),
}

impl ConnectionError {
    fn record(&self, metrics: &ServerMetricsHandle) {
        match self {
            Self::TlsHandshake(error) => {
                metrics.tls_handshake_failure();
                if error.is_timeout() {
                    metrics.idle_timeout();
                }
            }
            Self::Read(ProtocolError::Io(error)) if is_timeout(error) => metrics.idle_timeout(),
            Self::Read(ProtocolError::Io(_)) => {}
            Self::Read(_) => metrics.protocol_failure(),
            Self::WriteProtocol(_) | Self::Flush(_) => metrics.write_failure(),
            Self::Worker(WorkerRequestError::AuthorizationDenied(_)) => {
                metrics.authorization_denial();
            }
            Self::Configure(_) | Self::Spawn(_) | Self::Worker(_) => {}
        }
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configure(error) => write!(formatter, "socket configuration failed: {error}"),
            Self::Spawn(error) => write!(formatter, "connection thread spawn failed: {error}"),
            Self::TlsHandshake(error) => error.fmt(formatter),
            Self::Read(error) => write!(formatter, "request read failed: {error}"),
            Self::WriteProtocol(error) => write!(formatter, "response write failed: {error}"),
            Self::Flush(error) => write!(formatter, "response flush failed: {error}"),
            Self::Worker(error) => error.fmt(formatter),
        }
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn connection_configuration_applies_read_and_write_timeouts() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (stream, _) = listener.accept().unwrap();
        let limits =
            ServerLimits::new(1, Duration::from_millis(250), Duration::from_millis(500), 1)
                .unwrap();

        configure_connection_stream(&stream, limits).unwrap();
        assert!(stream.nodelay().unwrap());
        assert_eq!(stream.read_timeout().unwrap(), Some(limits.idle_timeout()));
        assert_eq!(
            stream.write_timeout().unwrap(),
            Some(limits.write_timeout())
        );
        drop(client);
    }

    #[test]
    fn server_handle_is_finished_observes_main_thread_completion() {
        let (shutdown_tx, _shutdown_rx) = mpsc::channel();
        let (adaptive_tx, _adaptive_rx) = mpsc::channel();
        let (physical_design_tx, _physical_design_rx) = mpsc::channel();
        let join = thread::spawn(|| Ok(()));
        while !join.is_finished() {
            thread::yield_now();
        }
        let server = ServerHandle {
            local_addr: "127.0.0.1:1".parse().unwrap(),
            table_count: 0,
            transport_kind: TransportKind::PlaintextLoopback,
            metrics: ServerMetricsHandle::new(),
            shutdown_tx,
            adaptive_control: ServerAdaptiveControlHandle::new(adaptive_tx),
            physical_design_control: ServerPhysicalDesignControlHandle::new(physical_design_tx),
            operator: None,
            join: Some(join),
        };

        assert!(server.is_finished());
        server.shutdown().unwrap();
    }
}

#[cfg(test)]
#[path = "native_adaptive_feedback_tests.rs"]
mod adaptive_feedback_tests;

#[cfg(test)]
#[path = "native_alter_table_tests.rs"]
mod alter_table_tests;
#[cfg(test)]
#[path = "native_create_table_tests.rs"]
mod create_table_tests;
#[cfg(test)]
#[path = "native_drop_table_tests.rs"]
mod drop_table_tests;
