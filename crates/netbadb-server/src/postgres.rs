use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use netbadb_core::{
    Database, DatabaseError, DatabaseErrorKind, ExecutionResult, PreparedStatement as CorePrepared,
    QueryResult, StatementAccess, StatementDescription,
};
use netbadb_pgwire::{
    BackendMessage, CloseTarget, DescribeTarget, ErrorResponse, FieldDescription, FormatCode,
    FrontendMessage, PostgresOid, PostgresType, StartupMessage, StartupPacket, TypeMappingError,
    WireError, decode_binary_parameter, decode_text_parameter, encode_binary_value,
    encode_text_value, read_frontend_message, read_startup_packet, write_backend_message,
};
use netbadb_protocol::WireTransactionState;
use netbadb_types::{PhysicalType, ScalarValue};

use crate::authorization::{AuthorizationAction, AuthorizationPolicy, PrincipalAuthorization};
use crate::{
    ClientIdentity, DatabaseSession, ServerConfig, ServerLimits, SessionPolicy, TableBootstrap,
    TransportKind,
};

const MAX_PREPARED_STATEMENTS: usize = 1_024;
const MAX_PORTALS: usize = 1_024;
const MAX_ERROR_BYTES: usize = 8 * 1024;

pub struct PostgresTcpServer {
    config: ServerConfig,
}

impl PostgresTcpServer {
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    pub fn start(self) -> Result<PostgresServerHandle, PostgresTcpServerError> {
        let (listen, tables, limits, security, authorization) = self.config.into_parts();
        if security.kind() != TransportKind::PlaintextLoopback {
            return Err(PostgresTcpServerError::TlsManifestUnsupported);
        }
        let worker = PgDatabaseWorker::start(tables, limits.session_policy(), authorization)?;
        let listener = match TcpListener::bind(listen) {
            Ok(listener) => listener,
            Err(source) => {
                let _ = worker.shutdown();
                return Err(PostgresTcpServerError::Bind {
                    address: listen,
                    source,
                });
            }
        };
        if let Err(error) = listener.set_nonblocking(true) {
            let _ = worker.shutdown();
            return Err(PostgresTcpServerError::ListenerConfiguration(error));
        }
        let local_addr = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                let _ = worker.shutdown();
                return Err(PostgresTcpServerError::ListenerConfiguration(error));
            }
        };
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let join = thread::Builder::new()
            .name("netbadb-postgres-server".into())
            .spawn(move || run_pg_accept_loop(listener, shutdown_rx, worker, limits))
            .map_err(PostgresTcpServerError::ThreadSpawn)?;
        Ok(PostgresServerHandle {
            local_addr,
            shutdown_tx,
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
    join: Option<JoinHandle<Result<(), PostgresTcpServerError>>>,
}

impl PostgresServerHandle {
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(mut self) -> Result<(), PostgresTcpServerError> {
        let _ = self.shutdown_tx.send(());
        self.join_server()
    }

    pub fn wait(mut self) -> Result<(), PostgresTcpServerError> {
        self.join_server()
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
            _ => None,
        }
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
) -> Result<(), PostgresTcpServerError> {
    let mut connections = Vec::new();
    let mut next_session_id = 1_u64;
    loop {
        match shutdown.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {}
        }
        reap_pg_connections(&mut connections);
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
                        let _ = ready_tx.send(Err(error));
                        return Ok(());
                    }
                };
                if ready_tx.send(Ok(())).is_err() {
                    return database.close().map_err(|error| error.to_string());
                }
                run_pg_worker(database, policy, authorization, receiver)
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
            Err(error) => {
                let _ = join.join();
                Err(PostgresTcpServerError::Database(error))
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
    Shutdown {
        reply: SyncSender<()>,
    },
}

fn run_pg_worker(
    mut database: Database,
    policy: SessionPolicy,
    authorization: AuthorizationPolicy,
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
                    PgWorkerSession::new(policy, principal, startup, session_id);
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
                let messages = session.handle(&mut database, message);
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
    prepared: CorePrepared,
    parameters: Vec<PostgresOid>,
    fields: Vec<FieldDescription>,
    is_query: bool,
}

struct Portal {
    statement: String,
    sql: String,
    prepared: CorePrepared,
    values: Vec<ScalarValue>,
    fields: Vec<FieldDescription>,
    result: Option<PortalResult>,
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
}

impl PgWorkerSession {
    fn new(
        policy: SessionPolicy,
        authorization: PrincipalAuthorization,
        startup: StartupMessage,
        session_id: u64,
    ) -> (Self, Vec<BackendMessage>) {
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
        (
            Self {
                execution: DatabaseSession::with_policy(policy),
                authorization,
                user,
                database_name,
                status: PgTransactionStatus::Idle,
                prepared: HashMap::new(),
                portals: HashMap::new(),
                awaiting_sync: false,
            },
            messages,
        )
    }

    fn handle(&mut self, database: &mut Database, message: FrontendMessage) -> Vec<BackendMessage> {
        if self.awaiting_sync {
            return match message {
                FrontendMessage::Sync => {
                    self.awaiting_sync = false;
                    vec![BackendMessage::ReadyForQuery(self.status.ready_byte())]
                }
                _ => Vec::new(),
            };
        }
        match message {
            FrontendMessage::Query(sql) => self.simple_query(database, &sql),
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
                self.execute_portal(database, &portal, max_rows)
            }
            FrontendMessage::Close { target, name } => self.close_object(target, &name),
            FrontendMessage::Sync => vec![BackendMessage::ReadyForQuery(self.status.ready_byte())],
            FrontendMessage::Flush => Vec::new(),
            FrontendMessage::Password(_) => {
                self.extended_error(fixed_error("08P01", "unexpected PasswordMessage"))
            }
            FrontendMessage::Terminate => Vec::new(),
        }
    }

    fn simple_query(&mut self, database: &mut Database, sql: &str) -> Vec<BackendMessage> {
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
            match self.execute_statement(database, statement) {
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
        let declared = match parameter_types
            .iter()
            .map(|oid| parameter_constraint(*oid))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(declared) => declared,
            Err(error) => return self.extended_error(error),
        };
        let prepared = match database.prepare_statement(&query, &declared) {
            Ok(prepared) => prepared,
            Err(error) => return self.extended_error(map_database_error(&error)),
        };
        let description = prepared.description();
        let inferred_oids = match prepared
            .parameters()
            .iter()
            .enumerate()
            .map(
                |(index, parameter)| match parameter_types.get(index).copied() {
                    Some(PostgresOid(oid)) if oid != 0 => PostgresType::from_oid(PostgresOid(oid))
                        .map(PostgresType::oid)
                        .ok_or_else(|| {
                            map_type_error(TypeMappingError::UnsupportedOid(PostgresOid(oid)))
                        }),
                    _ => PostgresType::from_netbadb(parameter.data_type.physical)
                        .map(PostgresType::oid)
                        .map_err(map_type_error),
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
                prepared,
                parameters: inferred_oids,
                fields,
                is_query: description.is_query,
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
            .zip(&parameter_formats)
            .map(|((bytes, oid), format)| match format {
                FormatCode::Text => decode_text_parameter(bytes.as_deref(), *oid),
                FormatCode::Binary => decode_binary_parameter(bytes.as_deref(), *oid),
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
        let core_prepared = prepared.prepared.clone();
        if portal.is_empty() {
            self.portals.remove("");
        }
        self.portals.insert(
            portal,
            Portal {
                statement,
                sql,
                prepared: core_prepared,
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
        name: &str,
        max_rows: u32,
    ) -> Vec<BackendMessage> {
        let Some(mut portal) = self.portals.remove(name) else {
            return self.extended_error(fixed_error("34000", "portal does not exist"));
        };
        if portal.result.is_none() {
            let result = match self.execute_to_portal(
                database,
                &portal.sql,
                &portal.prepared,
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
        sql: &str,
        prepared: &CorePrepared,
        values: &[ScalarValue],
        fields: &[FieldDescription],
    ) -> Result<PortalResult, ErrorResponse> {
        let result = self.execute_prepared_core(database, prepared, values)?;
        match result {
            ExecutionResult::Query(query) => Ok(PortalResult::Query {
                rows: encode_query_rows(&query, fields, self.execution.policy.max_result_rows())?,
                position: 0,
            }),
            ExecutionResult::AffectedRows(count) => Ok(PortalResult::Command {
                tag: command_tag(sql, count),
            }),
        }
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
        sql: &str,
    ) -> Result<Vec<BackendMessage>, ErrorResponse> {
        let normalized = normalize_sql(sql);
        match normalized.as_str() {
            "begin" | "begin transaction" | "start transaction" => return self.begin(database),
            "commit" | "commit transaction" => return self.commit(),
            "rollback" | "rollback transaction" => return self.rollback(),
            _ => {}
        }
        if self.status == PgTransactionStatus::Failed {
            return Err(fixed_error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        if let Some(messages) = self.compatibility_query(&normalized) {
            return Ok(messages);
        }
        let result = self.execute_core(database, sql)?;
        match result {
            ExecutionResult::Query(query) => {
                query_messages(query, self.execution.policy.max_result_rows())
                    .map_err(|error| self.record_protocol_error(error))
            }
            ExecutionResult::AffectedRows(count) => Ok(vec![BackendMessage::CommandComplete(
                command_tag(sql, count),
            )]),
        }
    }

    fn execute_core(
        &mut self,
        database: &mut Database,
        sql: &str,
    ) -> Result<ExecutionResult, ErrorResponse> {
        let access = database
            .statement_access(sql)
            .map_err(|error| self.record_error(&error))?;
        self.authorize_access(&access)?;
        self.execution
            .execute(database, sql)
            .map_err(|error| self.record_error(&error))
    }

    fn execute_prepared_core(
        &mut self,
        database: &mut Database,
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
        self.execution
            .execute_prepared(database, prepared, values)
            .map_err(|error| self.record_error(&error))
    }

    fn authorize_access(&mut self, access: &StatementAccess) -> Result<(), ErrorResponse> {
        for table in access.read_tables() {
            if self
                .authorization
                .authorize(AuthorizationAction::Read, *table)
                .is_err()
            {
                return Err(self.record_protocol_error(fixed_error(
                    "42501",
                    "permission denied for relation",
                )));
            }
        }
        for table in access.write_tables() {
            if self
                .authorization
                .authorize(AuthorizationAction::Write, *table)
                .is_err()
            {
                return Err(self.record_protocol_error(fixed_error(
                    "42501",
                    "permission denied for relation",
                )));
            }
        }
        Ok(())
    }

    fn record_error(&mut self, error: &DatabaseError) -> ErrorResponse {
        if self.status == PgTransactionStatus::InTransaction {
            self.status = PgTransactionStatus::Failed;
        }
        map_database_error(error)
    }

    fn record_protocol_error(&mut self, error: ErrorResponse) -> ErrorResponse {
        if self.status == PgTransactionStatus::InTransaction {
            self.status = PgTransactionStatus::Failed;
        }
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
        self.execution
            .begin(database, None)
            .map_err(|error| map_database_error(&error))?;
        self.status = PgTransactionStatus::InTransaction;
        Ok(vec![BackendMessage::CommandComplete("BEGIN".into())])
    }

    fn commit(&mut self) -> Result<Vec<BackendMessage>, ErrorResponse> {
        match self.status {
            PgTransactionStatus::Idle => Ok(vec![BackendMessage::CommandComplete("COMMIT".into())]),
            PgTransactionStatus::Failed => Err(fixed_error(
                "25P02",
                "current transaction is aborted; ROLLBACK is required",
            )),
            PgTransactionStatus::InTransaction => {
                self.execution
                    .commit()
                    .map_err(|error| map_database_error(&error))?;
                self.status = PgTransactionStatus::Idle;
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
            "select version()" => (
                "version",
                PostgresType::Text,
                "NetbaDB 0.1 experimental PostgreSQL compatibility".to_owned(),
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
        if self.status == PgTransactionStatus::InTransaction {
            self.status = PgTransactionStatus::Failed;
        }
        self.awaiting_sync = true;
        vec![BackendMessage::ErrorResponse(error)]
    }
}

fn parameter_status(name: &str, value: &str) -> BackendMessage {
    BackendMessage::ParameterStatus {
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

fn parameter_constraint(oid: PostgresOid) -> Result<Option<PhysicalType>, ErrorResponse> {
    if oid.0 == 0 {
        return Ok(None);
    }
    PostgresType::from_oid(oid)
        .map(|data_type| Some(data_type.netbadb_physical()))
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
        DatabaseErrorKind::Syntax => ("42601", Some(error.to_string())),
        DatabaseErrorKind::UndefinedTable => ("42P01", Some(error.to_string())),
        DatabaseErrorKind::UndefinedColumn => ("42703", Some(error.to_string())),
        DatabaseErrorKind::AmbiguousColumn => ("42702", Some(error.to_string())),
        DatabaseErrorKind::DatatypeMismatch => ("42804", Some(error.to_string())),
        DatabaseErrorKind::IndeterminateDatatype => ("42P18", Some(error.to_string())),
        DatabaseErrorKind::ParameterCount => ("08P01", Some(error.to_string())),
        DatabaseErrorKind::NotNullViolation => ("23502", Some(error.to_string())),
        DatabaseErrorKind::FeatureNotSupported => ("0A000", Some(error.to_string())),
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
    use super::*;

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
    fn database_error_mapping_uses_stable_sqlstate_and_position() {
        use netbadb_compiler::CompileError;
        use netbadb_parser::{ParseError, Span};

        let error = DatabaseError::Compile(CompileError::Parse(ParseError {
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
}
