use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use netbadb_core::{
    Database, DatabaseError, DatabaseErrorKind, ExecutionResult, IndexKindInspection,
    PreparedStatement as CorePrepared, QueryResult, StatementAccess, StatementDescription,
    TablePlacementInspection,
};
use netbadb_pgwire::{
    BackendMessage, CloseTarget, DescribeTarget, ErrorResponse, FieldDescription, FormatCode,
    FrontendMessage, PostgresOid, PostgresType, StartupMessage, StartupPacket, TypeMappingError,
    WireError, decode_binary_parameter, decode_text_parameter, encode_binary_value,
    encode_text_value, read_frontend_message, read_startup_packet, write_backend_message,
};
use netbadb_protocol::WireTransactionState;
use netbadb_types::{ColumnId, PhysicalType, ScalarValue, SemanticType, TableId};
use sha2::{Digest, Sha256};

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
    execution: PreparedExecution,
    parameters: Vec<PostgresOid>,
    fields: Vec<FieldDescription>,
    is_query: bool,
}

#[derive(Clone)]
enum PreparedExecution {
    Core(Box<CorePrepared>),
    Compatibility(CompatibilityStatement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompatibilityStatement {
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
                index_name_inputs.push((
                    key,
                    table.name.as_str(),
                    index.column_name.as_str(),
                    digest,
                ));
            }
        }
        identities.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        let object_oids = assign_synthetic_oids(&identities, SYNTHETIC_OID_BASE);
        let index_names = assign_compatibility_index_names(&index_name_inputs);
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
                            name: index_names[&key].clone(),
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

fn assign_compatibility_index_names(
    inputs: &[(PgCatalogObjectKey, &str, &str, [u8; 32])],
) -> HashMap<PgCatalogObjectKey, String> {
    let mut inputs = inputs.to_vec();
    inputs.sort_by(|left, right| left.3.cmp(&right.3).then(left.0.cmp(&right.0)));
    let mut assigned = HashMap::with_capacity(inputs.len());
    let mut used = HashSet::with_capacity(inputs.len());
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
                trace_enabled,
            },
            messages,
        ))
    }

    fn handle(&mut self, database: &mut Database, message: FrontendMessage) -> Vec<BackendMessage> {
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
        if is_index_ddl(&normalize_sql(&query)) {
            return self.extended_error(fixed_error(
                "0A000",
                "PostgreSQL index DDL is unsupported; use the native index API",
            ));
        }
        if let Some(compatibility) = classify_compatibility_statement(&query) {
            if self.trace_enabled {
                eprintln!("netbadb postgres trace: compatibility classification={compatibility:?}");
            }
            return self.parse_compatibility(statement, query, parameter_types, compatibility);
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
                execution: PreparedExecution::Core(Box::new(prepared)),
                parameters: inferred_oids,
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
        let expected_parameters = compatibility_parameter_types(compatibility);
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
                        if supplied.netbadb_physical() == expected.netbadb_physical() {
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
        sql: &str,
        execution: &PreparedExecution,
        values: &[ScalarValue],
        fields: &[FieldDescription],
    ) -> Result<PortalResult, ErrorResponse> {
        let result = match execution {
            PreparedExecution::Core(prepared) => {
                self.execute_prepared_core(database, prepared, values)?
            }
            PreparedExecution::Compatibility(statement) => {
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
        if classify_compatibility_statement(sql).is_some() {
            return Err(fixed_error(
                "0A000",
                "parameterized catalog reflection requires Extended Query",
            ));
        }
        if is_index_ddl(&normalized) {
            return Err(fixed_error(
                "0A000",
                "PostgreSQL index DDL is unsupported; use the native index API",
            ));
        }
        let result = self.execute_core(database, sql)?;
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
        let transaction_anchor = self
            .catalog
            .tables
            .iter()
            .find(|table| self.authorization.can_see(table.table_id))
            .map(|table| table.table_id)
            .ok_or_else(|| fixed_error("42501", "no authorized transaction anchor"))?;
        self.execution
            .begin(database, Some(transaction_anchor))
            .map_err(|error| map_database_error(&error))?;
        self.status = PgTransactionStatus::InTransaction;
        self.savepoints.clear();
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
        if self.status == PgTransactionStatus::InTransaction {
            self.status = PgTransactionStatus::Failed;
        }
        self.awaiting_sync = true;
        vec![BackendMessage::ErrorResponse(error)]
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

fn is_index_ddl(normalized: &str) -> bool {
    normalized.starts_with("create index ")
        || normalized.starts_with("create unique index ")
        || normalized.starts_with("drop index ")
}

fn compatibility_parameter_types(statement: CompatibilityStatement) -> &'static [PostgresType] {
    match statement {
        CompatibilityStatement::TypeLookup | CompatibilityStatement::SchemaNames => {
            &[PostgresType::Text]
        }
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
    match statement {
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
                        let format_type = match column.physical {
                            PhysicalType::Bool => "boolean",
                            PhysicalType::Int64 => "bigint",
                            PhysicalType::Text => "text",
                            PhysicalType::UInt64 => {
                                return Err(fixed_error(
                                    "0A000",
                                    "UINT64 has no lossless PostgreSQL reflection type",
                                ));
                            }
                        };
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
            let [
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
                    "table OID lookup requires seven text parameters",
                ));
            };
            let exposes_tables = [kind_1, kind_2, kind_3, kind_4, kind_5]
                .into_iter()
                .any(|kind| kind == "r");
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
                            PhysicalType::Int64 => Ok("int8_ops"),
                            PhysicalType::Text => Ok("text_ops"),
                            PhysicalType::UInt64 => Err(fixed_error(
                                "0A000",
                                "UINT64 indexes cannot be represented by PostgreSQL reflection",
                            )),
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

fn parameter_constraint(oid: PostgresOid) -> Result<Option<PhysicalType>, ErrorResponse> {
    if oid.0 == 0 {
        return Ok(None);
    }
    PostgresType::from_oid(oid)
        .and_then(PostgresType::netbadb_physical)
        .map(Some)
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

    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::ColumnId;

    use super::*;
    use crate::authorization::TablePermissions;

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
            Some(permissions),
            Vec::new(),
            known,
        )
        .expect("authorization policy")
        .admit(&ClientIdentity::LocalPlaintext)
        .expect("principal")
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
                columns: Vec::new(),
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

        database.close().expect("close catalog fixture");
        let reopened = Database::open_tables(paths.iter().cloned().zip(tables).collect::<Vec<_>>())
            .expect("reopen catalog fixture");
        let reopened_catalog =
            PgCompatibilityCatalog::derive(&reopened).expect("derive reopened catalog");
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
}
