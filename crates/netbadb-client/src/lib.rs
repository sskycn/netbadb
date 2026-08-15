//! Blocking Protocol v1 client for a single NetbaDB remote session.

mod tls;

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use netbadb_protocol::{
    ClientMessage, Frame, MAX_FRAME_PAYLOAD, PROTOCOL_VERSION, ProtocolError, ServerMessage,
    WireResultColumn, read_server_frame, write_client_frame,
};
use netbadb_schema::{SchemaError, SchemaFingerprint, TableDef};
use netbadb_types::{ScalarValue, SemanticType, TableId};

pub use netbadb_protocol::{
    CAPABILITY_ANALYZE, CAPABILITY_EXPLICIT_TRANSACTIONS, CAPABILITY_STREAMED_QUERY_RESULTS,
    ProtocolErrorCode, WireTransactionState,
};
use tls::ConnectionStream;
pub use tls::{TlsConfig, TlsConfigError, TlsHandshakeError};

/// Canonical identity required or advertised during Protocol v1 Hello.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableIdentity {
    pub table_id: TableId,
    pub fingerprint: SchemaFingerprint,
}

impl TableIdentity {
    /// Uses the authoritative canonical Rust schema fingerprint implementation.
    pub fn from_table(table: &TableDef) -> Result<Self, SchemaError> {
        Ok(Self {
            table_id: table.id,
            fingerprint: table.fingerprint()?,
        })
    }
}

/// Negotiated server properties from a validated HelloAck.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub protocol_version: u16,
    pub max_frame_payload: u32,
    pub capabilities: u64,
    pub tables: Vec<TableIdentity>,
}

/// One result column in a remote query stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
}

impl From<WireResultColumn> for ResultColumn {
    fn from(column: WireResultColumn) -> Self {
        Self {
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
        }
    }
}

/// A valid server-side operation error. Receiving this response does not by
/// itself poison the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub code: ProtocolErrorCode,
    pub transaction_state: WireTransactionState,
    pub message: String,
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "NetbaDB server error {:?} (transaction state {:?}): {}",
            self.code, self.transaction_state, self.message
        )
    }
}

impl Error for ServerError {}

/// Failures from transport setup, protocol validation, server execution, or
/// local client lifecycle checks.
#[derive(Debug)]
pub enum ClientError {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    Protocol {
        message: String,
    },
    TlsConfig(TlsConfigError),
    TlsHandshake(TlsHandshakeError),
    PlaintextRemoteNotAllowed {
        peer: SocketAddr,
    },
    InvalidAddress {
        address: String,
        message: String,
    },
    RequestIdExhausted,
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
    ConnectionClosed,
    CapabilityMismatch {
        required: u64,
        actual: u64,
    },
    SchemaUnavailable {
        table_id: TableId,
    },
    SchemaMismatch {
        table_id: TableId,
        required: SchemaFingerprint,
        actual: SchemaFingerprint,
    },
    Server(ServerError),
    ClientState {
        reason: &'static str,
    },
    ExpectedQuery,
    ExpectedAffectedRows,
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { kind, message } => {
                write!(
                    formatter,
                    "NetbaDB transport I/O failed ({kind:?}): {message}"
                )
            }
            Self::Protocol { message } => write!(formatter, "NetbaDB protocol error: {message}"),
            Self::TlsConfig(error) => error.fmt(formatter),
            Self::TlsHandshake(error) => error.fmt(formatter),
            Self::PlaintextRemoteNotAllowed { peer } => {
                write!(formatter, "plaintext NetbaDB peer `{peer}` is not loopback")
            }
            Self::InvalidAddress { address, message } => {
                write!(formatter, "invalid NetbaDB address `{address}`: {message}")
            }
            Self::RequestIdExhausted => formatter.write_str("NetbaDB request IDs are exhausted"),
            Self::UnexpectedResponse { expected, actual } => write!(
                formatter,
                "unexpected NetbaDB response `{actual}`; expected {expected}"
            ),
            Self::ConnectionClosed => formatter.write_str("NetbaDB connection is closed"),
            Self::CapabilityMismatch { required, actual } => write!(
                formatter,
                "required capabilities {required:#x} are not available in {actual:#x}"
            ),
            Self::SchemaUnavailable { table_id } => {
                write!(formatter, "required table {} is not visible", table_id.0)
            }
            Self::SchemaMismatch { table_id, .. } => write!(
                formatter,
                "schema fingerprint mismatch for table {}",
                table_id.0
            ),
            Self::Server(error) => error.fmt(formatter),
            Self::ClientState { reason } => {
                write!(formatter, "NetbaDB client state error: {reason}")
            }
            Self::ExpectedQuery => formatter.write_str("statement returned an affected-row count"),
            Self::ExpectedAffectedRows => formatter.write_str("statement returned query rows"),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::TlsConfig(error) => Some(error),
            Self::TlsHandshake(error) => Some(error),
            Self::Server(error) => Some(error),
            _ => None,
        }
    }
}

impl From<TlsConfigError> for ClientError {
    fn from(error: TlsConfigError) -> Self {
        Self::TlsConfig(error)
    }
}

/// Connection and Hello-gate configuration. Plaintext is the default and is
/// accepted only when the connected peer address is loopback.
#[derive(Debug, Clone)]
pub struct Config {
    address: String,
    tls: Option<TlsConfig>,
    required_schemas: Vec<TableIdentity>,
    required_capabilities: u64,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl Config {
    #[must_use]
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            tls: None,
            required_schemas: Vec::new(),
            required_capabilities: 0,
            read_timeout: None,
            write_timeout: None,
        }
    }

    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    #[must_use]
    pub fn require_schema(mut self, identity: TableIdentity) -> Self {
        self.required_schemas.push(identity);
        self
    }

    #[must_use]
    pub fn require_capabilities(mut self, capabilities: u64) -> Self {
        self.required_capabilities |= capabilities;
        self
    }

    #[must_use]
    pub fn read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.read_timeout = timeout;
        self
    }

    #[must_use]
    pub fn write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.write_timeout = timeout;
        self
    }
}

/// One blocking Protocol v1 connection. Every operation requires exclusive
/// access and one request is completed before the next is sent.
pub struct Client {
    stream: Option<ConnectionStream>,
    next_request_id: u64,
    server_info: ServerInfo,
    rows_active: bool,
    transaction_active: bool,
    broken: bool,
    closed: bool,
}

impl Client {
    /// Connects, establishes the selected transport, performs Hello, and
    /// validates required capabilities and schemas before returning.
    pub fn connect(config: Config) -> Result<Self, ClientError> {
        let addresses = resolve_address(&config.address)?;
        let stream = TcpStream::connect(addresses.as_slice()).map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        stream
            .set_read_timeout(config.read_timeout)
            .map_err(io_error)?;
        stream
            .set_write_timeout(config.write_timeout)
            .map_err(io_error)?;

        let peer = stream.peer_addr().map_err(io_error)?;
        let stream = match config.tls {
            Some(tls) => tls.establish(stream).map_err(ClientError::TlsHandshake)?,
            None => {
                validate_plaintext_peer(peer)?;
                ConnectionStream::Plain(stream)
            }
        };

        let mut client = Self {
            stream: Some(stream),
            next_request_id: 1,
            server_info: ServerInfo {
                protocol_version: 0,
                max_frame_payload: 0,
                capabilities: 0,
                tables: Vec::new(),
            },
            rows_active: false,
            transaction_active: false,
            broken: false,
            closed: false,
        };
        if let Err(error) = client.handshake(config.required_capabilities, &config.required_schemas)
        {
            client.close_silent();
            return Err(error);
        }
        Ok(client)
    }

    #[must_use]
    pub const fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    /// Starts a streamed query. The returned value exclusively borrows this
    /// client until the stream reaches QueryEnd or is explicitly closed.
    pub fn query(&mut self, sql: &str) -> Result<Rows<'_>, ClientError> {
        let (request_id, columns) = self.start_query(sql, false)?;
        Ok(Rows::new(self, request_id, columns))
    }

    /// Executes SQL expected to return an affected-row count.
    pub fn exec(&mut self, sql: &str) -> Result<u64, ClientError> {
        self.exec_internal(sql, false)
    }

    pub fn ping(&mut self) -> Result<(), ClientError> {
        let message = self.round_trip(ClientMessage::Ping, true)?;
        match message {
            ServerMessage::Pong => Ok(()),
            ServerMessage::Error {
                code,
                transaction_state,
                message,
            } => Err(self.server_failure(code, transaction_state, message)),
            other => Err(self.unexpected("Pong", message_name(&other))),
        }
    }

    pub fn analyze(&mut self, table_id: TableId) -> Result<(), ClientError> {
        let message = self.round_trip(ClientMessage::Analyze { table_id }, false)?;
        match message {
            ServerMessage::AnalyzeAck => Ok(()),
            ServerMessage::Error {
                code,
                transaction_state,
                message,
            } => Err(self.server_failure(code, transaction_state, message)),
            other => Err(self.unexpected("AnalyzeAck", message_name(&other))),
        }
    }

    /// Starts one table-scoped transaction and lends this connection to it.
    pub fn begin(&mut self, table_id: TableId) -> Result<Transaction<'_>, ClientError> {
        let message = self.round_trip(ClientMessage::Begin { table_id }, false)?;
        match message {
            ServerMessage::TransactionStarted => {
                self.transaction_active = true;
                Ok(Transaction {
                    client: self,
                    terminal: false,
                })
            }
            ServerMessage::Error {
                code,
                transaction_state,
                message,
            } => Err(self.server_failure(code, transaction_state, message)),
            other => Err(self.unexpected("TransactionStarted", message_name(&other))),
        }
    }

    /// Closes the transport. This does not confirm rollback of a server-side
    /// transaction; use [`Transaction::rollback`] when confirmation matters.
    pub fn close(&mut self) -> Result<(), ClientError> {
        if self.closed && self.stream.is_none() {
            return Ok(());
        }
        self.closed = true;
        self.rows_active = false;
        self.transaction_active = false;
        match self.stream.take() {
            Some(mut stream) => stream.close().map_err(io_error),
            None => Ok(()),
        }
    }

    fn handshake(
        &mut self,
        required_capabilities: u64,
        required_schemas: &[TableIdentity],
    ) -> Result<(), ClientError> {
        let message = self.round_trip(ClientMessage::Hello, true)?;
        let ServerMessage::HelloAck {
            protocol_version,
            max_frame_payload,
            capabilities,
            tables,
        } = message
        else {
            if let ServerMessage::Error {
                code,
                transaction_state,
                message,
            } = message
            {
                return Err(self.server_failure(code, transaction_state, message));
            }
            return Err(self.unexpected("HelloAck", message_name(&message)));
        };
        if protocol_version != PROTOCOL_VERSION {
            return Err(self.protocol_failure("HelloAck inner protocol version is not 1"));
        }
        if max_frame_payload == 0 || max_frame_payload > MAX_FRAME_PAYLOAD {
            return Err(self.protocol_failure("HelloAck maximum frame payload is invalid"));
        }

        let mut seen = HashSet::with_capacity(tables.len());
        let mut identities = Vec::with_capacity(tables.len());
        for table in tables {
            if !seen.insert(table.table_id) {
                return Err(self.protocol_failure("HelloAck contains a duplicate table ID"));
            }
            identities.push(TableIdentity {
                table_id: table.table_id,
                fingerprint: SchemaFingerprint::from_bytes(table.fingerprint),
            });
        }
        if capabilities & required_capabilities != required_capabilities {
            return Err(ClientError::CapabilityMismatch {
                required: required_capabilities,
                actual: capabilities,
            });
        }
        for required in required_schemas {
            let Some(actual) = identities
                .iter()
                .find(|identity| identity.table_id == required.table_id)
            else {
                return Err(ClientError::SchemaUnavailable {
                    table_id: required.table_id,
                });
            };
            if actual.fingerprint != required.fingerprint {
                return Err(ClientError::SchemaMismatch {
                    table_id: required.table_id,
                    required: required.fingerprint,
                    actual: actual.fingerprint,
                });
            }
        }
        self.server_info = ServerInfo {
            protocol_version,
            max_frame_payload,
            capabilities,
            tables: identities,
        };
        Ok(())
    }

    fn start_query(
        &mut self,
        sql: &str,
        in_transaction: bool,
    ) -> Result<(u64, Vec<ResultColumn>), ClientError> {
        self.check_ready(in_transaction)?;
        let request_id = self.allocate_request_id()?;
        self.write_request(
            request_id,
            ClientMessage::Execute {
                sql: sql.to_owned(),
            },
        )?;
        match self.read_response(request_id)? {
            ServerMessage::QueryStart { columns } => Ok((
                request_id,
                columns.into_iter().map(ResultColumn::from).collect(),
            )),
            ServerMessage::AffectedRows { .. } => Err(ClientError::ExpectedQuery),
            ServerMessage::Error {
                code,
                transaction_state,
                message,
            } => Err(self.server_failure(code, transaction_state, message)),
            other => Err(self.unexpected("QueryStart or Error", message_name(&other))),
        }
    }

    fn exec_internal(&mut self, sql: &str, in_transaction: bool) -> Result<u64, ClientError> {
        self.check_ready(in_transaction)?;
        let request_id = self.allocate_request_id()?;
        self.write_request(
            request_id,
            ClientMessage::Execute {
                sql: sql.to_owned(),
            },
        )?;
        match self.read_response(request_id)? {
            ServerMessage::AffectedRows { count } => Ok(count),
            ServerMessage::QueryStart { columns } => {
                let columns = columns.into_iter().map(ResultColumn::from).collect();
                Rows::new(self, request_id, columns).close()?;
                Err(ClientError::ExpectedAffectedRows)
            }
            ServerMessage::Error {
                code,
                transaction_state,
                message,
            } => Err(self.server_failure(code, transaction_state, message)),
            other => Err(self.unexpected("AffectedRows or QueryStart", message_name(&other))),
        }
    }

    fn round_trip(
        &mut self,
        request: ClientMessage,
        allow_transaction: bool,
    ) -> Result<ServerMessage, ClientError> {
        self.check_ready(allow_transaction)?;
        let request_id = self.allocate_request_id()?;
        self.write_request(request_id, request)?;
        self.read_response(request_id)
    }

    fn check_ready(&self, allow_transaction: bool) -> Result<(), ClientError> {
        if self.broken || self.closed || self.stream.is_none() {
            return Err(ClientError::ConnectionClosed);
        }
        if self.rows_active {
            return Err(ClientError::ClientState {
                reason: "a query response is still open",
            });
        }
        if self.transaction_active && !allow_transaction {
            return Err(ClientError::ClientState {
                reason: "an explicit transaction owns the connection",
            });
        }
        Ok(())
    }

    fn allocate_request_id(&mut self) -> Result<u64, ClientError> {
        if self.next_request_id == 0 || self.next_request_id == u64::MAX {
            return Err(ClientError::RequestIdExhausted);
        }
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        Ok(request_id)
    }

    fn write_request(
        &mut self,
        request_id: u64,
        message: ClientMessage,
    ) -> Result<(), ClientError> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(ClientError::ConnectionClosed);
        };
        match write_client_frame(
            stream,
            &Frame {
                request_id,
                message,
            },
        ) {
            Ok(()) => Ok(()),
            Err(ProtocolError::Io(error)) => Err(self.io_failure(error)),
            Err(error) => Err(ClientError::Protocol {
                message: error.to_string(),
            }),
        }
    }

    fn read_response(&mut self, request_id: u64) -> Result<ServerMessage, ClientError> {
        let Some(stream) = self.stream.as_mut() else {
            return Err(ClientError::ConnectionClosed);
        };
        let frame = match read_server_frame(stream) {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Err(self.protocol_failure("server closed connection during request"));
            }
            Err(ProtocolError::Io(error)) => return Err(self.io_failure(error)),
            Err(error) => return Err(self.protocol_failure_owned(error.to_string())),
        };
        if frame.request_id != request_id {
            return Err(self.protocol_failure("response request ID does not match request"));
        }
        Ok(frame.message)
    }

    fn server_failure(
        &mut self,
        code: ProtocolErrorCode,
        transaction_state: WireTransactionState,
        message: String,
    ) -> ClientError {
        if self.transaction_active && transaction_state == WireTransactionState::None {
            self.transaction_active = false;
        }
        ClientError::Server(ServerError {
            code,
            transaction_state,
            message,
        })
    }

    fn unexpected(&mut self, expected: &'static str, actual: &'static str) -> ClientError {
        self.poison(ClientError::UnexpectedResponse { expected, actual })
    }

    fn protocol_failure(&mut self, message: &'static str) -> ClientError {
        self.protocol_failure_owned(message.to_owned())
    }

    fn protocol_failure_owned(&mut self, message: String) -> ClientError {
        self.poison(ClientError::Protocol { message })
    }

    fn io_failure(&mut self, error: io::Error) -> ClientError {
        self.poison(io_error(error))
    }

    fn poison(&mut self, error: ClientError) -> ClientError {
        self.broken = true;
        self.closed = true;
        self.rows_active = false;
        self.transaction_active = false;
        self.close_silent();
        error
    }

    fn close_silent(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            let _ = stream.close();
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.closed = true;
        self.rows_active = false;
        self.transaction_active = false;
        self.close_silent();
    }
}

/// Streaming rows for exactly one query response.
pub struct Rows<'a> {
    client: &'a mut Client,
    request_id: u64,
    columns: Vec<ResultColumn>,
    row_count: u64,
    finished: bool,
    failure: Option<RowsFailure>,
}

impl Rows<'_> {
    fn new(client: &mut Client, request_id: u64, columns: Vec<ResultColumn>) -> Rows<'_> {
        client.rows_active = true;
        Rows {
            client,
            request_id,
            columns,
            row_count: 0,
            finished: false,
            failure: None,
        }
    }

    #[must_use]
    pub fn columns(&self) -> &[ResultColumn] {
        &self.columns
    }

    /// Reads and validates one row, or validates QueryEnd and returns `None`.
    pub fn next_row(&mut self) -> Result<Option<Vec<ScalarValue>>, ClientError> {
        if self.finished {
            return match &self.failure {
                Some(failure) => Err(failure.to_error()),
                None => Ok(None),
            };
        }
        let message = match self.client.read_response(self.request_id) {
            Ok(message) => message,
            Err(error) => return Err(self.fail(error)),
        };
        match message {
            ServerMessage::QueryRow { values } => {
                if values.len() != self.columns.len() {
                    return Err(self
                        .protocol_fail("QueryRow value count does not match QueryStart columns"));
                }
                for (value, column) in values.iter().zip(&self.columns) {
                    match value.physical_type() {
                        None if !column.nullable => {
                            return Err(self.protocol_fail(
                                "QueryRow contains NULL for a non-nullable column",
                            ));
                        }
                        Some(actual) if actual != column.data_type.physical => {
                            return Err(self.protocol_fail(
                                "QueryRow value physical type does not match column",
                            ));
                        }
                        _ => {}
                    }
                }
                let Some(row_count) = self.row_count.checked_add(1) else {
                    return Err(self.protocol_fail("streamed query row count overflowed"));
                };
                self.row_count = row_count;
                Ok(Some(values))
            }
            ServerMessage::QueryEnd { row_count } => {
                if row_count != self.row_count {
                    return Err(
                        self.protocol_fail("QueryEnd row count does not match streamed rows")
                    );
                }
                self.finished = true;
                self.client.rows_active = false;
                Ok(None)
            }
            other => {
                let error = self
                    .client
                    .unexpected("QueryRow or QueryEnd", message_name(&other));
                Err(self.fail(error))
            }
        }
    }

    /// Drains the remaining response through QueryEnd so the connection can
    /// safely process another request.
    pub fn close(mut self) -> Result<(), ClientError> {
        if self.finished {
            return match &self.failure {
                Some(failure) => Err(failure.to_error()),
                None => Ok(()),
            };
        }
        while self.next_row()?.is_some() {}
        Ok(())
    }

    fn protocol_fail(&mut self, message: &'static str) -> ClientError {
        let error = self.client.protocol_failure(message);
        self.fail(error)
    }

    fn fail(&mut self, error: ClientError) -> ClientError {
        let failure = RowsFailure::from_error(&error);
        self.failure = Some(failure);
        self.finished = true;
        error
    }
}

impl Drop for Rows<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.client.broken = true;
            self.client.closed = true;
            self.client.rows_active = false;
            self.client.transaction_active = false;
            self.client.close_silent();
            self.finished = true;
        }
    }
}

/// Exclusive handle to one table-scoped server transaction.
pub struct Transaction<'a> {
    client: &'a mut Client,
    terminal: bool,
}

impl Transaction<'_> {
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.terminal || !self.client.transaction_active
    }

    pub fn query(&mut self, sql: &str) -> Result<Rows<'_>, ClientError> {
        self.ensure_active()?;
        match self.client.start_query(sql, true) {
            Ok((request_id, columns)) => Ok(Rows::new(self.client, request_id, columns)),
            Err(error) => {
                if !self.client.transaction_active {
                    self.terminal = true;
                }
                Err(error)
            }
        }
    }

    pub fn exec(&mut self, sql: &str) -> Result<u64, ClientError> {
        self.ensure_active()?;
        let result = self.client.exec_internal(sql, true);
        self.sync_terminal();
        result
    }

    pub fn ping(&mut self) -> Result<(), ClientError> {
        self.ensure_active()?;
        let result = self.client.ping();
        self.sync_terminal();
        result
    }

    pub fn commit(&mut self) -> Result<(), ClientError> {
        self.finish(ClientMessage::Commit, "TransactionCommitted")
    }

    pub fn rollback(&mut self) -> Result<(), ClientError> {
        self.finish(ClientMessage::Rollback, "TransactionRolledBack")
    }

    fn finish(
        &mut self,
        request: ClientMessage,
        expected: &'static str,
    ) -> Result<(), ClientError> {
        self.ensure_active()?;
        let response = self.client.round_trip(request, true);
        let result = match response {
            Ok(ServerMessage::TransactionCommitted) if expected == "TransactionCommitted" => {
                self.client.transaction_active = false;
                Ok(())
            }
            Ok(ServerMessage::TransactionRolledBack) if expected == "TransactionRolledBack" => {
                self.client.transaction_active = false;
                Ok(())
            }
            Ok(ServerMessage::Error {
                code,
                transaction_state,
                message,
            }) => Err(self.client.server_failure(code, transaction_state, message)),
            Ok(other) => Err(self.client.unexpected(expected, message_name(&other))),
            Err(error) => Err(error),
        };
        self.sync_terminal();
        result
    }

    fn sync_terminal(&mut self) {
        if !self.client.transaction_active {
            self.terminal = true;
        }
    }

    fn ensure_active(&mut self) -> Result<(), ClientError> {
        if self.terminal || !self.client.transaction_active {
            self.terminal = true;
            return Err(ClientError::ClientState {
                reason: "transaction is terminal",
            });
        }
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.terminal && self.client.transaction_active {
            self.client.broken = true;
            self.client.closed = true;
            self.client.rows_active = false;
            self.client.transaction_active = false;
            self.client.close_silent();
            self.terminal = true;
        }
    }
}

#[derive(Debug, Clone)]
enum RowsFailure {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    Protocol(String),
    UnexpectedResponse {
        expected: &'static str,
        actual: &'static str,
    },
    ConnectionClosed,
    Other(String),
}

impl RowsFailure {
    fn from_error(error: &ClientError) -> Self {
        match error {
            ClientError::Io { kind, message } => Self::Io {
                kind: *kind,
                message: message.clone(),
            },
            ClientError::Protocol { message } => Self::Protocol(message.clone()),
            ClientError::UnexpectedResponse { expected, actual } => {
                Self::UnexpectedResponse { expected, actual }
            }
            ClientError::ConnectionClosed => Self::ConnectionClosed,
            other => Self::Other(other.to_string()),
        }
    }

    fn to_error(&self) -> ClientError {
        match self {
            Self::Io { kind, message } => ClientError::Io {
                kind: *kind,
                message: message.clone(),
            },
            Self::Protocol(message) => ClientError::Protocol {
                message: message.clone(),
            },
            Self::UnexpectedResponse { expected, actual } => {
                ClientError::UnexpectedResponse { expected, actual }
            }
            Self::ConnectionClosed => ClientError::ConnectionClosed,
            Self::Other(message) => ClientError::Protocol {
                message: message.clone(),
            },
        }
    }
}

fn resolve_address(address: &str) -> Result<Vec<SocketAddr>, ClientError> {
    if address.is_empty() {
        return Err(ClientError::InvalidAddress {
            address: address.to_owned(),
            message: "address is empty".into(),
        });
    }
    let addresses = address
        .to_socket_addrs()
        .map_err(|error| ClientError::InvalidAddress {
            address: address.to_owned(),
            message: error.to_string(),
        })?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(ClientError::InvalidAddress {
            address: address.to_owned(),
            message: "address resolved to no endpoints".into(),
        });
    }
    Ok(addresses)
}

fn validate_plaintext_peer(peer: SocketAddr) -> Result<(), ClientError> {
    validate_plaintext_ip(peer.ip()).map_err(|()| ClientError::PlaintextRemoteNotAllowed { peer })
}

fn validate_plaintext_ip(ip: IpAddr) -> Result<(), ()> {
    if ip.is_loopback() { Ok(()) } else { Err(()) }
}

fn io_error(error: io::Error) -> ClientError {
    ClientError::Io {
        kind: error.kind(),
        message: error.to_string(),
    }
}

const fn message_name(message: &ServerMessage) -> &'static str {
    match message {
        ServerMessage::HelloAck { .. } => "HelloAck",
        ServerMessage::QueryStart { .. } => "QueryStart",
        ServerMessage::QueryRow { .. } => "QueryRow",
        ServerMessage::QueryEnd { .. } => "QueryEnd",
        ServerMessage::AffectedRows { .. } => "AffectedRows",
        ServerMessage::TransactionStarted => "TransactionStarted",
        ServerMessage::TransactionCommitted => "TransactionCommitted",
        ServerMessage::TransactionRolledBack => "TransactionRolledBack",
        ServerMessage::AnalyzeAck => "AnalyzeAck",
        ServerMessage::Pong => "Pong",
        ServerMessage::Error { .. } => "Error",
    }
}

#[cfg(test)]
mod tests;
