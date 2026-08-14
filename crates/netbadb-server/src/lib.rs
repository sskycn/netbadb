//! Transport-neutral synchronous NetbaDB protocol sessions.

mod limits;
mod manifest;
mod metrics;
mod runtime;
mod tls;

use std::error::Error;
use std::fmt;

use netbadb_core::{
    Database, DatabaseError, ExecutionResult, QueryResult, Transaction, TransactionState,
};
use netbadb_protocol::{
    ClientMessage, MAX_ERROR_MESSAGE_BYTES, MAX_FRAME_PAYLOAD, PROTOCOL_VERSION, ProtocolError,
    ProtocolErrorCode, SERVER_CAPABILITIES, ServerMessage, TableSchemaIdentity, WireResultColumn,
    WireTransactionState, validate_server_message,
};

pub use limits::{
    DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_RESULT_ROWS, DEFAULT_WRITE_TIMEOUT,
    MAX_CONFIGURED_CONNECTIONS, MAX_CONFIGURED_RESULT_ROWS, MAX_SOCKET_TIMEOUT, ServerLimits,
    ServerLimitsError, SessionPolicy,
};
pub use manifest::{ManifestError, ServerConfig, TableBootstrap};
pub use metrics::{ServerMetricsHandle, ServerMetricsSnapshot};
pub use runtime::{ServerHandle, SessionId, TcpServer, TcpServerError, WorkerFatalError};
pub use tls::{AuthenticatedClientIdentity, ClientIdentity, TlsConfigError, TransportKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseBatch {
    pub request_id: u64,
    pub messages: Vec<ServerMessage>,
}

#[derive(Debug)]
pub(crate) struct SessionResponse {
    pub(crate) batch: ResponseBatch,
    pub(crate) result_row_limit_exceeded: bool,
}

impl SessionResponse {
    fn standard(batch: ResponseBatch) -> Self {
        Self {
            batch,
            result_row_limit_exceeded: false,
        }
    }
}

#[derive(Debug)]
pub enum ServerError {
    Database(DatabaseError),
    Protocol(ProtocolError),
    InternalResultMismatch {
        row: usize,
        column: Option<usize>,
        reason: &'static str,
    },
    ResponseTooLarge,
    ResultRowLimitExceeded {
        rows: usize,
        limit: usize,
    },
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::Protocol(error) => error.fmt(formatter),
            Self::InternalResultMismatch {
                row,
                column,
                reason,
            } => match column {
                Some(column) => write!(
                    formatter,
                    "query result row {row}, column {column} is invalid: {reason}"
                ),
                None => write!(formatter, "query result row {row} is invalid: {reason}"),
            },
            Self::ResponseTooLarge => {
                formatter.write_str("one response message exceeds the protocol frame limit")
            }
            Self::ResultRowLimitExceeded { rows, limit } => write!(
                formatter,
                "query result has {rows} rows and exceeds the configured server row limit {limit}"
            ),
        }
    }
}

impl Error for ServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::InternalResultMismatch { .. }
            | Self::ResponseTooLarge
            | Self::ResultRowLimitExceeded { .. } => None,
        }
    }
}

impl From<DatabaseError> for ServerError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl From<ProtocolError> for ServerError {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::FrameTooLarge(_) | ProtocolError::LengthOverflow => {
                Self::ResponseTooLarge
            }
            other => Self::Protocol(other),
        }
    }
}

#[derive(Debug, Default)]
pub struct SessionState {
    handshaken: bool,
    transaction: Option<Transaction>,
    policy: SessionPolicy,
}

impl SessionState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_policy(policy: SessionPolicy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn is_handshaken(&self) -> bool {
        self.handshaken
    }

    #[must_use]
    pub fn transaction_state(&self) -> WireTransactionState {
        self.transaction
            .as_ref()
            .map_or(WireTransactionState::None, |transaction| {
                wire_transaction_state(transaction.state())
            })
    }

    /// Processes exactly one request. Every response message in the batch uses
    /// the supplied request ID when framed by the transport layer.
    pub fn handle(
        &mut self,
        database: &mut Database,
        request_id: u64,
        request: ClientMessage,
    ) -> ResponseBatch {
        self.handle_with_metadata(database, request_id, request)
            .batch
    }

    pub(crate) fn handle_with_metadata(
        &mut self,
        database: &mut Database,
        request_id: u64,
        request: ClientMessage,
    ) -> SessionResponse {
        if request_id == 0 {
            return SessionResponse::standard(self.error_batch(
                request_id,
                ProtocolErrorCode::Protocol,
                "request ID zero is reserved",
            ));
        }
        if !self.handshaken {
            return match request {
                ClientMessage::Hello => match build_hello_ack(database) {
                    Ok(message) => {
                        if let Err(error) = validate_server_message(&message) {
                            self.classified_server_error_batch(request_id, error.into())
                        } else {
                            self.handshaken = true;
                            SessionResponse::standard(ResponseBatch {
                                request_id,
                                messages: vec![message],
                            })
                        }
                    }
                    Err(error) => self.classified_server_error_batch(request_id, error),
                },
                _ => SessionResponse::standard(self.error_batch(
                    request_id,
                    ProtocolErrorCode::HandshakeRequired,
                    "Hello must be the first request in a session",
                )),
            };
        }

        let result = match request {
            ClientMessage::Hello => Err(SessionFailure::fixed(
                ProtocolErrorCode::AlreadyHandshaken,
                "session handshake is already complete",
            )),
            ClientMessage::Ping => Ok(vec![ServerMessage::Pong]),
            ClientMessage::Execute { sql } => self.execute(database, &sql),
            ClientMessage::Begin { table_id } => self.begin(database, table_id),
            ClientMessage::Commit => self.commit(),
            ClientMessage::Rollback => self.rollback(),
            ClientMessage::Analyze { table_id } => {
                if self.transaction.is_some() {
                    Err(SessionFailure::fixed(
                        ProtocolErrorCode::OperationNotAllowedInTransaction,
                        "ANALYZE is not allowed while the session owns a transaction",
                    ))
                } else {
                    database
                        .analyze(table_id)
                        .map(|()| vec![ServerMessage::AnalyzeAck])
                        .map_err(SessionFailure::Database)
                }
            }
        };

        match result {
            Ok(messages) => SessionResponse::standard(self.success_batch(request_id, messages)),
            Err(SessionFailure::Fixed { code, message }) => {
                SessionResponse::standard(self.error_batch(request_id, code, message))
            }
            Err(SessionFailure::Database(error)) => {
                SessionResponse::standard(self.database_error_batch(request_id, error))
            }
            Err(SessionFailure::Server(error)) => {
                self.classified_server_error_batch(request_id, error)
            }
        }
    }

    /// Resolves a session-owned transaction before a transport disconnect.
    /// A failed rollback leaves the transaction available for retry.
    pub fn close(&mut self) -> Result<(), ServerError> {
        let Some(transaction) = self.transaction.as_mut() else {
            return Ok(());
        };
        if matches!(
            transaction.state(),
            TransactionState::Committed | TransactionState::RolledBack
        ) {
            self.transaction = None;
            return Ok(());
        }
        transaction
            .rollback()
            .map_err(|error| ServerError::Database(DatabaseError::from(error)))?;
        self.transaction = None;
        Ok(())
    }

    fn execute(
        &mut self,
        database: &mut Database,
        sql: &str,
    ) -> Result<Vec<ServerMessage>, SessionFailure> {
        let result = match self.transaction.as_mut() {
            Some(transaction) => database.execute_in(transaction, sql),
            None => database.execute(sql),
        };
        if self.transaction.as_ref().is_some_and(|transaction| {
            matches!(
                transaction.state(),
                TransactionState::Committed | TransactionState::RolledBack
            )
        }) {
            self.transaction = None;
        }
        let result = result.map_err(SessionFailure::Database)?;
        match result {
            ExecutionResult::Query(query) => {
                build_query_messages(query, self.policy.max_result_rows())
                    .map_err(SessionFailure::Server)
            }
            ExecutionResult::AffectedRows(count) => Ok(vec![ServerMessage::AffectedRows { count }]),
        }
    }

    fn begin(
        &mut self,
        database: &mut Database,
        table_id: netbadb_types::TableId,
    ) -> Result<Vec<ServerMessage>, SessionFailure> {
        if self.transaction.is_some() {
            return Err(SessionFailure::fixed(
                ProtocolErrorCode::TransactionAlreadyActive,
                "session already owns a transaction",
            ));
        }
        let transaction = database
            .begin_transaction_for(table_id)
            .map_err(SessionFailure::Database)?;
        self.transaction = Some(transaction);
        Ok(vec![ServerMessage::TransactionStarted])
    }

    fn commit(&mut self) -> Result<Vec<ServerMessage>, SessionFailure> {
        let Some(transaction) = self.transaction.as_mut() else {
            return Err(SessionFailure::fixed(
                ProtocolErrorCode::NoActiveTransaction,
                "session has no active transaction to commit",
            ));
        };
        transaction
            .commit()
            .map_err(|error| SessionFailure::Database(DatabaseError::from(error)))?;
        self.transaction = None;
        Ok(vec![ServerMessage::TransactionCommitted])
    }

    fn rollback(&mut self) -> Result<Vec<ServerMessage>, SessionFailure> {
        let Some(transaction) = self.transaction.as_mut() else {
            return Err(SessionFailure::fixed(
                ProtocolErrorCode::NoActiveTransaction,
                "session has no active transaction to roll back",
            ));
        };
        transaction
            .rollback()
            .map_err(|error| SessionFailure::Database(DatabaseError::from(error)))?;
        self.transaction = None;
        Ok(vec![ServerMessage::TransactionRolledBack])
    }

    fn success_batch(&self, request_id: u64, messages: Vec<ServerMessage>) -> ResponseBatch {
        match validate_messages(&messages) {
            Ok(()) => ResponseBatch {
                request_id,
                messages,
            },
            Err(error) => self.server_error_batch(request_id, error),
        }
    }

    fn server_error_batch(&self, request_id: u64, error: ServerError) -> ResponseBatch {
        let code = match &error {
            ServerError::ResponseTooLarge | ServerError::ResultRowLimitExceeded { .. } => {
                ProtocolErrorCode::ResponseTooLarge
            }
            ServerError::InternalResultMismatch { .. } => ProtocolErrorCode::InternalResultMismatch,
            ServerError::Protocol(_) => ProtocolErrorCode::Protocol,
            ServerError::Database(database) => database_error_code(database),
        };
        self.error_batch(request_id, code, &error.to_string())
    }

    fn classified_server_error_batch(
        &self,
        request_id: u64,
        error: ServerError,
    ) -> SessionResponse {
        let result_row_limit_exceeded =
            matches!(&error, ServerError::ResultRowLimitExceeded { .. });
        SessionResponse {
            batch: self.server_error_batch(request_id, error),
            result_row_limit_exceeded,
        }
    }

    fn database_error_batch(&self, request_id: u64, error: DatabaseError) -> ResponseBatch {
        let code = database_error_code(&error);
        self.error_batch(request_id, code, &error.to_string())
    }

    fn error_batch(
        &self,
        request_id: u64,
        code: ProtocolErrorCode,
        message: &str,
    ) -> ResponseBatch {
        ResponseBatch {
            request_id,
            messages: vec![ServerMessage::Error {
                code,
                transaction_state: self.transaction_state(),
                message: bounded_error_message(message),
            }],
        }
    }
}

fn bounded_error_message(message: &str) -> String {
    const TRUNCATED_SUFFIX: &str = "... [truncated]";

    if message.len() <= MAX_ERROR_MESSAGE_BYTES {
        return message.to_owned();
    }
    let mut prefix_end = MAX_ERROR_MESSAGE_BYTES - TRUNCATED_SUFFIX.len();
    while !message.is_char_boundary(prefix_end) {
        prefix_end -= 1;
    }
    let mut bounded = String::with_capacity(MAX_ERROR_MESSAGE_BYTES);
    bounded.push_str(&message[..prefix_end]);
    bounded.push_str(TRUNCATED_SUFFIX);
    bounded
}

enum SessionFailure {
    Fixed {
        code: ProtocolErrorCode,
        message: &'static str,
    },
    Database(DatabaseError),
    Server(ServerError),
}

impl SessionFailure {
    fn fixed(code: ProtocolErrorCode, message: &'static str) -> Self {
        Self::Fixed { code, message }
    }
}

fn build_hello_ack(database: &Database) -> Result<ServerMessage, ServerError> {
    let mut tables = Vec::with_capacity(database.schema().tables().len());
    for table in database.schema().tables() {
        let fingerprint = table.fingerprint().map_err(DatabaseError::from)?;
        tables.push(TableSchemaIdentity {
            table_id: table.id,
            fingerprint: *fingerprint.as_bytes(),
        });
    }
    Ok(ServerMessage::HelloAck {
        protocol_version: PROTOCOL_VERSION,
        max_frame_payload: MAX_FRAME_PAYLOAD,
        capabilities: SERVER_CAPABILITIES,
        tables,
    })
}

fn build_query_messages(
    query: QueryResult,
    max_result_rows: usize,
) -> Result<Vec<ServerMessage>, ServerError> {
    if query.rows.len() > max_result_rows {
        return Err(ServerError::ResultRowLimitExceeded {
            rows: query.rows.len(),
            limit: max_result_rows,
        });
    }
    let columns = query
        .columns
        .iter()
        .map(|column| WireResultColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        })
        .collect::<Vec<_>>();
    let row_count = u64::try_from(query.rows.len()).map_err(|_| ServerError::ResponseTooLarge)?;
    let mut messages = Vec::with_capacity(query.rows.len().saturating_add(2));
    messages.push(ServerMessage::QueryStart { columns });
    for (row_index, row) in query.rows.into_iter().enumerate() {
        if row.len() != query.columns.len() {
            return Err(ServerError::InternalResultMismatch {
                row: row_index,
                column: None,
                reason: "value count does not match result column count",
            });
        }
        for (column_index, (value, column)) in row.iter().zip(&query.columns).enumerate() {
            match value.physical_type() {
                None if !column.nullable => {
                    return Err(ServerError::InternalResultMismatch {
                        row: row_index,
                        column: Some(column_index),
                        reason: "NULL appeared in a non-nullable result column",
                    });
                }
                Some(actual) if actual != column.data_type.physical => {
                    return Err(ServerError::InternalResultMismatch {
                        row: row_index,
                        column: Some(column_index),
                        reason: "runtime value has the wrong physical type",
                    });
                }
                None | Some(_) => {}
            }
        }
        messages.push(ServerMessage::QueryRow { values: row });
    }
    messages.push(ServerMessage::QueryEnd { row_count });
    validate_messages(&messages)?;
    Ok(messages)
}

fn validate_messages(messages: &[ServerMessage]) -> Result<(), ServerError> {
    for message in messages {
        validate_server_message(message)?;
    }
    Ok(())
}

fn database_error_code(error: &DatabaseError) -> ProtocolErrorCode {
    match error {
        DatabaseError::Compile(_) => ProtocolErrorCode::Compile,
        DatabaseError::Schema(_) => ProtocolErrorCode::Schema,
        DatabaseError::Storage(_) => ProtocolErrorCode::Storage,
        DatabaseError::Execution(_) => ProtocolErrorCode::Execution,
        DatabaseError::ExpectedQuery
        | DatabaseError::EmptyCatalog
        | DatabaseError::TableSelectionRequired
        | DatabaseError::DuplicateStoragePath(_)
        | DatabaseError::CreateTablesRollback { .. } => ProtocolErrorCode::Database,
    }
}

fn wire_transaction_state(state: TransactionState) -> WireTransactionState {
    match state {
        TransactionState::Active => WireTransactionState::Active,
        TransactionState::RollbackRequired => WireTransactionState::RollbackRequired,
        TransactionState::CommitPending => WireTransactionState::CommitPending,
        TransactionState::RollbackPending => WireTransactionState::RollbackPending,
        TransactionState::Committed | TransactionState::RolledBack => WireTransactionState::None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use netbadb_core::{QueryResult, ResultColumn};
    use netbadb_protocol::{
        CAPABILITY_ANALYZE, CAPABILITY_EXPLICIT_TRANSACTIONS, CAPABILITY_STREAMED_QUERY_RESULTS,
        PhysicalType, ScalarValue, SemanticType, TableId,
    };
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_storage::{wal_alternate_path, wal_path};
    use netbadb_types::ColumnId;

    use super::*;

    fn users_table() -> TableDef {
        TableDef::new(
            TableId(1),
            "users",
            vec![
                ColumnDef::new(ColumnId(1), "id", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "name", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
            ],
        )
    }

    fn teams_table() -> TableDef {
        TableDef::new(
            TableId(2),
            "teams",
            vec![ColumnDef::new(
                ColumnId(1),
                "id",
                TypeSpec::Physical(PhysicalType::UInt64),
            )],
        )
    }

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("netbadb-server-{name}-{}", std::process::id()))
    }

    fn cleanup(path: &Path) {
        let wal = wal_path(path);
        let _ = std::fs::remove_file(wal_alternate_path(&wal));
        let _ = std::fs::remove_file(wal);
        let _ = std::fs::remove_file(path);
    }

    fn hello(session: &mut SessionState, database: &mut Database) -> ResponseBatch {
        session.handle(database, 1, ClientMessage::Hello)
    }

    fn assert_error(batch: &ResponseBatch, code: ProtocolErrorCode, state: WireTransactionState) {
        assert!(matches!(
            batch.messages.as_slice(),
            [ServerMessage::Error { code: actual, transaction_state, .. }]
                if *actual == code && *transaction_state == state
        ));
    }

    #[test]
    fn handshake_preserves_schema_order_fingerprints_capabilities_and_sequence_rules() {
        let users_path = test_path("handshake-users");
        let teams_path = test_path("handshake-teams");
        cleanup(&users_path);
        cleanup(&teams_path);
        let users = users_table();
        let teams = teams_table();
        let mut database = Database::create_tables(vec![
            (users_path.clone(), users.clone()),
            (teams_path.clone(), teams.clone()),
        ])
        .unwrap();
        let mut session = SessionState::new();

        let before = session.handle(&mut database, 1, ClientMessage::Ping);
        assert_error(
            &before,
            ProtocolErrorCode::HandshakeRequired,
            WireTransactionState::None,
        );
        let batch = hello(&mut session, &mut database);
        assert!(matches!(
            batch.messages.as_slice(),
            [ServerMessage::HelloAck {
                protocol_version: PROTOCOL_VERSION,
                max_frame_payload: MAX_FRAME_PAYLOAD,
                capabilities,
                tables,
            }] if *capabilities == CAPABILITY_EXPLICIT_TRANSACTIONS
                | CAPABILITY_ANALYZE
                | CAPABILITY_STREAMED_QUERY_RESULTS
                && tables == &vec![
                    TableSchemaIdentity {
                        table_id: users.id,
                        fingerprint: *users.fingerprint().unwrap().as_bytes(),
                    },
                    TableSchemaIdentity {
                        table_id: teams.id,
                        fingerprint: *teams.fingerprint().unwrap().as_bytes(),
                    },
                ]
        ));
        assert_eq!(
            session
                .handle(&mut database, 2, ClientMessage::Ping)
                .messages,
            vec![ServerMessage::Pong]
        );
        let duplicate = session.handle(&mut database, 3, ClientMessage::Hello);
        assert_error(
            &duplicate,
            ProtocolErrorCode::AlreadyHandshaken,
            WireTransactionState::None,
        );

        database.close().unwrap();
        cleanup(&users_path);
        cleanup(&teams_path);
    }

    #[test]
    fn query_and_dml_responses_form_ordered_per_request_batches() {
        let path = test_path("query-stream");
        cleanup(&path);
        let mut database = Database::create(&path, users_table()).unwrap();
        let mut session = SessionState::new();
        hello(&mut session, &mut database);
        for (request_id, sql) in [
            (2, "INSERT INTO users (id, name) VALUES (2, 'B')"),
            (3, "INSERT INTO users (id, name) VALUES (1, 'A')"),
            (4, "INSERT INTO users (id, name) VALUES (3, NULL)"),
        ] {
            assert_eq!(
                session
                    .handle(
                        &mut database,
                        request_id,
                        ClientMessage::Execute { sql: sql.into() }
                    )
                    .messages,
                vec![ServerMessage::AffectedRows { count: 1 }]
            );
        }
        let query = session.handle(
            &mut database,
            5,
            ClientMessage::Execute {
                sql: "SELECT id, name FROM users ORDER BY id".into(),
            },
        );
        assert_eq!(query.request_id, 5);
        assert!(
            matches!(query.messages.first(), Some(ServerMessage::QueryStart { columns }) if columns.len() == 2)
        );
        assert_eq!(
            &query.messages[1..4],
            &[
                ServerMessage::QueryRow {
                    values: vec![ScalarValue::Int64(1), ScalarValue::Text("A".into())],
                },
                ServerMessage::QueryRow {
                    values: vec![ScalarValue::Int64(2), ScalarValue::Text("B".into())],
                },
                ServerMessage::QueryRow {
                    values: vec![ScalarValue::Int64(3), ScalarValue::Null],
                },
            ]
        );
        assert_eq!(
            query.messages.last(),
            Some(&ServerMessage::QueryEnd { row_count: 3 })
        );

        let empty = session.handle(
            &mut database,
            6,
            ClientMessage::Execute {
                sql: "SELECT id FROM users WHERE id = 99".into(),
            },
        );
        assert!(matches!(
            empty.messages.as_slice(),
            [
                ServerMessage::QueryStart { .. },
                ServerMessage::QueryEnd { row_count: 0 }
            ]
        ));

        assert_eq!(
            session
                .handle(
                    &mut database,
                    7,
                    ClientMessage::Execute {
                        sql: "UPDATE users SET name = 'updated' WHERE id = 1".into(),
                    },
                )
                .messages,
            vec![ServerMessage::AffectedRows { count: 1 }]
        );
        assert_eq!(
            session
                .handle(
                    &mut database,
                    8,
                    ClientMessage::Execute {
                        sql: "DELETE FROM users WHERE id = 2".into(),
                    },
                )
                .messages,
            vec![ServerMessage::AffectedRows { count: 1 }]
        );
        let after_dml = session.handle(
            &mut database,
            9,
            ClientMessage::Execute {
                sql: "SELECT id, name FROM users ORDER BY id".into(),
            },
        );
        assert_eq!(
            &after_dml.messages[1..3],
            &[
                ServerMessage::QueryRow {
                    values: vec![ScalarValue::Int64(1), ScalarValue::Text("updated".into()),],
                },
                ServerMessage::QueryRow {
                    values: vec![ScalarValue::Int64(3), ScalarValue::Null],
                },
            ]
        );
        assert_eq!(
            after_dml.messages.last(),
            Some(&ServerMessage::QueryEnd { row_count: 2 })
        );

        database.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn explicit_transactions_support_read_your_writes_rollback_commit_and_reuse() {
        let path = test_path("transactions");
        cleanup(&path);
        let mut database = Database::create(&path, users_table()).unwrap();
        let mut session = SessionState::new();
        hello(&mut session, &mut database);

        assert_eq!(
            session
                .handle(
                    &mut database,
                    2,
                    ClientMessage::Begin {
                        table_id: TableId(1)
                    }
                )
                .messages,
            vec![ServerMessage::TransactionStarted]
        );
        assert_eq!(session.transaction_state(), WireTransactionState::Active);
        session.handle(
            &mut database,
            3,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (1, 'temporary')".into(),
            },
        );
        let own = session.handle(
            &mut database,
            4,
            ClientMessage::Execute {
                sql: "SELECT id FROM users WHERE id = 1".into(),
            },
        );
        assert!(
            own.messages
                .iter()
                .any(|message| matches!(message, ServerMessage::QueryRow { .. }))
        );
        assert_eq!(
            session
                .handle(&mut database, 5, ClientMessage::Rollback)
                .messages,
            vec![ServerMessage::TransactionRolledBack]
        );
        let absent = session.handle(
            &mut database,
            6,
            ClientMessage::Execute {
                sql: "SELECT id FROM users WHERE id = 1".into(),
            },
        );
        assert!(matches!(
            absent.messages.as_slice(),
            [
                ServerMessage::QueryStart { .. },
                ServerMessage::QueryEnd { row_count: 0 }
            ]
        ));

        session.handle(
            &mut database,
            7,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );
        session.handle(
            &mut database,
            8,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (2, 'committed')".into(),
            },
        );
        assert_eq!(
            session
                .handle(&mut database, 9, ClientMessage::Commit)
                .messages,
            vec![ServerMessage::TransactionCommitted]
        );
        let committed = session.handle(
            &mut database,
            10,
            ClientMessage::Execute {
                sql: "SELECT id FROM users WHERE id = 2".into(),
            },
        );
        assert!(
            committed
                .messages
                .iter()
                .any(|message| matches!(message, ServerMessage::QueryRow { .. }))
        );

        database.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn invalid_transaction_sequences_return_stable_errors_without_changing_state() {
        let path = test_path("transaction-sequences");
        cleanup(&path);
        let mut database = Database::create(&path, users_table()).unwrap();
        let mut session = SessionState::new();
        hello(&mut session, &mut database);

        let commit = session.handle(&mut database, 2, ClientMessage::Commit);
        assert_error(
            &commit,
            ProtocolErrorCode::NoActiveTransaction,
            WireTransactionState::None,
        );
        let rollback = session.handle(&mut database, 3, ClientMessage::Rollback);
        assert_error(
            &rollback,
            ProtocolErrorCode::NoActiveTransaction,
            WireTransactionState::None,
        );
        session.handle(
            &mut database,
            4,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );
        let duplicate = session.handle(
            &mut database,
            5,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );
        assert_error(
            &duplicate,
            ProtocolErrorCode::TransactionAlreadyActive,
            WireTransactionState::Active,
        );
        session.handle(&mut database, 6, ClientMessage::Rollback);

        database.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn compile_failure_keeps_active_transaction_but_dml_failure_clears_rollback() {
        let path = test_path("transaction-errors");
        cleanup(&path);
        let mut database = Database::create(&path, users_table()).unwrap();
        let mut session = SessionState::new();
        hello(&mut session, &mut database);
        session.handle(
            &mut database,
            2,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );

        let compile = session.handle(
            &mut database,
            3,
            ClientMessage::Execute {
                sql: "SELECT FROM".into(),
            },
        );
        assert_error(
            &compile,
            ProtocolErrorCode::Compile,
            WireTransactionState::Active,
        );
        assert_eq!(
            session
                .handle(
                    &mut database,
                    4,
                    ClientMessage::Execute {
                        sql: "INSERT INTO users (id, name) VALUES (1, 'ok')".into(),
                    },
                )
                .messages,
            vec![ServerMessage::AffectedRows { count: 1 }]
        );
        session.handle(&mut database, 5, ClientMessage::Rollback);

        database
            .execute("INSERT INTO users (id, name) VALUES (2, 'old')")
            .unwrap();
        session.handle(
            &mut database,
            6,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );
        let huge = "x".repeat(5_000);
        let failed = session.handle(
            &mut database,
            7,
            ClientMessage::Execute {
                sql: format!("UPDATE users SET name = '{huge}' WHERE id = 2"),
            },
        );
        assert_error(
            &failed,
            ProtocolErrorCode::Execution,
            WireTransactionState::None,
        );
        assert_eq!(
            session
                .handle(
                    &mut database,
                    8,
                    ClientMessage::Begin {
                        table_id: TableId(1)
                    }
                )
                .messages,
            vec![ServerMessage::TransactionStarted]
        );
        session.handle(&mut database, 9, ClientMessage::Rollback);

        database.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn analyze_is_blocked_in_transactions_and_close_explicitly_rolls_back() {
        let path = test_path("analyze-close");
        cleanup(&path);
        let mut database = Database::create(&path, users_table()).unwrap();
        let mut session = SessionState::new();
        hello(&mut session, &mut database);
        assert_eq!(
            session
                .handle(
                    &mut database,
                    2,
                    ClientMessage::Analyze {
                        table_id: TableId(1)
                    }
                )
                .messages,
            vec![ServerMessage::AnalyzeAck]
        );
        session.handle(
            &mut database,
            3,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );
        let blocked = session.handle(
            &mut database,
            4,
            ClientMessage::Analyze {
                table_id: TableId(1),
            },
        );
        assert_error(
            &blocked,
            ProtocolErrorCode::OperationNotAllowedInTransaction,
            WireTransactionState::Active,
        );
        session.handle(
            &mut database,
            5,
            ClientMessage::Execute {
                sql: "INSERT INTO users (id, name) VALUES (9, 'disconnect')".into(),
            },
        );
        session.close().unwrap();
        let result = database.query("SELECT id FROM users WHERE id = 9").unwrap();
        assert!(result.rows.is_empty());

        database.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn result_boundary_rejects_shape_nullability_and_runtime_type_mismatches() {
        let column = ResultColumn {
            name: "id".into(),
            data_type: SemanticType::physical(PhysicalType::UInt64),
            nullable: false,
        };
        for rows in [
            vec![vec![]],
            vec![vec![ScalarValue::Null]],
            vec![vec![ScalarValue::Text("wrong".into())]],
        ] {
            assert!(matches!(
                build_query_messages(
                    QueryResult {
                        columns: vec![column.clone()],
                        rows,
                    },
                    usize::MAX
                ),
                Err(ServerError::InternalResultMismatch { .. })
            ));
        }
    }

    #[test]
    fn result_row_limit_returns_one_error_and_preserves_an_active_transaction() {
        let path = test_path("result-row-limit");
        cleanup(&path);
        let mut database = Database::create(&path, users_table()).unwrap();
        for id in 1..=3 {
            database
                .execute(&format!(
                    "INSERT INTO users (id, name) VALUES ({id}, 'row')"
                ))
                .unwrap();
        }
        let mut session = SessionState::with_policy(SessionPolicy::new(2).unwrap());
        hello(&mut session, &mut database);
        session.handle(
            &mut database,
            2,
            ClientMessage::Begin {
                table_id: TableId(1),
            },
        );

        let limited = session.handle_with_metadata(
            &mut database,
            3,
            ClientMessage::Execute {
                sql: "SELECT id FROM users ORDER BY id".into(),
            },
        );
        assert!(limited.result_row_limit_exceeded);
        let limited = limited.batch;
        assert_error(
            &limited,
            ProtocolErrorCode::ResponseTooLarge,
            WireTransactionState::Active,
        );
        assert_eq!(limited.messages.len(), 1);

        let successful = session.handle(
            &mut database,
            4,
            ClientMessage::Execute {
                sql: "SELECT id FROM users ORDER BY id LIMIT 1".into(),
            },
        );
        assert!(matches!(
            successful.messages.as_slice(),
            [
                ServerMessage::QueryStart { .. },
                ServerMessage::QueryRow { .. },
                ServerMessage::QueryEnd { row_count: 1 }
            ]
        ));
        assert_eq!(
            session
                .handle(&mut database, 5, ClientMessage::Rollback)
                .messages,
            vec![ServerMessage::TransactionRolledBack]
        );

        let frame_limited = session.classified_server_error_batch(6, ServerError::ResponseTooLarge);
        assert!(!frame_limited.result_row_limit_exceeded);
        assert_error(
            &frame_limited.batch,
            ProtocolErrorCode::ResponseTooLarge,
            WireTransactionState::None,
        );

        database.close().unwrap();
        cleanup(&path);
    }

    #[test]
    fn oversized_error_text_is_utf8_truncated_to_an_encodable_frame() {
        let session = SessionState::new();
        let diagnostic = "界".repeat(MAX_ERROR_MESSAGE_BYTES / 3 + 1);
        let batch = session.error_batch(1, ProtocolErrorCode::Compile, &diagnostic);
        let [ServerMessage::Error { message, .. }] = batch.messages.as_slice() else {
            panic!("expected one error response");
        };
        assert!(message.len() <= MAX_ERROR_MESSAGE_BYTES);
        assert!(message.ends_with("... [truncated]"));
        netbadb_protocol::encode_server_frame(batch.request_id, &batch.messages[0]).unwrap();
    }
}
