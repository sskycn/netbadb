//! Versioned, language-neutral NetbaDB wire protocol v1.

use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};

pub use netbadb_types::{PhysicalType, ScalarValue, SemanticType, TableId};

pub const PROTOCOL_MAGIC: [u8; 4] = *b"NDBP";
pub const PROTOCOL_VERSION: u16 = 1;
pub const FRAME_HEADER_SIZE: usize = 24;
pub const MAX_FRAME_PAYLOAD: u32 = 16 * 1024 * 1024;
pub const MAX_COLLECTION_ITEMS: u32 = 65_536;
pub const MAX_ERROR_MESSAGE_BYTES: usize = MAX_FRAME_PAYLOAD as usize - 8;

pub const CAPABILITY_EXPLICIT_TRANSACTIONS: u64 = 1 << 0;
pub const CAPABILITY_ANALYZE: u64 = 1 << 1;
pub const CAPABILITY_STREAMED_QUERY_RESULTS: u64 = 1 << 2;
pub const SERVER_CAPABILITIES: u64 =
    CAPABILITY_EXPLICIT_TRANSACTIONS | CAPABILITY_ANALYZE | CAPABILITY_STREAMED_QUERY_RESULTS;

const CLIENT_HELLO: u16 = 1;
const CLIENT_EXECUTE: u16 = 2;
const CLIENT_BEGIN: u16 = 3;
const CLIENT_COMMIT: u16 = 4;
const CLIENT_ROLLBACK: u16 = 5;
const CLIENT_ANALYZE: u16 = 6;
const CLIENT_PING: u16 = 7;

const SERVER_HELLO_ACK: u16 = 0x8001;
const SERVER_QUERY_START: u16 = 0x8002;
const SERVER_QUERY_ROW: u16 = 0x8003;
const SERVER_QUERY_END: u16 = 0x8004;
const SERVER_AFFECTED_ROWS: u16 = 0x8005;
const SERVER_TRANSACTION_STARTED: u16 = 0x8006;
const SERVER_TRANSACTION_COMMITTED: u16 = 0x8007;
const SERVER_TRANSACTION_ROLLED_BACK: u16 = 0x8008;
const SERVER_ANALYZE_ACK: u16 = 0x8009;
const SERVER_PONG: u16 = 0x800A;
const SERVER_ERROR: u16 = 0x8FFF;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame<T> {
    pub request_id: u64,
    pub message: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Hello,
    Execute { sql: String },
    Begin { table_id: TableId },
    Commit,
    Rollback,
    Analyze { table_id: TableId },
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchemaIdentity {
    pub table_id: TableId,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireResultColumn {
    pub name: String,
    pub data_type: SemanticType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolErrorCode {
    Protocol,
    HandshakeRequired,
    AlreadyHandshaken,
    TransactionAlreadyActive,
    NoActiveTransaction,
    OperationNotAllowedInTransaction,
    Compile,
    Schema,
    Storage,
    Execution,
    Database,
    ResponseTooLarge,
    InternalResultMismatch,
}

impl ProtocolErrorCode {
    fn tag(self) -> u16 {
        match self {
            Self::Protocol => 1,
            Self::HandshakeRequired => 2,
            Self::AlreadyHandshaken => 3,
            Self::TransactionAlreadyActive => 4,
            Self::NoActiveTransaction => 5,
            Self::OperationNotAllowedInTransaction => 6,
            Self::Compile => 7,
            Self::Schema => 8,
            Self::Storage => 9,
            Self::Execution => 10,
            Self::Database => 11,
            Self::ResponseTooLarge => 12,
            Self::InternalResultMismatch => 13,
        }
    }

    fn from_tag(tag: u16) -> Result<Self, ProtocolError> {
        match tag {
            1 => Ok(Self::Protocol),
            2 => Ok(Self::HandshakeRequired),
            3 => Ok(Self::AlreadyHandshaken),
            4 => Ok(Self::TransactionAlreadyActive),
            5 => Ok(Self::NoActiveTransaction),
            6 => Ok(Self::OperationNotAllowedInTransaction),
            7 => Ok(Self::Compile),
            8 => Ok(Self::Schema),
            9 => Ok(Self::Storage),
            10 => Ok(Self::Execution),
            11 => Ok(Self::Database),
            12 => Ok(Self::ResponseTooLarge),
            13 => Ok(Self::InternalResultMismatch),
            other => Err(ProtocolError::InvalidErrorCode(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireTransactionState {
    None,
    Active,
    RollbackRequired,
    CommitPending,
    RollbackPending,
}

impl WireTransactionState {
    fn tag(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Active => 1,
            Self::RollbackRequired => 2,
            Self::CommitPending => 3,
            Self::RollbackPending => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, ProtocolError> {
        match tag {
            0 => Ok(Self::None),
            1 => Ok(Self::Active),
            2 => Ok(Self::RollbackRequired),
            3 => Ok(Self::CommitPending),
            4 => Ok(Self::RollbackPending),
            other => Err(ProtocolError::InvalidTransactionState(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    HelloAck {
        protocol_version: u16,
        max_frame_payload: u32,
        capabilities: u64,
        tables: Vec<TableSchemaIdentity>,
    },
    QueryStart {
        columns: Vec<WireResultColumn>,
    },
    QueryRow {
        values: Vec<ScalarValue>,
    },
    QueryEnd {
        row_count: u64,
    },
    AffectedRows {
        count: u64,
    },
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
    AnalyzeAck,
    Pong,
    Error {
        code: ProtocolErrorCode,
        transaction_state: WireTransactionState,
        message: String,
    },
}

#[derive(Debug)]
pub enum ProtocolError {
    Io(io::Error),
    BadMagic([u8; 4]),
    UnsupportedVersion(u16),
    UnknownMessageKind(u16),
    UnsupportedFlags(u16),
    InvalidReservedBytes,
    FrameTooLarge(u32),
    TruncatedFrame,
    ZeroRequestId,
    InvalidUtf8,
    InvalidPhysicalType(u8),
    InvalidScalarTag(u8),
    InvalidBoolean(u8),
    InvalidTransactionState(u8),
    InvalidErrorCode(u16),
    CollectionTooLarge(u32),
    InvalidPayload(&'static str),
    ExtraBytes,
    LengthOverflow,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "protocol I/O failed: {error}"),
            Self::BadMagic(magic) => write!(
                formatter,
                "invalid protocol magic {:02x}{:02x}{:02x}{:02x}",
                magic[0], magic[1], magic[2], magic[3]
            ),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported protocol version {version}")
            }
            Self::UnknownMessageKind(kind) => write!(formatter, "unknown message kind {kind:#06x}"),
            Self::UnsupportedFlags(flags) => {
                write!(formatter, "unsupported frame flags {flags:#06x}")
            }
            Self::InvalidReservedBytes => {
                formatter.write_str("protocol reserved bytes are non-zero")
            }
            Self::FrameTooLarge(length) => write!(
                formatter,
                "frame payload length {length} exceeds limit {MAX_FRAME_PAYLOAD}"
            ),
            Self::TruncatedFrame => formatter.write_str("protocol frame is truncated"),
            Self::ZeroRequestId => formatter.write_str("request ID zero is reserved"),
            Self::InvalidUtf8 => formatter.write_str("protocol string is not valid UTF-8"),
            Self::InvalidPhysicalType(tag) => write!(formatter, "invalid physical type tag {tag}"),
            Self::InvalidScalarTag(tag) => write!(formatter, "invalid scalar value tag {tag}"),
            Self::InvalidBoolean(value) => write!(formatter, "invalid boolean byte {value}"),
            Self::InvalidTransactionState(tag) => {
                write!(formatter, "invalid transaction-state tag {tag}")
            }
            Self::InvalidErrorCode(code) => write!(formatter, "invalid protocol error code {code}"),
            Self::CollectionTooLarge(count) => write!(
                formatter,
                "protocol collection item count {count} exceeds limit {MAX_COLLECTION_ITEMS}"
            ),
            Self::InvalidPayload(reason) => write!(formatter, "invalid message payload: {reason}"),
            Self::ExtraBytes => formatter.write_str("protocol message has trailing bytes"),
            Self::LengthOverflow => formatter.write_str("protocol length exceeds its wire width"),
        }
    }
}

impl Error for ProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ProtocolError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub fn encode_client_frame(
    request_id: u64,
    message: &ClientMessage,
) -> Result<Vec<u8>, ProtocolError> {
    let (kind, payload) = encode_client_payload(message)?;
    encode_frame(request_id, kind, &payload)
}

pub fn decode_client_frame(input: &[u8]) -> Result<Frame<ClientMessage>, ProtocolError> {
    let (request_id, kind, payload) = decode_frame(input)?;
    Ok(Frame {
        request_id,
        message: decode_client_payload(kind, payload)?,
    })
}

pub fn encode_server_frame(
    request_id: u64,
    message: &ServerMessage,
) -> Result<Vec<u8>, ProtocolError> {
    let (kind, payload) = encode_server_payload(message)?;
    encode_frame(request_id, kind, &payload)
}

pub fn decode_server_frame(input: &[u8]) -> Result<Frame<ServerMessage>, ProtocolError> {
    let (request_id, kind, payload) = decode_frame(input)?;
    Ok(Frame {
        request_id,
        message: decode_server_payload(kind, payload)?,
    })
}

/// Validates one response against all v1 payload bounds without writing it.
pub fn validate_server_message(message: &ServerMessage) -> Result<(), ProtocolError> {
    let (_, payload) = encode_server_payload(message)?;
    validate_payload_length(payload.len()).map(|_| ())
}

pub fn read_client_frame<R: Read>(
    reader: &mut R,
) -> Result<Option<Frame<ClientMessage>>, ProtocolError> {
    read_frame(reader, decode_client_payload)
}

pub fn read_server_frame<R: Read>(
    reader: &mut R,
) -> Result<Option<Frame<ServerMessage>>, ProtocolError> {
    read_frame(reader, decode_server_payload)
}

pub fn write_client_frame<W: Write>(
    writer: &mut W,
    frame: &Frame<ClientMessage>,
) -> Result<(), ProtocolError> {
    writer.write_all(&encode_client_frame(frame.request_id, &frame.message)?)?;
    Ok(())
}

pub fn write_server_frame<W: Write>(
    writer: &mut W,
    frame: &Frame<ServerMessage>,
) -> Result<(), ProtocolError> {
    writer.write_all(&encode_server_frame(frame.request_id, &frame.message)?)?;
    Ok(())
}

fn encode_client_payload(message: &ClientMessage) -> Result<(u16, Vec<u8>), ProtocolError> {
    let mut payload = Vec::new();
    let kind = match message {
        ClientMessage::Hello => CLIENT_HELLO,
        ClientMessage::Execute { sql } => {
            push_string(&mut payload, sql)?;
            CLIENT_EXECUTE
        }
        ClientMessage::Begin { table_id } => {
            payload.extend_from_slice(&table_id.0.to_le_bytes());
            CLIENT_BEGIN
        }
        ClientMessage::Commit => CLIENT_COMMIT,
        ClientMessage::Rollback => CLIENT_ROLLBACK,
        ClientMessage::Analyze { table_id } => {
            payload.extend_from_slice(&table_id.0.to_le_bytes());
            CLIENT_ANALYZE
        }
        ClientMessage::Ping => CLIENT_PING,
    };
    validate_payload_length(payload.len())?;
    Ok((kind, payload))
}

fn decode_client_payload(kind: u16, payload: &[u8]) -> Result<ClientMessage, ProtocolError> {
    let mut input = Cursor::new(payload);
    let message = match kind {
        CLIENT_HELLO => ClientMessage::Hello,
        CLIENT_EXECUTE => ClientMessage::Execute {
            sql: input.read_string()?,
        },
        CLIENT_BEGIN => ClientMessage::Begin {
            table_id: TableId(input.read_u64()?),
        },
        CLIENT_COMMIT => ClientMessage::Commit,
        CLIENT_ROLLBACK => ClientMessage::Rollback,
        CLIENT_ANALYZE => ClientMessage::Analyze {
            table_id: TableId(input.read_u64()?),
        },
        CLIENT_PING => ClientMessage::Ping,
        other => return Err(ProtocolError::UnknownMessageKind(other)),
    };
    input.finish()?;
    Ok(message)
}

fn encode_server_payload(message: &ServerMessage) -> Result<(u16, Vec<u8>), ProtocolError> {
    let mut payload = Vec::new();
    let kind = match message {
        ServerMessage::HelloAck {
            protocol_version,
            max_frame_payload,
            capabilities,
            tables,
        } => {
            if *protocol_version != PROTOCOL_VERSION {
                return Err(ProtocolError::UnsupportedVersion(*protocol_version));
            }
            if *max_frame_payload > MAX_FRAME_PAYLOAD {
                return Err(ProtocolError::FrameTooLarge(*max_frame_payload));
            }
            payload.extend_from_slice(&protocol_version.to_le_bytes());
            payload.extend_from_slice(&0_u16.to_le_bytes());
            payload.extend_from_slice(&max_frame_payload.to_le_bytes());
            payload.extend_from_slice(&capabilities.to_le_bytes());
            push_count(&mut payload, tables.len())?;
            for table in tables {
                payload.extend_from_slice(&table.table_id.0.to_le_bytes());
                payload.extend_from_slice(&table.fingerprint);
            }
            SERVER_HELLO_ACK
        }
        ServerMessage::QueryStart { columns } => {
            push_count(&mut payload, columns.len())?;
            for column in columns {
                push_string(&mut payload, &column.name)?;
                encode_semantic_type(&mut payload, &column.data_type)?;
                payload.push(u8::from(column.nullable));
            }
            SERVER_QUERY_START
        }
        ServerMessage::QueryRow { values } => {
            push_count(&mut payload, values.len())?;
            for value in values {
                encode_scalar(&mut payload, value)?;
            }
            SERVER_QUERY_ROW
        }
        ServerMessage::QueryEnd { row_count } => {
            payload.extend_from_slice(&row_count.to_le_bytes());
            SERVER_QUERY_END
        }
        ServerMessage::AffectedRows { count } => {
            payload.extend_from_slice(&count.to_le_bytes());
            SERVER_AFFECTED_ROWS
        }
        ServerMessage::TransactionStarted => SERVER_TRANSACTION_STARTED,
        ServerMessage::TransactionCommitted => SERVER_TRANSACTION_COMMITTED,
        ServerMessage::TransactionRolledBack => SERVER_TRANSACTION_ROLLED_BACK,
        ServerMessage::AnalyzeAck => SERVER_ANALYZE_ACK,
        ServerMessage::Pong => SERVER_PONG,
        ServerMessage::Error {
            code,
            transaction_state,
            message,
        } => {
            payload.extend_from_slice(&code.tag().to_le_bytes());
            payload.push(transaction_state.tag());
            payload.push(0);
            push_string(&mut payload, message)?;
            SERVER_ERROR
        }
    };
    validate_payload_length(payload.len())?;
    Ok((kind, payload))
}

fn decode_server_payload(kind: u16, payload: &[u8]) -> Result<ServerMessage, ProtocolError> {
    let mut input = Cursor::new(payload);
    let message = match kind {
        SERVER_HELLO_ACK => {
            let protocol_version = input.read_u16()?;
            if protocol_version != PROTOCOL_VERSION {
                return Err(ProtocolError::UnsupportedVersion(protocol_version));
            }
            if input.read_u16()? != 0 {
                return Err(ProtocolError::InvalidReservedBytes);
            }
            let max_frame_payload = input.read_u32()?;
            if max_frame_payload > MAX_FRAME_PAYLOAD {
                return Err(ProtocolError::FrameTooLarge(max_frame_payload));
            }
            let capabilities = input.read_u64()?;
            let table_count = input.read_bounded_count(40)?;
            let mut tables = Vec::with_capacity(table_count);
            for _ in 0..table_count {
                tables.push(TableSchemaIdentity {
                    table_id: TableId(input.read_u64()?),
                    fingerprint: input.read_array::<32>()?,
                });
            }
            ServerMessage::HelloAck {
                protocol_version,
                max_frame_payload,
                capabilities,
                tables,
            }
        }
        SERVER_QUERY_START => {
            let column_count = input.read_bounded_count(9)?;
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                columns.push(WireResultColumn {
                    name: input.read_string()?,
                    data_type: decode_semantic_type(&mut input)?,
                    nullable: input.read_bool()?,
                });
            }
            ServerMessage::QueryStart { columns }
        }
        SERVER_QUERY_ROW => {
            let value_count = input.read_bounded_count(1)?;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                values.push(decode_scalar(&mut input)?);
            }
            ServerMessage::QueryRow { values }
        }
        SERVER_QUERY_END => ServerMessage::QueryEnd {
            row_count: input.read_u64()?,
        },
        SERVER_AFFECTED_ROWS => ServerMessage::AffectedRows {
            count: input.read_u64()?,
        },
        SERVER_TRANSACTION_STARTED => ServerMessage::TransactionStarted,
        SERVER_TRANSACTION_COMMITTED => ServerMessage::TransactionCommitted,
        SERVER_TRANSACTION_ROLLED_BACK => ServerMessage::TransactionRolledBack,
        SERVER_ANALYZE_ACK => ServerMessage::AnalyzeAck,
        SERVER_PONG => ServerMessage::Pong,
        SERVER_ERROR => {
            let code = ProtocolErrorCode::from_tag(input.read_u16()?)?;
            let transaction_state = WireTransactionState::from_tag(input.read_u8()?)?;
            if input.read_u8()? != 0 {
                return Err(ProtocolError::InvalidReservedBytes);
            }
            ServerMessage::Error {
                code,
                transaction_state,
                message: input.read_string()?,
            }
        }
        other => return Err(ProtocolError::UnknownMessageKind(other)),
    };
    input.finish()?;
    Ok(message)
}

fn encode_frame(request_id: u64, kind: u16, payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    if request_id == 0 {
        return Err(ProtocolError::ZeroRequestId);
    }
    let payload_length = validate_payload_length(payload.len())?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&PROTOCOL_MAGIC);
    frame.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    frame.extend_from_slice(&kind.to_le_bytes());
    frame.extend_from_slice(&0_u16.to_le_bytes());
    frame.extend_from_slice(&0_u16.to_le_bytes());
    frame.extend_from_slice(&payload_length.to_le_bytes());
    frame.extend_from_slice(&request_id.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn decode_frame(input: &[u8]) -> Result<(u64, u16, &[u8]), ProtocolError> {
    if input.len() < FRAME_HEADER_SIZE {
        return Err(ProtocolError::TruncatedFrame);
    }
    let (request_id, kind, payload_length) = decode_header(&input[..FRAME_HEADER_SIZE])?;
    let expected = FRAME_HEADER_SIZE
        .checked_add(payload_length)
        .ok_or(ProtocolError::LengthOverflow)?;
    if input.len() < expected {
        return Err(ProtocolError::TruncatedFrame);
    }
    if input.len() > expected {
        return Err(ProtocolError::ExtraBytes);
    }
    Ok((request_id, kind, &input[FRAME_HEADER_SIZE..]))
}

fn decode_header(header: &[u8]) -> Result<(u64, u16, usize), ProtocolError> {
    if header.len() != FRAME_HEADER_SIZE {
        return Err(ProtocolError::TruncatedFrame);
    }
    let magic = header[0..4]
        .try_into()
        .map_err(|_| ProtocolError::TruncatedFrame)?;
    if magic != PROTOCOL_MAGIC {
        return Err(ProtocolError::BadMagic(magic));
    }
    let version = u16::from_le_bytes(
        header[4..6]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedFrame)?,
    );
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let kind = u16::from_le_bytes(
        header[6..8]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedFrame)?,
    );
    let flags = u16::from_le_bytes(
        header[8..10]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedFrame)?,
    );
    if flags != 0 {
        return Err(ProtocolError::UnsupportedFlags(flags));
    }
    let reserved = u16::from_le_bytes(
        header[10..12]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedFrame)?,
    );
    if reserved != 0 {
        return Err(ProtocolError::InvalidReservedBytes);
    }
    let payload_length = u32::from_le_bytes(
        header[12..16]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedFrame)?,
    );
    if payload_length > MAX_FRAME_PAYLOAD {
        return Err(ProtocolError::FrameTooLarge(payload_length));
    }
    let request_id = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedFrame)?,
    );
    if request_id == 0 {
        return Err(ProtocolError::ZeroRequestId);
    }
    Ok((request_id, kind, payload_length as usize))
}

fn read_frame<R: Read, T>(
    reader: &mut R,
    decode_payload: fn(u16, &[u8]) -> Result<T, ProtocolError>,
) -> Result<Option<Frame<T>>, ProtocolError> {
    let mut header = [0_u8; FRAME_HEADER_SIZE];
    let mut filled = 0;
    while filled < FRAME_HEADER_SIZE {
        match reader.read(&mut header[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err(ProtocolError::TruncatedFrame),
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    let (request_id, kind, payload_length) = decode_header(&header)?;
    let mut payload = vec![0_u8; payload_length];
    read_exact_payload(reader, &mut payload)?;
    Ok(Some(Frame {
        request_id,
        message: decode_payload(kind, &payload)?,
    }))
}

fn read_exact_payload<R: Read>(reader: &mut R, payload: &mut [u8]) -> Result<(), ProtocolError> {
    let mut filled = 0;
    while filled < payload.len() {
        match reader.read(&mut payload[filled..]) {
            Ok(0) => return Err(ProtocolError::TruncatedFrame),
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn validate_payload_length(length: usize) -> Result<u32, ProtocolError> {
    let length = u32::try_from(length).map_err(|_| ProtocolError::LengthOverflow)?;
    if length > MAX_FRAME_PAYLOAD {
        return Err(ProtocolError::FrameTooLarge(length));
    }
    Ok(length)
}

fn push_count(output: &mut Vec<u8>, count: usize) -> Result<(), ProtocolError> {
    let count = u32::try_from(count).map_err(|_| ProtocolError::LengthOverflow)?;
    if count > MAX_COLLECTION_ITEMS {
        return Err(ProtocolError::CollectionTooLarge(count));
    }
    output.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn push_string(output: &mut Vec<u8>, value: &str) -> Result<(), ProtocolError> {
    let length = u32::try_from(value.len()).map_err(|_| ProtocolError::LengthOverflow)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_semantic_type(
    output: &mut Vec<u8>,
    data_type: &SemanticType,
) -> Result<(), ProtocolError> {
    output.push(physical_type_tag(data_type.physical));
    output.push(u8::from(data_type.name.is_some()));
    output.extend_from_slice(&0_u16.to_le_bytes());
    if let Some(name) = &data_type.name {
        push_string(output, name)?;
    }
    Ok(())
}

fn decode_semantic_type(input: &mut Cursor<'_>) -> Result<SemanticType, ProtocolError> {
    let physical = physical_type_from_tag(input.read_u8()?)?;
    let has_name = input.read_bool()?;
    if input.read_u16()? != 0 {
        return Err(ProtocolError::InvalidReservedBytes);
    }
    Ok(match has_name {
        true => SemanticType::named(input.read_string()?, physical),
        false => SemanticType::physical(physical),
    })
}

fn physical_type_tag(data_type: PhysicalType) -> u8 {
    match data_type {
        PhysicalType::Bool => 1,
        PhysicalType::Int64 => 2,
        PhysicalType::UInt64 => 3,
        PhysicalType::Text => 4,
    }
}

fn physical_type_from_tag(tag: u8) -> Result<PhysicalType, ProtocolError> {
    match tag {
        1 => Ok(PhysicalType::Bool),
        2 => Ok(PhysicalType::Int64),
        3 => Ok(PhysicalType::UInt64),
        4 => Ok(PhysicalType::Text),
        other => Err(ProtocolError::InvalidPhysicalType(other)),
    }
}

fn encode_scalar(output: &mut Vec<u8>, value: &ScalarValue) -> Result<(), ProtocolError> {
    match value {
        ScalarValue::Null => output.push(0),
        ScalarValue::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        ScalarValue::Int64(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::UInt64(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::Text(value) => {
            output.push(4);
            push_string(output, value)?;
        }
    }
    Ok(())
}

fn decode_scalar(input: &mut Cursor<'_>) -> Result<ScalarValue, ProtocolError> {
    match input.read_u8()? {
        0 => Ok(ScalarValue::Null),
        1 => Ok(ScalarValue::Bool(input.read_bool()?)),
        2 => Ok(ScalarValue::Int64(input.read_i64()?)),
        3 => Ok(ScalarValue::UInt64(input.read_u64()?)),
        4 => Ok(ScalarValue::Text(input.read_string()?)),
        other => Err(ProtocolError::InvalidScalarTag(other)),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(ProtocolError::LengthOverflow)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ProtocolError::TruncatedFrame)?;
        self.position = end;
        bytes.try_into().map_err(|_| ProtocolError::TruncatedFrame)
    }

    fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64, ProtocolError> {
        Ok(i64::from_le_bytes(self.read_array()?))
    }

    fn read_bool(&mut self) -> Result<bool, ProtocolError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(ProtocolError::InvalidBoolean(other)),
        }
    }

    fn read_bounded_count(&mut self, minimum_item_size: usize) -> Result<usize, ProtocolError> {
        let wire_count = self.read_u32()?;
        if wire_count > MAX_COLLECTION_ITEMS {
            return Err(ProtocolError::CollectionTooLarge(wire_count));
        }
        let count = wire_count as usize;
        if minimum_item_size == 0 || count > self.remaining() / minimum_item_size {
            return Err(ProtocolError::InvalidPayload(
                "count exceeds remaining payload",
            ));
        }
        Ok(count)
    }

    fn read_string(&mut self) -> Result<String, ProtocolError> {
        let length = self.read_u32()? as usize;
        if length > self.remaining() {
            return Err(ProtocolError::InvalidPayload(
                "string length exceeds remaining payload",
            ));
        }
        let end = self.position + length;
        let bytes = &self.bytes[self.position..end];
        let value = std::str::from_utf8(bytes).map_err(|_| ProtocolError::InvalidUtf8)?;
        self.position = end;
        Ok(value.to_owned())
    }

    fn finish(self) -> Result<(), ProtocolError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ProtocolError::ExtraBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor as IoCursor;

    use super::*;

    fn frame_header(kind: u16, payload_length: u32, request_id: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"NDBP");
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&kind.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&payload_length.to_le_bytes());
        bytes.extend_from_slice(&request_id.to_le_bytes());
        bytes
    }

    #[test]
    fn client_golden_frames_are_exact() {
        assert_eq!(
            encode_client_frame(1, &ClientMessage::Hello).unwrap(),
            frame_header(0x0001, 0, 1)
        );

        let mut begin = frame_header(0x0003, 8, 7);
        begin.extend_from_slice(&42_u64.to_le_bytes());
        assert_eq!(
            encode_client_frame(
                7,
                &ClientMessage::Begin {
                    table_id: TableId(42)
                }
            )
            .unwrap(),
            begin
        );

        let sql = "SELECT id FROM users";
        let mut execute = frame_header(0x0002, 4 + sql.len() as u32, 9);
        execute.extend_from_slice(&(sql.len() as u32).to_le_bytes());
        execute.extend_from_slice(sql.as_bytes());
        assert_eq!(
            encode_client_frame(9, &ClientMessage::Execute { sql: sql.into() }).unwrap(),
            execute
        );
    }

    #[test]
    fn server_golden_frames_are_exact() {
        let fingerprint = [0xAB; 32];
        let hello = ServerMessage::HelloAck {
            protocol_version: 1,
            max_frame_payload: MAX_FRAME_PAYLOAD,
            capabilities: SERVER_CAPABILITIES,
            tables: vec![TableSchemaIdentity {
                table_id: TableId(5),
                fingerprint,
            }],
        };
        let mut hello_expected = frame_header(0x8001, 60, 1);
        hello_expected.extend_from_slice(&1_u16.to_le_bytes());
        hello_expected.extend_from_slice(&0_u16.to_le_bytes());
        hello_expected.extend_from_slice(&16_777_216_u32.to_le_bytes());
        hello_expected.extend_from_slice(&7_u64.to_le_bytes());
        hello_expected.extend_from_slice(&1_u32.to_le_bytes());
        hello_expected.extend_from_slice(&5_u64.to_le_bytes());
        hello_expected.extend_from_slice(&fingerprint);
        assert_eq!(encode_server_frame(1, &hello).unwrap(), hello_expected);

        let affected = ServerMessage::AffectedRows { count: 3 };
        let mut affected_expected = frame_header(0x8005, 8, 2);
        affected_expected.extend_from_slice(&3_u64.to_le_bytes());
        assert_eq!(
            encode_server_frame(2, &affected).unwrap(),
            affected_expected
        );

        let start = ServerMessage::QueryStart {
            columns: vec![WireResultColumn {
                name: "id".into(),
                data_type: SemanticType::named("UserId", PhysicalType::UInt64),
                nullable: false,
            }],
        };
        let mut start_expected = frame_header(0x8002, 25, 3);
        start_expected.extend_from_slice(&1_u32.to_le_bytes());
        start_expected.extend_from_slice(&2_u32.to_le_bytes());
        start_expected.extend_from_slice(b"id");
        start_expected.extend_from_slice(&[3, 1, 0, 0]);
        start_expected.extend_from_slice(&6_u32.to_le_bytes());
        start_expected.extend_from_slice(b"UserId");
        start_expected.push(0);
        assert_eq!(encode_server_frame(3, &start).unwrap(), start_expected);

        let row = ServerMessage::QueryRow {
            values: vec![
                ScalarValue::Null,
                ScalarValue::Bool(true),
                ScalarValue::Int64(-2),
                ScalarValue::UInt64(3),
                ScalarValue::Text("x".into()),
            ],
        };
        let mut row_payload = Vec::new();
        row_payload.extend_from_slice(&5_u32.to_le_bytes());
        row_payload.push(0);
        row_payload.extend_from_slice(&[1, 1]);
        row_payload.push(2);
        row_payload.extend_from_slice(&(-2_i64).to_le_bytes());
        row_payload.push(3);
        row_payload.extend_from_slice(&3_u64.to_le_bytes());
        row_payload.push(4);
        row_payload.extend_from_slice(&1_u32.to_le_bytes());
        row_payload.push(b'x');
        let mut row_expected = frame_header(0x8003, row_payload.len() as u32, 4);
        row_expected.extend_from_slice(&row_payload);
        assert_eq!(encode_server_frame(4, &row).unwrap(), row_expected);

        let error = ServerMessage::Error {
            code: ProtocolErrorCode::Compile,
            transaction_state: WireTransactionState::Active,
            message: "bad SQL".into(),
        };
        let mut error_expected = frame_header(0x8FFF, 15, 5);
        error_expected.extend_from_slice(&7_u16.to_le_bytes());
        error_expected.extend_from_slice(&[1, 0]);
        error_expected.extend_from_slice(&7_u32.to_le_bytes());
        error_expected.extend_from_slice(b"bad SQL");
        assert_eq!(encode_server_frame(5, &error).unwrap(), error_expected);
    }

    #[test]
    fn all_stable_tags_and_capability_bits_are_exact() {
        assert_eq!(CAPABILITY_EXPLICIT_TRANSACTIONS, 0x1);
        assert_eq!(CAPABILITY_ANALYZE, 0x2);
        assert_eq!(CAPABILITY_STREAMED_QUERY_RESULTS, 0x4);

        let clients = [
            (ClientMessage::Hello, 0x0001_u16),
            (ClientMessage::Execute { sql: String::new() }, 0x0002),
            (
                ClientMessage::Begin {
                    table_id: TableId(1),
                },
                0x0003,
            ),
            (ClientMessage::Commit, 0x0004),
            (ClientMessage::Rollback, 0x0005),
            (
                ClientMessage::Analyze {
                    table_id: TableId(1),
                },
                0x0006,
            ),
            (ClientMessage::Ping, 0x0007),
        ];
        for (message, expected) in clients {
            let frame = encode_client_frame(1, &message).unwrap();
            assert_eq!(
                u16::from_le_bytes(frame[6..8].try_into().unwrap()),
                expected
            );
        }

        let servers = [
            (
                ServerMessage::HelloAck {
                    protocol_version: 1,
                    max_frame_payload: 16_777_216,
                    capabilities: 7,
                    tables: vec![],
                },
                0x8001_u16,
            ),
            (ServerMessage::QueryStart { columns: vec![] }, 0x8002),
            (ServerMessage::QueryRow { values: vec![] }, 0x8003),
            (ServerMessage::QueryEnd { row_count: 0 }, 0x8004),
            (ServerMessage::AffectedRows { count: 0 }, 0x8005),
            (ServerMessage::TransactionStarted, 0x8006),
            (ServerMessage::TransactionCommitted, 0x8007),
            (ServerMessage::TransactionRolledBack, 0x8008),
            (ServerMessage::AnalyzeAck, 0x8009),
            (ServerMessage::Pong, 0x800A),
            (
                ServerMessage::Error {
                    code: ProtocolErrorCode::Protocol,
                    transaction_state: WireTransactionState::None,
                    message: String::new(),
                },
                0x8FFF,
            ),
        ];
        for (message, expected) in servers {
            let frame = encode_server_frame(1, &message).unwrap();
            assert_eq!(
                u16::from_le_bytes(frame[6..8].try_into().unwrap()),
                expected
            );
        }

        let error_codes = [
            (ProtocolErrorCode::Protocol, 1_u16),
            (ProtocolErrorCode::HandshakeRequired, 2),
            (ProtocolErrorCode::AlreadyHandshaken, 3),
            (ProtocolErrorCode::TransactionAlreadyActive, 4),
            (ProtocolErrorCode::NoActiveTransaction, 5),
            (ProtocolErrorCode::OperationNotAllowedInTransaction, 6),
            (ProtocolErrorCode::Compile, 7),
            (ProtocolErrorCode::Schema, 8),
            (ProtocolErrorCode::Storage, 9),
            (ProtocolErrorCode::Execution, 10),
            (ProtocolErrorCode::Database, 11),
            (ProtocolErrorCode::ResponseTooLarge, 12),
            (ProtocolErrorCode::InternalResultMismatch, 13),
        ];
        for (code, expected) in error_codes {
            let frame = encode_server_frame(
                1,
                &ServerMessage::Error {
                    code,
                    transaction_state: WireTransactionState::None,
                    message: String::new(),
                },
            )
            .unwrap();
            assert_eq!(
                u16::from_le_bytes(
                    frame[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + 2]
                        .try_into()
                        .unwrap()
                ),
                expected
            );
        }

        let states = [
            (WireTransactionState::None, 0_u8),
            (WireTransactionState::Active, 1),
            (WireTransactionState::RollbackRequired, 2),
            (WireTransactionState::CommitPending, 3),
            (WireTransactionState::RollbackPending, 4),
        ];
        for (transaction_state, expected) in states {
            let frame = encode_server_frame(
                1,
                &ServerMessage::Error {
                    code: ProtocolErrorCode::Protocol,
                    transaction_state,
                    message: String::new(),
                },
            )
            .unwrap();
            assert_eq!(frame[FRAME_HEADER_SIZE + 2], expected);
        }

        let physical_types = [
            (PhysicalType::Bool, 1_u8),
            (PhysicalType::Int64, 2),
            (PhysicalType::UInt64, 3),
            (PhysicalType::Text, 4),
        ];
        for (physical, expected) in physical_types {
            let frame = encode_server_frame(
                1,
                &ServerMessage::QueryStart {
                    columns: vec![WireResultColumn {
                        name: String::new(),
                        data_type: SemanticType::physical(physical),
                        nullable: false,
                    }],
                },
            )
            .unwrap();
            assert_eq!(frame[FRAME_HEADER_SIZE + 8], expected);
        }
    }

    #[test]
    fn all_messages_round_trip_in_their_own_direction() {
        let clients = vec![
            ClientMessage::Hello,
            ClientMessage::Execute {
                sql: "SELECT 1".into(),
            },
            ClientMessage::Begin {
                table_id: TableId(9),
            },
            ClientMessage::Commit,
            ClientMessage::Rollback,
            ClientMessage::Analyze {
                table_id: TableId(9),
            },
            ClientMessage::Ping,
        ];
        for (index, message) in clients.into_iter().enumerate() {
            let request_id = index as u64 + 1;
            let bytes = encode_client_frame(request_id, &message).unwrap();
            assert_eq!(
                decode_client_frame(&bytes).unwrap(),
                Frame {
                    request_id,
                    message
                }
            );
            assert!(matches!(
                decode_server_frame(&bytes),
                Err(ProtocolError::UnknownMessageKind(_))
            ));
        }

        let servers = vec![
            ServerMessage::HelloAck {
                protocol_version: 1,
                max_frame_payload: MAX_FRAME_PAYLOAD,
                capabilities: SERVER_CAPABILITIES,
                tables: vec![],
            },
            ServerMessage::QueryStart { columns: vec![] },
            ServerMessage::QueryRow { values: vec![] },
            ServerMessage::QueryEnd { row_count: 0 },
            ServerMessage::AffectedRows { count: 2 },
            ServerMessage::TransactionStarted,
            ServerMessage::TransactionCommitted,
            ServerMessage::TransactionRolledBack,
            ServerMessage::AnalyzeAck,
            ServerMessage::Pong,
            ServerMessage::Error {
                code: ProtocolErrorCode::Storage,
                transaction_state: WireTransactionState::RollbackPending,
                message: "retry".into(),
            },
        ];
        for (index, message) in servers.into_iter().enumerate() {
            let request_id = index as u64 + 1;
            let bytes = encode_server_frame(request_id, &message).unwrap();
            assert_eq!(
                decode_server_frame(&bytes).unwrap(),
                Frame {
                    request_id,
                    message
                }
            );
            assert!(matches!(
                decode_client_frame(&bytes),
                Err(ProtocolError::UnknownMessageKind(_))
            ));
        }
    }

    #[test]
    fn framing_distinguishes_clean_eof_from_truncation_and_streams_multiple_frames() {
        let first = Frame {
            request_id: 1,
            message: ClientMessage::Hello,
        };
        let second = Frame {
            request_id: 2,
            message: ClientMessage::Ping,
        };
        let mut bytes = Vec::new();
        write_client_frame(&mut bytes, &first).unwrap();
        write_client_frame(&mut bytes, &second).unwrap();
        let mut reader = IoCursor::new(bytes);
        assert_eq!(read_client_frame(&mut reader).unwrap(), Some(first));
        assert_eq!(read_client_frame(&mut reader).unwrap(), Some(second));
        assert_eq!(read_client_frame(&mut reader).unwrap(), None);

        assert!(matches!(
            read_client_frame(&mut IoCursor::new(&b"NDB"[..])),
            Err(ProtocolError::TruncatedFrame)
        ));
        let declared = frame_header(CLIENT_EXECUTE, 8, 1);
        assert!(matches!(
            read_client_frame(&mut IoCursor::new(declared)),
            Err(ProtocolError::TruncatedFrame)
        ));

        let server = Frame {
            request_id: 3,
            message: ServerMessage::Pong,
        };
        let mut server_bytes = Vec::new();
        write_server_frame(&mut server_bytes, &server).unwrap();
        assert_eq!(
            read_server_frame(&mut IoCursor::new(server_bytes)).unwrap(),
            Some(server)
        );
    }

    #[test]
    fn malformed_headers_are_rejected_before_payload_allocation() {
        let mut bad_magic = frame_header(CLIENT_HELLO, 0, 1);
        bad_magic[0] = b'X';
        assert!(matches!(
            decode_client_frame(&bad_magic),
            Err(ProtocolError::BadMagic(_))
        ));

        let mut bad_version = frame_header(CLIENT_HELLO, 0, 1);
        bad_version[4..6].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            decode_client_frame(&bad_version),
            Err(ProtocolError::UnsupportedVersion(2))
        ));

        let mut flags = frame_header(CLIENT_HELLO, 0, 1);
        flags[8..10].copy_from_slice(&1_u16.to_le_bytes());
        assert!(matches!(
            decode_client_frame(&flags),
            Err(ProtocolError::UnsupportedFlags(1))
        ));

        let mut reserved = frame_header(CLIENT_HELLO, 0, 1);
        reserved[10] = 1;
        assert!(matches!(
            decode_client_frame(&reserved),
            Err(ProtocolError::InvalidReservedBytes)
        ));

        let oversized = frame_header(CLIENT_HELLO, u32::MAX, 1);
        assert!(matches!(
            decode_client_frame(&oversized),
            Err(ProtocolError::FrameTooLarge(u32::MAX))
        ));
        assert!(matches!(
            read_client_frame(&mut IoCursor::new(oversized)),
            Err(ProtocolError::FrameTooLarge(u32::MAX))
        ));

        assert!(matches!(
            decode_client_frame(&frame_header(CLIENT_HELLO, 0, 0)),
            Err(ProtocolError::ZeroRequestId)
        ));
        assert!(matches!(
            decode_client_frame(&frame_header(99, 0, 1)),
            Err(ProtocolError::UnknownMessageKind(99))
        ));
    }

    #[test]
    fn malformed_payloads_are_typed_errors() {
        let mut invalid_utf8 = frame_header(CLIENT_EXECUTE, 5, 1);
        invalid_utf8.extend_from_slice(&1_u32.to_le_bytes());
        invalid_utf8.push(0xFF);
        assert!(matches!(
            decode_client_frame(&invalid_utf8),
            Err(ProtocolError::InvalidUtf8)
        ));

        let mut extra = encode_client_frame(1, &ClientMessage::Hello).unwrap();
        extra[12..16].copy_from_slice(&1_u32.to_le_bytes());
        extra.push(0);
        assert!(matches!(
            decode_client_frame(&extra),
            Err(ProtocolError::ExtraBytes)
        ));

        let mut huge_count = frame_header(SERVER_QUERY_ROW, 4, 1);
        huge_count.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_server_frame(&huge_count),
            Err(ProtocolError::CollectionTooLarge(u32::MAX))
        ));

        let too_many_values = ServerMessage::QueryRow {
            values: vec![ScalarValue::Null; MAX_COLLECTION_ITEMS as usize + 1],
        };
        assert!(matches!(
            encode_server_frame(1, &too_many_values),
            Err(ProtocolError::CollectionTooLarge(count)) if count == MAX_COLLECTION_ITEMS + 1
        ));

        let mut bad_physical = encode_server_frame(
            1,
            &ServerMessage::QueryStart {
                columns: vec![WireResultColumn {
                    name: "x".into(),
                    data_type: SemanticType::physical(PhysicalType::Bool),
                    nullable: false,
                }],
            },
        )
        .unwrap();
        bad_physical[FRAME_HEADER_SIZE + 9] = 99;
        assert!(matches!(
            decode_server_frame(&bad_physical),
            Err(ProtocolError::InvalidPhysicalType(99))
        ));

        let mut bad_scalar = frame_header(SERVER_QUERY_ROW, 5, 1);
        bad_scalar.extend_from_slice(&1_u32.to_le_bytes());
        bad_scalar.push(99);
        assert!(matches!(
            decode_server_frame(&bad_scalar),
            Err(ProtocolError::InvalidScalarTag(99))
        ));

        let mut bad_bool = frame_header(SERVER_QUERY_ROW, 6, 1);
        bad_bool.extend_from_slice(&1_u32.to_le_bytes());
        bad_bool.extend_from_slice(&[1, 2]);
        assert!(matches!(
            decode_server_frame(&bad_bool),
            Err(ProtocolError::InvalidBoolean(2))
        ));

        let mut bad_state = frame_header(SERVER_ERROR, 8, 1);
        bad_state.extend_from_slice(&1_u16.to_le_bytes());
        bad_state.extend_from_slice(&[9, 0]);
        bad_state.extend_from_slice(&0_u32.to_le_bytes());
        assert!(matches!(
            decode_server_frame(&bad_state),
            Err(ProtocolError::InvalidTransactionState(9))
        ));

        let mut bad_error_code = frame_header(SERVER_ERROR, 8, 1);
        bad_error_code.extend_from_slice(&99_u16.to_le_bytes());
        bad_error_code.extend_from_slice(&[0, 0]);
        bad_error_code.extend_from_slice(&0_u32.to_le_bytes());
        assert!(matches!(
            decode_server_frame(&bad_error_code),
            Err(ProtocolError::InvalidErrorCode(99))
        ));

        let mut bad_error_reserved = frame_header(SERVER_ERROR, 8, 1);
        bad_error_reserved.extend_from_slice(&1_u16.to_le_bytes());
        bad_error_reserved.extend_from_slice(&[0, 1]);
        bad_error_reserved.extend_from_slice(&0_u32.to_le_bytes());
        assert!(matches!(
            decode_server_frame(&bad_error_reserved),
            Err(ProtocolError::InvalidReservedBytes)
        ));
    }
}
