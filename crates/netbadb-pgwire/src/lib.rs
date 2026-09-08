//! Bounded PostgreSQL v3 wire codecs and PostgreSQL type adaptation.
//!
//! This crate deliberately has no database, planner, executor, socket-listener,
//! or session policy dependency. It turns untrusted bytes into typed frontend
//! messages and typed backend messages into bytes.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};

use netbadb_types::{Float32Value, Float64Value, PhysicalType, ScalarValue};

pub const PROTOCOL_VERSION_3: u32 = 196_608;
pub const SSL_REQUEST_CODE: u32 = 80_877_103;
pub const CANCEL_REQUEST_CODE: u32 = 80_877_102;
pub const MAX_STARTUP_PACKET_BYTES: usize = 64 * 1024;
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_STRING_BYTES: usize = 1024 * 1024;
pub const MAX_PARAMETERS: usize = 1_024;
pub const MAX_FIELDS: usize = 4_096;

#[derive(Debug)]
pub enum WireError {
    Io(io::Error),
    InvalidLength {
        length: i32,
        context: &'static str,
    },
    MessageTooLarge {
        length: usize,
        limit: usize,
    },
    InvalidUtf8 {
        context: &'static str,
    },
    MissingTerminator {
        context: &'static str,
    },
    UnexpectedTrailingBytes {
        context: &'static str,
    },
    UnsupportedProtocolVersion(u32),
    UnsupportedMessageTag(u8),
    InvalidMessage {
        context: &'static str,
    },
    CountTooLarge {
        context: &'static str,
        count: usize,
        limit: usize,
    },
    IntegerOverflow,
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::InvalidLength { length, context } => {
                write!(formatter, "invalid PostgreSQL {context} length {length}")
            }
            Self::MessageTooLarge { length, limit } => write!(
                formatter,
                "PostgreSQL message length {length} exceeds limit {limit}"
            ),
            Self::InvalidUtf8 { context } => {
                write!(formatter, "PostgreSQL {context} is not valid UTF-8")
            }
            Self::MissingTerminator { context } => {
                write!(
                    formatter,
                    "PostgreSQL {context} is missing its zero terminator"
                )
            }
            Self::UnexpectedTrailingBytes { context } => {
                write!(formatter, "PostgreSQL {context} contains trailing bytes")
            }
            Self::UnsupportedProtocolVersion(version) => {
                write!(
                    formatter,
                    "unsupported PostgreSQL protocol version {version}"
                )
            }
            Self::UnsupportedMessageTag(tag) => {
                write!(
                    formatter,
                    "unsupported PostgreSQL frontend message tag 0x{tag:02x}"
                )
            }
            Self::InvalidMessage { context } => {
                write!(formatter, "invalid PostgreSQL {context}")
            }
            Self::CountTooLarge {
                context,
                count,
                limit,
            } => write!(
                formatter,
                "PostgreSQL {context} count {count} exceeds limit {limit}"
            ),
            Self::IntegerOverflow => formatter.write_str("PostgreSQL wire integer overflow"),
        }
    }
}

impl Error for WireError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for WireError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    Startup(StartupMessage),
    SslRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    pub parameters: BTreeMap<String, String>,
}

impl StartupMessage {
    #[must_use]
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement,
    Portal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseTarget {
    Statement,
    Portal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCode {
    Text,
    Binary,
}

impl FormatCode {
    fn decode(value: i16) -> Result<Self, WireError> {
        match value {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            _ => Err(WireError::InvalidMessage {
                context: "format code",
            }),
        }
    }

    #[must_use]
    pub const fn as_i16(self) -> i16 {
        match self {
            Self::Text => 0,
            Self::Binary => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Query(String),
    Parse {
        statement: String,
        query: String,
        parameter_types: Vec<PostgresOid>,
    },
    Bind {
        portal: String,
        statement: String,
        parameter_formats: Vec<FormatCode>,
        parameters: Vec<Option<Vec<u8>>>,
        result_formats: Vec<FormatCode>,
    },
    Describe {
        target: DescribeTarget,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: u32,
    },
    Sync,
    Close {
        target: CloseTarget,
        name: String,
    },
    Flush,
    Terminate,
    Password(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PostgresOid(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresType {
    Bool,
    Bytea,
    BoolArray,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Varchar,
    Unknown,
    TextArray,
}

impl PostgresType {
    #[must_use]
    pub const fn oid(self) -> PostgresOid {
        PostgresOid(match self {
            Self::Bool => 16,
            Self::Bytea => 17,
            Self::BoolArray => 1_000,
            Self::Int8 => 20,
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Text => 25,
            Self::Float4 => 700,
            Self::Float8 => 701,
            Self::Varchar => 1_043,
            Self::Unknown => 705,
            Self::TextArray => 1_009,
        })
    }

    #[must_use]
    pub const fn type_size(self) -> i16 {
        match self {
            Self::Bool => 1,
            Self::Int2 => 2,
            Self::Int4 | Self::Float4 => 4,
            Self::Int8 | Self::Float8 => 8,
            Self::Bytea
            | Self::Text
            | Self::Varchar
            | Self::Unknown
            | Self::BoolArray
            | Self::TextArray => -1,
        }
    }

    #[must_use]
    pub const fn from_oid(oid: PostgresOid) -> Option<Self> {
        match oid.0 {
            16 => Some(Self::Bool),
            17 => Some(Self::Bytea),
            1_000 => Some(Self::BoolArray),
            20 => Some(Self::Int8),
            21 => Some(Self::Int2),
            23 => Some(Self::Int4),
            25 => Some(Self::Text),
            700 => Some(Self::Float4),
            701 => Some(Self::Float8),
            705 => Some(Self::Unknown),
            1_009 => Some(Self::TextArray),
            1_043 => Some(Self::Varchar),
            _ => None,
        }
    }

    /// NetbaDB type used only when SQL supplies no contextual parameter type.
    ///
    /// This is a transport fallback, not an exact database type declaration.
    #[must_use]
    pub const fn parameter_fallback(self) -> Option<PhysicalType> {
        match self {
            Self::Bool => Some(PhysicalType::Bool),
            Self::Bytea => Some(PhysicalType::Bytes),
            Self::Int2 => Some(PhysicalType::Int16),
            Self::Int4 => Some(PhysicalType::Int32),
            Self::Int8 => Some(PhysicalType::Int64),
            Self::Float4 => Some(PhysicalType::Float32),
            Self::Float8 => Some(PhysicalType::Float64),
            Self::Text | Self::Varchar | Self::Unknown => Some(PhysicalType::Text),
            Self::BoolArray | Self::TextArray => None,
        }
    }

    /// Whether this PostgreSQL type is a valid value carrier for an exact
    /// NetbaDB parameter target. Carrier compatibility does not imply type
    /// identity; integer range checks happen while binding the value.
    #[must_use]
    pub const fn is_parameter_carrier_for(self, target: PhysicalType) -> bool {
        match target {
            PhysicalType::Bool => matches!(self, Self::Bool),
            PhysicalType::Int8
            | PhysicalType::Int16
            | PhysicalType::Int32
            | PhysicalType::Int64
            | PhysicalType::Int128
            | PhysicalType::UInt8
            | PhysicalType::UInt16
            | PhysicalType::UInt32
            | PhysicalType::UInt64
            | PhysicalType::UInt128 => matches!(self, Self::Int2 | Self::Int4 | Self::Int8),
            PhysicalType::Float32 => matches!(self, Self::Float4),
            PhysicalType::Float64 => matches!(self, Self::Float8),
            PhysicalType::Text => matches!(self, Self::Text | Self::Varchar | Self::Unknown),
            PhysicalType::Bytes => matches!(self, Self::Bytea),
        }
    }

    /// Default PostgreSQL carrier for one exact NetbaDB parameter target.
    /// This differs from result-type reflection for unsigned and 128-bit
    /// integers, whose full domains cannot be represented by PostgreSQL.
    pub const fn for_parameter(target: PhysicalType) -> Self {
        match target {
            PhysicalType::Bool => Self::Bool,
            PhysicalType::Int8 | PhysicalType::Int16 | PhysicalType::UInt8 => Self::Int2,
            PhysicalType::Int32 | PhysicalType::UInt16 => Self::Int4,
            PhysicalType::Int64
            | PhysicalType::Int128
            | PhysicalType::UInt32
            | PhysicalType::UInt64
            | PhysicalType::UInt128 => Self::Int8,
            PhysicalType::Float32 => Self::Float4,
            PhysicalType::Float64 => Self::Float8,
            PhysicalType::Text => Self::Text,
            PhysicalType::Bytes => Self::Bytea,
        }
    }

    pub fn from_netbadb(physical: PhysicalType) -> Result<Self, TypeMappingError> {
        match physical {
            PhysicalType::Bool => Ok(Self::Bool),
            PhysicalType::Int8 | PhysicalType::Int16 => Ok(Self::Int2),
            PhysicalType::Int32 => Ok(Self::Int4),
            PhysicalType::Int64 => Ok(Self::Int8),
            PhysicalType::UInt8 => Ok(Self::Int2),
            PhysicalType::UInt16 => Ok(Self::Int4),
            PhysicalType::UInt32 => Ok(Self::Int8),
            PhysicalType::Float32 => Ok(Self::Float4),
            PhysicalType::Float64 => Ok(Self::Float8),
            PhysicalType::Text => Ok(Self::Text),
            PhysicalType::Bytes => Ok(Self::Bytea),
            PhysicalType::UInt64 => Err(TypeMappingError::UnsupportedUInt64),
            PhysicalType::Int128 | PhysicalType::UInt128 => {
                Err(TypeMappingError::UnsupportedPhysicalType(physical))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeMappingError {
    UnsupportedUInt64,
    UnsupportedPhysicalType(PhysicalType),
    UnsupportedOid(PostgresOid),
    InvalidTextValue(PostgresType),
    ValueOutOfRange(PostgresType),
    TypeMismatch,
    BinaryFormatUnsupported(PostgresType),
    InvalidBinaryValue(PostgresType),
}

impl fmt::Display for TypeMappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedUInt64 => {
                formatter.write_str("NetbaDB UINT64 has no lossless PostgreSQL integer mapping")
            }
            Self::UnsupportedPhysicalType(data_type) => {
                write!(
                    formatter,
                    "NetbaDB {data_type} has no lossless PostgreSQL mapping"
                )
            }
            Self::UnsupportedOid(oid) => {
                write!(formatter, "PostgreSQL type OID {} is unsupported", oid.0)
            }
            Self::InvalidTextValue(data_type) => {
                write!(formatter, "invalid text value for PostgreSQL {data_type:?}")
            }
            Self::ValueOutOfRange(data_type) => {
                write!(
                    formatter,
                    "value is out of range for PostgreSQL {data_type:?}"
                )
            }
            Self::TypeMismatch => {
                formatter.write_str("runtime value does not match PostgreSQL field type")
            }
            Self::BinaryFormatUnsupported(data_type) => {
                write!(
                    formatter,
                    "binary format is unsupported for PostgreSQL {data_type:?}"
                )
            }
            Self::InvalidBinaryValue(data_type) => {
                write!(
                    formatter,
                    "invalid binary value for PostgreSQL {data_type:?}"
                )
            }
        }
    }
}

impl Error for TypeMappingError {}

pub fn encode_text_value(
    value: &ScalarValue,
    data_type: PostgresType,
) -> Result<Option<Vec<u8>>, TypeMappingError> {
    match (value, data_type) {
        (ScalarValue::Null, _) => Ok(None),
        (ScalarValue::Bool(value), PostgresType::Bool) => {
            Ok(Some(if *value { b"t".to_vec() } else { b"f".to_vec() }))
        }
        (ScalarValue::Int64(value), PostgresType::Int8) => Ok(Some(value.to_string().into_bytes())),
        (ScalarValue::Int8(value), PostgresType::Int2) => Ok(Some(value.to_string().into_bytes())),
        (ScalarValue::Int16(value), PostgresType::Int2) => Ok(Some(value.to_string().into_bytes())),
        (ScalarValue::Int32(value), PostgresType::Int4) => Ok(Some(value.to_string().into_bytes())),
        (ScalarValue::UInt8(value), PostgresType::Int2) => Ok(Some(value.to_string().into_bytes())),
        (ScalarValue::UInt16(value), PostgresType::Int4) => {
            Ok(Some(value.to_string().into_bytes()))
        }
        (ScalarValue::UInt32(value), PostgresType::Int8) => {
            Ok(Some(value.to_string().into_bytes()))
        }
        (ScalarValue::Float32(value), PostgresType::Float4) => {
            Ok(Some(value.to_string().into_bytes()))
        }
        (ScalarValue::Float64(value), PostgresType::Float8) => {
            Ok(Some(value.to_string().into_bytes()))
        }
        (ScalarValue::Bytes(value), PostgresType::Bytea) => Ok(Some(bytea_text(value))),
        (
            ScalarValue::Text(value),
            PostgresType::Text
            | PostgresType::Varchar
            | PostgresType::BoolArray
            | PostgresType::TextArray,
        ) => Ok(Some(value.as_bytes().to_vec())),
        _ => Err(TypeMappingError::TypeMismatch),
    }
}

pub fn encode_binary_value(
    value: &ScalarValue,
    data_type: PostgresType,
) -> Result<Option<Vec<u8>>, TypeMappingError> {
    match (value, data_type) {
        (ScalarValue::Null, _) => Ok(None),
        (ScalarValue::Bool(value), PostgresType::Bool) => Ok(Some(vec![u8::from(*value)])),
        (ScalarValue::Int64(value), PostgresType::Int8) => Ok(Some(value.to_be_bytes().to_vec())),
        (ScalarValue::Int8(value), PostgresType::Int2) => {
            Ok(Some(i16::from(*value).to_be_bytes().to_vec()))
        }
        (ScalarValue::Int16(value), PostgresType::Int2) => Ok(Some(value.to_be_bytes().to_vec())),
        (ScalarValue::Int32(value), PostgresType::Int4) => Ok(Some(value.to_be_bytes().to_vec())),
        (ScalarValue::UInt8(value), PostgresType::Int2) => {
            Ok(Some(i16::from(*value).to_be_bytes().to_vec()))
        }
        (ScalarValue::UInt16(value), PostgresType::Int4) => {
            Ok(Some(i32::from(*value).to_be_bytes().to_vec()))
        }
        (ScalarValue::UInt32(value), PostgresType::Int8) => {
            Ok(Some(i64::from(*value).to_be_bytes().to_vec()))
        }
        (ScalarValue::Float32(value), PostgresType::Float4) => {
            Ok(Some(value.to_bits().to_be_bytes().to_vec()))
        }
        (ScalarValue::Float64(value), PostgresType::Float8) => {
            Ok(Some(value.to_bits().to_be_bytes().to_vec()))
        }
        (ScalarValue::Bytes(value), PostgresType::Bytea) => Ok(Some(value.clone())),
        (ScalarValue::Text(value), PostgresType::Text | PostgresType::Varchar) => {
            Ok(Some(value.as_bytes().to_vec()))
        }
        (
            _,
            PostgresType::Int2
            | PostgresType::Int4
            | PostgresType::Float4
            | PostgresType::Float8
            | PostgresType::Bytea
            | PostgresType::Unknown
            | PostgresType::BoolArray
            | PostgresType::TextArray,
        ) => Err(TypeMappingError::BinaryFormatUnsupported(data_type)),
        _ => Err(TypeMappingError::TypeMismatch),
    }
}

pub fn decode_text_parameter(
    bytes: Option<&[u8]>,
    oid: PostgresOid,
) -> Result<ScalarValue, TypeMappingError> {
    let Some(bytes) = bytes else {
        return Ok(ScalarValue::Null);
    };
    let data_type = PostgresType::from_oid(oid).ok_or(TypeMappingError::UnsupportedOid(oid))?;
    let value =
        std::str::from_utf8(bytes).map_err(|_| TypeMappingError::InvalidTextValue(data_type))?;
    match data_type {
        PostgresType::Bool => match value {
            "t" | "true" | "TRUE" | "1" => Ok(ScalarValue::Bool(true)),
            "f" | "false" | "FALSE" | "0" => Ok(ScalarValue::Bool(false)),
            _ => Err(TypeMappingError::InvalidTextValue(data_type)),
        },
        PostgresType::Int2 => value
            .parse::<i16>()
            .map(|value| ScalarValue::Int64(i64::from(value)))
            .map_err(|error| integer_text_error(data_type, &error)),
        PostgresType::Int4 => value
            .parse::<i32>()
            .map(|value| ScalarValue::Int64(i64::from(value)))
            .map_err(|error| integer_text_error(data_type, &error)),
        PostgresType::Int8 => value
            .parse::<i64>()
            .map(ScalarValue::Int64)
            .map_err(|error| integer_text_error(data_type, &error)),
        PostgresType::Float4 => value
            .parse::<f32>()
            .map(|value| ScalarValue::Float32(Float32Value::new(value)))
            .map_err(|_| TypeMappingError::InvalidTextValue(data_type)),
        PostgresType::Float8 => value
            .parse::<f64>()
            .map(|value| ScalarValue::Float64(Float64Value::new(value)))
            .map_err(|_| TypeMappingError::InvalidTextValue(data_type)),
        PostgresType::Bytea => decode_bytea_text(value).map(ScalarValue::Bytes),
        PostgresType::Text | PostgresType::Varchar | PostgresType::Unknown => {
            Ok(ScalarValue::Text(value.to_owned()))
        }
        PostgresType::BoolArray | PostgresType::TextArray => {
            Err(TypeMappingError::InvalidTextValue(data_type))
        }
    }
}

pub fn decode_text_parameter_as(
    bytes: Option<&[u8]>,
    oid: PostgresOid,
    expected: PhysicalType,
) -> Result<ScalarValue, TypeMappingError> {
    coerce_parameter(decode_text_parameter(bytes, oid)?, expected)
}

fn integer_text_error(
    data_type: PostgresType,
    error: &std::num::ParseIntError,
) -> TypeMappingError {
    match error.kind() {
        std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
            TypeMappingError::ValueOutOfRange(data_type)
        }
        _ => TypeMappingError::InvalidTextValue(data_type),
    }
}

pub fn decode_binary_parameter(
    bytes: Option<&[u8]>,
    oid: PostgresOid,
) -> Result<ScalarValue, TypeMappingError> {
    let Some(bytes) = bytes else {
        return Ok(ScalarValue::Null);
    };
    let data_type = PostgresType::from_oid(oid).ok_or(TypeMappingError::UnsupportedOid(oid))?;
    match data_type {
        PostgresType::Bool if bytes.len() == 1 && bytes[0] <= 1 => {
            Ok(ScalarValue::Bool(bytes[0] == 1))
        }
        PostgresType::Int2 if bytes.len() == 2 => {
            Ok(ScalarValue::Int64(i64::from(i16::from_be_bytes([
                bytes[0], bytes[1],
            ]))))
        }
        PostgresType::Int4 if bytes.len() == 4 => {
            Ok(ScalarValue::Int64(i64::from(i32::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))))
        }
        PostgresType::Int8 if bytes.len() == 8 => Ok(ScalarValue::Int64(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))),
        PostgresType::Float4 if bytes.len() == 4 => Ok(ScalarValue::Float32(
            Float32Value::from_bits(u32::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| TypeMappingError::InvalidBinaryValue(data_type))?,
            )),
        )),
        PostgresType::Float8 if bytes.len() == 8 => Ok(ScalarValue::Float64(
            Float64Value::from_bits(u64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| TypeMappingError::InvalidBinaryValue(data_type))?,
            )),
        )),
        PostgresType::Bytea => Ok(ScalarValue::Bytes(bytes.to_vec())),
        PostgresType::Text | PostgresType::Varchar => std::str::from_utf8(bytes)
            .map(|value| ScalarValue::Text(value.to_owned()))
            .map_err(|_| TypeMappingError::InvalidBinaryValue(data_type)),
        PostgresType::Unknown | PostgresType::BoolArray | PostgresType::TextArray => {
            Err(TypeMappingError::BinaryFormatUnsupported(data_type))
        }
        _ => Err(TypeMappingError::InvalidBinaryValue(data_type)),
    }
}

pub fn decode_binary_parameter_as(
    bytes: Option<&[u8]>,
    oid: PostgresOid,
    expected: PhysicalType,
) -> Result<ScalarValue, TypeMappingError> {
    coerce_parameter(decode_binary_parameter(bytes, oid)?, expected)
}

fn coerce_parameter(
    value: ScalarValue,
    expected: PhysicalType,
) -> Result<ScalarValue, TypeMappingError> {
    if matches!(value, ScalarValue::Null) {
        return Ok(value);
    }
    if value.physical_type() == Some(expected) {
        return Ok(value);
    }
    let integer = match value {
        ScalarValue::Int64(value) => value,
        _ => return Err(TypeMappingError::TypeMismatch),
    };
    let range_type = match expected {
        PhysicalType::Int8 | PhysicalType::Int16 | PhysicalType::UInt8 => PostgresType::Int2,
        PhysicalType::Int32 | PhysicalType::UInt16 => PostgresType::Int4,
        PhysicalType::Int64
        | PhysicalType::Int128
        | PhysicalType::UInt32
        | PhysicalType::UInt64
        | PhysicalType::UInt128 => PostgresType::Int8,
        _ => return Err(TypeMappingError::TypeMismatch),
    };
    macro_rules! checked {
        ($kind:ident, $type:ty) => {
            <$type>::try_from(integer)
                .map(ScalarValue::$kind)
                .map_err(|_| TypeMappingError::ValueOutOfRange(range_type))
        };
    }
    match expected {
        PhysicalType::Int8 => checked!(Int8, i8),
        PhysicalType::Int16 => checked!(Int16, i16),
        PhysicalType::Int32 => checked!(Int32, i32),
        PhysicalType::Int64 => Ok(ScalarValue::Int64(integer)),
        PhysicalType::Int128 => Ok(ScalarValue::Int128(i128::from(integer))),
        PhysicalType::UInt8 => checked!(UInt8, u8),
        PhysicalType::UInt16 => checked!(UInt16, u16),
        PhysicalType::UInt32 => checked!(UInt32, u32),
        PhysicalType::UInt64 => checked!(UInt64, u64),
        PhysicalType::UInt128 => checked!(UInt128, u128),
        PhysicalType::Bool
        | PhysicalType::Float32
        | PhysicalType::Float64
        | PhysicalType::Text
        | PhysicalType::Bytes => Err(TypeMappingError::TypeMismatch),
    }
}

fn bytea_text(value: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = Vec::with_capacity(2 + value.len() * 2);
    output.extend_from_slice(b"\\x");
    for byte in value {
        output.push(HEX[usize::from(byte >> 4)]);
        output.push(HEX[usize::from(byte & 0x0f)]);
    }
    output
}

fn decode_bytea_text(value: &str) -> Result<Vec<u8>, TypeMappingError> {
    let hex = value
        .strip_prefix("\\x")
        .ok_or(TypeMappingError::InvalidTextValue(PostgresType::Bytea))?;
    if hex.len() % 2 != 0 {
        return Err(TypeMappingError::InvalidTextValue(PostgresType::Bytea));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])
                .ok_or(TypeMappingError::InvalidTextValue(PostgresType::Bytea))?;
            let low = hex_digit(pair[1])
                .ok_or(TypeMappingError::InvalidTextValue(PostgresType::Bytea))?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_attribute: i16,
    pub data_type: PostgresType,
    pub type_modifier: i32,
    pub format: FormatCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    AuthenticationOk,
    ParameterStatus { name: String, value: String },
    BackendKeyData { process_id: i32, secret_key: i32 },
    ReadyForQuery(u8),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    EmptyQueryResponse,
    ErrorResponse(ErrorResponse),
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    ParameterDescription(Vec<PostgresOid>),
    PortalSuspended,
}

pub fn read_startup_packet(reader: &mut impl Read) -> Result<Option<StartupPacket>, WireError> {
    let mut length_bytes = [0_u8; 4];
    if !read_exact_or_eof(reader, &mut length_bytes)? {
        return Ok(None);
    }
    let length = i32::from_be_bytes(length_bytes);
    let payload_length = checked_payload_length(length, 4, "startup packet")?;
    if usize::try_from(length).map_err(|_| WireError::IntegerOverflow)? > MAX_STARTUP_PACKET_BYTES {
        return Err(WireError::MessageTooLarge {
            length: usize::try_from(length).map_err(|_| WireError::IntegerOverflow)?,
            limit: MAX_STARTUP_PACKET_BYTES,
        });
    }
    let mut payload = vec![0_u8; payload_length];
    reader.read_exact(&mut payload)?;
    let mut cursor = Cursor::new(&payload);
    let code = cursor.read_u32()?;
    match code {
        SSL_REQUEST_CODE => {
            cursor.finish("SSLRequest")?;
            Ok(Some(StartupPacket::SslRequest))
        }
        CANCEL_REQUEST_CODE => {
            let process_id = cursor.read_i32()?;
            let secret_key = cursor.read_i32()?;
            cursor.finish("CancelRequest")?;
            Ok(Some(StartupPacket::CancelRequest {
                process_id,
                secret_key,
            }))
        }
        PROTOCOL_VERSION_3 => {
            let mut parameters = BTreeMap::new();
            loop {
                let key = cursor.read_cstring("startup parameter name")?;
                if key.is_empty() {
                    break;
                }
                let value = cursor.read_cstring("startup parameter value")?;
                parameters.insert(key, value);
            }
            cursor.finish("StartupMessage")?;
            Ok(Some(StartupPacket::Startup(StartupMessage { parameters })))
        }
        version => Err(WireError::UnsupportedProtocolVersion(version)),
    }
}

pub fn read_frontend_message(reader: &mut impl Read) -> Result<Option<FrontendMessage>, WireError> {
    let mut tag = [0_u8; 1];
    if !read_exact_or_eof(reader, &mut tag)? {
        return Ok(None);
    }
    let length = read_i32(reader)?;
    let payload_length = checked_payload_length(length, 4, "frontend message")?;
    if payload_length > MAX_MESSAGE_BYTES {
        return Err(WireError::MessageTooLarge {
            length: payload_length,
            limit: MAX_MESSAGE_BYTES,
        });
    }
    let mut payload = vec![0_u8; payload_length];
    reader.read_exact(&mut payload)?;
    decode_frontend_message(tag[0], &payload).map(Some)
}

fn decode_frontend_message(tag: u8, payload: &[u8]) -> Result<FrontendMessage, WireError> {
    let mut cursor = Cursor::new(payload);
    let message = match tag {
        b'Q' => FrontendMessage::Query(cursor.read_cstring("Query string")?),
        b'P' => {
            let statement = cursor.read_cstring("prepared statement name")?;
            let query = cursor.read_cstring("prepared query")?;
            let count = cursor.read_count("parameter type", MAX_PARAMETERS)?;
            let mut parameter_types = Vec::with_capacity(count);
            for _ in 0..count {
                parameter_types.push(PostgresOid(cursor.read_u32()?));
            }
            FrontendMessage::Parse {
                statement,
                query,
                parameter_types,
            }
        }
        b'B' => {
            let portal = cursor.read_cstring("portal name")?;
            let statement = cursor.read_cstring("bound statement name")?;
            let format_count = cursor.read_count("parameter format", MAX_PARAMETERS)?;
            let mut parameter_formats = Vec::with_capacity(format_count);
            for _ in 0..format_count {
                parameter_formats.push(FormatCode::decode(cursor.read_i16()?)?);
            }
            let parameter_count = cursor.read_count("parameter", MAX_PARAMETERS)?;
            let mut parameters = Vec::with_capacity(parameter_count);
            for _ in 0..parameter_count {
                let length = cursor.read_i32()?;
                if length == -1 {
                    parameters.push(None);
                } else {
                    let length = usize::try_from(length).map_err(|_| WireError::InvalidLength {
                        length,
                        context: "Bind parameter",
                    })?;
                    if length > MAX_MESSAGE_BYTES {
                        return Err(WireError::MessageTooLarge {
                            length,
                            limit: MAX_MESSAGE_BYTES,
                        });
                    }
                    parameters.push(Some(cursor.read_bytes(length)?.to_vec()));
                }
            }
            let result_count = cursor.read_count("result format", MAX_FIELDS)?;
            let mut result_formats = Vec::with_capacity(result_count);
            for _ in 0..result_count {
                result_formats.push(FormatCode::decode(cursor.read_i16()?)?);
            }
            FrontendMessage::Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            }
        }
        b'D' => {
            let target = match cursor.read_u8()? {
                b'S' => DescribeTarget::Statement,
                b'P' => DescribeTarget::Portal,
                _ => {
                    return Err(WireError::InvalidMessage {
                        context: "Describe target",
                    });
                }
            };
            FrontendMessage::Describe {
                target,
                name: cursor.read_cstring("Describe name")?,
            }
        }
        b'E' => {
            let portal = cursor.read_cstring("Execute portal")?;
            let max_rows = cursor.read_u32()?;
            FrontendMessage::Execute { portal, max_rows }
        }
        b'S' => FrontendMessage::Sync,
        b'C' => {
            let target = match cursor.read_u8()? {
                b'S' => CloseTarget::Statement,
                b'P' => CloseTarget::Portal,
                _ => {
                    return Err(WireError::InvalidMessage {
                        context: "Close target",
                    });
                }
            };
            FrontendMessage::Close {
                target,
                name: cursor.read_cstring("Close name")?,
            }
        }
        b'H' => FrontendMessage::Flush,
        b'X' => FrontendMessage::Terminate,
        b'p' => FrontendMessage::Password(payload.to_vec()),
        _ => return Err(WireError::UnsupportedMessageTag(tag)),
    };
    cursor.finish("frontend message")?;
    Ok(message)
}

pub fn write_backend_message(
    writer: &mut impl Write,
    message: &BackendMessage,
) -> Result<(), WireError> {
    let mut payload = Vec::new();
    let tag = match message {
        BackendMessage::AuthenticationOk => {
            push_i32(&mut payload, 0);
            b'R'
        }
        BackendMessage::ParameterStatus { name, value } => {
            push_cstring(&mut payload, name)?;
            push_cstring(&mut payload, value)?;
            b'S'
        }
        BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        } => {
            push_i32(&mut payload, *process_id);
            push_i32(&mut payload, *secret_key);
            b'K'
        }
        BackendMessage::ReadyForQuery(status) => {
            payload.push(*status);
            b'Z'
        }
        BackendMessage::RowDescription(fields) => {
            push_count(&mut payload, fields.len(), "row field", MAX_FIELDS)?;
            for field in fields {
                push_cstring(&mut payload, &field.name)?;
                payload.extend_from_slice(&field.table_oid.to_be_bytes());
                payload.extend_from_slice(&field.column_attribute.to_be_bytes());
                payload.extend_from_slice(&field.data_type.oid().0.to_be_bytes());
                payload.extend_from_slice(&field.data_type.type_size().to_be_bytes());
                payload.extend_from_slice(&field.type_modifier.to_be_bytes());
                payload.extend_from_slice(&field.format.as_i16().to_be_bytes());
            }
            b'T'
        }
        BackendMessage::DataRow(values) => {
            push_count(&mut payload, values.len(), "data row field", MAX_FIELDS)?;
            for value in values {
                match value {
                    None => push_i32(&mut payload, -1),
                    Some(value) => {
                        let length =
                            i32::try_from(value.len()).map_err(|_| WireError::IntegerOverflow)?;
                        push_i32(&mut payload, length);
                        payload.extend_from_slice(value);
                    }
                }
            }
            b'D'
        }
        BackendMessage::CommandComplete(tag) => {
            push_cstring(&mut payload, tag)?;
            b'C'
        }
        BackendMessage::EmptyQueryResponse => b'I',
        BackendMessage::ErrorResponse(error) => {
            push_error_field(&mut payload, b'S', error.severity)?;
            push_error_field(&mut payload, b'V', error.severity)?;
            push_error_field(&mut payload, b'C', error.sqlstate)?;
            push_error_field(&mut payload, b'M', &error.message)?;
            if let Some(detail) = &error.detail {
                push_error_field(&mut payload, b'D', detail)?;
            }
            if let Some(hint) = &error.hint {
                push_error_field(&mut payload, b'H', hint)?;
            }
            if let Some(position) = error.position {
                push_error_field(&mut payload, b'P', &position.to_string())?;
            }
            payload.push(0);
            b'E'
        }
        BackendMessage::ParseComplete => b'1',
        BackendMessage::BindComplete => b'2',
        BackendMessage::CloseComplete => b'3',
        BackendMessage::NoData => b'n',
        BackendMessage::ParameterDescription(parameters) => {
            push_count(&mut payload, parameters.len(), "parameter", MAX_PARAMETERS)?;
            for oid in parameters {
                payload.extend_from_slice(&oid.0.to_be_bytes());
            }
            b't'
        }
        BackendMessage::PortalSuspended => b's',
    };
    let length = payload
        .len()
        .checked_add(4)
        .ok_or(WireError::IntegerOverflow)?;
    if length > MAX_MESSAGE_BYTES {
        return Err(WireError::MessageTooLarge {
            length,
            limit: MAX_MESSAGE_BYTES,
        });
    }
    writer.write_all(&[tag])?;
    writer.write_all(
        &i32::try_from(length)
            .map_err(|_| WireError::IntegerOverflow)?
            .to_be_bytes(),
    )?;
    writer.write_all(&payload)?;
    Ok(())
}

fn read_exact_or_eof(reader: &mut impl Read, bytes: &mut [u8]) -> Result<bool, io::Error> {
    let mut read = 0;
    while read < bytes.len() {
        match reader.read(&mut bytes[read..])? {
            0 if read == 0 => return Ok(false),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated PostgreSQL frame",
                ));
            }
            count => read += count,
        }
    }
    Ok(true)
}

fn read_i32(reader: &mut impl Read) -> Result<i32, WireError> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_be_bytes(bytes))
}

fn checked_payload_length(
    length: i32,
    header: usize,
    context: &'static str,
) -> Result<usize, WireError> {
    let length =
        usize::try_from(length).map_err(|_| WireError::InvalidLength { length, context })?;
    length.checked_sub(header).ok_or(WireError::InvalidLength {
        length: i32::try_from(length).unwrap_or(i32::MAX),
        context,
    })
}

fn push_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_count(
    output: &mut Vec<u8>,
    count: usize,
    context: &'static str,
    limit: usize,
) -> Result<(), WireError> {
    if count > limit {
        return Err(WireError::CountTooLarge {
            context,
            count,
            limit,
        });
    }
    let count = i16::try_from(count).map_err(|_| WireError::IntegerOverflow)?;
    output.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn push_cstring(output: &mut Vec<u8>, value: &str) -> Result<(), WireError> {
    if value.len() > MAX_STRING_BYTES || value.as_bytes().contains(&0) {
        return Err(WireError::InvalidMessage {
            context: "zero-terminated string",
        });
    }
    output.extend_from_slice(value.as_bytes());
    output.push(0);
    Ok(())
}

fn push_error_field(output: &mut Vec<u8>, tag: u8, value: &str) -> Result<(), WireError> {
    output.push(tag);
    push_cstring(output, value)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_bytes(&mut self, length: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(WireError::IntegerOverflow)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(WireError::InvalidMessage {
                context: "truncated message body",
            })?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, WireError> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_i16(&mut self) -> Result<i16, WireError> {
        let bytes: [u8; 2] = self
            .read_bytes(2)?
            .try_into()
            .map_err(|_| WireError::InvalidMessage { context: "i16" })?;
        Ok(i16::from_be_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32, WireError> {
        let bytes: [u8; 4] = self
            .read_bytes(4)?
            .try_into()
            .map_err(|_| WireError::InvalidMessage { context: "i32" })?;
        Ok(i32::from_be_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, WireError> {
        let bytes: [u8; 4] = self
            .read_bytes(4)?
            .try_into()
            .map_err(|_| WireError::InvalidMessage { context: "u32" })?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_count(&mut self, context: &'static str, limit: usize) -> Result<usize, WireError> {
        let count = self.read_i16()?;
        let count = usize::try_from(count).map_err(|_| WireError::InvalidMessage { context })?;
        if count > limit {
            return Err(WireError::CountTooLarge {
                context,
                count,
                limit,
            });
        }
        Ok(count)
    }

    fn read_cstring(&mut self, context: &'static str) -> Result<String, WireError> {
        let remaining = self
            .bytes
            .get(self.position..)
            .ok_or(WireError::InvalidMessage { context })?;
        let terminator = remaining
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(WireError::MissingTerminator { context })?;
        if terminator > MAX_STRING_BYTES {
            return Err(WireError::MessageTooLarge {
                length: terminator,
                limit: MAX_STRING_BYTES,
            });
        }
        let bytes = &remaining[..terminator];
        self.position = self
            .position
            .checked_add(terminator + 1)
            .ok_or(WireError::IntegerOverflow)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| WireError::InvalidUtf8 { context })
    }

    fn finish(&self, context: &'static str) -> Result<(), WireError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(WireError::UnexpectedTrailingBytes { context })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontend(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&i32::try_from(payload.len() + 4).unwrap().to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn decodes_startup_ssl_cancel_and_parameters() {
        let mut startup = Vec::new();
        startup.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
        startup.extend_from_slice(b"user\0netbadb\0database\0test\0\0");
        let mut frame = Vec::new();
        frame.extend_from_slice(&i32::try_from(startup.len() + 4).unwrap().to_be_bytes());
        frame.extend_from_slice(&startup);
        let StartupPacket::Startup(message) =
            read_startup_packet(&mut frame.as_slice()).unwrap().unwrap()
        else {
            panic!("expected startup");
        };
        assert_eq!(message.parameter("user"), Some("netbadb"));
        assert_eq!(message.parameter("database"), Some("test"));

        let mut ssl = 8_i32.to_be_bytes().to_vec();
        ssl.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        assert_eq!(
            read_startup_packet(&mut ssl.as_slice()).unwrap(),
            Some(StartupPacket::SslRequest)
        );

        let mut cancel = 16_i32.to_be_bytes().to_vec();
        cancel.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        cancel.extend_from_slice(&7_i32.to_be_bytes());
        cancel.extend_from_slice(&9_i32.to_be_bytes());
        assert_eq!(
            read_startup_packet(&mut cancel.as_slice()).unwrap(),
            Some(StartupPacket::CancelRequest {
                process_id: 7,
                secret_key: 9
            })
        );
    }

    #[test]
    fn rejects_invalid_and_unbounded_lengths() {
        assert!(matches!(
            read_startup_packet(&mut 3_i32.to_be_bytes().as_slice()),
            Err(WireError::InvalidLength { .. })
        ));
        let bytes = frontend(b'Q', b"unterminated");
        assert!(matches!(
            read_frontend_message(&mut bytes.as_slice()),
            Err(WireError::MissingTerminator { .. })
        ));
        let mut oversized = vec![b'Q'];
        oversized.extend_from_slice(&i32::try_from(MAX_MESSAGE_BYTES + 5).unwrap().to_be_bytes());
        assert!(matches!(
            read_frontend_message(&mut oversized.as_slice()),
            Err(WireError::MessageTooLarge { .. })
        ));
    }

    #[test]
    fn decodes_extended_query_lifecycle() {
        let mut parse = b"named\0SELECT id FROM users WHERE id = 1\0".to_vec();
        parse.extend_from_slice(&1_i16.to_be_bytes());
        parse.extend_from_slice(&20_u32.to_be_bytes());
        let message = read_frontend_message(&mut frontend(b'P', &parse).as_slice())
            .unwrap()
            .unwrap();
        assert!(
            matches!(message, FrontendMessage::Parse { statement, parameter_types, .. } if statement == "named" && parameter_types == [PostgresOid(20)])
        );

        let mut bind = b"portal\0named\0".to_vec();
        bind.extend_from_slice(&0_i16.to_be_bytes());
        bind.extend_from_slice(&1_i16.to_be_bytes());
        bind.extend_from_slice(&1_i32.to_be_bytes());
        bind.push(b'7');
        bind.extend_from_slice(&0_i16.to_be_bytes());
        let message = read_frontend_message(&mut frontend(b'B', &bind).as_slice())
            .unwrap()
            .unwrap();
        assert!(
            matches!(message, FrontendMessage::Bind { portal, parameters, .. } if portal == "portal" && parameters == [Some(vec![b'7'])])
        );
    }

    #[test]
    fn encodes_rows_null_and_pg_text_scalars() {
        assert_eq!(
            encode_text_value(&ScalarValue::Null, PostgresType::Text).unwrap(),
            None
        );
        assert_eq!(
            encode_text_value(&ScalarValue::Bool(true), PostgresType::Bool).unwrap(),
            Some(b"t".to_vec())
        );
        assert_eq!(
            encode_text_value(&ScalarValue::Int64(-7), PostgresType::Int8).unwrap(),
            Some(b"-7".to_vec())
        );
        assert_eq!(
            encode_text_value(&ScalarValue::Text("Ada".into()), PostgresType::Text).unwrap(),
            Some(b"Ada".to_vec())
        );

        let message = BackendMessage::DataRow(vec![Some(b"1".to_vec()), None]);
        let mut bytes = Vec::new();
        write_backend_message(&mut bytes, &message).unwrap();
        assert_eq!(&bytes[..5], &[b'D', 0, 0, 0, 15]);
        assert_eq!(&bytes[5..7], &[0, 2]);
        assert_eq!(&bytes[12..], &[255, 255, 255, 255]);
    }

    #[test]
    fn type_mapping_is_centralized_and_lossless() {
        assert_eq!(
            PostgresType::from_netbadb(PhysicalType::Bool).unwrap(),
            PostgresType::Bool
        );
        assert_eq!(
            PostgresType::from_netbadb(PhysicalType::Int64).unwrap(),
            PostgresType::Int8
        );
        assert_eq!(
            PostgresType::from_netbadb(PhysicalType::Text).unwrap(),
            PostgresType::Text
        );
        assert_eq!(
            PostgresType::from_netbadb(PhysicalType::UInt64),
            Err(TypeMappingError::UnsupportedUInt64)
        );
        assert_eq!(
            PostgresType::from_oid(PostgresOid(1_009)),
            Some(PostgresType::TextArray)
        );
        assert_eq!(PostgresType::TextArray.parameter_fallback(), None);
        assert_eq!(
            encode_text_value(&ScalarValue::Text("{id}".into()), PostgresType::TextArray).unwrap(),
            Some(b"{id}".to_vec())
        );
        assert!(matches!(
            decode_text_parameter(Some(b"{id}"), PostgresType::TextArray.oid()),
            Err(TypeMappingError::InvalidTextValue(PostgresType::TextArray))
        ));
        assert_eq!(
            PostgresType::from_oid(PostgresOid(1_000)),
            Some(PostgresType::BoolArray)
        );
        assert_eq!(PostgresType::BoolArray.parameter_fallback(), None);
        assert_eq!(
            encode_text_value(&ScalarValue::Text("{t,f}".into()), PostgresType::BoolArray).unwrap(),
            Some(b"{t,f}".to_vec())
        );
        assert!(matches!(
            decode_text_parameter(Some(b"{t,f}"), PostgresType::BoolArray.oid()),
            Err(TypeMappingError::InvalidTextValue(PostgresType::BoolArray))
        ));
        assert!(matches!(
            encode_binary_value(&ScalarValue::Text("{t,f}".into()), PostgresType::BoolArray),
            Err(TypeMappingError::BinaryFormatUnsupported(
                PostgresType::BoolArray
            ))
        ));
        assert_eq!(
            decode_text_parameter(Some(b"32767"), PostgresOid(21)).unwrap(),
            ScalarValue::Int64(32767)
        );
        assert_eq!(
            decode_text_parameter(Some(b"true"), PostgresType::Bool.oid()).unwrap(),
            ScalarValue::Bool(true)
        );
        assert_eq!(
            decode_text_parameter(Some(b"-2147483648"), PostgresType::Int4.oid()).unwrap(),
            ScalarValue::Int64(i64::from(i32::MIN))
        );
        assert_eq!(
            decode_text_parameter(Some(b"9223372036854775807"), PostgresType::Int8.oid()).unwrap(),
            ScalarValue::Int64(i64::MAX)
        );
        assert_eq!(
            decode_text_parameter(Some("你好".as_bytes()), PostgresType::Text.oid()).unwrap(),
            ScalarValue::Text("你好".into())
        );
        assert_eq!(
            decode_text_parameter(Some(b"varchar"), PostgresType::Varchar.oid()).unwrap(),
            ScalarValue::Text("varchar".into())
        );
        assert_eq!(
            decode_text_parameter(None, PostgresType::Int8.oid()).unwrap(),
            ScalarValue::Null
        );
        assert!(matches!(
            decode_text_parameter(Some(b"32768"), PostgresOid(21)),
            Err(TypeMappingError::ValueOutOfRange(PostgresType::Int2))
        ));
        assert!(matches!(
            decode_text_parameter(Some(b"1"), PostgresOid(999_999)),
            Err(TypeMappingError::UnsupportedOid(PostgresOid(999_999)))
        ));
    }

    #[test]
    fn decodes_and_encodes_base_binary_values_with_exact_widths() {
        assert_eq!(
            decode_binary_parameter(Some(&[1]), PostgresType::Bool.oid()).unwrap(),
            ScalarValue::Bool(true)
        );
        assert_eq!(
            decode_binary_parameter(Some(&(-7_i16).to_be_bytes()), PostgresType::Int2.oid())
                .unwrap(),
            ScalarValue::Int64(-7)
        );
        assert_eq!(
            decode_binary_parameter(Some(&42_i32.to_be_bytes()), PostgresType::Int4.oid()).unwrap(),
            ScalarValue::Int64(42)
        );
        assert_eq!(
            decode_binary_parameter(Some(&99_i64.to_be_bytes()), PostgresType::Int8.oid()).unwrap(),
            ScalarValue::Int64(99)
        );
        assert_eq!(
            decode_binary_parameter(Some(b"Ada"), PostgresType::Text.oid()).unwrap(),
            ScalarValue::Text("Ada".into())
        );
        assert_eq!(
            decode_binary_parameter(Some(b"Lin"), PostgresType::Varchar.oid()).unwrap(),
            ScalarValue::Text("Lin".into())
        );
        assert_eq!(
            decode_binary_parameter(None, PostgresType::Bool.oid()).unwrap(),
            ScalarValue::Null
        );
        assert!(matches!(
            decode_binary_parameter(Some(&[0, 1]), PostgresType::Bool.oid()),
            Err(TypeMappingError::InvalidBinaryValue(PostgresType::Bool))
        ));
        assert!(matches!(
            decode_binary_parameter(Some(&[0; 3]), PostgresType::Int4.oid()),
            Err(TypeMappingError::InvalidBinaryValue(PostgresType::Int4))
        ));
        assert_eq!(
            encode_binary_value(&ScalarValue::Int64(7), PostgresType::Int8).unwrap(),
            Some(7_i64.to_be_bytes().to_vec())
        );
        assert_eq!(
            encode_binary_value(&ScalarValue::Null, PostgresType::Text).unwrap(),
            None
        );
    }

    #[test]
    fn extended_lossless_mappings_parameters_and_results_are_explicit() {
        for (physical, postgres) in [
            (PhysicalType::Int8, PostgresType::Int2),
            (PhysicalType::Int16, PostgresType::Int2),
            (PhysicalType::Int32, PostgresType::Int4),
            (PhysicalType::Int64, PostgresType::Int8),
            (PhysicalType::UInt8, PostgresType::Int2),
            (PhysicalType::UInt16, PostgresType::Int4),
            (PhysicalType::UInt32, PostgresType::Int8),
            (PhysicalType::Float32, PostgresType::Float4),
            (PhysicalType::Float64, PostgresType::Float8),
            (PhysicalType::Bytes, PostgresType::Bytea),
        ] {
            assert_eq!(PostgresType::from_netbadb(physical).unwrap(), postgres);
            assert_eq!(PostgresType::from_oid(postgres.oid()), Some(postgres));
        }
        for physical in [
            PhysicalType::UInt64,
            PhysicalType::Int128,
            PhysicalType::UInt128,
        ] {
            assert!(PostgresType::from_netbadb(physical).is_err());
        }

        assert_eq!(
            decode_text_parameter_as(Some(b"255"), PostgresType::Int2.oid(), PhysicalType::UInt8)
                .unwrap(),
            ScalarValue::UInt8(u8::MAX)
        );
        assert_eq!(
            decode_binary_parameter_as(
                Some(&i64::MAX.to_be_bytes()),
                PostgresType::Int8.oid(),
                PhysicalType::Int128,
            )
            .unwrap(),
            ScalarValue::Int128(i128::from(i64::MAX))
        );
        assert_eq!(
            decode_text_parameter_as(
                Some(b"9223372036854775807"),
                PostgresType::Int8.oid(),
                PhysicalType::UInt128,
            )
            .unwrap(),
            ScalarValue::UInt128(i64::MAX as u128)
        );
        assert!(matches!(
            decode_text_parameter_as(Some(b"-1"), PostgresType::Int2.oid(), PhysicalType::UInt8),
            Err(TypeMappingError::ValueOutOfRange(PostgresType::Int2))
        ));

        let bytes = ScalarValue::Bytes(vec![0, 0xff, 0x80]);
        assert_eq!(
            encode_text_value(&bytes, PostgresType::Bytea).unwrap(),
            Some(b"\\x00ff80".to_vec())
        );
        assert_eq!(
            decode_text_parameter(Some(b"\\x00ff80"), PostgresType::Bytea.oid()).unwrap(),
            bytes
        );
        assert_eq!(
            encode_binary_value(&bytes, PostgresType::Bytea).unwrap(),
            Some(vec![0, 0xff, 0x80])
        );

        let nan = ScalarValue::Float32(Float32Value::from_bits(0xffff_ffff));
        assert_eq!(
            encode_binary_value(&nan, PostgresType::Float4).unwrap(),
            Some(Float32Value::CANONICAL_NAN_BITS.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn integer_parameter_narrowing_checks_every_boundary_in_text_and_binary() {
        fn binary(carrier: PostgresType, value: i64) -> Vec<u8> {
            match carrier {
                PostgresType::Int2 => i16::try_from(value).unwrap().to_be_bytes().to_vec(),
                PostgresType::Int4 => i32::try_from(value).unwrap().to_be_bytes().to_vec(),
                PostgresType::Int8 => value.to_be_bytes().to_vec(),
                _ => panic!("test carrier must be an integer"),
            }
        }

        let cases = [
            (
                PhysicalType::Int8,
                PostgresType::Int2,
                i64::from(i8::MIN),
                i64::from(i8::MAX),
                i64::from(i8::MIN) - 1,
                i64::from(i8::MAX) + 1,
                ScalarValue::Int8(i8::MIN),
                ScalarValue::Int8(i8::MAX),
            ),
            (
                PhysicalType::Int16,
                PostgresType::Int4,
                i64::from(i16::MIN),
                i64::from(i16::MAX),
                i64::from(i16::MIN) - 1,
                i64::from(i16::MAX) + 1,
                ScalarValue::Int16(i16::MIN),
                ScalarValue::Int16(i16::MAX),
            ),
            (
                PhysicalType::Int32,
                PostgresType::Int8,
                i64::from(i32::MIN),
                i64::from(i32::MAX),
                i64::from(i32::MIN) - 1,
                i64::from(i32::MAX) + 1,
                ScalarValue::Int32(i32::MIN),
                ScalarValue::Int32(i32::MAX),
            ),
            (
                PhysicalType::UInt8,
                PostgresType::Int2,
                0,
                i64::from(u8::MAX),
                -1,
                i64::from(u8::MAX) + 1,
                ScalarValue::UInt8(u8::MIN),
                ScalarValue::UInt8(u8::MAX),
            ),
            (
                PhysicalType::UInt16,
                PostgresType::Int4,
                0,
                i64::from(u16::MAX),
                -1,
                i64::from(u16::MAX) + 1,
                ScalarValue::UInt16(u16::MIN),
                ScalarValue::UInt16(u16::MAX),
            ),
            (
                PhysicalType::UInt32,
                PostgresType::Int8,
                0,
                i64::from(u32::MAX),
                -1,
                i64::from(u32::MAX) + 1,
                ScalarValue::UInt32(u32::MIN),
                ScalarValue::UInt32(u32::MAX),
            ),
        ];
        for (target, carrier, min, max, below, above, min_value, max_value) in cases {
            for (value, expected) in [(min, min_value), (max, max_value)] {
                assert_eq!(
                    decode_text_parameter_as(
                        Some(value.to_string().as_bytes()),
                        carrier.oid(),
                        target,
                    )
                    .unwrap(),
                    expected.clone()
                );
                assert_eq!(
                    decode_binary_parameter_as(
                        Some(&binary(carrier, value)),
                        carrier.oid(),
                        target
                    )
                    .unwrap(),
                    expected
                );
            }
            for value in [below, above] {
                assert!(matches!(
                    decode_text_parameter_as(
                        Some(value.to_string().as_bytes()),
                        carrier.oid(),
                        target,
                    ),
                    Err(TypeMappingError::ValueOutOfRange(_))
                ));
                assert!(matches!(
                    decode_binary_parameter_as(
                        Some(&binary(carrier, value)),
                        carrier.oid(),
                        target
                    ),
                    Err(TypeMappingError::ValueOutOfRange(_))
                ));
            }
        }
    }
}
