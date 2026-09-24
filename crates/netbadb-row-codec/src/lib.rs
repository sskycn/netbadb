//! Positional row scalar codec shared by authoritative engines and the change stream.

use std::error::Error;
use std::fmt;

use netbadb_types::{ColumnId, PhysicalType};

/// Errors raised while decoding the explicit row scalar format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    MissingScalarTag,
    UnknownScalarTag(u8),
    InvalidBoolean(u8),
    ScalarTruncated,
    LengthOverflow,
    TextNotUtf8,
    ExtraValues,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingScalarTag => formatter.write_str("row is missing a scalar tag"),
            Self::UnknownScalarTag(tag) => write!(formatter, "unknown scalar tag {tag}"),
            Self::InvalidBoolean(value) => write!(formatter, "invalid boolean value {value}"),
            Self::ScalarTruncated => formatter.write_str("scalar value is truncated"),
            Self::LengthOverflow => formatter.write_str("scalar length overflows the row"),
            Self::TextNotUtf8 => formatter.write_str("text value is not valid UTF-8"),
            Self::ExtraValues => formatter.write_str("row contains extra values"),
        }
    }
}

impl Error for CodecError {}

/// Schema and payload errors produced by the shared row codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowError {
    Codec(CodecError),
    InvalidRowLength {
        expected: usize,
        actual: usize,
    },
    TypeMismatch {
        column: String,
        expected: PhysicalType,
        actual: Option<PhysicalType>,
    },
    NullNotAllowed {
        column: String,
    },
    UnknownColumn {
        column_id: ColumnId,
    },
    InvalidFormat(&'static str),
    ResourceLimit {
        resource: &'static str,
        limit: u64,
    },
}

impl From<CodecError> for RowError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

impl fmt::Display for RowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => error.fmt(formatter),
            Self::InvalidRowLength { expected, actual } => {
                write!(formatter, "expected {expected} row values, found {actual}")
            }
            Self::TypeMismatch {
                column,
                expected,
                actual,
            } => {
                write!(
                    formatter,
                    "column `{column}` expects {expected}, found {actual:?}"
                )
            }
            Self::NullNotAllowed { column } => {
                write!(formatter, "column `{column}` is not nullable")
            }
            Self::UnknownColumn { column_id } => {
                write!(formatter, "table has no column with ID {}", column_id.0)
            }
            Self::InvalidFormat(message) => write!(formatter, "invalid row format: {message}"),
            Self::ResourceLimit { resource, limit } => {
                write!(formatter, "{resource} exceeds limit {limit}")
            }
        }
    }
}

impl Error for RowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}

use netbadb_schema::{ColumnDef, TableDef};
use netbadb_types::{Float32Value, Float64Value, ScalarRef, ScalarValue};

/// The engine-neutral row codec used by both Heap and LSM. Its tags and byte
/// order intentionally preserve the original Heap encoding byte-for-byte.
pub fn encode_row(values: &[ScalarValue]) -> Result<Vec<u8>, RowError> {
    let mut encoded = Vec::new();
    for value in values {
        match value {
            ScalarValue::Bool(value) => {
                encoded.push(0);
                encoded.push(u8::from(*value));
            }
            ScalarValue::Int64(value) => {
                encoded.push(1);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::UInt64(value) => {
                encoded.push(2);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Text(value) => {
                encoded.push(3);
                let length = u32::try_from(value.len()).map_err(|_| RowError::ResourceLimit {
                    resource: "encoded row bytes",
                    limit: u32::MAX as u64,
                })?;
                encoded.extend_from_slice(&length.to_le_bytes());
                encoded.extend_from_slice(value.as_bytes());
            }
            ScalarValue::Null => encoded.push(4),
            ScalarValue::Int8(value) => {
                encoded.push(5);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Int16(value) => {
                encoded.push(6);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Int32(value) => {
                encoded.push(7);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Int128(value) => {
                encoded.push(8);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::UInt8(value) => {
                encoded.push(9);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::UInt16(value) => {
                encoded.push(10);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::UInt32(value) => {
                encoded.push(11);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::UInt128(value) => {
                encoded.push(12);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Float32(value) => {
                encoded.push(13);
                encoded.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            ScalarValue::Float64(value) => {
                encoded.push(14);
                encoded.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            ScalarValue::Bytes(value) => {
                encoded.push(15);
                let length = u32::try_from(value.len()).map_err(|_| RowError::ResourceLimit {
                    resource: "encoded row bytes",
                    limit: u32::MAX as u64,
                })?;
                encoded.extend_from_slice(&length.to_le_bytes());
                encoded.extend_from_slice(value);
            }
        }
    }
    Ok(encoded)
}

pub fn validate_row(table: &TableDef, values: &[ScalarValue]) -> Result<(), RowError> {
    if values.len() != table.columns.len() {
        return Err(RowError::InvalidRowLength {
            expected: table.columns.len(),
            actual: values.len(),
        });
    }
    for (value, column) in values.iter().zip(&table.columns) {
        if matches!(value, ScalarValue::Null) {
            if !column.nullable {
                return Err(RowError::NullNotAllowed {
                    column: column.name.clone(),
                });
            }
        } else if !value.matches_type(&column.semantic_type()) {
            return Err(RowError::TypeMismatch {
                column: column.name.clone(),
                expected: column.semantic_type().physical,
                actual: value.physical_type(),
            });
        }
    }
    Ok(())
}

pub fn resolve_columns(table: &TableDef, columns: &[ColumnId]) -> Result<Vec<usize>, RowError> {
    columns
        .iter()
        .map(|column_id| {
            table
                .columns
                .iter()
                .position(|column| column.id == *column_id)
                .ok_or(RowError::UnknownColumn {
                    column_id: *column_id,
                })
        })
        .collect()
}

pub fn decode_row(payload: &[u8], table: &TableDef) -> Result<Vec<ScalarValue>, RowError> {
    let positions = (0..table.columns.len()).collect::<Vec<_>>();
    decode_row_positions(payload, table, &positions)
}

pub fn decode_row_columns(
    payload: &[u8],
    table: &TableDef,
    columns: &[ColumnId],
) -> Result<Vec<ScalarValue>, RowError> {
    let positions = resolve_columns(table, columns)?;
    decode_row_positions(payload, table, &positions)
}

pub fn decode_row_positions(
    payload: &[u8],
    table: &TableDef,
    positions: &[usize],
) -> Result<Vec<ScalarValue>, RowError> {
    let mut offset = 0;
    let mut selected = vec![None; positions.len()];
    for (schema_position, column) in table.columns.iter().enumerate() {
        let value = decode_value(payload, &mut offset)?;
        validate_decoded_scalar(value, column)?;
        for (output_position, requested) in positions.iter().enumerate() {
            if *requested == schema_position {
                selected[output_position] = Some(value.to_owned());
            }
        }
    }
    if offset != payload.len() {
        return Err(CodecError::ExtraValues.into());
    }
    selected
        .into_iter()
        .map(|value| {
            value.ok_or(RowError::InvalidFormat(
                "row projection position is out of bounds",
            ))
        })
        .collect()
}

fn decode_value<'a>(payload: &'a [u8], offset: &mut usize) -> Result<ScalarRef<'a>, RowError> {
    let tag = *payload.get(*offset).ok_or(CodecError::MissingScalarTag)?;
    *offset = offset.checked_add(1).ok_or(CodecError::LengthOverflow)?;
    match tag {
        0 => {
            let value = *payload.get(*offset).ok_or(CodecError::ScalarTruncated)?;
            *offset = offset.checked_add(1).ok_or(CodecError::LengthOverflow)?;
            match value {
                0 => Ok(ScalarRef::Bool(false)),
                1 => Ok(ScalarRef::Bool(true)),
                other => Err(CodecError::InvalidBoolean(other).into()),
            }
        }
        1 => Ok(ScalarRef::Int64(i64::from_le_bytes(take_array(
            payload, offset,
        )?))),
        2 => Ok(ScalarRef::UInt64(u64::from_le_bytes(take_array(
            payload, offset,
        )?))),
        3 => {
            let length = u32::from_le_bytes(take_array(payload, offset)?) as usize;
            let end = offset
                .checked_add(length)
                .ok_or(CodecError::LengthOverflow)?;
            let bytes = payload
                .get(*offset..end)
                .ok_or(CodecError::ScalarTruncated)?;
            *offset = end;
            Ok(ScalarRef::Text(
                std::str::from_utf8(bytes).map_err(|_| CodecError::TextNotUtf8)?,
            ))
        }
        4 => Ok(ScalarRef::Null),
        5 => Ok(ScalarRef::Int8(i8::from_le_bytes(take_array(
            payload, offset,
        )?))),
        6 => Ok(ScalarRef::Int16(i16::from_le_bytes(take_array(
            payload, offset,
        )?))),
        7 => Ok(ScalarRef::Int32(i32::from_le_bytes(take_array(
            payload, offset,
        )?))),
        8 => Ok(ScalarRef::Int128(i128::from_le_bytes(take_array(
            payload, offset,
        )?))),
        9 => Ok(ScalarRef::UInt8(u8::from_le_bytes(take_array(
            payload, offset,
        )?))),
        10 => Ok(ScalarRef::UInt16(u16::from_le_bytes(take_array(
            payload, offset,
        )?))),
        11 => Ok(ScalarRef::UInt32(u32::from_le_bytes(take_array(
            payload, offset,
        )?))),
        12 => Ok(ScalarRef::UInt128(u128::from_le_bytes(take_array(
            payload, offset,
        )?))),
        13 => Ok(ScalarRef::Float32(Float32Value::from_bits(
            u32::from_le_bytes(take_array(payload, offset)?),
        ))),
        14 => Ok(ScalarRef::Float64(Float64Value::from_bits(
            u64::from_le_bytes(take_array(payload, offset)?),
        ))),
        15 => {
            let length = u32::from_le_bytes(take_array(payload, offset)?) as usize;
            let end = offset
                .checked_add(length)
                .ok_or(CodecError::LengthOverflow)?;
            let bytes = payload
                .get(*offset..end)
                .ok_or(CodecError::ScalarTruncated)?;
            *offset = end;
            Ok(ScalarRef::Bytes(bytes))
        }
        other => Err(CodecError::UnknownScalarTag(other).into()),
    }
}

fn take_array<const N: usize>(payload: &[u8], offset: &mut usize) -> Result<[u8; N], RowError> {
    let end = offset.checked_add(N).ok_or(CodecError::LengthOverflow)?;
    let bytes = payload
        .get(*offset..end)
        .ok_or(CodecError::ScalarTruncated)?;
    *offset = end;
    bytes
        .try_into()
        .map_err(|_| CodecError::ScalarTruncated.into())
}

fn validate_decoded_scalar(value: ScalarRef<'_>, column: &ColumnDef) -> Result<(), RowError> {
    if value.is_null() {
        if !column.nullable {
            return Err(RowError::NullNotAllowed {
                column: column.name.clone(),
            });
        }
    } else if !value.matches_type(&column.semantic_type()) {
        return Err(RowError::TypeMismatch {
            column: column.name.clone(),
            expected: column.semantic_type().physical,
            actual: value.physical_type(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use netbadb_schema::{ColumnDef, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, ScalarValue, TableId};

    use super::{CodecError, RowError, decode_row, encode_row};

    #[test]
    fn baseline_bytes_and_corruption_are_independent_of_storage() {
        let table = TableDef::new(
            TableId(1),
            "row",
            vec![
                ColumnDef::new(
                    ColumnId(1),
                    "value",
                    TypeSpec::Physical(PhysicalType::Int64),
                ),
                ColumnDef::new(ColumnId(2), "maybe", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
            ],
        );
        let values = [ScalarValue::Int64(-7), ScalarValue::Null];
        let expected = [1, 249, 255, 255, 255, 255, 255, 255, 255, 4];
        assert_eq!(encode_row(&values).unwrap(), expected);
        assert_eq!(decode_row(&expected, &table).unwrap(), values);
        assert!(matches!(
            decode_row(&expected[..expected.len() - 1], &table),
            Err(RowError::Codec(CodecError::MissingScalarTag))
        ));
    }
}
