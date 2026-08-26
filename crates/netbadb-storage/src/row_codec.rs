use netbadb_schema::{ColumnDef, TableDef};
use netbadb_types::{ColumnId, ScalarRef, ScalarValue};

use crate::{CodecError, StorageError};

/// The engine-neutral row codec used by both Heap and LSM. Its tags and byte
/// order intentionally preserve the original Heap encoding byte-for-byte.
pub(crate) fn encode_row(values: &[ScalarValue]) -> Result<Vec<u8>, StorageError> {
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
                let length =
                    u32::try_from(value.len()).map_err(|_| StorageError::ResourceLimit {
                        resource: "encoded row bytes",
                        limit: u32::MAX as u64,
                    })?;
                encoded.extend_from_slice(&length.to_le_bytes());
                encoded.extend_from_slice(value.as_bytes());
            }
            ScalarValue::Null => encoded.push(4),
        }
    }
    Ok(encoded)
}

pub(crate) fn validate_row(table: &TableDef, values: &[ScalarValue]) -> Result<(), StorageError> {
    if values.len() != table.columns.len() {
        return Err(StorageError::InvalidRowLength {
            expected: table.columns.len(),
            actual: values.len(),
        });
    }
    for (value, column) in values.iter().zip(&table.columns) {
        if matches!(value, ScalarValue::Null) {
            if !column.nullable {
                return Err(StorageError::NullNotAllowed {
                    column: column.name.clone(),
                });
            }
        } else if !value.matches_type(&column.semantic_type()) {
            return Err(StorageError::TypeMismatch {
                column: column.name.clone(),
                expected: column.semantic_type().physical,
                actual: value.physical_type(),
            });
        }
    }
    Ok(())
}

pub(crate) fn resolve_columns(
    table: &TableDef,
    columns: &[ColumnId],
) -> Result<Vec<usize>, StorageError> {
    columns
        .iter()
        .map(|column_id| {
            table
                .columns
                .iter()
                .position(|column| column.id == *column_id)
                .ok_or(StorageError::UnknownColumn {
                    column_id: *column_id,
                })
        })
        .collect()
}

pub(crate) fn decode_row(
    payload: &[u8],
    table: &TableDef,
) -> Result<Vec<ScalarValue>, StorageError> {
    let positions = (0..table.columns.len()).collect::<Vec<_>>();
    decode_row_positions(payload, table, &positions)
}

pub(crate) fn decode_row_columns(
    payload: &[u8],
    table: &TableDef,
    columns: &[ColumnId],
) -> Result<Vec<ScalarValue>, StorageError> {
    let positions = resolve_columns(table, columns)?;
    decode_row_positions(payload, table, &positions)
}

pub(crate) fn decode_row_positions(
    payload: &[u8],
    table: &TableDef,
    positions: &[usize],
) -> Result<Vec<ScalarValue>, StorageError> {
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
            value.ok_or_else(|| crate::invalid_format("row projection position is out of bounds"))
        })
        .collect()
}

fn decode_value<'a>(payload: &'a [u8], offset: &mut usize) -> Result<ScalarRef<'a>, StorageError> {
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
        other => Err(CodecError::UnknownScalarTag(other).into()),
    }
}

fn take_array<const N: usize>(payload: &[u8], offset: &mut usize) -> Result<[u8; N], StorageError> {
    let end = offset.checked_add(N).ok_or(CodecError::LengthOverflow)?;
    let bytes = payload
        .get(*offset..end)
        .ok_or(CodecError::ScalarTruncated)?;
    *offset = end;
    bytes
        .try_into()
        .map_err(|_| CodecError::ScalarTruncated.into())
}

fn validate_decoded_scalar(value: ScalarRef<'_>, column: &ColumnDef) -> Result<(), StorageError> {
    if value.is_null() {
        if !column.nullable {
            return Err(StorageError::NullNotAllowed {
                column: column.name.clone(),
            });
        }
    } else if !value.matches_type(&column.semantic_type()) {
        return Err(StorageError::TypeMismatch {
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

    use super::{decode_row, decode_row_columns, encode_row};

    fn table() -> TableDef {
        TableDef::new(
            TableId(1),
            "rows",
            vec![
                ColumnDef::new(ColumnId(1), "i", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "s", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
            ],
        )
    }

    #[test]
    fn heap_compatible_golden_and_selective_validation() {
        let row = vec![ScalarValue::Int64(-7), ScalarValue::Text("ok".into())];
        let bytes = encode_row(&row).expect("encode");
        assert_eq!(
            bytes,
            vec![
                1, 249, 255, 255, 255, 255, 255, 255, 255, 3, 2, 0, 0, 0, b'o', b'k'
            ]
        );
        assert_eq!(decode_row(&bytes, &table()).expect("decode"), row);
        assert_eq!(
            decode_row_columns(&bytes, &table(), &[ColumnId(2)]).expect("project"),
            vec![ScalarValue::Text("ok".into())]
        );
        let mut corrupt = bytes;
        corrupt[0] = 99;
        assert!(decode_row_columns(&corrupt, &table(), &[ColumnId(2)]).is_err());
    }
}
