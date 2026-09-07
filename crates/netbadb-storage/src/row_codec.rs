use netbadb_schema::{ColumnDef, TableDef};
use netbadb_types::{ColumnId, Float32Value, Float64Value, ScalarRef, ScalarValue};

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
                let length =
                    u32::try_from(value.len()).map_err(|_| StorageError::ResourceLimit {
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
    use netbadb_types::{ColumnId, Float32Value, Float64Value, PhysicalType, ScalarValue, TableId};

    use super::{decode_row, decode_row_columns, encode_row};
    use crate::{CodecError, StorageError};

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

    #[test]
    fn schema_evolution_audit_pins_the_positional_unversioned_row_contract() {
        let baseline = TableDef::new(
            TableId(10),
            "records",
            vec![
                ColumnDef::new(ColumnId(1), "a", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "b", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
                ColumnDef::new(ColumnId(3), "c", TypeSpec::Physical(PhysicalType::Bool)),
            ],
        );
        let row = vec![
            ScalarValue::Int64(7),
            ScalarValue::Text("hi".into()),
            ScalarValue::Bool(true),
        ];
        let bytes = encode_row(&row).expect("encode baseline row");

        // This is the entire engine-neutral row payload used by Heap and LSM:
        // one tag per positional scalar, with no envelope, arity, schema
        // version, TableId or ColumnId. Null is the single tag 4.
        assert_eq!(
            bytes,
            vec![1, 7, 0, 0, 0, 0, 0, 0, 0, 3, 2, 0, 0, 0, b'h', b'i', 0, 1]
        );
        assert_eq!(encode_row(&[ScalarValue::Null]).unwrap(), vec![4]);
        assert_eq!(decode_row(&bytes, &baseline).unwrap(), row);

        let mut renamed = baseline.clone();
        renamed.name = "renamed_records".into();
        renamed.columns[1].name = "renamed_b".into();
        assert_eq!(decode_row(&bytes, &renamed).unwrap(), row);

        let sparse_ids = TableDef::new(
            TableId(999),
            "other_identity",
            vec![
                ColumnDef::new(ColumnId(40), "x", TypeSpec::Physical(PhysicalType::Int64)),
                ColumnDef::new(ColumnId(2), "y", TypeSpec::Physical(PhysicalType::Text))
                    .nullable(true),
                ColumnDef::new(ColumnId(900), "z", TypeSpec::Physical(PhysicalType::Bool)),
            ],
        );
        assert_eq!(decode_row(&bytes, &sparse_ids).unwrap(), row);
        assert_eq!(
            decode_row_columns(&bytes, &sparse_ids, &[ColumnId(900), ColumnId(40)]).unwrap(),
            vec![ScalarValue::Bool(true), ScalarValue::Int64(7)]
        );

        let mut appended_nullable = baseline.clone();
        appended_nullable.columns.push(
            ColumnDef::new(ColumnId(4), "d", TypeSpec::Physical(PhysicalType::Bool)).nullable(true),
        );
        assert!(matches!(
            decode_row(&bytes, &appended_nullable),
            Err(StorageError::Codec(CodecError::MissingScalarTag))
        ));

        let mut dropped_middle = baseline.clone();
        dropped_middle.columns.remove(1);
        assert!(matches!(
            decode_row(&bytes, &dropped_middle),
            Err(StorageError::TypeMismatch {
                expected: PhysicalType::Bool,
                actual: Some(PhysicalType::Text),
                ..
            })
        ));

        let mut changed_physical = baseline.clone();
        changed_physical.columns[0].type_spec = TypeSpec::Physical(PhysicalType::UInt64);
        assert!(matches!(
            decode_row(&bytes, &changed_physical),
            Err(StorageError::TypeMismatch {
                expected: PhysicalType::UInt64,
                actual: Some(PhysicalType::Int64),
                ..
            })
        ));

        let mut dropped_trailing = baseline;
        dropped_trailing.columns.pop();
        assert!(matches!(
            decode_row(&bytes, &dropped_trailing),
            Err(StorageError::Codec(CodecError::ExtraValues))
        ));
    }

    #[test]
    fn every_appended_scalar_tag_round_trips_without_changing_legacy_tags() {
        let values = vec![
            ScalarValue::Int8(i8::MIN),
            ScalarValue::Int16(i16::MAX),
            ScalarValue::Int32(i32::MIN),
            ScalarValue::Int128(i128::MAX),
            ScalarValue::UInt8(u8::MAX),
            ScalarValue::UInt16(u16::MAX),
            ScalarValue::UInt32(u32::MAX),
            ScalarValue::UInt128(u128::MAX),
            ScalarValue::Float32(Float32Value::from_bits(0xffff_ffff)),
            ScalarValue::Float64(Float64Value::new(f64::NEG_INFINITY)),
            ScalarValue::Bytes(vec![0, 0xff, 0x80]),
        ];
        let physical = [
            PhysicalType::Int8,
            PhysicalType::Int16,
            PhysicalType::Int32,
            PhysicalType::Int128,
            PhysicalType::UInt8,
            PhysicalType::UInt16,
            PhysicalType::UInt32,
            PhysicalType::UInt128,
            PhysicalType::Float32,
            PhysicalType::Float64,
            PhysicalType::Bytes,
        ];
        let table = TableDef::new(
            TableId(20),
            "all_scalars",
            physical
                .into_iter()
                .enumerate()
                .map(|(index, physical)| {
                    ColumnDef::new(
                        ColumnId(index as u32 + 1),
                        format!("c{index}"),
                        TypeSpec::Physical(physical),
                    )
                })
                .collect(),
        );

        let bytes = encode_row(&values).expect("encode all scalar kinds");
        let mut offset = 0;
        let widths = [1, 2, 4, 16, 1, 2, 4, 16, 4, 8];
        for (expected_tag, width) in (5_u8..=14).zip(widths) {
            assert_eq!(bytes[offset], expected_tag);
            offset += 1 + width;
        }
        assert_eq!(bytes[offset], 15);
        assert_eq!(&bytes[offset + 1..offset + 5], &3_u32.to_le_bytes());
        assert_eq!(
            decode_row(&bytes, &table).expect("decode all scalar kinds"),
            values
        );
    }
}
