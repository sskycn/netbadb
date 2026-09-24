use crate::StorageError;
use netbadb_row_codec::RowError;
use netbadb_schema::TableDef;
#[cfg(test)]
use netbadb_types::ColumnId;
use netbadb_types::ScalarValue;

impl From<RowError> for StorageError {
    fn from(error: RowError) -> Self {
        match error {
            RowError::Codec(error) => Self::Codec(error),
            RowError::InvalidRowLength { expected, actual } => {
                Self::InvalidRowLength { expected, actual }
            }
            RowError::TypeMismatch {
                column,
                expected,
                actual,
            } => Self::TypeMismatch {
                column,
                expected,
                actual,
            },
            RowError::NullNotAllowed { column } => Self::NullNotAllowed { column },
            RowError::UnknownColumn { column_id } => Self::UnknownColumn { column_id },
            RowError::InvalidFormat(message) => crate::invalid_format(message),
            RowError::ResourceLimit { resource, limit } => Self::ResourceLimit { resource, limit },
        }
    }
}

pub(crate) fn encode_row(values: &[ScalarValue]) -> Result<Vec<u8>, StorageError> {
    netbadb_row_codec::encode_row(values).map_err(Into::into)
}

pub(crate) fn decode_row(
    payload: &[u8],
    table: &TableDef,
) -> Result<Vec<ScalarValue>, StorageError> {
    netbadb_row_codec::decode_row(payload, table).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn decode_row_columns(
    payload: &[u8],
    table: &TableDef,
    columns: &[ColumnId],
) -> Result<Vec<ScalarValue>, StorageError> {
    netbadb_row_codec::decode_row_columns(payload, table, columns).map_err(Into::into)
}

pub(crate) fn decode_row_positions(
    payload: &[u8],
    table: &TableDef,
    positions: &[usize],
) -> Result<Vec<ScalarValue>, StorageError> {
    netbadb_row_codec::decode_row_positions(payload, table, positions).map_err(Into::into)
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
