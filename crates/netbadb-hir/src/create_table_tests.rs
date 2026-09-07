use super::*;

#[test]
fn typed_declaration_is_unnamed_and_preserves_source_order() {
    for (name, physical) in [
        ("BOOLEAN", PhysicalType::Bool),
        ("BOOL", PhysicalType::Bool),
        ("BIGINT", PhysicalType::Int64),
        ("INT64", PhysicalType::Int64),
        ("INT8", PhysicalType::Int64),
        ("TINYINT", PhysicalType::Int8),
        ("SMALLINT", PhysicalType::Int16),
        ("INTEGER", PhysicalType::Int32),
        ("INT128", PhysicalType::Int128),
        ("UINT8", PhysicalType::UInt8),
        ("UINT16", PhysicalType::UInt16),
        ("UINT32", PhysicalType::UInt32),
        ("TEXT", PhysicalType::Text),
        ("VARCHAR", PhysicalType::Text),
        ("UINT64", PhysicalType::UInt64),
        ("UINT128", PhysicalType::UInt128),
        ("REAL", PhysicalType::Float32),
        ("DOUBLE", PhysicalType::Float64),
        ("BYTEA", PhysicalType::Bytes),
    ] {
        let sql = format!("CREATE TABLE t (id {name} NOT NULL, next {name}, ending {name} NULL)");
        let AstStatement::CreateTable(ast) = netbadb_parser::parse_statement(&sql).unwrap() else {
            panic!()
        };
        let typed = lower_create_table(&ast).unwrap();
        assert_eq!(
            typed
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "next", "ending"]
        );
        assert_eq!(
            typed.columns.iter().map(|c| c.nullable).collect::<Vec<_>>(),
            [false, true, true]
        );
        for column in typed.columns {
            assert_eq!(column.data_type, SemanticType::physical(physical));
            assert_eq!(&sql[column.type_span.start..column.type_span.end], name);
        }
    }
}

#[test]
fn duplicate_and_unknown_types_have_precise_positions() {
    for (sql, expected) in [
        ("CREATE TABLE t (a TEXT, a BIGINT)", "a"),
        ("CREATE TABLE t (a MAGIC_TYPE)", "MAGIC_TYPE"),
    ] {
        let AstStatement::CreateTable(ast) = netbadb_parser::parse_statement(sql).unwrap() else {
            panic!()
        };
        let error = lower_create_table(&ast).unwrap_err();
        let span = error.span();
        assert_eq!(&sql[span.start..span.end], expected);
        match expected {
            "a" => assert!(matches!(error, HirError::DuplicateColumn { .. })),
            "MAGIC_TYPE" => assert!(matches!(error, HirError::UnknownType { .. })),
            _ => unreachable!(),
        }
    }
    for name in [
        "NUMERIC",
        "DECIMAL",
        "FLOAT",
        "DATE",
        "TIMESTAMP",
        "JSON",
        "UUID",
        "ARRAY",
        "SERIAL",
        "BIGSERIAL",
    ] {
        assert!(matches!(
            resolve_declared_type(&Ident {
                name: name.into(),
                span: Span {
                    start: 0,
                    end: name.len()
                }
            }),
            Err(HirError::UnsupportedType { .. })
        ));
    }
}
