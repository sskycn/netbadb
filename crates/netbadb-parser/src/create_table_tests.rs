use super::*;

#[test]
fn declarations_preserve_order_types_nullability_and_spans() {
    let sql = "CREATE TABLE projects (id BIGINT NOT NULL, name TEXT, active BOOL NULL);";
    let Statement::CreateTable(table) = parse_statement(sql).unwrap() else {
        panic!()
    };
    assert_eq!(table.name.name, "projects");
    assert_eq!(table.columns.len(), 3);
    assert!(!table.columns[0].nullability.unwrap().nullable);
    assert!(table.columns[1].nullability.is_none());
    assert!(table.columns[2].nullability.unwrap().nullable);
    for column in table.columns {
        assert_eq!(
            &sql[column.name.span.start..column.name.span.end],
            column.name.name
        );
        assert_eq!(
            &sql[column.data_type.span.start..column.data_type.span.end],
            column.data_type.name
        );
    }
    for name in [
        "BOOLEAN",
        "BOOL",
        "BIGINT",
        "INT64",
        "INT8",
        "TEXT",
        "VARCHAR",
        "UINT64",
        "MAGIC_TYPE",
    ] {
        assert!(parse_statement(&format!("CREATE TABLE t (a {name})")).is_ok());
    }
    // New contextual grammar must not reserve these words in existing SQL.
    assert!(parse_statement("SELECT table, primary, identity FROM projects").is_ok());
    assert!(parse_statement("CREATE INDEX i ON projects (id)").is_ok());
    assert!(parse_statement("DROP INDEX i").is_ok());
}

#[test]
fn unsupported_clauses_fail_explicitly() {
    for sql in [
        "CREATE TABLE t (a BIGINT PRIMARY KEY)",
        "CREATE TABLE t (a BIGINT, PRIMARY KEY (a))",
        "CREATE TABLE t (a TEXT UNIQUE)",
        "CREATE TABLE t (a TEXT, UNIQUE (a))",
        "CREATE TABLE t (a TEXT DEFAULT 'x')",
        "CREATE TABLE t (a BIGINT CHECK (a > 0))",
        "CREATE TABLE t (a BIGINT REFERENCES other)",
        "CREATE TABLE t (a BIGINT, FOREIGN KEY (a) REFERENCES other)",
        "CREATE TABLE t (a BIGINT GENERATED ALWAYS AS IDENTITY)",
        "CREATE TABLE t (a BIGINT IDENTITY)",
        "CREATE TABLE t (a TEXT COLLATE c)",
        "CREATE TABLE t (CONSTRAINT c CHECK (true))",
        "CREATE TABLE t (EXCLUDE (a))",
        "CREATE TABLE IF NOT EXISTS t (a BIGINT)",
        "CREATE TEMP TABLE t (a BIGINT)",
        "CREATE TEMPORARY TABLE t (a BIGINT)",
        "CREATE UNLOGGED TABLE t (a BIGINT)",
        "CREATE TABLE t AS SELECT 1",
        "CREATE TABLE t (LIKE other)",
        "CREATE TABLE t (a BIGINT) INHERITS (other)",
        "CREATE TABLE t (a BIGINT) PARTITION BY RANGE (a)",
        "CREATE TABLE t (a BIGINT) USING lsm",
        "CREATE TABLE t (a BIGINT) WITH (x = 1)",
        "CREATE TABLE t (a BIGINT) TABLESPACE space",
        "CREATE TABLE t (a BIGINT) ON COMMIT DROP",
        "CREATE TABLE t (a VARCHAR(255))",
        "CREATE TABLE t (a CHAR(20))",
        "CREATE TABLE public.t (a BIGINT)",
    ] {
        let error = parse_statement(sql).expect_err(sql);
        assert_eq!(
            error.kind,
            ParseErrorKind::UnsupportedFeature,
            "{sql}: {error}"
        );
        assert!(error.span.start < sql.len());
    }
}

#[test]
fn malformed_declarations_and_conflicting_constraints_fail_closed() {
    for sql in [
        "CREATE TABLE t ()",
        "CREATE TABLE (a TEXT)",
        "CREATE TABLE \"Projects\" (a TEXT)",
        "CREATE TABLE t (\"a\" TEXT)",
        "CREATE TABLE t (a TEXT,)",
        "CREATE TABLE t (a TEXT b TEXT)",
        "CREATE TABLE t (a TEXT",
        "CREATE TABLE t (a TEXT) junk",
        "CREATE TABLE t (a TEXT) SELECT 1",
        "CREATE TABLE $1 (a TEXT)",
        "CREATE TABLE t (a $1)",
        "CREATE TABLE t (a TEXT NOT NULL NULL)",
        "CREATE TABLE t (a TEXT NULL NULL)",
        "CREATE TABLE t (a TEXT NOT NULL NOT NULL)",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn parser_bounds_and_deterministic_mutations() {
    let columns = (0..MAX_CREATE_COLUMNS)
        .map(|i| format!("c{i} TEXT"))
        .collect::<Vec<_>>()
        .join(",");
    assert!(parse_statement(&format!("CREATE TABLE t ({columns})")).is_ok());
    assert!(parse_statement(&format!("CREATE TABLE t ({columns}, extra TEXT)")).is_err());
    assert!(
        parse_statement(&format!(
            "CREATE TABLE {} (a TEXT)",
            "x".repeat(MAX_IDENTIFIER_BYTES + 1)
        ))
        .is_err()
    );
    assert!(parse_statement(&" ".repeat(MAX_SQL_BYTES + 1)).is_err());
    assert!(parse_statement(&"x ".repeat(MAX_SQL_TOKENS + 1)).is_err());
    assert!(
        parse_statement(&format!(
            "SELECT {}true{}",
            "(".repeat(1000),
            ")".repeat(1000)
        ))
        .is_err()
    );
    assert!(parse_statement(&format!("SELECT {}true", "NOT ".repeat(1000))).is_err());
    assert!(parse_statement(&format!("SELECT true{}", "::BOOL".repeat(1000))).is_err());
    let seed = b"CREATE TABLE projects (id BIGINT NOT NULL, name TEXT, active BOOLEAN);";
    let mut state = 19_u64;
    for _ in 0..2000 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mut input = seed.to_vec();
        let index = (state as usize) % input.len();
        input[index] = ((state >> 32) % 128) as u8;
        let input = String::from_utf8(input).unwrap();
        if let Err(error) = parse_statement(&input) {
            assert!(error.span.start <= error.span.end && error.span.end <= input.len());
            assert!(
                input.is_char_boundary(error.span.start) && input.is_char_boundary(error.span.end)
            );
        }
    }
}
