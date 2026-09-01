use super::*;

#[test]
fn generic_dispatch_produces_logical_ddl_without_catalog_identities() {
    let schema = Schema::new(vec![]).unwrap();
    let sql = "CREATE TABLE projects (id INT64 NOT NULL, name TEXT)";
    let CompiledSqlStatement::Ddl(CompiledDdlStatement::CreateTable(table)) =
        compile_sql_statement(&schema, sql, &[], &[], &[]).unwrap()
    else {
        panic!()
    };
    assert_eq!(table.name, "projects");
    assert_eq!(
        table.columns[0].data_type,
        SemanticType::physical(PhysicalType::Int64)
    );
    assert!(!table.columns[0].nullable);
    assert!(table.columns[1].nullable);
    assert!(compile_sql_statement(&schema, sql, &[], &[], &[Some(PhysicalType::Int64)]).is_err());
    for (sql, kind) in [
        (
            "CREATE TABLE t (a TEXT, a TEXT)",
            CompileErrorKind::DuplicateColumn,
        ),
        (
            "CREATE TABLE t (a UNKNOWN)",
            CompileErrorKind::UndefinedType,
        ),
        (
            "CREATE TABLE t (a INTEGER)",
            CompileErrorKind::FeatureNotSupported,
        ),
        (
            "CREATE TABLE t (a TEXT PRIMARY KEY)",
            CompileErrorKind::FeatureNotSupported,
        ),
    ] {
        assert_eq!(
            compile_sql_statement(&schema, sql, &[], &[], &[])
                .unwrap_err()
                .kind(),
            kind
        );
    }
}
