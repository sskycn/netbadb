use super::*;
use netbadb_schema::{ColumnDef, DropTableTarget, SchemaFingerprint, TypeSpec};

#[test]
fn drop_table_lowering_preserves_name_span_and_exact_schema_identity() {
    let sql = "DROP TABLE projects;";
    let AstStatement::DropTable(ast) = netbadb_parser::parse_statement(sql).unwrap() else {
        panic!("DROP TABLE AST")
    };
    let table = TableDef::new(
        TableId(41),
        "projects",
        vec![ColumnDef::new(
            ColumnId(1),
            "id",
            TypeSpec::Physical(PhysicalType::Int64),
        )],
    );
    let schema = Schema::new(vec![table.clone()]).unwrap();
    let target = DropTableTarget {
        table_id: table.id,
        table_version: netbadb_types::TableSchemaVersion(7),
        fingerprint: table.fingerprint().unwrap(),
    };
    let typed = lower_drop_table(&schema, &ast, &[TableIdentityBinding { target }]).unwrap();
    assert_eq!(typed.name, "projects");
    assert_eq!(typed.target, target);
    assert_eq!(&sql[typed.name_span.start..typed.name_span.end], "projects");
    assert_eq!(
        &sql[typed.span.start..typed.span.end],
        "DROP TABLE projects"
    );

    let missing = lower_drop_table(&schema, &ast, &[]).unwrap_err();
    assert!(matches!(missing, HirError::UnknownTable { .. }));
    assert_eq!(&sql[missing.span().start..missing.span().end], "projects");
    let _compile_time_boundary: SchemaFingerprint = typed.target.fingerprint;
}
