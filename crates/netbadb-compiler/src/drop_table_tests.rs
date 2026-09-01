use super::*;
use netbadb_schema::{ColumnDef, DropTableTarget, TableDef, TypeSpec};
use netbadb_types::{ColumnId, TableId, TableSchemaVersion};

#[test]
fn generic_compiler_emits_drop_table_with_the_exact_bound_target() {
    let table = TableDef::new(
        TableId(9),
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
        table_version: TableSchemaVersion(3),
        fingerprint: table.fingerprint().unwrap(),
    };
    let compiled = compile_sql_statement(
        &schema,
        "DROP TABLE projects",
        &[],
        &[TableIdentityBinding { target }],
        &[],
    )
    .unwrap();
    let CompiledSqlStatement::Ddl(CompiledDdlStatement::DropTable(statement)) = compiled else {
        panic!("compiled DROP TABLE")
    };
    assert_eq!(statement.name, "projects");
    assert_eq!(statement.target, target);
    assert!(
        compile_sql_statement(
            &schema,
            "DROP TABLE projects",
            &[],
            &[TableIdentityBinding { target }],
            &[Some(PhysicalType::Int64)],
        )
        .is_err()
    );
}
