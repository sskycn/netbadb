use super::*;
use netbadb_schema::{ColumnDef, DropTableTarget, TableDef, TypeSpec};
use netbadb_types::{ColumnId, TableId, TableSchemaVersion};

fn fixture() -> (Schema, TableIdentityBinding) {
    let table = TableDef::new(
        TableId(8),
        "projects",
        vec![ColumnDef::new(
            ColumnId(3),
            "name",
            TypeSpec::Physical(PhysicalType::Text),
        )],
    );
    let target = DropTableTarget {
        table_id: table.id,
        table_version: TableSchemaVersion(12),
        fingerprint: table.fingerprint().unwrap(),
    };
    (
        Schema::new(vec![table]).unwrap(),
        TableIdentityBinding { target },
    )
}

#[test]
fn compiler_retains_exact_typed_alter_payload() {
    let (schema, binding) = fixture();
    let compiled = compile_ddl_statement(
        &schema,
        "ALTER TABLE projects RENAME COLUMN name TO title",
        &[],
        &[binding],
    )
    .unwrap();
    let CompiledDdlStatement::AlterTable(statement) = compiled else {
        panic!("ALTER TABLE compiled DDL")
    };
    assert_eq!(statement.target.table_id, TableId(8));
    assert_eq!(statement.target.table_version, TableSchemaVersion(12));
    assert!(matches!(
        statement.operation,
        TypedAlterTableOperation::RenameColumn { column_id: ColumnId(3), ref new_name }
            if new_name == "title"
    ));
}

#[test]
fn generic_dispatch_rejects_alter_parameters() {
    let (schema, binding) = fixture();
    assert!(matches!(
        compile_sql_statement(
            &schema,
            "ALTER TABLE projects ADD COLUMN active BOOL",
            &[],
            &[binding],
            &[Some(PhysicalType::Int64)],
        ),
        Err(CompileError::Hir(HirError::InvalidTableDefinition { .. }))
    ));
}

#[test]
fn compiler_lowers_alter_type_using_to_relational_expression_ir() {
    let (schema, binding) = fixture();
    let compiled = compile_ddl_statement(
        &schema,
        "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING name::BIGINT",
        &[],
        &[binding],
    )
    .unwrap();
    let CompiledDdlStatement::AlterTypeUsing(statement) = compiled else {
        panic!("compiled ALTER TYPE USING")
    };
    assert_eq!(statement.target.table_id, TableId(8));
    assert_eq!(statement.column_id, ColumnId(3));
    assert_eq!(
        statement.target_type,
        SemanticType::physical(PhysicalType::Int64)
    );
    assert_eq!(statement.using.expr_type.data_type, statement.target_type);
    let ExprKind::Cast { expression } = statement.using.kind else {
        panic!("relational CAST")
    };
    let ExprKind::Column(column) = expression.kind else {
        panic!("bound relational column")
    };
    assert_eq!(column.table_id, TableId(8));
    assert_eq!(column.column_id, ColumnId(3));
}
