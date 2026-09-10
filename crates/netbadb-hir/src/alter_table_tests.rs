use super::*;
use netbadb_schema::{ColumnDef, DropTableTarget, TypeSpec};

fn fixture() -> (Schema, DropTableTarget) {
    let table = TableDef::new(
        TableId(41),
        "projects",
        vec![
            ColumnDef::new(ColumnId(7), "id", TypeSpec::Physical(PhysicalType::Int64)),
            ColumnDef::new(ColumnId(9), "name", TypeSpec::Physical(PhysicalType::Text))
                .nullable(true),
        ],
    );
    let target = DropTableTarget {
        table_id: table.id,
        table_version: netbadb_types::TableSchemaVersion(6),
        fingerprint: table.fingerprint().unwrap(),
    };
    (Schema::new(vec![table]).unwrap(), target)
}

#[test]
fn alter_lowering_binds_exact_table_and_column_identities() {
    let (schema, target) = fixture();
    for (sql, expected) in [
        ("ALTER TABLE projects RENAME TO work", None),
        (
            "ALTER TABLE projects RENAME COLUMN name TO title",
            Some(ColumnId(9)),
        ),
        ("ALTER TABLE projects DROP COLUMN name", Some(ColumnId(9))),
        (
            "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
            Some(ColumnId(9)),
        ),
        (
            "ALTER TABLE projects ALTER COLUMN name DROP NOT NULL",
            Some(ColumnId(9)),
        ),
    ] {
        let AstStatement::AlterTable(ast) = netbadb_parser::parse_statement(sql).unwrap() else {
            panic!("ALTER AST")
        };
        let typed = lower_alter_table(&schema, &ast, &[TableIdentityBinding { target }]).unwrap();
        assert_eq!(typed.target, target);
        let actual = match typed.operation {
            TypedAlterTableOperation::RenameTable { .. } => None,
            TypedAlterTableOperation::RenameColumn { column_id, .. }
            | TypedAlterTableOperation::DropColumn { column_id }
            | TypedAlterTableOperation::SetNotNull { column_id }
            | TypedAlterTableOperation::DropNotNull { column_id } => Some(column_id),
            TypedAlterTableOperation::AlterColumnTypeUsing { column_id, .. } => Some(column_id),
            TypedAlterTableOperation::AddNullableColumn { .. } => unreachable!(),
        };
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn alter_type_using_binds_the_old_table_scope_and_exact_target_type() {
    let (schema, target) = fixture();
    for (sql, expected_column) in [
        (
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING name::BIGINT",
            ColumnId(9),
        ),
        (
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING id",
            ColumnId(7),
        ),
        (
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING projects.name::BIGINT",
            ColumnId(9),
        ),
    ] {
        let AstStatement::AlterTable(ast) = netbadb_parser::parse_statement(sql).unwrap() else {
            panic!("ALTER AST")
        };
        let typed = lower_alter_table(&schema, &ast, &[TableIdentityBinding { target }]).unwrap();
        let TypedAlterTableOperation::AlterColumnTypeUsing {
            column_id,
            target_type,
            using,
        } = typed.operation
        else {
            panic!("typed ALTER TYPE USING")
        };
        assert_eq!(column_id, ColumnId(9));
        assert_eq!(target_type, SemanticType::physical(PhysicalType::Int64));
        assert_eq!(using.expr_type.data_type, target_type);
        let actual = match &using.kind {
            TypedExprKind::Column(column) => column.column_id,
            TypedExprKind::Cast { expression } => match &expression.kind {
                TypedExprKind::Column(column) => column.column_id,
                _ => panic!("cast child column"),
            },
            _ => unreachable!(),
        };
        assert_eq!(actual, expected_column);
    }
}

#[test]
fn alter_type_using_rejects_parameters_mismatch_and_unsupported_casts() {
    let (schema, target) = fixture();
    for (sql, kind) in [
        (
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING $1",
            "parameter",
        ),
        (
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING name",
            "mismatch",
        ),
        (
            "ALTER TABLE projects ALTER COLUMN name TYPE BIGINT USING true::BIGINT",
            "cast",
        ),
    ] {
        let AstStatement::AlterTable(ast) = netbadb_parser::parse_statement(sql).unwrap() else {
            panic!("ALTER AST")
        };
        let error =
            lower_alter_table(&schema, &ast, &[TableIdentityBinding { target }]).unwrap_err();
        assert!(matches!(
            (kind, error),
            ("parameter", HirError::InvalidTableDefinition { .. })
                | ("mismatch", HirError::TypeMismatch { .. })
                | ("cast", HirError::UnsupportedCast { .. })
        ));
    }
}

#[test]
fn add_reuses_create_type_resolution_without_allocating_a_column_id() {
    let (schema, target) = fixture();
    for (name, physical) in [
        ("BOOL", PhysicalType::Bool),
        ("BOOLEAN", PhysicalType::Bool),
        ("BIGINT", PhysicalType::Int64),
        ("INT64", PhysicalType::Int64),
        ("INT8", PhysicalType::Int64),
        ("TEXT", PhysicalType::Text),
        ("VARCHAR", PhysicalType::Text),
        ("UINT64", PhysicalType::UInt64),
    ] {
        let sql = format!("ALTER TABLE projects ADD COLUMN added {name}");
        let AstStatement::AlterTable(ast) = netbadb_parser::parse_statement(&sql).unwrap() else {
            panic!("ALTER AST")
        };
        let typed = lower_alter_table(&schema, &ast, &[TableIdentityBinding { target }]).unwrap();
        assert!(matches!(
            typed.operation,
            TypedAlterTableOperation::AddNullableColumn { name, data_type }
                if name == "added" && data_type == SemanticType::physical(physical)
        ));
    }
}

#[test]
fn alter_lowering_rejects_missing_and_duplicate_names_at_their_spans() {
    let (schema, target) = fixture();
    for (sql, expected) in [
        ("ALTER TABLE projects DROP COLUMN missing", "missing"),
        ("ALTER TABLE projects ADD COLUMN name TEXT", "name"),
        ("ALTER TABLE projects RENAME COLUMN name TO id", "id"),
        ("ALTER TABLE projects ADD COLUMN other MAGIC", "MAGIC"),
    ] {
        let AstStatement::AlterTable(ast) = netbadb_parser::parse_statement(sql).unwrap() else {
            panic!("ALTER AST")
        };
        let error =
            lower_alter_table(&schema, &ast, &[TableIdentityBinding { target }]).unwrap_err();
        assert_eq!(&sql[error.span().start..error.span().end], expected);
    }
}
