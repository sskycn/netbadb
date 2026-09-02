use super::{AlterTableAction, ParseErrorKind, Statement, parse_statement};

#[test]
fn parses_six_alter_table_actions_with_exact_spans() {
    let cases = [
        "ALTER TABLE projects RENAME TO work;",
        "ALTER TABLE projects RENAME COLUMN name TO title",
        "ALTER TABLE projects RENAME name TO title",
        "ALTER TABLE projects ADD COLUMN active BOOLEAN",
        "ALTER TABLE projects ADD COLUMN note VARCHAR NULL",
        "ALTER TABLE projects DROP COLUMN active",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL",
        "ALTER TABLE projects ALTER COLUMN name DROP NOT NULL",
    ];
    for sql in cases {
        let Statement::AlterTable(statement) = parse_statement(sql).unwrap() else {
            panic!("expected ALTER TABLE: {sql}");
        };
        assert_eq!(statement.table.name, "projects");
        assert_eq!(
            &sql[statement.table.span.start..statement.table.span.end],
            "projects"
        );
        match statement.action {
            AlterTableAction::RenameTable { new_name } => assert_eq!(new_name.name, "work"),
            AlterTableAction::RenameColumn { old_name, new_name } => {
                assert_eq!(old_name.name, "name");
                assert_eq!(new_name.name, "title");
            }
            AlterTableAction::AddColumn { column } => {
                assert!(matches!(column.name.name.as_str(), "active" | "note"));
                assert!(matches!(
                    column.data_type.name.as_str(),
                    "BOOLEAN" | "VARCHAR"
                ));
            }
            AlterTableAction::DropColumn { name } => assert_eq!(name.name, "active"),
            AlterTableAction::SetNotNull { column_name }
            | AlterTableAction::DropNotNull { column_name } => assert_eq!(column_name.name, "name"),
        }
    }
}

#[test]
fn rejects_unsupported_alter_table_grammar_without_ignoring_clauses() {
    for sql in [
        "ALTER TABLE IF EXISTS t RENAME TO x",
        "ALTER TABLE ONLY t RENAME TO x",
        "ALTER TABLE public.t RENAME TO x",
        "ALTER TABLE t ADD COLUMN c BIGINT NOT NULL",
        "ALTER TABLE t ADD COLUMN c BIGINT DEFAULT NULL",
        "ALTER TABLE t ADD COLUMN c BIGINT PRIMARY KEY",
        "ALTER TABLE t ADD COLUMN c BIGINT UNIQUE",
        "ALTER TABLE t DROP COLUMN IF EXISTS c",
        "ALTER TABLE t DROP COLUMN c CASCADE",
        "ALTER TABLE t DROP COLUMN c RESTRICT",
        "ALTER TABLE t ALTER COLUMN c TYPE TEXT",
        "ALTER TABLE t ALTER COLUMN c SET DATA TYPE TEXT",
        "ALTER TABLE t ALTER COLUMN c SET DEFAULT 1",
        "ALTER TABLE t ALTER COLUMN c DROP DEFAULT",
        "ALTER TABLE t ADD CONSTRAINT c CHECK (true)",
        "ALTER TABLE t DROP CONSTRAINT c",
        "ALTER TABLE t RENAME CONSTRAINT a TO b",
        "ALTER TABLE t ADD COLUMN a BIGINT, ADD COLUMN b BIGINT",
        "ALTER TABLE \"t\" RENAME TO x",
    ] {
        assert!(
            matches!(
                parse_statement(sql),
                Err(error) if matches!(error.kind, ParseErrorKind::UnsupportedFeature | ParseErrorKind::Syntax)
            ),
            "{sql}"
        );
    }
}

#[test]
fn rejects_malformed_and_trailing_alter_table_syntax() {
    for sql in [
        "ALTER TABLE",
        "ALTER TABLE t",
        "ALTER TABLE t RENAME",
        "ALTER TABLE t RENAME TO",
        "ALTER TABLE t RENAME COLUMN c",
        "ALTER TABLE t ADD COLUMN c",
        "ALTER TABLE t DROP COLUMN",
        "ALTER TABLE t ALTER COLUMN c SET NOT",
        "ALTER TABLE t ALTER COLUMN c DROP NOT",
        "ALTER TABLE t RENAME TO x junk",
        "ALTER TABLE t RENAME TO x,",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
}

#[test]
fn deterministic_alter_mutations_never_escape_the_bounded_parser() {
    let seeds = [
        "ALTER TABLE projects RENAME COLUMN name TO title;",
        "ALTER TABLE projects ADD COLUMN active BOOLEAN;",
        "ALTER TABLE projects ALTER COLUMN name SET NOT NULL;",
        "ALTER TABLE projects ALTER COLUMN name DROP NOT NULL;",
    ];
    let replacements = *b" ,.();$'0A";
    let mut cases = 0_usize;
    for seed in seeds {
        for position in 0..seed.len() {
            for replacement in replacements {
                let mut bytes = seed.as_bytes().to_vec();
                bytes[position] = replacement;
                if let Ok(candidate) = std::str::from_utf8(&bytes) {
                    let _ = parse_statement(candidate);
                }
                cases += 1;
            }
        }
    }
    for fragment in [
        "ALTER",
        "ALTER TABLE",
        "ALTER TABLE t RENAME",
        "ALTER TABLE t ADD COLUMN",
        "ALTER TABLE t DROP COLUMN",
        "ALTER TABLE t ALTER COLUMN c SET",
        "ALTER TABLE t ALTER COLUMN c DROP",
        "ALTER TABLE t ALTER COLUMN c SET NOT",
        "ALTER TABLE t ALTER COLUMN c DROP NOT",
    ] {
        for suffix in 0..128_u16 {
            let candidate = format!("{fragment} {suffix} junk ( , )");
            let _ = parse_statement(&candidate);
            cases += 1;
        }
    }
    assert!(cases >= 2_000, "only exercised {cases} mutations");
}
