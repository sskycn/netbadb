use super::*;

#[test]
fn parses_the_single_unqualified_drop_table_shape_with_exact_spans() {
    for sql in ["DROP TABLE projects", "DROP TABLE projects;"] {
        let Statement::DropTable(drop) = parse_statement(sql).unwrap() else {
            panic!("expected DROP TABLE AST")
        };
        assert_eq!(drop.name.name, "projects");
        assert_eq!(&sql[drop.name.span.start..drop.name.span.end], "projects");
        assert_eq!(&sql[drop.span.start..drop.span.end], "DROP TABLE projects");
    }
}

#[test]
fn rejects_unsupported_and_malformed_drop_table_forms_with_bounded_spans() {
    for sql in [
        "DROP TABLE IF EXISTS projects",
        "DROP TABLE projects CASCADE",
        "DROP TABLE projects RESTRICT",
        "DROP TABLE projects, users",
        "DROP TABLE public.projects",
        "DROP TABLE ONLY projects",
        "DROP SCHEMA projects",
        "DROP VIEW projects",
        "DROP MATERIALIZED VIEW projects",
        "DROP SEQUENCE projects",
        "DROP TYPE projects",
    ] {
        let error = parse_statement(sql).expect_err(sql);
        assert_eq!(error.kind, ParseErrorKind::UnsupportedFeature, "{sql}");
        assert!(error.span.start <= error.span.end && error.span.end <= sql.len());
    }
    for sql in [
        "DROP TABLE",
        "DROP TABLE ;",
        "DROP TABLE projects junk",
        "DROP TABLE $1",
        "DROP TABLE \"projects\"",
        "DROP",
    ] {
        let error = parse_statement(sql).expect_err(sql);
        assert!(error.span.start <= error.span.end && error.span.end <= sql.len());
    }
}

#[test]
fn drop_table_parser_mutations_never_escape_source_bounds() {
    for input in [
        "DR0P TABLE projects",
        "DROP TABL projects",
        "DROP TABLE",
        "DROP TABLE ,",
        "DROP TABLE projects,",
        "DROP TABLE IF",
        "DROP TABLE IF EXIST projects",
        "DROP TABLE projects CASCAD",
        "DROP TABLE projects RESTRIC",
        "DROP TABLE public.",
        "DROP TABLE .projects",
        "DROP TABLE 'projects'",
        "DROP TABLE \"projects\"",
        "DROP TABLE projects trailing",
    ] {
        let error = parse_statement(input).expect_err(input);
        assert!(error.span.start <= error.span.end && error.span.end <= input.len());
    }

    let seed = b"DROP TABLE projects;";
    let mut state = 21_u64;
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
