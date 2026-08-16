//! Schema-driven SQL diagnostics independent of databases and editor protocols.

use netbadb_compiler::{CompileError, compile_statement};
use netbadb_hir::HirError;
use netbadb_parser::Span;
use netbadb_schema::Schema;

/// A half-open UTF-8 byte range into the exact source passed to diagnostics.
///
/// Compiler-produced spans satisfy `start <= end <= source.len()` and both
/// offsets are UTF-8 character boundaries. Editor adapters must convert this
/// byte-oriented representation at their protocol boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextSpan {
    pub start: usize,
    pub end: usize,
}

impl TextSpan {
    /// Returns whether this span can safely index the supplied source.
    #[must_use]
    pub fn is_valid_for(self, source: &str) -> bool {
        self.start <= self.end
            && self.end <= source.len()
            && source.is_char_boundary(self.start)
            && source.is_char_boundary(self.end)
    }
}

/// Stable machine-facing classifications for current parser and HIR errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticCode {
    Parse,
    UnknownTable,
    UnknownColumn,
    UnknownRelationQualifier,
    DuplicateRelationName,
    AmbiguousColumn,
    TooManyRelations,
    TypeMismatch,
    IncompatibleComparison,
    CannotInferNullType,
    DuplicateColumn,
    ValueCountMismatch,
    MissingRequiredColumn,
    NullNotAllowed,
    InsertValueReferencesColumn,
    UngroupedColumn,
    WildcardNotSupportedWithGroupBy,
    OrderByNotSupportedWithGrouping,
    InvalidAggregateArgument,
    InvalidAggregateType,
}

impl DiagnosticCode {
    /// Returns the stable snake-case code exposed to external tooling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::UnknownTable => "unknown_table",
            Self::UnknownColumn => "unknown_column",
            Self::UnknownRelationQualifier => "unknown_relation_qualifier",
            Self::DuplicateRelationName => "duplicate_relation_name",
            Self::AmbiguousColumn => "ambiguous_column",
            Self::TooManyRelations => "too_many_relations",
            Self::TypeMismatch => "type_mismatch",
            Self::IncompatibleComparison => "incompatible_comparison",
            Self::CannotInferNullType => "cannot_infer_null_type",
            Self::DuplicateColumn => "duplicate_column",
            Self::ValueCountMismatch => "value_count_mismatch",
            Self::MissingRequiredColumn => "missing_required_column",
            Self::NullNotAllowed => "null_not_allowed",
            Self::InsertValueReferencesColumn => "insert_value_references_column",
            Self::UngroupedColumn => "ungrouped_column",
            Self::WildcardNotSupportedWithGroupBy => "wildcard_not_supported_with_group_by",
            Self::OrderByNotSupportedWithGrouping => "order_by_not_supported_with_grouping",
            Self::InvalidAggregateArgument => "invalid_aggregate_argument",
            Self::InvalidAggregateType => "invalid_aggregate_type",
        }
    }
}

/// One stable compiler diagnostic with a human message and UTF-8 byte span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolingDiagnostic {
    pub code: DiagnosticCode,
    pub message: String,
    pub span: TextSpan,
}

/// Compiles one statement against a canonical schema and reports its first
/// parse, name-resolution, or type error.
///
/// The current compiler is fail-fast, so this returns either no diagnostics or
/// exactly one diagnostic. It performs no planning, execution, or database I/O.
#[must_use]
pub fn diagnose_statement(schema: &Schema, source: &str) -> Vec<ToolingDiagnostic> {
    match compile_statement(schema, source) {
        Ok(_) => Vec::new(),
        Err(CompileError::Parse(error)) => {
            let diagnostic = ToolingDiagnostic {
                code: DiagnosticCode::Parse,
                message: error.message,
                span: text_span(error.span),
            };
            debug_assert!(diagnostic.span.is_valid_for(source));
            vec![diagnostic]
        }
        Err(CompileError::Hir(error)) => {
            let diagnostic = ToolingDiagnostic {
                code: hir_code(&error),
                message: error.to_string(),
                span: text_span(error.span()),
            };
            debug_assert!(diagnostic.span.is_valid_for(source));
            vec![diagnostic]
        }
    }
}

const fn text_span(span: Span) -> TextSpan {
    TextSpan {
        start: span.start,
        end: span.end,
    }
}

const fn hir_code(error: &HirError) -> DiagnosticCode {
    match error {
        HirError::UnknownTable { .. } => DiagnosticCode::UnknownTable,
        HirError::UnknownColumn { .. } => DiagnosticCode::UnknownColumn,
        HirError::UnknownRelationQualifier { .. } => DiagnosticCode::UnknownRelationQualifier,
        HirError::DuplicateRelationName { .. } => DiagnosticCode::DuplicateRelationName,
        HirError::AmbiguousColumn { .. } => DiagnosticCode::AmbiguousColumn,
        HirError::TooManyRelations { .. } => DiagnosticCode::TooManyRelations,
        HirError::TypeMismatch { .. } => DiagnosticCode::TypeMismatch,
        HirError::IncompatibleComparison { .. } => DiagnosticCode::IncompatibleComparison,
        HirError::CannotInferNullType { .. } => DiagnosticCode::CannotInferNullType,
        HirError::DuplicateColumn { .. } => DiagnosticCode::DuplicateColumn,
        HirError::ValueCountMismatch { .. } => DiagnosticCode::ValueCountMismatch,
        HirError::MissingRequiredColumn { .. } => DiagnosticCode::MissingRequiredColumn,
        HirError::NullNotAllowed { .. } => DiagnosticCode::NullNotAllowed,
        HirError::InsertValueReferencesColumn { .. } => DiagnosticCode::InsertValueReferencesColumn,
        HirError::UngroupedColumn { .. } => DiagnosticCode::UngroupedColumn,
        HirError::WildcardNotSupportedWithGroupBy { .. } => {
            DiagnosticCode::WildcardNotSupportedWithGroupBy
        }
        HirError::OrderByNotSupportedWithGrouping { .. } => {
            DiagnosticCode::OrderByNotSupportedWithGrouping
        }
        HirError::InvalidAggregateArgument { .. } => DiagnosticCode::InvalidAggregateArgument,
        HirError::InvalidAggregateType { .. } => DiagnosticCode::InvalidAggregateType,
    }
}

#[cfg(test)]
mod tests {
    use netbadb_schema::{ColumnDef, Schema, TableDef, TypeSpec};
    use netbadb_types::{ColumnId, PhysicalType, TableId};

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            TableDef::new(
                TableId(1),
                "users",
                vec![
                    ColumnDef::new(
                        ColumnId(1),
                        "user_id",
                        TypeSpec::Semantic {
                            name: "UserId".into(),
                            physical: PhysicalType::UInt64,
                        },
                    ),
                    ColumnDef::new(
                        ColumnId(2),
                        "team_id",
                        TypeSpec::Semantic {
                            name: "TeamId".into(),
                            physical: PhysicalType::UInt64,
                        },
                    ),
                    ColumnDef::new(ColumnId(3), "name", TypeSpec::Physical(PhysicalType::Text)),
                    ColumnDef::new(
                        ColumnId(4),
                        "nickname",
                        TypeSpec::Physical(PhysicalType::Text),
                    )
                    .nullable(true),
                    ColumnDef::new(
                        ColumnId(5),
                        "active",
                        TypeSpec::Physical(PhysicalType::Bool),
                    )
                    .nullable(true),
                ],
            ),
            TableDef::new(
                TableId(2),
                "teams",
                vec![ColumnDef::new(
                    ColumnId(1),
                    "team_id",
                    TypeSpec::Semantic {
                        name: "TeamId".into(),
                        physical: PhysicalType::UInt64,
                    },
                )],
            ),
        ])
        .expect("valid schema")
    }

    fn diagnostic(source: &str) -> ToolingDiagnostic {
        let diagnostics = diagnose_statement(&schema(), source);
        assert_eq!(diagnostics.len(), 1, "{source}");
        let diagnostic = diagnostics.into_iter().next().expect("one diagnostic");
        assert!(diagnostic.span.is_valid_for(source));
        diagnostic
    }

    fn assert_code_and_slice(source: &str, code: DiagnosticCode, slice: &str) {
        let diagnostic = diagnostic(source);
        assert_eq!(diagnostic.code, code);
        assert_eq!(&source[diagnostic.span.start..diagnostic.span.end], slice);
    }

    #[test]
    fn maps_parse_name_ambiguity_and_type_errors() {
        let parse = diagnostic("SELECT FROM users");
        assert_eq!(parse.code, DiagnosticCode::Parse);
        assert!(!parse.message.contains(" at "));
        assert_eq!(
            &"SELECT FROM users"[parse.span.start..parse.span.end],
            "FROM"
        );

        assert_code_and_slice(
            "SELECT user_id FROM missing",
            DiagnosticCode::UnknownTable,
            "missing",
        );
        assert_code_and_slice(
            "SELECT missing FROM users",
            DiagnosticCode::UnknownColumn,
            "missing",
        );
        assert_code_and_slice(
            "SELECT user_id FROM users u JOIN users v ON u.user_id = v.user_id",
            DiagnosticCode::AmbiguousColumn,
            "user_id",
        );
        assert_code_and_slice(
            "SELECT user_id FROM users WHERE name",
            DiagnosticCode::TypeMismatch,
            "name",
        );
    }

    #[test]
    fn preserves_nominal_null_grouping_and_aggregate_semantics() {
        assert_code_and_slice(
            "SELECT user_id FROM users WHERE user_id = team_id",
            DiagnosticCode::IncompatibleComparison,
            "user_id = team_id",
        );
        let null_error = HirError::CannotInferNullType {
            span: Span { start: 0, end: 4 },
        };
        assert_eq!(hir_code(&null_error), DiagnosticCode::CannotInferNullType);
        assert_code_and_slice(
            "SELECT user_id, COUNT(*) FROM users",
            DiagnosticCode::UngroupedColumn,
            "user_id",
        );
        assert_code_and_slice(
            "SELECT SUM(name) FROM users",
            DiagnosticCode::InvalidAggregateType,
            "SUM(name)",
        );
    }

    #[test]
    fn maps_insert_update_and_delete_errors() {
        assert_code_and_slice(
            "INSERT INTO users (name, name) VALUES ('a', 'b')",
            DiagnosticCode::DuplicateColumn,
            "name",
        );
        let mismatch = diagnostic("INSERT INTO users (user_id, name) VALUES (NULL)");
        assert_eq!(mismatch.code, DiagnosticCode::ValueCountMismatch);
        assert_eq!(
            &"INSERT INTO users (user_id, name) VALUES (NULL)"
                [mismatch.span.start..mismatch.span.end],
            "INSERT INTO users (user_id, name) VALUES (NULL)"
        );
        assert_code_and_slice(
            "UPDATE users SET missing = 1",
            DiagnosticCode::UnknownColumn,
            "missing",
        );
        assert_code_and_slice(
            "UPDATE users SET name = true",
            DiagnosticCode::TypeMismatch,
            "true",
        );
        assert_code_and_slice(
            "DELETE FROM users WHERE name",
            DiagnosticCode::TypeMismatch,
            "name",
        );
    }

    #[test]
    fn byte_spans_remain_exact_after_multibyte_text() {
        let source = "SELECT user_id FROM users WHERE name = '😀' AND missing = 1";
        assert_code_and_slice(source, DiagnosticCode::UnknownColumn, "missing");
        let diagnostic = diagnostic(source);
        assert!(source[..diagnostic.span.start].chars().count() < diagnostic.span.start);
    }

    #[test]
    fn valid_statements_have_no_diagnostics() {
        assert!(diagnose_statement(&schema(), "SELECT user_id FROM users").is_empty());
        assert!(diagnose_statement(&schema(), "DELETE FROM users WHERE active").is_empty());
    }

    #[test]
    fn every_code_has_an_explicit_stable_spelling() {
        let codes = [
            (DiagnosticCode::Parse, "parse"),
            (DiagnosticCode::UnknownTable, "unknown_table"),
            (DiagnosticCode::UnknownColumn, "unknown_column"),
            (
                DiagnosticCode::UnknownRelationQualifier,
                "unknown_relation_qualifier",
            ),
            (
                DiagnosticCode::DuplicateRelationName,
                "duplicate_relation_name",
            ),
            (DiagnosticCode::AmbiguousColumn, "ambiguous_column"),
            (DiagnosticCode::TooManyRelations, "too_many_relations"),
            (DiagnosticCode::TypeMismatch, "type_mismatch"),
            (
                DiagnosticCode::IncompatibleComparison,
                "incompatible_comparison",
            ),
            (
                DiagnosticCode::CannotInferNullType,
                "cannot_infer_null_type",
            ),
            (DiagnosticCode::DuplicateColumn, "duplicate_column"),
            (DiagnosticCode::ValueCountMismatch, "value_count_mismatch"),
            (
                DiagnosticCode::MissingRequiredColumn,
                "missing_required_column",
            ),
            (DiagnosticCode::NullNotAllowed, "null_not_allowed"),
            (
                DiagnosticCode::InsertValueReferencesColumn,
                "insert_value_references_column",
            ),
            (DiagnosticCode::UngroupedColumn, "ungrouped_column"),
            (
                DiagnosticCode::WildcardNotSupportedWithGroupBy,
                "wildcard_not_supported_with_group_by",
            ),
            (
                DiagnosticCode::OrderByNotSupportedWithGrouping,
                "order_by_not_supported_with_grouping",
            ),
            (
                DiagnosticCode::InvalidAggregateArgument,
                "invalid_aggregate_argument",
            ),
            (
                DiagnosticCode::InvalidAggregateType,
                "invalid_aggregate_type",
            ),
        ];
        for (code, expected) in codes {
            assert_eq!(code.as_str(), expected);
        }
    }
}
