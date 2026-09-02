//! A deliberately small, typed-query parser for the first vertical slice.

use std::error::Error;
use std::fmt;

use netbadb_types::ParameterId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub projection: Vec<SelectItem>,
    pub from: Option<FromItem>,
    pub joins: Vec<Join>,
    pub selection: Option<Expr>,
    pub group_by: Vec<ColumnName>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromItem {
    pub table: Ident,
    pub alias: Option<Ident>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub kind: JoinKind,
    pub right: FromItem,
    pub condition: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem {
    pub column: ColumnName,
    pub direction: Option<SortDirection>,
    pub null_order: Option<NullOrder>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnName {
    pub qualifier: Option<Ident>,
    pub name: Ident,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Select(Query),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
}

/// A logical declaration: persistent identities belong exclusively to Core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub name: Ident,
    pub columns: Vec<CreateColumn>,
    pub span: Span,
}

/// A logical unqualified table name. Persistent identities are resolved by HIR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub name: Ident,
    pub span: Span,
}

/// One generic table-schema mutation. Persistent identities are resolved by HIR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableStatement {
    pub table: Ident,
    pub action: AlterTableAction,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    RenameTable { new_name: Ident },
    RenameColumn { old_name: Ident, new_name: Ident },
    AddColumn { column: CreateColumn },
    DropColumn { name: Ident },
    SetNotNull { column_name: Ident },
    DropNotNull { column_name: Ident },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateColumn {
    pub name: Ident,
    pub data_type: Ident,
    /// None means the SQL default (nullable).
    pub nullability: Option<ColumnNullability>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnNullability {
    pub nullable: bool,
    pub span: Span,
}

/// Bounds apply before allocation or recursive expression construction.
pub const MAX_SQL_BYTES: usize = 1_048_576;
pub const MAX_SQL_TOKENS: usize = 32_768;
pub const MAX_IDENTIFIER_BYTES: usize = 1024;
pub const MAX_CREATE_COLUMNS: usize = 4096;
const MAX_EXPRESSION_NODES: usize = 256;
const MAX_EXPRESSION_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStatement {
    pub name: ColumnName,
    pub if_exists: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub name: Ident,
    pub table: ColumnName,
    pub column: Ident,
    pub if_not_exists: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table: Ident,
    /// Empty means the canonical declaration order (no explicit column list).
    pub columns: Vec<Ident>,
    pub values: Vec<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub column: Ident,
    pub value: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table: Ident,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table: Ident,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Wildcard(Span),
    Column(ColumnName),
    Aggregate(AggregateCall),
    Expression {
        expression: Expr,
        alias: Option<Ident>,
        span: Span,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArgument {
    Star(Span),
    Column(ColumnName),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateCall {
    pub function: AggregateFunction,
    pub argument: AggregateArgument,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(ColumnName),
    Literal {
        value: Literal,
        span: Span,
    },
    Parameter {
        id: ParameterId,
        span: Span,
    },
    Cast {
        expression: Box<Expr>,
        data_type: SqlTypeName,
        span: Span,
    },
    Binary {
        left: Box<Expr>,
        operator: BinaryOp,
        right: Box<Expr>,
        span: Span,
    },
    Unary {
        operator: UnaryOp,
        expression: Box<Expr>,
        span: Span,
    },
    IsNull {
        expression: Box<Expr>,
        negated: bool,
        span: Span,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTypeName {
    Bool,
    Int64,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    String(String),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub kind: ParseErrorKind,
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseErrorKind {
    Syntax,
    UnsupportedFeature,
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}..{}",
            self.message, self.span.start, self.span.end
        )
    }
}

impl Error for ParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenKind {
    Select,
    From,
    Where,
    Group,
    Order,
    By,
    Asc,
    Desc,
    Nulls,
    First,
    Last,
    Limit,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Drop,
    Index,
    If,
    Exists,
    As,
    Join,
    Inner,
    On,
    And,
    Or,
    Is,
    Not,
    True,
    False,
    Null,
    Ident(String),
    Number(i64),
    String(String),
    Parameter(ParameterId),
    ColonColon,
    Comma,
    Dot,
    Star,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    LParen,
    RParen,
    Semicolon,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    span: Span,
}

pub fn parse(input: &str) -> Result<Query, ParseError> {
    match parse_statement(input)? {
        Statement::Select(query) => Ok(query),
        statement => Err(ParseError {
            kind: ParseErrorKind::Syntax,
            message: "expected a SELECT statement".into(),
            span: statement_span(&statement),
        }),
    }
}

pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    let tokens = tokenize(input)?;
    Parser {
        tokens,
        position: 0,
        expression_nodes: 0,
        expression_depth: 0,
    }
    .parse_statement()
}

fn tokenize(input: &str) -> Result<Vec<Token>, ParseError> {
    if input.len() > MAX_SQL_BYTES {
        return Err(ParseError {
            kind: ParseErrorKind::Syntax,
            message: "SQL byte limit exceeded".into(),
            span: Span { start: 0, end: 0 },
        });
    }
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut position = 0;

    while position < bytes.len() {
        let byte = bytes[position];
        if byte.is_ascii_whitespace() {
            position += 1;
            continue;
        }
        if tokens.len() >= MAX_SQL_TOKENS {
            return Err(ParseError {
                kind: ParseErrorKind::Syntax,
                message: "SQL token limit exceeded".into(),
                span: Span {
                    start: position,
                    end: position,
                },
            });
        }
        let start = position;
        let token = match byte {
            b',' => {
                position += 1;
                TokenKind::Comma
            }
            b'.' => {
                position += 1;
                TokenKind::Dot
            }
            b':' if bytes.get(position + 1) == Some(&b':') => {
                position += 2;
                TokenKind::ColonColon
            }
            b'*' => {
                position += 1;
                TokenKind::Star
            }
            b'(' => {
                position += 1;
                TokenKind::LParen
            }
            b')' => {
                position += 1;
                TokenKind::RParen
            }
            b';' => {
                position += 1;
                TokenKind::Semicolon
            }
            b'=' => {
                position += 1;
                TokenKind::Eq
            }
            b'!' if bytes.get(position + 1) == Some(&b'=') => {
                position += 2;
                TokenKind::NotEq
            }
            b'<' if bytes.get(position + 1) == Some(&b'=') => {
                position += 2;
                TokenKind::LtEq
            }
            b'>' if bytes.get(position + 1) == Some(&b'=') => {
                position += 2;
                TokenKind::GtEq
            }
            b'<' => {
                position += 1;
                TokenKind::Lt
            }
            b'>' => {
                position += 1;
                TokenKind::Gt
            }
            b'$' => {
                position += 1;
                let digits_start = position;
                while bytes.get(position).is_some_and(u8::is_ascii_digit) {
                    position += 1;
                }
                if digits_start == position {
                    return Err(ParseError {
                        kind: ParseErrorKind::Syntax,
                        message: "parameter marker expects a positive integer".into(),
                        span: Span {
                            start,
                            end: position,
                        },
                    });
                }
                let ordinal =
                    input[digits_start..position]
                        .parse::<u32>()
                        .map_err(|_| ParseError {
                            kind: ParseErrorKind::Syntax,
                            message: "parameter number is too large".into(),
                            span: Span {
                                start,
                                end: position,
                            },
                        })?;
                let zero_based = ordinal.checked_sub(1).ok_or_else(|| ParseError {
                    kind: ParseErrorKind::Syntax,
                    message: "parameter numbers start at $1".into(),
                    span: Span {
                        start,
                        end: position,
                    },
                })?;
                TokenKind::Parameter(ParameterId(zero_based))
            }
            b'\'' => {
                position += 1;
                let content_start = position;
                while position < bytes.len() && bytes[position] != b'\'' {
                    position += 1;
                }
                if position == bytes.len() {
                    return Err(ParseError {
                        kind: ParseErrorKind::Syntax,
                        message: "unterminated string literal".into(),
                        span: Span {
                            start,
                            end: position,
                        },
                    });
                }
                let value = input[content_start..position].to_owned();
                position += 1;
                TokenKind::String(value)
            }
            byte if byte.is_ascii_digit() || byte == b'-' => {
                position += 1;
                while bytes.get(position).is_some_and(u8::is_ascii_digit) {
                    position += 1;
                }
                let value = input[start..position]
                    .parse::<i64>()
                    .map_err(|_| ParseError {
                        kind: ParseErrorKind::Syntax,
                        message: "invalid integer literal".into(),
                        span: Span {
                            start,
                            end: position,
                        },
                    })?;
                TokenKind::Number(value)
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                position += 1;
                while bytes
                    .get(position)
                    .is_some_and(|value| value.is_ascii_alphanumeric() || *value == b'_')
                {
                    position += 1;
                }
                if position - start > MAX_IDENTIFIER_BYTES {
                    return Err(ParseError {
                        kind: ParseErrorKind::Syntax,
                        message: "identifier byte limit exceeded".into(),
                        span: Span {
                            start,
                            end: position,
                        },
                    });
                }
                match input[start..position].to_ascii_uppercase().as_str() {
                    "SELECT" => TokenKind::Select,
                    "FROM" => TokenKind::From,
                    "WHERE" => TokenKind::Where,
                    "GROUP" => TokenKind::Group,
                    "ORDER" => TokenKind::Order,
                    "BY" => TokenKind::By,
                    "ASC" => TokenKind::Asc,
                    "DESC" => TokenKind::Desc,
                    "NULLS" => TokenKind::Nulls,
                    "FIRST" => TokenKind::First,
                    "LAST" => TokenKind::Last,
                    "LIMIT" => TokenKind::Limit,
                    "INSERT" => TokenKind::Insert,
                    "INTO" => TokenKind::Into,
                    "VALUES" => TokenKind::Values,
                    "UPDATE" => TokenKind::Update,
                    "SET" => TokenKind::Set,
                    "DELETE" => TokenKind::Delete,
                    "CREATE" => TokenKind::Create,
                    "DROP" => TokenKind::Drop,
                    "INDEX" => TokenKind::Index,
                    "IF" => TokenKind::If,
                    "EXISTS" => TokenKind::Exists,
                    "AS" => TokenKind::As,
                    "JOIN" => TokenKind::Join,
                    "INNER" => TokenKind::Inner,
                    "ON" => TokenKind::On,
                    "AND" => TokenKind::And,
                    "OR" => TokenKind::Or,
                    "IS" => TokenKind::Is,
                    "NOT" => TokenKind::Not,
                    "TRUE" => TokenKind::True,
                    "FALSE" => TokenKind::False,
                    "NULL" => TokenKind::Null,
                    _ => TokenKind::Ident(input[start..position].to_owned()),
                }
            }
            _ => {
                let character = input[start..].chars().next().unwrap_or('\0');
                let end = start + character.len_utf8();
                return Err(ParseError {
                    kind: ParseErrorKind::Syntax,
                    message: format!("unexpected character `{}`", character.escape_default()),
                    span: Span { start, end },
                });
            }
        };
        tokens.push(Token {
            kind: token,
            span: Span {
                start,
                end: position,
            },
        });
    }

    tokens.push(Token {
        kind: TokenKind::Eof,
        span: Span {
            start: input.len(),
            end: input.len(),
        },
    });
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
    expression_nodes: usize,
    expression_depth: usize,
}

impl Parser {
    fn parse_statement(mut self) -> Result<Statement, ParseError> {
        let statement = match self.current().kind {
            TokenKind::Select => Statement::Select(self.parse_query()?),
            TokenKind::Insert => Statement::Insert(self.parse_insert()?),
            TokenKind::Update => Statement::Update(self.parse_update()?),
            TokenKind::Delete => Statement::Delete(self.parse_delete()?),
            TokenKind::Create => self.parse_create()?,
            TokenKind::Drop => self.parse_drop()?,
            TokenKind::Ident(ref name) if name.eq_ignore_ascii_case("alter") => {
                Statement::AlterTable(self.parse_alter_table()?)
            }
            _ => {
                return Err(self.error_here(
                    "expected SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX, or DROP INDEX",
                ));
            }
        };
        if self.matches(&TokenKind::Semicolon) {
            self.position += 1;
        }
        self.expect_simple(TokenKind::Eof)?;
        Ok(statement)
    }

    fn contextual(&self, word: &str) -> bool {
        matches!(&self.current().kind, TokenKind::Ident(name) if name.eq_ignore_ascii_case(word))
    }

    fn unsupported(&self, feature: &str) -> ParseError {
        ParseError {
            kind: ParseErrorKind::UnsupportedFeature,
            message: format!("{feature} is not supported"),
            span: self.current().span,
        }
    }

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        // TABLE and new clause words remain contextual, preserving identifier policy.
        let start = self.current().span.start;
        self.position += 1;
        if self.contextual("table") {
            self.position += 1;
            return self.parse_create_table(start).map(Statement::CreateTable);
        }
        if ["temp", "temporary", "unlogged"]
            .iter()
            .any(|word| self.contextual(word))
        {
            return Err(self.unsupported("temporary/unlogged CREATE TABLE"));
        }
        self.position -= 1;
        self.parse_create_index().map(Statement::CreateIndex)
    }

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        let object = self.tokens.get(self.position + 1).unwrap_or(self.current());
        match &object.kind {
            TokenKind::Index => self.parse_drop_index().map(Statement::DropIndex),
            TokenKind::Ident(name) if name.eq_ignore_ascii_case("table") => {
                self.parse_drop_table().map(Statement::DropTable)
            }
            TokenKind::Ident(_) => Err(ParseError {
                kind: ParseErrorKind::UnsupportedFeature,
                message: "DROP object type is not supported".into(),
                span: object.span,
            }),
            _ => Err(ParseError {
                kind: ParseErrorKind::Syntax,
                message: "DROP expects TABLE or INDEX".into(),
                span: object.span,
            }),
        }
    }

    fn parse_drop_table(&mut self) -> Result<DropTableStatement, ParseError> {
        let start = self.expect_simple(TokenKind::Drop)?.span.start;
        if !self.contextual("table") {
            return Err(self.error_here("DROP expects TABLE"));
        }
        self.position += 1;
        if self.matches(&TokenKind::If) {
            return Err(self.unsupported("DROP TABLE IF EXISTS"));
        }
        if self.contextual("only") {
            return Err(self.unsupported("DROP TABLE ONLY"));
        }
        let name = self.expect_ident()?;
        if self.matches(&TokenKind::Dot) {
            return Err(self.unsupported("qualified DROP TABLE name"));
        }
        if self.matches(&TokenKind::Comma) {
            return Err(self.unsupported("multi-table DROP TABLE"));
        }
        if self.contextual("cascade") || self.contextual("restrict") {
            return Err(self.unsupported("DROP TABLE dependency behavior"));
        }
        Ok(DropTableStatement {
            span: Span {
                start,
                end: name.span.end,
            },
            name,
        })
    }

    fn expect_contextual(&mut self, word: &str) -> Result<Token, ParseError> {
        if self.contextual(word) {
            let token = self.current().clone();
            self.position += 1;
            Ok(token)
        } else {
            Err(self.error_here(&format!("expected {word}")))
        }
    }

    fn reject_alter_tail(&self) -> Result<(), ParseError> {
        if self.matches(&TokenKind::Comma) {
            return Err(self.unsupported("multiple ALTER TABLE actions"));
        }
        if self.contextual("cascade") || self.contextual("restrict") {
            return Err(self.unsupported("ALTER TABLE dependency behavior"));
        }
        Ok(())
    }

    fn parse_alter_table(&mut self) -> Result<AlterTableStatement, ParseError> {
        let start = self.expect_contextual("alter")?.span.start;
        self.expect_contextual("table")?;
        if self.matches(&TokenKind::If) {
            return Err(self.unsupported("ALTER TABLE IF EXISTS"));
        }
        if self.contextual("only") {
            return Err(self.unsupported("ALTER TABLE ONLY"));
        }
        let table = self.expect_ident()?;
        if self.matches(&TokenKind::Dot) {
            return Err(self.unsupported("qualified ALTER TABLE name"));
        }

        let action = if self.contextual("rename") {
            self.position += 1;
            if self.contextual("to") {
                self.position += 1;
                AlterTableAction::RenameTable {
                    new_name: self.expect_ident()?,
                }
            } else {
                if self.contextual("constraint") {
                    return Err(self.unsupported("ALTER TABLE RENAME CONSTRAINT"));
                }
                if self.contextual("column") {
                    self.position += 1;
                }
                let old_name = self.expect_ident()?;
                self.expect_contextual("to")?;
                AlterTableAction::RenameColumn {
                    old_name,
                    new_name: self.expect_ident()?,
                }
            }
        } else if self.contextual("add") {
            self.position += 1;
            if self.contextual("constraint") {
                return Err(self.unsupported("ALTER TABLE ADD CONSTRAINT"));
            }
            self.expect_contextual("column")?;
            let name = self.expect_ident()?;
            let data_type = self.expect_ident()?;
            if self.matches(&TokenKind::LParen) {
                return Err(self.unsupported("type modifiers/length constraints"));
            }
            let nullability = if self.matches(&TokenKind::Null) {
                let token = self.current().clone();
                self.position += 1;
                Some(ColumnNullability {
                    nullable: true,
                    span: token.span,
                })
            } else if self.matches(&TokenKind::Not) {
                return Err(self.unsupported("ALTER TABLE ADD COLUMN NOT NULL"));
            } else {
                None
            };
            if ["default", "primary", "unique", "check", "references"]
                .iter()
                .any(|word| self.contextual(word))
            {
                return Err(self.unsupported("ALTER TABLE ADD COLUMN clause"));
            }
            let span = Span {
                start: name.span.start,
                end: nullability.map_or(data_type.span.end, |value| value.span.end),
            };
            AlterTableAction::AddColumn {
                column: CreateColumn {
                    name,
                    data_type,
                    nullability,
                    span,
                },
            }
        } else if self.matches(&TokenKind::Drop) {
            self.position += 1;
            if self.contextual("constraint") {
                return Err(self.unsupported("ALTER TABLE DROP CONSTRAINT"));
            }
            self.expect_contextual("column")?;
            if self.matches(&TokenKind::If) {
                return Err(self.unsupported("ALTER TABLE DROP COLUMN IF EXISTS"));
            }
            AlterTableAction::DropColumn {
                name: self.expect_ident()?,
            }
        } else if self.contextual("alter") {
            self.position += 1;
            self.expect_contextual("column")?;
            let column_name = self.expect_ident()?;
            if self.matches(&TokenKind::Set) {
                self.position += 1;
                if self.contextual("default") {
                    return Err(self.unsupported("ALTER COLUMN SET DEFAULT"));
                }
                if self.contextual("data") || self.contextual("type") {
                    return Err(self.unsupported("ALTER COLUMN physical type conversion"));
                }
                self.expect_simple(TokenKind::Not)?;
                self.expect_simple(TokenKind::Null)?;
                AlterTableAction::SetNotNull { column_name }
            } else if self.matches(&TokenKind::Drop) {
                self.position += 1;
                if self.contextual("default") {
                    return Err(self.unsupported("ALTER COLUMN DROP DEFAULT"));
                }
                self.expect_simple(TokenKind::Not)?;
                self.expect_simple(TokenKind::Null)?;
                AlterTableAction::DropNotNull { column_name }
            } else if self.contextual("type") || self.contextual("using") {
                return Err(self.unsupported("ALTER COLUMN physical type conversion"));
            } else {
                return Err(self.error_here("expected SET NOT NULL or DROP NOT NULL"));
            }
        } else {
            return Err(self.unsupported("ALTER TABLE action"));
        };
        self.reject_alter_tail()?;
        Ok(AlterTableStatement {
            table,
            action,
            span: Span {
                start,
                end: self.current().span.start,
            },
        })
    }

    fn reject_table_clause(&self) -> Result<(), ParseError> {
        if [
            "primary",
            "unique",
            "default",
            "check",
            "references",
            "foreign",
            "generated",
            "identity",
            "collate",
            "constraint",
            "exclude",
            "like",
            "inherits",
            "partition",
            "using",
            "with",
            "tablespace",
        ]
        .iter()
        .any(|word| self.contextual(word))
            || matches!(
                self.current().kind,
                TokenKind::As | TokenKind::On | TokenKind::If
            )
        {
            return Err(self.unsupported("CREATE TABLE clause"));
        }
        Ok(())
    }

    fn parse_create_table(&mut self, start: usize) -> Result<CreateTableStatement, ParseError> {
        self.reject_table_clause()?;
        let name = self.expect_ident()?;
        if self.matches(&TokenKind::Dot) {
            return Err(self.unsupported("qualified CREATE TABLE name"));
        }
        self.reject_table_clause()?;
        self.expect_simple(TokenKind::LParen)?;
        let mut columns = Vec::new();
        loop {
            self.reject_table_clause()?;
            if columns.len() >= MAX_CREATE_COLUMNS {
                return Err(self.error_here("CREATE TABLE column limit exceeded"));
            }
            let name = self.expect_ident()?;
            let data_type = self.expect_ident()?;
            if self.matches(&TokenKind::LParen) {
                return Err(self.unsupported("type modifiers/length constraints"));
            }
            let nullability = if self.matches(&TokenKind::Not) || self.matches(&TokenKind::Null) {
                let start = self.current().span.start;
                let nullable = !self.matches(&TokenKind::Not);
                if !nullable {
                    self.position += 1;
                }
                let end = self.expect_simple(TokenKind::Null)?.span.end;
                Some(ColumnNullability {
                    nullable,
                    span: Span { start, end },
                })
            } else {
                None
            };
            if self.matches(&TokenKind::Not) || self.matches(&TokenKind::Null) {
                return Err(self.error_here("duplicate or conflicting nullability constraint"));
            }
            self.reject_table_clause()?;
            let span = Span {
                start: name.span.start,
                end: nullability.map_or(data_type.span.end, |n| n.span.end),
            };
            columns.push(CreateColumn {
                name,
                data_type,
                nullability,
                span,
            });
            if !self.matches(&TokenKind::Comma) {
                break;
            }
            self.position += 1;
        }
        let end = self.expect_simple(TokenKind::RParen)?.span.end;
        self.reject_table_clause()?;
        Ok(CreateTableStatement {
            name,
            columns,
            span: Span { start, end },
        })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexStatement, ParseError> {
        let start = self.expect_simple(TokenKind::Drop)?.span.start;
        self.expect_simple(TokenKind::Index)?;
        let if_exists = if self.matches(&TokenKind::If) {
            self.position += 1;
            self.expect_simple(TokenKind::Exists)?;
            true
        } else {
            false
        };
        let first = self.expect_ident()?;
        let name = if self.matches(&TokenKind::Dot) {
            self.position += 1;
            let name = self.expect_ident()?;
            ColumnName {
                span: Span {
                    start: first.span.start,
                    end: name.span.end,
                },
                qualifier: Some(first),
                name,
            }
        } else {
            ColumnName {
                span: first.span,
                qualifier: None,
                name: first,
            }
        };
        Ok(DropIndexStatement {
            name,
            if_exists,
            span: Span {
                start,
                end: self.current().span.start,
            },
        })
    }

    fn parse_create_index(&mut self) -> Result<CreateIndexStatement, ParseError> {
        let start = self.expect_simple(TokenKind::Create)?.span.start;
        self.expect_simple(TokenKind::Index)?;
        let if_not_exists = if self.matches(&TokenKind::If) {
            self.position += 1;
            self.expect_simple(TokenKind::Not)?;
            self.expect_simple(TokenKind::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        self.expect_simple(TokenKind::On)?;
        let first = self.expect_ident()?;
        let table = if self.matches(&TokenKind::Dot) {
            self.position += 1;
            let relation = self.expect_ident()?;
            let span = Span {
                start: first.span.start,
                end: relation.span.end,
            };
            ColumnName {
                qualifier: Some(first),
                name: relation,
                span,
            }
        } else {
            ColumnName {
                span: first.span,
                qualifier: None,
                name: first,
            }
        };
        self.expect_simple(TokenKind::LParen)?;
        let column = self.expect_ident()?;
        self.expect_simple(TokenKind::RParen)?;
        Ok(CreateIndexStatement {
            name,
            table,
            column,
            if_not_exists,
            span: Span {
                start,
                end: self.current().span.start,
            },
        })
    }

    fn parse_query(&mut self) -> Result<Query, ParseError> {
        let start = self.expect_simple(TokenKind::Select)?.span.start;
        let projection = self.parse_projection()?;
        let from = if self.matches(&TokenKind::From) {
            self.position += 1;
            Some(self.parse_from_item()?)
        } else {
            None
        };
        let mut joins = Vec::new();
        while self.matches(&TokenKind::Join) || self.matches(&TokenKind::Inner) {
            joins.push(self.parse_join()?);
        }
        let selection = if self.matches(&TokenKind::Where) {
            self.position += 1;
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        let group_by = if self.matches(&TokenKind::Group) {
            self.parse_group_by()?
        } else {
            Vec::new()
        };
        let order_by = if self.matches(&TokenKind::Order) {
            self.parse_order_by()?
        } else {
            Vec::new()
        };
        let limit = if self.matches(&TokenKind::Limit) {
            self.position += 1;
            let token = self.current().clone();
            match token.kind {
                TokenKind::Number(value) if value >= 0 => {
                    self.position += 1;
                    Some(value as u64)
                }
                _ => return Err(self.error_here("LIMIT expects a non-negative integer")),
            }
        } else {
            None
        };
        let end = self.current().span.start;
        Ok(Query {
            projection,
            from,
            joins,
            selection,
            group_by,
            order_by,
            limit,
            span: Span { start, end },
        })
    }

    fn parse_group_by(&mut self) -> Result<Vec<ColumnName>, ParseError> {
        self.expect_simple(TokenKind::Group)?;
        self.expect_simple(TokenKind::By)?;
        let mut columns = vec![self.parse_column_name()?];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            columns.push(self.parse_column_name()?);
        }
        Ok(columns)
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByItem>, ParseError> {
        self.expect_simple(TokenKind::Order)?;
        self.expect_simple(TokenKind::By)?;
        let mut items = vec![self.parse_order_by_item()?];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, ParseError> {
        let column = self.parse_column_name()?;
        let mut end = column.span.end;
        let direction = if self.matches(&TokenKind::Asc) {
            end = self.current().span.end;
            self.position += 1;
            Some(SortDirection::Asc)
        } else if self.matches(&TokenKind::Desc) {
            end = self.current().span.end;
            self.position += 1;
            Some(SortDirection::Desc)
        } else {
            None
        };
        let null_order = if self.matches(&TokenKind::Nulls) {
            self.position += 1;
            let token = self.current().clone();
            let null_order = match token.kind {
                TokenKind::First => NullOrder::First,
                TokenKind::Last => NullOrder::Last,
                _ => return Err(self.error_here("NULLS expects FIRST or LAST")),
            };
            end = token.span.end;
            self.position += 1;
            Some(null_order)
        } else {
            None
        };
        Ok(OrderByItem {
            span: Span {
                start: column.span.start,
                end,
            },
            column,
            direction,
            null_order,
        })
    }

    fn parse_from_item(&mut self) -> Result<FromItem, ParseError> {
        let table = self.expect_ident()?;
        let alias = if self.matches(&TokenKind::As) {
            self.position += 1;
            Some(self.expect_ident()?)
        } else if matches!(self.current().kind, TokenKind::Ident(_)) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        let end = alias
            .as_ref()
            .map_or(table.span.end, |alias| alias.span.end);
        Ok(FromItem {
            span: Span {
                start: table.span.start,
                end,
            },
            table,
            alias,
        })
    }

    fn parse_join(&mut self) -> Result<Join, ParseError> {
        let start = if self.matches(&TokenKind::Inner) {
            let start = self.current().span.start;
            self.position += 1;
            self.expect_simple(TokenKind::Join)?;
            start
        } else {
            self.expect_simple(TokenKind::Join)?.span.start
        };
        let right = self.parse_from_item()?;
        self.expect_simple(TokenKind::On)?;
        let condition = self.parse_expr(0)?;
        let end = expr_span(&condition).end;
        Ok(Join {
            kind: JoinKind::Inner,
            right,
            condition,
            span: Span { start, end },
        })
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, ParseError> {
        let start = self.expect_simple(TokenKind::Insert)?.span.start;
        self.expect_simple(TokenKind::Into)?;
        let table = self.expect_ident()?;
        let columns = if self.matches(&TokenKind::LParen) {
            self.position += 1;
            let columns = self.parse_identifier_list()?;
            self.expect_simple(TokenKind::RParen)?;
            columns
        } else {
            Vec::new()
        };
        self.expect_simple(TokenKind::Values)?;
        self.expect_simple(TokenKind::LParen)?;
        let values = self.parse_expression_list()?;
        let end = self.expect_simple(TokenKind::RParen)?.span.end;
        Ok(InsertStatement {
            table,
            columns,
            values,
            span: Span { start, end },
        })
    }

    fn parse_update(&mut self) -> Result<UpdateStatement, ParseError> {
        let start = self.expect_simple(TokenKind::Update)?.span.start;
        let table = self.expect_ident()?;
        self.expect_simple(TokenKind::Set)?;
        let mut assignments = vec![self.parse_assignment()?];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            assignments.push(self.parse_assignment()?);
        }
        let selection = if self.matches(&TokenKind::Where) {
            self.position += 1;
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        let assignment_end = assignments
            .last()
            .map_or(table.span.end, |assignment| assignment.span.end);
        let end = selection
            .as_ref()
            .map_or(assignment_end, |expr| expr_span(expr).end);
        Ok(UpdateStatement {
            table,
            assignments,
            selection,
            span: Span { start, end },
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStatement, ParseError> {
        let start = self.expect_simple(TokenKind::Delete)?.span.start;
        self.expect_simple(TokenKind::From)?;
        let table = self.expect_ident()?;
        let selection = if self.matches(&TokenKind::Where) {
            self.position += 1;
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        let end = selection
            .as_ref()
            .map_or(table.span.end, |expression| expr_span(expression).end);
        Ok(DeleteStatement {
            table,
            selection,
            span: Span { start, end },
        })
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<Ident>, ParseError> {
        let mut identifiers = vec![self.expect_ident()?];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            identifiers.push(self.expect_ident()?);
        }
        Ok(identifiers)
    }

    fn parse_expression_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut expressions = vec![self.parse_expr(0)?];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            expressions.push(self.parse_expr(0)?);
        }
        Ok(expressions)
    }

    fn parse_assignment(&mut self) -> Result<Assignment, ParseError> {
        let column = self.expect_ident()?;
        self.expect_simple(TokenKind::Eq)?;
        let value = self.parse_expr(0)?;
        let span = Span {
            start: column.span.start,
            end: expr_span(&value).end,
        };
        Ok(Assignment {
            column,
            value,
            span,
        })
    }

    fn parse_projection(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        if self.matches(&TokenKind::Star) {
            let span = self.current().span;
            self.position += 1;
            return Ok(vec![SelectItem::Wildcard(span)]);
        }

        let mut projection = vec![self.parse_projection_item()?];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            projection.push(self.parse_projection_item()?);
        }
        Ok(projection)
    }

    fn parse_projection_item(&mut self) -> Result<SelectItem, ParseError> {
        let token = self.current().clone();
        let TokenKind::Ident(name) = &token.kind else {
            let expression = self.parse_expr(0)?;
            let alias = if self.matches(&TokenKind::As) {
                self.position += 1;
                Some(self.expect_ident()?)
            } else {
                None
            };
            let end = alias
                .as_ref()
                .map_or(expr_span(&expression).end, |alias| alias.span.end);
            return Ok(SelectItem::Expression {
                span: Span {
                    start: expr_span(&expression).start,
                    end,
                },
                expression,
                alias,
            });
        };
        if !self
            .tokens
            .get(self.position + 1)
            .is_some_and(|next| matches!(next.kind, TokenKind::LParen | TokenKind::ColonColon))
        {
            let column = self.parse_column_name()?;
            if self.matches(&TokenKind::As) {
                self.position += 1;
                let alias = self.expect_ident()?;
                return Ok(SelectItem::Expression {
                    span: Span {
                        start: column.span.start,
                        end: alias.span.end,
                    },
                    expression: Expr::Column(column),
                    alias: Some(alias),
                });
            }
            return Ok(SelectItem::Column(column));
        }

        if self
            .tokens
            .get(self.position + 1)
            .is_some_and(|next| next.kind == TokenKind::ColonColon)
        {
            let expression = self.parse_expr(0)?;
            let end = expr_span(&expression).end;
            return Ok(SelectItem::Expression {
                span: Span {
                    start: expr_span(&expression).start,
                    end,
                },
                expression,
                alias: None,
            });
        }

        let function = if name.eq_ignore_ascii_case("count") {
            AggregateFunction::Count
        } else if name.eq_ignore_ascii_case("sum") {
            AggregateFunction::Sum
        } else if name.eq_ignore_ascii_case("min") {
            AggregateFunction::Min
        } else if name.eq_ignore_ascii_case("max") {
            AggregateFunction::Max
        } else {
            return Err(ParseError {
                kind: ParseErrorKind::Syntax,
                message: format!("unsupported projection function `{name}`"),
                span: token.span,
            });
        };
        self.position += 1;
        self.expect_simple(TokenKind::LParen)?;
        let argument = if self.matches(&TokenKind::Star) {
            let span = self.current().span;
            if function != AggregateFunction::Count {
                return Err(ParseError {
                    kind: ParseErrorKind::Syntax,
                    message: format!("{} does not accept `*`", aggregate_name(function)),
                    span,
                });
            }
            self.position += 1;
            AggregateArgument::Star(span)
        } else if matches!(self.current().kind, TokenKind::Ident(_)) {
            AggregateArgument::Column(self.parse_column_name()?)
        } else {
            return Err(self.error_here("aggregate expects `*` or a source column"));
        };
        let close = self.expect_simple(TokenKind::RParen)?;
        Ok(SelectItem::Aggregate(AggregateCall {
            function,
            argument,
            span: Span {
                start: token.span.start,
                end: close.span.end,
            },
        }))
    }

    fn expression_node(&mut self) -> Result<(), ParseError> {
        self.expression_nodes += 1;
        if self.expression_nodes > MAX_EXPRESSION_NODES {
            return Err(self.error_here("expression node limit exceeded"));
        }
        Ok(())
    }

    fn parse_expr(&mut self, minimum_precedence: u8) -> Result<Expr, ParseError> {
        if self.expression_depth >= MAX_EXPRESSION_DEPTH {
            return Err(self.error_here("expression depth limit exceeded"));
        }
        self.expression_node()?;
        self.expression_depth += 1;
        let result = self.parse_expr_inner(minimum_precedence);
        self.expression_depth -= 1;
        result
    }

    fn parse_expr_inner(&mut self, minimum_precedence: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix()?;
        loop {
            if self.matches(&TokenKind::ColonColon) {
                self.position += 1;
                let data_type_token = self.current().clone();
                let TokenKind::Ident(name) = data_type_token.kind else {
                    return Err(self.error_here("expected a type name after `::`"));
                };
                let data_type = if name.eq_ignore_ascii_case("bool")
                    || name.eq_ignore_ascii_case("boolean")
                {
                    SqlTypeName::Bool
                } else if name.eq_ignore_ascii_case("bigint") || name.eq_ignore_ascii_case("int8") {
                    SqlTypeName::Int64
                } else if name.eq_ignore_ascii_case("text") || name.eq_ignore_ascii_case("varchar")
                {
                    SqlTypeName::Text
                } else {
                    return Err(ParseError {
                        kind: ParseErrorKind::Syntax,
                        message: format!("unsupported cast type `{name}`"),
                        span: data_type_token.span,
                    });
                };
                self.position += 1;
                let span = Span {
                    start: expr_span(&left).start,
                    end: data_type_token.span.end,
                };
                self.expression_node()?;
                left = Expr::Cast {
                    expression: Box::new(left),
                    data_type,
                    span,
                };
                continue;
            }
            if self.matches(&TokenKind::Is) {
                const IS_PRECEDENCE: u8 = 4;
                if IS_PRECEDENCE < minimum_precedence {
                    break;
                }
                self.position += 1;
                let negated = if self.matches(&TokenKind::Not) {
                    self.position += 1;
                    true
                } else {
                    false
                };
                let null = self.expect_simple(TokenKind::Null)?;
                let span = Span {
                    start: expr_span(&left).start,
                    end: null.span.end,
                };
                self.expression_node()?;
                left = Expr::IsNull {
                    expression: Box::new(left),
                    negated,
                    span,
                };
                continue;
            }

            let Some((operator, precedence)) = self.current_binary_operator() else {
                break;
            };
            if precedence < minimum_precedence {
                break;
            }
            let operator_span = self.current().span;
            self.position += 1;
            let right = self.parse_expr(precedence + 1)?;
            let span = Span {
                start: expr_span(&left).start,
                end: expr_span(&right).end.max(operator_span.end),
            };
            left = Expr::Binary {
                left: Box::new(left),
                operator,
                right: Box::new(right),
                span,
            };
        }
        Ok(left)
    }

    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        if self.matches(&TokenKind::Not) {
            let start = self.current().span.start;
            self.position += 1;
            // Comparisons and IS NULL bind inside NOT; AND and OR do not.
            let expression = self.parse_expr(3)?;
            let span = Span {
                start,
                end: expr_span(&expression).end,
            };
            return Ok(Expr::Unary {
                operator: UnaryOp::Not,
                expression: Box::new(expression),
                span,
            });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::Ident(name) => {
                self.position += 1;
                let first = Ident {
                    name,
                    span: token.span,
                };
                Ok(Expr::Column(self.finish_column_name(first)?))
            }
            TokenKind::Number(value) => {
                self.position += 1;
                Ok(Expr::Literal {
                    value: Literal::Int(value),
                    span: token.span,
                })
            }
            TokenKind::String(value) => {
                self.position += 1;
                Ok(Expr::Literal {
                    value: Literal::String(value),
                    span: token.span,
                })
            }
            TokenKind::Parameter(id) => {
                self.position += 1;
                Ok(Expr::Parameter {
                    id,
                    span: token.span,
                })
            }
            TokenKind::True | TokenKind::False | TokenKind::Null => {
                self.position += 1;
                let value = match token.kind {
                    TokenKind::True => Literal::Bool(true),
                    TokenKind::False => Literal::Bool(false),
                    TokenKind::Null => Literal::Null,
                    _ => {
                        return Err(ParseError {
                            kind: ParseErrorKind::Syntax,
                            message: "invalid literal token".into(),
                            span: token.span,
                        });
                    }
                };
                Ok(Expr::Literal {
                    value,
                    span: token.span,
                })
            }
            TokenKind::LParen => {
                self.position += 1;
                let expression = self.parse_expr(0)?;
                self.expect_simple(TokenKind::RParen)?;
                Ok(expression)
            }
            _ => Err(self.error_here("expected a column, literal, or parenthesized expression")),
        }
    }

    fn current_binary_operator(&self) -> Option<(BinaryOp, u8)> {
        match self.current().kind {
            TokenKind::Or => Some((BinaryOp::Or, 1)),
            TokenKind::And => Some((BinaryOp::And, 2)),
            TokenKind::Eq => Some((BinaryOp::Eq, 4)),
            TokenKind::NotEq => Some((BinaryOp::NotEq, 4)),
            TokenKind::Lt => Some((BinaryOp::Lt, 4)),
            TokenKind::LtEq => Some((BinaryOp::LtEq, 4)),
            TokenKind::Gt => Some((BinaryOp::Gt, 4)),
            TokenKind::GtEq => Some((BinaryOp::GtEq, 4)),
            _ => None,
        }
    }

    fn expect_ident(&mut self) -> Result<Ident, ParseError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::Ident(name) => {
                self.position += 1;
                Ok(Ident {
                    name,
                    span: token.span,
                })
            }
            _ => Err(self.error_here("expected an identifier")),
        }
    }

    fn parse_column_name(&mut self) -> Result<ColumnName, ParseError> {
        let first = self.expect_ident()?;
        self.finish_column_name(first)
    }

    fn finish_column_name(&mut self, first: Ident) -> Result<ColumnName, ParseError> {
        if self.matches(&TokenKind::Dot) {
            self.position += 1;
            let name = self.expect_ident()?;
            Ok(ColumnName {
                span: Span {
                    start: first.span.start,
                    end: name.span.end,
                },
                qualifier: Some(first),
                name,
            })
        } else {
            Ok(ColumnName {
                span: first.span,
                qualifier: None,
                name: first,
            })
        }
    }

    fn expect_simple(&mut self, expected: TokenKind) -> Result<Token, ParseError> {
        let token = self.current().clone();
        if token.kind == expected {
            self.position += 1;
            Ok(token)
        } else {
            Err(self.error_here(&format!("expected {expected:?}")))
        }
    }

    fn matches(&self, expected: &TokenKind) -> bool {
        &self.current().kind == expected
    }

    fn current(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn error_here(&self, message: &str) -> ParseError {
        ParseError {
            kind: ParseErrorKind::Syntax,
            message: message.into(),
            span: self.current().span,
        }
    }
}

fn expr_span(expression: &Expr) -> Span {
    match expression {
        Expr::Column(column) => column.span,
        Expr::Literal { span, .. }
        | Expr::Parameter { span, .. }
        | Expr::Cast { span, .. }
        | Expr::Binary { span, .. }
        | Expr::Unary { span, .. }
        | Expr::IsNull { span, .. } => *span,
    }
}

fn statement_span(statement: &Statement) -> Span {
    match statement {
        Statement::Select(statement) => statement.span,
        Statement::Insert(statement) => statement.span,
        Statement::Update(statement) => statement.span,
        Statement::Delete(statement) => statement.span,
        Statement::CreateTable(statement) => statement.span,
        Statement::DropTable(statement) => statement.span,
        Statement::AlterTable(statement) => statement.span,
        Statement::CreateIndex(statement) => statement.span,
        Statement::DropIndex(statement) => statement.span,
    }
}

const fn aggregate_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AggregateArgument, AggregateFunction, BinaryOp, Expr, Literal, NullOrder, SelectItem,
        SortDirection, SqlTypeName, Statement, UnaryOp, parse, parse_statement,
    };

    #[test]
    fn parses_the_initial_query_subset() {
        let query = parse("SELECT id, name FROM users WHERE id >= 2 AND name != 'bob' LIMIT 10")
            .expect("query parses");
        assert_eq!(query.from.as_ref().expect("FROM").table.name, "users");
        assert_eq!(query.projection.len(), 2);
        assert_eq!(query.limit, Some(10));
        assert!(matches!(query.projection[0], SelectItem::Column(_)));
        assert!(matches!(
            query.selection,
            Some(Expr::Binary {
                operator: BinaryOp::And,
                ..
            })
        ));
    }

    #[test]
    fn parses_wildcard_and_boolean_literal() {
        let query = parse("select * from users where active = true").expect("query parses");
        assert!(matches!(query.projection[0], SelectItem::Wildcard(_)));
        assert!(
            matches!(query.selection, Some(Expr::Binary { right, .. }) if matches!(*right, Expr::Literal { value: Literal::Bool(true), .. }))
        );
    }

    #[test]
    fn reports_non_ascii_input_without_panicking() {
        let error =
            parse("SELECT 名 FROM users").expect_err("unsupported identifier should be rejected");
        assert_eq!(error.span.start, 7);
        assert_eq!(error.span.end, 10);
    }

    #[test]
    fn parses_null_predicates_and_not() {
        let null = parse("SELECT id FROM users WHERE NULL").expect("parse");
        assert!(matches!(
            null.selection,
            Some(Expr::Literal {
                value: Literal::Null,
                ..
            })
        ));

        let is_null = parse("SELECT id FROM users WHERE nickname IS NULL").expect("parse");
        assert!(matches!(
            is_null.selection,
            Some(Expr::IsNull { negated: false, .. })
        ));

        let is_not_null = parse("SELECT id FROM users WHERE nickname IS NOT NULL").expect("parse");
        assert!(matches!(
            is_not_null.selection,
            Some(Expr::IsNull { negated: true, .. })
        ));

        let not = parse("SELECT id FROM users WHERE NOT active").expect("parse");
        assert!(matches!(
            not.selection,
            Some(Expr::Unary {
                operator: UnaryOp::Not,
                ..
            })
        ));
    }

    #[test]
    fn not_wraps_comparison_before_and() {
        let query =
            parse("SELECT id FROM users WHERE NOT id = 1 AND active = true").expect("parse");
        let Some(Expr::Binary {
            operator: BinaryOp::And,
            left,
            ..
        }) = query.selection
        else {
            panic!("expected top-level AND");
        };
        assert!(matches!(
            *left,
            Expr::Unary {
                operator: UnaryOp::Not,
                expression,
                ..
            } if matches!(*expression, Expr::Binary { operator: BinaryOp::Eq, .. })
        ));
    }

    #[test]
    fn parentheses_override_boolean_precedence() {
        let query =
            parse("SELECT id FROM users WHERE NOT (active OR false) AND true").expect("parse");
        assert!(matches!(
            query.selection,
            Some(Expr::Binary {
                operator: BinaryOp::And,
                left,
                ..
            }) if matches!(*left, Expr::Unary { .. })
        ));
    }

    #[test]
    fn invalid_is_and_not_syntax_report_spans() {
        for source in [
            "SELECT id FROM users WHERE id IS",
            "SELECT id FROM users WHERE id IS truth",
            "SELECT id FROM users WHERE NOT",
        ] {
            let error = parse(source).expect_err("invalid expression");
            assert!(error.span.start <= error.span.end);
            assert!(error.span.end <= source.len());
        }
    }

    #[test]
    fn parses_insert_update_and_delete_statements() {
        let insert =
            parse_statement("INSERT INTO users (id, name, nickname) VALUES (1, 'Ada', NULL);")
                .expect("insert parses");
        assert!(matches!(
            insert,
            Statement::Insert(statement)
                if statement.columns.len() == 3 && statement.values.len() == 3
        ));

        let update =
            parse_statement("UPDATE users SET nickname = 'ada', active = NOT active WHERE id = 1;")
                .expect("update parses");
        assert!(matches!(
            update,
            Statement::Update(statement)
                if statement.assignments.len() == 2 && statement.selection.is_some()
        ));

        let delete =
            parse_statement("DELETE FROM users WHERE nickname IS NULL;").expect("delete parses");
        assert!(matches!(
            delete,
            Statement::Delete(statement) if statement.selection.is_some()
        ));
    }

    #[test]
    fn reports_structural_dml_errors_with_spans() {
        for source in [
            "INSERT users (id) VALUES (1)",
            "INSERT INTO users (id) (1)",
            "UPDATE users id = 1",
            "UPDATE users SET",
            "DELETE users WHERE id = 1",
            "DELETE FROM users WHERE",
        ] {
            let error = parse_statement(source).expect_err("invalid DML");
            assert!(error.span.start <= error.span.end);
            assert!(error.span.end <= source.len());
        }
    }

    #[test]
    fn rejects_multiple_statements() {
        assert!(parse_statement("DELETE FROM users; DELETE FROM users").is_err());
    }

    #[test]
    fn parses_aliases_qualified_columns_and_left_associative_joins() {
        let query = parse(
            "SELECT u.id, o.name FROM users AS u INNER JOIN teams t \
             ON u.team_id = t.id JOIN organizations o ON t.org_id = o.id \
             WHERE o.active = true",
        )
        .expect("join query parses");
        assert_eq!(
            query
                .from
                .as_ref()
                .expect("FROM")
                .alias
                .as_ref()
                .expect("alias")
                .name,
            "u"
        );
        assert_eq!(query.joins.len(), 2);
        assert_eq!(query.joins[0].right.table.name, "teams");
        assert_eq!(
            query.joins[0].right.alias.as_ref().expect("alias").name,
            "t"
        );
        let SelectItem::Column(first) = &query.projection[0] else {
            panic!("expected projected column");
        };
        assert_eq!(first.qualifier.as_ref().expect("qualifier").name, "u");
        assert_eq!(first.name.name, "id");
        assert!(query.selection.is_some());
    }

    #[test]
    fn join_keywords_are_not_consumed_as_shorthand_aliases() {
        let query =
            parse("SELECT * FROM users JOIN teams ON users.id = teams.id").expect("join parses");
        assert!(query.from.as_ref().expect("FROM").alias.is_none());
        assert!(query.joins[0].right.alias.is_none());

        for source in [
            "SELECT * FROM users JOIN ON users.id = teams.id",
            "SELECT * FROM users INNER teams ON users.id = teams.id",
            "SELECT * FROM users JOIN teams users.id = teams.id",
            "SELECT * FROM users AS JOIN teams ON users.id = teams.id",
        ] {
            assert!(parse(source).is_err(), "{source} must fail");
        }
    }

    #[test]
    fn parses_order_by_columns_directions_nulls_and_limit() {
        let query = parse(
            "SELECT name FROM users WHERE active = true \
             ORDER BY users.id, name DESC NULLS LAST, active ASC NULLS FIRST LIMIT 2",
        )
        .expect("ORDER BY query parses");
        assert_eq!(query.order_by.len(), 3);
        assert_eq!(
            query.order_by[0]
                .column
                .qualifier
                .as_ref()
                .expect("qualified key")
                .name,
            "users"
        );
        assert_eq!(query.order_by[0].direction, None);
        assert_eq!(query.order_by[0].null_order, None);
        assert_eq!(query.order_by[1].direction, Some(SortDirection::Desc));
        assert_eq!(query.order_by[1].null_order, Some(NullOrder::Last));
        assert_eq!(query.order_by[2].direction, Some(SortDirection::Asc));
        assert_eq!(query.order_by[2].null_order, Some(NullOrder::First));
        assert_eq!(query.limit, Some(2));

        let nulls = parse("SELECT id FROM users ORDER BY id NULLS FIRST")
            .expect("NULLS without direction parses");
        assert_eq!(nulls.order_by[0].direction, None);
        assert_eq!(nulls.order_by[0].null_order, Some(NullOrder::First));
        let explicit = parse("SELECT id FROM users ORDER BY id ASC NULLS LAST")
            .expect("explicit ASC NULLS LAST parses");
        assert_eq!(explicit.order_by[0].direction, Some(SortDirection::Asc));
        assert_eq!(explicit.order_by[0].null_order, Some(NullOrder::Last));

        let alias =
            parse("SELECT u.id FROM users u JOIN teams t ON u.id = t.id ORDER BY u.id DESC")
                .expect("qualified join ORDER BY parses");
        assert_eq!(
            alias.order_by[0]
                .column
                .qualifier
                .as_ref()
                .expect("alias qualifier")
                .name,
            "u"
        );
    }

    #[test]
    fn rejects_invalid_order_by_syntax_and_clause_order() {
        for source in [
            "SELECT id FROM users ORDER BY",
            "SELECT id FROM users ORDER BY ,",
            "SELECT id FROM users ORDER BY id,",
            "SELECT id FROM users ORDER BY id ASC DESC",
            "SELECT id FROM users ORDER BY id NULLS",
            "SELECT id FROM users ORDER BY id NULLS UNKNOWN",
            "SELECT id FROM users ORDER BY 1",
            "SELECT id FROM users ORDER BY id = 1",
            "SELECT id FROM users LIMIT 1 ORDER BY id",
        ] {
            let error = parse(source).expect_err("invalid ORDER BY must fail");
            assert!(error.span.start <= error.span.end, "{source}");
            assert!(error.span.end <= source.len(), "{source}");
        }
    }

    #[test]
    fn parses_group_by_columns_in_select_clause_order() {
        let query = parse(
            "SELECT COUNT(*), u.team_id FROM users u WHERE u.active = true \
             GROUP BY u.country, u.team_id ORDER BY u.team_id LIMIT 10",
        )
        .expect("GROUP BY query parses");
        assert_eq!(query.group_by.len(), 2);
        assert_eq!(query.group_by[0].name.name, "country");
        assert_eq!(
            query.group_by[0]
                .qualifier
                .as_ref()
                .expect("qualified group key")
                .name,
            "u"
        );
        assert_eq!(query.group_by[1].name.name, "team_id");
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn rejects_invalid_group_by_syntax_and_clause_order() {
        for source in [
            "SELECT id FROM users GROUP",
            "SELECT id FROM users GROUP id",
            "SELECT id FROM users GROUP BY",
            "SELECT id FROM users GROUP BY ,",
            "SELECT id FROM users GROUP BY id,",
            "SELECT id FROM users GROUP BY 1",
            "SELECT id FROM users GROUP BY id = 1",
            "SELECT id FROM users ORDER BY id GROUP BY id",
            "SELECT id FROM users LIMIT 1 GROUP BY id",
        ] {
            let error = parse(source).expect_err("invalid GROUP BY must fail");
            assert!(error.span.start <= error.span.end, "{source}");
            assert!(error.span.end <= source.len(), "{source}");
        }
    }

    #[test]
    fn parses_contextual_global_aggregates_without_reserving_names() {
        let query = parse(
            "SELECT COUNT(*), count(score), SUM(u.score), MIN(name), MAX(active) FROM users u",
        )
        .expect("aggregates parse");
        assert_eq!(query.projection.len(), 5);
        assert!(matches!(
            &query.projection[0],
            SelectItem::Aggregate(call)
                if call.function == AggregateFunction::Count
                    && matches!(call.argument, AggregateArgument::Star(_))
        ));
        assert!(matches!(
            &query.projection[1],
            SelectItem::Aggregate(call)
                if call.function == AggregateFunction::Count
                    && matches!(&call.argument, AggregateArgument::Column(column) if column.name.name == "score")
        ));
        assert!(matches!(
            &query.projection[2],
            SelectItem::Aggregate(call)
                if call.function == AggregateFunction::Sum
                    && matches!(&call.argument, AggregateArgument::Column(column) if column.qualifier.as_ref().is_some_and(|qualifier| qualifier.name == "u"))
        ));
        assert!(matches!(
            &query.projection[3],
            SelectItem::Aggregate(call) if call.function == AggregateFunction::Min
        ));
        assert!(matches!(
            &query.projection[4],
            SelectItem::Aggregate(call) if call.function == AggregateFunction::Max
        ));

        for name in ["count", "sum", "min", "max"] {
            let ordinary = parse(&format!("SELECT {name} FROM metrics"))
                .expect("aggregate names remain contextual identifiers");
            assert!(matches!(ordinary.projection[0], SelectItem::Column(_)));
        }
    }

    #[test]
    fn rejects_invalid_aggregate_projection_syntax() {
        for source in [
            "SELECT COUNT() FROM users",
            "SELECT COUNT(a, b) FROM users",
            "SELECT COUNT(1) FROM users",
            "SELECT SUM(*) FROM users",
            "SELECT SUM(1) FROM users",
            "SELECT MIN(*) FROM users",
            "SELECT MAX(*) FROM users",
            "SELECT foo(id) FROM users",
        ] {
            let error = parse(source).expect_err("invalid aggregate must fail");
            assert!(error.span.start <= error.span.end, "{source}");
            assert!(error.span.end <= source.len(), "{source}");
        }
    }

    #[test]
    fn parses_fromless_scalar_select_and_parameter_slots() {
        let query = parse("SELECT $2 AS value").expect("parse parameter projection");
        assert!(query.from.is_none());
        assert!(matches!(
            &query.projection[0],
            SelectItem::Expression {
                expression: Expr::Parameter { id, .. },
                alias: Some(alias),
                ..
            } if *id == netbadb_types::ParameterId(1) && alias.name == "value"
        ));
        assert!(parse("SELECT $0").is_err());
        assert!(parse("SELECT $").is_err());
    }

    #[test]
    fn parses_postfix_casts_as_generic_typed_expressions() {
        let query = parse("SELECT $1::BIGINT AS id").expect("parse cast projection");
        assert!(matches!(
            &query.projection[0],
            SelectItem::Expression {
                expression: Expr::Cast {
                    expression,
                    data_type: SqlTypeName::Int64,
                    ..
                },
                alias: Some(alias),
                ..
            } if matches!(**expression, Expr::Parameter { .. }) && alias.name == "id"
        ));

        let statement = parse_statement(
            "INSERT INTO users (id, name, active) VALUES ($1::INT8, $2::VARCHAR, $3::BOOL)",
        )
        .expect("parse cast parameters in DML");
        assert!(matches!(statement, Statement::Insert(_)));
        assert!(parse("SELECT $1::REGCLASS").is_err());
    }

    #[test]
    fn parses_qualified_column_projection_aliases() {
        let query = parse("SELECT users.id AS users_id, users.name AS users_name FROM users")
            .expect("parse aliased columns");
        assert!(matches!(
            &query.projection[0],
            SelectItem::Expression {
                expression: Expr::Column(column),
                alias: Some(alias),
                ..
            } if column.name.name == "id" && alias.name == "users_id"
        ));
    }

    #[test]
    fn parses_drop_index_and_rejects_unimplemented_clauses() {
        for sql in [
            "DROP INDEX users_name_idx",
            "DROP INDEX IF EXISTS public.users_name_idx;",
        ] {
            let Statement::DropIndex(drop) = parse_statement(sql).unwrap() else {
                panic!("DROP AST");
            };
            assert_eq!(drop.name.name.name, "users_name_idx");
        }
        for sql in [
            "DROP INDEX CONCURRENTLY idx",
            "DROP INDEX a, b",
            "DROP INDEX idx CASCADE",
            "DROP INDEX idx RESTRICT",
            "DROP INDEX $1",
            "DROP INDEX \"idx\"",
        ] {
            assert!(parse_statement(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn parses_only_the_bounded_create_index_shape() {
        let statement =
            parse_statement("CREATE INDEX IF NOT EXISTS users_name_idx ON public.users (name);")
                .expect("CREATE INDEX parses");
        let Statement::CreateIndex(create) = statement else {
            panic!("expected CREATE INDEX");
        };
        assert_eq!(create.name.name, "users_name_idx");
        assert_eq!(create.table.qualifier.unwrap().name, "public");
        assert_eq!(create.table.name.name, "users");
        assert_eq!(create.column.name, "name");
        assert!(create.if_not_exists);

        for unsupported in [
            "CREATE INDEX idx ON users (a, b)",
            "CREATE INDEX idx ON users (name) WHERE active = true",
            "CREATE INDEX idx ON users (name) INCLUDE (id)",
            "CREATE INDEX idx ON users (lower(name))",
        ] {
            assert!(parse_statement(unsupported).is_err(), "{unsupported}");
        }
    }
}

#[cfg(test)]
mod alter_table_tests;
#[cfg(test)]
mod create_table_tests;
#[cfg(test)]
mod drop_table_tests;
