//! A deliberately small, typed-query parser for the first vertical slice.

use std::error::Error;
use std::fmt;

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
    pub from: Ident,
    pub selection: Option<Expr>,
    pub limit: Option<u64>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    Wildcard(Span),
    Column(Ident),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(Ident),
    Literal {
        value: Literal,
        span: Span,
    },
    Binary {
        left: Box<Expr>,
        operator: BinaryOp,
        right: Box<Expr>,
        span: Span,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
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
    Limit,
    And,
    Or,
    True,
    False,
    Null,
    Ident(String),
    Number(i64),
    String(String),
    Comma,
    Star,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    LParen,
    RParen,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    span: Span,
}

pub fn parse(input: &str) -> Result<Query, ParseError> {
    let tokens = tokenize(input)?;
    Parser {
        tokens,
        position: 0,
    }
    .parse_query()
}

fn tokenize(input: &str) -> Result<Vec<Token>, ParseError> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut position = 0;

    while position < bytes.len() {
        let byte = bytes[position];
        if byte.is_ascii_whitespace() {
            position += 1;
            continue;
        }
        let start = position;
        let token = match byte {
            b',' => {
                position += 1;
                TokenKind::Comma
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
            b'\'' => {
                position += 1;
                let content_start = position;
                while position < bytes.len() && bytes[position] != b'\'' {
                    position += 1;
                }
                if position == bytes.len() {
                    return Err(ParseError {
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
                match input[start..position].to_ascii_uppercase().as_str() {
                    "SELECT" => TokenKind::Select,
                    "FROM" => TokenKind::From,
                    "WHERE" => TokenKind::Where,
                    "LIMIT" => TokenKind::Limit,
                    "AND" => TokenKind::And,
                    "OR" => TokenKind::Or,
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
}

impl Parser {
    fn parse_query(mut self) -> Result<Query, ParseError> {
        let start = self.expect_simple(TokenKind::Select)?.span.start;
        let projection = self.parse_projection()?;
        self.expect_simple(TokenKind::From)?;
        let from = self.expect_ident()?;
        let selection = if self.matches(&TokenKind::Where) {
            self.position += 1;
            Some(self.parse_expr(0)?)
        } else {
            None
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
        let end = self.expect_simple(TokenKind::Eof)?.span.end;
        Ok(Query {
            projection,
            from,
            selection,
            limit,
            span: Span { start, end },
        })
    }

    fn parse_projection(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        if self.matches(&TokenKind::Star) {
            let span = self.current().span;
            self.position += 1;
            return Ok(vec![SelectItem::Wildcard(span)]);
        }

        let mut projection = vec![SelectItem::Column(self.expect_ident()?)];
        while self.matches(&TokenKind::Comma) {
            self.position += 1;
            projection.push(SelectItem::Column(self.expect_ident()?));
        }
        Ok(projection)
    }

    fn parse_expr(&mut self, minimum_precedence: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary()?;
        while let Some((operator, precedence)) = self.current_binary_operator() {
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

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::Ident(name) => {
                self.position += 1;
                Ok(Expr::Column(Ident {
                    name,
                    span: token.span,
                }))
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
            TokenKind::True | TokenKind::False | TokenKind::Null => {
                self.position += 1;
                let value = match token.kind {
                    TokenKind::True => Literal::Bool(true),
                    TokenKind::False => Literal::Bool(false),
                    TokenKind::Null => Literal::Null,
                    _ => {
                        return Err(ParseError {
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
            TokenKind::Eq => Some((BinaryOp::Eq, 3)),
            TokenKind::NotEq => Some((BinaryOp::NotEq, 3)),
            TokenKind::Lt => Some((BinaryOp::Lt, 3)),
            TokenKind::LtEq => Some((BinaryOp::LtEq, 3)),
            TokenKind::Gt => Some((BinaryOp::Gt, 3)),
            TokenKind::GtEq => Some((BinaryOp::GtEq, 3)),
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
            message: message.into(),
            span: self.current().span,
        }
    }
}

fn expr_span(expression: &Expr) -> Span {
    match expression {
        Expr::Column(column) => column.span,
        Expr::Literal { span, .. } | Expr::Binary { span, .. } => *span,
    }
}

#[cfg(test)]
mod tests {
    use super::{BinaryOp, Expr, Literal, SelectItem, parse};

    #[test]
    fn parses_the_initial_query_subset() {
        let query = parse("SELECT id, name FROM users WHERE id >= 2 AND name != 'bob' LIMIT 10")
            .expect("query parses");
        assert_eq!(query.from.name, "users");
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
}
