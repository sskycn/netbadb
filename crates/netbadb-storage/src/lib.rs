//! Synchronous page and heap storage for the first embedded vertical slice.

mod heap;
mod page;

pub use heap::HeapStorage;
pub use page::{PAGE_SIZE, Page, PageManager};

use std::error::Error;
use std::fmt;

use netbadb_types::PhysicalType;

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    InvalidFormat(String),
    SchemaMismatch {
        expected: String,
        actual: String,
    },
    InvalidRowLength {
        expected: usize,
        actual: usize,
    },
    TypeMismatch {
        column: String,
        expected: PhysicalType,
        actual: Option<PhysicalType>,
    },
    NullNotAllowed {
        column: String,
    },
    RowTooLarge {
        size: usize,
        capacity: usize,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage I/O error: {error}"),
            Self::InvalidFormat(message) => write!(formatter, "invalid database format: {message}"),
            Self::SchemaMismatch { expected, actual } => write!(
                formatter,
                "schema mismatch: expected {expected}, found {actual}"
            ),
            Self::InvalidRowLength { expected, actual } => {
                write!(formatter, "expected {expected} row values, found {actual}")
            }
            Self::TypeMismatch {
                column,
                expected,
                actual,
            } => write!(
                formatter,
                "column `{column}` expects {expected}, found {actual:?}"
            ),
            Self::NullNotAllowed { column } => {
                write!(formatter, "column `{column}` is not nullable")
            }
            Self::RowTooLarge { size, capacity } => write!(
                formatter,
                "row payload of {size} bytes exceeds page capacity {capacity}"
            ),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn invalid_format(message: impl Into<String>) -> StorageError {
    StorageError::InvalidFormat(message.into())
}
