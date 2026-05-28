use thiserror::Error;

/// Boxed variants keep `Error` small — the underlying `redb` errors are
/// each over 100 bytes, which would inflate every `Result<_, Error>`.
#[derive(Debug, Error)]
pub enum Error {
    #[error("redb error: {0}")]
    Redb(Box<redb::Error>),
    #[error("transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),
    #[error("table error: {0}")]
    Table(Box<redb::TableError>),
    #[error("storage error: {0}")]
    Storage(Box<redb::StorageError>),
    #[error("commit error: {0}")]
    Commit(Box<redb::CommitError>),
    #[error("database error: {0}")]
    Database(Box<redb::DatabaseError>),
    #[error("io error: {0}")]
    Io(Box<std::io::Error>),
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}

impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Self {
        Self::Redb(Box::new(e))
    }
}
impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Self::Transaction(Box::new(e))
    }
}
impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Self::Table(Box::new(e))
    }
}
impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Self::Storage(Box::new(e))
    }
}
impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Self::Commit(Box::new(e))
    }
}
impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Self::Database(Box::new(e))
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(Box::new(e))
    }
}
