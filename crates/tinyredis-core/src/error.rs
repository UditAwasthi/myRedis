use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("key not found")]
    KeyNotFound,

    #[error("wrong type for key: expected {expected}, found {found}")]
    WrongType { expected: String, found: String },

    #[error("invalid integer value")]
    InvalidInteger,

    #[error("integer overflow")]
    IntegerOverflow,

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("syntax error")]
    SyntaxError,

    #[error("memory limit reached")]
    MemoryLimit,

    #[error("transaction error: {0}")]
    Transaction(String),

    #[error("watch key modified")]
    WatchConflict,

    #[error("internal error: {0}")]
    Internal(String),
}

pub type CoreResult<T> = Result<T, CoreError>;
