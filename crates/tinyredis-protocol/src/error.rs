use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("incomplete frame")]
    Incomplete,

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("payload too large: {size} exceeds max {max}")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("unknown command: {0}")]
    UnknownCommand(String),

    #[error("wrong number of arguments for {command}: expected {expected}, got {got}")]
    WrongArgCount {
        command: String,
        expected: String,
        got: usize,
    },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("{0}")]
    Other(String),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;
