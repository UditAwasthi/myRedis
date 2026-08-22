//! RESP-inspired protocol codec for TinyRedis.

pub mod decode;
pub mod encode;
pub mod error;
pub mod frame;
pub mod parser;

pub use decode::RespDecoder;
pub use encode::{encode_error, encode_frame, encode_result, encode_simple};
pub use error::{ProtocolError, ProtocolResult};
pub use frame::RespFrame;
pub use parser::parse_command;
