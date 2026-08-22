use tinyredis_core::CommandResult;

use crate::frame::RespFrame;

pub fn encode_frame(frame: &RespFrame) -> Vec<u8> {
    match frame {
        RespFrame::Simple(s) => format!("+{s}\r\n").into_bytes(),
        RespFrame::Error(s) => format!("-{s}\r\n").into_bytes(),
        RespFrame::Integer(n) => format!(":{n}\r\n").into_bytes(),
        RespFrame::Bulk(None) => b"$-1\r\n".to_vec(),
        RespFrame::Bulk(Some(data)) => {
            let mut out = format!("${}\r\n", data.len()).into_bytes();
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
            out
        }
        RespFrame::Array(items) => {
            let mut out = format!("*{}\r\n", items.len()).into_bytes();
            for item in items {
                out.extend_from_slice(&encode_frame(item));
            }
            out
        }
    }
}

pub fn encode_result(result: &CommandResult) -> Vec<u8> {
    encode_frame(&result_to_frame(result))
}

fn result_to_frame(result: &CommandResult) -> RespFrame {
    match result {
        CommandResult::Ok => RespFrame::Simple("OK".into()),
        CommandResult::Status(s) => RespFrame::Simple(s.clone()),
        CommandResult::Integer(n) => RespFrame::Integer(*n),
        CommandResult::BulkString(None) => RespFrame::Bulk(None),
        CommandResult::BulkString(Some(s)) => RespFrame::Bulk(Some(s.as_bytes().to_vec())),
        CommandResult::BulkStrings(items) => {
            let frames = items
                .iter()
                .map(|item| match item {
                    None => RespFrame::Bulk(None),
                    Some(s) => RespFrame::Bulk(Some(s.as_bytes().to_vec())),
                })
                .collect();
            RespFrame::Array(frames)
        }
        CommandResult::Array(items) => {
            let frames = items
                .iter()
                .map(|s| RespFrame::Bulk(Some(s.as_bytes().to_vec())))
                .collect();
            RespFrame::Array(frames)
        }
        CommandResult::Error(msg) => RespFrame::Error(msg.clone()),
    }
}

pub fn encode_error(message: &str) -> Vec<u8> {
    encode_frame(&RespFrame::Error(message.into()))
}

pub fn encode_simple(message: &str) -> Vec<u8> {
    encode_frame(&RespFrame::Simple(message.into()))
}
