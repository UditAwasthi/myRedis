use bytes::{Buf, BytesMut};

use crate::error::{ProtocolError, ProtocolResult};
use crate::frame::RespFrame;

const MAX_BULK_LEN: usize = 512 * 1024 * 1024;

pub struct RespDecoder {
    max_bulk_len: usize,
}

impl Default for RespDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl RespDecoder {
    pub fn new() -> Self {
        Self {
            max_bulk_len: MAX_BULK_LEN,
        }
    }

    pub fn with_max_bulk_len(max_bulk_len: usize) -> Self {
        Self { max_bulk_len }
    }

    /// Decode one frame from the buffer. Returns `Ok(None)` if more data is needed.
    pub fn decode(&self, buf: &mut BytesMut) -> ProtocolResult<Option<RespFrame>> {
        if buf.is_empty() {
            return Ok(None);
        }
        let mut reader = buf.clone();
        match decode_frame(self.max_bulk_len, &mut reader)? {
            None => Ok(None),
            Some(frame) => {
                let consumed = buf.len() - reader.len();
                buf.advance(consumed);
                Ok(Some(frame))
            }
        }
    }
}

fn decode_frame(max_bulk_len: usize, buf: &mut BytesMut) -> ProtocolResult<Option<RespFrame>> {
    if buf.is_empty() {
        return Ok(None);
    }
    let prefix = buf[0];
    match prefix {
        b'+' => decode_line(buf).map(|s| Some(RespFrame::Simple(s))),
        b'-' => decode_line(buf).map(|s| Some(RespFrame::Error(s))),
        b':' => decode_line(buf)
            .and_then(|s| {
                s.parse::<i64>()
                    .map(RespFrame::Integer)
                    .map_err(|_| ProtocolError::InvalidFrame(format!("invalid integer: {s}")))
            })
            .map(Some),
        b'$' => decode_bulk(max_bulk_len, buf).map(Some),
        b'*' => decode_array(max_bulk_len, buf).map(Some),
        _ => Err(ProtocolError::InvalidFrame(format!(
            "unknown type prefix: {prefix}"
        ))),
    }
}

fn decode_line(buf: &mut BytesMut) -> ProtocolResult<String> {
    let pos = find_crlf(buf).ok_or(ProtocolError::Incomplete)?;
    let line = buf.split_to(pos);
    buf.advance(2);
    let text = String::from_utf8_lossy(&line[1..]).into_owned();
    Ok(text)
}

fn decode_bulk(max_bulk_len: usize, buf: &mut BytesMut) -> ProtocolResult<RespFrame> {
    let len_line_end = find_crlf(buf).ok_or(ProtocolError::Incomplete)?;
    let len_bytes = &buf[1..len_line_end];
    let len_str = std::str::from_utf8(len_bytes)
        .map_err(|_| ProtocolError::InvalidFrame("invalid bulk length".into()))?;
    let len: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidFrame(format!("invalid bulk length: {len_str}")))?;
    if len < -1 {
        return Err(ProtocolError::InvalidFrame(format!(
            "invalid bulk length: {len}"
        )));
    }
    if len == -1 {
        buf.advance(len_line_end + 2);
        return Ok(RespFrame::Bulk(None));
    }
    let len = len as usize;
    if len > max_bulk_len {
        return Err(ProtocolError::PayloadTooLarge {
            size: len,
            max: max_bulk_len,
        });
    }
    let total = len_line_end + 2 + len + 2;
    if buf.len() < total {
        return Err(ProtocolError::Incomplete);
    }
    buf.advance(len_line_end + 2);
    let data = buf.split_to(len);
    if buf.len() < 2 || &buf[..2] != b"\r\n" {
        return Err(ProtocolError::InvalidFrame(
            "bulk string missing CRLF".into(),
        ));
    }
    buf.advance(2);
    Ok(RespFrame::Bulk(Some(data.to_vec())))
}

fn decode_array(max_bulk_len: usize, buf: &mut BytesMut) -> ProtocolResult<RespFrame> {
    let len_line_end = find_crlf(buf).ok_or(ProtocolError::Incomplete)?;
    let len_bytes = &buf[1..len_line_end];
    let len_str = std::str::from_utf8(len_bytes)
        .map_err(|_| ProtocolError::InvalidFrame("invalid array length".into()))?;
    let count: i64 = len_str
        .parse()
        .map_err(|_| ProtocolError::InvalidFrame(format!("invalid array length: {len_str}")))?;
    if count < 0 {
        return Err(ProtocolError::InvalidFrame(format!(
            "invalid array length: {count}"
        )));
    }
    buf.advance(len_line_end + 2);
    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        match decode_frame(max_bulk_len, buf)? {
            Some(frame) => items.push(frame),
            None => return Err(ProtocolError::Incomplete),
        }
    }
    Ok(RespFrame::Array(items))
}

fn find_crlf(buf: &BytesMut) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}
