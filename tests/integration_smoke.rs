use std::time::Duration;

use tinyredis_core::{Database, ExpiryInfo};
use tinyredis_protocol::{encode_frame, parse_command, RespDecoder, RespFrame};

#[test]
fn set_get_roundtrip_via_protocol() {
    let frame = command_frame("SET", &["hello", "world"]);
    let cmd = parse_command(&frame).expect("parse SET");
    let mut db = Database::new();
    tinyredis_core::execute(&mut db, &cmd);
    let get = parse_command(&command_frame("GET", &["hello"])).expect("parse GET");
    let result = tinyredis_core::execute(&mut db, &get);
    assert_eq!(
        result,
        tinyredis_core::CommandResult::BulkString(Some("world".into()))
    );
}

#[test]
fn resp_encode_decode_roundtrip() {
    let frame = RespFrame::Bulk(Some(b"payload".to_vec()));
    let encoded = encode_frame(&frame);
    let mut buf = bytes::BytesMut::from(&encoded[..]);
    let decoder = RespDecoder::new();
    let decoded = decoder.decode(&mut buf).expect("decode").expect("frame");
    assert_eq!(decoded, frame);
}

#[test]
fn expire_ttl_integration() {
    let mut db = Database::new();
    db.set_with_expiry(
        "temp".into(),
        "value".into(),
        Some(ExpiryInfo::from_ttl(Duration::from_secs(120))),
    )
    .unwrap();
    assert!(db.ttl("temp").unwrap() >= 119);
}

fn command_frame(cmd: &str, args: &[&str]) -> RespFrame {
    let mut items = vec![RespFrame::Bulk(Some(cmd.as_bytes().to_vec()))];
    for arg in args {
        items.push(RespFrame::Bulk(Some(arg.as_bytes().to_vec())));
    }
    RespFrame::Array(items)
}
