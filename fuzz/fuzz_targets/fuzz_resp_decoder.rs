use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tinyredis_protocol::RespDecoder;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let decoder = RespDecoder::with_max_bulk_len(64 * 1024);
    loop {
        match decoder.decode(&mut buf) {
            Ok(None) | Err(_) => break,
            Ok(Some(_)) => continue,
        }
    }
});
