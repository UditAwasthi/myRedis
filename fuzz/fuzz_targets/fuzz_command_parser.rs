use libfuzzer_sys::fuzz_target;
use tinyredis_protocol::{parse_command, RespDecoder, RespFrame};

fuzz_target!(|data: &[u8]| {
    let mut buf = bytes::BytesMut::from(data);
    let decoder = RespDecoder::with_max_bulk_len(64 * 1024);
    while let Ok(Some(frame)) = decoder.decode(&mut buf) {
        let _ = parse_command(&frame);
    }

    // Also try parsing raw bulk-only frames built from fuzz bytes.
    if !data.is_empty() {
        let frame = RespFrame::Bulk(Some(data.to_vec()));
        let _ = parse_command(&frame);
    }
});
