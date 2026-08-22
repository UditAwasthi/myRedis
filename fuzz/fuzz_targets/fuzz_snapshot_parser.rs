use libfuzzer_sys::fuzz_target;
use tinyredis_server::persistence::parse_snapshot_bytes;

fuzz_target!(|data: &[u8]| {
    let _ = parse_snapshot_bytes(data);
});
