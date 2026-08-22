use libfuzzer_sys::fuzz_target;
use tinyredis_server::replication::parse_replication_record;

fuzz_target!(|data: &[u8]| {
    let _ = parse_replication_record(data);
});
