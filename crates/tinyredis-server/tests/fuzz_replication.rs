use proptest::prelude::*;
use tinyredis_server::replication::parse_replication_record;

proptest! {
    #[test]
    fn replication_parser_never_panics(input in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = parse_replication_record(&input);
    }
}
