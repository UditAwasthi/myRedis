use proptest::prelude::*;
use tinyredis_core::Database;
use tinyredis_server::persistence::{parse_snapshot_bytes, replay_aof_bytes};

proptest! {
    #[test]
    fn snapshot_parser_never_panics(input in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = parse_snapshot_bytes(&input);
    }

    #[test]
    fn aof_replay_never_panics(input in prop::collection::vec(any::<u8>(), 0..4096)) {
        let mut db = Database::new();
        let _ = replay_aof_bytes(&mut db, &input);
    }

    #[test]
    fn valid_aof_set_replays(key in prop::string::string_regex("[a-z]{1,8}").unwrap(), val in prop::string::string_regex("[a-z]{1,8}").unwrap()) {
        let line = format!(
            r#"{{"command":{{"Set":{{"key":"{key}","value":"{val}","nx":false,"ex_secs":null}}}}}}"#
        );
        let mut db = Database::new();
        let report = replay_aof_bytes(&mut db, line.as_bytes());
        prop_assert_eq!(report.replayed, 1);
        prop_assert_eq!(report.skipped, 0);
        prop_assert_eq!(db.get(&key).unwrap(), Some(val));
    }
}
