use proptest::prelude::*;
use tinyredis_core::Database;

proptest! {
    #[test]
    fn set_then_get_returns_same_value(
        key in prop::string::string_regex("[a-zA-Z0-9:_-]{1,32}").unwrap(),
        value in prop::string::string_regex("[\\x20-\\x7e]{0,64}").unwrap(),
    ) {
        let mut db = Database::new();
        db.set(key.clone(), value.clone()).unwrap();
        prop_assert_eq!(db.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn del_removes_key(
        key in prop::string::string_regex("[a-z]{1,16}").unwrap(),
        value in prop::string::string_regex("[a-z]{1,16}").unwrap(),
    ) {
        let mut db = Database::new();
        db.set(key.clone(), value).unwrap();
        prop_assert!(db.exists(&key));
        prop_assert_eq!(db.del(&[key.clone()]), 1);
        prop_assert!(!db.exists(&key));
    }

    #[test]
    fn incr_from_zero_is_monotonic(
        delta in 1i64..1000,
        steps in 1usize..20,
    ) {
        let mut db = Database::new();
        db.set("n".into(), "0".into()).unwrap();
        for _ in 0..steps {
            let current = db.get("n").unwrap().unwrap().parse::<i64>().unwrap();
            db.set("n".into(), (current + delta).to_string()).unwrap();
        }
        let final_val: i64 = db.get("n").unwrap().unwrap().parse().unwrap();
        prop_assert_eq!(final_val, delta * steps as i64);
    }
}
