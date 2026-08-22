use tinyredis_core::Command;

use crate::error::ProtocolError;
use crate::frame::RespFrame;

pub fn parse_command(frame: &RespFrame) -> Result<Command, ProtocolError> {
    let mut args = RespFrame::command_args(frame).ok_or_else(|| {
        ProtocolError::InvalidFrame("command must be a RESP array of bulk strings".into())
    })?;

    if args.is_empty() {
        return Err(ProtocolError::InvalidFrame("empty command array".into()));
    }

    let cmd_name = args.remove(0);
    match cmd_name.to_ascii_uppercase().as_str() {
        "PING" => {
            expect_arg_count(&args, 0, "PING")?;
            Ok(Command::Ping)
        }
        "ECHO" => {
            expect_arg_count(&args, 1, "ECHO")?;
            Ok(Command::Echo(args[0].clone()))
        }
        "SET" => parse_set(args),
        "GET" => {
            expect_arg_count(&args, 1, "GET")?;
            Ok(Command::Get(args[0].clone()))
        }
        "DEL" => Ok(Command::Del(args)),
        "EXISTS" => Ok(Command::Exists(args)),
        "MGET" => Ok(Command::Mget(args)),
        "MSET" => Ok(Command::Mset(parse_mset_pairs(&args)?)),
        "GETSET" => {
            expect_arg_count(&args, 2, "GETSET")?;
            Ok(Command::GetSet {
                key: args[0].clone(),
                value: args[1].clone(),
            })
        }
        "INCR" => {
            expect_arg_count(&args, 1, "INCR")?;
            Ok(Command::Incr(args[0].clone()))
        }
        "DECR" => {
            expect_arg_count(&args, 1, "DECR")?;
            Ok(Command::Decr(args[0].clone()))
        }
        "INCRBY" => {
            expect_arg_count(&args, 2, "INCRBY")?;
            Ok(Command::IncrBy {
                key: args[0].clone(),
                by: parse_i64(&args[1])?,
            })
        }
        "DECRBY" => {
            expect_arg_count(&args, 2, "DECRBY")?;
            Ok(Command::DecrBy {
                key: args[0].clone(),
                by: parse_i64(&args[1])?,
            })
        }
        "APPEND" => {
            expect_arg_count(&args, 2, "APPEND")?;
            Ok(Command::Append {
                key: args[0].clone(),
                value: args[1].clone(),
            })
        }
        "STRLEN" => {
            expect_arg_count(&args, 1, "STRLEN")?;
            Ok(Command::Strlen(args[0].clone()))
        }
        "EXPIRE" => {
            expect_arg_count(&args, 2, "EXPIRE")?;
            Ok(Command::Expire {
                key: args[0].clone(),
                seconds: parse_u64(&args[1])?,
            })
        }
        "PEXPIRE" => {
            expect_arg_count(&args, 2, "PEXPIRE")?;
            Ok(Command::Pexpire {
                key: args[0].clone(),
                millis: parse_u64(&args[1])?,
            })
        }
        "TTL" => {
            expect_arg_count(&args, 1, "TTL")?;
            Ok(Command::Ttl(args[0].clone()))
        }
        "PTTL" => {
            expect_arg_count(&args, 1, "PTTL")?;
            Ok(Command::Pttl(args[0].clone()))
        }
        "PERSIST" => {
            expect_arg_count(&args, 1, "PERSIST")?;
            Ok(Command::Persist(args[0].clone()))
        }
        "LPUSH" => parse_key_values(args, true),
        "RPUSH" => parse_key_values(args, false),
        "LPOP" => {
            expect_arg_count(&args, 1, "LPOP")?;
            Ok(Command::Lpop(args[0].clone()))
        }
        "RPOP" => {
            expect_arg_count(&args, 1, "RPOP")?;
            Ok(Command::Rpop(args[0].clone()))
        }
        "LRANGE" => {
            expect_arg_count(&args, 3, "LRANGE")?;
            Ok(Command::Lrange {
                key: args[0].clone(),
                start: parse_isize(&args[1])?,
                stop: parse_isize(&args[2])?,
            })
        }
        "LLEN" => {
            expect_arg_count(&args, 1, "LLEN")?;
            Ok(Command::Llen(args[0].clone()))
        }
        "LINDEX" => {
            expect_arg_count(&args, 2, "LINDEX")?;
            Ok(Command::Lindex {
                key: args[0].clone(),
                index: parse_isize(&args[1])?,
            })
        }
        "SADD" => parse_key_members(args, true),
        "SREM" => parse_key_members(args, false),
        "SISMEMBER" => {
            expect_arg_count(&args, 2, "SISMEMBER")?;
            Ok(Command::Sismember {
                key: args[0].clone(),
                member: args[1].clone(),
            })
        }
        "SMEMBERS" => {
            expect_arg_count(&args, 1, "SMEMBERS")?;
            Ok(Command::Smembers(args[0].clone()))
        }
        "SCARD" => {
            expect_arg_count(&args, 1, "SCARD")?;
            Ok(Command::Scard(args[0].clone()))
        }
        "SINTER" => Ok(Command::Sinter(args)),
        "SUNION" => Ok(Command::Sunion(args)),
        "SDIFF" => Ok(Command::Sdiff(args)),
        "HSET" => {
            expect_arg_count(&args, 3, "HSET")?;
            Ok(Command::Hset {
                key: args[0].clone(),
                field: args[1].clone(),
                value: args[2].clone(),
            })
        }
        "HGET" => {
            expect_arg_count(&args, 2, "HGET")?;
            Ok(Command::Hget {
                key: args[0].clone(),
                field: args[1].clone(),
            })
        }
        "HDEL" => {
            expect_min_args(&args, 2, "HDEL")?;
            let key = args[0].clone();
            let fields = args[1..].to_vec();
            Ok(Command::Hdel { key, fields })
        }
        "HEXISTS" => {
            expect_arg_count(&args, 2, "HEXISTS")?;
            Ok(Command::Hexists {
                key: args[0].clone(),
                field: args[1].clone(),
            })
        }
        "HGETALL" => {
            expect_arg_count(&args, 1, "HGETALL")?;
            Ok(Command::Hgetall(args[0].clone()))
        }
        "HKEYS" => {
            expect_arg_count(&args, 1, "HKEYS")?;
            Ok(Command::Hkeys(args[0].clone()))
        }
        "HVALS" => {
            expect_arg_count(&args, 1, "HVALS")?;
            Ok(Command::Hvals(args[0].clone()))
        }
        "HLEN" => {
            expect_arg_count(&args, 1, "HLEN")?;
            Ok(Command::Hlen(args[0].clone()))
        }
        "ZADD" => {
            expect_arg_count(&args, 3, "ZADD")?;
            Ok(Command::Zadd {
                key: args[0].clone(),
                score: parse_f64(&args[1])?,
                member: args[2].clone(),
            })
        }
        "ZREM" => {
            expect_min_args(&args, 2, "ZREM")?;
            let key = args[0].clone();
            let members = args[1..].to_vec();
            Ok(Command::Zrem { key, members })
        }
        "ZSCORE" => {
            expect_arg_count(&args, 2, "ZSCORE")?;
            Ok(Command::Zscore {
                key: args[0].clone(),
                member: args[1].clone(),
            })
        }
        "ZRANK" => {
            expect_arg_count(&args, 2, "ZRANK")?;
            Ok(Command::Zrank {
                key: args[0].clone(),
                member: args[1].clone(),
            })
        }
        "ZRANGE" => {
            expect_arg_count(&args, 3, "ZRANGE")?;
            Ok(Command::Zrange {
                key: args[0].clone(),
                start: parse_isize(&args[1])?,
                stop: parse_isize(&args[2])?,
            })
        }
        "ZREVRANGE" => {
            expect_arg_count(&args, 3, "ZREVRANGE")?;
            Ok(Command::Zrevrange {
                key: args[0].clone(),
                start: parse_isize(&args[1])?,
                stop: parse_isize(&args[2])?,
            })
        }
        "ZCARD" => {
            expect_arg_count(&args, 1, "ZCARD")?;
            Ok(Command::Zcard(args[0].clone()))
        }
        "MULTI" => {
            expect_arg_count(&args, 0, "MULTI")?;
            Ok(Command::Multi)
        }
        "EXEC" => {
            expect_arg_count(&args, 0, "EXEC")?;
            Ok(Command::Exec)
        }
        "DISCARD" => {
            expect_arg_count(&args, 0, "DISCARD")?;
            Ok(Command::Discard)
        }
        "WATCH" => Ok(Command::Watch(args)),
        "UNWATCH" => {
            expect_arg_count(&args, 0, "UNWATCH")?;
            Ok(Command::Unwatch)
        }
        "SUBSCRIBE" => Ok(Command::Subscribe(args)),
        "UNSUBSCRIBE" => Ok(Command::Unsubscribe(args)),
        "PUBLISH" => {
            expect_arg_count(&args, 2, "PUBLISH")?;
            Ok(Command::Publish {
                channel: args[0].clone(),
                message: args[1].clone(),
            })
        }
        "SAVE" => {
            expect_arg_count(&args, 0, "SAVE")?;
            Ok(Command::Save)
        }
        "BGSAVE" => {
            expect_arg_count(&args, 0, "BGSAVE")?;
            Ok(Command::Bgsave)
        }
        "AUTH" => {
            expect_arg_count(&args, 1, "AUTH")?;
            Ok(Command::Auth(args[0].clone()))
        }
        "INFO" => {
            let section = if args.is_empty() {
                None
            } else if args.len() == 1 {
                Some(args[0].clone())
            } else {
                return Err(ProtocolError::WrongArgCount {
                    command: "INFO".into(),
                    expected: "0 or 1".into(),
                    got: args.len(),
                });
            };
            Ok(Command::Info(section))
        }
        "REPLICAOF" => {
            expect_arg_count(&args, 2, "REPLICAOF")?;
            Ok(Command::ReplicaOf {
                host: args[0].clone(),
                port: parse_u16(&args[1])?,
            })
        }
        "CLUSTER" => parse_cluster(&args),
        _ => Err(ProtocolError::UnknownCommand(cmd_name)),
    }
}

fn parse_set(mut args: Vec<String>) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArgCount {
            command: "SET".into(),
            expected: "at least 2".into(),
            got: args.len(),
        });
    }

    let key = args.remove(0);
    let value = args.remove(0);
    let mut nx = false;
    let mut ex_secs = None;
    let mut i = 0;

    while i < args.len() {
        let token = args[i].to_ascii_uppercase();
        match token.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "EX" => {
                if i + 1 >= args.len() {
                    return Err(ProtocolError::InvalidArgument(
                        "SET EX requires a seconds value".into(),
                    ));
                }
                ex_secs = Some(parse_u64(&args[i + 1])?);
                i += 2;
            }
            other => {
                return Err(ProtocolError::InvalidArgument(format!(
                    "unknown SET option: {other}"
                )));
            }
        }
    }

    Ok(Command::Set {
        key,
        value,
        nx,
        ex_secs,
    })
}

fn parse_key_values(args: Vec<String>, left: bool) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArgCount {
            command: if left { "LPUSH" } else { "RPUSH" }.into(),
            expected: "at least 2".into(),
            got: args.len(),
        });
    }
    let key = args[0].clone();
    let values = args[1..].to_vec();
    if left {
        Ok(Command::Lpush { key, values })
    } else {
        Ok(Command::Rpush { key, values })
    }
}

fn parse_key_members(args: Vec<String>, add: bool) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArgCount {
            command: if add { "SADD" } else { "SREM" }.into(),
            expected: "at least 2".into(),
            got: args.len(),
        });
    }
    let key = args[0].clone();
    let members = args[1..].to_vec();
    if add {
        Ok(Command::Sadd { key, members })
    } else {
        Ok(Command::Srem { key, members })
    }
}

fn parse_cluster(args: &[String]) -> Result<Command, ProtocolError> {
    expect_arg_count(args, 1, "CLUSTER")?;
    match args[0].to_ascii_uppercase().as_str() {
        "INFO" => Ok(Command::ClusterInfo),
        "NODES" => Ok(Command::ClusterNodes),
        other => Err(ProtocolError::InvalidArgument(format!(
            "unknown CLUSTER subcommand: {other}"
        ))),
    }
}

fn parse_mset_pairs(args: &[String]) -> Result<Vec<(String, String)>, ProtocolError> {
    if args.len() % 2 != 0 {
        return Err(ProtocolError::InvalidArgument(
            "MSET requires an even number of arguments".into(),
        ));
    }
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for chunk in args.chunks_exact(2) {
        pairs.push((chunk[0].clone(), chunk[1].clone()));
    }
    Ok(pairs)
}

fn parse_i64(s: &str) -> Result<i64, ProtocolError> {
    s.parse::<i64>()
        .map_err(|_| ProtocolError::InvalidArgument(format!("invalid integer: {s}")))
}

fn parse_isize(s: &str) -> Result<isize, ProtocolError> {
    s.parse::<isize>()
        .map_err(|_| ProtocolError::InvalidArgument(format!("invalid integer: {s}")))
}

fn parse_u64(s: &str) -> Result<u64, ProtocolError> {
    s.parse::<u64>()
        .map_err(|_| ProtocolError::InvalidArgument(format!("invalid unsigned integer: {s}")))
}

fn parse_u16(s: &str) -> Result<u16, ProtocolError> {
    s.parse::<u16>()
        .map_err(|_| ProtocolError::InvalidArgument(format!("invalid port: {s}")))
}

fn parse_f64(s: &str) -> Result<f64, ProtocolError> {
    s.parse::<f64>()
        .map_err(|_| ProtocolError::InvalidArgument(format!("invalid float: {s}")))
}

fn expect_arg_count(args: &[String], expected: usize, cmd: &str) -> Result<(), ProtocolError> {
    if args.len() != expected {
        return Err(ProtocolError::WrongArgCount {
            command: cmd.into(),
            expected: expected.to_string(),
            got: args.len(),
        });
    }
    Ok(())
}

fn expect_min_args(args: &[String], min: usize, cmd: &str) -> Result<(), ProtocolError> {
    if args.len() < min {
        return Err(ProtocolError::WrongArgCount {
            command: cmd.into(),
            expected: format!("at least {min}"),
            got: args.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_frame(cmd: &str, args: &[&str]) -> RespFrame {
        let mut items = vec![RespFrame::Bulk(Some(cmd.as_bytes().to_vec()))];
        for arg in args {
            items.push(RespFrame::Bulk(Some(arg.as_bytes().to_vec())));
        }
        RespFrame::Array(items)
    }

    #[test]
    fn parse_ping() {
        assert_eq!(
            parse_command(&command_frame("PING", &[])).unwrap(),
            Command::Ping
        );
    }

    #[test]
    fn parse_ping_case_insensitive() {
        assert_eq!(
            parse_command(&command_frame("ping", &[])).unwrap(),
            Command::Ping
        );
    }

    #[test]
    fn parse_echo() {
        assert_eq!(
            parse_command(&command_frame("ECHO", &["hello"])).unwrap(),
            Command::Echo("hello".into())
        );
    }

    #[test]
    fn parse_set_basic() {
        assert_eq!(
            parse_command(&command_frame("SET", &["key", "value"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: "value".into(),
                nx: false,
                ex_secs: None,
            }
        );
    }

    #[test]
    fn parse_set_with_ex() {
        assert_eq!(
            parse_command(&command_frame("SET", &["key", "value", "EX", "60"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: "value".into(),
                nx: false,
                ex_secs: Some(60),
            }
        );
    }

    #[test]
    fn parse_set_with_nx_and_ex() {
        assert_eq!(
            parse_command(&command_frame("SET", &["key", "value", "NX", "EX", "10"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: "value".into(),
                nx: true,
                ex_secs: Some(10),
            }
        );
    }

    #[test]
    fn parse_get() {
        assert_eq!(
            parse_command(&command_frame("GET", &["key"])).unwrap(),
            Command::Get("key".into())
        );
    }

    #[test]
    fn parse_del_multiple() {
        assert_eq!(
            parse_command(&command_frame("DEL", &["a", "b"])).unwrap(),
            Command::Del(vec!["a".into(), "b".into()])
        );
    }

    #[test]
    fn parse_mset() {
        assert_eq!(
            parse_command(&command_frame("MSET", &["k1", "v1", "k2", "v2"])).unwrap(),
            Command::Mset(vec![("k1".into(), "v1".into()), ("k2".into(), "v2".into()),])
        );
    }

    #[test]
    fn parse_mset_odd_args_is_error() {
        assert!(matches!(
            parse_command(&command_frame("MSET", &["k1", "v1", "k2"])),
            Err(ProtocolError::InvalidArgument(_))
        ));
    }

    #[test]
    fn parse_incrby() {
        assert_eq!(
            parse_command(&command_frame("INCRBY", &["counter", "5"])).unwrap(),
            Command::IncrBy {
                key: "counter".into(),
                by: 5,
            }
        );
    }

    #[test]
    fn parse_lpush() {
        assert_eq!(
            parse_command(&command_frame("LPUSH", &["list", "a", "b"])).unwrap(),
            Command::Lpush {
                key: "list".into(),
                values: vec!["a".into(), "b".into()],
            }
        );
    }

    #[test]
    fn parse_hset() {
        assert_eq!(
            parse_command(&command_frame("HSET", &["hash", "field", "val"])).unwrap(),
            Command::Hset {
                key: "hash".into(),
                field: "field".into(),
                value: "val".into(),
            }
        );
    }

    #[test]
    fn parse_zadd() {
        assert_eq!(
            parse_command(&command_frame("ZADD", &["z", "1.5", "member"])).unwrap(),
            Command::Zadd {
                key: "z".into(),
                score: 1.5,
                member: "member".into(),
            }
        );
    }

    #[test]
    fn parse_transaction_commands() {
        assert_eq!(
            parse_command(&command_frame("MULTI", &[])).unwrap(),
            Command::Multi
        );
        assert_eq!(
            parse_command(&command_frame("EXEC", &[])).unwrap(),
            Command::Exec
        );
        assert_eq!(
            parse_command(&command_frame("DISCARD", &[])).unwrap(),
            Command::Discard
        );
        assert_eq!(
            parse_command(&command_frame("WATCH", &["k1", "k2"])).unwrap(),
            Command::Watch(vec!["k1".into(), "k2".into()])
        );
        assert_eq!(
            parse_command(&command_frame("UNWATCH", &[])).unwrap(),
            Command::Unwatch
        );
    }

    #[test]
    fn parse_pubsub() {
        assert_eq!(
            parse_command(&command_frame("SUBSCRIBE", &["news"])).unwrap(),
            Command::Subscribe(vec!["news".into()])
        );
        assert_eq!(
            parse_command(&command_frame("UNSUBSCRIBE", &["news"])).unwrap(),
            Command::Unsubscribe(vec!["news".into()])
        );
        assert_eq!(
            parse_command(&command_frame("PUBLISH", &["news", "hello"])).unwrap(),
            Command::Publish {
                channel: "news".into(),
                message: "hello".into(),
            }
        );
    }

    #[test]
    fn parse_admin_commands() {
        assert_eq!(
            parse_command(&command_frame("SAVE", &[])).unwrap(),
            Command::Save
        );
        assert_eq!(
            parse_command(&command_frame("BGSAVE", &[])).unwrap(),
            Command::Bgsave
        );
        assert_eq!(
            parse_command(&command_frame("AUTH", &["secret"])).unwrap(),
            Command::Auth("secret".into())
        );
        assert_eq!(
            parse_command(&command_frame("INFO", &[])).unwrap(),
            Command::Info(None)
        );
        assert_eq!(
            parse_command(&command_frame("INFO", &["memory"])).unwrap(),
            Command::Info(Some("memory".into()))
        );
        assert_eq!(
            parse_command(&command_frame("REPLICAOF", &["127.0.0.1", "6379"])).unwrap(),
            Command::ReplicaOf {
                host: "127.0.0.1".into(),
                port: 6379,
            }
        );
    }

    #[test]
    fn parse_cluster_subcommands() {
        assert_eq!(
            parse_command(&command_frame("CLUSTER", &["INFO"])).unwrap(),
            Command::ClusterInfo
        );
        assert_eq!(
            parse_command(&command_frame("cluster", &["nodes"])).unwrap(),
            Command::ClusterNodes
        );
    }

    #[test]
    fn non_array_input_is_error() {
        assert!(matches!(
            parse_command(&RespFrame::Simple("PING".into())),
            Err(ProtocolError::InvalidFrame(_))
        ));
    }

    #[test]
    fn unknown_command_is_error() {
        assert!(matches!(
            parse_command(&command_frame("NOTACOMMAND", &[])),
            Err(ProtocolError::UnknownCommand(_))
        ));
    }
}
