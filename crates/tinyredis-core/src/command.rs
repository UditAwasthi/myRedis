use std::collections::{HashMap, HashSet, VecDeque};

use crate::database::Database;
use crate::error::{CoreError, CoreResult};
use crate::expiration::ExpiryInfo;
use crate::value::{normalize_range, SortedSet, Value};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        nx: bool,
        ex_secs: Option<u64>,
    },
    Get(String),
    Del(Vec<String>),
    Exists(Vec<String>),
    Mget(Vec<String>),
    Mset(Vec<(String, String)>),
    GetSet {
        key: String,
        value: String,
    },
    Incr(String),
    Decr(String),
    IncrBy {
        key: String,
        by: i64,
    },
    DecrBy {
        key: String,
        by: i64,
    },
    Append {
        key: String,
        value: String,
    },
    Strlen(String),
    Expire {
        key: String,
        seconds: u64,
    },
    Pexpire {
        key: String,
        millis: u64,
    },
    Ttl(String),
    Pttl(String),
    Persist(String),
    Lpush {
        key: String,
        values: Vec<String>,
    },
    Rpush {
        key: String,
        values: Vec<String>,
    },
    Lpop(String),
    Rpop(String),
    Lrange {
        key: String,
        start: isize,
        stop: isize,
    },
    Llen(String),
    Lindex {
        key: String,
        index: isize,
    },
    Sadd {
        key: String,
        members: Vec<String>,
    },
    Srem {
        key: String,
        members: Vec<String>,
    },
    Sismember {
        key: String,
        member: String,
    },
    Smembers(String),
    Scard(String),
    Sinter(Vec<String>),
    Sunion(Vec<String>),
    Sdiff(Vec<String>),
    Hset {
        key: String,
        field: String,
        value: String,
    },
    Hget {
        key: String,
        field: String,
    },
    Hdel {
        key: String,
        fields: Vec<String>,
    },
    Hexists {
        key: String,
        field: String,
    },
    Hgetall(String),
    Hkeys(String),
    Hvals(String),
    Hlen(String),
    Zadd {
        key: String,
        score: f64,
        member: String,
    },
    Zrem {
        key: String,
        members: Vec<String>,
    },
    Zscore {
        key: String,
        member: String,
    },
    Zrank {
        key: String,
        member: String,
    },
    Zrange {
        key: String,
        start: isize,
        stop: isize,
    },
    Zrevrange {
        key: String,
        start: isize,
        stop: isize,
    },
    Zcard(String),
    Multi,
    Exec,
    Discard,
    Watch(Vec<String>),
    Unwatch,
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Publish {
        channel: String,
        message: String,
    },
    Save,
    Bgsave,
    Auth(String),
    Info(Option<String>),
    ReplicaOf {
        host: String,
        port: u16,
    },
    ClusterInfo,
    ClusterNodes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    Ok,
    Status(String),
    Integer(i64),
    BulkString(Option<String>),
    BulkStrings(Vec<Option<String>>),
    Array(Vec<String>),
    Error(String),
}

pub fn execute(db: &mut Database, cmd: &Command) -> CommandResult {
    match execute_inner(db, cmd) {
        Ok(result) => result,
        Err(err) => CommandResult::Error(err.to_string()),
    }
}

fn execute_inner(db: &mut Database, cmd: &Command) -> CoreResult<CommandResult> {
    match cmd {
        Command::Ping => Ok(CommandResult::Status("PONG".into())),
        Command::Echo(msg) => Ok(CommandResult::BulkString(Some(msg.clone()))),
        Command::Set {
            key,
            value,
            nx,
            ex_secs,
        } => {
            if *nx && db.exists(key) {
                return Ok(CommandResult::BulkString(None));
            }
            let expiry = ex_secs.map(|s| ExpiryInfo::from_ttl(std::time::Duration::from_secs(s)));
            db.set_with_expiry(key.clone(), value.clone(), expiry)?;
            Ok(CommandResult::Status("OK".into()))
        }
        Command::Get(key) => Ok(CommandResult::BulkString(db.get(key)?)),
        Command::Del(keys) => Ok(CommandResult::Integer(db.del(keys) as i64)),
        Command::Exists(keys) => {
            let count = keys.iter().filter(|k| db.exists(k)).count();
            Ok(CommandResult::Integer(count as i64))
        }
        Command::Mget(keys) => Ok(CommandResult::BulkStrings(db.mget(keys))),
        Command::Mset(pairs) => {
            db.mset(pairs)?;
            Ok(CommandResult::Status("OK".into()))
        }
        Command::GetSet { key, value } => Ok(CommandResult::BulkString(
            db.get_set(key.clone(), value.clone())?,
        )),
        Command::Incr(key) => incr_by(db, key, 1),
        Command::Decr(key) => incr_by(db, key, -1),
        Command::IncrBy { key, by } => incr_by(db, key, *by),
        Command::DecrBy { key, by } => incr_by(db, key, -(*by)),
        Command::Append { key, value } => {
            let current = db.get(key)?.unwrap_or_default();
            let new_val = format!("{current}{value}");
            let len = new_val.len() as i64;
            db.set(key.clone(), new_val)?;
            Ok(CommandResult::Integer(len))
        }
        Command::Strlen(key) => {
            let len = db.get(key)?.map(|s| s.len()).unwrap_or(0) as i64;
            Ok(CommandResult::Integer(len))
        }
        Command::Expire { key, seconds } => {
            if !db.exists(key) {
                return Ok(CommandResult::Integer(0));
            }
            db.set_expire(key, std::time::Duration::from_secs(*seconds))?;
            Ok(CommandResult::Integer(1))
        }
        Command::Pexpire { key, millis } => {
            if !db.exists(key) {
                return Ok(CommandResult::Integer(0));
            }
            db.set_expire_at_ms(key, *millis)?;
            Ok(CommandResult::Integer(1))
        }
        Command::Ttl(key) => match db.ttl(key) {
            Ok(v) => Ok(CommandResult::Integer(v)),
            Err(CoreError::KeyNotFound) => Ok(CommandResult::Integer(-2)),
            Err(e) => Err(e),
        },
        Command::Pttl(key) => match db.pttl(key) {
            Ok(v) => Ok(CommandResult::Integer(v)),
            Err(CoreError::KeyNotFound) => Ok(CommandResult::Integer(-2)),
            Err(e) => Err(e),
        },
        Command::Persist(key) => {
            if !db.exists(key) {
                return Ok(CommandResult::Integer(0));
            }
            Ok(CommandResult::Integer(if db.persist(key)? { 1 } else { 0 }))
        }
        Command::Lpush { key, values } => list_push(db, key, values, true),
        Command::Rpush { key, values } => list_push(db, key, values, false),
        Command::Lpop(key) => list_pop(db, key, true),
        Command::Rpop(key) => list_pop(db, key, false),
        Command::Lrange { key, start, stop } => {
            let list = get_list(db, key)?;
            let len = list.len() as isize;
            let (s, e) = normalize_range(*start, *stop, len);
            if e < s {
                return Ok(CommandResult::Array(vec![]));
            }
            let items: Vec<String> = list
                .iter()
                .skip(s as usize)
                .take((e - s + 1) as usize)
                .cloned()
                .collect();
            Ok(CommandResult::Array(items))
        }
        Command::Llen(key) => Ok(CommandResult::Integer(get_list(db, key)?.len() as i64)),
        Command::Lindex { key, index } => {
            let list = get_list(db, key)?;
            let len = list.len() as isize;
            let idx = if *index < 0 { len + *index } else { *index };
            if idx < 0 || idx >= len {
                Ok(CommandResult::BulkString(None))
            } else {
                Ok(CommandResult::BulkString(Some(list[idx as usize].clone())))
            }
        }
        Command::Sadd { key, members } => {
            let mut added = 0i64;
            db.update_value(key, Some(Value::Set(HashSet::new())), |val| {
                let set = as_set_mut(val)?;
                for m in members {
                    if set.insert(m.clone()) {
                        added += 1;
                    }
                }
                Ok(())
            })?;
            Ok(CommandResult::Integer(added))
        }
        Command::Srem { key, members } => {
            let removed = db.update_value(key, None, |val| {
                let set = as_set_mut(val)?;
                Ok(members.iter().filter(|m| set.remove(*m)).count() as i64)
            })?;
            Ok(CommandResult::Integer(removed))
        }
        Command::Sismember { key, member } => {
            let present = match get_set(db, key) {
                Ok(set) => set.contains(member),
                Err(CoreError::KeyNotFound) => false,
                Err(e) => return Err(e),
            };
            Ok(CommandResult::Integer(if present { 1 } else { 0 }))
        }
        Command::Smembers(key) => {
            let mut members: Vec<String> = get_set(db, key)?.into_iter().collect();
            members.sort();
            Ok(CommandResult::Array(members))
        }
        Command::Scard(key) => Ok(CommandResult::Integer(get_set(db, key)?.len() as i64)),
        Command::Sinter(keys) => set_op(db, keys, SetOp::Inter),
        Command::Sunion(keys) => set_op(db, keys, SetOp::Union),
        Command::Sdiff(keys) => set_op(db, keys, SetOp::Diff),
        Command::Hset { key, field, value } => {
            let added = db.update_value(key, Some(Value::Hash(HashMap::new())), |val| {
                let hash = as_hash_mut(val)?;
                Ok(if hash.insert(field.clone(), value.clone()).is_none() {
                    1
                } else {
                    0
                })
            })?;
            Ok(CommandResult::Integer(added as i64))
        }
        Command::Hget { key, field } => Ok(CommandResult::BulkString(
            get_hash(db, key)?.get(field).cloned(),
        )),
        Command::Hdel { key, fields } => {
            let removed = db.update_value(key, None, |val| {
                let hash = as_hash_mut(val)?;
                Ok(fields.iter().filter(|f| hash.remove(*f).is_some()).count() as i64)
            })?;
            Ok(CommandResult::Integer(removed))
        }
        Command::Hexists { key, field } => {
            let exists = get_hash(db, key)?.contains_key(field);
            Ok(CommandResult::Integer(if exists { 1 } else { 0 }))
        }
        Command::Hgetall(key) => {
            let hash = get_hash(db, key)?;
            let mut pairs = Vec::new();
            let mut fields: Vec<_> = hash.keys().cloned().collect();
            fields.sort();
            for field in fields {
                if let Some(val) = hash.get(&field) {
                    pairs.push(field);
                    pairs.push(val.clone());
                }
            }
            Ok(CommandResult::Array(pairs))
        }
        Command::Hkeys(key) => {
            let mut keys: Vec<String> = get_hash(db, key)?.keys().cloned().collect();
            keys.sort();
            Ok(CommandResult::Array(keys))
        }
        Command::Hvals(key) => {
            let mut vals: Vec<String> = get_hash(db, key)?.values().cloned().collect();
            vals.sort();
            Ok(CommandResult::Array(vals))
        }
        Command::Hlen(key) => Ok(CommandResult::Integer(get_hash(db, key)?.len() as i64)),
        Command::Zadd { key, score, member } => {
            let added = db.update_value(key, Some(Value::SortedSet(SortedSet::new())), |val| {
                let zset = as_zset_mut(val)?;
                Ok(zset.add(member.clone(), *score)? as i64)
            })?;
            Ok(CommandResult::Integer(added))
        }
        Command::Zrem { key, members } => {
            let removed = db.update_value(key, None, |val| {
                let zset = as_zset_mut(val)?;
                Ok(members.iter().filter(|m| zset.remove(m)).count() as i64)
            })?;
            Ok(CommandResult::Integer(removed))
        }
        Command::Zscore { key, member } => Ok(CommandResult::BulkString(
            get_zset(db, key)?.score(member).map(|s| s.to_string()),
        )),
        Command::Zrank { key, member } => match get_zset(db, key)?.rank(member) {
            Some(rank) => Ok(CommandResult::Integer(rank as i64)),
            None => Ok(CommandResult::BulkString(None)),
        },
        Command::Zrange { key, start, stop } => Ok(CommandResult::Array(
            get_zset(db, key)?.range(*start, *stop),
        )),
        Command::Zrevrange { key, start, stop } => Ok(CommandResult::Array(
            get_zset(db, key)?.rev_range(*start, *stop),
        )),
        Command::Zcard(key) => Ok(CommandResult::Integer(get_zset(db, key)?.len() as i64)),
        Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::Watch(_)
        | Command::Unwatch
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::Publish { .. }
        | Command::Save
        | Command::Bgsave
        | Command::Auth(_)
        | Command::Info(_)
        | Command::ReplicaOf { .. }
        | Command::ClusterInfo
        | Command::ClusterNodes => Err(CoreError::Internal(
            "command handled at server layer".into(),
        )),
    }
}

enum SetOp {
    Inter,
    Union,
    Diff,
}

fn set_op(db: &Database, keys: &[String], op: SetOp) -> CoreResult<CommandResult> {
    if keys.is_empty() {
        return Ok(CommandResult::Array(vec![]));
    }
    let sets: CoreResult<Vec<HashSet<String>>> = keys.iter().map(|k| get_set(db, k)).collect();
    let sets = sets?;
    let result = match op {
        SetOp::Inter => sets.iter().skip(1).fold(sets[0].clone(), |acc, s| {
            acc.intersection(s).cloned().collect()
        }),
        SetOp::Union => sets.iter().fold(HashSet::new(), |mut acc, s| {
            acc.extend(s.iter().cloned());
            acc
        }),
        SetOp::Diff => sets.iter().skip(1).fold(sets[0].clone(), |acc, s| {
            acc.difference(s).cloned().collect()
        }),
    };
    let mut out: Vec<String> = result.into_iter().collect();
    out.sort();
    Ok(CommandResult::Array(out))
}

fn incr_by(db: &mut Database, key: &str, by: i64) -> CoreResult<CommandResult> {
    let current = db.get(key)?.unwrap_or_else(|| "0".into());
    let num: i64 = current.parse().map_err(|_| CoreError::InvalidInteger)?;
    let new_val = num.checked_add(by).ok_or(CoreError::IntegerOverflow)?;
    db.set(key.to_string(), new_val.to_string())?;
    Ok(CommandResult::Integer(new_val))
}

fn list_push(
    db: &mut Database,
    key: &str,
    values: &[String],
    left: bool,
) -> CoreResult<CommandResult> {
    let len = db.update_value(key, Some(Value::List(VecDeque::new())), |val| {
        let list = as_list_mut(val)?;
        for v in values {
            if left {
                list.push_front(v.clone());
            } else {
                list.push_back(v.clone());
            }
        }
        Ok(list.len() as i64)
    })?;
    Ok(CommandResult::Integer(len))
}

fn list_pop(db: &mut Database, key: &str, left: bool) -> CoreResult<CommandResult> {
    let val = db.update_value(key, None, |val| {
        let list = as_list_mut(val)?;
        Ok(if left {
            list.pop_front()
        } else {
            list.pop_back()
        })
    })?;
    Ok(CommandResult::BulkString(val))
}

fn get_list(db: &Database, key: &str) -> CoreResult<VecDeque<String>> {
    match db.get_value(key)? {
        Value::List(list) => Ok(list.clone()),
        other => Err(Database::wrong_type("list", other.type_name())),
    }
}

fn get_set(db: &Database, key: &str) -> CoreResult<HashSet<String>> {
    match db.get_value(key)? {
        Value::Set(set) => Ok(set.clone()),
        other => Err(Database::wrong_type("set", other.type_name())),
    }
}

fn get_hash(db: &Database, key: &str) -> CoreResult<HashMap<String, String>> {
    match db.get_value(key)? {
        Value::Hash(hash) => Ok(hash.clone()),
        other => Err(Database::wrong_type("hash", other.type_name())),
    }
}

fn get_zset(db: &Database, key: &str) -> CoreResult<SortedSet> {
    match db.get_value(key)? {
        Value::SortedSet(z) => Ok(z.clone()),
        other => Err(Database::wrong_type("sortedset", other.type_name())),
    }
}

fn as_list_mut(val: &mut Value) -> CoreResult<&mut VecDeque<String>> {
    match val {
        Value::List(list) => Ok(list),
        other => Err(Database::wrong_type("list", other.type_name())),
    }
}

fn as_set_mut(val: &mut Value) -> CoreResult<&mut HashSet<String>> {
    match val {
        Value::Set(set) => Ok(set),
        other => Err(Database::wrong_type("set", other.type_name())),
    }
}

fn as_hash_mut(val: &mut Value) -> CoreResult<&mut HashMap<String, String>> {
    match val {
        Value::Hash(hash) => Ok(hash),
        other => Err(Database::wrong_type("hash", other.type_name())),
    }
}

fn as_zset_mut(val: &mut Value) -> CoreResult<&mut SortedSet> {
    match val {
        Value::SortedSet(z) => Ok(z),
        other => Err(Database::wrong_type("sortedset", other.type_name())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_commands() {
        let mut db = Database::new();
        execute(
            &mut db,
            &Command::Lpush {
                key: "list".into(),
                values: vec!["a".into(), "b".into()],
            },
        );
        let r = execute(
            &mut db,
            &Command::Lrange {
                key: "list".into(),
                start: 0,
                stop: -1,
            },
        );
        assert_eq!(r, CommandResult::Array(vec!["b".into(), "a".into()]));
    }
}
