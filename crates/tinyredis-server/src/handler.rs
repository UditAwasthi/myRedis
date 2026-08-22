use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::BytesMut;
use tinyredis_core::transaction::{TransactionManager, TransactionState};
use tinyredis_core::{execute, Command, CommandResult, CoreError};
use tinyredis_protocol::{
    encode_error, encode_frame, encode_result, parse_command, ProtocolError, RespDecoder, RespFrame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::warn;

use crate::auth;
use crate::config::ReplicationRole;
use crate::state::SharedAppState;

pub async fn handle_connection(stream: TcpStream, state: SharedAppState) -> anyhow::Result<()> {
    let permit = match state.connection_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let mut stream = stream;
            stream
                .write_all(&encode_error("ERR max number of clients reached"))
                .await?;
            stream.shutdown().await?;
            return Ok(());
        }
    };

    state.client_connected();
    let result = handle_connection_inner(stream, state.clone()).await;
    state.client_disconnected();
    drop(permit);
    result
}

async fn handle_connection_inner(
    mut stream: TcpStream,
    state: SharedAppState,
) -> anyhow::Result<()> {
    let timeout_duration = state.config.connection_timeout;
    let mut buf = BytesMut::with_capacity(4096);
    let decoder = RespDecoder::new();
    let mut conn = ConnectionContext::new();

    loop {
        if !conn.has_subscriptions() {
            let read_result = timeout(timeout_duration, stream.read_buf(&mut buf)).await;
            match read_result {
                Ok(Ok(0)) => break,
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    let _ = stream.write_all(&encode_error("ERR timeout")).await;
                    break;
                }
            }
        } else {
            tokio::select! {
                read_result = timeout(timeout_duration, stream.read_buf(&mut buf)) => {
                    match read_result {
                        Ok(Ok(0)) => break,
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => return Err(e.into()),
                        Err(_) => {
                            let _ = stream.write_all(&encode_error("ERR timeout")).await;
                            break;
                        }
                    }
                }
                msg = wait_pubsub_message(&mut conn) => {
                    if let Some((channel, message)) = msg {
                        write_pubsub_message(&mut stream, &channel, &message).await?;
                    }
                }
            }
        }

        while let Some(frame) = decoder.decode(&mut buf)? {
            if conn.pubsub_mode {
                match handle_pubsub_frame(&mut conn, &state, &frame, &mut stream).await {
                    Ok(continue_loop) => {
                        if !continue_loop {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            let cmd = match parse_command(&frame) {
                Ok(cmd) => cmd,
                Err(ProtocolError::UnknownCommand(name)) => {
                    stream
                        .write_all(&encode_error(&format!("ERR unknown command `{name}`")))
                        .await?;
                    continue;
                }
                Err(e) => {
                    stream.write_all(&encode_error(&format!("ERR {e}"))).await?;
                    continue;
                }
            };

            let cmd_name = command_name(&cmd);
            state.metrics.record_command(cmd_name);

            if requires_auth(&cmd) && state.auth_required() && !conn.authenticated {
                stream
                    .write_all(&encode_error("NOAUTH Authentication required."))
                    .await?;
                continue;
            }

            let result = dispatch_command(&mut conn, &state, cmd).await?;
            stream.write_all(&encode_result(&result)).await?;
        }
    }

    Ok(())
}

struct ConnectionContext {
    authenticated: bool,
    txn: TransactionManager,
    pubsub_mode: bool,
    subscriptions: HashSet<String>,
    pubsub_delivery: Option<(
        mpsc::UnboundedSender<(String, String)>,
        mpsc::UnboundedReceiver<(String, String)>,
    )>,
    pubsub_forwarders: HashMap<String, JoinHandle<()>>,
}

impl ConnectionContext {
    fn new() -> Self {
        Self {
            authenticated: false,
            txn: TransactionManager::new(),
            pubsub_mode: false,
            subscriptions: HashSet::new(),
            pubsub_delivery: None,
            pubsub_forwarders: HashMap::new(),
        }
    }

    fn has_subscriptions(&self) -> bool {
        !self.subscriptions.is_empty()
    }
}

fn pubsub_outbox(conn: &mut ConnectionContext) -> mpsc::UnboundedSender<(String, String)> {
    if conn.pubsub_delivery.is_none() {
        let (tx, rx) = mpsc::unbounded_channel();
        conn.pubsub_delivery = Some((tx.clone(), rx));
    }
    conn.pubsub_delivery.as_ref().unwrap().0.clone()
}

fn start_pubsub_forwarder(conn: &mut ConnectionContext, state: &SharedAppState, channel: &str) {
    if conn.subscriptions.contains(channel) {
        return;
    }

    let outbox = pubsub_outbox(conn);
    let mut rx = state.pubsub.subscribe(channel);
    let channel_name = channel.to_string();
    let handle = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(message) => {
                    if outbox.send((channel_name.clone(), message)).is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    conn.pubsub_forwarders.insert(channel.to_string(), handle);
    conn.subscriptions.insert(channel.to_string());
}

fn stop_pubsub_forwarder(conn: &mut ConnectionContext, state: &SharedAppState, channel: &str) {
    if conn.subscriptions.remove(channel) {
        if let Some(handle) = conn.pubsub_forwarders.remove(channel) {
            handle.abort();
        }
        state.pubsub.unsubscribe(channel);
    }
}

fn requires_auth(cmd: &Command) -> bool {
    !matches!(cmd, Command::Auth(_) | Command::Ping)
}

fn command_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Ping => "PING",
        Command::Echo(_) => "ECHO",
        Command::Set { .. } => "SET",
        Command::Get(_) => "GET",
        Command::Del(_) => "DEL",
        Command::Exists(_) => "EXISTS",
        Command::Mget(_) => "MGET",
        Command::Mset(_) => "MSET",
        Command::GetSet { .. } => "GETSET",
        Command::Incr(_) => "INCR",
        Command::Decr(_) => "DECR",
        Command::IncrBy { .. } => "INCRBY",
        Command::DecrBy { .. } => "DECRBY",
        Command::Append { .. } => "APPEND",
        Command::Strlen(_) => "STRLEN",
        Command::Expire { .. } => "EXPIRE",
        Command::Pexpire { .. } => "PEXPIRE",
        Command::Ttl(_) => "TTL",
        Command::Pttl(_) => "PTTL",
        Command::Persist(_) => "PERSIST",
        Command::Lpush { .. } => "LPUSH",
        Command::Rpush { .. } => "RPUSH",
        Command::Lpop(_) => "LPOP",
        Command::Rpop(_) => "RPOP",
        Command::Lrange { .. } => "LRANGE",
        Command::Llen(_) => "LLEN",
        Command::Lindex { .. } => "LINDEX",
        Command::Sadd { .. } => "SADD",
        Command::Srem { .. } => "SREM",
        Command::Sismember { .. } => "SISMEMBER",
        Command::Smembers(_) => "SMEMBERS",
        Command::Scard(_) => "SCARD",
        Command::Sinter(_) => "SINTER",
        Command::Sunion(_) => "SUNION",
        Command::Sdiff(_) => "SDIFF",
        Command::Hset { .. } => "HSET",
        Command::Hget { .. } => "HGET",
        Command::Hdel { .. } => "HDEL",
        Command::Hexists { .. } => "HEXISTS",
        Command::Hgetall(_) => "HGETALL",
        Command::Hkeys(_) => "HKEYS",
        Command::Hvals(_) => "HVALS",
        Command::Hlen(_) => "HLEN",
        Command::Zadd { .. } => "ZADD",
        Command::Zrem { .. } => "ZREM",
        Command::Zscore { .. } => "ZSCORE",
        Command::Zrank { .. } => "ZRANK",
        Command::Zrange { .. } => "ZRANGE",
        Command::Zrevrange { .. } => "ZREVRANGE",
        Command::Zcard(_) => "ZCARD",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Watch(_) => "WATCH",
        Command::Unwatch => "UNWATCH",
        Command::Subscribe(_) => "SUBSCRIBE",
        Command::Unsubscribe(_) => "UNSUBSCRIBE",
        Command::Publish { .. } => "PUBLISH",
        Command::Save => "SAVE",
        Command::Bgsave => "BGSAVE",
        Command::Auth(_) => "AUTH",
        Command::Info(_) => "INFO",
        Command::ReplicaOf { .. } => "REPLICAOF",
        Command::ClusterInfo => "CLUSTER",
        Command::ClusterNodes => "CLUSTER",
    }
}

async fn dispatch_command(
    conn: &mut ConnectionContext,
    state: &SharedAppState,
    cmd: Command,
) -> anyhow::Result<CommandResult> {
    match cmd {
        Command::Auth(password) => match &state.password_hash {
            Some(hash) => match auth::verify_password(&password, hash) {
                Ok(()) => {
                    conn.authenticated = true;
                    Ok(CommandResult::Status("OK".into()))
                }
                Err(_) => Ok(CommandResult::Error("ERR invalid password".into())),
            },
            None => {
                conn.authenticated = true;
                Ok(CommandResult::Status("OK".into()))
            }
        },
        Command::Multi => match conn.txn.multi() {
            Ok(()) => Ok(CommandResult::Status("OK".into())),
            Err(e) => Ok(CommandResult::Error(format!("ERR {e}"))),
        },
        Command::Discard => match conn.txn.discard() {
            Ok(()) => Ok(CommandResult::Status("OK".into())),
            Err(e) => Ok(CommandResult::Error(format!("ERR {e}"))),
        },
        Command::Watch(keys) => {
            let db = state.db.read().await;
            match conn.txn.watch(&keys, &db) {
                Ok(()) => Ok(CommandResult::Status("OK".into())),
                Err(e) => Ok(CommandResult::Error(format!("ERR {e}"))),
            }
        }
        Command::Unwatch => {
            conn.txn.unwatch();
            Ok(CommandResult::Status("OK".into()))
        }
        Command::Exec => {
            let (result, refresh) = {
                let mut db = state.db.write().await;
                match conn.txn.exec(&mut db) {
                    Ok(cmds) => {
                        let mut results = Vec::with_capacity(cmds.len());
                        for queued in cmds {
                            let result = apply_mutating_command(state, &mut db, queued).await?;
                            results.push(result_to_bulk(result));
                        }
                        (CommandResult::Array(results), true)
                    }
                    Err(CoreError::WatchConflict) => (CommandResult::BulkString(None), false),
                    Err(e) => (CommandResult::Error(format!("ERR {e}")), false),
                }
            };
            if refresh {
                state.refresh_memory_metric().await;
            }
            Ok(result)
        }
        Command::Subscribe(channels) => handle_subscribe(conn, state, channels).await,
        Command::Unsubscribe(channels) => handle_unsubscribe(conn, state, channels).await,
        Command::Publish { channel, message } => {
            let receivers = state.pubsub.publish(&channel, &message);
            Ok(CommandResult::Integer(receivers as i64))
        }
        Command::Save => {
            let db = state.db.read().await;
            match state.persistence.save(&db).await {
                Ok(()) => Ok(CommandResult::Status("OK".into())),
                Err(e) => Ok(CommandResult::Error(format!("ERR save failed: {e}"))),
            }
        }
        Command::Bgsave => {
            let persistence = Arc::clone(&state.persistence);
            let db = Arc::clone(&state.db);
            let _handle = persistence.bgsave(db);
            Ok(CommandResult::Status("Background saving started".into()))
        }
        Command::Info(section) => Ok(CommandResult::BulkString(Some(
            build_info(state, section).await,
        ))),
        Command::ReplicaOf { host, port } => {
            state.replication.set_replica_of(host, port);
            Ok(CommandResult::Status("OK".into()))
        }
        Command::ClusterInfo => Ok(CommandResult::BulkString(Some(
            build_cluster_info(state).await,
        ))),
        Command::ClusterNodes => Ok(CommandResult::BulkString(Some(
            build_cluster_nodes(state).await,
        ))),
        other if conn.txn.state() == TransactionState::Multi => match conn.txn.queue(other) {
            Ok(()) => Ok(CommandResult::Status("QUEUED".into())),
            Err(e) => Ok(CommandResult::Error(format!("ERR {e}"))),
        },
        other => {
            if is_write_command(&other) {
                if let Err(e) = state.replication.assert_writable() {
                    return Ok(CommandResult::Error(format!("ERR {e}")));
                }
            }
            let result = {
                let mut db = state.db.write().await;
                apply_mutating_command(state, &mut db, other).await?
            };
            state.refresh_memory_metric().await;
            Ok(result)
        }
    }
}

async fn apply_mutating_command(
    state: &SharedAppState,
    db: &mut tinyredis_core::Database,
    cmd: Command,
) -> anyhow::Result<CommandResult> {
    if is_write_command(&cmd) {
        if let Err(e) = state.replication.assert_writable() {
            return Ok(CommandResult::Error(format!("ERR {e}")));
        }
        if let Err(e) = route_cluster_key(state, &cmd).await {
            return Ok(CommandResult::Error(format!("ERR {e}")));
        }
    }

    let result = execute(db, &cmd);

    if is_write_command(&cmd) && !matches!(result, CommandResult::Error(_)) {
        if let Err(e) = state.persistence.append(&cmd) {
            warn!(error = %e, "AOF append failed");
        }
        state.replication.append_record(cmd);
    }

    Ok(result)
}

async fn route_cluster_key(state: &SharedAppState, cmd: &Command) -> Result<(), CoreError> {
    if !state.config.cluster.enabled {
        return Ok(());
    }
    if let Some(key) = primary_key(cmd) {
        let router = state.cluster.read().await;
        if !router.is_local_key(&key) {
            let node = router.route(&key)?;
            return Err(CoreError::Internal(format!("MOVED to {}", node.address)));
        }
    }
    Ok(())
}

fn primary_key(cmd: &Command) -> Option<String> {
    match cmd {
        Command::Set { key, .. }
        | Command::Get(key)
        | Command::GetSet { key, .. }
        | Command::Incr(key)
        | Command::Decr(key)
        | Command::IncrBy { key, .. }
        | Command::DecrBy { key, .. }
        | Command::Append { key, .. }
        | Command::Strlen(key)
        | Command::Expire { key, .. }
        | Command::Pexpire { key, .. }
        | Command::Ttl(key)
        | Command::Pttl(key)
        | Command::Persist(key)
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpop(key)
        | Command::Rpop(key)
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Sismember { key, .. }
        | Command::Smembers(key)
        | Command::Scard(key)
        | Command::Hset { key, .. }
        | Command::Hget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Hlen(key)
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zrank { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zrevrange { key, .. }
        | Command::Zcard(key) => Some(key.clone()),
        Command::Del(keys) | Command::Exists(keys) | Command::Mget(keys) => keys.first().cloned(),
        Command::Mset(pairs) => pairs.first().map(|(k, _)| k.clone()),
        Command::Sinter(keys) | Command::Sunion(keys) | Command::Sdiff(keys) => {
            keys.first().cloned()
        }
        _ => None,
    }
}

fn is_write_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Set { .. }
            | Command::Del(_)
            | Command::Mset(_)
            | Command::GetSet { .. }
            | Command::Incr(_)
            | Command::Decr(_)
            | Command::IncrBy { .. }
            | Command::DecrBy { .. }
            | Command::Append { .. }
            | Command::Expire { .. }
            | Command::Pexpire { .. }
            | Command::Persist(_)
            | Command::Lpush { .. }
            | Command::Rpush { .. }
            | Command::Lpop(_)
            | Command::Rpop(_)
            | Command::Sadd { .. }
            | Command::Srem { .. }
            | Command::Hset { .. }
            | Command::Hdel { .. }
            | Command::Zadd { .. }
            | Command::Zrem { .. }
    )
}

async fn handle_subscribe(
    conn: &mut ConnectionContext,
    state: &SharedAppState,
    channels: Vec<String>,
) -> anyhow::Result<CommandResult> {
    conn.pubsub_mode = true;
    let targets = if channels.is_empty() {
        conn.subscriptions.iter().cloned().collect()
    } else {
        channels
    };

    let mut response_items = Vec::new();
    for channel in targets {
        start_pubsub_forwarder(conn, state, &channel);
        response_items.push("subscribe".to_string());
        response_items.push(channel);
        response_items.push("1".to_string());
    }
    Ok(CommandResult::Array(response_items))
}

async fn handle_unsubscribe(
    conn: &mut ConnectionContext,
    state: &SharedAppState,
    channels: Vec<String>,
) -> anyhow::Result<CommandResult> {
    let targets: Vec<String> = if channels.is_empty() {
        conn.subscriptions.iter().cloned().collect()
    } else {
        channels
    };

    let mut response_items = Vec::new();
    for channel in targets {
        stop_pubsub_forwarder(conn, state, &channel);
        response_items.push("unsubscribe".to_string());
        response_items.push(channel);
        response_items.push("0".to_string());
    }

    if conn.subscriptions.is_empty() {
        conn.pubsub_mode = false;
    }
    Ok(CommandResult::Array(response_items))
}

async fn handle_pubsub_frame(
    conn: &mut ConnectionContext,
    state: &SharedAppState,
    frame: &RespFrame,
    stream: &mut TcpStream,
) -> anyhow::Result<bool> {
    let cmd = parse_command(frame)?;
    match cmd {
        Command::Subscribe(channels) => {
            let result = handle_subscribe(conn, state, channels).await?;
            stream.write_all(&encode_result(&result)).await?;
        }
        Command::Unsubscribe(channels) => {
            let result = handle_unsubscribe(conn, state, channels).await?;
            stream.write_all(&encode_result(&result)).await?;
        }
        Command::Ping => {
            stream
                .write_all(&encode_result(&CommandResult::Status("PONG".into())))
                .await?;
        }
        _ => {
            stream
                .write_all(&encode_error(
                    "ERR only SUBSCRIBE / UNSUBSCRIBE / PING allowed in pubsub mode",
                ))
                .await?;
        }
    }

    Ok(true)
}

async fn wait_pubsub_message(conn: &mut ConnectionContext) -> Option<(String, String)> {
    conn.pubsub_delivery.as_mut()?.1.recv().await
}

async fn write_pubsub_message(
    stream: &mut TcpStream,
    channel: &str,
    message: &str,
) -> anyhow::Result<()> {
    let frame = RespFrame::Array(vec![
        RespFrame::Bulk(Some(b"message".to_vec())),
        RespFrame::Bulk(Some(channel.as_bytes().to_vec())),
        RespFrame::Bulk(Some(message.as_bytes().to_vec())),
    ]);
    stream.write_all(&encode_frame(&frame)).await?;
    stream.flush().await?;
    Ok(())
}

fn result_to_bulk(result: CommandResult) -> String {
    match result {
        CommandResult::Ok => "OK".into(),
        CommandResult::Status(s) => s,
        CommandResult::Integer(n) => n.to_string(),
        CommandResult::BulkString(Some(s)) => s,
        CommandResult::BulkString(None) => "(nil)".into(),
        CommandResult::BulkStrings(items) => format!("{:?}", items),
        CommandResult::Array(items) => items.join(","),
        CommandResult::Error(e) => format!("ERR {e}"),
    }
}

async fn build_info(state: &SharedAppState, section: Option<String>) -> String {
    let db = state.db.read().await;
    let stats = db.memory_stats();
    let role = match state.replication.role() {
        ReplicationRole::Primary => "master",
        ReplicationRole::Replica => "slave",
    };
    let mut out = format!(
        "# Server\r\ntinyredis_version:0.1.0\r\n\
         # Memory\r\nused_memory:{}\r\n\
         # Stats\r\ndbkeys:{}\r\n\
         # Replication\r\nrole:{}\r\nrepl_offset:{}\r\n",
        stats.used_bytes,
        stats.key_count,
        role,
        state.replication.offset(),
    );
    if let Some(section) = section {
        out.push_str(&format!("# Section\r\nrequested:{section}\r\n"));
    }
    out
}

async fn build_cluster_info(state: &SharedAppState) -> String {
    if !state.config.cluster.enabled {
        return "cluster_enabled:0\r\n".into();
    }
    let cluster = state.cluster.read().await;
    format!(
        "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:{}\r\n",
        cluster.ring().nodes().count().max(1)
    )
}

async fn build_cluster_nodes(state: &SharedAppState) -> String {
    let router = state.cluster.read().await;
    let mut lines = Vec::new();
    for node in router.ring().nodes() {
        lines.push(format!(
            "{} {} - 0 master - 0 0 0 connected",
            node.id, node.address
        ));
    }
    if lines.is_empty() {
        lines.push(format!(
            "{} {}:{}@{} - 0 myself,master - 0 0 0 connected",
            state.config.cluster.node_id, state.config.host, state.config.port, state.config.port
        ));
    }
    lines.join("\n")
}
