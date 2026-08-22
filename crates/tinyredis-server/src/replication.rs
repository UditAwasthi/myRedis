use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tinyredis_core::{Command, Database};
use tracing::{info, warn};

use crate::config::ReplicationRole;

const REPLICATION_BACKLOG: usize = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationRecord {
    pub offset: u64,
    pub command: Command,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("replica read-only")]
    ReadOnly,

    #[error("not primary")]
    NotPrimary,

    #[error("sync offset {requested} behind backlog start {start}")]
    OffsetTooOld { requested: u64, start: u64 },
}

pub struct ReplicationManager {
    role: RwLock<ReplicationRole>,
    offset: AtomicU64,
    backlog: RwLock<VecDeque<ReplicationRecord>>,
    primary_host: RwLock<Option<String>>,
    primary_port: RwLock<Option<u16>>,
}

impl ReplicationManager {
    pub fn new(role: ReplicationRole) -> Self {
        Self {
            role: RwLock::new(role),
            offset: AtomicU64::new(0),
            backlog: RwLock::new(VecDeque::new()),
            primary_host: RwLock::new(None),
            primary_port: RwLock::new(None),
        }
    }

    pub fn role(&self) -> ReplicationRole {
        *self.role.read()
    }

    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn set_replica_of(&self, host: String, port: u16) {
        *self.primary_host.write() = if host.is_empty() { None } else { Some(host) };
        *self.primary_port.write() = if port == 0 { None } else { Some(port) };
    }

    pub fn primary_addr(&self) -> Option<(String, u16)> {
        match (self.primary_host.read().clone(), *self.primary_port.read()) {
            (Some(host), Some(port)) => Some((host, port)),
            _ => None,
        }
    }

    pub fn append_record(&self, command: Command) -> u64 {
        if self.role() != ReplicationRole::Primary {
            return self.offset();
        }
        let offset = self.offset.fetch_add(1, Ordering::SeqCst) + 1;
        let record = ReplicationRecord { offset, command };
        let mut backlog = self.backlog.write();
        backlog.push_back(record);
        while backlog.len() > REPLICATION_BACKLOG {
            backlog.pop_front();
        }
        offset
    }

    pub fn records_since(
        &self,
        from_offset: u64,
    ) -> Result<Vec<ReplicationRecord>, ReplicationError> {
        let backlog = self.backlog.read();
        if backlog.is_empty() {
            return Ok(Vec::new());
        }
        let first = backlog.front().map(|r| r.offset).unwrap_or(0);
        if from_offset > 0 && from_offset < first.saturating_sub(1) {
            return Err(ReplicationError::OffsetTooOld {
                requested: from_offset,
                start: first,
            });
        }
        Ok(backlog
            .iter()
            .filter(|r| r.offset > from_offset)
            .cloned()
            .collect())
    }

    pub fn apply_records(&self, db: &mut Database, records: &[ReplicationRecord]) -> u64 {
        let mut last = self.offset();
        for record in records {
            tinyredis_core::command::execute(db, &record.command);
            last = record.offset;
        }
        self.offset.store(last, Ordering::SeqCst);
        last
    }

    pub fn promote_to_primary(&self) {
        *self.role.write() = ReplicationRole::Primary;
        info!("promoted to primary");
    }

    pub fn assert_writable(&self) -> Result<(), ReplicationError> {
        if self.role() == ReplicationRole::Replica {
            Err(ReplicationError::ReadOnly)
        } else {
            Ok(())
        }
    }

    pub fn snapshot_for_sync(&self, db: &Database) -> (u64, Vec<ReplicationRecord>) {
        let offset = self.offset();
        let records = self.backlog.read().iter().cloned().collect();
        let _ = db.len();
        (offset, records)
    }
}

pub type SharedReplicationManager = Arc<ReplicationManager>;

/// Parse a replication record from JSON bytes. Returns an error on malformed input.
pub fn parse_replication_record(bytes: &[u8]) -> Result<ReplicationRecord, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub async fn sync_replica_from_primary(
    replication: &ReplicationManager,
    db: &mut Database,
    host: &str,
    port: u16,
) -> Result<(), ReplicationError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr)
        .await
        .map_err(|_| ReplicationError::NotPrimary)?;

    stream
        .write_all(b"*2\r\n$4\r\nPING\r\n$4\r\nSYNC\r\n")
        .await
        .map_err(|_| ReplicationError::NotPrimary)?;

    let mut buf = vec![0u8; 4096];
    let n = stream
        .read(&mut buf)
        .await
        .map_err(|_| ReplicationError::NotPrimary)?;
    if n == 0 {
        warn!(%addr, "primary closed connection during sync");
        return Err(ReplicationError::NotPrimary);
    }

    let _ = replication.apply_records(db, &[]);
    info!(%addr, "replica sync handshake completed");
    Ok(())
}
