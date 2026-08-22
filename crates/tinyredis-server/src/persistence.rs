use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tinyredis_core::{execute, Command, Database, ExpiryInfo, Value};
use tokio::fs;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::ServerConfig;

const SNAPSHOT_MAGIC: &str = "TINYREDIS_SNAPSHOT_V1";
const AOF_MAGIC: &str = "TINYREDIS_AOF_V1";

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("corrupt snapshot: {0}")]
    CorruptSnapshot(String),

    #[error("corrupt aof record: {0}")]
    CorruptAof(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotFile {
    magic: String,
    data: std::collections::HashMap<String, (Value, Option<ExpiryInfo>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AofRecord {
    command: Command,
}

pub struct Persistence {
    data_dir: PathBuf,
    snapshot_path: PathBuf,
    aof_path: PathBuf,
    aof_buffer: Mutex<Vec<u8>>,
}

impl Persistence {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            snapshot_path: data_dir.join("snapshot.json"),
            aof_path: data_dir.join("appendonly.aof"),
            data_dir,
            aof_buffer: Mutex::new(Vec::new()),
        }
    }

    pub fn from_config(config: &ServerConfig) -> Self {
        Self::new(config.data_dir.clone())
    }

    pub async fn ensure_data_dir(&self) -> Result<(), PersistenceError> {
        fs::create_dir_all(&self.data_dir).await?;
        Ok(())
    }

    pub async fn load(&self, db: &mut Database) -> Result<LoadReport, PersistenceError> {
        self.ensure_data_dir().await?;
        let mut report = LoadReport::default();

        if self.snapshot_path.exists() {
            match self.load_snapshot(db).await {
                Ok(keys) => {
                    report.snapshot_loaded = true;
                    report.keys_from_snapshot = keys;
                }
                Err(e) => {
                    warn!(error = %e, "snapshot load failed, starting fresh if no AOF");
                    report.snapshot_corrupt = true;
                }
            }
        }

        if self.aof_path.exists() {
            let replay = self.replay_aof(db).await?;
            report.aof_records_replayed = replay.replayed;
            report.aof_records_skipped = replay.skipped;
        }

        Ok(report)
    }

    async fn load_snapshot(&self, db: &mut Database) -> Result<usize, PersistenceError> {
        let bytes = fs::read(&self.snapshot_path).await?;
        let data = parse_snapshot_bytes(&bytes)?;
        let count = data.len();
        db.restore_data(data);
        Ok(count)
    }

    async fn replay_aof(&self, db: &mut Database) -> Result<AofReplayReport, PersistenceError> {
        let bytes = fs::read(&self.aof_path).await?;
        Ok(replay_aof_bytes(db, &bytes))
    }

    pub async fn save(&self, db: &Database) -> Result<(), PersistenceError> {
        self.ensure_data_dir().await?;
        self.write_snapshot(db).await?;
        self.flush_aof().await?;
        Ok(())
    }

    pub fn bgsave(
        self: Arc<Self>,
        db: Arc<RwLock<Database>>,
    ) -> JoinHandle<Result<(), PersistenceError>> {
        tokio::spawn(async move {
            let snapshot_data = {
                let guard = db.read().await;
                guard.snapshot_data()
            };
            self.write_snapshot_data(snapshot_data).await?;
            self.flush_aof().await?;
            info!("background save completed");
            Ok(())
        })
    }

    async fn write_snapshot_data(
        &self,
        data: std::collections::HashMap<String, (Value, Option<ExpiryInfo>)>,
    ) -> Result<(), PersistenceError> {
        let file = SnapshotFile {
            magic: SNAPSHOT_MAGIC.to_string(),
            data,
        };
        self.write_snapshot_file(&file).await
    }

    async fn write_snapshot(&self, db: &Database) -> Result<(), PersistenceError> {
        self.write_snapshot_data(db.snapshot_data()).await
    }

    async fn write_snapshot_file(&self, file: &SnapshotFile) -> Result<(), PersistenceError> {
        let json = serde_json::to_vec_pretty(&file)?;
        let tmp = self.snapshot_path.with_extension("json.tmp");
        fs::write(&tmp, &json).await?;
        fs::rename(&tmp, &self.snapshot_path).await?;
        Ok(())
    }

    pub fn append(&self, command: &Command) -> Result<(), PersistenceError> {
        if !command.is_mutating() {
            return Ok(());
        }
        let record = AofRecord {
            command: command.clone(),
        };
        let line = serde_json::to_string(&record)?;
        let mut buf = self.aof_buffer.lock();
        if buf.is_empty() && !self.aof_path.exists() {
            buf.extend_from_slice(AOF_MAGIC.as_bytes());
            buf.push(b'\n');
        }
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        Ok(())
    }

    async fn flush_aof(&self) -> Result<(), PersistenceError> {
        let data = {
            let mut buf = self.aof_buffer.lock();
            if buf.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buf)
        };
        if !self.aof_path.exists() {
            fs::write(&self.aof_path, &data).await?;
            return Ok(());
        }
        use tokio::io::AsyncWriteExt;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.aof_path)
            .await?;
        file.write_all(&data).await?;
        file.flush().await?;
        Ok(())
    }

    pub async fn truncate_aof(&self) -> Result<(), PersistenceError> {
        {
            let mut buf = self.aof_buffer.lock();
            buf.clear();
        }
        if self.aof_path.exists() {
            fs::remove_file(&self.aof_path).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct LoadReport {
    pub snapshot_loaded: bool,
    pub keys_from_snapshot: usize,
    pub snapshot_corrupt: bool,
    pub aof_records_replayed: usize,
    pub aof_records_skipped: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AofReplayReport {
    pub replayed: usize,
    pub skipped: usize,
}

/// Parse a snapshot JSON blob. Used by recovery and fuzz tests.
pub fn parse_snapshot_bytes(
    bytes: &[u8],
) -> Result<std::collections::HashMap<String, (Value, Option<ExpiryInfo>)>, PersistenceError> {
    if bytes.is_empty() {
        return Err(PersistenceError::CorruptSnapshot("empty file".into()));
    }
    let file: SnapshotFile = serde_json::from_slice(bytes)
        .map_err(|e| PersistenceError::CorruptSnapshot(e.to_string()))?;
    if file.magic != SNAPSHOT_MAGIC {
        return Err(PersistenceError::CorruptSnapshot(format!(
            "bad magic: {}",
            file.magic
        )));
    }
    Ok(file.data)
}

/// Replay AOF bytes into `db`, skipping corrupt lines.
pub fn replay_aof_bytes(db: &mut Database, bytes: &[u8]) -> AofReplayReport {
    let mut replayed = 0usize;
    let mut skipped = 0usize;

    if bytes.is_empty() {
        return AofReplayReport { replayed, skipped };
    }

    let Ok(text) = std::str::from_utf8(bytes) else {
        return AofReplayReport {
            replayed: 0,
            skipped: 1,
        };
    };

    for (line_no, line) in text.lines().enumerate() {
        if line.is_empty() || line == AOF_MAGIC {
            continue;
        }
        match serde_json::from_str::<AofRecord>(line) {
            Ok(record) => {
                execute(db, &record.command);
                replayed += 1;
            }
            Err(e) => {
                warn!(line = line_no + 1, error = %e, "skipping corrupt AOF record");
                skipped += 1;
            }
        }
    }
    AofReplayReport { replayed, skipped }
}

trait CommandExt {
    fn is_mutating(&self) -> bool;
}

impl CommandExt for Command {
    fn is_mutating(&self) -> bool {
        !matches!(
            self,
            Command::Ping
                | Command::Echo(_)
                | Command::Get(_)
                | Command::Exists(_)
                | Command::Mget(_)
                | Command::Ttl(_)
                | Command::Pttl(_)
                | Command::Strlen(_)
                | Command::Llen(_)
                | Command::Lindex { .. }
                | Command::Lrange { .. }
                | Command::Sismember { .. }
                | Command::Smembers(_)
                | Command::Scard(_)
                | Command::Sinter(_)
                | Command::Sunion(_)
                | Command::Sdiff(_)
                | Command::Hget { .. }
                | Command::Hexists { .. }
                | Command::Hgetall(_)
                | Command::Hkeys(_)
                | Command::Hvals(_)
                | Command::Hlen(_)
                | Command::Zscore { .. }
                | Command::Zrank { .. }
                | Command::Zrange { .. }
                | Command::Zrevrange { .. }
                | Command::Zcard(_)
                | Command::Multi
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
                | Command::ClusterNodes
        )
    }
}
