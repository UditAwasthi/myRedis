use std::env;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationRole {
    Primary,
    Replica,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    pub enabled: bool,
    pub node_id: String,
    pub vnode_count: usize,
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: "node-1".into(),
            vnode_count: 128,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub max_memory: Option<usize>,
    pub max_connections: usize,
    pub password: Option<String>,
    pub log_level: String,
    pub cluster: ClusterSettings,
    pub replication_role: ReplicationRole,
    pub primary_host: Option<String>,
    pub primary_port: Option<u16>,
    pub connection_timeout: Duration,
    pub health_check: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 6379,
            data_dir: PathBuf::from("./data"),
            max_memory: None,
            max_connections: 10_000,
            password: None,
            log_level: "info".into(),
            cluster: ClusterSettings::default(),
            replication_role: ReplicationRole::Primary,
            primary_host: None,
            primary_port: None,
            connection_timeout: Duration::from_secs(300),
            health_check: true,
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(host) = env::var("TINYREDIS_HOST") {
            cfg.host = host;
        }
        if let Ok(port) = env::var("TINYREDIS_PORT") {
            if let Ok(port) = port.parse() {
                cfg.port = port;
            }
        }
        if let Ok(dir) = env::var("TINYREDIS_DATA_DIR") {
            cfg.data_dir = PathBuf::from(dir);
        }
        if let Ok(max) = env::var("TINYREDIS_MAX_MEMORY") {
            if let Ok(max) = max.parse() {
                cfg.max_memory = Some(max);
            }
        }
        if let Ok(max_conn) = env::var("TINYREDIS_MAX_CONNECTIONS") {
            if let Ok(max_conn) = max_conn.parse() {
                cfg.max_connections = max_conn;
            }
        }
        if let Ok(password) = env::var("TINYREDIS_PASSWORD") {
            if password.is_empty() {
                cfg.password = None;
            } else {
                cfg.password = Some(password);
            }
        }
        if let Ok(level) = env::var("TINYREDIS_LOG_LEVEL") {
            cfg.log_level = level;
        }
        if let Ok(role) = env::var("TINYREDIS_REPLICATION_ROLE") {
            cfg.replication_role = match role.to_ascii_lowercase().as_str() {
                "replica" | "slave" => ReplicationRole::Replica,
                _ => ReplicationRole::Primary,
            };
        }
        if let Ok(enabled) = env::var("TINYREDIS_CLUSTER_ENABLED") {
            cfg.cluster.enabled =
                matches!(enabled.to_ascii_lowercase().as_str(), "1" | "true" | "yes");
        }
        if let Ok(node_id) = env::var("TINYREDIS_CLUSTER_NODE_ID") {
            cfg.cluster.node_id = node_id;
        }
        cfg
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn health_addr(&self) -> String {
        format!("{}:{}", self.host, self.port.saturating_add(1))
    }

    pub fn auth_required(&self) -> bool {
        self.password.as_ref().is_some_and(|p| !p.is_empty())
    }
}
