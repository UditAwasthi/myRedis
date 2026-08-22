use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tinyredis_core::{ClusterRouter, Database};
use tokio::sync::{RwLock, Semaphore};

use crate::auth;
use crate::config::ServerConfig;
use crate::metrics::Metrics;
use crate::persistence::Persistence;
use crate::pubsub::PubSubHub;
use crate::replication::ReplicationManager;

pub struct AppState {
    pub db: Arc<RwLock<Database>>,
    pub cluster: Arc<RwLock<ClusterRouter>>,
    pub password_hash: Option<String>,
    pub pubsub: Arc<PubSubHub>,
    pub replication: Arc<ReplicationManager>,
    pub metrics: Arc<Metrics>,
    pub persistence: Arc<Persistence>,
    pub config: Arc<ServerConfig>,
    pub connection_limit: Arc<Semaphore>,
    pub connected_clients: AtomicUsize,
}

impl AppState {
    pub fn new(config: ServerConfig, db: Database) -> Result<Self, crate::metrics::MetricsError> {
        let password_hash = match &config.password {
            Some(p) if !p.is_empty() => Some(
                auth::hash_password(p)
                    .map_err(|e| crate::metrics::MetricsError::Registry(e.to_string()))?,
            ),
            _ => None,
        };

        let mut db = db;
        if let Some(max) = config.max_memory {
            db.set_max_memory(Some(max));
        }

        let cluster =
            ClusterRouter::new(config.cluster.node_id.clone(), config.cluster.vnode_count);

        let replication = ReplicationManager::new(config.replication_role);
        if let (Some(host), Some(port)) = (&config.primary_host, config.primary_port) {
            replication.set_replica_of(host.clone(), port);
        }

        let max_connections = config.max_connections;
        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            cluster: Arc::new(RwLock::new(cluster)),
            password_hash,
            pubsub: Arc::new(PubSubHub::new()),
            replication: Arc::new(replication),
            metrics: Arc::new(Metrics::new()?),
            persistence: Arc::new(Persistence::from_config(&config)),
            config: Arc::new(config),
            connection_limit: Arc::new(Semaphore::new(max_connections)),
            connected_clients: AtomicUsize::new(0),
        })
    }

    pub fn auth_required(&self) -> bool {
        self.password_hash.is_some()
    }

    pub fn client_connected(&self) {
        let count = self.connected_clients.fetch_add(1, Ordering::SeqCst) + 1;
        self.metrics.set_connected_clients(count as i64);
    }

    pub fn client_disconnected(&self) {
        let count = self
            .connected_clients
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        self.metrics.set_connected_clients(count as i64);
    }

    pub fn refresh_memory_metric(&self) {
        let stats = self.db.blocking_read().memory_stats();
        self.metrics.set_memory_bytes(stats.used_bytes as i64);
    }
}

pub type SharedAppState = Arc<AppState>;
