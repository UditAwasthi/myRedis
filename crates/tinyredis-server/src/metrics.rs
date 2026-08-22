use prometheus::{
    register_counter_vec, register_int_counter, register_int_gauge, CounterVec, Encoder,
    IntCounter, IntGauge, TextEncoder,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("prometheus registry error: {0}")]
    Registry(String),
}

pub struct Metrics {
    pub commands_total: CounterVec,
    pub connected_clients: IntGauge,
    pub memory_bytes: IntGauge,
    pub expired_keys: IntCounter,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new().expect("metrics registration")
    }
}

impl Metrics {
    pub fn new() -> Result<Self, MetricsError> {
        let commands_total = register_counter_vec!(
            "tinyredis_commands_total",
            "Total number of commands processed",
            &["command"]
        )
        .map_err(|e| MetricsError::Registry(e.to_string()))?;

        let connected_clients = register_int_gauge!(
            "tinyredis_connected_clients",
            "Number of currently connected clients"
        )
        .map_err(|e| MetricsError::Registry(e.to_string()))?;

        let memory_bytes = register_int_gauge!(
            "tinyredis_memory_bytes",
            "Approximate memory used by the database"
        )
        .map_err(|e| MetricsError::Registry(e.to_string()))?;

        let expired_keys = register_int_counter!(
            "tinyredis_expired_keys_total",
            "Total number of keys expired"
        )
        .map_err(|e| MetricsError::Registry(e.to_string()))?;

        Ok(Self {
            commands_total,
            connected_clients,
            memory_bytes,
            expired_keys,
        })
    }

    pub fn record_command(&self, command: &str) {
        self.commands_total.with_label_values(&[command]).inc();
    }

    pub fn set_connected_clients(&self, count: i64) {
        self.connected_clients.set(count);
    }

    pub fn set_memory_bytes(&self, bytes: i64) {
        self.memory_bytes.set(bytes);
    }

    pub fn inc_expired_keys(&self, count: u64) {
        self.expired_keys.inc_by(count);
    }

    pub fn gather_text(&self) -> Result<String, MetricsError> {
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .map_err(|e| MetricsError::Registry(e.to_string()))?;
        String::from_utf8(buffer).map_err(|e| MetricsError::Registry(e.to_string()))
    }
}
