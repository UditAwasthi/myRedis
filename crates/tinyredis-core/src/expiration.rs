use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct ExpirationPolicy {
    pub active_sample_size: usize,
}

impl Default for ExpirationPolicy {
    fn default() -> Self {
        Self {
            active_sample_size: 20,
        }
    }
}

impl ExpirationPolicy {
    pub fn new(active_sample_size: usize) -> Self {
        Self { active_sample_size }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiryInfo {
    expires_at_ms: u64,
}

impl ExpiryInfo {
    pub fn from_ttl(ttl: Duration) -> Self {
        let now = now_ms();
        Self {
            expires_at_ms: now + ttl.as_millis() as u64,
        }
    }

    pub fn at_instant(instant: Instant) -> Self {
        let elapsed = instant.elapsed();
        Self::from_ttl(Duration::from_secs(3600).saturating_sub(elapsed))
    }

    pub fn from_unix_ms(expires_at_ms: u64) -> Self {
        Self { expires_at_ms }
    }

    pub fn is_expired(&self) -> bool {
        now_ms() >= self.expires_at_ms
    }

    pub fn remaining_ms(&self) -> i64 {
        self.expires_at_ms as i64 - now_ms() as i64
    }

    pub fn remaining_secs(&self) -> i64 {
        (self.remaining_ms() + 999) / 1000
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_from_ttl() {
        let info = ExpiryInfo::from_ttl(Duration::from_secs(10));
        assert!(!info.is_expired());
        assert!(info.remaining_secs() <= 10);
    }
}
