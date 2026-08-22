use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EvictionPolicy {
    #[default]
    Lru,
    Lfu,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryTracker {
    pub used: usize,
    lru: VecDeque<String>,
    lfu: HashMap<String, u64>,
    access_order: HashMap<String, Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryStats {
    pub used_bytes: usize,
    pub key_count: usize,
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn stats(&self, key_count: usize) -> MemoryStats {
        MemoryStats {
            used_bytes: self.used,
            key_count,
        }
    }

    pub fn record_insert(&mut self, key: &str, size: usize) {
        self.used = self.used.saturating_add(size);
        self.touch(key);
    }

    pub fn record_remove(&mut self, key: &str, size: usize) {
        self.used = self.used.saturating_sub(size);
        self.lru.retain(|k| k != key);
        self.lfu.remove(key);
        self.access_order.remove(key);
    }

    pub fn touch(&mut self, key: &str) {
        self.lru.retain(|k| k != key);
        self.lru.push_back(key.to_string());
        *self.lfu.entry(key.to_string()).or_insert(0) += 1;
        self.access_order.insert(key.to_string(), Instant::now());
    }

    pub fn select_eviction_key(&self, policy: EvictionPolicy) -> Option<String> {
        match policy {
            EvictionPolicy::Lru => self.lru.front().cloned(),
            EvictionPolicy::Lfu => self
                .lfu
                .iter()
                .min_by_key(|(_, count)| **count)
                .map(|(k, _)| k.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_eviction_order() {
        let mut tracker = MemoryTracker::new();
        tracker.record_insert("a", 10);
        tracker.record_insert("b", 10);
        tracker.touch("a");
        assert_eq!(
            tracker.select_eviction_key(EvictionPolicy::Lru),
            Some("b".into())
        );
    }
}
