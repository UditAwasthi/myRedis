use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};
use crate::expiration::{ExpirationPolicy, ExpiryInfo};
use crate::memory::{EvictionPolicy, MemoryStats, MemoryTracker};
use crate::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    value: Value,
    expiry: Option<ExpiryInfo>,
}

#[derive(Debug, Clone)]
pub struct Database {
    data: HashMap<String, Entry>,
    max_memory: Option<usize>,
    memory: MemoryTracker,
    eviction: EvictionPolicy,
    expiration: ExpirationPolicy,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            max_memory: None,
            memory: MemoryTracker::new(),
            eviction: EvictionPolicy::Lru,
            expiration: ExpirationPolicy::default(),
        }
    }

    pub fn with_limits(max_memory: Option<usize>, eviction: EvictionPolicy) -> Self {
        let mut db = Self::new();
        db.max_memory = max_memory;
        db.eviction = eviction;
        db
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn memory_stats(&self) -> MemoryStats {
        self.memory.stats(self.data.len())
    }

    pub fn set_max_memory(&mut self, max: Option<usize>) {
        self.max_memory = max;
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.data.keys()
    }

    pub fn snapshot_data(&self) -> HashMap<String, (Value, Option<ExpiryInfo>)> {
        self.data
            .iter()
            .map(|(k, e)| (k.clone(), (e.value.clone(), e.expiry.clone())))
            .collect()
    }

    pub fn restore_data(&mut self, data: HashMap<String, (Value, Option<ExpiryInfo>)>) {
        self.data.clear();
        self.memory.reset();
        for (key, (value, expiry)) in data {
            let size = key.len() + value.approximate_size();
            self.memory.record_insert(&key, size);
            self.data.insert(key, Entry { value, expiry });
        }
    }

    fn entry_mut(&mut self, key: &str) -> CoreResult<&mut Entry> {
        if self.is_expired(key) {
            self.delete_key(key);
            return Err(CoreError::KeyNotFound);
        }
        self.data.get_mut(key).ok_or(CoreError::KeyNotFound)
    }

    fn entry(&self, key: &str) -> CoreResult<&Entry> {
        if self.is_expired(key) {
            return Err(CoreError::KeyNotFound);
        }
        self.data.get(key).ok_or(CoreError::KeyNotFound)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.entry(key).is_ok()
    }

    pub fn get(&self, key: &str) -> CoreResult<Option<String>> {
        match self.entry(key) {
            Ok(entry) => match &entry.value {
                Value::String(s) => Ok(Some(s.clone())),
                other => Err(CoreError::WrongType {
                    expected: "string".into(),
                    found: other.type_name().into(),
                }),
            },
            Err(CoreError::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> CoreResult<()> {
        self.set_with_expiry(key, value, None)
    }

    pub fn set_with_expiry(
        &mut self,
        key: String,
        value: String,
        expiry: Option<ExpiryInfo>,
    ) -> CoreResult<()> {
        let size = key.len() + value.len();
        self.ensure_memory(key.len(), size)?;
        let val = Value::String(value);
        if let Some(old) = self.data.insert(key.clone(), Entry { value: val, expiry }) {
            self.memory
                .record_remove(&key, key.len() + old.value.approximate_size());
        }
        self.memory.record_insert(&key, size);
        Ok(())
    }

    pub fn set_nx(&mut self, key: String, value: String) -> CoreResult<bool> {
        if self.exists(&key) {
            return Ok(false);
        }
        self.set(key, value)?;
        Ok(true)
    }

    pub fn get_set(&mut self, key: String, value: String) -> CoreResult<Option<String>> {
        let old = self.get(&key)?;
        self.set(key, value)?;
        Ok(old)
    }

    pub fn del(&mut self, keys: &[String]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.delete_key(key) {
                removed += 1;
            }
        }
        removed
    }

    fn delete_key(&mut self, key: &str) -> bool {
        if let Some(entry) = self.data.remove(key) {
            self.memory
                .record_remove(key, key.len() + entry.value.approximate_size());
            true
        } else {
            false
        }
    }

    pub fn mget(&self, keys: &[String]) -> Vec<Option<String>> {
        keys.iter().map(|k| self.get(k).unwrap_or(None)).collect()
    }

    pub fn mset(&mut self, pairs: &[(String, String)]) -> CoreResult<()> {
        for (k, v) in pairs {
            self.set(k.clone(), v.clone())?;
        }
        Ok(())
    }

    pub fn get_value(&self, key: &str) -> CoreResult<Value> {
        Ok(self.entry(key)?.value.clone())
    }

    pub fn set_value(&mut self, key: String, value: Value) -> CoreResult<()> {
        let size = key.len() + value.approximate_size();
        self.ensure_memory(key.len(), size)?;
        if let Some(old) = self.data.insert(
            key.clone(),
            Entry {
                value,
                expiry: None,
            },
        ) {
            self.memory
                .record_remove(&key, key.len() + old.value.approximate_size());
        }
        self.memory.record_insert(&key, size);
        Ok(())
    }

    pub fn set_expire(&mut self, key: &str, ttl: Duration) -> CoreResult<bool> {
        let entry = self.entry_mut(key)?;
        entry.expiry = Some(ExpiryInfo::from_ttl(ttl));
        Ok(true)
    }

    pub fn set_expire_at_ms(&mut self, key: &str, ttl_ms: u64) -> CoreResult<bool> {
        let entry = self.entry_mut(key)?;
        entry.expiry = Some(ExpiryInfo::from_ttl(Duration::from_millis(ttl_ms)));
        Ok(true)
    }

    pub fn persist(&mut self, key: &str) -> CoreResult<bool> {
        let entry = self.entry_mut(key)?;
        let had = entry.expiry.take().is_some();
        Ok(had)
    }

    pub fn ttl(&self, key: &str) -> CoreResult<i64> {
        if !self.data.contains_key(key) {
            return Err(CoreError::KeyNotFound);
        }
        if self.is_expired(key) {
            return Ok(-2);
        }
        match &self.data[key].expiry {
            Some(info) => Ok(info.remaining_secs()),
            None => Ok(-1),
        }
    }

    pub fn pttl(&self, key: &str) -> CoreResult<i64> {
        if !self.data.contains_key(key) {
            return Err(CoreError::KeyNotFound);
        }
        if self.is_expired(key) {
            return Ok(-2);
        }
        match &self.data[key].expiry {
            Some(info) => Ok(info.remaining_ms()),
            None => Ok(-1),
        }
    }

    pub fn is_expired(&self, key: &str) -> bool {
        self.data
            .get(key)
            .and_then(|e| e.expiry.as_ref())
            .map(|exp| exp.is_expired())
            .unwrap_or(false)
    }

    pub fn expire_passive(&mut self, key: &str) -> bool {
        if self.is_expired(key) {
            self.delete_key(key);
            true
        } else {
            false
        }
    }

    pub fn expire_active_sample(&mut self, sample_size: usize) -> usize {
        let keys: Vec<String> = self
            .data
            .keys()
            .take(sample_size)
            .filter(|k| self.is_expired(k))
            .cloned()
            .collect();
        let count = keys.len();
        for key in keys {
            self.delete_key(&key);
        }
        count
    }

    fn ensure_memory(&mut self, key_len: usize, value_size: usize) -> CoreResult<()> {
        let Some(max) = self.max_memory else {
            return Ok(());
        };
        let needed = key_len + value_size;
        while self.memory.used + needed > max {
            if !self.evict_one()? {
                return Err(CoreError::MemoryLimit);
            }
        }
        Ok(())
    }

    fn evict_one(&mut self) -> CoreResult<bool> {
        let key = self
            .memory
            .select_eviction_key(self.eviction)
            .ok_or(CoreError::MemoryLimit)?;
        self.delete_key(&key);
        Ok(true)
    }

    pub fn wrong_type(expected: &str, found: &str) -> CoreError {
        CoreError::WrongType {
            expected: expected.into(),
            found: found.into(),
        }
    }

    pub fn update_value<R>(
        &mut self,
        key: &str,
        create: Option<Value>,
        f: impl FnOnce(&mut Value) -> CoreResult<R>,
    ) -> CoreResult<R> {
        if !self.data.contains_key(key) {
            if let Some(val) = create {
                self.set_value(key.to_string(), val)?;
            } else {
                return Err(CoreError::KeyNotFound);
            }
        } else if self.is_expired(key) {
            self.delete_key(key);
            if let Some(val) = create {
                self.set_value(key.to_string(), val)?;
            } else {
                return Err(CoreError::KeyNotFound);
            }
        }
        let entry = self.data.get_mut(key).ok_or(CoreError::KeyNotFound)?;
        f(&mut entry.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_del_exists() {
        let mut db = Database::new();
        db.set("name".into(), "Udit".into()).unwrap();
        assert_eq!(db.get("name").unwrap(), Some("Udit".into()));
        assert!(db.exists("name"));
        assert_eq!(db.del(&["name".into()]), 1);
        assert!(!db.exists("name"));
    }

    #[test]
    fn mget_mset() {
        let mut db = Database::new();
        db.mset(&[("a".into(), "1".into()), ("b".into(), "2".into())])
            .unwrap();
        assert_eq!(
            db.mget(&["a".into(), "b".into(), "c".into()]),
            vec![Some("1".into()), Some("2".into()), None]
        );
    }

    #[test]
    fn expiry_ttl() {
        let mut db = Database::new();
        db.set("k".into(), "v".into()).unwrap();
        db.set_expire("k", Duration::from_secs(60)).unwrap();
        assert!(db.ttl("k").unwrap() >= 59);
        assert!(db.persist("k").unwrap());
        assert_eq!(db.ttl("k").unwrap(), -1);
    }
}
