use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
    SortedSet(SortedSet),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortedSet {
    pub scores: BTreeMap<OrderedScore, String>,
    pub members: HashMap<String, OrderedScore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderedScore(i64);

impl OrderedScore {
    pub fn new(score: f64) -> CoreResult<Self> {
        if !score.is_finite() {
            return Err(CoreError::InvalidArgument("score must be finite".into()));
        }
        Ok(Self(score.to_bits() as i64))
    }

    pub fn as_f64(self) -> f64 {
        f64::from_bits(self.0 as u64)
    }
}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            scores: BTreeMap::new(),
            members: HashMap::new(),
        }
    }

    pub fn add(&mut self, member: String, score: f64) -> CoreResult<bool> {
        let ordered = OrderedScore::new(score)?;
        let is_new = !self.members.contains_key(&member);
        if let Some(old) = self.members.insert(member.clone(), ordered) {
            self.scores.remove(&old);
        }
        self.scores.insert(ordered, member);
        Ok(is_new)
    }

    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.scores.remove(&score);
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|s| s.as_f64())
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = self.members.get(member)?;
        self.scores
            .range(..=score)
            .position(|(s, m)| s == score && m == member)
    }

    pub fn range(&self, start: isize, stop: isize) -> Vec<String> {
        let len = self.scores.len() as isize;
        if len == 0 {
            return Vec::new();
        }
        let (s, e) = normalize_range(start, stop, len);
        self.scores
            .values()
            .skip(s as usize)
            .take((e - s + 1) as usize)
            .cloned()
            .collect()
    }

    pub fn rev_range(&self, start: isize, stop: isize) -> Vec<String> {
        let len = self.scores.len() as isize;
        if len == 0 {
            return Vec::new();
        }
        let (s, e) = normalize_range(start, stop, len);
        self.scores
            .values()
            .rev()
            .skip(s as usize)
            .take((e - s + 1) as usize)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::Hash(_) => "hash",
            Value::SortedSet(_) => "sortedset",
        }
    }

    pub fn approximate_size(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::List(items) => items.iter().map(|i| i.len()).sum(),
            Value::Set(items) => items.iter().map(|i| i.len()).sum(),
            Value::Hash(map) => map.iter().map(|(k, v)| k.len() + v.len()).sum(),
            Value::SortedSet(z) => {
                z.members.keys().map(|m| m.len()).sum::<usize>() + z.members.len() * 8
            }
        }
    }
}

pub fn normalize_range(start: isize, stop: isize, len: isize) -> (isize, isize) {
    let start = if start < 0 {
        (len + start).max(0)
    } else {
        start.min(len - 1)
    };
    let stop = if stop < 0 {
        (len + stop).max(0)
    } else {
        stop.min(len - 1)
    };
    if start > stop {
        return (0, -1);
    }
    (start, stop)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_set_add_and_score() {
        let mut z = SortedSet::new();
        assert!(z.add("a".into(), 1.0).unwrap());
        assert!(!z.add("a".into(), 2.0).unwrap());
        assert_eq!(z.score("a"), Some(2.0));
    }

    #[test]
    fn normalize_negative_indices() {
        assert_eq!(normalize_range(-1, -1, 5), (4, 4));
    }
}
