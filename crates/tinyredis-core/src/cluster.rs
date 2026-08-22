use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Up,
    Down,
    Joining,
    Leaving,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub state: NodeState,
}

#[derive(Debug, Clone)]
pub struct HashRing {
    vnode_count: usize,
    ring: BTreeMap<u64, String>,
    nodes: HashMap<String, NodeInfo>,
}

impl HashRing {
    pub fn new(vnode_count: usize) -> Self {
        Self {
            vnode_count: vnode_count.max(1),
            ring: BTreeMap::new(),
            nodes: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: NodeInfo) -> CoreResult<()> {
        if self.nodes.contains_key(&node.id) {
            return Err(CoreError::InvalidArgument(format!(
                "node {} already exists",
                node.id
            )));
        }
        for i in 0..self.vnode_count {
            let hash = hash_position(&format!("{}:{}", node.id, i));
            self.ring.insert(hash, node.id.clone());
        }
        self.nodes.insert(node.id.clone(), node);
        Ok(())
    }

    pub fn remove_node(&mut self, node_id: &str) -> CoreResult<()> {
        if self.nodes.remove(node_id).is_none() {
            return Err(CoreError::InvalidArgument(format!(
                "node {node_id} not found"
            )));
        }
        self.ring.retain(|_, id| id != node_id);
        Ok(())
    }

    pub fn locate(&self, key: &str) -> Option<&NodeInfo> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = hash_position(key);
        let node_id = self
            .ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, id)| id)?;
        self.nodes.get(node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }
}

#[derive(Debug, Clone)]
pub struct ClusterRouter {
    ring: HashRing,
    local_node_id: String,
}

impl ClusterRouter {
    pub fn new(local_node_id: String, vnode_count: usize) -> Self {
        Self {
            ring: HashRing::new(vnode_count),
            local_node_id,
        }
    }

    pub fn ring(&self) -> &HashRing {
        &self.ring
    }

    pub fn ring_mut(&mut self) -> &mut HashRing {
        &mut self.ring
    }

    pub fn is_local_key(&self, key: &str) -> bool {
        self.ring
            .locate(key)
            .map(|n| n.id == self.local_node_id)
            .unwrap_or(true)
    }

    pub fn route(&self, key: &str) -> CoreResult<&NodeInfo> {
        self.ring
            .locate(key)
            .ok_or_else(|| CoreError::Internal("empty cluster".into()))
    }
}

pub fn hash_position(input: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistent_routing() {
        let mut ring = HashRing::new(8);
        ring.add_node(NodeInfo {
            id: "a".into(),
            address: "127.0.0.1:7001".into(),
            state: NodeState::Up,
        })
        .unwrap();
        ring.add_node(NodeInfo {
            id: "b".into(),
            address: "127.0.0.1:7002".into(),
            state: NodeState::Up,
        })
        .unwrap();
        let n1 = ring.locate("user:1").unwrap().id.clone();
        let n2 = ring.locate("user:1").unwrap().id.clone();
        assert_eq!(n1, n2);
    }
}
