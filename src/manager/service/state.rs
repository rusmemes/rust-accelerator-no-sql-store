use crate::common::NodeId;
use crate::manager::domain::NodeType;
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug)]
pub(super) struct Node {
    pub host: String,
    pub port: u32,
    pub last_heartbeat: u64,
    pub node_type: NodeType,
}

#[derive(Debug)]
pub(super) struct Partition {
    pub master: NodeId,
    pub replicas: HashSet<NodeId>,
}

impl Node {
    pub(super) fn is_manager(&self) -> bool {
        matches!(self.node_type, NodeType::Manager)
    }

    pub(super) fn is_worker(&self) -> bool {
        matches!(self.node_type, NodeType::Worker)
    }
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) epoch: Option<u64>,
    pub(super) elected_leader_id: Option<NodeId>,
    pub(super) nodes: HashMap<NodeId, Node>,
    pub(super) partitions: HashMap<u16, Partition>,
    pub(super) workers_with_calculated_partitions: BTreeSet<NodeId>,
}
