use crate::common::NodeId;
use std::collections::{BTreeSet, HashMap};

#[derive(Debug)]
pub(super) enum Node {
    Manager {
        host: String,
        port: u32,
        last_heartbeat: u64,
    },
    Worker {
        host: String,
        port: u32,
        last_heartbeat: u64,
        partitions: Vec<u16>,
    },
}

impl Node {
    pub(super) fn is_manager(&self) -> bool {
        matches!(self, Node::Manager { .. })
    }

    pub(super) fn is_worker(&self) -> bool {
        matches!(self, Node::Worker { .. })
    }

    pub(super) fn last_heartbeat_mut(&mut self) -> &mut u64 {
        match self {
            Node::Manager { last_heartbeat, .. } => last_heartbeat,
            Node::Worker { last_heartbeat, .. } => last_heartbeat,
        }
    }
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) epoch: Option<u64>,
    pub(super) elected_leader_id: Option<NodeId>,
    pub(super) nodes: HashMap<NodeId, Node>,
    pub(super) workers_with_calculated_partitions: BTreeSet<NodeId>,
}
