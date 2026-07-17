use crate::common::NodeId;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Manager,
    Worker,
}

#[derive(Debug)]
pub enum NodeProtocol {
    NewConnection {
        id: Option<NodeId>,
        host: String,
        port: u32,
        manager: bool,
    },
    Heartbeat {
        recipient_id: NodeId,
        heartbeat: Heartbeat,
    },
    GetClusterState {
        id: NodeId,
    },
    ClusterState {
        recipient_id: NodeId,
        state: ClusterState,
    },
    VoteRequest {
        id: NodeId,
        epoch: u64,
        ts: u64,
    },
    VoteResponse {
        id: NodeId,
        leader_id: NodeId,
        ts: u64,
    },
    Leader {
        id: NodeId,
        epoch: u64,
        ts: u64,
    },
    NodeDisconnected {
        id: NodeId,
    },
}

#[derive(Debug)]
pub struct Heartbeat {
    pub id: NodeId,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: NodeId,
    pub host: String,
    pub port: u32,
    pub last_heartbeat: u64,
    pub node_type: NodeType,
}

#[derive(Debug, Clone, Default)]
pub struct Partitions {
    pub mapping: HashMap<u16, Partition>,
    pub old_replicas: HashMap<u16, HashSet<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub master: NodeId,
    pub replicas: HashSet<NodeId>,
}

#[derive(Debug, Clone)]
pub struct ClusterState {
    pub epoch: u64,
    pub leader_id: NodeId,
    pub items: Vec<ClusterNode>,
    pub partitions: Partitions,
}
