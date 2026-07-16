use crate::common::NodeId;

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
pub enum ClusterNode {
    Manager {
        id: NodeId,
        host: String,
        port: u32,
        last_heartbeat: u64,
    },
    Worker {
        id: NodeId,
        host: String,
        port: u32,
        last_heartbeat: u64,
        masters: Vec<u16>,
        replicas: Vec<u16>,
    },
}

#[derive(Debug, Clone)]
pub struct ClusterState {
    pub epoch: u64,
    pub leader_id: NodeId,
    pub items: Vec<ClusterNode>,
    pub config: Option<Config>
}

#[derive(Debug, Clone)]
pub struct Config {
    pub replication_factor: usize,
}
