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

#[derive(Debug)]
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
        partitions: Vec<u16>,
    },
}

#[derive(Debug)]
pub struct ClusterState {
    pub epoch: u64,
    pub leader_id: NodeId,
    pub items: Vec<ClusterNode>,
}
