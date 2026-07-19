use crate::common::{ClusterState, Heartbeat, NodeId};

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
    RemoveOldPartition {
        id: NodeId,
        replica_id: NodeId,
        partition_id: u16,
    },
}
