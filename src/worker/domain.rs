use crate::common::{ClusterState, Heartbeat, NodeId};

#[derive(Debug)]
pub enum WorkerProtocol {
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
    NodeDisconnected {
        id: NodeId,
    },
    Leader {
        id: NodeId,
        epoch: u64,
        ts: u64,
    },
    RemoveOldPartition {
        id: NodeId,
        replica_id: NodeId,
        partition_id: u16,
    },
}
