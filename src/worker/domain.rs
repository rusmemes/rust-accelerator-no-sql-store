use crate::common::{ClusterState, Heartbeat, NodeId};
use std::sync::Arc;

#[derive(Debug)]
pub enum WorkerProtocol {
    NewConnection {
        id: Option<NodeId>,
        host: String,
        port: u32,
        manager: bool,
    },
    Heartbeat {
        id: NodeId,
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
    RemovePartitionFromReplica {
        id: NodeId,
        replica_id: NodeId,
        partition_id: u16,
    },
    SyncBatch {
        recipient_id: NodeId,
        request: Arc<SyncBatchRequest>,
    }
}

#[derive(Debug, Clone)]
pub struct SyncBatchRequest {
    pub id: String,
    pub records: Vec<Record>
}

#[derive(Debug, Clone)]
pub struct Record {
    pub key: u64,
    pub value: Vec<u8>,
    pub ttl: u64,
    pub creation_time_ms: u64,
}
