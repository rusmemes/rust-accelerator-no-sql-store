use crate::common::NodeId;

/// Messages exchanged by the manager service and its peers.
#[derive(Debug)]
pub enum NodeProtocol {
    /// Announces a new node or requests registration when `id` is `None`.
    NewConnection {
        id: Option<NodeId>,
        host: String,
        port: u32,
        manager: bool,
    },
    /// Forwards a heartbeat for the given node to another recipient.
    Heartbeat {
        recipient_id: NodeId,
        heartbeat: Heartbeat,
    },
    /// Requests the current cluster snapshot from a node.
    GetClusterState {
        id: NodeId,
    },
    /// Carries a cluster snapshot to a recipient.
    ClusterState {
        recipient_id: NodeId,
        state: ClusterState,
    },
    /// Requests a vote for a candidate in a given election epoch.
    VoteRequest {
        id: NodeId,
        epoch: u64,
        ts: u64,
    },
    /// Responds to a vote request.
    VoteResponse {
        id: NodeId,
        leader_id: NodeId,
        ts: u64,
    },
    /// Announces the elected leader for an epoch.
    Leader {
        id: NodeId,
        epoch: u64,
        ts: u64,
    },
    /// Notifies the manager that a node left the cluster.
    NodeDisconnected {
        id: NodeId,
    },
}

/// A node heartbeat payload.
#[derive(Debug)]
pub struct Heartbeat {
    /// Sender node identifier.
    pub id: NodeId,
    /// Timestamp in milliseconds since the Unix epoch.
    pub ts: u64,
}

/// A single node entry in a cluster snapshot.
#[derive(Debug)]
pub enum ClusterNode {
    Manager {
        /// Node identifier.
        id: NodeId,
        /// Node host name or address.
        host: String,
        /// Node port.
        port: u32,
        /// Last known heartbeat timestamp.
        last_heartbeat: u64,
    },
    Worker {
        /// Node identifier.
        id: NodeId,
        /// Node host name or address.
        host: String,
        /// Node port.
        port: u32,
        /// Last known heartbeat timestamp.
        last_heartbeat: u64,
        partitions: Vec<u16>,
    }
}

/// A complete cluster snapshot shared between manager nodes.
#[derive(Debug)]
pub struct ClusterState {
    /// Current election epoch.
    pub epoch: u64,
    /// Current elected leader.
    pub leader_id: NodeId,
    /// Known nodes in the cluster.
    pub items: Vec<ClusterNode>,
}
