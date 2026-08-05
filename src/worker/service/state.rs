use crate::common::{Node, NodeId, PartitionId, Partitions};
use crate::worker::runtime_store::Key;
use std::collections::HashMap;

#[derive(Debug)]
pub struct State {
    pub epoch: Option<u64>,
    pub elected_leader_id: Option<NodeId>,
    pub nodes: HashMap<NodeId, Node>,
    pub partitions: Partitions,
    pub sync: HashMap<PartitionId, HashMap<NodeId, SyncState>>
}

#[derive(Debug)]
pub struct SyncState {
    pub prev_max_key: Option<Key>,
    pub curr_max_key: Key,
    pub confirmed: bool,
    pub last_start_time: u64
}
