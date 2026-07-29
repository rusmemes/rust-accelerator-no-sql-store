use crate::common::{Node, NodeId, Partitions};
use crate::worker::runtime_store::{Key, PartitionId};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct State {
    pub epoch: Option<u64>,
    pub elected_leader_id: Option<NodeId>,
    pub nodes: HashMap<NodeId, Node>,
    pub partitions: Partitions,
    pub sync: HashMap<PartitionId, HashMap<String, SyncData>>
}

#[derive(Debug)]
pub struct SyncData {
    pub recipient_id_to_state: HashSet<NodeId>,
    pub keys: Vec<Key>,
}
