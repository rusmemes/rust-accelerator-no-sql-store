use crate::common::{Node, NodeId, Partitions};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct State {
    pub epoch: Option<u64>,
    pub elected_leader_id: Option<NodeId>,
    pub nodes: HashMap<NodeId, Node>,
    pub partitions: Partitions,
    pub expected_partitions: HashSet<u16>,
    pub sync: HashMap<u16, HashMap<String, SyncData>>
}

#[derive(Debug)]
pub struct SyncData {
    pub recipient_id_to_state: HashSet<NodeId>,
    pub keys: Vec<u64>,
}
