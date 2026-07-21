use crate::common::{Node, NodeId};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub(super) struct State {
    pub(super) epoch: Option<u64>,
    pub(super) elected_leader_id: Option<NodeId>,
    pub(super) nodes: HashMap<NodeId, Node>,
    pub(super) master_partitions: HashSet<u16>,
    pub(super) secondary_partitions: HashSet<u16>,
}
