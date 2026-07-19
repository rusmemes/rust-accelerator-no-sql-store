use crate::common::{Node, NodeId, Partitions};
use std::collections::HashMap;

#[derive(Debug)]
pub(super) struct State {
    pub(super) epoch: Option<u64>,
    pub(super) elected_leader_id: Option<NodeId>,
    pub(super) nodes: HashMap<NodeId, Node>,
    pub(super) partitions: Partitions,
}