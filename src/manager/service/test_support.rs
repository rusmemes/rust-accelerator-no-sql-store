use super::{ManagerService, Node, State};
use crate::common::{Config, Me, NodeId};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) const TEST_PARTITIONS_AMOUNT: usize = 4096;

pub(super) fn node_id(id: &str) -> NodeId {
    NodeId::from_string(id)
}

pub(super) fn me(id: &str) -> Arc<Me> {
    Arc::new(Me {
        id: node_id(id),
        host: "127.0.0.1".to_string(),
        port: 7000,
    })
}

pub(super) fn shared_config(manager_host_port: Option<(String, u16)>) -> Arc<RwLock<Config>> {
    Arc::new(RwLock::new(Config {
        grpc_port: 8080,
        self_host_port: ("127.0.0.1".to_string(), 7000),
        manager_host_port,
        replication_factor: Some(3),
    }))
}

pub(super) fn service(me: Arc<Me>) -> (ManagerService, Arc<RwLock<Config>>) {
    let config = shared_config(Some(("manager.local".to_string(), 9000)));
    (ManagerService::new(me, config.clone()), config)
}

pub(super) fn fresh_node(me: &Me, last_heartbeat: u64) -> Node {
    Node::Manager {
        host: me.host.clone(),
        port: me.port,
        last_heartbeat,
    }
}

pub(super) fn node(host: &str, port: u32, last_heartbeat: u64) -> Node {
    Node::Manager {
        host: host.to_string(),
        port,
        last_heartbeat,
    }
}

pub(super) fn worker_node(host: &str, port: u32, last_heartbeat: u64, masters: Vec<u16>) -> Node {
    Node::Worker {
        host: host.to_string(),
        port,
        last_heartbeat,
        masters,
        replicas: vec![],
    }
}

pub(super) fn masters_for_worker(state: &State, id: &NodeId) -> Vec<u16> {
    match state.nodes.get(id).expect("worker exists") {
        Node::Worker { masters, .. } => masters.clone(),
        _ => panic!("unexpected node type"),
    }
}

pub(super) fn replicas_for_worker(state: &State, id: &NodeId) -> Vec<u16> {
    match state.nodes.get(id).expect("worker exists") {
        Node::Worker { replicas, .. } => replicas.clone(),
        _ => panic!("unexpected node type"),
    }
}
