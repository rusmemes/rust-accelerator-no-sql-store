use super::{Node, State};
use crate::common::{Config, NodeId};
use crate::manager::domain::{ClusterNode, ClusterState, NodeProtocol};
use tokio::sync::RwLock;

pub(super) fn handle_cluster_state(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
) {
    let accept: bool = if state.epoch.is_none() || state.epoch < Some(epoch) {
        state.epoch = Some(epoch);
        state.elected_leader_id = Some(leader_id);
        true
    } else if state.epoch == Some(epoch) && state.elected_leader_id == Some(leader_id) {
        true
    } else {
        false
    };

    if accept {
        for item in items {
            match item {
                ClusterNode::Manager {
                    id,
                    host,
                    port,
                    last_heartbeat,
                } => {
                    if let Some(Node::Manager {
                        last_heartbeat: node_last_heartbeat,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }
                    } else {
                        output.push(NodeProtocol::NewConnection {
                            id: None,
                            host,
                            port,
                            manager: true,
                        });
                    }
                }
                ClusterNode::Worker {
                    id,
                    host,
                    port,
                    last_heartbeat,
                    partitions,
                } => {
                    if let Some(Node::Worker {
                        last_heartbeat: node_last_heartbeat,
                        partitions: node_partitions,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }

                        *node_partitions = partitions;
                    } else {
                        state.nodes.insert(
                            id,
                            Node::Worker {
                                host,
                                port,
                                last_heartbeat,
                                partitions,
                            },
                        );
                    }
                }
            }
        }
    }
}

pub(super) async fn handle_get_cluster_state(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    config: &RwLock<Config>,
) {
    if let Some((epoch, leader_id)) = state.epoch.zip(state.elected_leader_id.clone()) {
        let guard = config.read().await;
        let replication_factor = guard.replication_factor.expect("Partitions and replication factor must be set");
        output.push(NodeProtocol::ClusterState {
            recipient_id: id.clone(),
            state: ClusterState {
                config: Some(crate::manager::domain::Config { replication_factor }),
                epoch,
                leader_id,
                items: state
                    .nodes
                    .iter()
                    .map(|(id, node)| match node {
                        Node::Manager {
                            host,
                            port,
                            last_heartbeat,
                        } => ClusterNode::Manager {
                            id: id.clone(),
                            host: host.clone(),
                            port: *port,
                            last_heartbeat: *last_heartbeat,
                        },
                        Node::Worker {
                            host,
                            port,
                            last_heartbeat,
                            partitions,
                        } => ClusterNode::Worker {
                            id: id.clone(),
                            host: host.clone(),
                            port: *port,
                            last_heartbeat: *last_heartbeat,
                            partitions: partitions.clone(),
                        },
                    })
                    .collect(),
            },
        });
    }
}
