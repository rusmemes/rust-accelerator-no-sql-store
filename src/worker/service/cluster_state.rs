use crate::common::{ClusterNode, Me, Node, NodeId, NodeType, Partitions};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;

pub(super) fn handle_cluster_state(
    output: &mut Vec<WorkerProtocol>,
    state: &mut State,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    partitions: Partitions,
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
        state.partitions = partitions;

        for item in items {
            match item {
                ClusterNode {
                    id,
                    host,
                    port,
                    last_heartbeat,
                    node_type,
                } => {
                    if let Some(Node {
                        last_heartbeat: node_last_heartbeat,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }
                    } else {
                        match node_type {
                            NodeType::Manager => output.push(WorkerProtocol::NewConnection {
                                id: None,
                                host,
                                port,
                                manager: true,
                            }),
                            NodeType::Worker => {
                                state.nodes.insert(
                                    id,
                                    Node {
                                        host,
                                        port,
                                        last_heartbeat,
                                        node_type,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn handle_remove_old_partition(
    state: &mut State,
    replica_id: NodeId,
    partition_id: u16,
    output: &mut Vec<WorkerProtocol>,
    me: &Me
) {
    let remove = state
        .partitions
        .old_replicas
        .get_mut(&partition_id)
        .map(|old_replicas| old_replicas.remove(&replica_id) && old_replicas.is_empty())
        .unwrap_or(false);

    if remove {
        state.partitions.old_replicas.remove(&partition_id);
    }

    if !state.nodes.get(&replica_id).is_none() {
        output.extend(
            state
                .nodes
                .iter()
                .filter(|(key, node)| *key != &me.id && node.is_manager())
                .map(|(key, _)| WorkerProtocol::GetClusterState { id: key.clone() }),
        );
    }
}
