use super::{state, Node, State};
use crate::common::NodeId;
use crate::manager::domain::{
    ClusterNode, ClusterState, NodeProtocol, NodeType, Partition, Partitions,
};
use std::collections::HashMap;

pub(super) fn handle_cluster_state(
    output: &mut Vec<NodeProtocol>,
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
        state.partitions = domain_partitions_to_state(partitions);

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
                            NodeType::Manager => output.push(NodeProtocol::NewConnection {
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

pub(super) fn handle_get_cluster_state(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
) {
    if let Some((epoch, leader_id)) = state.epoch.zip(state.elected_leader_id.clone()) {
        output.push(NodeProtocol::ClusterState {
            recipient_id: id.clone(),
            state: ClusterState {
                epoch,
                leader_id,
                items: state
                    .nodes
                    .iter()
                    .map(
                        |(
                            id,
                            Node {
                                host,
                                port,
                                last_heartbeat,
                                node_type,
                            },
                        )| ClusterNode {
                            id: id.clone(),
                            host: host.clone(),
                            port: *port,
                            last_heartbeat: *last_heartbeat,
                            node_type: *node_type,
                        },
                    )
                    .collect(),
                partitions: Partitions {
                    mapping: state_partition_mapping_to_domain(&state.partitions.mapping),
                    old_replicas: state.partitions.old_replicas.clone(),
                },
            },
        });
    }
}

fn domain_partitions_to_state(partitions: Partitions) -> state::Partitions {
    state::Partitions {
        mapping: domain_partition_mapping_to_state(partitions.mapping),
        old_replicas: partitions.old_replicas,
    }
}

fn domain_partition_mapping_to_state(
    mapping: HashMap<u16, Partition>,
) -> HashMap<u16, state::Partition> {
    mapping
        .into_iter()
        .map(|(partition_id, partition)| {
            (
                partition_id,
                state::Partition {
                    master: partition.master,
                    replicas: partition.replicas,
                },
            )
        })
        .collect()
}

fn state_partition_mapping_to_domain(
    mapping: &HashMap<u16, state::Partition>,
) -> HashMap<u16, Partition> {
    mapping
        .iter()
        .map(|(partition_id, partition)| {
            (
                *partition_id,
                Partition {
                    master: partition.master.clone(),
                    replicas: partition.replicas.clone(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests;
