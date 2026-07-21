use crate::common::{ClusterNode, Me, Node, NodeId, NodeType, Partitions};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;
use std::collections::HashSet;

pub(super) fn handle_cluster_state(
    output: &mut Vec<WorkerProtocol>,
    state: &mut State,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    partitions: Partitions,
    me: &Me,
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
        let (master, secondary) = calc_partitions(partitions, me);
        state.master_partitions = master;
        state.secondary_partitions = secondary;

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
                        output.push(WorkerProtocol::NewConnection {
                            id: None,
                            host,
                            port,
                            manager: match node_type {
                                NodeType::Manager => true,
                                NodeType::Worker => false,
                            },
                        });
                    }
                }
            }
        }
    }
}

fn calc_partitions(partitions: Partitions, me: &Me) -> (HashSet<u16>, HashSet<u16>) {

    let mut master = HashSet::new();
    let mut secondary = HashSet::new();

    for (id, partition) in partitions.mapping {
        if partition.master == me.id {
            master.insert(id);
        } else if partition.replicas.contains(&me.id) {
            secondary.insert(id);
        }
    }

    (master, secondary)
}

pub(super) fn handle_remove_old_partition(
    state: &mut State,
    replica_id: NodeId,
    output: &mut Vec<WorkerProtocol>,
    me: &Me,
) {
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
