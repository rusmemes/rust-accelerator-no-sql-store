use super::{state, Node, State};
use crate::common::{Me, NodeId};
use crate::manager::domain::{ClusterNode, ClusterState, NodeProtocol, Partitions};
use std::collections::{BTreeSet, HashSet};

const PARTITIONS_AMOUNT: usize = 4096;

pub(super) fn worker_partitions(
    state: &mut State,
    output: &mut Vec<NodeProtocol>,
    me: &Me,
    replication_factor: usize,
) {
    if state.elected_leader_id.as_ref() == Some(&me.id) {
        let current_keys = state
            .nodes
            .iter()
            .filter(|(_, v)| v.is_worker())
            .map(|(k, _)| k)
            .collect::<BTreeSet<_>>();

        if state.workers_with_calculated_partitions.len() != current_keys.len()
            || current_keys
                != state
                    .workers_with_calculated_partitions
                    .iter()
                    .collect::<BTreeSet<_>>()
        {
            let vec = current_keys
                .into_iter()
                .map(|it| it.clone())
                .collect::<Vec<_>>();

            move_current_partitions_to_old(state);

            if !vec.is_empty() {
                calculate_and_add_partitions(state, PARTITIONS_AMOUNT, replication_factor, &vec);
                deduplicate_partitions(state);

                let workers_state = create_new_workers_state(state, replication_factor);

                state.workers_with_calculated_partitions = vec.into_iter().collect();

                for id in state.nodes.keys().filter(|&key| *key != me.id) {
                    output.push(NodeProtocol::ClusterState {
                        recipient_id: id.clone(),
                        state: workers_state.clone(),
                    });
                }
            } else {
                state.workers_with_calculated_partitions.clear();
            }
        }
    }
}

fn create_new_workers_state(state: &mut State, replication_factor: usize) -> ClusterState {
    let items: Vec<ClusterNode> = state
        .nodes
        .iter()
        .filter_map(|(id, node)| match node {
            Node::Manager { .. } => None,
            Node::Worker {
                host,
                port,
                last_heartbeat,
                partitions,
            } => Some(ClusterNode::Worker {
                id: id.clone(),
                host: host.clone(),
                port: *port,
                last_heartbeat: *last_heartbeat,
                partitions: Partitions {
                    masters: partitions.masters.clone(),
                    old_masters: partitions.old_masters.clone(),
                    replicas: partitions.replicas.clone(),
                    old_replicas: partitions.old_replicas.clone(),
                },
            }),
        })
        .collect();

    ClusterState {
        config: Some(crate::manager::domain::Config { replication_factor }),
        epoch: state
            .epoch
            .expect("present as elected leader id is also present"),
        leader_id: state
            .elected_leader_id
            .clone()
            .expect("existing checked above"),
        items: items.clone(),
    }
}

fn deduplicate_partitions(state: &mut State) {
    let mut seen = HashSet::new();
    state
        .nodes
        .values_mut()
        .filter(|node| node.is_worker())
        .for_each(|node| {
            if let Node::Worker { partitions, .. } = node {
                retain_unseen(&mut partitions.masters, &mut seen);
                retain_unseen(&mut partitions.replicas, &mut seen);
                retain_unseen(&mut partitions.old_masters, &mut seen);
                retain_unseen(&mut partitions.old_replicas, &mut seen);
                seen.clear();
            }
        });
}

fn retain_unseen(partitions: &mut Vec<u16>, seen: &mut HashSet<u16>) {
    partitions.retain(|partition| seen.insert(*partition));
}

fn move_current_partitions_to_old(state: &mut State) {
    state
        .nodes
        .values_mut()
        .filter(|node| node.is_worker())
        .for_each(|node| {
            if let Node::Worker { partitions, .. } = node {
                partitions.old_masters.append(&mut partitions.masters);
                partitions.old_replicas.append(&mut partitions.replicas);
            }
        });
}

fn calculate_and_add_partitions(
    state: &mut State,
    partitions_amount: usize,
    replication_factor: usize,
    vec: &Vec<NodeId>,
) {
    for partition in 0..partitions_amount {
        let master_partition_index = partition % vec.len();
        for replica in 0..replication_factor {
            let index = calc_replica_index(vec.len(), master_partition_index, replica);
            let id = vec.get(index).unwrap();
            let node = state.nodes.get_mut(id).unwrap();
            if let Node::Worker {
                partitions:
                    state::Partitions {
                        masters, replicas, ..
                    },
                ..
            } = node
            {
                if replica == 0 {
                    masters.push(partition as u16);
                } else {
                    replicas.push(partition as u16);
                }
            }
        }
    }
}

fn calc_replica_index(
    total_amount: usize,
    master_partition_index: usize,
    mut replica: usize,
) -> usize {
    while replica >= total_amount {
        replica -= total_amount;
    }
    let mut index = master_partition_index;
    if replica > index {
        replica -= index;
        index = total_amount - replica;
    } else {
        index -= replica;
    }
    index
}

#[cfg(test)]
mod tests;
