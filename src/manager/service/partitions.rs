use super::State;
use crate::common::{ClusterState, Me, NodeId, Partition, Partitions};
use crate::manager::domain::NodeProtocol;
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

            move_current_mapping_to_old(state, &vec);

            if !vec.is_empty() {
                calculate_new_mapping(state, PARTITIONS_AMOUNT, replication_factor, &vec);
                deduplicate_partitions(&mut state.partitions);

                let workers_state = create_new_workers_state(state);

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

fn deduplicate_partitions(partitions: &mut Partitions) {
    partitions.old_replicas.retain(|partition, old_replicas| {
        if let Some(new_mapping) = partitions.mapping.get(partition) {
            old_replicas.retain(|replica| {
                replica != &new_mapping.master && !new_mapping.replicas.contains(replica)
            });
        }
        !old_replicas.is_empty()
    });
}

fn create_new_workers_state(state: &mut State) -> ClusterState {
    ClusterState {
        epoch: state
            .epoch
            .expect("present as elected leader id is also present"),
        leader_id: state
            .elected_leader_id
            .clone()
            .expect("existing checked above"),
        items: vec![],
        partitions: Partitions {
            mapping: state
                .partitions
                .mapping
                .iter()
                .map(|(id, partition)| {
                    (
                        *id,
                        Partition {
                            master: partition.master.clone(),
                            replicas: partition.replicas.clone(),
                        },
                    )
                })
                .collect(),
            old_replicas: state.partitions.old_replicas.clone(),
        },
    }
}

fn move_current_mapping_to_old(state: &mut State, current_keys: &[NodeId]) {
    for (partition_id, partition) in state.partitions.mapping.drain() {
        if let Some(old_replicas) = state.partitions.old_replicas.get_mut(&partition_id) {
            old_replicas.extend(partition.replicas);
            old_replicas.insert(partition.master.clone());
        } else {
            let mut replicas = partition.replicas;
            replicas.insert(partition.master);
            state.partitions.old_replicas.insert(partition_id, replicas);
        }
    }

    state.partitions.old_replicas.retain(|_, old_replicas| {
        old_replicas.retain(|node_id| current_keys.contains(node_id));
        !old_replicas.is_empty()
    });
}

fn calculate_new_mapping(
    state: &mut State,
    partitions_amount: usize,
    replication_factor: usize,
    vec: &Vec<NodeId>,
) {
    let mapping = &mut state.partitions.mapping;
    for partition in 0..partitions_amount {
        let master_partition_index = partition % vec.len();
        for replica in 0..replication_factor {
            let index = calc_replica_index(vec.len(), master_partition_index, replica);
            let id = vec.get(index).unwrap();
            if replica == 0 {
                mapping.insert(
                    partition as u16,
                    Partition {
                        master: id.clone(),
                        replicas: HashSet::new(),
                    },
                );
            } else {
                mapping
                    .get_mut(&(partition as u16))
                    .expect("entry is added on replica == 0")
                    .replicas
                    .insert(id.clone());
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
