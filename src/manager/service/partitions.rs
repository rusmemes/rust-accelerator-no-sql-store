use super::State;
use crate::common::{Me, NodeId};
use crate::manager::domain;
use crate::manager::domain::{ClusterState, NodeProtocol};
use crate::manager::service::state::Partition;
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

            state.partitions.clear();
            if !vec.is_empty() {
                calculate_new_mapping(state, PARTITIONS_AMOUNT, replication_factor, &vec);

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
        partitions: state
            .partitions
            .iter()
            .map(|(id, partition)| {
                (
                    *id,
                    domain::Partition {
                        master: partition.master.clone(),
                        replicas: partition.replicas.clone(),
                    },
                )
            })
            .collect(),
    }
}

fn calculate_new_mapping(
    state: &mut State,
    partitions_amount: usize,
    replication_factor: usize,
    vec: &Vec<NodeId>,
) {
    let mapping = &mut state.partitions;
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
