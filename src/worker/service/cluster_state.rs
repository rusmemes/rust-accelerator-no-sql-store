use crate::common::{ClusterNode, Me, Node, NodeId, NodeType, Partitions};
use crate::worker::domain;
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store::RuntimeStore;
use crate::worker::service::state::State;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

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
        let (mut master, secondary) = calc_partitions(&partitions, me);
        state.partitions = partitions;
        master.extend(secondary);
        state.expected_partitions = master;

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

fn calc_partitions(partitions: &Partitions, me: &Me) -> (HashSet<u16>, HashSet<u16>) {
    let mut master = HashSet::new();
    let mut secondary = HashSet::new();

    for (id, partition) in &partitions.mapping {
        if partition.master == me.id {
            master.insert(*id);
        } else if partition.replicas.contains(&me.id) {
            secondary.insert(*id);
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

pub fn handle_sync_batch(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    sync_batch_request: &SyncBatchRequest,
    runtime_store: &RuntimeStore,
) {
    todo!()
}

pub fn sync_partitions(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
) {
    let unexpected_partitions = runtime_store.unexpected_partitions(&state.expected_partitions);
    for partition_id in unexpected_partitions {
        if let Some(partition) = state.partitions.mapping.get(&partition_id) {
            let mut right_nodes = HashSet::with_capacity(partition.replicas.len() + 1);
            right_nodes.insert(&partition.master);
            right_nodes.extend(partition.replicas.iter());

            let records = runtime_store.get_partition_records(partition_id, 1000);

            let records = records
                .into_iter()
                .map(|(key, record)| domain::Record {
                    key,
                    value: record.value.clone(),
                    ttl: record.expiration_time_ms,
                    creation_time_ms: record.creation_time_ms,
                })
                .collect::<Vec<_>>();

            let request = Arc::new(SyncBatchRequest {
                id: Uuid::new_v4().to_string(),
                records,
            });

            // todo: memorize

            for recipient_id in right_nodes {
                output.push(WorkerProtocol::SyncBatch {
                    recipient_id: recipient_id.clone(),
                    request: request.clone(),
                });
            }
        }
    }
}
