use crate::common::{ClusterNode, Me, Node, NodeId, NodeType, Partitions};
use crate::worker::domain;
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store::RuntimeStore;
use crate::worker::service::state::{State, SyncData};
use std::collections::{HashMap, HashSet};
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
    output: &mut Vec<WorkerProtocol>,
    sync_batch_request: &SyncBatchRequest,
    runtime_store: &RuntimeStore,
) {
    for record in &sync_batch_request.records {
        runtime_store.put(
            record.key,
            record.value.clone(),
            record.ttl,
            record.creation_time_ms,
        );
    }

    output.push(WorkerProtocol::SyncBatchResponse {
        recipient_id: sync_batch_request.sender_id.clone(),
        request_id: sync_batch_request.request_id.clone(),
    });
}

pub fn sync_partitions(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
) {
    let unexpected_partitions = runtime_store.unexpected_partitions(&state.expected_partitions);
    for partition_id in unexpected_partitions {
        if !state.sync.contains_key(&partition_id) {
            if let Some(partition) = state.partitions.mapping.get(&partition_id) {
                let mut right_nodes = HashSet::with_capacity(partition.replicas.len() + 1);
                right_nodes.insert(&partition.master);
                right_nodes.extend(partition.replicas.iter());

                const PARTITION_SYNC_BATCH_SIZE: usize = 1000;
                let records = runtime_store.get_partition_records(partition_id, PARTITION_SYNC_BATCH_SIZE);

                let records = records
                    .into_iter()
                    .map(|(key, record)| domain::Record {
                        key,
                        value: record.value.clone(),
                        ttl: record.expiration_time_ms,
                        creation_time_ms: record.creation_time_ms,
                    })
                    .collect::<Vec<_>>();

                let keys = records.iter().map(|record| record.key).collect::<Vec<_>>();

                let request = Arc::new(SyncBatchRequest {
                    sender_id: me.id.clone(),
                    request_id: Uuid::new_v4().to_string(),
                    records,
                });

                let request_id_to_sync_data = state
                    .sync
                    .entry(partition_id)
                    .or_insert_with(|| HashMap::new());

                let recipient_id_to_state = &mut request_id_to_sync_data
                    .entry(request.request_id.clone())
                    .or_insert_with(|| SyncData {
                        keys,
                        recipient_id_to_state: HashSet::new(),
                    })
                    .recipient_id_to_state;

                for recipient_id in right_nodes {
                    output.push(WorkerProtocol::SyncBatch {
                        recipient_id: recipient_id.clone(),
                        request: request.clone(),
                    });
                    recipient_id_to_state.insert(recipient_id.clone());
                }
            }
        }
    }
}

pub fn handle_sync_batch_response(
    state: &mut State,
    request_id: String,
    recipient_id: NodeId,
    runtime_store: &RuntimeStore,
) {
    let sync = &mut state.sync;
    for (partition, data) in sync {
        if let Some(SyncData {
            recipient_id_to_state,
            keys,
        }) = data.get_mut(&request_id)
        {
            recipient_id_to_state.remove(&recipient_id);
            recipient_id_to_state.retain(|node_id| state.nodes.contains_key(node_id));
            if recipient_id_to_state.is_empty() {
                runtime_store.remove_from_partition(*partition, keys);
            }
        }
        data.retain(|_, sync_data| !sync_data.recipient_id_to_state.is_empty());
    }
    state.sync.retain(|_, data| !data.is_empty());
}
