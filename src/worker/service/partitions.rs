use crate::common::{Me, Node, NodeId, PARTITIONS_AMOUNT, Partitions};
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store;
use crate::worker::runtime_store::{PartitionId, RuntimeStore};
use crate::worker::service::state::{State, SyncData};
use std::collections::{HashMap, HashSet};

pub fn sync_partitions(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
) {
    for partition_id in 0..PARTITIONS_AMOUNT {
        let partition_id = PartitionId(partition_id as u16);
        if !runtime_store
            .get_partition_records(partition_id, 1)
            .is_empty()
        {
            let node_ids: HashSet<NodeId> = get_node_ids_curr_node_has_to_sync_the_partition_to(
                partition_id,
                &me.id,
                &state.partitions,
                &state.nodes,
            );
            todo!()
        }
    }
}

fn get_node_ids_curr_node_has_to_sync_the_partition_to(
    partition: PartitionId,
    me: &NodeId,
    partitions: &Partitions,
    cluster_nodes: &HashMap<NodeId, Node>,
) -> HashSet<NodeId> {
    todo!()
}

pub fn handle_sync_batch(
    output: &mut Vec<WorkerProtocol>,
    sync_batch_request: &SyncBatchRequest,
    runtime_store: &RuntimeStore,
) {
    for record in &sync_batch_request.records {
        runtime_store.put(
            record.key,
            runtime_store::Record{
              value: record.value.clone(),
                expiration_time_ms: record.ttl,
                creation_time_ms: record.creation_time_ms,
            },
        );
    }

    output.push(WorkerProtocol::SyncBatchResponse {
        recipient_id: sync_batch_request.sender_id.clone(),
        request_id: sync_batch_request.request_id.clone(),
    });
}

pub fn handle_sync_batch_response(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    request_id: String,
    recipient_id: NodeId,
    runtime_store: &RuntimeStore,
    me: &Me,
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

            // todo: no need to remove synced records always
            if recipient_id_to_state.is_empty() {
                runtime_store.remove_from_partition(*partition, keys);
                if runtime_store
                    .get_partition_records(*partition, 1)
                    .is_empty()
                {
                    for (node_id, node) in &state.nodes {
                        if node.is_manager() {
                            output.push(WorkerProtocol::RemovePartitionFromReplica {
                                id: node_id.clone(),
                                replica_id: me.id.clone(),
                                partition_id: partition.clone(),
                            });
                        }
                    }
                }
            }
        }
        data.retain(|_, sync_data| !sync_data.recipient_id_to_state.is_empty());
    }
    state.sync.retain(|_, data| !data.is_empty());
}
