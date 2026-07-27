use crate::common::{ClusterNode, Me, Node, NodeId, NodeType, Partitions};
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store::RuntimeStore;
use crate::worker::service::state::{State, SyncData};

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
    todo!()
}

pub fn handle_sync_batch_response(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    request_id: String,
    recipient_id: NodeId,
    runtime_store: &RuntimeStore,
    me: &Me
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
                if runtime_store.get_partition_records(*partition, 1).is_empty() {
                    for (node_id, node) in &state.nodes {
                        if node.is_manager() {
                            output.push(WorkerProtocol::RemovePartitionFromReplica {
                                id: node_id.clone(),
                                replica_id: me.id.clone(),
                                partition_id: *partition,
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
