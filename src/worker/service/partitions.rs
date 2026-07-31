use crate::common::{Me, Node, NodeId, PARTITIONS_AMOUNT, PartitionId, Partitions};
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store;
use crate::worker::runtime_store::RuntimeStore;
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
            let node_ids: HashSet<&NodeId> = get_node_ids_curr_node_has_to_sync_the_partition_to(
                partition_id,
                &me.id,
                &state.partitions,
                &state.nodes,
            );
            todo!()
        }
    }
}

fn get_node_ids_curr_node_has_to_sync_the_partition_to<'a>(
    partition: PartitionId,
    me: &NodeId,
    partitions: &'a Partitions,
    cluster_nodes: &HashMap<NodeId, Node>,
) -> HashSet<&'a NodeId> {
    let mut node_ids: HashSet<&'a NodeId> = HashSet::new();

    if partitions
        .old_replicas
        .get(&partition)
        .map(|old| old.contains(me))
        .unwrap_or(false)
    {
        if let Some(mapping) = partitions.mapping.get(&partition) {
            if &mapping.master != me {
                node_ids.insert(&mapping.master);
            }
            node_ids.extend(mapping.replicas.iter().filter(|&node_id| node_id != me));
        }
    } else if partitions
        .old_replicas
        .get(&partition)
        .map(|old| old.is_empty())
        .unwrap_or(true)
        && let Some(mapping) = partitions.mapping.get(&partition)
        && (&mapping.master == me || mapping.replicas.contains(me))
        && let Some(new_replicas) = partitions.new_replicas.get(&partition)
        && !new_replicas.contains(me)
    {
        node_ids.extend(new_replicas);
    }

    node_ids
        .into_iter()
        .filter(|&node_id| cluster_nodes.contains_key(node_id))
        .collect()
}

pub fn handle_sync_batch(
    output: &mut Vec<WorkerProtocol>,
    sync_batch_request: &SyncBatchRequest,
    runtime_store: &RuntimeStore,
) {
    for record in &sync_batch_request.records {
        runtime_store.put(
            record.key,
            runtime_store::Record {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{NodeType, Partition};

    const PARTITION: PartitionId = PartitionId(17);

    fn node_id(value: u128) -> NodeId {
        NodeId::from_string(&uuid::Uuid::from_u128(value).to_string())
    }

    fn worker() -> Node {
        Node {
            host: "worker.local".to_owned(),
            port: 9000,
            last_heartbeat: 0,
            node_type: NodeType::Worker,
        }
    }

    fn cluster_nodes(ids: &[NodeId]) -> HashMap<NodeId, Node> {
        ids.iter().cloned().map(|id| (id, worker())).collect()
    }

    fn mapping(master: NodeId, replicas: &[NodeId]) -> HashMap<PartitionId, Partition> {
        HashMap::from([(
            PARTITION,
            Partition {
                master,
                replicas: replicas.iter().cloned().collect(),
            },
        )])
    }

    fn owned_node_ids(actual: HashSet<&NodeId>) -> HashSet<NodeId> {
        actual.into_iter().cloned().collect()
    }

    #[test]
    fn old_replica_syncs_to_every_available_node_in_the_current_mapping_except_itself() {
        let me = node_id(1);
        let master = node_id(2);
        let replica = node_id(3);
        let disconnected_replica = node_id(4);
        let partitions = Partitions {
            mapping: mapping(
                master.clone(),
                &[me.clone(), replica.clone(), disconnected_replica],
            ),
            old_replicas: HashMap::from([(PARTITION, HashSet::from([me.clone()]))]),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([replica.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), master.clone(), replica.clone()]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
        );

        assert_eq!(owned_node_ids(actual), HashSet::from([master, replica]));
    }

    #[test]
    fn node_does_not_start_second_phase_while_an_old_replica_remains() {
        let me = node_id(1);
        let new_replica = node_id(2);
        let old_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(me.clone(), &[new_replica.clone()]),
            old_replicas: HashMap::from([(PARTITION, HashSet::from([old_replica.clone()]))]),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_replica.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), new_replica, old_replica]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
        );

        assert!(actual.is_empty());
    }

    #[test]
    fn retained_current_node_syncs_to_available_new_replicas_after_old_replicas_are_done() {
        let me = node_id(1);
        let new_replica = node_id(2);
        let disconnected_new_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(
                me.clone(),
                &[new_replica.clone(), disconnected_new_replica.clone()],
            ),
            old_replicas: HashMap::from([(PARTITION, HashSet::new())]),
            new_replicas: HashMap::from([(
                PARTITION,
                HashSet::from([new_replica.clone(), disconnected_new_replica]),
            )]),
        };
        let nodes = cluster_nodes(&[me.clone(), new_replica.clone()]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
        );

        assert_eq!(owned_node_ids(actual), HashSet::from([new_replica]));
    }

    #[test]
    fn absent_old_replicas_entry_also_means_the_first_phase_is_done() {
        let me = node_id(1);
        let new_master = node_id(2);
        let partitions = Partitions {
            mapping: mapping(new_master.clone(), &[me.clone()]),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_master.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), new_master.clone()]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
        );

        assert_eq!(owned_node_ids(actual), HashSet::from([new_master]));
    }

    #[test]
    fn newly_added_node_does_not_relay_the_partition_to_other_new_nodes() {
        let me = node_id(1);
        let retained_master = node_id(2);
        let other_new_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(
                retained_master.clone(),
                &[me.clone(), other_new_replica.clone()],
            ),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(
                PARTITION,
                HashSet::from([me.clone(), other_new_replica.clone()]),
            )]),
        };
        let nodes = cluster_nodes(&[me.clone(), retained_master, other_new_replica]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
        );

        assert!(actual.is_empty());
    }

    #[test]
    fn node_outside_both_old_and_current_mapping_has_no_sync_targets() {
        let me = node_id(1);
        let master = node_id(2);
        let new_replica = node_id(3);
        let partitions = Partitions {
            mapping: mapping(master.clone(), &[new_replica.clone()]),
            old_replicas: HashMap::new(),
            new_replicas: HashMap::from([(PARTITION, HashSet::from([new_replica.clone()]))]),
        };
        let nodes = cluster_nodes(&[me.clone(), master, new_replica]);

        let actual = get_node_ids_curr_node_has_to_sync_the_partition_to(
            PARTITION,
            &me,
            &partitions,
            &nodes,
        );

        assert!(actual.is_empty());
    }
}
