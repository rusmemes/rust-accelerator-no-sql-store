use crate::common::{Me, Node, NodeId, PARTITIONS_AMOUNT, PartitionId, Partitions, now_millis};
use crate::worker::domain::{SyncBatchRequest, WorkerProtocol};
use crate::worker::runtime_store;
use crate::worker::runtime_store::{Key, RuntimeStore};
use crate::worker::service::state::{State, SyncState};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

pub fn handle_sync_batch_response(
    state: &mut State,
    partition_id_to_max_applied_key: HashMap<PartitionId, Key>,
    sender_id: NodeId,
) {
    for (partition_id, max_applied_record_key) in partition_id_to_max_applied_key {
        if let Some(node_id_to_state) = state.sync.get_mut(&partition_id) {
            if let Some(SyncState {
                curr_max_key,
                confirmed,
                ..
            }) = node_id_to_state.get_mut(&sender_id)
            {
                if *curr_max_key == max_applied_record_key {
                    *confirmed = true;
                }
            }
        }
    }
}

pub fn sync_partitions(
    state: &mut State,
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    me: &Me,
) {
    for partition_id in 0..PARTITIONS_AMOUNT {
        let partition_id = PartitionId(partition_id as u16);
        if !runtime_store
            .get_partition_records(&partition_id, 1, None)
            .is_empty()
        {
            let recipient_ids: HashSet<&NodeId> =
                get_node_ids_curr_node_has_to_sync_the_partition_to(
                    partition_id,
                    &me.id,
                    &state.partitions,
                    &state.nodes,
                );

            if let Some(recipient_id_to_last_state) = state.sync.get_mut(&partition_id) {
                for recipient_id in recipient_ids {
                    if !state.nodes.contains_key(recipient_id) {
                        recipient_id_to_last_state.remove(recipient_id);
                    } else if let Some(SyncState {
                        prev_max_key,
                        curr_max_key,
                        confirmed,
                        last_start_time,
                    }) = recipient_id_to_last_state.get_mut(recipient_id)
                    {
                        const SYNC_TIMEOUT_MS: u64 = 60000;
                        if *confirmed {
                            let new_max_key = sync_batch(output, runtime_store, recipient_id, &partition_id, Some(curr_max_key));
                            *prev_max_key = Some(*curr_max_key);
                            *curr_max_key = new_max_key;
                            *confirmed = false;
                            *last_start_time = now_millis();
                        } else if now_millis() - *last_start_time >= SYNC_TIMEOUT_MS {
                            let new_max_key = sync_batch(output, runtime_store, recipient_id, &partition_id, prev_max_key.as_ref());
                            *curr_max_key = new_max_key;
                            *confirmed = false;
                            *last_start_time = now_millis();
                        }
                    } else {
                        let new_max_key = sync_batch(output, runtime_store, recipient_id, &partition_id, None);
                        recipient_id_to_last_state.insert(recipient_id.clone(), SyncState {
                            prev_max_key: None,
                            curr_max_key: new_max_key,
                            confirmed: false,
                            last_start_time: now_millis(),
                        });
                    }
                }
            } else {
                for recipient_id in recipient_ids {
                    let new_max_key = sync_batch(output, runtime_store, recipient_id, &partition_id, None);
                    let recipient_id_to_last_state = state.sync.entry(partition_id.clone()).or_default();
                    recipient_id_to_last_state.insert(recipient_id.clone(), SyncState {
                        prev_max_key: None,
                        curr_max_key: new_max_key,
                        confirmed: false,
                        last_start_time: now_millis(),
                    });
                }
            }
        }
    }
}

fn sync_batch(
    output: &mut Vec<WorkerProtocol>,
    runtime_store: &RuntimeStore,
    recipient: &NodeId,
    partition: &PartitionId,
    after_key: Option<&Key>,
) -> Key { // todo: Option<Key>
    todo!("sync batch")
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
    let mut partition_id_to_max_applied_key = HashMap::new();

    for record in &sync_batch_request.records {
        runtime_store.put(
            record.key,
            runtime_store::Record {
                value: record.value.clone(),
                expiration_time_ms: record.ttl,
                creation_time_ms: record.creation_time_ms,
            },
        );

        match partition_id_to_max_applied_key.entry(record.key.partition()) {
            Entry::Occupied(mut occupied) => {
                if occupied.get() < &record.key {
                    occupied.insert(record.key);
                }
            }
            Entry::Vacant(occupied) => {
                occupied.insert(record.key);
            }
        }
    }

    output.push(WorkerProtocol::SyncBatchResponse {
        recipient_id: sync_batch_request.sender_id.clone(),
        partition_id_to_max_applied_key,
    });
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
