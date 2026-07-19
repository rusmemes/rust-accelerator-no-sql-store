use super::*;
use crate::common::now_millis;
use crate::manager::service::test_support::*;
use crate::manager::service::State;
use std::collections::{HashMap, HashSet};

fn replicas(replicas: Vec<NodeId>) -> HashSet<NodeId> {
    replicas.into_iter().collect()
}

#[test]
fn deduplicate_partitions_removes_new_master_and_secondary_replicas_from_old_replicas() {
    let old_only = node_id("11111111-1111-1111-1111-111111111111");
    let new_master = node_id("22222222-2222-2222-2222-222222222222");
    let new_replica = node_id("33333333-3333-3333-3333-333333333333");
    let other_old = node_id("44444444-4444-4444-4444-444444444444");
    let mut partitions = Partitions {
        mapping: HashMap::from([(
            1,
            Partition {
                master: new_master.clone(),
                replicas: replicas(vec![new_replica.clone()]),
            },
        )]),
        old_replicas: HashMap::from([
            (
                1,
                replicas(vec![
                    old_only.clone(),
                    new_master.clone(),
                    new_replica.clone(),
                ]),
            ),
            (2, replicas(vec![other_old.clone()])),
        ]),
    };

    deduplicate_partitions(&mut partitions);

    assert_eq!(
        partitions.old_replicas.get(&1),
        Some(&replicas(vec![old_only]))
    );
    assert_eq!(
        partitions.old_replicas.get(&2),
        Some(&replicas(vec![other_old]))
    );
}

#[test]
fn deduplicate_partitions_removes_partition_when_all_old_replicas_are_new_replicas() {
    let new_master = node_id("11111111-1111-1111-1111-111111111111");
    let new_replica = node_id("22222222-2222-2222-2222-222222222222");
    let mut partitions = Partitions {
        mapping: HashMap::from([(
            1,
            Partition {
                master: new_master.clone(),
                replicas: replicas(vec![new_replica.clone()]),
            },
        )]),
        old_replicas: HashMap::from([(1, replicas(vec![new_master, new_replica]))]),
    };

    deduplicate_partitions(&mut partitions);

    assert!(!partitions.old_replicas.contains_key(&1));
}

#[test]
fn move_current_mapping_to_old_merges_existing_old_mapping_and_filters_stale_replicas() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let current_master = node_id("22222222-2222-2222-2222-222222222222");
    let current_replica = node_id("33333333-3333-3333-3333-333333333333");
    let old_master = node_id("44444444-4444-4444-4444-444444444444");
    let old_replica = node_id("55555555-5555-5555-5555-555555555555");
    let stale_worker = node_id("66666666-6666-6666-6666-666666666666");
    let mut state = State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (current_master.clone(), worker_node("worker.local", 9100, 0)),
            (
                current_replica.clone(),
                worker_node("replica.local", 9101, 0),
            ),
            (
                old_replica.clone(),
                worker_node("old-replica.local", 9102, 0),
            ),
        ]),
        partitions: Partitions {
            mapping: HashMap::from([(
                1,
                Partition {
                    master: current_master.clone(),
                    replicas: replicas(vec![current_replica.clone(), old_replica.clone()]),
                },
            )]),
            old_replicas: HashMap::from([
                (1, replicas(vec![old_master.clone(), old_replica.clone()])),
                (2, replicas(vec![stale_worker.clone()])),
            ]),
        },
        workers_with_calculated_partitions: Default::default(),
    };

    let current_keys = vec![
        current_master.clone(),
        current_replica.clone(),
        old_replica.clone(),
    ];
    move_current_mapping_to_old(&mut state, &current_keys);

    assert!(state.partitions.mapping.is_empty());
    assert_eq!(
        state.partitions.old_replicas.get(&1),
        Some(&replicas(vec![
            current_master,
            current_replica,
            old_replica
        ]))
    );
    assert!(
        !state
            .partitions
            .old_replicas
            .get(&1)
            .unwrap()
            .contains(&old_master)
    );
    assert!(!state.partitions.old_replicas.contains_key(&2));
    assert!(
        !state
            .partitions
            .old_replicas
            .values()
            .any(|replicas| replicas.contains(&stale_worker))
    );
}
