use super::*;
use crate::common::now_millis;
use crate::manager::domain::{ClusterNode, NodeProtocol};
use crate::manager::service::state::Partitions;
use crate::manager::service::test_support::*;
use crate::manager::service::State;
use std::collections::HashMap;

#[tokio::test]
async fn tick_recomputes_worker_partitions_and_broadcasts_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, config) = service(me.clone());
    {
        let mut guard = config.write().await;
        guard.replication_factor = Some(3);
    }
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (
                worker_a.clone(),
                worker_node("worker-a.local", 9100, 0, vec![]),
            ),
            (
                worker_b.clone(),
                worker_node("worker-b.local", 9101, 0, vec![]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert_eq!(output.len(), 2);
    assert!(output.iter().all(|msg| matches!(
        msg,
        NodeProtocol::ClusterState { recipient_id, state }
            if state.config.clone().unwrap().replication_factor == 3
                && (recipient_id == &worker_a || recipient_id == &worker_b)
                && state.items.iter().all(|item| matches!(item, ClusterNode::Worker { .. }))
    )));

    let state = service.state.as_ref().expect("state exists");
    let expected_worker_a_masters = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 0)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_b_masters = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 1)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_a_replicas = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 1)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_b_replicas = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 0)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    assert_eq!(
        masters_for_worker(state, &worker_a),
        expected_worker_a_masters
    );
    assert_eq!(
        masters_for_worker(state, &worker_b),
        expected_worker_b_masters
    );
    assert_eq!(
        replicas_for_worker(state, &worker_a),
        expected_worker_a_replicas
    );
    assert_eq!(
        replicas_for_worker(state, &worker_b),
        expected_worker_b_replicas
    );
    assert!(old_masters_for_worker(state, &worker_a).is_empty());
    assert!(old_masters_for_worker(state, &worker_b).is_empty());
    assert!(old_replicas_for_worker(state, &worker_a).is_empty());
    assert!(old_replicas_for_worker(state, &worker_b).is_empty());
}

#[tokio::test]
async fn tick_keeps_partitions_from_previous_worker_layouts() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let worker_c = node_id("44444444-4444-4444-4444-444444444444");
    let (mut service, config) = service(me.clone());
    {
        let mut guard = config.write().await;
        guard.replication_factor = Some(1);
    }
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (
                worker_a.clone(),
                worker_node("worker-a.local", 9100, 0, vec![]),
            ),
            (
                worker_b.clone(),
                worker_node("worker-b.local", 9101, 0, vec![]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert_eq!(output.len(), 2);
    {
        let state = service.state.as_ref().expect("state exists");
        assert_eq!(
            masters_for_worker(state, &worker_a),
            (0..TEST_PARTITIONS_AMOUNT)
                .filter(|partition| partition % 2 == 0)
                .map(|partition| partition as u16)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            masters_for_worker(state, &worker_b),
            (0..TEST_PARTITIONS_AMOUNT)
                .filter(|partition| partition % 2 == 1)
                .map(|partition| partition as u16)
                .collect::<Vec<_>>()
        );
    }

    service.state.as_mut().unwrap().nodes.insert(
        worker_c.clone(),
        worker_node("worker-c.local", 9102, 0, vec![]),
    );

    output.clear();
    service.tick(&mut output).await;

    assert_eq!(output.len(), 3);
    let expected_worker_a_partitions = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 3 == 0)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_b_partitions = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 3 == 1)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_c_partitions = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 3 == 2)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_a_old_masters = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 0 && partition % 3 != 0)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_b_old_masters = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 1 && partition % 3 != 1)
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    assert!(output.iter().all(|msg| matches!(
        msg,
        NodeProtocol::ClusterState { recipient_id, state }
            if (recipient_id == &worker_a
                || recipient_id == &worker_b
                || recipient_id == &worker_c)
                && state.items.iter().any(|item| matches!(
                    item,
                    ClusterNode::Worker { id, partitions, .. }
                        if id == &worker_a
                            && partitions.masters == expected_worker_a_partitions
                            && partitions.replicas.is_empty()
                            && partitions.old_masters == expected_worker_a_old_masters
                            && partitions.old_replicas.is_empty()
                ))
                && state.items.iter().any(|item| matches!(
                    item,
                    ClusterNode::Worker { id, partitions, .. }
                        if id == &worker_b
                            && partitions.masters == expected_worker_b_partitions
                            && partitions.replicas.is_empty()
                            && partitions.old_masters == expected_worker_b_old_masters
                            && partitions.old_replicas.is_empty()
                ))
                && state.items.iter().any(|item| matches!(
                    item,
                    ClusterNode::Worker { id, partitions, .. }
                        if id == &worker_c
                            && partitions.masters == expected_worker_c_partitions
                            && partitions.replicas.is_empty()
                            && partitions.old_masters.is_empty()
                            && partitions.old_replicas.is_empty()
                ))
    )));

    let state = service.state.as_ref().expect("state exists");
    assert_eq!(
        masters_for_worker(state, &worker_a),
        expected_worker_a_partitions
    );
    assert_eq!(
        masters_for_worker(state, &worker_b),
        expected_worker_b_partitions
    );
    assert_eq!(
        masters_for_worker(state, &worker_c),
        expected_worker_c_partitions
    );
    assert!(replicas_for_worker(state, &worker_a).is_empty());
    assert!(replicas_for_worker(state, &worker_b).is_empty());
    assert!(replicas_for_worker(state, &worker_c).is_empty());
    assert_eq!(
        old_masters_for_worker(state, &worker_a),
        expected_worker_a_old_masters
    );
    assert_eq!(
        old_masters_for_worker(state, &worker_b),
        expected_worker_b_old_masters
    );
    assert!(old_masters_for_worker(state, &worker_c).is_empty());
    assert!(old_replicas_for_worker(state, &worker_a).is_empty());
    assert!(old_replicas_for_worker(state, &worker_b).is_empty());
    assert!(old_replicas_for_worker(state, &worker_c).is_empty());
}

#[test]
fn deduplicate_partitions_prioritizes_new_over_old_and_master_over_replica() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker = node_id("22222222-2222-2222-2222-222222222222");
    let mut state = State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (
                worker.clone(),
                worker_node_with_partitions(
                    "worker.local",
                    9100,
                    0,
                    Partitions {
                        masters: vec![1, 1],
                        replicas: vec![1, 2, 2],
                        old_masters: vec![1, 2, 3, 3],
                        old_replicas: vec![1, 2, 3, 4, 4],
                    },
                ),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    };

    deduplicate_partitions(&mut state);

    assert_eq!(masters_for_worker(&state, &worker), vec![1]);
    assert_eq!(replicas_for_worker(&state, &worker), vec![2]);
    assert_eq!(old_masters_for_worker(&state, &worker), vec![3]);
    assert_eq!(old_replicas_for_worker(&state, &worker), vec![4]);
}
