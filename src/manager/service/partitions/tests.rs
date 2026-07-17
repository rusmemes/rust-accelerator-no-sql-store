use super::*;
use crate::common::now_millis;
use crate::manager::domain::NodeProtocol;
use crate::manager::service::state::Partition;
use crate::manager::service::test_support::*;
use crate::manager::service::State;
use std::collections::{HashMap, HashSet};

fn expected_master_for(partition: u16, workers: &[crate::common::NodeId]) -> crate::common::NodeId {
    workers[partition as usize % workers.len()].clone()
}

fn expected_replicas_for(
    partition: u16,
    replication_factor: usize,
    workers: &[crate::common::NodeId],
) -> Vec<crate::common::NodeId> {
    let master_index = partition as usize % workers.len();
    (1..replication_factor)
        .map(|replica| workers[calc_replica_index(workers.len(), master_index, replica)].clone())
        .collect()
}

fn replicas(replicas: Vec<crate::common::NodeId>) -> HashSet<crate::common::NodeId> {
    replicas.into_iter().collect()
}

#[tokio::test]
async fn tick_recomputes_cluster_partition_mapping_and_broadcasts_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let workers = vec![worker_a.clone(), worker_b.clone()];
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
            (worker_a.clone(), worker_node("worker-a.local", 9100, 0)),
            (worker_b.clone(), worker_node("worker-b.local", 9101, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert_eq!(output.len(), 2);
    assert!(output.iter().all(|msg| matches!(
        msg,
        NodeProtocol::ClusterState { recipient_id, state }
            if (recipient_id == &worker_a || recipient_id == &worker_b)
                && state.items.is_empty()
                && state.partitions.len() == TEST_PARTITIONS_AMOUNT
    )));

    let state = service.state.as_ref().expect("state exists");
    assert_eq!(state.partitions.len(), TEST_PARTITIONS_AMOUNT);

    for partition in [0, 1, 2, 4095] {
        let actual = state.partitions.get(&partition).unwrap();
        assert_eq!(actual.master, expected_master_for(partition, &workers));
        assert_eq!(
            actual.replicas,
            replicas(expected_replicas_for(partition, 3, &workers))
        );
    }
}

#[tokio::test]
async fn tick_replaces_partition_mapping_when_worker_layout_changes() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_a = node_id("22222222-2222-2222-2222-222222222222");
    let worker_b = node_id("33333333-3333-3333-3333-333333333333");
    let worker_c = node_id("44444444-4444-4444-4444-444444444444");
    let initial_workers = vec![worker_a.clone(), worker_b.clone()];
    let new_workers = vec![worker_a.clone(), worker_b.clone(), worker_c.clone()];
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
            (worker_a.clone(), worker_node("worker-a.local", 9100, 0)),
            (worker_b.clone(), worker_node("worker-b.local", 9101, 0)),
        ]),
        partitions: Default::default(),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert_eq!(output.len(), 2);
    {
        let state = service.state.as_ref().expect("state exists");
        assert_eq!(state.partitions.len(), TEST_PARTITIONS_AMOUNT);
        for partition in [0, 1, 4095] {
            assert_eq!(
                state.partitions.get(&partition).unwrap().master,
                expected_master_for(partition, &initial_workers)
            );
        }
    }

    service
        .state
        .as_mut()
        .unwrap()
        .nodes
        .insert(worker_c.clone(), worker_node("worker-c.local", 9102, 0));

    output.clear();
    service.tick(&mut output).await;

    assert_eq!(output.len(), 3);
    assert!(output.iter().all(|msg| matches!(
        msg,
        NodeProtocol::ClusterState { recipient_id, state }
            if (recipient_id == &worker_a
                || recipient_id == &worker_b
                || recipient_id == &worker_c)
                && state.partitions.len() == TEST_PARTITIONS_AMOUNT
    )));

    let state = service.state.as_ref().expect("state exists");
    for partition in [0, 1, 2, 4095] {
        assert_eq!(
            state.partitions.get(&partition).unwrap().master,
            expected_master_for(partition, &new_workers)
        );
        assert_eq!(
            state.partitions.get(&partition).unwrap().replicas,
            replicas(expected_replicas_for(partition, 1, &new_workers))
        );
    }
}

#[tokio::test]
async fn tick_recomputes_while_existing_mapping_is_present_and_replaces_mapping() {
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
            (worker_a.clone(), worker_node("worker-a.local", 9100, 0)),
            (worker_b.clone(), worker_node("worker-b.local", 9101, 0)),
            (worker_c.clone(), worker_node("worker-c.local", 9102, 0)),
        ]),
        partitions: HashMap::from([(
            1,
            Partition {
                master: worker_a.clone(),
                replicas: HashSet::new(),
            },
        )]),
        workers_with_calculated_partitions: [worker_a.clone(), worker_b.clone()]
            .into_iter()
            .collect(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert_eq!(output.len(), 3);
    let state = service.state.as_ref().expect("state exists");
    assert_eq!(state.partitions.len(), TEST_PARTITIONS_AMOUNT);
    assert_eq!(
        state.partitions.get(&1).map(|partition| &partition.master),
        Some(&worker_b)
    );
    assert!(state.workers_with_calculated_partitions.contains(&worker_c));
}
