use super::*;
use crate::common::now_millis;
use crate::manager::domain::{self, ClusterNode, NodeProtocol};
use crate::manager::service::test_support::*;
use crate::manager::service::{Node, State};
use std::collections::HashMap;

#[tokio::test]
async fn get_cluster_state_returns_current_cluster_snapshot() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, config) = service(me.clone());
    {
        let mut guard = config.write().await;
        guard.replication_factor = Some(4);
    }
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(3),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                peer_id.clone(),
                Node::Manager {
                    host: "peer.local".to_string(),
                    port: 9001,
                    last_heartbeat: 120,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            NodeProtocol::GetClusterState {
                id: peer_id.clone(),
            },
            &mut output,
        )
        .await;

    let cluster_state = match output.as_slice() {
        [
            NodeProtocol::ClusterState {
                recipient_id,
                state,
            },
        ] => {
            assert_eq!(recipient_id, &peer_id);
            state
        }
        other => panic!("unexpected output: {:?}", other),
    };

    assert_eq!(cluster_state.epoch, 3);
    assert_eq!(cluster_state.leader_id, me.id);
    let config = cluster_state.config.clone().unwrap();
    assert_eq!(config.replication_factor, 4);
    assert_eq!(cluster_state.items.len(), 2);
}

#[tokio::test]
async fn get_cluster_state_returns_worker_items_with_partitions() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(3),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                worker_id.clone(),
                worker_node("worker.local", 9100, now - 5, vec![4, 2, 9]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_get_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        worker_id.clone(),
        config.as_ref(),
    )
    .await;

    let cluster_state = match output.as_slice() {
        [
            NodeProtocol::ClusterState {
                recipient_id,
                state,
            },
        ] => {
            assert_eq!(recipient_id, &worker_id);
            state
        }
        other => panic!("unexpected output: {:?}", other),
    };

    let worker_item = cluster_state
        .items
        .iter()
        .find(|item| matches!(item, ClusterNode::Worker { id, .. } if id == &worker_id))
        .expect("worker node present");

    assert!(matches!(
        worker_item,
        ClusterNode::Worker {
            id,
            host,
            port,
            last_heartbeat,
            partitions,
        } if id == &worker_id
            && host == "worker.local"
            && *port == 9100
            && *last_heartbeat == now - 5
            && partitions.masters == vec![4, 2, 9]
            && partitions.replicas.is_empty()
            && partitions.old_masters.is_empty()
            && partitions.old_replicas.is_empty()
    ));
    assert_eq!(cluster_state.config.clone().unwrap().replication_factor, 3);
}

#[tokio::test]
async fn cluster_state_updates_known_nodes_and_requests_unknown_ones() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let unknown = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer.clone(), node("peer.local", 9001, now)),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        2,
        me.id.clone(),
        vec![
            ClusterNode::Manager {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 1,
            },
            ClusterNode::Manager {
                id: unknown.clone(),
                host: "unknown.local".to_string(),
                port: 9002,
                last_heartbeat: now + 2,
            },
        ],
    );

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::NewConnection { id, host, port, manager }]
            if id.is_none() && host == "unknown.local" && *port == 9002 && *manager
    ));

    let state = service.state.as_mut().unwrap();
    assert_eq!(state.epoch, Some(2));
    assert_eq!(state.elected_leader_id, Some(me.id.clone()));
    assert_eq!(
        *state.nodes.get_mut(&me.id).unwrap().last_heartbeat_mut(),
        now + 1
    );
    assert_eq!(
        *state.nodes.get_mut(&peer).unwrap().last_heartbeat_mut(),
        now
    );
}

#[tokio::test]
async fn cluster_state_accepts_new_epoch_same_leader_and_rejects_conflicts() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let other_leader = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: None,
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                leader.clone(),
                Node::Manager {
                    host: "leader.local".to_string(),
                    port: 9001,
                    last_heartbeat: now,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut first = vec![];
    handle_cluster_state(
        &mut first,
        service.state.as_mut().unwrap(),
        4,
        leader.clone(),
        vec![
            ClusterNode::Manager {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now,
            },
            ClusterNode::Manager {
                id: leader.clone(),
                host: "leader.local".to_string(),
                port: 9001,
                last_heartbeat: now,
            },
        ],
    );
    assert!(first.is_empty());
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));
    assert_eq!(
        *service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&me.id)
            .unwrap()
            .last_heartbeat_mut(),
        now
    );
    assert_eq!(
        *service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&leader)
            .unwrap()
            .last_heartbeat_mut(),
        now
    );

    let mut same_epoch_same_leader = vec![];
    handle_cluster_state(
        &mut same_epoch_same_leader,
        service.state.as_mut().unwrap(),
        4,
        leader.clone(),
        vec![ClusterNode::Manager {
            id: me.id.clone(),
            host: me.host.clone(),
            port: me.port,
            last_heartbeat: now + 1,
        }],
    );
    assert!(same_epoch_same_leader.is_empty());
    assert_eq!(
        *service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&me.id)
            .unwrap()
            .last_heartbeat_mut(),
        now + 1
    );

    let mut conflict = vec![];
    handle_cluster_state(
        &mut conflict,
        service.state.as_mut().unwrap(),
        4,
        other_leader,
        vec![ClusterNode::Manager {
            id: me.id.clone(),
            host: me.host.clone(),
            port: me.port,
            last_heartbeat: now + 2,
        }],
    );
    assert!(conflict.is_empty());
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));
    assert_eq!(
        *service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&me.id)
            .unwrap()
            .last_heartbeat_mut(),
        now + 1
    );
}

#[tokio::test]
async fn cluster_state_overwrites_known_worker_partitions_and_adds_unknown_workers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker = node_id("22222222-2222-2222-2222-222222222222");
    let other_worker = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                worker.clone(),
                worker_node("worker.local", 9100, now, vec![1, 2]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        2,
        me.id.clone(),
        vec![
            ClusterNode::Worker {
                id: worker.clone(),
                host: "worker.local".to_string(),
                port: 9100,
                last_heartbeat: now + 10,
                partitions: domain::Partitions {
                    masters: vec![2, 3, 3, 4],
                    replicas: vec![5, 6],
                    old_masters: vec![10],
                    old_replicas: vec![11],
                },
            },
            ClusterNode::Worker {
                id: other_worker.clone(),
                host: "worker-two.local".to_string(),
                port: 9101,
                last_heartbeat: now + 20,
                partitions: domain::Partitions {
                    masters: vec![7, 8],
                    replicas: vec![9],
                    old_masters: vec![12],
                    old_replicas: vec![13],
                },
            },
        ],
    );

    assert!(output.is_empty());
    let state = service.state.as_ref().unwrap();
    assert!(matches!(
        state.nodes.get(&worker),
        Some(Node::Worker {
            host,
            port,
            last_heartbeat,
            partitions,
        }) if host == "worker.local"
            && *port == 9100
            && *last_heartbeat == now + 10
            && partitions.masters == vec![2, 3, 3, 4]
            && partitions.replicas == vec![5, 6]
            && partitions.old_masters == vec![10]
            && partitions.old_replicas == vec![11]
    ));
    assert!(matches!(
        state.nodes.get(&other_worker),
        Some(Node::Worker {
            host,
            port,
            last_heartbeat,
            partitions,
        }) if host == "worker-two.local"
            && *port == 9101
            && *last_heartbeat == now + 20
            && partitions.masters == vec![7, 8]
            && partitions.replicas == vec![9]
            && partitions.old_masters == vec![12]
            && partitions.old_replicas == vec![13]
    ));
}

#[tokio::test]
async fn stale_cluster_state_is_ignored() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(3),
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now)),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });
    {
        let mut guard = config.write().await;
        guard.replication_factor = Some(1);
    }

    let mut output = vec![];
    handle_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        2,
        me.id.clone(),
        vec![ClusterNode::Manager {
            id: me.id.clone(),
            host: me.host.clone(),
            port: me.port,
            last_heartbeat: now + 50,
        }],
    );

    assert!(output.is_empty());
    let state = service.state.as_mut().unwrap();
    assert_eq!(state.epoch, Some(3));
    assert_eq!(state.elected_leader_id, Some(leader));
    assert_eq!(
        *state.nodes.get_mut(&me.id).unwrap().last_heartbeat_mut(),
        now
    );
    let guard = config.read().await;
    assert_eq!(guard.replication_factor, Some(1));
}

#[tokio::test]
async fn get_cluster_state_is_ignored_until_the_cluster_is_known() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let (mut service, config) = service(me.clone());
    service.state = Some(State {
        epoch: None,
        elected_leader_id: None,
        nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_get_cluster_state(
        &mut output,
        service.state.as_mut().unwrap(),
        node_id("22222222-2222-2222-2222-222222222222"),
        config.as_ref(),
    )
    .await;

    assert!(output.is_empty());
}
