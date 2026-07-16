use super::*;
use crate::common::NodeId;
use crate::manager::domain::ClusterNode;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

const TEST_PARTITIONS_AMOUNT: usize = 4096;

fn node_id(id: &str) -> NodeId {
    NodeId::from_string(id)
}

fn me(id: &str) -> Arc<Me> {
    Arc::new(Me {
        id: node_id(id),
        host: "127.0.0.1".to_string(),
        port: 7000,
    })
}

fn shared_config(manager_host_port: Option<(String, u16)>) -> Arc<RwLock<Config>> {
    Arc::new(RwLock::new(Config {
        grpc_port: 8080,
        self_host_port: ("127.0.0.1".to_string(), 7000),
        manager_host_port,
        replication_factor: Some(3),
    }))
}

fn service(me: Arc<Me>) -> (ManagerService, Arc<RwLock<Config>>) {
    let config = shared_config(Some(("manager.local".to_string(), 9000)));
    (ManagerService::new(me, config.clone()), config)
}

fn fresh_node(me: &Me, last_heartbeat: u64) -> Node {
    Node::Manager {
        host: me.host.clone(),
        port: me.port,
        last_heartbeat,
    }
}

fn node(host: &str, port: u32, last_heartbeat: u64) -> Node {
    Node::Manager {
        host: host.to_string(),
        port,
        last_heartbeat,
    }
}

fn worker_node(host: &str, port: u32, last_heartbeat: u64, masters: Vec<u16>) -> Node {
    Node::Worker {
        host: host.to_string(),
        port,
        last_heartbeat,
        masters,
        replicas: vec![],
    }
}

fn masters_for_worker(state: &State, id: &NodeId) -> Vec<u16> {
    match state.nodes.get(id).expect("worker exists") {
        Node::Worker { masters, .. } => masters.clone(),
        _ => panic!("unexpected node type"),
    }
}

fn replicas_for_worker(state: &State, id: &NodeId) -> Vec<u16> {
    match state.nodes.get(id).expect("worker exists") {
        Node::Worker { replicas, .. } => replicas.clone(),
        _ => panic!("unexpected node type"),
    }
}

#[tokio::test]
async fn init_with_manager_connection_requests_connection_and_sets_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let (mut service, _config) = service(me.clone());

    let output = service.get_init_messages().await;

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::NewConnection {
            id: _,
            host,
            port,
            manager: true,
        }] if host == "manager.local" && *port == 9000
    ));

    let state = service.state.as_ref().expect("state initialized");
    assert_eq!(state.epoch, None);
    assert_eq!(state.elected_leader_id, None);
    assert!(state.nodes.contains_key(&me.id));
}

#[tokio::test]
async fn init_without_manager_starts_as_epoch_zero() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let (mut service, config) = service(me.clone());
    config.write().await.manager_host_port = None;

    let output = service.get_init_messages().await;

    assert!(output.is_empty());

    let state = service.state.as_ref().expect("state initialized");
    assert_eq!(state.epoch, Some(0));
    assert_eq!(state.elected_leader_id, None);
    assert_eq!(state.nodes.len(), 1);
}

#[tokio::test]
async fn new_connection_adds_node_and_requests_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: None,
        elected_leader_id: None,
        nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            NodeProtocol::NewConnection {
                id: Some(peer_id.clone()),
                host: "peer.local".to_string(),
                port: 9001,
                manager: true,
            },
            &mut output,
        )
        .await;

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::GetClusterState { id }] if id == &peer_id
    ));

    let state = service.state.as_ref().expect("state exists");
    assert!(state.nodes.contains_key(&peer_id));
}

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
            masters,
            replicas,
        } if id == &worker_id
            && host == "worker.local"
            && *port == 9100
            && *last_heartbeat == now - 5
            && masters == &vec![4, 2, 9]
            && replicas.is_empty()
    ));
    assert_eq!(cluster_state.config.clone().unwrap().replication_factor, 3);
}

#[tokio::test]
async fn heartbeat_from_unknown_node_requests_cluster_state_from_peers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_id.clone(), node("peer.local", 9001, 0)),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            NodeProtocol::Heartbeat {
                recipient_id: me.id.clone(),
                heartbeat: Heartbeat {
                    id: node_id("33333333-3333-3333-3333-333333333333"),
                    ts: 42,
                },
            },
            &mut output,
        )
        .await;

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::GetClusterState { id }] if id == &peer_id
    ));
}

#[tokio::test]
async fn heartbeat_from_known_peer_is_forwarded_only_when_we_are_leader() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_one = node_id("22222222-2222-2222-2222-222222222222");
    let peer_two = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (peer_one.clone(), node("peer-one.local", 9001, 0)),
            (peer_two.clone(), node("peer-two.local", 9002, 0)),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut forwarded = vec![];
    handle_heartbeat(
        &mut forwarded,
        service.state.as_mut().unwrap(),
        peer_one.clone(),
        42,
        &me,
    );

    assert!(matches!(
        forwarded.as_slice(),
        [NodeProtocol::Heartbeat { recipient_id, heartbeat }]
            if recipient_id == &peer_two
                && heartbeat.id == peer_one
                && heartbeat.ts == 42
    ));

    service.state.as_mut().unwrap().elected_leader_id = Some(peer_two.clone());

    let mut not_forwarded = vec![];
    handle_heartbeat(
        &mut not_forwarded,
        service.state.as_mut().unwrap(),
        peer_one.clone(),
        43,
        &me,
    );

    assert!(not_forwarded.is_empty());
    assert_eq!(
        *service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&peer_one)
            .unwrap()
            .last_heartbeat_mut(),
        43
    );
}

#[tokio::test]
async fn heartbeat_from_worker_updates_worker_without_forwarding() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker = node_id("22222222-2222-2222-2222-222222222222");
    let manager_peer = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (
                worker.clone(),
                worker_node("worker.local", 9100, 12, vec![1, 2]),
            ),
            (manager_peer.clone(), node("manager.local", 9001, 0)),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_heartbeat(
        &mut output,
        service.state.as_mut().unwrap(),
        worker.clone(),
        44,
        &me,
    );

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::Heartbeat { recipient_id, heartbeat }]
            if recipient_id == &manager_peer && heartbeat.id == worker && heartbeat.ts == 44
    ));
    assert_eq!(
        *service
            .state
            .as_mut()
            .unwrap()
            .nodes
            .get_mut(&worker)
            .unwrap()
            .last_heartbeat_mut(),
        44
    );
}

#[tokio::test]
async fn vote_request_adds_new_election_reuses_candidate_and_ignores_stale_epochs() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_one = node_id("22222222-2222-2222-2222-222222222222");
    let peer_two = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(0),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                peer_one.clone(),
                Node::Manager {
                    host: "peer-one.local".to_string(),
                    port: 9001,
                    last_heartbeat: now,
                },
            ),
            (
                peer_two.clone(),
                Node::Manager {
                    host: "peer-two.local".to_string(),
                    port: 9002,
                    last_heartbeat: now,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut first = vec![];
    service
        .process(
            NodeProtocol::VoteRequest {
                id: peer_one.clone(),
                epoch: 1,
                ts: 100,
            },
            &mut first,
        )
        .await;
    assert!(matches!(
        first.as_slice(),
        [NodeProtocol::VoteResponse { id, leader_id, ts }]
            if id == &peer_one && leader_id == &peer_one && *ts == 100
    ));

    let mut second = vec![];
    service
        .process(
            NodeProtocol::VoteRequest {
                id: peer_two.clone(),
                epoch: 1,
                ts: 200,
            },
            &mut second,
        )
        .await;

    assert!(matches!(
        second.as_slice(),
        [NodeProtocol::VoteResponse { id, leader_id, ts }]
            if id == &peer_two && leader_id == &peer_one && *ts == 100
    ));

    let mut stale = vec![];
    service
        .process(
            NodeProtocol::VoteRequest {
                id: peer_two.clone(),
                epoch: 0,
                ts: 300,
            },
            &mut stale,
        )
        .await;
    assert!(stale.is_empty());
}

#[tokio::test]
async fn vote_response_for_unknown_leader_requests_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let unknown_leader = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                peer.clone(),
                Node::Manager {
                    host: "peer.local".to_string(),
                    port: 9001,
                    last_heartbeat: now,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        1,
        Election::Mine {
            ts: 100,
            approvers: HashSet::new(),
        },
    );

    let mut output = vec![];
    handle_vote_response(
        &mut output,
        service.state.as_mut().unwrap(),
        peer.clone(),
        unknown_leader,
        100,
        &me,
        &mut service.elections,
    );

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::GetClusterState { id }] if id == &peer
    ));
}

#[tokio::test]
async fn vote_responses_elect_self_and_broadcast_leader() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_one = node_id("22222222-2222-2222-2222-222222222222");
    let peer_two = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(0),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                peer_one.clone(),
                Node::Manager {
                    host: "peer-one.local".to_string(),
                    port: 9001,
                    last_heartbeat: 0,
                },
            ),
            (
                peer_two.clone(),
                Node::Manager {
                    host: "peer-two.local".to_string(),
                    port: 9002,
                    last_heartbeat: 0,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        1,
        Election::Mine {
            ts: 100,
            approvers: HashSet::new(),
        },
    );

    let mut first = vec![];
    handle_vote_response(
        &mut first,
        service.state.as_mut().unwrap(),
        peer_one.clone(),
        me.id.clone(),
        100,
        &me,
        &mut service.elections,
    );
    assert!(first.is_empty());

    let mut second = vec![];
    handle_vote_response(
        &mut second,
        service.state.as_mut().unwrap(),
        peer_two.clone(),
        me.id.clone(),
        100,
        &me,
        &mut service.elections,
    );

    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(me.id.clone())
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(1));
    assert!(matches!(
        second.as_slice(),
        [
            NodeProtocol::Leader { id, epoch, ts },
            NodeProtocol::Leader { id: id2, epoch: epoch2, ts: ts2 }
        ] if *epoch == 1
            && *ts == 100
            && *epoch2 == 1
            && *ts2 == 100
            && ((id == &peer_one && id2 == &peer_two) || (id == &peer_two && id2 == &peer_one))
    ));
}

#[tokio::test]
async fn vote_responses_complete_election_with_worker_nodes_present() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
    let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(0),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                manager_peer.clone(),
                Node::Manager {
                    host: "manager.local".to_string(),
                    port: 9001,
                    last_heartbeat: 0,
                },
            ),
            (
                worker_peer.clone(),
                worker_node("worker.local", 9100, 0, vec![1, 2]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        1,
        Election::Mine {
            ts: 100,
            approvers: HashSet::new(),
        },
    );

    let mut output = vec![];
    handle_vote_response(
        &mut output,
        service.state.as_mut().unwrap(),
        manager_peer.clone(),
        me.id.clone(),
        100,
        &me,
        &mut service.elections,
    );

    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(me.id.clone())
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(1));
    assert_eq!(output.len(), 2);
    assert!(
        output
            .iter()
            .any(|msg| matches!(msg, NodeProtocol::Leader { id, .. } if id == &manager_peer))
    );
    assert!(
        output
            .iter()
            .any(|msg| matches!(msg, NodeProtocol::Leader { id, .. } if id == &worker_peer))
    );
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
                masters: vec![2, 3, 3, 4],
                replicas: vec![5, 6],
            },
            ClusterNode::Worker {
                id: other_worker.clone(),
                host: "worker-two.local".to_string(),
                port: 9101,
                last_heartbeat: now + 20,
                masters: vec![7, 8],
                replicas: vec![9],
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
            masters,
            replicas,
        }) if host == "worker.local"
            && *port == 9100
            && *last_heartbeat == now + 10
            && masters == &vec![2, 3, 3, 4]
            && replicas == &vec![5, 6]
    ));
    assert!(matches!(
        state.nodes.get(&other_worker),
        Some(Node::Worker {
            host,
            port,
            last_heartbeat,
            masters,
            replicas,
        }) if host == "worker-two.local"
            && *port == 9101
            && *last_heartbeat == now + 20
            && masters == &vec![7, 8]
            && replicas == &vec![9]
    ));
}

#[tokio::test]
async fn new_connection_while_we_are_leader_does_not_request_cluster_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                peer.clone(),
                Node::Manager {
                    host: "peer.local".to_string(),
                    port: 9001,
                    last_heartbeat: now,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_new_connection(
        &mut output,
        service.state.as_mut().unwrap(),
        Some(node_id("33333333-3333-3333-3333-333333333333")),
        "third.local".to_string(),
        9002,
        &me,
        true,
    );

    assert!(output.is_empty());
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
async fn leader_message_updates_epoch_and_clears_pending_elections() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis();
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (leader.clone(), node("leader.local", 9001, now)),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });
    service.elections.insert(
        2,
        Election::Mine {
            ts: now,
            approvers: HashSet::from([leader.clone()]),
        },
    );

    handle_leader(
        &mut Vec::new(),
        service.state.as_mut().unwrap(),
        leader.clone(),
        2,
        now + 10,
        &me,
        &mut service.elections,
    );

    let state = service.state.as_mut().unwrap();
    assert_eq!(state.epoch, Some(2));
    assert_eq!(state.elected_leader_id, Some(leader.clone()));
    assert_eq!(
        *state.nodes.get_mut(&leader).unwrap().last_heartbeat_mut(),
        now + 10
    );
    assert!(service.elections.is_empty());
}

#[tokio::test]
async fn leader_with_unknown_id_requests_cluster_state_from_manager_peers_only() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
    let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
    let leader = node_id("44444444-4444-4444-4444-444444444444");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (manager_peer.clone(), node("manager.local", 9001, now)),
            (
                worker_peer.clone(),
                worker_node("worker.local", 9100, now, vec![1]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    handle_leader(
        &mut output,
        service.state.as_mut().unwrap(),
        leader,
        2,
        now + 10,
        &me,
        &mut service.elections,
    );

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::GetClusterState { id }] if id == &manager_peer
    ));
}

#[tokio::test]
async fn leader_messages_only_move_state_forward() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
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

    handle_leader(
        &mut vec![],
        service.state.as_mut().unwrap(),
        leader.clone(),
        2,
        now,
        &me,
        &mut service.elections,
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(2));
    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(leader.clone())
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

    handle_leader(
        &mut vec![],
        service.state.as_mut().unwrap(),
        me.id.clone(),
        1,
        88,
        &me,
        &mut service.elections,
    );
    assert_eq!(service.state.as_ref().unwrap().epoch, Some(2));
    assert_eq!(
        service.state.as_ref().unwrap().elected_leader_id,
        Some(leader)
    );
}

#[tokio::test]
async fn tick_emits_heartbeats_for_stale_self_heartbeat() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer_id = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    let now = now_millis() - 1_000;
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(me.id.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                peer_id.clone(),
                Node::Manager {
                    host: "peer.local".to_string(),
                    port: 9001,
                    last_heartbeat: 0,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::Heartbeat { recipient_id, heartbeat }] if recipient_id == &peer_id
            && heartbeat.id == me.id
    ));
}

#[tokio::test]
async fn tick_starts_election_when_leader_is_missing() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let peer = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(4),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (
                peer.clone(),
                Node::Manager {
                    host: "peer.local".to_string(),
                    port: 9001,
                    last_heartbeat: 0,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::VoteRequest { id, epoch, .. }] if id == &peer && *epoch == 5
    ));
    assert!(matches!(
        service.elections.last_key_value(),
        Some((5, Election::Mine { .. }))
    ));
}

#[tokio::test]
async fn tick_starts_a_new_election_only_for_manager_peers() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
    let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
    let (mut service, _config) = service(me.clone());
    let future = now_millis() + 10_000;
    service.state = Some(State {
        epoch: Some(7),
        elected_leader_id: None,
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, future)),
            (manager_peer.clone(), node("manager.local", 9001, 0)),
            (
                worker_peer.clone(),
                worker_node("worker.local", 9100, 0, vec![1]),
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(matches!(
        output.as_slice(),
        [NodeProtocol::VoteRequest { id, epoch, .. }] if id == &manager_peer && *epoch == 8
    ));
    assert!(matches!(
        service.elections.last_key_value(),
        Some((8, Election::Mine { .. }))
    ));
}

#[tokio::test]
async fn tick_clears_stale_remote_leader_without_emitting_messages() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let now = now_millis();
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: None,
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now)),
            (
                leader.clone(),
                Node::Manager {
                    host: "leader.local".to_string(),
                    port: 9001,
                    last_heartbeat: now - 1_000,
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service.tick(&mut output).await;

    assert!(output.is_empty());
    assert_eq!(service.state.as_ref().unwrap().elected_leader_id, None);
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

#[tokio::test]
async fn node_disconnected_for_unknown_node_is_a_noop() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me.clone());
    service.state = Some(State {
        epoch: Some(1),
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([
            (me.id.clone(), fresh_node(&me, now_millis())),
            (
                leader.clone(),
                Node::Manager {
                    host: "leader.local".to_string(),
                    port: 9001,
                    last_heartbeat: now_millis(),
                },
            ),
        ]),
        workers_with_calculated_partitions: Default::default(),
    });

    handle_node_disconnected(
        service.state.as_mut().unwrap(),
        node_id("33333333-3333-3333-3333-333333333333"),
        &me,
    );

    let state = service.state.as_ref().unwrap();
    assert!(state.nodes.contains_key(&me.id));
    assert!(state.nodes.contains_key(&leader));
    assert_eq!(state.elected_leader_id, Some(leader));
}

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
    let expected_replicas = (0..TEST_PARTITIONS_AMOUNT)
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
    assert_eq!(replicas_for_worker(state, &worker_a), expected_replicas);
    assert_eq!(replicas_for_worker(state, &worker_b), expected_replicas);
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
        .filter(|partition| partition % 2 == 0)
        .chain(
            (0..TEST_PARTITIONS_AMOUNT)
                .filter(|partition| partition % 3 == 0 && partition % 2 == 1),
        )
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_b_partitions = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 2 == 1)
        .chain(
            (0..TEST_PARTITIONS_AMOUNT)
                .filter(|partition| partition % 3 == 1 && partition % 2 == 0),
        )
        .map(|partition| partition as u16)
        .collect::<Vec<_>>();
    let expected_worker_c_partitions = (0..TEST_PARTITIONS_AMOUNT)
        .filter(|partition| partition % 3 == 2)
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
                    ClusterNode::Worker { id, masters, replicas, .. }
                        if id == &worker_a
                            && masters == &expected_worker_a_partitions
                            && replicas.is_empty()
                ))
                && state.items.iter().any(|item| matches!(
                    item,
                    ClusterNode::Worker { id, masters, replicas, .. }
                        if id == &worker_b
                            && masters == &expected_worker_b_partitions
                            && replicas.is_empty()
                ))
                && state.items.iter().any(|item| matches!(
                    item,
                    ClusterNode::Worker { id, masters, replicas, .. }
                        if id == &worker_c
                            && masters == &expected_worker_c_partitions
                            && replicas.is_empty()
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
}

#[tokio::test]
async fn node_disconnected_clears_current_leader() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let leader = node_id("22222222-2222-2222-2222-222222222222");
    let (mut service, _config) = service(me);
    service.state = Some(State {
        epoch: Some(7),
        elected_leader_id: Some(leader.clone()),
        nodes: HashMap::from([(
            leader.clone(),
            Node::Manager {
                host: "leader.local".to_string(),
                port: 9001,
                last_heartbeat: 0,
            },
        )]),
        workers_with_calculated_partitions: Default::default(),
    });

    let mut output = vec![];
    service
        .process(
            NodeProtocol::NodeDisconnected { id: leader.clone() },
            &mut output,
        )
        .await;

    assert!(output.is_empty());
    let state = service.state.as_ref().unwrap();
    assert_eq!(state.elected_leader_id, None);
    assert!(!state.nodes.contains_key(&leader));
}
