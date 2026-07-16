use super::*;
use crate::manager::domain::{self, ClusterNode, NodeProtocol};
use crate::manager::grpc::api::v1::{worker_event, Leader};
use crate::manager::grpc::common::v1::{node, Manager, Node, Worker};
use crate::manager::grpc::test_support::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::test]
async fn output_routes_leader_to_worker_session() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (worker_tx, mut worker_rx) = worker_output_sender();
    let manager_sessions = Arc::new(RwLock::new(HashMap::new()));
    let worker_sessions = worker_session(&worker_id, WorkerIOStream::Output(worker_tx));

    handle_output_leader(
        &me,
        &tx,
        &manager_sessions,
        &worker_sessions,
        worker_id.clone(),
        3,
        44,
    )
    .await;

    let event = worker_rx.recv().await.expect("worker event");
    assert!(matches!(
        event.payload,
        Some(worker_event::Payload::ManagerLeader(Leader { id, epoch, ts }))
            if id == me.id.to_string() && epoch == 3 && ts == 44
    ));
}

#[tokio::test]
async fn output_routes_leader_to_manager_session() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (manager_tx, mut manager_rx) = manager_output_sender();
    let manager_sessions = manager_session(&manager_id, ManagerIOStream::Output(manager_tx));
    let worker_sessions = Arc::new(RwLock::new(HashMap::new()));

    handle_output_leader(
        &me,
        &tx,
        &manager_sessions,
        &worker_sessions,
        manager_id.clone(),
        7,
        99,
    )
    .await;

    let event = manager_rx.recv().await.expect("manager event");
    assert!(matches!(
        event.payload,
        Some(Payload::Leader(Leader { id, epoch, ts }))
            if id == me.id.to_string() && epoch == 7 && ts == 99
    ));
}

#[tokio::test]
async fn output_routes_cluster_state_to_worker_session_includes_config() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let manager_node_id = node_id("33333333-3333-3333-3333-333333333333");
    let worker_node_id = node_id("44444444-4444-4444-4444-444444444444");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (worker_tx, mut worker_rx) = worker_output_sender();
    let manager_sessions = Arc::new(RwLock::new(HashMap::new()));
    let worker_sessions = worker_session(&worker_id, WorkerIOStream::Output(worker_tx));

    handle_output_cluster_state(
        &tx,
        &manager_sessions,
        &worker_sessions,
        worker_id.clone(),
        5,
        manager_node_id.clone(),
        vec![
            ClusterNode::Manager {
                id: manager_node_id.clone(),
                host: "manager.local".to_string(),
                port: 9001,
                last_heartbeat: 10,
            },
            ClusterNode::Worker {
                id: worker_node_id.clone(),
                host: "worker.local".to_string(),
                port: 9100,
                last_heartbeat: 11,
                masters: vec![1, 2],
                replicas: vec![3, 4],
            },
        ],
        Some(domain::Config {
            replication_factor: 4,
        }),
    )
    .await;

    let event = worker_rx.recv().await.expect("worker cluster state");
    let cluster_state = match event.payload {
        Some(worker_event::Payload::ClusterState(cluster_state)) => cluster_state,
        other => panic!("unexpected payload: {:?}", other),
    };

    assert_eq!(cluster_state.epoch, 5);
    assert_eq!(cluster_state.leader_id, manager_node_id.to_string());
    assert_eq!(
        cluster_state.config.as_ref().map(|c| c.replication_factor),
        Some(4)
    );
    assert_eq!(cluster_state.nodes.len(), 2);
    assert!(cluster_state.nodes.iter().any(|node| matches!(
        &node.payload,
        Some(node::Payload::Manager(Manager {
            id,
            addr: Some(Addr { host, port }),
            last_heartbeat,
        })) if *id == manager_node_id.to_string()
            && host == "manager.local"
            && *port == 9001
            && *last_heartbeat == 10
    )));
    assert!(cluster_state.nodes.iter().any(|node| matches!(
        &node.payload,
        Some(node::Payload::Worker(Worker {
            id,
            addr: Some(Addr { host, port }),
            last_heartbeat,
            masters,
            replicas,
        })) if *id == worker_node_id.to_string()
            && host == "worker.local"
            && *port == 9100
            && *last_heartbeat == 11
            && *masters == vec![1, 2]
            && *replicas == vec![3, 4]
    )));

    let _ = me;
}

#[tokio::test]
async fn output_routes_cluster_state_to_manager_session_includes_config() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let (manager_tx, mut manager_rx) = manager_output_sender();
    let manager_sessions = manager_session(&manager_id, ManagerIOStream::Output(manager_tx));
    let worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> =
        Arc::new(RwLock::new(HashMap::new()));

    handle_output_cluster_state(
        &tx,
        &manager_sessions,
        &worker_sessions,
        manager_id.clone(),
        2,
        me.id.clone(),
        vec![ClusterNode::Manager {
            id: me.id.clone(),
            host: "self.local".to_string(),
            port: 7000,
            last_heartbeat: 77,
        }],
        Some(domain::Config {
            replication_factor: 2,
        }),
    )
    .await;

    let event = manager_rx.recv().await.expect("manager cluster state");
    let cluster_state = match event.payload {
        Some(Payload::ClusterState(cluster_state)) => cluster_state,
        other => panic!("unexpected payload: {:?}", other),
    };

    assert_eq!(cluster_state.epoch, 2);
    assert_eq!(cluster_state.leader_id, me.id.to_string());
    assert_eq!(
        cluster_state.config.as_ref().map(|c| c.replication_factor),
        Some(2)
    );
    assert_eq!(cluster_state.nodes.len(), 1);
    assert!(matches!(
        cluster_state.nodes.as_slice(),
        [Node {
            payload: Some(node::Payload::Manager(Manager {
                id,
                addr: Some(Addr { host, port }),
                last_heartbeat,
            })),
        }] if *id == me.id.to_string()
            && host == "self.local"
            && *port == 7000
            && *last_heartbeat == 77
    ));
}

#[tokio::test]
async fn output_removes_closed_manager_session() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let manager_id = node_id("22222222-2222-2222-2222-222222222222");
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let (manager_tx, manager_rx) = manager_output_sender();
    drop(manager_rx);
    let manager_sessions = manager_session(&manager_id, ManagerIOStream::Output(manager_tx));

    handle_output_heartbeat(
        &tx,
        &manager_sessions,
        manager_id.clone(),
        me.id.clone(),
        123,
    )
    .await;

    assert!(!manager_sessions.read().await.contains_key(&manager_id));
    assert!(matches!(
        rx.recv().await.expect("protocol message"),
        NodeProtocol::NodeDisconnected { id } if id == manager_id
    ));
}
