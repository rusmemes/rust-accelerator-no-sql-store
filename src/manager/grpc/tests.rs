use super::*;
use crate::manager::domain;
use crate::manager::domain::ClusterNode;
use crate::manager::grpc::api::v1::{worker_event, Heartbeat, Leader};
use crate::manager::grpc::common::v1::{node, GetState, Manager, Node, Worker};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;

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

fn manager_output_sender() -> (Sender<ManagerEvent>, Receiver<ManagerEvent>) {
    tokio::sync::mpsc::channel(4)
}

fn worker_output_sender() -> (Sender<WorkerEvent>, Receiver<WorkerEvent>) {
    tokio::sync::mpsc::channel(4)
}

fn manager_session(
    id: &NodeId,
    stream: ManagerIOStream,
) -> Arc<RwLock<HashMap<NodeId, ManagerIOStream>>> {
    Arc::new(RwLock::new(HashMap::from([(id.clone(), stream)])))
}

fn worker_session(
    id: &NodeId,
    stream: WorkerIOStream,
) -> Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> {
    Arc::new(RwLock::new(HashMap::from([(id.clone(), stream)])))
}

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
                partitions: vec![1, 2],
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
            partitions,
        })) if *id == worker_node_id.to_string()
            && host == "worker.local"
            && *port == 9100
            && *last_heartbeat == 11
            && *partitions == vec![1, 2]
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

#[tokio::test]
async fn input_from_worker_forwards_messages_and_stops_when_stream_ends() {
    let worker_id = node_id("22222222-2222-2222-2222-222222222222");
    let (protocol_tx, mut protocol_rx) = tokio::sync::mpsc::channel(8);
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(4);
    let stream = ReceiverStream::new(request_rx);
    let worker_id_clone = worker_id.clone();
    let worker_task = tokio::spawn(async move {
        input_from_worker(
            stream,
            &worker_id_clone,
            "worker.local".to_string(),
            9100,
            protocol_tx,
        )
        .await;
    });

    let new_connection = timeout(Duration::from_secs(1), protocol_rx.recv())
        .await
        .expect("new connection timeout")
        .expect("new connection");
    assert!(matches!(
        new_connection,
        NodeProtocol::NewConnection {
            id: Some(id),
            host,
            port,
            manager: false,
        } if id == worker_id && host == "worker.local" && port == 9100
    ));

    request_tx
        .send(Ok(WorkerEvent {
            payload: Some(worker_event::Payload::Heartbeat(Heartbeat {
                id: worker_id.to_string(),
                ts: 44,
            })),
        }))
        .await
        .expect("heartbeat message");

    request_tx
        .send(Ok(WorkerEvent {
            payload: Some(worker_event::Payload::GetClusterState(GetState {})),
        }))
        .await
        .expect("cluster state message");
    drop(request_tx);

    let heartbeat = timeout(Duration::from_secs(1), protocol_rx.recv())
        .await
        .expect("heartbeat timeout")
        .expect("heartbeat");
    assert!(matches!(
        heartbeat,
        NodeProtocol::Heartbeat {
            recipient_id,
            heartbeat: domain::Heartbeat { id, ts },
        } if recipient_id == worker_id && id == worker_id && ts == 44
    ));

    let get_cluster_state = timeout(Duration::from_secs(1), protocol_rx.recv())
        .await
        .expect("cluster state timeout")
        .expect("cluster state");
    assert!(matches!(
        get_cluster_state,
        NodeProtocol::GetClusterState { id } if id == worker_id
    ));

    timeout(Duration::from_secs(1), worker_task)
        .await
        .expect("worker task timeout")
        .expect("worker task");
}
