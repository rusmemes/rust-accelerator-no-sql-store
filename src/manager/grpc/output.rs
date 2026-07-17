use crate::common::{Me, NodeId};
use crate::manager::domain::{self, ClusterNode, NodeProtocol};
use crate::manager::grpc::api::v1::manager_event::Payload;
use crate::manager::grpc::api::v1::worker_event;
use crate::manager::grpc::api::v1::{
    Heartbeat, Leader, ManagerEvent, VoteRequest, VoteResponse, WorkerEvent,
};
use crate::manager::grpc::common::v1::{Addr, ClusterState, GetState, Node};
use crate::manager::grpc::conversions::{domain_node_type_to_grpc, domain_partitions_to_grpc};
use crate::manager::grpc::session::{handle_common, ManagerIOStream, WorkerIOStream};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

pub(super) async fn output(
    me: Arc<Me>,
    tx: Sender<NodeProtocol>,
    mut rx: Receiver<NodeProtocol>,
    manager_sessions: Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    config: Arc<RwLock<crate::common::Config>>,
) {
    while let Some(message) = rx.recv().await {
        tracing::debug!("output: {:?}", message);
        match message {
            NodeProtocol::Heartbeat {
                recipient_id: id,
                heartbeat: domain::Heartbeat { id: node_id, ts },
            } => {
                handle_output_heartbeat(&tx, &manager_sessions, id, node_id, ts).await;
            }
            NodeProtocol::NewConnection {
                id: _,
                host,
                port,
                manager,
            } => {
                let replication_factor = config
                    .read()
                    .await
                    .replication_factor
                    .expect("Partitions and replication factor should be defined");
                if manager {
                    super::new_manager_connection(
                        &me,
                        &tx,
                        &manager_sessions,
                        host,
                        port,
                        replication_factor,
                    )
                    .await;
                } else {
                    tracing::error!("NewConnection is not expected to be received for worker");
                }
            }
            NodeProtocol::GetClusterState { id } => {
                handle_output_get_cluster_state(&tx, &manager_sessions, id).await;
            }
            NodeProtocol::ClusterState {
                recipient_id,
                state:
                    domain::ClusterState {
                        epoch,
                        leader_id,
                        items,
                        partitions,
                    },
            } => {
                handle_output_cluster_state(
                    &tx,
                    &manager_sessions,
                    &worker_sessions,
                    recipient_id,
                    epoch,
                    leader_id,
                    items,
                    partitions,
                )
                .await;
            }
            NodeProtocol::VoteRequest { id, epoch, ts } => {
                handle_output_vote_request(&tx, &manager_sessions, id, epoch, ts).await;
            }
            NodeProtocol::VoteResponse { id, leader_id, ts } => {
                handle_output_vote_response(&tx, &manager_sessions, id, leader_id, ts).await;
            }
            NodeProtocol::Leader { id, epoch, ts } => {
                handle_output_leader(&me, &tx, &manager_sessions, &worker_sessions, id, epoch, ts)
                    .await;
            }
            NodeProtocol::NodeDisconnected { .. } => {
                unreachable!("NodeDisconnected is not expected to be sent");
            }
        }
    }
}

pub(super) async fn handle_output_leader(
    me: &Arc<Me>,
    tx: &Sender<NodeProtocol>,
    manager_sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: &Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    id: NodeId,
    epoch: u64,
    ts: u64,
) {
    // only leader node can send it
    let leader = || Leader {
        id: me.id.to_string(),
        epoch,
        ts,
    };
    let is_worker = worker_sessions.read().await.contains_key(&id);
    if is_worker {
        handle_common(
            "Leader",
            || WorkerEvent {
                payload: Some(worker_event::Payload::ManagerLeader(leader())),
            },
            tx,
            worker_sessions,
            id,
        )
        .await
    } else {
        handle_common(
            "Leader",
            || ManagerEvent {
                payload: Some(Payload::Leader(leader())),
            },
            tx,
            manager_sessions,
            id,
        )
        .await
    }
}

pub(super) async fn handle_output_vote_response(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    id: NodeId,
    leader_id: NodeId,
    ts: u64,
) {
    handle_common(
        "VoteResponse",
        || ManagerEvent {
            payload: Some(Payload::VoteResponse(VoteResponse {
                leader_id: leader_id.to_string(),
                ts,
            })),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_vote_request(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    id: NodeId,
    epoch: u64,
    ts: u64,
) {
    handle_common(
        "VoteRequest",
        || ManagerEvent {
            payload: Some(Payload::VoteRequest(VoteRequest { epoch, ts })),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_cluster_state(
    tx: &Sender<NodeProtocol>,
    manager_sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: &Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    id: NodeId,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    partitions: domain::Partitions,
) {
    let state = || ClusterState {
        epoch,
        leader_id: leader_id.to_string(),
        nodes: items
            .into_iter()
            .map(
                |ClusterNode {
                     id,
                     host,
                     port,
                     last_heartbeat,
                     node_type,
                 }| Node {
                    id: id.to_string(),
                    addr: Some(Addr { host, port }),
                    last_heartbeat,
                    node_type: domain_node_type_to_grpc(node_type),
                },
            )
            .collect(),
        partitions: Some(domain_partitions_to_grpc(partitions)),
    };

    let is_worker = worker_sessions.read().await.contains_key(&id);
    if is_worker {
        handle_common(
            "ClusterState",
            || WorkerEvent {
                payload: Some(worker_event::Payload::ClusterState(state())),
            },
            tx,
            worker_sessions,
            id,
        )
        .await;
    } else {
        handle_common(
            "ClusterState",
            || ManagerEvent {
                payload: Some(Payload::ClusterState(state())),
            },
            tx,
            manager_sessions,
            id,
        )
        .await;
    }
}

pub(super) async fn handle_output_get_cluster_state(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    id: NodeId,
) {
    handle_common(
        "GetClusterState",
        || ManagerEvent {
            payload: Some(Payload::GetClusterState(GetState {})),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_heartbeat(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    id: NodeId,
    node_id: NodeId,
    ts: u64,
) {
    handle_common(
        "Heartbeat",
        || ManagerEvent {
            payload: Some(Payload::Heartbeat(Heartbeat {
                id: node_id.to_string(),
                ts,
            })),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

#[cfg(test)]
mod tests;
