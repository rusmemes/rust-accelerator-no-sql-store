use crate::{
    common::{Heartbeat, Me, NodeId},
    conversions::{
        self,
        api::v1::{worker_event, RemovePartitionFromReplica, WorkerEvent},
        common::v1::GetState,
    },
    worker::{
        domain::WorkerProtocol,
        grpc::manager_connection::new_manager_connection,
        grpc::session::{IOStreamExt, WorkerIOStream}
    }
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

pub(super) async fn output(
    me: Me,
    tx: Sender<WorkerProtocol>,
    mut rx: Receiver<WorkerProtocol>,
    manager_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
) {
    while let Some(message) = rx.recv().await {
        tracing::debug!("output: {:?}", message);
        match message {
            WorkerProtocol::RemovePartitionFromReplica {
                id,
                replica_id,
                partition_id,
            } => {
                handle_output_remove_partition_from_replica(
                    &tx,
                    &manager_sessions,
                    id,
                    replica_id,
                    partition_id,
                )
                .await;
            }
            WorkerProtocol::Heartbeat {
                id,
                heartbeat: Heartbeat { id: node_id, ts },
            } => {
                handle_output_heartbeat(&tx, &manager_sessions, id, node_id, ts).await;
            }
            WorkerProtocol::GetClusterState { id } => {
                handle_output_get_cluster_state(&tx, &manager_sessions, id).await;
            }
            WorkerProtocol::NewConnection {
                id: _,
                host,
                port,
                manager,
            } => {
                if manager {
                    new_manager_connection(&me, &tx, &manager_sessions, host, port).await;
                } else {
                    tracing::error!("NewConnection is not expected to be received for worker");
                }
            }
            WorkerProtocol::ClusterState { .. } => {
                tracing::error!("ClusterState is not expected to be sent by workers");
            }
            WorkerProtocol::Leader { id, epoch, ts } => {
                tracing::error!("Leader is not expected to be sent by workers");
            }
            WorkerProtocol::NodeDisconnected { .. } => {
                unreachable!("NodeDisconnected is not expected to be sent");
            }
        }
    }
}

pub(super) async fn handle_output_remove_partition_from_replica(
    tx: &Sender<WorkerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    recipient_id: NodeId,
    replica_id: NodeId,
    partition_id: u16,
) {
    let remove_partition_from_replica = || RemovePartitionFromReplica {
        partition_id: partition_id as u32,
        replica_id: replica_id.to_string(),
    };
    let is_manager = manager_sessions.read().await.contains_key(&recipient_id);
    if is_manager {
        handle_common(
            "RemovePartitionFromReplica",
            || WorkerEvent {
                payload: Some(worker_event::Payload::RemovePartitionFromReplica(
                    remove_partition_from_replica(),
                )),
            },
            tx,
            manager_sessions,
            recipient_id,
        )
        .await;
    } else {
        todo!()
    }
}

pub(super) async fn handle_output_get_cluster_state(
    tx: &Sender<WorkerProtocol>,
    sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
) {
    handle_common(
        "GetClusterState",
        || WorkerEvent {
            payload: Some(worker_event::Payload::GetClusterState(GetState {})),
        },
        tx,
        sessions,
        id,
    )
    .await;
}

pub(super) async fn handle_output_heartbeat(
    tx: &Sender<WorkerProtocol>,
    manager_sessions: &RwLock<HashMap<NodeId, WorkerIOStream>>,
    id: NodeId,
    node_id: NodeId,
    ts: u64,
) {
    let heartbeat = || conversions::api::v1::Heartbeat {
        id: node_id.to_string(),
        ts,
    };

    let is_manager = manager_sessions.read().await.contains_key(&id);
    if is_manager {
        handle_common(
            "Heartbeat",
            || WorkerEvent {
                payload: Some(worker_event::Payload::Heartbeat(heartbeat())),
            },
            tx,
            manager_sessions,
            id,
        )
        .await;
    } else {
        todo!()
    }
}

pub(super) async fn handle_common<Event, Error, Stream>(
    event_type: &'static str,
    event: impl FnOnce() -> Event,
    tx: &Sender<WorkerProtocol>,
    sessions: &RwLock<HashMap<NodeId, Stream>>,
    id: NodeId,
) where
    Error: Debug,
    Stream: IOStreamExt<Event, Error> + Clone,
{
    let is_closed = {
        sessions
            .read()
            .await
            .get(&id)
            .is_some_and(|sender| sender.is_closed())
    };

    if is_closed {
        tracing::debug!("Node {} is disconnected", id);
        sessions.write().await.remove(&id);
        let _ = tx.send(WorkerProtocol::NodeDisconnected { id }).await;
    } else if let Some(sender) = { sessions.read().await.get(&id).cloned() } {
        if let Err(e) = sender.send(event()).await {
            tracing::error!("Error sending {event_type} to {}: {:?}", id, e);
        }
    }
}
