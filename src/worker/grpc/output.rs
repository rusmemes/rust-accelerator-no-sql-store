use crate::common::{ClusterState, Heartbeat, Me, NodeId};
use crate::worker::domain::WorkerProtocol;
use crate::worker::grpc::manager_connection::new_manager_connection;
use crate::worker::grpc::session::WorkerIOStream;
use std::collections::HashMap;
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
            WorkerProtocol::RemoveOldPartition {
                id,
                replica_id,
                partition_id,
            } => {
                todo!()
            }
            WorkerProtocol::Heartbeat {
                recipient_id: id,
                heartbeat: Heartbeat { id: node_id, ts },
            } => {
                todo!()
            }
            WorkerProtocol::NewConnection {
                id: _,
                host,
                port,
                manager,
            } => {
                if manager {
                    new_manager_connection(
                        &me,
                        &tx,
                        &manager_sessions,
                        host,
                        port
                    )
                        .await;
                } else {
                    tracing::error!("NewConnection is not expected to be received for worker");
                }
            }
            WorkerProtocol::GetClusterState { id } => {
                todo!()
            }
            WorkerProtocol::ClusterState {
                recipient_id,
                state:
                ClusterState {
                    epoch,
                    leader_id,
                    items,
                    partitions,
                },
            } => {
                todo!()
            }
            WorkerProtocol::Leader { id, epoch, ts } => {
                todo!()
            }
            WorkerProtocol::NodeDisconnected { .. } => {
                unreachable!("NodeDisconnected is not expected to be sent");
            }
        }
    }
}
