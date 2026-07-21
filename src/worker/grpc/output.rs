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
            WorkerProtocol::RemovePartitionFromReplica {
                id,
                replica_id,
                partition_id,
            } => {
                todo!()
            }
            WorkerProtocol::Heartbeat {
                id,
                heartbeat: Heartbeat { id: node_id, ts },
            } => {
                todo!()
            }
            WorkerProtocol::GetClusterState { id } => {
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
