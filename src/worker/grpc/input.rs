use crate::common::NodeId;
use crate::worker::domain::WorkerProtocol;
use crate::worker::grpc::api::v1::{worker_event, Connect, WorkerEvent};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::Status;
use worker_event::Payload;

pub(super) async fn input_from_manager<S>(
    mut input: S,
    id: &NodeId,
    host: String,
    port: u32,
    tx: Sender<WorkerProtocol>,
) where
    S: tokio_stream::Stream<Item = Result<WorkerEvent, Status>> + Unpin,
{
    if tx
        .send(WorkerProtocol::NewConnection {
            id: Some(id.clone()),
            host,
            port,
            manager: false,
        })
        .await
        .is_ok()
    {
        tracing::info!("Manager with ID {} is connected", id);
        while let Some(Ok(WorkerEvent {
            payload: Some(payload),
        })) = input.next().await
        {
            match payload {
                Payload::RemovePartitionFromReplica(_) => {
                    todo!()
                }
                Payload::Heartbeat(_) => {
                    todo!()
                }
                Payload::GetClusterState(_) => {
                    tracing::error!("GetClusterState is not expected to be received");
                    break;
                }
                Payload::ClusterState(_) => {
                    todo!()
                }
                Payload::ManagerLeader(_) => {
                    todo!()
                }
                Payload::Connect(Connect {
                    id: request_id, ..
                }) => {
                    tracing::error!(
                        "Received duplicated connect request from {}: id {}",
                        id,
                        request_id
                    );
                    break;
                }
                Payload::ConnectResponse(_) => {
                    tracing::error!("ConnectResponse is not expected to be received");
                    break;
                }
            }
        }
    }
}
