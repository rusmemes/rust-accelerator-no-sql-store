use crate::common::{Me, NodeId};
use crate::manager::domain::{self, ClusterNode, NodeProtocol};
use crate::manager::grpc::api::v1::manager_event::Payload;
use crate::manager::grpc::api::v1::worker_event;
use crate::manager::grpc::api::v1::{
    Connect, Heartbeat, Leader, ManagerEvent, VoteRequest, VoteResponse, WorkerEvent,
};
use crate::manager::grpc::common::v1::{node, Addr, ClusterState, Manager, Worker};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

pub(super) async fn input_from_worker<S>(
    mut input: S,
    id: &NodeId,
    host: String,
    port: u32,
    tx: Sender<NodeProtocol>,
) where
    S: tokio_stream::Stream<Item = Result<WorkerEvent, Status>> + Unpin,
{
    if tx
        .send(NodeProtocol::NewConnection {
            id: Some(id.clone()),
            host,
            port,
            manager: false,
        })
        .await
        .is_ok()
    {
        tracing::info!("Worker with ID {} is connected", id);
        while let Some(Ok(WorkerEvent {
            payload: Some(payload),
        })) = input.next().await
        {
            match payload {
                worker_event::Payload::Heartbeat(Heartbeat { id: node_id, ts }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::Heartbeat {
                            recipient_id: id.clone(),
                            heartbeat: domain::Heartbeat {
                                id: node_id.into(),
                                ts,
                            },
                        })
                        .await
                    {
                        tracing::error!("Error processing Heartbeat signal: {}", e);
                        break;
                    }
                }
                worker_event::Payload::GetClusterState(_) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::GetClusterState { id: id.clone() })
                        .await
                    {
                        tracing::error!("Error processing GetClusterState request: {}", e);
                        break;
                    }
                }
                worker_event::Payload::ClusterState(_) => {
                    tracing::error!("ClusterState is not expected to be received");
                    break;
                }
                worker_event::Payload::ManagerLeader(_) => {
                    tracing::error!("ManagerLeader is not expected to be received");
                    break;
                }
                worker_event::Payload::Connect(Connect { id: request_id, .. }) => {
                    tracing::error!(
                        "Received duplicated connect request from {}: id {}",
                        id,
                        request_id
                    );
                    break;
                }
                worker_event::Payload::ConnectResponse(_) => {
                    tracing::error!("ConnectResponse is not expected to be received");
                    break;
                }
            }
        }
    }
}

pub(super) async fn input_from_manager(
    me: Arc<Me>,
    mut input: Streaming<ManagerEvent>,
    id: &NodeId,
    host: String,
    port: u32,
    tx: Sender<NodeProtocol>,
) {
    if tx
        .send(NodeProtocol::NewConnection {
            id: Some(id.clone()),
            host,
            port,
            manager: true,
        })
        .await
        .is_ok()
    {
        tracing::info!("Manager with ID {} is connected", id);
        while let Ok(Some(ManagerEvent {
            payload: Some(payload),
        })) = input.message().await
        {
            match payload {
                Payload::Heartbeat(Heartbeat { id: node_id, ts }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::Heartbeat {
                            recipient_id: id.clone(),
                            heartbeat: domain::Heartbeat {
                                id: node_id.into(),
                                ts,
                            },
                        })
                        .await
                    {
                        tracing::error!("Error processing Heartbeat signal: {}", e);
                        break;
                    }
                }
                Payload::GetClusterState(_) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::GetClusterState { id: id.clone() })
                        .await
                    {
                        tracing::error!("Error processing GetClusterState request: {}", e);
                        break;
                    }
                }
                Payload::ClusterState(ClusterState {
                    epoch,
                    leader_id,
                    nodes,
                    ..
                }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::ClusterState {
                            recipient_id: me.id.clone(),
                            state: domain::ClusterState {
                                config: None,
                                epoch,
                                leader_id: leader_id.into(),
                                items: nodes
                                    .into_iter()
                                    .filter_map(|grpc| match grpc.payload {
                                        Some(node::Payload::Manager(Manager {
                                            id,
                                            addr: Some(Addr { host, port }),
                                            last_heartbeat,
                                        })) => Some(ClusterNode::Manager {
                                            id: id.into(),
                                            host,
                                            port,
                                            last_heartbeat,
                                        }),
                                        Some(node::Payload::Worker(Worker {
                                            id,
                                            addr: Some(Addr { host, port }),
                                            last_heartbeat,
                                            masters,
                                            replicas,
                                        })) => Some(ClusterNode::Worker {
                                            id: id.into(),
                                            host,
                                            port,
                                            last_heartbeat,
                                            masters: masters
                                                .into_iter()
                                                .map(|p| p as u16)
                                                .collect(),
                                            replicas: replicas
                                                .into_iter()
                                                .map(|p| p as u16)
                                                .collect(),
                                        }),
                                        _ => None,
                                    })
                                    .collect(),
                            },
                        })
                        .await
                    {
                        tracing::error!("Error processing ClusterState response: {}", e);
                        break;
                    }
                }
                Payload::VoteRequest(VoteRequest { epoch, ts }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::VoteRequest {
                            id: id.clone(),
                            epoch,
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing VoteRequest request: {}", e);
                        break;
                    }
                }
                Payload::VoteResponse(VoteResponse { leader_id, ts }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::VoteResponse {
                            id: id.clone(),
                            leader_id: leader_id.into(),
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing VoteResponse response: {}", e);
                        break;
                    }
                }
                Payload::Leader(Leader { id, epoch, ts }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::Leader {
                            id: id.into(),
                            epoch,
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing Leader notification: {}", e);
                        break;
                    }
                }
                Payload::Connect(Connect { id: request_id, .. }) => {
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
