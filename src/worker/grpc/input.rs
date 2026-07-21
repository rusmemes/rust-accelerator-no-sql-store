use crate::{
    common,
    common::{ClusterNode, Me, NodeId},
    conversions::{
        api::v1::{
            worker_event::Payload,
            Connect,
            Heartbeat,
            Leader,
            RemovePartitionFromReplica,
            WorkerEvent
        },
        common::v1::{Addr, ClusterState, Node},
        grpc_node_type_to_domain,
        grpc_partitions_to_domain
    },
    worker::domain::WorkerProtocol,
};

use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::Status;

pub(super) async fn input_from_manager<S>(
    mut input: S,
    id: &NodeId,
    host: String,
    port: u32,
    tx: Sender<WorkerProtocol>,
    me: &Me,
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
                Payload::RemovePartitionFromReplica(RemovePartitionFromReplica {
                    replica_id,
                    partition_id,
                }) => {
                    if let Err(e) = tx
                        .send(WorkerProtocol::RemovePartitionFromReplica {
                            id: id.clone(),
                            replica_id: replica_id.into(),
                            partition_id: partition_id as u16,
                        })
                        .await
                    {
                        tracing::error!(
                            "Error processing RemovePartitionFromReplica signal: {}",
                            e
                        );
                        break;
                    }
                }
                Payload::Heartbeat(Heartbeat { id: node_id, ts }) => {
                    if let Err(e) = tx
                        .send(WorkerProtocol::Heartbeat {
                            id: id.clone(),
                            heartbeat: common::Heartbeat {
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
                    tracing::error!("GetClusterState is not expected to be received");
                    break;
                }
                Payload::ClusterState(ClusterState {
                    epoch,
                    leader_id,
                    nodes,
                    partitions,
                }) => {
                    if let Some(partitions) = partitions {
                        if let Err(e) = tx
                            .send(WorkerProtocol::ClusterState {
                                recipient_id: me.id.clone(),
                                state: common::ClusterState {
                                    epoch,
                                    leader_id: leader_id.into(),
                                    partitions: grpc_partitions_to_domain(partitions),
                                    items: nodes
                                        .into_iter()
                                        .filter_map(|node| {
                                            if let Node {
                                                id,
                                                addr: Some(Addr { host, port }),
                                                last_heartbeat,
                                                node_type,
                                            } = node
                                            {
                                                Some(ClusterNode {
                                                    id: id.into(),
                                                    host,
                                                    port,
                                                    last_heartbeat,
                                                    node_type: grpc_node_type_to_domain(node_type),
                                                })
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                },
                            })
                            .await
                        {
                            tracing::error!("Error processing ClusterState response: {}", e);
                            break;
                        }
                    } else {
                        tracing::error!("Received ClusterState response with no partitions");
                        break;
                    }
                }
                Payload::ManagerLeader(Leader { id, epoch, ts }) => {
                    if let Err(e) = tx
                        .send(WorkerProtocol::Leader {
                            id: id.into(),
                            epoch,
                            ts,
                        })
                        .await
                    {
                        tracing::error!("Error processing ManagerLeader signal: {}", e);
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
