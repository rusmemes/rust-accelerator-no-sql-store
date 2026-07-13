use crate::common::Either;
use crate::{
    common::{Me, NodeId},
    manager::{
        domain,
        domain::NodeProtocol,
        grpc::{
            api::v1::{
                manager_api_client::ManagerApiClient, manager_api_server::{ManagerApi, ManagerApiServer}, message::Payload, Connect, ConnectResponse, Heartbeat, Leader,
                Message,
                VoteRequest,
                VoteResponse,
            },
            common::v1::{Addr, ClusterState, ClusterStateItem, GetClusterState},
        },
    },
};
use std::{collections::HashMap, net::AddrParseError, sync::Arc};
use thiserror::Error;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    sync::RwLock,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status, Streaming};

mod api {
    pub mod v1 {
        tonic::include_proto!("manager_api.v1");
    }
}

mod common {
    pub mod v1 {
        tonic::include_proto!("common.v1");
    }
}

const GRPC_CONNECTION_CHANNEL_BUFFER_SIZE: usize = 32;

/**
EitherStream is a stream that can send messages to either a Sender<Result<Message, Status>> or a Sender<Message>.
It is used to send messages to either the gRPC output stream or the gRPC input stream.
The first Sender is used for gRPC output stream, the second Sender is used for gRPC input stream.
*/
type EitherStream = Either<Sender<Result<Message, Status>>, Sender<Message>>;

impl EitherStream {
    async fn send(&self, message: Message) -> Result<(), Either<Status, Message>> {
        match self {
            EitherStream::Left(sender) => {
                if let Err(e) = sender.send(Ok(message)).await {
                    let result = e.0;
                    if let Err(status) = result {
                        return Err(Either::Left(status));
                    }
                }
            }
            EitherStream::Right(sender) => {
                if let Err(e) = sender.send(message).await {
                    return Err(Either::Right(e.0));
                }
            }
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        match self {
            EitherStream::Left(sender) => sender.is_closed(),
            EitherStream::Right(sender) => sender.is_closed(),
        }
    }
}

type OpenConnectionStream = ReceiverStream<Result<Message, Status>>;

async fn output(
    me: Arc<Me>,
    tx: Sender<NodeProtocol>,
    mut rx: Receiver<NodeProtocol>,
    sessions: Arc<RwLock<HashMap<NodeId, EitherStream>>>,
) {
    while let Some(message) = rx.recv().await {
        tracing::debug!("output: {:?}", message);
        match message {
            NodeProtocol::Heartbeat {
                recipient_id: id,
                heartbeat: domain::Heartbeat { id: node_id, ts },
            } => {
                handle_output_heartbeat(&tx, &sessions, id, node_id, ts).await;
            }
            NodeProtocol::NewConnection { id: _, host, port } => {
                new_connection(&me, &tx, &sessions, host, port).await;
            }
            NodeProtocol::GetClusterState { id } => {
                handle_output_get_cluster_state(&tx, &sessions, id).await;
            }
            NodeProtocol::ClusterState {
                recipient_id,
                state:
                    domain::ClusterState {
                        epoch,
                        leader_id,
                        items,
                    },
            } => {
                handle_output_cluster_state(&tx, &sessions, recipient_id, epoch, leader_id, items)
                    .await;
            }
            NodeProtocol::VoteRequest { id, epoch, ts } => {
                handle_output_vote_request(&tx, &sessions, id, epoch, ts).await;
            }
            NodeProtocol::VoteResponse { id, leader_id, ts } => {
                handle_output_vote_response(&tx, &sessions, id, leader_id, ts).await;
            }
            NodeProtocol::Leader { id, epoch, ts } => {
                handle_output_leader(&me, &tx, &sessions, id, epoch, ts).await;
            }
            NodeProtocol::NodeDisconnected { .. } => {
                unreachable!("NodeDisconnected is not expected to be sent");
            }
        }
    }
}

async fn handle_common(
    event_type: &'static str,
    payload: Payload,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
) {
    if let Some(sender) = sessions.read().await.get(&id) {
        if sender.is_closed() {
            tracing::debug!("Node {} is disconnected", id);
            sessions.write().await.remove(&id);
            let _ = tx.send(NodeProtocol::NodeDisconnected { id }).await;
        } else if let Err(e) = sender
            .send(Message {
                payload: Some(payload),
            })
            .await
        {
            if let Either::Left(status) = e {
                tracing::error!("Error sending {event_type} to {}: status {}", id, status);
            } else {
                tracing::error!("Error sending {event_type} to {}", id);
            }
        }
    }
}

async fn handle_output_leader(
    me: &Arc<Me>,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
    epoch: u64,
    ts: u64,
) {
    // only leader node can send it
    handle_common(
        "Leader",
        Payload::Leader(Leader {
            id: me.id.to_string(),
            epoch,
            ts,
        }),
        tx,
        sessions,
        id,
    ).await;
}

async fn handle_output_vote_response(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
    leader_id: NodeId,
    ts: u64,
) {
    handle_common(
        "VoteResponse",
        Payload::VoteResponse(VoteResponse {
            leader_id: leader_id.to_string(),
            ts,
        }),
        tx,
        sessions,
        id,
    ).await;
}

async fn handle_output_vote_request(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
    epoch: u64,
    ts: u64,
) {
    handle_common(
        "VoteRequest",
        Payload::VoteRequest(VoteRequest {
            epoch,
            ts,
        }),
        tx,
        sessions,
        id,
    ).await;
}

async fn handle_output_cluster_state(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<domain::ClusterStateItem>,
) {
    handle_common(
        "ClusterState",
        Payload::ClusterState(ClusterState {
            epoch,
            leader_id: leader_id.to_string(),
            items: items
                .into_iter()
                .map(|item| ClusterStateItem {
                    id: item.id.to_string(),
                    addr: Some(Addr {
                        host: item.host,
                        port: item.port,
                    }),
                    last_heartbeat: item.last_heartbeat,
                })
                .collect(),
        }),
        tx,
        sessions,
        id,
    ).await;
}

async fn handle_output_get_cluster_state(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
) {
    handle_common(
        "GetClusterState",
        Payload::GetClusterState(GetClusterState {}),
        tx,
        sessions,
        id,
    ).await;
}

async fn handle_output_heartbeat(
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    id: NodeId,
    node_id: NodeId,
    ts: u64,
) {
    handle_common(
        "Heartbeat",
        Payload::Heartbeat(Heartbeat {
            id: node_id.to_string(),
            ts,
        }),
        tx,
        sessions,
        id,
    ).await;
}

async fn new_connection(
    me: &Arc<Me>,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    host: String,
    port: u32,
) {
    let client = ManagerApiClient::connect(format!("http://{host}:{port}")).await;
    match client {
        Ok(mut client) => {
            tracing::debug!("Connecting to manager");
            let (grpc_output, rx) = tokio::sync::mpsc::channel::<Message>(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);
            let outbound = ReceiverStream::new(rx);

            let response = client.open_connection(Request::new(outbound)).await;
            match response {
                Ok(response) => {
                    tracing::debug!("Connected to manager");
                    start_communication(me, tx, sessions, host, port, grpc_output, response).await;
                }
                Err(e) => {
                    tracing::error!("Failed to open connection to manager: {}", e);
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to connect to manager: {}", e);
        }
    }
}

async fn start_communication(
    me: &Arc<Me>,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    host: String,
    port: u32,
    grpc_output: Sender<Message>,
    response: Response<Streaming<Message>>,
) {
    let mut input_stream = response.into_inner();
    let sender = Either::Right(grpc_output);
    if sender
        .send(Message {
            payload: Some(Payload::Connect(Connect {
                id: me.id.to_string(),
                addr: Some(Addr {
                    host: me.host.clone(),
                    port: me.port,
                }),
            })),
        })
        .await
        .is_ok()
    {
        if let Ok(Some(Message {
            payload: Some(Payload::ConnectResponse(ConnectResponse { id })),
        })) = input_stream.message().await
        {
            let id: NodeId = id.into();
            sessions.write().await.insert(id.clone(), sender);
            tracing::info!("Node {} is connected", id);

            let sessions = sessions.clone();
            let tx = tx.clone();
            let me = me.clone();
            tokio::spawn(async move {
                input(me, input_stream, &id, host, port, tx.clone()).await;
                sessions.write().await.remove(&id);
                tracing::info!("Node {} is disconnected", id);
                let _ = tx.send(NodeProtocol::NodeDisconnected { id }).await;
            });
        } else {
            tracing::error!("Node {}:{} is not connected ", host, port);
        }
    } else {
        tracing::error!("Failed to open connection to manager");
    }
}

async fn input(
    me: Arc<Me>,
    mut input: Streaming<Message>,
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
        })
        .await
        .is_ok()
    {
        tracing::info!("Node with ID {} is connected", id);
        while let Ok(Some(Message {
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
                Payload::GetClusterState(GetClusterState {}) => {
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
                    items,
                }) => {
                    if let Err(e) = tx
                        .send(NodeProtocol::ClusterState {
                            recipient_id: me.id.clone(),
                            state: domain::ClusterState {
                                epoch,
                                leader_id: leader_id.into(),
                                items: items
                                    .into_iter()
                                    .filter_map(|grpc| {
                                        grpc.addr.map(|Addr { host, port }| {
                                            domain::ClusterStateItem {
                                                id: grpc.id.into(),
                                                host,
                                                port,
                                                last_heartbeat: grpc.last_heartbeat,
                                            }
                                        })
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
                    tracing::debug!(
                        "Received duplicated connect request from {}: id {}",
                        id,
                        request_id
                    );
                    break;
                }
                Payload::ConnectResponse(ConnectResponse { .. }) => {
                    tracing::error!("ConnectResponse is not expected to be received");
                    break;
                }
            }
        }
    }
}

pub struct ManagerApiService {
    me: Arc<Me>,
    sessions: Arc<RwLock<HashMap<NodeId, EitherStream>>>,
    tx: Sender<NodeProtocol>,
}
impl ManagerApiService {
    pub fn new((tx, rx): (Sender<NodeProtocol>, Receiver<NodeProtocol>), me: Arc<Me>) -> Self {
        let sessions: Arc<RwLock<HashMap<NodeId, EitherStream>>> = Default::default();
        let sessions_clone = sessions.clone();
        let tx_clone = tx.clone();
        let service = Self {
            me: me.clone(),
            sessions,
            tx,
        };
        tokio::spawn(output(me, tx_clone, rx, sessions_clone));
        service
    }
}

#[tonic::async_trait]
impl ManagerApi for ManagerApiService {
    type OpenConnectionStream = OpenConnectionStream;

    async fn open_connection(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<Self::OpenConnectionStream>, Status> {
        tracing::info!("Received open_connection request");

        let remote_addr = request.remote_addr();
        let mut input_stream: Streaming<Message> = request.into_inner();
        let (grpc_tx, rx) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let sessions = self.sessions.clone();
        let tx = self.tx.clone();
        let me = self.me.clone();

        tokio::spawn(async move {
            if let Ok(Some(Message {
                payload:
                    Some(Payload::Connect(Connect {
                        id,
                        addr: Some(Addr { host, port }),
                    })),
            })) = input_stream.message().await
            {
                let id: NodeId = id.into();
                sessions
                    .write()
                    .await
                    .insert(id.clone(), Either::Left(grpc_tx.clone()));

                tokio::spawn(async move {
                    if grpc_tx
                        .send(Ok(Message {
                            payload: Some(Payload::ConnectResponse(ConnectResponse {
                                id: me.id.to_string(),
                            })),
                        }))
                        .await
                        .is_ok()
                    {
                        input(me, input_stream, &id, host, port, tx.clone()).await;
                    }
                    sessions.write().await.remove(&id);
                    tracing::info!("Node {} is disconnected", id);
                    let _ = tx.send(NodeProtocol::NodeDisconnected { id }).await;
                });
            } else {
                tracing::error!("Failed to read Connect message from {:?}", remote_addr);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug, Error)]
pub enum GrpcServerError {
    #[error("Failed to parse address: {0}")]
    AddressParse(#[from] AddrParseError),
    #[error("GRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

pub async fn start_server(
    me: Arc<Me>,
    channel: (Sender<NodeProtocol>, Receiver<NodeProtocol>),
    port: u16,
    cancellation_token: CancellationToken,
) -> Result<(), GrpcServerError> {
    let grpc_address = format!("127.0.0.1:{}", port).as_str().parse()?;

    tracing::info!("GRPC Server is starting at {}", grpc_address);

    Server::builder()
        .add_service(ManagerApiServer::new(ManagerApiService::new(channel, me)))
        .serve_with_shutdown(grpc_address, cancellation_token.cancelled())
        .await
        .map_err(|error| GrpcServerError::Transport(error))?;

    tracing::info!("GRPC Server is stopped");

    Ok(())
}
