use crate::manager::grpc::api::v1::worker_event;
use crate::manager::grpc::common::v1::Config;
use crate::{
    common::{Me, NodeId},
    manager::{
        domain,
        domain::ClusterNode,
        domain::NodeProtocol,
        grpc::{
            api::v1::{
                manager_api_client::ManagerApiClient, manager_api_server::{ManagerApi, ManagerApiServer}, manager_event::Payload, Connect, ConnectResponse, Heartbeat,
                Leader, ManagerEvent,
                VoteRequest,
                VoteResponse,
                WorkerEvent,
            },
            common::v1::{node, Addr, ClusterState, GetState, Manager, Node, Worker},
        },
    },
};
use async_trait::async_trait;
use std::fmt::{Debug, Formatter};
use std::{collections::HashMap, net::AddrParseError, sync::Arc};
use thiserror::Error;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    sync::RwLock,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
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
*/
enum CommunicationStreamEither<A, B> {
    Input(A),
    Output(B),
}

type ManagerIOStream =
    CommunicationStreamEither<Sender<Result<ManagerEvent, Status>>, Sender<ManagerEvent>>;

type ManagerIOStreamError = CommunicationStreamEither<Status, ManagerEvent>;

impl<E> Debug for CommunicationStreamEither<Status, E>
where
    E: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommunicationStreamEither::Input(sender) => write!(f, "Input: {:?}", sender),
            CommunicationStreamEither::Output(sender) => write!(f, "Output: {:?}", sender),
        }
    }
}

type WorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;
type WorkerIOStreamError = CommunicationStreamEither<Status, WorkerEvent>;

#[async_trait]
trait IOStreamExt<Event, Error> {
    async fn send(&self, event: Event) -> Result<(), Error>;
    fn is_closed(&self) -> bool;
}

#[async_trait]
impl IOStreamExt<ManagerEvent, ManagerIOStreamError> for ManagerIOStream {
    async fn send(&self, event: ManagerEvent) -> Result<(), ManagerIOStreamError> {
        match self {
            ManagerIOStream::Input(sender) => {
                if let Err(e) = sender.send(Ok(event)).await {
                    let result = e.0;
                    if let Err(status) = result {
                        return Err(ManagerIOStreamError::Input(status));
                    }
                }
            }
            ManagerIOStream::Output(sender) => {
                if let Err(e) = sender.send(event).await {
                    return Err(ManagerIOStreamError::Output(e.0));
                }
            }
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        match self {
            ManagerIOStream::Input(sender) => sender.is_closed(),
            ManagerIOStream::Output(sender) => sender.is_closed(),
        }
    }
}

#[async_trait]
impl IOStreamExt<WorkerEvent, WorkerIOStreamError> for WorkerIOStream {
    async fn send(&self, event: WorkerEvent) -> Result<(), WorkerIOStreamError> {
        match self {
            WorkerIOStream::Input(sender) => {
                if let Err(e) = sender.send(Ok(event)).await {
                    let result = e.0;
                    if let Err(status) = result {
                        return Err(WorkerIOStreamError::Input(status));
                    }
                }
            }
            WorkerIOStream::Output(sender) => {
                if let Err(e) = sender.send(event).await {
                    return Err(WorkerIOStreamError::Output(e.0));
                }
            }
        }
        Ok(())
    }

    fn is_closed(&self) -> bool {
        match self {
            WorkerIOStream::Input(sender) => sender.is_closed(),
            WorkerIOStream::Output(sender) => sender.is_closed(),
        }
    }
}

type OpenConnectionStream = ReceiverStream<Result<ManagerEvent, Status>>;
type OpenWorkerConnectionStream = ReceiverStream<Result<WorkerEvent, Status>>;

async fn output(
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
                let guard = config.read().await;
                let (partitions_amount, replication_factor) = guard
                    .partitions_amount
                    .zip(guard.replication_factor)
                    .expect("Partitions and replication factor should be defined");
                drop(guard);
                if manager {
                    new_manager_connection(
                        &me,
                        &tx,
                        &manager_sessions,
                        host,
                        port,
                        partitions_amount,
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
                        config,
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
                    config,
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

async fn handle_common<Event, Error>(
    event_type: &'static str,
    event: impl FnOnce() -> Event,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, impl IOStreamExt<Event, Error>>>>,
    id: NodeId,
) where
    Error: Debug,
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
        let _ = tx.send(NodeProtocol::NodeDisconnected { id }).await;
    } else if let Some(sender) = sessions.read().await.get(&id) {
        if let Err(e) = sender.send(event()).await {
            tracing::error!("Error sending {event_type} to {}: {:?}", id, e);
        }
    }
}

async fn handle_output_leader(
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

async fn handle_output_vote_response(
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

async fn handle_output_vote_request(
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

async fn handle_output_cluster_state(
    tx: &Sender<NodeProtocol>,
    manager_sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: &Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    id: NodeId,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    config: Option<domain::Config>,
) {
    let state = || ClusterState {
        epoch,
        leader_id: leader_id.to_string(),
        nodes: items
            .into_iter()
            .map(|node| match node {
                ClusterNode::Manager {
                    id,
                    host,
                    port,
                    last_heartbeat,
                } => Node {
                    payload: Some(node::Payload::Manager(Manager {
                        id: id.to_string(),
                        addr: Some(Addr { host, port }),
                        last_heartbeat,
                    })),
                },
                ClusterNode::Worker {
                    id,
                    host,
                    port,
                    last_heartbeat,
                    partitions,
                } => Node {
                    payload: Some(node::Payload::Worker(Worker {
                        id: id.to_string(),
                        addr: Some(Addr { host, port }),
                        last_heartbeat,
                        partitions: partitions.into_iter().map(|p| p as u32).collect(),
                    })),
                },
            })
            .collect(),
        config: config.map(|config| {
            Config {
                partitions_amount: config.partitions_amount as u32,
                replication_factor: config.replication_factor as u32,
            }
        }),
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

async fn handle_output_get_cluster_state(
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

async fn handle_output_heartbeat(
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

async fn new_manager_connection(
    me: &Arc<Me>,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    host: String,
    port: u32,
    partitions_amount: usize,
    replication_factor: usize,
) {
    let client = ManagerApiClient::connect(format!("http://{host}:{port}")).await;
    match client {
        Ok(mut client) => {
            tracing::debug!("Connecting to manager");
            let (grpc_output, rx) =
                tokio::sync::mpsc::channel::<ManagerEvent>(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);
            let outbound = ReceiverStream::new(rx);

            let response = client.open_connection(Request::new(outbound)).await;
            match response {
                Ok(response) => {
                    tracing::debug!("Connected to manager");
                    start_communication_with_manager(
                        me,
                        tx,
                        sessions,
                        host,
                        port,
                        grpc_output,
                        response,
                        partitions_amount,
                        replication_factor,
                    )
                    .await;
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

async fn start_communication_with_manager(
    me: &Arc<Me>,
    tx: &Sender<NodeProtocol>,
    sessions: &Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    host: String,
    port: u32,
    grpc_output: Sender<ManagerEvent>,
    response: Response<Streaming<ManagerEvent>>,
    partitions_amount: usize,
    replication_factor: usize,
) {
    let mut input_stream = response.into_inner();
    let sender = ManagerIOStream::Output(grpc_output);
    if sender
        .send(ManagerEvent {
            payload: Some(Payload::Connect(Connect {
                id: me.id.to_string(),
                addr: Some(Addr {
                    host: me.host.clone(),
                    port: me.port,
                }),
                config: Some(Config {
                    partitions_amount: partitions_amount as u32,
                    replication_factor: replication_factor as u32,
                }),
            })),
        })
        .await
        .is_ok()
    {
        if let Ok(Some(ManagerEvent {
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
                input_from_manager(me, input_stream, &id, host, port, tx.clone()).await;
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

async fn input_from_worker<S>(
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

async fn input_from_manager(
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
                                            partitions,
                                        })) => Some(ClusterNode::Worker {
                                            id: id.into(),
                                            host,
                                            port,
                                            last_heartbeat,
                                            partitions: partitions
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

pub struct ManagerApiService {
    me: Arc<Me>,
    manager_sessions: Arc<RwLock<HashMap<NodeId, ManagerIOStream>>>,
    worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    tx: Sender<NodeProtocol>,
    config: Arc<RwLock<crate::common::Config>>,
}
impl ManagerApiService {
    pub fn new(
        (tx, rx): (Sender<NodeProtocol>, Receiver<NodeProtocol>),
        me: Arc<Me>,
        config: Arc<RwLock<crate::common::Config>>,
    ) -> Self {
        let manager_sessions: Arc<RwLock<HashMap<NodeId, ManagerIOStream>>> = Default::default();
        let manager_sessions_clone = manager_sessions.clone();
        let worker_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> = Default::default();
        let worker_sessions_clone = worker_sessions.clone();
        let tx_clone = tx.clone();
        let service = Self {
            me: me.clone(),
            manager_sessions,
            worker_sessions,
            tx,
            config: config.clone(),
        };
        tokio::spawn(output(
            me.clone(),
            tx_clone,
            rx,
            manager_sessions_clone,
            worker_sessions_clone,
            config,
        ));
        service
    }
}

#[tonic::async_trait]
impl ManagerApi for ManagerApiService {
    type OpenConnectionStream = OpenConnectionStream;

    async fn open_connection(
        &self,
        request: Request<Streaming<ManagerEvent>>,
    ) -> Result<Response<Self::OpenConnectionStream>, Status> {
        tracing::info!("Received open_connection request");

        let remote_addr = request.remote_addr();
        let mut input_stream: Streaming<ManagerEvent> = request.into_inner();
        let (grpc_tx, rx) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let manager_sessions = self.manager_sessions.clone();
        let tx = self.tx.clone();
        let me = self.me.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            if let Ok(Some(ManagerEvent {
                payload:
                    Some(Payload::Connect(Connect {
                        id,
                        addr: Some(Addr { host, port }),
                        config:
                            Some(Config {
                                partitions_amount,
                                replication_factor,
                            }),
                    })),
            })) = input_stream.message().await
            {
                let mut guard = config.write().await;
                guard.partitions_amount = Some(partitions_amount as usize);
                guard.replication_factor = Some(replication_factor as usize);
                drop(guard);

                let id: NodeId = id.into();
                manager_sessions
                    .write()
                    .await
                    .insert(id.clone(), ManagerIOStream::Input(grpc_tx.clone()));

                tokio::spawn(async move {
                    if grpc_tx
                        .send(Ok(ManagerEvent {
                            payload: Some(Payload::ConnectResponse(ConnectResponse {
                                id: me.id.to_string(),
                            })),
                        }))
                        .await
                        .is_ok()
                    {
                        input_from_manager(me, input_stream, &id, host, port, tx.clone()).await;
                    }
                    manager_sessions.write().await.remove(&id);
                    tracing::info!("Node {} is disconnected", id);
                    let _ = tx.send(NodeProtocol::NodeDisconnected { id }).await;
                });
            } else {
                tracing::error!("Failed to read Connect message from {:?}", remote_addr);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type OpenWorkerConnectionStream = OpenWorkerConnectionStream;

    async fn open_worker_connection(
        &self,
        request: Request<Streaming<WorkerEvent>>,
    ) -> Result<Response<Self::OpenWorkerConnectionStream>, Status> {
        tracing::info!("Received open_worker_connection request");

        let remote_addr = request.remote_addr();
        let mut input_stream: Streaming<WorkerEvent> = request.into_inner();
        let (grpc_tx, rx): (
            Sender<Result<WorkerEvent, Status>>,
            Receiver<Result<WorkerEvent, Status>>,
        ) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let worker_sessions = self.worker_sessions.clone();
        let tx = self.tx.clone();
        let me = self.me.clone();

        tokio::spawn(async move {
            if let Ok(Some(WorkerEvent {
                payload:
                    Some(worker_event::Payload::Connect(Connect {
                        id,
                        addr: Some(Addr { host, port }),
                        ..
                    })),
            })) = input_stream.message().await
            {
                let id: NodeId = id.into();
                worker_sessions
                    .write()
                    .await
                    .insert(id.clone(), WorkerIOStream::Input(grpc_tx.clone()));

                tokio::spawn(async move {
                    if grpc_tx
                        .send(Ok(WorkerEvent {
                            payload: Some(worker_event::Payload::ConnectResponse(
                                ConnectResponse {
                                    id: me.id.to_string(),
                                },
                            )),
                        }))
                        .await
                        .is_ok()
                    {
                        input_from_worker(input_stream, &id, host, port, tx.clone()).await;
                    }
                    worker_sessions.write().await.remove(&id);
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
    config: Arc<RwLock<crate::common::Config>>,
    me: Arc<Me>,
    channel: (Sender<NodeProtocol>, Receiver<NodeProtocol>),
    cancellation_token: CancellationToken,
) -> Result<(), GrpcServerError> {
    let guard = config.read().await;
    let grpc_address = format!("127.0.0.1:{}", guard.grpc_port).as_str().parse()?;
    drop(guard);

    tracing::info!("GRPC Server is starting at {}", grpc_address);

    Server::builder()
        .add_service(ManagerApiServer::new(ManagerApiService::new(
            channel, me, config,
        )))
        .serve_with_shutdown(grpc_address, cancellation_token.cancelled())
        .await
        .map_err(|error| GrpcServerError::Transport(error))?;

    tracing::info!("GRPC Server is stopped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::grpc::api::v1::worker_event;
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
                partitions_amount: 12,
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
        assert_eq!(cluster_state.config.as_ref().map(|c| c.partitions_amount), Some(12));
        assert_eq!(cluster_state.config.as_ref().map(|c| c.replication_factor), Some(4));
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
                partitions_amount: 8,
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
        assert_eq!(cluster_state.config.as_ref().map(|c| c.partitions_amount), Some(8));
        assert_eq!(cluster_state.config.as_ref().map(|c| c.replication_factor), Some(2));
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
}
