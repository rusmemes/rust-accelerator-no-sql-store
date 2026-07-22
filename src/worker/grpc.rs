mod input;
mod manager_connection;
mod output;
mod session;
mod worker_connection;

use crate::{
    common::{CommunicationStreamEither, Config, Me, NodeId},
    conversions::{
        common::v1::Addr,
        manager_api::v1::WorkerEvent,
        worker_api::v1::{
            worker_api_server::WorkerApi, worker_event, ClientEvent, Connect,
            ConnectResponse, WorkerEvent as ClientApiWorkerEvent,
        },
    },
    worker::{
        domain::WorkerProtocol,
        grpc::{input::input_from_worker, output::output},
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

const GRPC_CONNECTION_CHANNEL_BUFFER_SIZE: usize = 32;

pub(super) type WorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;

pub(super) type ClientApiWorkerIOStream = CommunicationStreamEither<
    Sender<Result<ClientApiWorkerEvent, Status>>,
    Sender<ClientApiWorkerEvent>,
>;

type OpenWorkerConnectionStream = ReceiverStream<Result<ClientApiWorkerEvent, Status>>;
type OpenClientConnectionStream = ReceiverStream<Result<ClientEvent, Status>>;

pub struct WorkerApiService {
    me: Me,
    worker_sessions: Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>>,
    tx: Sender<WorkerProtocol>,
}

impl WorkerApiService {
    pub fn new((tx, rx): (Sender<WorkerProtocol>, Receiver<WorkerProtocol>), me: Me) -> Self {
        let worker_sessions: Arc<RwLock<HashMap<NodeId, ClientApiWorkerIOStream>>> =
            Default::default();
        let worker_sessions_clone = worker_sessions.clone();
        let tx_clone = tx.clone();
        let service = Self {
            me: me.clone(),
            worker_sessions,
            tx,
        };
        tokio::spawn(output(
            me.clone(),
            tx_clone,
            rx,
            Arc::new(RwLock::new(HashMap::new())),
            worker_sessions_clone,
        ));
        service
    }
}

#[tonic::async_trait]
impl WorkerApi for WorkerApiService {
    type OpenClientConnectionStream = OpenClientConnectionStream;

    async fn open_client_connection(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::OpenClientConnectionStream>, Status> {
        todo!()
    }

    type OpenWorkerConnectionStream = OpenWorkerConnectionStream;

    async fn open_worker_connection(
        &self,
        request: Request<Streaming<ClientApiWorkerEvent>>,
    ) -> Result<Response<Self::OpenWorkerConnectionStream>, Status> {
        tracing::info!("Received open_worker_connection request");

        let remote_addr = request.remote_addr();
        let mut input_stream: Streaming<ClientApiWorkerEvent> = request.into_inner();

        let (grpc_tx, rx): (
            Sender<Result<ClientApiWorkerEvent, Status>>,
            Receiver<Result<ClientApiWorkerEvent, Status>>,
        ) = tokio::sync::mpsc::channel(GRPC_CONNECTION_CHANNEL_BUFFER_SIZE);

        let worker_sessions = self.worker_sessions.clone();
        let tx = self.tx.clone();
        let me = self.me.clone();

        tokio::spawn(async move {
            if let Ok(Some(ClientApiWorkerEvent {
                payload:
                    Some(worker_event::Payload::Connect(Connect {
                        id,
                        addr: Some(Addr { host, port }),
                    })),
            })) = input_stream.message().await
            {
                let id: NodeId = id.into();
                worker_sessions
                    .write()
                    .await
                    .insert(id.clone(), ClientApiWorkerIOStream::Input(grpc_tx.clone()));

                tokio::spawn(async move {
                    if grpc_tx
                        .send(Ok(ClientApiWorkerEvent {
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
                    let _ = tx.send(WorkerProtocol::NodeDisconnected { id }).await;
                });
            } else {
                tracing::error!("Failed to read Connect message from {:?}", remote_addr);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
