mod input;
mod manager_connection;
mod output;
mod session;

use crate::common::{CommunicationStreamEither, Config, Me, NodeId};
use crate::conversions::manager_api::v1::WorkerEvent;
use crate::conversions::worker_api::v1::worker_api_server::WorkerApi;
use crate::conversions::worker_api::v1::ClientEvent;
use crate::conversions::worker_api::v1::WorkerEvent as ClientApiWorkerEvent;
use crate::worker::domain::WorkerProtocol;
use crate::worker::grpc::output::output;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

const GRPC_CONNECTION_CHANNEL_BUFFER_SIZE: usize = 32;

pub(super) type WorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;

type OpenWorkerConnectionStream = ReceiverStream<Result<ClientApiWorkerEvent, Status>>;
type OpenClientConnectionStream = ReceiverStream<Result<ClientEvent, Status>>;

pub struct WorkerApiService {
    me: Me,
    manager_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    tx: Sender<WorkerProtocol>,
    config: Config,
}

impl WorkerApiService {
    pub fn new(
        (tx, rx): (Sender<WorkerProtocol>, Receiver<WorkerProtocol>),
        me: Me,
        config: Config,
    ) -> Self {
        let manager_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>> = Default::default();
        let manager_sessions_clone = manager_sessions.clone();
        let tx_clone = tx.clone();
        let service = Self {
            me: me.clone(),
            manager_sessions,
            tx,
            config,
        };
        tokio::spawn(output(me.clone(), tx_clone, rx, manager_sessions_clone));
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
        todo!()
    }
}
