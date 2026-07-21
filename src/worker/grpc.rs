mod output;
mod session;
mod manager_connection;
mod input;

use crate::common::{CommunicationStreamEither, Config, Me, NodeId};
use crate::worker::domain::WorkerProtocol;
use crate::worker::grpc::api::v1::WorkerEvent;
use crate::worker::grpc::output::output;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tonic::Status;

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

pub(super) type WorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;

pub struct ManagerApiService {
    me: Me,
    manager_sessions: Arc<RwLock<HashMap<NodeId, WorkerIOStream>>>,
    tx: Sender<WorkerProtocol>,
    config: Config,
}

impl ManagerApiService {
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
        tokio::spawn(output(
            me.clone(),
            tx_clone,
            rx,
            manager_sessions_clone,
        ));
        service
    }
}
