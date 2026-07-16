use crate::common::NodeId;
use crate::manager::domain::NodeProtocol;
use crate::manager::grpc::api::v1::{ManagerEvent, WorkerEvent};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tonic::Status;

/**
EitherStream is a stream that can send messages to either a Sender<Result<Message, Status>> or a Sender<Message>.
It is used to send messages to either the gRPC output stream or the gRPC input stream.
*/
pub(super) enum CommunicationStreamEither<A, B> {
    Input(A),
    Output(B),
}

pub(super) type ManagerIOStream =
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

pub(super) type WorkerIOStream =
    CommunicationStreamEither<Sender<Result<WorkerEvent, Status>>, Sender<WorkerEvent>>;
type WorkerIOStreamError = CommunicationStreamEither<Status, WorkerEvent>;

#[async_trait]
pub(super) trait IOStreamExt<Event, Error> {
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

pub(super) async fn handle_common<Event, Error>(
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
