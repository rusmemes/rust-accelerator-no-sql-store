use crate::common::{now_millis, Config, Me};
use crate::manager::domain::{ClusterState, Heartbeat, NodeProtocol};
use cluster_state::{handle_cluster_state, handle_get_cluster_state};
use connection::{handle_new_connection, handle_node_disconnected};
use election::{
    handle_leader, handle_vote_request, handle_vote_response, start_election_if_needed, Election,
};
use heartbeat::{handle_heartbeat, heartbeats};
use partitions::worker_partitions;
use rand::random_range;
use std::ops::Range;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};
use tokio_util::sync::CancellationToken;

mod cluster_state;
mod connection;
mod election;
mod heartbeat;
mod partitions;
mod state;

use state::{Node, State};

// to make nodes trying to start elections at different times, we randomize the election timeout interval
// so that the elections are not all started at the same time
// the idea comes from https://www.studocu.com/en-us/document/university-of-southern-california/database-systems/raft-atc14-this-description/146541342?utm_source=chatgpt.com&sid=97f67133-a2c0-4139-90bd-dabaf62ce79f1783977310
const RANDOMIZED_ELECTION_TIMEOUT_INTERVAL: Range<u64> = 500..1000;
fn get_random_number() -> u64 {
    random_range(RANDOMIZED_ELECTION_TIMEOUT_INTERVAL)
}

#[derive(Debug)]
struct ManagerService {
    me: Arc<Me>,
    state: Option<State>,
    elections: BTreeMap<u64, Election>,
    config: Arc<RwLock<Config>>,
}

impl ManagerService {
    pub fn new(me: Arc<Me>, config: Arc<RwLock<Config>>) -> Self {
        Self {
            me,
            state: Default::default(),
            elections: Default::default(),
            config,
        }
    }

    async fn tick(&mut self, output: &mut Vec<NodeProtocol>) {
        if let Some(state) = self.state.as_mut() {
            heartbeats(state, output, &self.me);
            start_election_if_needed(state, &mut self.elections, &self.me, output);
            let config = self.config.read().await;
            let replication_factor = config.replication_factor.expect("required and has default");
            drop(config);
            worker_partitions(
                state,
                output,
                &self.me,
                replication_factor,
            );
        }
        tracing::debug!("state: {:?}", self.state);
        tracing::debug!("elections: {:?}", self.elections);
    }

    async fn process(&mut self, msg: NodeProtocol, output: &mut Vec<NodeProtocol>) {
        if let Some(state) = self.state.as_mut() {
            match msg {
                NodeProtocol::NewConnection {
                    id,
                    host,
                    port,
                    manager,
                } => handle_new_connection(output, state, id, host, port, &self.me, manager),
                NodeProtocol::Heartbeat {
                    heartbeat: Heartbeat { id, ts },
                    ..
                } => handle_heartbeat(output, state, id, ts, &self.me),
                NodeProtocol::GetClusterState { id } => {
                    handle_get_cluster_state(output, state, id, &self.config).await
                }
                NodeProtocol::ClusterState {
                    state:
                        ClusterState {
                            epoch,
                            leader_id,
                            items,
                            config: _,
                        },
                    ..
                } => handle_cluster_state(output, state, epoch, leader_id, items),
                NodeProtocol::VoteRequest { id, epoch, ts } => {
                    tracing::info!("VoteRequest: {:?} {:?}", id, epoch);
                    handle_vote_request(output, state, id, epoch, ts, &mut self.elections);
                }
                NodeProtocol::VoteResponse { id, leader_id, ts } => {
                    tracing::info!("VoteResponse: {:?} {:?}", id, leader_id);
                    handle_vote_response(
                        output,
                        state,
                        id,
                        leader_id,
                        ts,
                        &self.me,
                        &mut self.elections,
                    );
                }
                NodeProtocol::Leader { id, epoch, ts } => {
                    handle_leader(output, state, id, epoch, ts, &self.me, &mut self.elections);
                }
                NodeProtocol::NodeDisconnected { id } => {
                    handle_node_disconnected(state, id, &self.me);
                }
            }
        }

        self.tick(output).await
    }

    async fn get_init_messages(&mut self) -> Vec<NodeProtocol> {
        if self.state.is_some() {
            vec![]
        } else if let Some((manager_host, manager_port)) =
            &self.config.read().await.manager_host_port
        {
            let mut nodes = HashMap::new();
            nodes.insert(
                self.me.id.clone(),
                Node::Manager {
                    host: self.me.host.clone(),
                    port: self.me.port,
                    last_heartbeat: now_millis(),
                },
            );
            self.state = Some(State {
                epoch: None,
                elected_leader_id: None,
                nodes,
                workers_with_calculated_partitions: Default::default(),
            });
            vec![NodeProtocol::NewConnection {
                id: None,
                host: manager_host.clone(),
                port: *manager_port as u32,
                manager: true,
            }]
        } else {
            let mut nodes = HashMap::new();
            nodes.insert(
                self.me.id.clone(),
                Node::Manager {
                    host: self.me.host.clone(),
                    port: self.me.port,
                    last_heartbeat: now_millis(),
                },
            );
            self.state = Some(State {
                epoch: Some(0),
                elected_leader_id: None,
                nodes,
                workers_with_calculated_partitions: Default::default(),
            });
            vec![]
        }
    }
}

/// Runs the manager service event loop.
///
/// The service owns the in-memory cluster state machine. It consumes protocol
/// messages from gRPC, emits outbound protocol messages, and performs periodic
/// heartbeat and election checks.
pub async fn start_service(
    me: Arc<Me>,
    config: Arc<RwLock<Config>>,
    (tx, mut rx): (Sender<NodeProtocol>, Receiver<NodeProtocol>),
    cancellation_token: CancellationToken,
) {
    let mut service = ManagerService::new(me, config);
    for msg in service.get_init_messages().await {
        if let Err(e) = tx.send(msg).await {
            tracing::error!("Error sending response: {}", e);
            return;
        }
    }

    tracing::info!("Manager service started");
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut output = vec![];
    loop {
        select! {
            biased;
            _ = cancellation_token.cancelled() => {
                tracing::info!("Manager service stopped");
                break;
            }
            node_protocol = rx.recv() => {
                if let Some(message) = node_protocol {
                    tracing::debug!("input: {:?}", message);
                    service.process(message, &mut output).await;
                    for msg in output.drain(..) {
                        if let Err(e) = tx.send(msg).await {
                            tracing::error!("Error sending response: {}", e);
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                service.tick(&mut output).await;
                for msg in output.drain(..) {
                    if let Err(e) = tx.send(msg).await {
                        tracing::error!("Error sending response: {}", e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
