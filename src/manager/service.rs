use crate::common::{now_millis, Config, Me, NodeId};
use crate::manager::domain::{ClusterNode, ClusterState, Heartbeat, NodeProtocol};
use rand::random_range;
use std::collections::BTreeSet;
use std::ops::Range;
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
enum Node {
    Manager {
        host: String,
        port: u32,
        last_heartbeat: u64,
    },
    Worker {
        host: String,
        port: u32,
        last_heartbeat: u64,
        partitions: Vec<u16>,
    },
}

impl Node {
    fn is_manager(&self) -> bool {
        matches!(self, Node::Manager { .. })
    }

    fn is_worker(&self) -> bool {
        matches!(self, Node::Worker { .. })
    }

    fn last_heartbeat_mut(&mut self) -> &mut u64 {
        match self {
            Node::Manager { last_heartbeat, .. } => last_heartbeat,
            Node::Worker { last_heartbeat, .. } => last_heartbeat,
        }
    }
}

#[derive(Debug)]
struct State {
    epoch: Option<u64>,
    elected_leader_id: Option<NodeId>,
    nodes: HashMap<NodeId, Node>,
    workers_with_calculated_partitions: BTreeSet<NodeId>,
}

#[derive(Debug)]
enum Election {
    Mine { ts: u64, approvers: HashSet<NodeId> },
    Other { ts: u64, candidate_id: NodeId },
}

impl Election {
    fn ts(&self) -> u64 {
        match self {
            Election::Mine { ts, .. } => *ts,
            Election::Other { ts, .. } => *ts,
        }
    }
}

// to make nodes trying to start elections at different times, we randomize the election timeout interval
// so that the elections are not all started at the same time
// the idea comes from https://www.studocu.com/en-us/document/university-of-southern-california/database-systems/raft-atc14-this-description/146541342?utm_source=chatgpt.com&sid=97f67133-a2c0-4139-90bd-dabaf62ce79f1783977310
const RANDOMIZED_ELECTION_TIMEOUT_INTERVAL: Range<u64> = 500..1000;
const HEARTBEAT_INTERVAL_MS: u64 = 200;

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
            let partitions_amount = config.partitions_amount.expect("required and has default");
            let replication_factor = config.replication_factor.expect("required and has default");
            drop(config);
            worker_partitions(
                state,
                output,
                &self.me,
                partitions_amount,
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

fn worker_partitions(
    state: &mut State,
    output: &mut Vec<NodeProtocol>,
    me: &Me,
    partitions_amount: usize,
    replication_factor: usize,
) {
    if state.elected_leader_id.as_ref() == Some(&me.id) {
        let current_keys = state
            .nodes
            .iter()
            .filter(|(_, v)| v.is_worker())
            .map(|(k, _)| k)
            .collect::<BTreeSet<_>>();

        if state.workers_with_calculated_partitions.len() != current_keys.len()
            || current_keys
                != state
                    .workers_with_calculated_partitions
                    .iter()
                    .collect::<BTreeSet<_>>()
        {
            let vec = current_keys
                .into_iter()
                .map(|it| it.clone())
                .collect::<Vec<_>>();

            calculate_and_add_partitions(state, partitions_amount, replication_factor, &vec);
            deduplicate_partitions(state);

            let workers_state =
                create_new_workers_state(state, partitions_amount, replication_factor);

            state.workers_with_calculated_partitions = vec.into_iter().collect();

            for id in state.nodes.keys().filter(|&key| *key != me.id) {
                output.push(NodeProtocol::ClusterState {
                    recipient_id: id.clone(),
                    state: workers_state.clone(),
                });
            }
        }
    }
}

fn create_new_workers_state(
    state: &mut State,
    partitions_amount: usize,
    replication_factor: usize,
) -> ClusterState {
    let items: Vec<ClusterNode> = state
        .nodes
        .iter()
        .filter_map(|(id, node)| match node {
            Node::Manager { .. } => None,
            Node::Worker {
                host,
                port,
                last_heartbeat,
                partitions,
            } => Some(ClusterNode::Worker {
                id: id.clone(),
                host: host.clone(),
                port: *port,
                last_heartbeat: *last_heartbeat,
                partitions: partitions.clone(),
            }),
        })
        .collect();

    ClusterState {
        config: Some(crate::manager::domain::Config {
            partitions_amount,
            replication_factor,
        }),
        epoch: state
            .epoch
            .expect("present as elected leader id is also present"),
        leader_id: state
            .elected_leader_id
            .clone()
            .expect("existing checked above"),
        items: items.clone(),
    }
}

fn deduplicate_partitions(state: &mut State) {
    let mut seen = HashSet::new();
    state
        .nodes
        .values_mut()
        .filter(|node| node.is_worker())
        .for_each(|node| {
            if let Node::Worker { partitions, .. } = node {
                partitions.retain(|partition| seen.insert(*partition));
                seen.clear();
            }
        });
}

fn calculate_and_add_partitions(
    state: &mut State,
    partitions_amount: usize,
    replication_factor: usize,
    vec: &Vec<NodeId>,
) {
    for partition in 0..partitions_amount {
        let master_partition_index = partition % vec.len();
        for replica in 0..replication_factor {
            let index = calc_replica_index(vec.len(), master_partition_index, replica);
            let id = vec.get(index).unwrap();
            let node = state.nodes.get_mut(id).unwrap();
            if let Node::Worker { partitions, .. } = node {
                partitions.push(partition as u16);
            }
        }
    }
}

fn calc_replica_index(
    total_amount: usize,
    master_partition_index: usize,
    mut replica: usize,
) -> usize {
    while replica >= total_amount {
        replica -= total_amount;
    }
    let mut index = master_partition_index;
    if replica > index {
        replica -= index;
        index = total_amount - replica;
    } else {
        index -= replica;
    }
    index
}

fn heartbeats(state: &mut State, output: &mut Vec<NodeProtocol>, me: &Me) {
    if let Some(Node::Manager { last_heartbeat, .. }) = state.nodes.get_mut(&me.id) {
        let now = now_millis();
        if *last_heartbeat + HEARTBEAT_INTERVAL_MS <= now {
            *last_heartbeat = now;
            output.extend(
                state
                    .nodes
                    .iter()
                    .filter(|(key, node)| **key != me.id && node.is_manager())
                    .map(|(key, _)| NodeProtocol::Heartbeat {
                        recipient_id: key.clone(),
                        heartbeat: Heartbeat {
                            id: me.id.clone(),
                            ts: now,
                        },
                    }),
            );
        }

        if state.elected_leader_id.is_some() && state.elected_leader_id.as_ref() != Some(&me.id) {
            if let Some(Node::Manager { last_heartbeat, .. }) = state
                .nodes
                .get_mut(&state.elected_leader_id.as_ref().unwrap())
            {
                if *last_heartbeat + get_random_number() < now {
                    state.elected_leader_id = None;
                }
            }
        }
    }
}

fn start_election_if_needed(
    state: &mut State,
    elections: &mut BTreeMap<u64, Election>,
    me: &Me,
    output: &mut Vec<NodeProtocol>,
) {
    if state.elected_leader_id.is_none()
        && state.nodes.len() > 1
        && let Some(epoch) = state.epoch
    {
        let curr_ts = now_millis();
        let election = elections.last_key_value();
        let start_new = if let Some((_, last_election)) = election {
            match last_election {
                Election::Mine { ts, .. } => ts + get_random_number() < curr_ts,
                Election::Other { ts, .. } => ts + get_random_number() < curr_ts,
            }
        } else {
            true
        };

        if start_new {
            let next_epoch = election
                .map(|(last_epoch, _)| max(*last_epoch + 1, epoch + 1))
                .unwrap_or_else(|| epoch + 1);

            let election = Election::Mine {
                ts: curr_ts,
                approvers: HashSet::new(),
            };

            elections.clear();
            elections.insert(next_epoch, election);
            state
                .nodes
                .iter()
                .filter(|(key, value)| *key != &me.id && value.is_manager())
                .for_each(|(node_id, _)| {
                    output.push(NodeProtocol::VoteRequest {
                        id: node_id.clone(),
                        epoch: next_epoch,
                        ts: curr_ts,
                    });
                });

            tracing::info!("New election started: {:?}", elections);
        }
    }
}

fn handle_node_disconnected(state: &mut State, id: NodeId, me: &Me) {
    if let Some(_) = state.nodes.remove(&id) {
        tracing::info!("Node disconnected: {:?}", id);
        if Some(id) == state.elected_leader_id {
            state.elected_leader_id = None;
        }
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
    }
}

fn handle_leader(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    epoch: u64,
    ts: u64,
    me: &Me,
    elections: &mut BTreeMap<u64, Election>,
) {
    if state.epoch < Some(epoch) {
        elections.clear();
        if let Some(Node::Manager { last_heartbeat, .. }) = state.nodes.get_mut(&id) {
            *last_heartbeat = ts;
            state.elected_leader_id = Some(id);
            state.epoch = Some(epoch);
            tracing::info!("Me: {:?}", me);
            tracing::info!("Leader elected, State: {:?}", state);
        } else {
            output.extend(
                state
                    .nodes
                    .iter()
                    .filter(|(key, node)| *key != &me.id && node.is_manager())
                    .map(|(key, _)| NodeProtocol::GetClusterState { id: key.clone() }),
            );
        }
    }
}

fn handle_vote_response(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    leader_id: NodeId,
    ts: u64,
    me: &Me,
    elections: &mut BTreeMap<u64, Election>,
) {
    let approver = if let Some((
        epoch,
        Election::Mine {
            ts: election_ts, ..
        },
    )) = elections.last_key_value()
        && *election_ts == ts
        && &leader_id == &me.id
    {
        Some((*epoch, id))
    } else {
        None
    };

    if let Some((epoch, approver)) = approver {
        if let Some(Election::Mine { approvers, .. }) = elections.get_mut(&epoch) {
            approvers.insert(approver);
            let manager_count = state
                .nodes
                .iter()
                .filter(|(node_id, node)| *node_id != &me.id && node.is_manager())
                .count();

            if approvers.len() == manager_count
                && state
                    .nodes
                    .iter()
                    .filter(|(node_id, node)| *node_id != &me.id && node.is_manager())
                    .all(|(node_id, _)| approvers.contains(node_id))
            {
                state.elected_leader_id = Some(me.id.clone());
                state.epoch = Some(epoch);

                state
                    .nodes
                    .keys()
                    .filter(|&key| *key != me.id)
                    .for_each(|key| {
                        output.push(NodeProtocol::Leader {
                            id: key.clone(),
                            epoch,
                            ts,
                        });
                    });

                elections.clear();
                tracing::info!("Me: {:?}", me);
                tracing::info!("Leader elected, State: {:?}", state);
            } else {
                tracing::info!("Leader not elected: {:?}", epoch);
            }
        }
    } else if !state.nodes.contains_key(&leader_id) {
        output.extend(
            state
                .nodes
                .iter()
                .filter(|(key, node)| *key != &me.id && node.is_manager())
                .map(|(key, _)| NodeProtocol::GetClusterState { id: key.clone() }),
        );
    }
}

fn handle_vote_request(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    epoch: u64,
    ts: u64,
    elections: &mut BTreeMap<u64, Election>,
) {
    if state.epoch < Some(epoch) {
        let add_new = if let Some((last_epoch, last_election)) = elections.last_key_value() {
            let res = epoch > *last_epoch || ts < last_election.ts();
            if res {
                elections.clear();
            }
            res
        } else {
            true
        };
        if add_new {
            elections.insert(
                epoch,
                Election::Other {
                    ts,
                    candidate_id: id.clone(),
                },
            );
            output.push(NodeProtocol::VoteResponse {
                id: id.clone(),
                leader_id: id,
                ts,
            });
        } else if let Some(Election::Other { ts, candidate_id }) = elections.get(&epoch) {
            output.push(NodeProtocol::VoteResponse {
                id,
                leader_id: candidate_id.clone(),
                ts: *ts,
            });
        }
    }
}

fn handle_cluster_state(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
) {
    let accept: bool = if state.epoch.is_none() || state.epoch < Some(epoch) {
        state.epoch = Some(epoch);
        state.elected_leader_id = Some(leader_id);
        true
    } else if state.epoch == Some(epoch) && state.elected_leader_id == Some(leader_id) {
        true
    } else {
        false
    };

    if accept {
        for item in items {
            match item {
                ClusterNode::Manager {
                    id,
                    host,
                    port,
                    last_heartbeat,
                } => {
                    if let Some(Node::Manager {
                        last_heartbeat: node_last_heartbeat,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }
                    } else {
                        output.push(NodeProtocol::NewConnection {
                            id: None,
                            host,
                            port,
                            manager: true,
                        });
                    }
                }
                ClusterNode::Worker {
                    id,
                    host,
                    port,
                    last_heartbeat,
                    partitions,
                } => {
                    if let Some(Node::Worker {
                        last_heartbeat: node_last_heartbeat,
                        partitions: node_partitions,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }

                        *node_partitions = partitions;
                    } else {
                        state.nodes.insert(
                            id,
                            Node::Worker {
                                host,
                                port,
                                last_heartbeat,
                                partitions,
                            },
                        );
                    }
                }
            }
        }
    }
}

async fn handle_get_cluster_state(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    config: &RwLock<Config>,
) {
    if let Some((epoch, leader_id)) = state.epoch.zip(state.elected_leader_id.clone()) {
        let guard = config.read().await;
        let (pa, rf) = guard
            .partitions_amount
            .zip(guard.replication_factor)
            .expect("Partitions and replication factor must be set");
        output.push(NodeProtocol::ClusterState {
            recipient_id: id.clone(),
            state: ClusterState {
                config: Some(crate::manager::domain::Config {
                    partitions_amount: pa,
                    replication_factor: rf,
                }),
                epoch,
                leader_id,
                items: state
                    .nodes
                    .iter()
                    .map(|(id, node)| match node {
                        Node::Manager {
                            host,
                            port,
                            last_heartbeat,
                        } => ClusterNode::Manager {
                            id: id.clone(),
                            host: host.clone(),
                            port: *port,
                            last_heartbeat: *last_heartbeat,
                        },
                        Node::Worker {
                            host,
                            port,
                            last_heartbeat,
                            partitions,
                        } => ClusterNode::Worker {
                            id: id.clone(),
                            host: host.clone(),
                            port: *port,
                            last_heartbeat: *last_heartbeat,
                            partitions: partitions.clone(),
                        },
                    })
                    .collect(),
            },
        });
    }
}

fn handle_heartbeat(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    ts: u64,
    me: &Me,
) {
    match state.nodes.get_mut(&id) {
        None => {
            output.extend(
                state
                    .nodes
                    .iter()
                    .filter(|(key, node)| *key != &me.id && node.is_manager())
                    .map(|(key, _)| NodeProtocol::GetClusterState { id: key.clone() }),
            );
        }
        Some(node) => {
            *node.last_heartbeat_mut() = ts;
            if state.elected_leader_id.as_ref() == Some(&me.id) {
                output.extend(
                    state
                        .nodes
                        .iter()
                        .filter(|(key, node)| *key != &id && *key != &me.id && node.is_manager())
                        .map(|(key, _)| NodeProtocol::Heartbeat {
                            recipient_id: key.clone(),
                            heartbeat: Heartbeat { id: id.clone(), ts },
                        }),
                );
            }
        }
    }
}

fn handle_new_connection(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: Option<NodeId>,
    host: String,
    port: u32,
    me: &Me,
    manager: bool,
) {
    if let Some(id) = id {
        tracing::info!("New connection: {:?}", id);
        state.nodes.insert(
            id.clone(),
            if manager {
                if state.elected_leader_id.is_none()
                    || state.elected_leader_id.as_ref() != Some(&me.id)
                {
                    output.push(NodeProtocol::GetClusterState { id });
                }
                Node::Manager {
                    host,
                    port,
                    last_heartbeat: now_millis(),
                }
            } else {
                Node::Worker {
                    host,
                    port,
                    last_heartbeat: now_millis(),
                    partitions: vec![], // will be filled on next tick
                }
            },
        );
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
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
mod tests;
