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
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::RwLock;

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

    fn shared_config(manager_host_port: Option<(String, u16)>) -> Arc<RwLock<Config>> {
        Arc::new(RwLock::new(Config {
            grpc_port: 8080,
            self_host_port: ("127.0.0.1".to_string(), 7000),
            manager_host_port,
            partitions_amount: Some(6),
            replication_factor: Some(3),
        }))
    }

    fn service(me: Arc<Me>) -> (ManagerService, Arc<RwLock<Config>>) {
        let config = shared_config(Some(("manager.local".to_string(), 9000)));
        (ManagerService::new(me, config.clone()), config)
    }

    fn fresh_node(me: &Me, last_heartbeat: u64) -> Node {
        Node::Manager {
            host: me.host.clone(),
            port: me.port,
            last_heartbeat,
        }
    }

    fn node(host: &str, port: u32, last_heartbeat: u64) -> Node {
        Node::Manager {
            host: host.to_string(),
            port,
            last_heartbeat,
        }
    }

    fn worker_node(host: &str, port: u32, last_heartbeat: u64, partitions: Vec<u16>) -> Node {
        Node::Worker {
            host: host.to_string(),
            port,
            last_heartbeat,
            partitions,
        }
    }

    fn partitions_for_worker(state: &State, id: &NodeId) -> Vec<u16> {
        match state.nodes.get(id).expect("worker exists") {
            Node::Worker { partitions, .. } => partitions.clone(),
            _ => panic!("unexpected node type"),
        }
    }

    #[tokio::test]
    async fn init_with_manager_connection_requests_connection_and_sets_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let (mut service, _config) = service(me.clone());

        let output = service.get_init_messages().await;

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::NewConnection {
                id: _,
                host,
                port,
                manager: true,
            }] if host == "manager.local" && *port == 9000
        ));

        let state = service.state.as_ref().expect("state initialized");
        assert_eq!(state.epoch, None);
        assert_eq!(state.elected_leader_id, None);
        assert!(state.nodes.contains_key(&me.id));
    }

    #[tokio::test]
    async fn init_without_manager_starts_as_epoch_zero() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let (mut service, config) = service(me.clone());
        config.write().await.manager_host_port = None;

        let output = service.get_init_messages().await;

        assert!(output.is_empty());

        let state = service.state.as_ref().expect("state initialized");
        assert_eq!(state.epoch, Some(0));
        assert_eq!(state.elected_leader_id, None);
        assert_eq!(state.nodes.len(), 1);
    }

    #[tokio::test]
    async fn new_connection_adds_node_and_requests_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: None,
            nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service
            .process(
                NodeProtocol::NewConnection {
                    id: Some(peer_id.clone()),
                    host: "peer.local".to_string(),
                    port: 9001,
                    manager: true,
                },
                &mut output,
            )
            .await;

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &peer_id
        ));

        let state = service.state.as_ref().expect("state exists");
        assert!(state.nodes.contains_key(&peer_id));
    }

    #[tokio::test]
    async fn get_cluster_state_returns_current_cluster_snapshot() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, config) = service(me.clone());
        {
            let mut guard = config.write().await;
            guard.partitions_amount = Some(12);
            guard.replication_factor = Some(4);
        }
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(3),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_id.clone(),
                    Node::Manager {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: 120,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service
            .process(
                NodeProtocol::GetClusterState {
                    id: peer_id.clone(),
                },
                &mut output,
            )
            .await;

        let cluster_state = match output.as_slice() {
            [
                NodeProtocol::ClusterState {
                    recipient_id,
                    state,
                },
            ] => {
                assert_eq!(recipient_id, &peer_id);
                state
            }
            other => panic!("unexpected output: {:?}", other),
        };

        assert_eq!(cluster_state.epoch, 3);
        assert_eq!(cluster_state.leader_id, me.id);
        let config = cluster_state.config.clone().unwrap();
        assert_eq!(config.partitions_amount, 12);
        assert_eq!(config.replication_factor, 4);
        assert_eq!(cluster_state.items.len(), 2);
    }

    #[tokio::test]
    async fn get_cluster_state_returns_worker_items_with_partitions() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let worker_id = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(3),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    worker_id.clone(),
                    worker_node("worker.local", 9100, now - 5, vec![4, 2, 9]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_get_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            worker_id.clone(),
            config.as_ref(),
        )
        .await;

        let cluster_state = match output.as_slice() {
            [
                NodeProtocol::ClusterState {
                    recipient_id,
                    state,
                },
            ] => {
                assert_eq!(recipient_id, &worker_id);
                state
            }
            other => panic!("unexpected output: {:?}", other),
        };

        let worker_item = cluster_state
            .items
            .iter()
            .find(|item| matches!(item, ClusterNode::Worker { id, .. } if id == &worker_id))
            .expect("worker node present");

        assert!(matches!(
            worker_item,
            ClusterNode::Worker {
                id,
                host,
                port,
                last_heartbeat,
                partitions,
            } if id == &worker_id
                && host == "worker.local"
                && *port == 9100
                && *last_heartbeat == now - 5
                && partitions == &vec![4, 2, 9]
        ));
        assert_eq!(cluster_state.config.clone().unwrap().partitions_amount, 6);
        assert_eq!(cluster_state.config.clone().unwrap().replication_factor, 3);
    }

    #[tokio::test]
    async fn heartbeat_from_unknown_node_requests_cluster_state_from_peers() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer_id.clone(), node("peer.local", 9001, 0)),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service
            .process(
                NodeProtocol::Heartbeat {
                    recipient_id: me.id.clone(),
                    heartbeat: Heartbeat {
                        id: node_id("33333333-3333-3333-3333-333333333333"),
                        ts: 42,
                    },
                },
                &mut output,
            )
            .await;

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &peer_id
        ));
    }

    #[tokio::test]
    async fn heartbeat_from_known_peer_is_forwarded_only_when_we_are_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer_one.clone(), node("peer-one.local", 9001, 0)),
                (peer_two.clone(), node("peer-two.local", 9002, 0)),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut forwarded = vec![];
        handle_heartbeat(
            &mut forwarded,
            service.state.as_mut().unwrap(),
            peer_one.clone(),
            42,
            &me,
        );

        assert!(matches!(
            forwarded.as_slice(),
            [NodeProtocol::Heartbeat { recipient_id, heartbeat }]
                if recipient_id == &peer_two
                    && heartbeat.id == peer_one
                    && heartbeat.ts == 42
        ));

        service.state.as_mut().unwrap().elected_leader_id = Some(peer_two.clone());

        let mut not_forwarded = vec![];
        handle_heartbeat(
            &mut not_forwarded,
            service.state.as_mut().unwrap(),
            peer_one.clone(),
            43,
            &me,
        );

        assert!(not_forwarded.is_empty());
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&peer_one)
                .unwrap()
                .last_heartbeat_mut(),
            43
        );
    }

    #[tokio::test]
    async fn heartbeat_from_worker_updates_worker_without_forwarding() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let worker = node_id("22222222-2222-2222-2222-222222222222");
        let manager_peer = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    worker.clone(),
                    worker_node("worker.local", 9100, 12, vec![1, 2]),
                ),
                (manager_peer.clone(), node("manager.local", 9001, 0)),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_heartbeat(
            &mut output,
            service.state.as_mut().unwrap(),
            worker.clone(),
            44,
            &me,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::Heartbeat { recipient_id, heartbeat }]
                if recipient_id == &manager_peer && heartbeat.id == worker && heartbeat.ts == 44
        ));
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&worker)
                .unwrap()
                .last_heartbeat_mut(),
            44
        );
    }

    #[tokio::test]
    async fn vote_request_adds_new_election_reuses_candidate_and_ignores_stale_epochs() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(0),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_one.clone(),
                    Node::Manager {
                        host: "peer-one.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
                (
                    peer_two.clone(),
                    Node::Manager {
                        host: "peer-two.local".to_string(),
                        port: 9002,
                        last_heartbeat: now,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut first = vec![];
        service
            .process(
                NodeProtocol::VoteRequest {
                    id: peer_one.clone(),
                    epoch: 1,
                    ts: 100,
                },
                &mut first,
            )
            .await;
        assert!(matches!(
            first.as_slice(),
            [NodeProtocol::VoteResponse { id, leader_id, ts }]
                if id == &peer_one && leader_id == &peer_one && *ts == 100
        ));

        let mut second = vec![];
        service
            .process(
                NodeProtocol::VoteRequest {
                    id: peer_two.clone(),
                    epoch: 1,
                    ts: 200,
                },
                &mut second,
            )
            .await;

        assert!(matches!(
            second.as_slice(),
            [NodeProtocol::VoteResponse { id, leader_id, ts }]
                if id == &peer_two && leader_id == &peer_one && *ts == 100
        ));

        let mut stale = vec![];
        service
            .process(
                NodeProtocol::VoteRequest {
                    id: peer_two.clone(),
                    epoch: 0,
                    ts: 300,
                },
                &mut stale,
            )
            .await;
        assert!(stale.is_empty());
    }

    #[tokio::test]
    async fn vote_response_for_unknown_leader_requests_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let unknown_leader = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer.clone(),
                    Node::Manager {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });
        service.elections.insert(
            1,
            Election::Mine {
                ts: 100,
                approvers: HashSet::new(),
            },
        );

        let mut output = vec![];
        handle_vote_response(
            &mut output,
            service.state.as_mut().unwrap(),
            peer.clone(),
            unknown_leader,
            100,
            &me,
            &mut service.elections,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &peer
        ));
    }

    #[tokio::test]
    async fn vote_responses_elect_self_and_broadcast_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(0),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_one.clone(),
                    Node::Manager {
                        host: "peer-one.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
                (
                    peer_two.clone(),
                    Node::Manager {
                        host: "peer-two.local".to_string(),
                        port: 9002,
                        last_heartbeat: 0,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });
        service.elections.insert(
            1,
            Election::Mine {
                ts: 100,
                approvers: HashSet::new(),
            },
        );

        let mut first = vec![];
        handle_vote_response(
            &mut first,
            service.state.as_mut().unwrap(),
            peer_one.clone(),
            me.id.clone(),
            100,
            &me,
            &mut service.elections,
        );
        assert!(first.is_empty());

        let mut second = vec![];
        handle_vote_response(
            &mut second,
            service.state.as_mut().unwrap(),
            peer_two.clone(),
            me.id.clone(),
            100,
            &me,
            &mut service.elections,
        );

        assert_eq!(
            service.state.as_ref().unwrap().elected_leader_id,
            Some(me.id.clone())
        );
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(1));
        assert!(matches!(
            second.as_slice(),
            [
                NodeProtocol::Leader { id, epoch, ts },
                NodeProtocol::Leader { id: id2, epoch: epoch2, ts: ts2 }
            ] if *epoch == 1
                && *ts == 100
                && *epoch2 == 1
                && *ts2 == 100
                && ((id == &peer_one && id2 == &peer_two) || (id == &peer_two && id2 == &peer_one))
        ));
    }

    #[tokio::test]
    async fn vote_responses_complete_election_with_worker_nodes_present() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
        let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(0),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    manager_peer.clone(),
                    Node::Manager {
                        host: "manager.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
                (
                    worker_peer.clone(),
                    worker_node("worker.local", 9100, 0, vec![1, 2]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });
        service.elections.insert(
            1,
            Election::Mine {
                ts: 100,
                approvers: HashSet::new(),
            },
        );

        let mut output = vec![];
        handle_vote_response(
            &mut output,
            service.state.as_mut().unwrap(),
            manager_peer.clone(),
            me.id.clone(),
            100,
            &me,
            &mut service.elections,
        );

        assert_eq!(
            service.state.as_ref().unwrap().elected_leader_id,
            Some(me.id.clone())
        );
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(1));
        assert_eq!(output.len(), 2);
        assert!(
            output
                .iter()
                .any(|msg| matches!(msg, NodeProtocol::Leader { id, .. } if id == &manager_peer))
        );
        assert!(
            output
                .iter()
                .any(|msg| matches!(msg, NodeProtocol::Leader { id, .. } if id == &worker_peer))
        );
    }

    #[tokio::test]
    async fn cluster_state_updates_known_nodes_and_requests_unknown_ones() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let unknown = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer.clone(), node("peer.local", 9001, now)),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            2,
            me.id.clone(),
            vec![
                ClusterNode::Manager {
                    id: me.id.clone(),
                    host: me.host.clone(),
                    port: me.port,
                    last_heartbeat: now + 1,
                },
                ClusterNode::Manager {
                    id: unknown.clone(),
                    host: "unknown.local".to_string(),
                    port: 9002,
                    last_heartbeat: now + 2,
                },
            ],
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::NewConnection { id, host, port, manager }]
                if id.is_none() && host == "unknown.local" && *port == 9002 && *manager
        ));

        let state = service.state.as_mut().unwrap();
        assert_eq!(state.epoch, Some(2));
        assert_eq!(state.elected_leader_id, Some(me.id.clone()));
        assert_eq!(
            *state.nodes.get_mut(&me.id).unwrap().last_heartbeat_mut(),
            now + 1
        );
        assert_eq!(
            *state.nodes.get_mut(&peer).unwrap().last_heartbeat_mut(),
            now
        );
    }

    #[tokio::test]
    async fn cluster_state_accepts_new_epoch_same_leader_and_rejects_conflicts() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let other_leader = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: None,
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    leader.clone(),
                    Node::Manager {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut first = vec![];
        handle_cluster_state(
            &mut first,
            service.state.as_mut().unwrap(),
            4,
            leader.clone(),
            vec![
                ClusterNode::Manager {
                    id: me.id.clone(),
                    host: me.host.clone(),
                    port: me.port,
                    last_heartbeat: now,
                },
                ClusterNode::Manager {
                    id: leader.clone(),
                    host: "leader.local".to_string(),
                    port: 9001,
                    last_heartbeat: now,
                },
            ],
        );
        assert!(first.is_empty());
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&me.id)
                .unwrap()
                .last_heartbeat_mut(),
            now
        );
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&leader)
                .unwrap()
                .last_heartbeat_mut(),
            now
        );

        let mut same_epoch_same_leader = vec![];
        handle_cluster_state(
            &mut same_epoch_same_leader,
            service.state.as_mut().unwrap(),
            4,
            leader.clone(),
            vec![ClusterNode::Manager {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 1,
            }],
        );
        assert!(same_epoch_same_leader.is_empty());
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&me.id)
                .unwrap()
                .last_heartbeat_mut(),
            now + 1
        );

        let mut conflict = vec![];
        handle_cluster_state(
            &mut conflict,
            service.state.as_mut().unwrap(),
            4,
            other_leader,
            vec![ClusterNode::Manager {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 2,
            }],
        );
        assert!(conflict.is_empty());
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&me.id)
                .unwrap()
                .last_heartbeat_mut(),
            now + 1
        );
    }

    #[tokio::test]
    async fn cluster_state_overwrites_known_worker_partitions_and_adds_unknown_workers() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let worker = node_id("22222222-2222-2222-2222-222222222222");
        let other_worker = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    worker.clone(),
                    worker_node("worker.local", 9100, now, vec![1, 2]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            2,
            me.id.clone(),
            vec![
                ClusterNode::Worker {
                    id: worker.clone(),
                    host: "worker.local".to_string(),
                    port: 9100,
                    last_heartbeat: now + 10,
                    partitions: vec![2, 3, 3, 4],
                },
                ClusterNode::Worker {
                    id: other_worker.clone(),
                    host: "worker-two.local".to_string(),
                    port: 9101,
                    last_heartbeat: now + 20,
                    partitions: vec![7, 8],
                },
            ],
        );

        assert!(output.is_empty());
        let state = service.state.as_ref().unwrap();
        assert!(matches!(
            state.nodes.get(&worker),
            Some(Node::Worker {
                host,
                port,
                last_heartbeat,
                partitions,
            }) if host == "worker.local"
                && *port == 9100
                && *last_heartbeat == now + 10
                && partitions == &vec![2, 3, 3, 4]
        ));
        assert!(matches!(
            state.nodes.get(&other_worker),
            Some(Node::Worker {
                host,
                port,
                last_heartbeat,
                partitions,
            }) if host == "worker-two.local"
                && *port == 9101
                && *last_heartbeat == now + 20
                && partitions == &vec![7, 8]
        ));
    }

    #[tokio::test]
    async fn new_connection_while_we_are_leader_does_not_request_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let now = now_millis();
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer.clone(),
                    Node::Manager {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_new_connection(
            &mut output,
            service.state.as_mut().unwrap(),
            Some(node_id("33333333-3333-3333-3333-333333333333")),
            "third.local".to_string(),
            9002,
            &me,
            true,
        );

        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn stale_cluster_state_is_ignored() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(3),
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (leader.clone(), node("leader.local", 9001, now)),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });
        {
            let mut guard = config.write().await;
            guard.partitions_amount = Some(1);
            guard.replication_factor = Some(1);
        }

        let mut output = vec![];
        handle_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            2,
            me.id.clone(),
            vec![ClusterNode::Manager {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 50,
            }],
        );

        assert!(output.is_empty());
        let state = service.state.as_mut().unwrap();
        assert_eq!(state.epoch, Some(3));
        assert_eq!(state.elected_leader_id, Some(leader));
        assert_eq!(
            *state.nodes.get_mut(&me.id).unwrap().last_heartbeat_mut(),
            now
        );
        let guard = config.read().await;
        assert_eq!(guard.partitions_amount, Some(1));
        assert_eq!(guard.replication_factor, Some(1));
    }

    #[tokio::test]
    async fn leader_message_updates_epoch_and_clears_pending_elections() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (leader.clone(), node("leader.local", 9001, now)),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });
        service.elections.insert(
            2,
            Election::Mine {
                ts: now,
                approvers: HashSet::from([leader.clone()]),
            },
        );

        handle_leader(
            &mut Vec::new(),
            service.state.as_mut().unwrap(),
            leader.clone(),
            2,
            now + 10,
            &me,
            &mut service.elections,
        );

        let state = service.state.as_mut().unwrap();
        assert_eq!(state.epoch, Some(2));
        assert_eq!(state.elected_leader_id, Some(leader.clone()));
        assert_eq!(
            *state.nodes.get_mut(&leader).unwrap().last_heartbeat_mut(),
            now + 10
        );
        assert!(service.elections.is_empty());
    }

    #[tokio::test]
    async fn leader_with_unknown_id_requests_cluster_state_from_manager_peers_only() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
        let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
        let leader = node_id("44444444-4444-4444-4444-444444444444");
        let now = now_millis();
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (manager_peer.clone(), node("manager.local", 9001, now)),
                (
                    worker_peer.clone(),
                    worker_node("worker.local", 9100, now, vec![1]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_leader(
            &mut output,
            service.state.as_mut().unwrap(),
            leader,
            2,
            now + 10,
            &me,
            &mut service.elections,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &manager_peer
        ));
    }

    #[tokio::test]
    async fn leader_messages_only_move_state_forward() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let now = now_millis();
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    leader.clone(),
                    Node::Manager {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        handle_leader(
            &mut vec![],
            service.state.as_mut().unwrap(),
            leader.clone(),
            2,
            now,
            &me,
            &mut service.elections,
        );
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(2));
        assert_eq!(
            service.state.as_ref().unwrap().elected_leader_id,
            Some(leader.clone())
        );
        assert_eq!(
            *service
                .state
                .as_mut()
                .unwrap()
                .nodes
                .get_mut(&leader)
                .unwrap()
                .last_heartbeat_mut(),
            now
        );

        handle_leader(
            &mut vec![],
            service.state.as_mut().unwrap(),
            me.id.clone(),
            1,
            88,
            &me,
            &mut service.elections,
        );
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(2));
        assert_eq!(
            service.state.as_ref().unwrap().elected_leader_id,
            Some(leader)
        );
    }

    #[tokio::test]
    async fn tick_emits_heartbeats_for_stale_self_heartbeat() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me.clone());
        let now = now_millis() - 1_000;
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_id.clone(),
                    Node::Manager {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service.tick(&mut output).await;

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::Heartbeat { recipient_id, heartbeat }] if recipient_id == &peer_id
                && heartbeat.id == me.id
        ));
    }

    #[tokio::test]
    async fn tick_starts_election_when_leader_is_missing() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: Some(4),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    peer.clone(),
                    Node::Manager {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service.tick(&mut output).await;

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::VoteRequest { id, epoch, .. }] if id == &peer && *epoch == 5
        ));
        assert!(matches!(
            service.elections.last_key_value(),
            Some((5, Election::Mine { .. }))
        ));
    }

    #[tokio::test]
    async fn tick_starts_a_new_election_only_for_manager_peers() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let manager_peer = node_id("22222222-2222-2222-2222-222222222222");
        let worker_peer = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, _config) = service(me.clone());
        let future = now_millis() + 10_000;
        service.state = Some(State {
            epoch: Some(7),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, future)),
                (manager_peer.clone(), node("manager.local", 9001, 0)),
                (
                    worker_peer.clone(),
                    worker_node("worker.local", 9100, 0, vec![1]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service.tick(&mut output).await;

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::VoteRequest { id, epoch, .. }] if id == &manager_peer && *epoch == 8
        ));
        assert!(matches!(
            service.elections.last_key_value(),
            Some((8, Election::Mine { .. }))
        ));
    }

    #[tokio::test]
    async fn tick_clears_stale_remote_leader_without_emitting_messages() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let now = now_millis();
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    leader.clone(),
                    Node::Manager {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now - 1_000,
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service.tick(&mut output).await;

        assert!(output.is_empty());
        assert_eq!(service.state.as_ref().unwrap().elected_leader_id, None);
    }

    #[tokio::test]
    async fn get_cluster_state_is_ignored_until_the_cluster_is_known() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let (mut service, config) = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: None,
            nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        handle_get_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            node_id("22222222-2222-2222-2222-222222222222"),
            config.as_ref(),
        )
        .await;

        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn node_disconnected_for_unknown_node_is_a_noop() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    leader.clone(),
                    Node::Manager {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now_millis(),
                    },
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        handle_node_disconnected(
            service.state.as_mut().unwrap(),
            node_id("33333333-3333-3333-3333-333333333333"),
            &me,
        );

        let state = service.state.as_ref().unwrap();
        assert!(state.nodes.contains_key(&me.id));
        assert!(state.nodes.contains_key(&leader));
        assert_eq!(state.elected_leader_id, Some(leader));
    }

    #[tokio::test]
    async fn tick_recomputes_worker_partitions_and_broadcasts_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let worker_a = node_id("22222222-2222-2222-2222-222222222222");
        let worker_b = node_id("33333333-3333-3333-3333-333333333333");
        let (mut service, config) = service(me.clone());
        {
            let mut guard = config.write().await;
            guard.partitions_amount = Some(5);
            guard.replication_factor = Some(3);
        }
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    worker_a.clone(),
                    worker_node("worker-a.local", 9100, 0, vec![]),
                ),
                (
                    worker_b.clone(),
                    worker_node("worker-b.local", 9101, 0, vec![]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service.tick(&mut output).await;

        assert_eq!(output.len(), 2);
        assert!(output.iter().all(|msg| matches!(
            msg,
            NodeProtocol::ClusterState { recipient_id, state }
                if state.config.clone().unwrap().partitions_amount == 5
                    && state.config.clone().unwrap().replication_factor == 3
                    && (recipient_id == &worker_a || recipient_id == &worker_b)
                    && state.items.iter().all(|item| matches!(item, ClusterNode::Worker { .. }))
        )));

        let state = service.state.as_ref().expect("state exists");
        let worker_a_partitions = match state.nodes.get(&worker_a).expect("worker a") {
            Node::Worker { partitions, .. } => partitions.clone(),
            _ => panic!("unexpected node type"),
        };
        let worker_b_partitions = match state.nodes.get(&worker_b).expect("worker b") {
            Node::Worker { partitions, .. } => partitions.clone(),
            _ => panic!("unexpected node type"),
        };

        assert_eq!(worker_a_partitions, vec![0, 1, 2, 3, 4]);
        assert_eq!(worker_b_partitions, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn tick_keeps_partitions_from_previous_worker_layouts() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let worker_a = node_id("22222222-2222-2222-2222-222222222222");
        let worker_b = node_id("33333333-3333-3333-3333-333333333333");
        let worker_c = node_id("44444444-4444-4444-4444-444444444444");
        let (mut service, config) = service(me.clone());
        {
            let mut guard = config.write().await;
            guard.partitions_amount = Some(6);
            guard.replication_factor = Some(1);
        }
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    worker_a.clone(),
                    worker_node("worker-a.local", 9100, 0, vec![]),
                ),
                (
                    worker_b.clone(),
                    worker_node("worker-b.local", 9101, 0, vec![]),
                ),
            ]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service.tick(&mut output).await;

        assert_eq!(output.len(), 2);
        {
            let state = service.state.as_ref().expect("state exists");
            assert_eq!(partitions_for_worker(state, &worker_a), vec![0, 2, 4]);
            assert_eq!(partitions_for_worker(state, &worker_b), vec![1, 3, 5]);
        }

        service.state.as_mut().unwrap().nodes.insert(
            worker_c.clone(),
            worker_node("worker-c.local", 9102, 0, vec![]),
        );

        output.clear();
        service.tick(&mut output).await;

        assert_eq!(output.len(), 3);
        assert!(output.iter().all(|msg| matches!(
            msg,
            NodeProtocol::ClusterState { recipient_id, state }
                if (recipient_id == &worker_a
                    || recipient_id == &worker_b
                    || recipient_id == &worker_c)
                    && state.items.iter().any(|item| matches!(
                        item,
                        ClusterNode::Worker { id, partitions, .. }
                            if id == &worker_a && partitions == &vec![0, 2, 4, 3]
                    ))
                    && state.items.iter().any(|item| matches!(
                        item,
                        ClusterNode::Worker { id, partitions, .. }
                            if id == &worker_b && partitions == &vec![1, 3, 5, 4]
                    ))
                    && state.items.iter().any(|item| matches!(
                        item,
                        ClusterNode::Worker { id, partitions, .. }
                            if id == &worker_c && partitions == &vec![2, 5]
                    ))
        )));

        let state = service.state.as_ref().expect("state exists");
        assert_eq!(partitions_for_worker(state, &worker_a), vec![0, 2, 4, 3]);
        assert_eq!(partitions_for_worker(state, &worker_b), vec![1, 3, 5, 4]);
        assert_eq!(partitions_for_worker(state, &worker_c), vec![2, 5]);
    }

    #[tokio::test]
    async fn node_disconnected_clears_current_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let (mut service, _config) = service(me);
        service.state = Some(State {
            epoch: Some(7),
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([(
                leader.clone(),
                Node::Manager {
                    host: "leader.local".to_string(),
                    port: 9001,
                    last_heartbeat: 0,
                },
            )]),
            workers_with_calculated_partitions: Default::default(),
        });

        let mut output = vec![];
        service
            .process(
                NodeProtocol::NodeDisconnected { id: leader.clone() },
                &mut output,
            )
            .await;

        assert!(output.is_empty());
        let state = service.state.as_ref().unwrap();
        assert_eq!(state.elected_leader_id, None);
        assert!(!state.nodes.contains_key(&leader));
    }
}
