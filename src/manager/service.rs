use crate::common::{now_millis, Config, Me, NodeId};
use crate::manager::domain::{ClusterState, ClusterStateItem, Heartbeat, NodeProtocol};
use rand::random_range;
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct Node {
    host: String,
    port: u32,
    last_heartbeat: u64,
}

#[derive(Debug)]
struct State {
    epoch: Option<u64>,
    elected_leader_id: Option<NodeId>,
    nodes: HashMap<NodeId, Node>,
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

#[derive(Debug)]
struct ManagerService {
    me: Arc<Me>,
    state: Option<State>,
    elections: BTreeMap<u64, Election>,
}

impl ManagerService {
    pub fn new(me: Arc<Me>) -> Self {
        Self {
            me,
            state: Default::default(),
            elections: Default::default(),
        }
    }

    fn start_election_if_needed(&mut self) -> Vec<NodeProtocol> {
        let mut output = vec![];

        if let Some(state) = self.state.as_mut()
            && state.elected_leader_id.is_none()
            && state.nodes.len() > 1
            && let Some(epoch) = state.epoch
        {
            let curr_ts = now_millis();
            let election = self.elections.last_key_value();
            let start_new = if let Some((_, last_election)) = election {
                match last_election {
                    Election::Mine { ts, approvers: _ } => ts + random_range(500..1000) < curr_ts,
                    Election::Other {
                        ts,
                        candidate_id: _,
                    } => ts + random_range(500..1000) < curr_ts,
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

                self.elections.clear();
                self.elections.insert(next_epoch, election);
                state
                    .nodes
                    .keys()
                    .filter(|&key| *key != self.me.id)
                    .for_each(|node_id| {
                        output.push(NodeProtocol::VoteRequest {
                            id: node_id.clone(),
                            epoch: next_epoch,
                            ts: curr_ts,
                        });
                    });

                tracing::info!("New election started: {:?}", self.elections);
            }
        }

        output
    }

    fn tick(&mut self, output: &mut Vec<NodeProtocol>) {
        if let Some(state) = self.state.as_mut() {
            if let Some(node) = state.nodes.get_mut(&self.me.id) {
                let now = now_millis();
                if node.last_heartbeat + 200 < now {
                    node.last_heartbeat = now;
                    output.extend(state.nodes.keys().filter(|&key| *key != self.me.id).map(
                        |key| NodeProtocol::Heartbeat {
                            recipient_id: key.clone(),
                            heartbeat: Heartbeat {
                                id: self.me.id.clone(),
                                ts: now,
                            },
                        },
                    ));
                }

                if state.elected_leader_id.is_some()
                    && state.elected_leader_id.as_ref() != Some(&self.me.id)
                {
                    if let Some(leader) = state
                        .nodes
                        .get_mut(&state.elected_leader_id.as_ref().unwrap())
                    {
                        if leader.last_heartbeat + 500 < now {
                            state.elected_leader_id = None;
                        }
                    }
                }

                output.extend(self.start_election_if_needed());
            }
        }
        tracing::debug!("state: {:?}", self.state);
        tracing::debug!("elections: {:?}", self.elections);
    }

    fn process(&mut self, msg: NodeProtocol, output: &mut Vec<NodeProtocol>) {
        if let Some(state) = self.state.as_mut() {
            match msg {
                NodeProtocol::NewConnection { id, host, port } => {
                    handle_new_connection(output, state, id, host, port, &self.me);
                }
                NodeProtocol::Heartbeat {
                    recipient_id: _,
                    heartbeat: Heartbeat { id, ts },
                } => {
                    handle_heartbeat(output, state, id, ts, &self.me);
                }
                NodeProtocol::GetClusterState { id } => {
                    handle_get_cluster_state(output, state, id);
                }
                NodeProtocol::ClusterState {
                    recipient_id: _,
                    state:
                        ClusterState {
                            epoch,
                            leader_id,
                            items,
                        },
                } => {
                    handle_cluster_state(output, state, epoch, leader_id, items);
                }
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
                    handle_leader(state, id, epoch, ts, &self.me, &mut self.elections);
                }
                NodeProtocol::NodeDisconnected { id } => {
                    handle_node_disconnected(state, id, &self.me);
                }
            }
        }

        self.tick(output)
    }

    fn get_init_messages(&mut self, config: Config) -> Vec<NodeProtocol> {
        if self.state.is_some() {
            vec![]
        } else if let Some((manager_host, manager_port)) = config.manager_host_port {
            let mut nodes = HashMap::new();
            nodes.insert(
                self.me.id.clone(),
                Node {
                    host: self.me.host.clone(),
                    port: self.me.port,
                    last_heartbeat: now_millis(),
                },
            );
            self.state = Some(State {
                epoch: None,
                elected_leader_id: None,
                nodes,
            });
            vec![NodeProtocol::NewConnection {
                id: None,
                host: manager_host,
                port: manager_port as u32,
            }]
        } else {
            let mut nodes = HashMap::new();
            nodes.insert(
                self.me.id.clone(),
                Node {
                    host: self.me.host.clone(),
                    port: self.me.port,
                    last_heartbeat: now_millis(),
                },
            );
            self.state = Some(State {
                epoch: Some(0),
                elected_leader_id: None,
                nodes,
            });
            vec![]
        }
    }
}

fn handle_node_disconnected(state: &mut State, id: NodeId, me: &Arc<Me>) {
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
    state: &mut State,
    id: NodeId,
    epoch: u64,
    ts: u64,
    me: &Arc<Me>,
    elections: &mut BTreeMap<u64, Election>,
) {
    if state.epoch < Some(epoch) {
        elections.clear();
        if let Some(leader) = state.nodes.get_mut(&id) {
            leader.last_heartbeat = ts;
            state.elected_leader_id = Some(id);
            state.epoch = Some(epoch);
        }
        tracing::info!("Me: {:?}", me);
        tracing::info!("Leader elected, State: {:?}", state);
    }
}

fn handle_vote_response(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: NodeId,
    leader_id: NodeId,
    ts: u64,
    me: &Arc<Me>,
    elections: &mut BTreeMap<u64, Election>,
) {
    let approver = if let Some((
        epoch,
        Election::Mine {
            ts: election_ts,
            approvers: _,
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
        if let Some(Election::Mine { ts: _, approvers }) = elections.get_mut(&epoch) {
            approvers.insert(approver);

            if approvers.len() == state.nodes.len() - 1
                && state
                    .nodes
                    .keys()
                    .filter(|&node_id| *node_id != me.id)
                    .all(|node_id| approvers.contains(node_id))
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
                .keys()
                .filter(|&key| *key != me.id)
                .map(|key| NodeProtocol::GetClusterState { id: key.clone() }),
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
    items: Vec<ClusterStateItem>,
) {
    let accept_items: bool = if state.epoch.is_none() || state.epoch < Some(epoch) {
        state.epoch = Some(epoch);
        state.elected_leader_id = Some(leader_id);
        true
    } else if state.epoch == Some(epoch) && state.elected_leader_id == Some(leader_id) {
        true
    } else {
        false
    };

    if accept_items {
        for ClusterStateItem {
            id,
            host,
            port,
            last_heartbeat,
        } in items
        {
            if let Some(node) = state.nodes.get_mut(&id) {
                if node.last_heartbeat < last_heartbeat {
                    node.last_heartbeat = last_heartbeat;
                }
            } else {
                output.push(NodeProtocol::NewConnection {
                    id: None,
                    host,
                    port,
                });
            }
        }
    }
}

fn handle_get_cluster_state(output: &mut Vec<NodeProtocol>, state: &mut State, id: NodeId) {
    if let Some((epoch, leader_id)) = state.epoch.zip(state.elected_leader_id.clone()) {
        output.push(NodeProtocol::ClusterState {
            recipient_id: id.clone(),
            state: ClusterState {
                epoch,
                leader_id,
                items: state
                    .nodes
                    .iter()
                    .map(|(id, node)| ClusterStateItem {
                        id: id.clone(),
                        host: node.host.clone(),
                        port: node.port,
                        last_heartbeat: node.last_heartbeat,
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
    me: &Arc<Me>,
) {
    if let Some(node) = state.nodes.get_mut(&id) {
        node.last_heartbeat = ts;
        if state.elected_leader_id.as_ref() == Some(&me.id) {
            output.extend(
                state
                    .nodes
                    .keys()
                    .filter(|&key| key != &id && key != &me.id)
                    .map(|key| NodeProtocol::Heartbeat {
                        recipient_id: key.clone(),
                        heartbeat: Heartbeat { id: id.clone(), ts },
                    }),
            );
        }
    } else {
        output.extend(
            state
                .nodes
                .keys()
                .filter(|&key| key != &me.id)
                .map(|key| NodeProtocol::GetClusterState { id: key.clone() }),
        );
    }
}

fn handle_new_connection(
    output: &mut Vec<NodeProtocol>,
    state: &mut State,
    id: Option<NodeId>,
    host: String,
    port: u32,
    me: &Arc<Me>,
) {
    if let Some(id) = id {
        state.nodes.insert(
            id.clone(),
            Node {
                host,
                port,
                last_heartbeat: now_millis(),
            },
        );
        tracing::info!("New connection: {:?}", id);
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
        if state.elected_leader_id.is_none() || state.elected_leader_id.as_ref() != Some(&me.id) {
            output.push(NodeProtocol::GetClusterState { id });
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
    config: Config,
    (tx, mut rx): (Sender<NodeProtocol>, Receiver<NodeProtocol>),
    cancellation_token: CancellationToken,
) {
    let mut service = ManagerService::new(me);
    for msg in service.get_init_messages(config) {
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
                    service.process(message, &mut output);
                    for msg in output.drain(..) {
                        if let Err(e) = tx.send(msg).await {
                            tracing::error!("Error sending response: {}", e);
                        }
                    }
                }
            }
            _ = ticker.tick() => {
                service.tick(&mut output);
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
    use std::sync::Arc;

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

    fn service(me: Arc<Me>) -> ManagerService {
        ManagerService::new(me)
    }

    fn fresh_node(me: &Me, last_heartbeat: u64) -> Node {
        Node {
            host: me.host.clone(),
            port: me.port,
            last_heartbeat,
        }
    }

    fn node(host: &str, port: u32, last_heartbeat: u64) -> Node {
        Node {
            host: host.to_string(),
            port,
            last_heartbeat,
        }
    }

    #[test]
    fn init_with_manager_connection_requests_connection_and_sets_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let mut service = service(me.clone());

        let output = service.get_init_messages(Config {
            grpc_port: 8080,
            self_host_port: ("127.0.0.1".to_string(), 7000),
            manager_host_port: Some(("manager.local".to_string(), 9000)),
        });

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::NewConnection {
                id: _,
                host,
                port
            }] if host == "manager.local" && *port == 9000
        ));

        let state = service.state.as_ref().expect("state initialized");
        assert_eq!(state.epoch, None);
        assert_eq!(state.elected_leader_id, None);
        assert!(state.nodes.contains_key(&me.id));
    }

    #[test]
    fn init_without_manager_starts_as_epoch_zero() {
        let mut service = service(me("11111111-1111-1111-1111-111111111111"));

        let output = service.get_init_messages(Config {
            grpc_port: 8080,
            self_host_port: ("127.0.0.1".to_string(), 7000),
            manager_host_port: None,
        });

        assert!(output.is_empty());

        let state = service.state.as_ref().expect("state initialized");
        assert_eq!(state.epoch, Some(0));
        assert_eq!(state.elected_leader_id, None);
        assert_eq!(state.nodes.len(), 1);
    }

    #[test]
    fn new_connection_adds_node_and_requests_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: None,
            nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::NewConnection {
                id: Some(peer_id.clone()),
                host: "peer.local".to_string(),
                port: 9001,
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &peer_id
        ));

        let state = service.state.as_ref().expect("state exists");
        assert!(state.nodes.contains_key(&peer_id));
    }

    #[test]
    fn get_cluster_state_returns_current_cluster_snapshot() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(3),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_id.clone(),
                    Node {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: 120,
                    },
                ),
            ]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::GetClusterState {
                id: peer_id.clone(),
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::ClusterState {
                recipient_id,
                state
            }] if recipient_id == &peer_id
                && state.epoch == 3
                && state.leader_id == me.id
                && state.items.len() == 2
        ));
    }

    #[test]
    fn heartbeat_from_unknown_node_requests_cluster_state_from_peers() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer_id.clone(), node("peer.local", 9001, 0)),
            ]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::Heartbeat {
                recipient_id: me.id.clone(),
                heartbeat: Heartbeat {
                    id: node_id("33333333-3333-3333-3333-333333333333"),
                    ts: 42,
                },
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &peer_id
        ));
    }

    #[test]
    fn heartbeat_from_known_node_is_forwarded_when_me_is_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer_one.clone(), node("peer-one.local", 9001, 0)),
                (peer_two.clone(), node("peer-two.local", 9002, 0)),
            ]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::Heartbeat {
                recipient_id: me.id.clone(),
                heartbeat: Heartbeat {
                    id: peer_one.clone(),
                    ts: 42,
                },
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::Heartbeat { recipient_id, heartbeat }]
                if recipient_id == &peer_two
                    && heartbeat.id == peer_one
                    && heartbeat.ts == 42
        ));
    }

    #[test]
    fn vote_request_starts_new_election_and_remembers_candidate() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let candidate = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (candidate.clone(), node("candidate.local", 9001, 0)),
            ]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::VoteRequest {
                id: candidate.clone(),
                epoch: 2,
                ts: 100,
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::VoteResponse { id, leader_id, ts }]
                if id == &candidate && leader_id == &candidate && *ts == 100
        ));
        assert!(matches!(
            service.elections.get(&2),
            Some(Election::Other {
                ts: 100,
                candidate_id
            }) if candidate_id == &candidate
        ));
    }

    #[test]
    fn vote_request_reuses_existing_candidate_for_same_epoch() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let stored_candidate = node_id("22222222-2222-2222-2222-222222222222");
        let requester = node_id("33333333-3333-3333-3333-333333333333");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (stored_candidate.clone(), node("stored.local", 9001, 0)),
                (requester.clone(), node("requester.local", 9002, 0)),
            ]),
        });
        service.elections.insert(
            2,
            Election::Other {
                ts: 100,
                candidate_id: stored_candidate.clone(),
            },
        );

        let mut output = vec![];
        service.process(
            NodeProtocol::VoteRequest {
                id: requester.clone(),
                epoch: 2,
                ts: 200,
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::VoteResponse { id, leader_id, ts }]
                if id == &requester && leader_id == &stored_candidate && *ts == 100
        ));
        assert!(matches!(
            service.elections.get(&2),
            Some(Election::Other {
                ts: 100,
                candidate_id
            }) if candidate_id == &stored_candidate
        ));
    }

    #[test]
    fn cluster_state_updates_known_nodes_and_requests_unknown_ones() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let unknown = node_id("33333333-3333-3333-3333-333333333333");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer.clone(), node("peer.local", 9001, now)),
            ]),
        });

        let mut output = vec![];
        handle_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            2,
            me.id.clone(),
            vec![
                ClusterStateItem {
                    id: me.id.clone(),
                    host: me.host.clone(),
                    port: me.port,
                    last_heartbeat: now + 1,
                },
                ClusterStateItem {
                    id: unknown.clone(),
                    host: "unknown.local".to_string(),
                    port: 9002,
                    last_heartbeat: now + 2,
                },
            ],
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::NewConnection { id, host, port }]
                if id.is_none() && host == "unknown.local" && *port == 9002
        ));

        let state = service.state.as_ref().unwrap();
        assert_eq!(state.epoch, Some(2));
        assert_eq!(state.elected_leader_id, Some(me.id.clone()));
        assert_eq!(state.nodes.get(&me.id).unwrap().last_heartbeat, now + 1);
        assert_eq!(state.nodes.get(&peer).unwrap().last_heartbeat, now);
    }

    #[test]
    fn stale_cluster_state_is_ignored() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(3),
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (leader.clone(), node("leader.local", 9001, now)),
            ]),
        });

        let mut output = vec![];
        handle_cluster_state(
            &mut output,
            service.state.as_mut().unwrap(),
            2,
            me.id.clone(),
            vec![ClusterStateItem {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 50,
            }],
        );

        assert!(output.is_empty());
        let state = service.state.as_ref().unwrap();
        assert_eq!(state.epoch, Some(3));
        assert_eq!(state.elected_leader_id, Some(leader));
        assert_eq!(state.nodes.get(&me.id).unwrap().last_heartbeat, now);
    }

    #[test]
    fn leader_message_updates_epoch_and_clears_pending_elections() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (leader.clone(), node("leader.local", 9001, now)),
            ]),
        });
        service.elections.insert(
            2,
            Election::Mine {
                ts: now,
                approvers: HashSet::from([leader.clone()]),
            },
        );

        handle_leader(
            service.state.as_mut().unwrap(),
            leader.clone(),
            2,
            now + 10,
            &me,
            &mut service.elections,
        );

        let state = service.state.as_ref().unwrap();
        assert_eq!(state.epoch, Some(2));
        assert_eq!(state.elected_leader_id, Some(leader.clone()));
        assert_eq!(state.nodes.get(&leader).unwrap().last_heartbeat, now + 10);
        assert!(service.elections.is_empty());
    }

    #[test]
    fn tick_emits_heartbeats_for_stale_self_heartbeat() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_id = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis() - 1_000;
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_id.clone(),
                    Node {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
            ]),
        });

        let mut output = vec![];
        service.tick(&mut output);

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::Heartbeat { recipient_id, heartbeat }] if recipient_id == &peer_id
                && heartbeat.id == me.id
        ));
    }

    #[test]
    fn tick_starts_election_when_leader_is_missing() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(4),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (peer.clone(), node("peer.local", 9001, 0)),
            ]),
        });

        let mut output = vec![];
        service.tick(&mut output);

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::VoteRequest { id, epoch, .. }] if id == &peer && *epoch == 5
        ));
        assert!(matches!(
            service.elections.get(&5),
            Some(Election::Mine { .. })
        ));
    }

    #[test]
    fn vote_responses_elect_self_and_broadcast_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let mut service = service(me.clone());
        let now = now_millis();
        service.state = Some(State {
            epoch: Some(0),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_one.clone(),
                    Node {
                        host: "peer-one.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
                (
                    peer_two.clone(),
                    Node {
                        host: "peer-two.local".to_string(),
                        port: 9002,
                        last_heartbeat: 0,
                    },
                ),
            ]),
        });
        service.elections.insert(
            1,
            Election::Mine {
                ts: 100,
                approvers: HashSet::new(),
            },
        );

        let mut first = vec![];
        service.process(
            NodeProtocol::VoteResponse {
                id: peer_one.clone(),
                leader_id: me.id.clone(),
                ts: 100,
            },
            &mut first,
        );
        assert!(first.is_empty());

        let mut second = vec![];
        service.process(
            NodeProtocol::VoteResponse {
                id: peer_two.clone(),
                leader_id: me.id.clone(),
                ts: 100,
            },
            &mut second,
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

    #[test]
    fn node_disconnected_clears_current_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me);
        service.state = Some(State {
            epoch: Some(7),
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([(
                leader.clone(),
                Node {
                    host: "leader.local".to_string(),
                    port: 9001,
                    last_heartbeat: 0,
                },
            )]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::NodeDisconnected { id: leader.clone() },
            &mut output,
        );

        assert!(output.is_empty());
        let state = service.state.as_ref().unwrap();
        assert_eq!(state.elected_leader_id, None);
        assert!(!state.nodes.contains_key(&leader));
    }

    #[test]
    fn new_connection_while_we_are_leader_does_not_request_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer.clone(),
                    Node {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::NewConnection {
                id: Some(node_id("33333333-3333-3333-3333-333333333333")),
                host: "third.local".to_string(),
                port: 9002,
            },
            &mut output,
        );

        assert!(output.is_empty());
    }

    #[test]
    fn heartbeat_from_known_peer_is_forwarded_only_when_we_are_leader() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_one.clone(),
                    Node {
                        host: "peer-one.local".to_string(),
                        port: 9001,
                        last_heartbeat: 0,
                    },
                ),
                (
                    peer_two.clone(),
                    Node {
                        host: "peer-two.local".to_string(),
                        port: 9002,
                        last_heartbeat: now,
                    },
                ),
            ]),
        });

        let mut forwarded = vec![];
        service.process(
            NodeProtocol::Heartbeat {
                recipient_id: me.id.clone(),
                heartbeat: Heartbeat {
                    id: peer_one.clone(),
                    ts: 42,
                },
            },
            &mut forwarded,
        );

        assert!(matches!(
            forwarded.as_slice(),
            [NodeProtocol::Heartbeat { recipient_id, heartbeat }]
                if recipient_id == &peer_two && heartbeat.id == peer_one && heartbeat.ts == 42
        ));

        service.state.as_mut().unwrap().elected_leader_id = Some(peer_two.clone());

        let mut not_forwarded = vec![];
        service.process(
            NodeProtocol::Heartbeat {
                recipient_id: me.id.clone(),
                heartbeat: Heartbeat {
                    id: peer_one.clone(),
                    ts: 43,
                },
            },
            &mut not_forwarded,
        );

        assert!(not_forwarded.is_empty());
        assert_eq!(
            service
                .state
                .as_ref()
                .unwrap()
                .nodes
                .get(&peer_one)
                .unwrap()
                .last_heartbeat,
            43
        );
    }

    #[test]
    fn vote_request_adds_new_election_reuses_candidate_and_ignores_stale_epochs() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer_one = node_id("22222222-2222-2222-2222-222222222222");
        let peer_two = node_id("33333333-3333-3333-3333-333333333333");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(0),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer_one.clone(),
                    Node {
                        host: "peer-one.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
                (
                    peer_two.clone(),
                    Node {
                        host: "peer-two.local".to_string(),
                        port: 9002,
                        last_heartbeat: now,
                    },
                ),
            ]),
        });

        let mut first = vec![];
        service.process(
            NodeProtocol::VoteRequest {
                id: peer_one.clone(),
                epoch: 1,
                ts: 100,
            },
            &mut first,
        );
        assert!(matches!(
            first.as_slice(),
            [NodeProtocol::VoteResponse { id, leader_id, ts }]
                if id == &peer_one && leader_id == &peer_one && *ts == 100
        ));

        let mut second = vec![];
        service.process(
            NodeProtocol::VoteRequest {
                id: peer_two.clone(),
                epoch: 1,
                ts: 200,
            },
            &mut second,
        );

        assert!(matches!(
            second.as_slice(),
            [NodeProtocol::VoteResponse { id, leader_id, ts }]
                if id == &peer_two && leader_id == &peer_one && *ts == 100
        ));

        let mut stale = vec![];
        service.process(
            NodeProtocol::VoteRequest {
                id: peer_two.clone(),
                epoch: 0,
                ts: 300,
            },
            &mut stale,
        );
        assert!(stale.is_empty());
    }

    #[test]
    fn vote_response_for_unknown_leader_requests_cluster_state() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let unknown_leader = node_id("33333333-3333-3333-3333-333333333333");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(me.id.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    peer.clone(),
                    Node {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
        });
        service.elections.insert(
            1,
            Election::Mine {
                ts: 100,
                approvers: HashSet::new(),
            },
        );

        let mut output = vec![];
        service.process(
            NodeProtocol::VoteResponse {
                id: peer.clone(),
                leader_id: unknown_leader,
                ts: 100,
            },
            &mut output,
        );

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::GetClusterState { id }] if id == &peer
        ));
    }

    #[test]
    fn cluster_state_accepts_new_epoch_same_leader_and_rejects_conflicts() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let other_leader = node_id("33333333-3333-3333-3333-333333333333");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    leader.clone(),
                    Node {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
        });

        let mut first = vec![];
        handle_cluster_state(
            &mut first,
            service.state.as_mut().unwrap(),
            4,
            leader.clone(),
            vec![
                ClusterStateItem {
                    id: me.id.clone(),
                    host: me.host.clone(),
                    port: me.port,
                    last_heartbeat: now,
                },
                ClusterStateItem {
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
            service
                .state
                .as_ref()
                .unwrap()
                .nodes
                .get(&me.id)
                .unwrap()
                .last_heartbeat,
            now
        );
        assert_eq!(
            service
                .state
                .as_ref()
                .unwrap()
                .nodes
                .get(&leader)
                .unwrap()
                .last_heartbeat,
            now
        );

        let mut same_epoch_same_leader = vec![];
        handle_cluster_state(
            &mut same_epoch_same_leader,
            service.state.as_mut().unwrap(),
            4,
            leader.clone(),
            vec![ClusterStateItem {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 1,
            }],
        );
        assert!(same_epoch_same_leader.is_empty());
        assert_eq!(
            service
                .state
                .as_ref()
                .unwrap()
                .nodes
                .get(&me.id)
                .unwrap()
                .last_heartbeat,
            now + 1
        );

        let mut conflict = vec![];
        handle_cluster_state(
            &mut conflict,
            service.state.as_mut().unwrap(),
            4,
            other_leader,
            vec![ClusterStateItem {
                id: me.id.clone(),
                host: me.host.clone(),
                port: me.port,
                last_heartbeat: now + 2,
            }],
        );
        assert!(conflict.is_empty());
        assert_eq!(service.state.as_ref().unwrap().epoch, Some(4));
        assert_eq!(
            service
                .state
                .as_ref()
                .unwrap()
                .nodes
                .get(&me.id)
                .unwrap()
                .last_heartbeat,
            now + 1
        );
    }

    #[test]
    fn leader_messages_only_move_state_forward() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    leader.clone(),
                    Node {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now,
                    },
                ),
            ]),
        });

        handle_leader(
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
            service
                .state
                .as_ref()
                .unwrap()
                .nodes
                .get(&leader)
                .unwrap()
                .last_heartbeat,
            now
        );

        handle_leader(
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

    #[test]
    fn get_cluster_state_is_ignored_until_the_cluster_is_known() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: None,
            nodes: HashMap::from([(me.id.clone(), fresh_node(&me, now_millis()))]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::GetClusterState {
                id: node_id("22222222-2222-2222-2222-222222222222"),
            },
            &mut output,
        );

        assert!(output.is_empty());
    }

    #[test]
    fn node_disconnected_for_unknown_node_is_a_noop() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(1),
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    leader.clone(),
                    Node {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now_millis(),
                    },
                ),
            ]),
        });

        let mut output = vec![];
        service.process(
            NodeProtocol::NodeDisconnected {
                id: node_id("33333333-3333-3333-3333-333333333333"),
            },
            &mut output,
        );

        assert!(output.is_empty());
        let state = service.state.as_ref().unwrap();
        assert!(state.nodes.contains_key(&me.id));
        assert!(state.nodes.contains_key(&leader));
        assert_eq!(state.elected_leader_id, Some(leader));
    }

    #[test]
    fn tick_clears_stale_remote_leader_without_emitting_messages() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let leader = node_id("22222222-2222-2222-2222-222222222222");
        let now = now_millis();
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: None,
            elected_leader_id: Some(leader.clone()),
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now)),
                (
                    leader.clone(),
                    Node {
                        host: "leader.local".to_string(),
                        port: 9001,
                        last_heartbeat: now - 1_000,
                    },
                ),
            ]),
        });

        let mut output = vec![];
        service.tick(&mut output);

        assert!(output.is_empty());
        assert_eq!(service.state.as_ref().unwrap().elected_leader_id, None);
    }

    #[test]
    fn tick_starts_a_new_election_when_no_leader_exists() {
        let me = me("11111111-1111-1111-1111-111111111111");
        let peer = node_id("22222222-2222-2222-2222-222222222222");
        let mut service = service(me.clone());
        service.state = Some(State {
            epoch: Some(7),
            elected_leader_id: None,
            nodes: HashMap::from([
                (me.id.clone(), fresh_node(&me, now_millis())),
                (
                    peer.clone(),
                    Node {
                        host: "peer.local".to_string(),
                        port: 9001,
                        last_heartbeat: now_millis(),
                    },
                ),
            ]),
        });

        let mut output = vec![];
        service.tick(&mut output);

        assert!(matches!(
            output.as_slice(),
            [NodeProtocol::VoteRequest { id, epoch, .. }] if id == &peer && *epoch == 8
        ));
        assert!(matches!(
            service.elections.last_key_value(),
            Some((8, Election::Mine { .. }))
        ));
    }
}
