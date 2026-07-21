use crate::common::{ClusterNode, Node, NodeId, NodeType, Partitions};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::state::State;

pub(super) fn handle_cluster_state(
    output: &mut Vec<WorkerProtocol>,
    state: &mut State,
    epoch: u64,
    leader_id: NodeId,
    items: Vec<ClusterNode>,
    partitions: Partitions,
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
        state.partitions = partitions;

        for item in items {
            match item {
                ClusterNode {
                    id,
                    host,
                    port,
                    last_heartbeat,
                    node_type,
                } => {
                    if let Some(Node {
                        last_heartbeat: node_last_heartbeat,
                        ..
                    }) = state.nodes.get_mut(&id)
                    {
                        if *node_last_heartbeat < last_heartbeat {
                            *node_last_heartbeat = last_heartbeat;
                        }
                    } else {
                        match node_type {
                            NodeType::Manager => output.push(WorkerProtocol::NewConnection {
                                id: None,
                                host,
                                port,
                                manager: true,
                            }),
                            NodeType::Worker => {
                                state.nodes.insert(
                                    id,
                                    Node {
                                        host,
                                        port,
                                        last_heartbeat,
                                        node_type,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
