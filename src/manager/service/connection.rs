use super::{Node, State};
use crate::common::{now_millis, Me, NodeId};
use crate::manager::domain::{NodeProtocol, NodeType};

pub(super) fn handle_node_disconnected(state: &mut State, id: NodeId, me: &Me) {
    if let Some(_) = state.nodes.remove(&id) {
        tracing::info!("Node disconnected: {:?}", id);
        if Some(id) == state.elected_leader_id || state.nodes.len() == 1 {
            state.elected_leader_id = None;
        }
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
    }
}

pub(super) fn handle_new_connection(
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
                Node {
                    host,
                    port,
                    last_heartbeat: now_millis(),
                    node_type: NodeType::Manager,
                }
            } else {
                Node {
                    host,
                    port,
                    last_heartbeat: now_millis(),
                    node_type: NodeType::Worker,
                }
            },
        );
        tracing::info!("Me: {:?}", me);
        tracing::info!("State: {:?}", state);
    }
}

#[cfg(test)]
mod tests;
