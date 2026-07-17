use super::{get_random_number, Node, State};
use crate::common::{now_millis, Me, NodeId};
use crate::manager::domain::{Heartbeat, NodeProtocol, NodeType};

const HEARTBEAT_INTERVAL_MS: u64 = 200;

pub(super) fn heartbeats(state: &mut State, output: &mut Vec<NodeProtocol>, me: &Me) {
    if let Some(Node {
        node_type: NodeType::Manager,
        last_heartbeat,
        ..
    }) = state.nodes.get_mut(&me.id)
    {
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
            if let Some(Node {
                node_type: NodeType::Manager,
                last_heartbeat,
                ..
            }) = state
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

pub(super) fn handle_heartbeat(
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
            node.last_heartbeat = ts;
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

#[cfg(test)]
mod tests;
