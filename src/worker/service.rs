use crate::common::{now_millis, Config, Me, Node, NodeType};
use crate::worker::domain::WorkerProtocol;
use crate::worker::service::heartbeat::heartbeats;
use crate::worker::service::state::State;
use std::collections::HashMap;

mod state;
mod heartbeat;

#[derive(Debug)]
struct WorkerService {
    me: Me,
    state: Option<State>,
    config: Config,
}

impl WorkerService {
    pub fn new(me: Me, config: Config) -> Self {
        Self {
            me,
            state: Default::default(),
            config,
        }
    }

    async fn get_init_messages(&mut self) -> Vec<WorkerProtocol> {
        let mut output = vec![];
        if let None = self.state {
            let mut nodes = HashMap::new();
            nodes.insert(
                self.me.id.clone(),
                Node {
                    host: self.me.host.clone(),
                    port: self.me.port,
                    last_heartbeat: now_millis(),
                    node_type: NodeType::Worker,
                },
            );
            let (host, port) = self
                .config
                .manager_host_and_port()
                .expect("Worker cannot run without connection options");
            output.push(WorkerProtocol::NewConnection {
                id: None,
                host: host.clone(),
                port: (*port) as u32,
                manager: true,
            })
        }
        output
    }

    async fn tick(&mut self, output: &mut Vec<WorkerProtocol>) {
        if let Some(state) = self.state.as_mut() {
            heartbeats(state, output, &self.me);
            // TODO: work on state
        }
        tracing::debug!("state: {:?}", self.state);
    }
}
