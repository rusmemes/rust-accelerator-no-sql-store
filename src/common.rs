use crate::cli::{Cli, Command};
use uuid::Uuid;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Debug)]
pub struct NodeId(String);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(id: &str) -> Self {
        Self(
            Uuid::parse_str(id)
                .expect(format!("Unexpected NodeId format: {}", id).as_str())
                .into(),
        )
    }

    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

impl From<String> for NodeId {
    fn from(id: String) -> Self {
        Self::from_string(&id)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct Me {
    pub id: NodeId,
    pub host: String,
    pub port: u32,
}

impl Me {
    pub fn new(host: String, port: u32) -> Self {
        Self { id: NodeId::new(), host, port }
    }
}
pub struct Config {
    pub grpc_port: u16,
    pub self_host_port: (String, u16),
    pub manager_host_port: Option<(String, u16)>,
}

impl From<Cli> for Config {
    fn from(value: Cli) -> Self {
        match value.command {
            Command::Manager {
                common,
                manager_host,
                manager_port,
            } => Config {
                grpc_port: common.grpc_port,
                self_host_port: (common.self_host.clone(), common.self_port()),
                manager_host_port: manager_host.zip(manager_port),
            },
            Command::Worker {
                common,
                manager_host,
                manager_port,
            } => Config {
                grpc_port: common.grpc_port,
                self_host_port: (common.self_host.clone(), common.self_port()),
                manager_host_port: Some((manager_host, manager_port)),
            },
        }
    }
}

use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
