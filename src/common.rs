use crate::cli::{Cli, Command};
use uuid::Uuid;

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Hash, Debug)]
pub struct NodeId(String);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
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
        Self {
            id: NodeId::new(),
            host,
            port,
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub grpc_port: u16,
    pub self_host_port: (String, u16),
    pub manager_host_port: Option<(String, u16)>,
    pub replication_factor: Option<usize>,
}

impl From<Cli> for Config {
    fn from(value: Cli) -> Self {
        match value.command {
            Command::Manager {
                common,
                manager_host,
                manager_port,
                replication_factor,
            } => Config {
                grpc_port: common.grpc_port,
                self_host_port: (common.self_host.clone(), common.self_port()),
                manager_host_port: manager_host.zip(manager_port),
                replication_factor: Some(replication_factor),
            },
            Command::Worker {
                common,
                manager_host,
                manager_port,
            } => Config {
                grpc_port: common.grpc_port,
                self_host_port: (common.self_host.clone(), common.self_port()),
                manager_host_port: Some((manager_host, manager_port)),
                replication_factor: None,
            },
        }
    }
}

use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::Parser;

    #[test]
    fn node_id_roundtrips_uuid_string() {
        let id = NodeId::new();
        let s = id.to_string();
        let parsed = NodeId::from_string(&s);
        assert_eq!(parsed.to_string(), s);
        assert_eq!(parsed.to_string(), format!("{}", parsed));
    }

    #[test]
    fn node_id_from_string_from_string_impl() {
        let id = NodeId::new().to_string();
        let parsed: NodeId = id.clone().into();
        assert_eq!(parsed.to_string(), id);
    }

    #[test]
    fn config_from_cli_manager_sets_replication_factor_and_optional_manager_addr() {
        let cli = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--replication-factor",
            "5",
        ])
        .unwrap();

        let cfg: Config = cli.into();
        assert_eq!(cfg.grpc_port, 5000);
        assert_eq!(cfg.self_host_port, ("127.0.0.1".to_string(), 5000));
        assert_eq!(cfg.manager_host_port, None);
        assert_eq!(cfg.replication_factor, Some(5));
    }

    #[test]
    fn config_from_cli_worker_sets_manager_addr_and_clears_replication_factor() {
        let cli = Cli::try_parse_from([
            "bin",
            "worker",
            "--grpc-port",
            "5001",
            "--self-host",
            "127.0.0.1",
            "--self-port",
            "7777",
            "--manager-host",
            "10.0.0.1",
            "--manager-port",
            "6000",
        ])
        .unwrap();

        let cfg: Config = cli.into();
        assert_eq!(cfg.grpc_port, 5001);
        assert_eq!(cfg.self_host_port, ("127.0.0.1".to_string(), 7777));
        assert_eq!(
            cfg.manager_host_port,
            Some(("10.0.0.1".to_string(), 6000))
        );
        assert_eq!(cfg.replication_factor, None);
    }

    #[test]
    fn now_millis_is_non_decreasing_over_time() {
        let a = now_millis();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = now_millis();
        assert!(b >= a);
    }
}
