mod service;

use crate::common::Config;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let (_host, _port) = &config
        .manager_host_port
        .ok_or_else(|| anyhow::anyhow!("Manager host and port are not specified"))?;

    // TODO

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_errors_when_manager_addr_missing() {
        let cfg = Config {
            grpc_port: 5000,
            self_host_port: ("127.0.0.1".to_string(), 5000),
            manager_host_port: None,
            replication_factor: None,
        };

        let err = run(cfg).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("Manager host and port are not specified"));
    }
}
