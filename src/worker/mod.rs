mod service;
mod domain;
mod grpc;
mod runtime_store;

use crate::common::Config;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let (_host, _port) = &config
        .manager_host_port()
        .ok_or_else(|| anyhow::anyhow!("Manager host and port are not specified"))?;

    // TODO

    Ok(())
}
