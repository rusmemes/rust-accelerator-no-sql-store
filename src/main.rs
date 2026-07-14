use crate::cli::{Cli, Command};
use clap::Parser;

mod cli;
mod common;
mod manager;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match &cli.command {
        Command::Manager { .. } => manager::run(cli.into()).await?,
        Command::Worker { .. } => {},
    }

    Ok(())
}
