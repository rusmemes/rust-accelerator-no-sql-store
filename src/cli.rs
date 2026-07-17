use clap_derive::{Args, Parser, Subcommand};

fn parse_replication_factor(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| "replication_factor must be a positive integer".to_string())?;

    if value == 0 {
        Err("replication_factor must be at least 1".to_string())
    } else {
        Ok(value)
    }
}

#[derive(Args)]
pub struct CommonArgs {
    #[arg(long)]
    pub grpc_port: u16,

    #[arg(long)]
    pub self_host: String,

    #[arg(long)]
    self_port: Option<u16>,
}

impl CommonArgs {
    pub fn self_port(&self) -> u16 {
        self.self_port.unwrap_or(self.grpc_port)
    }
}

#[derive(Subcommand)]
pub enum Command {
    Manager {
        #[command(flatten)]
        common: CommonArgs,

        #[arg(long, requires = "manager_port")]
        manager_host: Option<String>,

        #[arg(long, requires = "manager_host")]
        manager_port: Option<u16>,

        #[arg(
            long,
            default_value_t = 3,
            value_parser = parse_replication_factor
        )]
        replication_factor: usize,
    },

    Worker {
        #[command(flatten)]
        common: CommonArgs,

        #[arg(long)]
        manager_host: String,

        #[arg(long)]
        manager_port: u16,
    },
}

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}
