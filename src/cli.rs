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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_replication_factor_rejects_zero() {
        assert!(parse_replication_factor("0").is_err());
    }

    #[test]
    fn parse_replication_factor_rejects_non_positive_or_non_int() {
        assert!(parse_replication_factor("-1").is_err());
        assert!(parse_replication_factor("abc").is_err());
    }

    #[test]
    fn parse_replication_factor_accepts_positive_int() {
        assert_eq!(parse_replication_factor("1").unwrap(), 1);
        assert_eq!(parse_replication_factor("3").unwrap(), 3);
    }

    #[test]
    fn common_args_self_port_defaults_to_grpc_port() {
        let args = CommonArgs {
            grpc_port: 1234,
            self_host: "127.0.0.1".to_string(),
            self_port: None,
        };
        assert_eq!(args.self_port(), 1234);
    }

    #[test]
    fn common_args_self_port_overrides_grpc_port() {
        let args = CommonArgs {
            grpc_port: 1234,
            self_host: "127.0.0.1".to_string(),
            self_port: Some(5678),
        };
        assert_eq!(args.self_port(), 5678);
    }

    #[test]
    fn manager_requires_host_and_port_together() {
        // manager_host without manager_port should fail due to `requires = "manager_port"`.
        let res = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--manager-host",
            "10.0.0.1",
        ]);
        assert!(res.is_err());

        // manager_port without manager_host should fail due to `requires = "manager_host"`.
        let res = Cli::try_parse_from([
            "bin",
            "manager",
            "--grpc-port",
            "5000",
            "--self-host",
            "127.0.0.1",
            "--manager-port",
            "6000",
        ]);
        assert!(res.is_err());
    }
}
