use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod aws;
mod cli;
mod config;
mod engine;
mod logging;
mod provider;
mod trigger;

#[derive(Parser)]
#[command(
    name = "watcher",
    version,
    about = "Read-only sync of AWS Parameter Store to disk"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the long-lived sync daemon (full sync + event-driven sync).
    Sync {
        #[arg(short, long, default_value = "config.toml", env = "WATCHER_CONFIG")]
        config: PathBuf,
        #[arg(long)]
        log_level: Option<String>,
        #[arg(long)]
        log_format: Option<String>,
    },
    /// Provision the SQS queue + EventBridge rule used for event-driven sync.
    Setup {
        #[arg(short, long, default_value = "config.toml", env = "WATCHER_CONFIG")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Sync {
            config,
            log_level,
            log_format,
        } => cli::sync::run(config, log_level, log_format).await,
        Command::Setup { config } => cli::setup::run(config).await,
    }
}
