use std::path::PathBuf;

use clap::{Parser, Subcommand};
use uuid::Uuid;

use crate::output::OutputFormat;

/// `ns` talks to the service's database directly rather than to its HTTP
/// API. It's an operator tool for the machine (or container) the service
/// runs on -- inspecting the queue, replaying dead-lettered work, fixing a
/// template -- and going straight to storage means it keeps working when
/// the API is exactly what's broken.
#[derive(Debug, Parser)]
#[command(name = "ns", version, about = "Admin CLI for the notification service")]
pub struct Cli {
    /// Configuration file to read settings from.
    #[arg(long, global = true, env = "NS_CONFIG_PATH", default_value = "config/local.toml")]
    pub config: PathBuf,

    /// Database to operate on, overriding whatever the config resolves to.
    #[arg(long, global = true)]
    pub database_url: Option<String>,

    /// Output format: human-readable text, or JSON for scripting.
    #[arg(long, short, global = true, value_enum, default_value = "text")]
    pub output: OutputFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Inspect and operate on the delivery queue.
    #[command(subcommand)]
    Queue(QueueCommand),
}

#[derive(Debug, Subcommand)]
pub enum QueueCommand {
    /// Summarise how many notifications sit in each delivery status.
    Stats {
        /// Restrict the summary to one tenant.
        #[arg(long)]
        tenant: Option<Uuid>,
    },
}
