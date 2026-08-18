use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use ns_core::{ChannelKind, DeliveryStatus, Priority};
use uuid::Uuid;

use crate::output::OutputFormat;
use crate::parse;

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
    /// Submit and inspect individual notifications.
    #[command(subcommand)]
    Notification(NotificationCommand),

    /// Inspect and operate on the delivery queue.
    #[command(subcommand)]
    Queue(QueueCommand),
}

#[derive(Debug, Subcommand)]
pub enum NotificationCommand {
    /// Queue a notification for delivery.
    Submit(SubmitArgs),

    /// Show one notification in full.
    Show {
        /// The notification's id.
        id: Uuid,
    },

    /// List a tenant's notifications, newest first.
    List {
        #[arg(long)]
        tenant: Uuid,

        /// Only show notifications in this delivery status.
        #[arg(long, value_parser = parse::status)]
        status: Option<DeliveryStatus>,

        #[arg(long, default_value_t = 50)]
        limit: u32,
    },

    /// Cancel a notification that hasn't started sending yet.
    Cancel { id: Uuid },

    /// Re-queue a dead-lettered notification as a fresh copy.
    Replay { id: Uuid },
}

#[derive(Debug, Args)]
pub struct SubmitArgs {
    #[arg(long)]
    pub tenant: Uuid,

    #[arg(long)]
    pub recipient: Uuid,

    #[arg(long, value_parser = parse::channel)]
    pub channel: ChannelKind,

    #[arg(long, value_parser = parse::priority, default_value = "normal")]
    pub priority: Priority,

    #[arg(long)]
    pub subject: Option<String>,

    /// The message text. Mutually exclusive with --template: a
    /// notification takes its body from one or the other, and accepting
    /// both would leave which one wins up to the reader.
    #[arg(long, conflicts_with = "template")]
    pub body: Option<String>,

    /// Id of a stored template to render at delivery time.
    #[arg(long)]
    pub template: Option<String>,

    /// A template variable, as name=value. Repeat for each one.
    #[arg(long = "var", value_parser = parse::key_value)]
    pub vars: Vec<(String, String)>,

    #[arg(long)]
    pub max_attempts: Option<u32>,

    /// Hold the notification until this RFC 3339 instant.
    #[arg(long)]
    pub schedule: Option<String>,
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
