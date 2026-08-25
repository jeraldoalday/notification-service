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

    /// Register and inspect recipients.
    #[command(subcommand)]
    Recipient(RecipientCommand),

    /// Manage reusable notification templates.
    #[command(subcommand)]
    Template(TemplateCommand),

    /// Inspect and operate on the delivery queue.
    #[command(subcommand)]
    Queue(QueueCommand),

    /// Mint, list and revoke the API keys the HTTP API authenticates.
    ///
    /// This lives in the CLI rather than only behind an admin-scoped
    /// endpoint because a deployment's *first* key has to come from
    /// somewhere, and an API that can only be used by someone who already
    /// holds a key has no way to issue one.
    #[command(subcommand)]
    Apikey(ApikeyCommand),
}

#[derive(Debug, Subcommand)]
pub enum ApikeyCommand {
    /// Mint a key. Its token is printed once and cannot be recovered.
    Create {
        #[arg(long)]
        tenant: Uuid,

        /// A label to recognise this key by later, e.g. ci-deploy.
        #[arg(long)]
        name: String,

        /// A scope to grant. Repeat for each one: read, write, admin.
        #[arg(long = "scope", value_parser = parse::scope)]
        scopes: Vec<ns_auth::Scope>,
    },

    /// List a tenant's keys, newest first, including revoked ones.
    List {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, default_value_t = 50)]
        limit: u32,
    },

    /// Revoke a key, so it stops authenticating immediately.
    Revoke { id: Uuid },
}

#[derive(Debug, Subcommand)]
pub enum RecipientCommand {
    /// Register a recipient and the addresses it can be reached on.
    Add {
        /// The tenant this recipient belongs to. Only that tenant's keys
        /// can send to it.
        #[arg(long)]
        tenant: Uuid,

        #[arg(long)]
        display_name: Option<String>,

        #[arg(long)]
        email: Option<String>,

        /// E.164 phone number, e.g. +15555550100.
        #[arg(long)]
        phone: Option<String>,

        #[arg(long)]
        push_token: Option<String>,

        #[arg(long)]
        webhook_url: Option<String>,
    },

    /// Show one recipient.
    Show { id: Uuid },
}

#[derive(Debug, Subcommand)]
pub enum TemplateCommand {
    /// Store a new template.
    Create(TemplateSaveArgs),

    /// Replace an existing template's content.
    Update(TemplateSaveArgs),

    /// Show one template in full.
    Show {
        #[arg(long)]
        tenant: Uuid,
        id: String,
    },

    /// List a tenant's templates.
    List {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, default_value_t = 50)]
        limit: u32,
    },

    /// Delete a template.
    Delete {
        #[arg(long)]
        tenant: Uuid,
        id: String,
    },

    /// Render a template locally, without sending anything.
    Render {
        #[arg(long)]
        tenant: Uuid,
        id: String,

        /// A template variable, as name=value. Repeat for each one.
        #[arg(long = "var", value_parser = parse::key_value)]
        vars: Vec<(String, String)>,
    },
}

#[derive(Debug, Args)]
pub struct TemplateSaveArgs {
    #[arg(long)]
    pub tenant: Uuid,

    /// The template's slug, e.g. welcome_email. Referenced by
    /// `notification submit --template`.
    pub id: String,

    #[arg(long)]
    pub name: Option<String>,

    /// Restrict this template to one channel.
    #[arg(long, value_parser = parse::channel)]
    pub channel: Option<ChannelKind>,

    #[arg(long)]
    pub subject: Option<String>,

    #[arg(long, conflicts_with = "body_file")]
    pub body: Option<String>,

    /// Read the body from a file, which is the practical option for
    /// anything multi-line.
    #[arg(long)]
    pub body_file: Option<std::path::PathBuf>,
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

    /// Show one notification's delivery history, oldest attempt first.
    Attempts {
        id: Uuid,

        #[arg(long, default_value_t = 100)]
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

    /// Show the work the next worker poll would claim.
    Due {
        #[arg(long, default_value_t = 25)]
        limit: u32,
    },

    /// Re-queue a tenant's dead-lettered notifications as fresh copies.
    Replay {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, default_value_t = 25)]
        limit: u32,
    },

    /// Delete delivery attempts older than a cutoff.
    ///
    /// The attempt log is the one table that grows without bound, so this
    /// is the retention job -- run it from cron. Notifications themselves
    /// are never touched.
    PurgeAttempts {
        #[arg(long, default_value_t = 30)]
        older_than_days: i64,
    },
}
