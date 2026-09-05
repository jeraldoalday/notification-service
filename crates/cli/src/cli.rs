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

    /// Manage the addresses a tenant may no longer send to.
    #[command(subcommand)]
    Suppression(SuppressionCommand),

    /// Manage what each recipient has asked to be sent, per topic.
    ///
    /// Distinct from `suppression`, which is about an address being
    /// undeliverable. This is about a person's choice: keep the receipts,
    /// stop the newsletter.
    #[command(subcommand)]
    Preference(PreferenceCommand),

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
pub enum PreferenceCommand {
    /// Record a recipient's answer about one topic. Re-running with a
    /// different setting replaces the answer rather than failing.
    Set {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long)]
        recipient: Uuid,

        #[arg(long, value_parser = parse::topic)]
        topic: ns_core::Topic,

        /// Narrow the answer to one channel. Omit it to cover every
        /// channel, including ones this deployment adds later.
        #[arg(long, value_parser = parse::channel)]
        channel: Option<ChannelKind>,

        #[arg(long, value_parser = parse::preference_setting)]
        setting: ns_core::PreferenceSetting,
    },

    /// List everything one recipient has said, most recent first.
    List {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long)]
        recipient: Uuid,

        #[arg(long, default_value_t = 50)]
        limit: u32,
    },

    /// Answer whether one topic would be sent on one channel, and say
    /// which preference decided it.
    Check {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long)]
        recipient: Uuid,

        #[arg(long, value_parser = parse::topic)]
        topic: ns_core::Topic,

        #[arg(long, value_parser = parse::channel)]
        channel: ChannelKind,
    },

    /// Forget one recorded answer, returning the recipient to having said
    /// nothing about it.
    ///
    /// Not the same as setting it to `allowed`: that records a decision,
    /// which is what carves an exception out of a broader block.
    Clear {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long)]
        recipient: Uuid,

        #[arg(long, value_parser = parse::topic)]
        topic: ns_core::Topic,

        #[arg(long, value_parser = parse::channel)]
        channel: Option<ChannelKind>,
    },
}

#[derive(Debug, Subcommand)]
pub enum SuppressionCommand {
    /// Suppress an address. Re-running updates the reason rather than
    /// failing, so working through a list of bounces is idempotent.
    Add {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, value_parser = parse::channel)]
        channel: ChannelKind,

        /// The address itself: an email, an E.164 number, a push token or
        /// a webhook URL, matching --channel.
        address: String,

        #[arg(long, value_parser = parse::suppression_reason, default_value = "manual")]
        reason: ns_core::SuppressionReason,

        /// Free text to remember why -- a bounce code, a ticket id.
        #[arg(long)]
        note: Option<String>,
    },

    /// List a tenant's suppressed addresses, newest first.
    List {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, default_value_t = 100)]
        limit: u32,
    },

    /// Ask whether one address is suppressed.
    Check {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, value_parser = parse::channel)]
        channel: ChannelKind,

        address: String,
    },

    /// Lift a suppression, so the address can be sent to again.
    ///
    /// Unlike `add`, an address that wasn't suppressed is an error: this
    /// re-enables sending to someone who asked not to be sent to, so a
    /// typo should not look like a success.
    Remove {
        #[arg(long)]
        tenant: Uuid,

        #[arg(long, value_parser = parse::channel)]
        channel: ChannelKind,

        address: String,
    },
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

    /// Show the notification a tenant submitted under an idempotency key.
    ///
    /// The lookup a client makes when it never saw the response to its
    /// own submission -- and the one an operator makes to answer "did
    /// that order's email actually go out?" from the order id alone.
    ByKey {
        #[arg(long)]
        tenant: Uuid,

        key: String,
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

    /// What this message is about, so the recipient's preferences for
    /// that topic apply to it. Omit it and the message goes out whatever
    /// they have opted out of.
    #[arg(long, value_parser = parse::topic)]
    pub topic: Option<ns_core::Topic>,

    /// Name this submission, so re-running the same command resolves to
    /// the notification the first run created instead of sending a
    /// second message.
    #[arg(long, value_parser = parse::idempotency_key)]
    pub idempotency_key: Option<ns_core::IdempotencyKey>,

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
