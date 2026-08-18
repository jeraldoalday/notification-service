use std::collections::HashMap;

use chrono::{DateTime, Utc};
use ns_core::{ChannelKind, DeliveryStatus, Notification, Priority};
use ns_queue::NotificationQueue;
use ns_storage::{NotificationRepository, RecipientRepository, SqliteStorage, TemplateRepository};
use uuid::Uuid;

use crate::error::{CliError, CliResult};
use crate::output::{table, OutputFormat};

/// Everything `notification submit` needs. Grouped into a struct rather
/// than passed as ten positional arguments so the call site reads the
/// same way the command line does.
#[derive(Debug)]
pub struct SubmitArgs {
    pub tenant_id: Uuid,
    pub recipient_id: Uuid,
    pub channel: ChannelKind,
    pub priority: Priority,
    pub subject: Option<String>,
    pub body: Option<String>,
    pub template_id: Option<String>,
    pub template_vars: HashMap<String, String>,
    pub max_attempts: Option<u32>,
    pub scheduled_for: Option<DateTime<Utc>>,
}

/// Submits a notification, applying the same up-front checks the HTTP API
/// does: an unknown recipient or template is reported now rather than
/// discovered by the worker several backoff delays later.
pub async fn submit(storage: &SqliteStorage, args: SubmitArgs, output: OutputFormat) -> CliResult<String> {
    storage.get_recipient(args.recipient_id).await?;

    let mut builder =
        Notification::builder(args.tenant_id, args.recipient_id, args.channel).priority(args.priority);
    if let Some(subject) = args.subject {
        builder = builder.subject(subject);
    }
    if let Some(body) = args.body {
        builder = builder.body(body);
    }
    if let Some(template_id) = args.template_id {
        builder = builder.template(template_id);
    }
    for (key, value) in args.template_vars {
        builder = builder.template_var(key, value);
    }
    if let Some(max_attempts) = args.max_attempts {
        builder = builder.max_attempts(max_attempts);
    }
    if let Some(scheduled_for) = args.scheduled_for {
        builder = builder.scheduled_for(scheduled_for);
    }

    let notification = builder.build()?;
    if let Some(template_id) = &notification.template_id {
        let template = storage.get_template(notification.tenant_id, template_id).await?;
        template.render_for(&notification)?;
    }

    let queue = NotificationQueue::new(storage.clone());
    let submitted = queue.submit(notification, Utc::now()).await?;
    output.render(&submitted, || describe(&submitted))
}

pub async fn show(storage: &SqliteStorage, id: Uuid, output: OutputFormat) -> CliResult<String> {
    let notification = storage.get_notification(id).await?;
    output.render(&notification, || describe(&notification))
}

pub async fn list(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    status: Option<DeliveryStatus>,
    limit: u32,
    output: OutputFormat,
) -> CliResult<String> {
    let notifications = storage.list_by_tenant(tenant_id, status, limit).await?;
    output.render(&notifications, || summary_table(&notifications))
}

/// The one-line-per-notification listing shared by `notification list` and
/// `queue due`, so the two can't drift into showing different columns for
/// the same rows.
pub fn summary_table(notifications: &[Notification]) -> String {
    let rows: Vec<Vec<String>> = notifications
        .iter()
        .map(|n| {
            vec![
                n.id.to_string(),
                n.channel.to_string(),
                n.status.to_string(),
                format!("{}/{}", n.attempt_count, n.max_attempts),
                n.created_at.to_rfc3339(),
            ]
        })
        .collect();
    table(&["id", "channel", "status", "attempts", "created"], &rows)
}

/// Cancels a notification that hasn't started sending. The refusal comes
/// from `DeliveryStatus`'s own state machine rather than a check here, so
/// the CLI can't cancel something the API wouldn't.
pub async fn cancel(storage: &SqliteStorage, id: Uuid, output: OutputFormat) -> CliResult<String> {
    let mut notification = storage.get_notification(id).await?;
    notification.transition(DeliveryStatus::Cancelled, Utc::now())?;
    storage.update_notification(&notification).await?;
    output.render(&notification, || describe(&notification))
}

/// Replays one dead-lettered notification by id, queueing a fresh copy.
pub async fn replay(storage: &SqliteStorage, id: Uuid, output: OutputFormat) -> CliResult<String> {
    let notification = storage.get_notification(id).await?;
    let queue = NotificationQueue::new(storage.clone());
    let replayed = queue.replay(&notification, Utc::now()).await?;
    output.render(&replayed, || describe(&replayed))
}

/// One field per line, `key: value` -- the shape that survives being
/// grepped, which is what an operator does with it.
fn describe(n: &Notification) -> String {
    let mut out = String::new();
    let mut field = |key: &str, value: String| {
        out.push_str(key);
        out.push_str(": ");
        out.push_str(&value);
        out.push('\n');
    };

    field("id", n.id.to_string());
    field("tenant", n.tenant_id.to_string());
    field("recipient", n.recipient_id.to_string());
    field("channel", n.channel.to_string());
    field("status", n.status.to_string());
    field("priority", format!("{:?}", n.priority).to_lowercase());
    field("attempts", format!("{}/{}", n.attempt_count, n.max_attempts));
    if let Some(subject) = &n.subject {
        field("subject", subject.clone());
    }
    if let Some(template_id) = &n.template_id {
        field("template", template_id.clone());
    }
    if let Some(scheduled_for) = n.scheduled_for {
        field("scheduled", scheduled_for.to_rfc3339());
    }
    field("created", n.created_at.to_rfc3339());
    field("updated", n.updated_at.to_rfc3339());
    if !n.body.is_empty() {
        field("body", n.body.clone());
    }
    out
}

/// Parses an RFC 3339 timestamp for `--schedule`, reporting the expected
/// shape rather than chrono's parser internals.
pub fn parse_timestamp(raw: &str) -> CliResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw).map(|dt| dt.with_timezone(&Utc)).map_err(|_| {
        CliError::Invalid(format!("`{raw}` is not an RFC 3339 timestamp, e.g. 2026-01-31T09:00:00Z"))
    })
}
