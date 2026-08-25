use chrono::Utc;
use ns_core::{ChannelKind, RecipientAddress, Suppression, SuppressionReason};
use ns_storage::{SqliteStorage, SuppressionRepository};
use uuid::Uuid;

use crate::error::{CliError, CliResult};
use crate::output::{table, OutputFormat};

/// Adds an address to a tenant's suppression list.
///
/// Re-running this on an already-suppressed address succeeds and updates
/// the reason, matching the HTTP endpoint: an operator working through a
/// list of bounces shouldn't have to remember which ones they already did.
pub async fn add(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    channel: ChannelKind,
    address: String,
    reason: SuppressionReason,
    note: Option<String>,
    output: OutputFormat,
) -> CliResult<String> {
    let address = RecipientAddress::for_channel(channel, address);
    let mut suppression = Suppression::new(tenant_id, address, reason, Utc::now())?;
    if let Some(note) = note {
        suppression = suppression.with_note(note);
    }

    storage.insert_suppression(&suppression).await?;
    output.render(&suppression, || describe(&suppression))
}

pub async fn list(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    limit: u32,
    output: OutputFormat,
) -> CliResult<String> {
    let suppressions = storage.list_suppressions(tenant_id, limit).await?;
    output.render(&suppressions, || summary_table(&suppressions))
}

/// Answers whether one address is suppressed, without listing the rest.
pub async fn check(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    channel: ChannelKind,
    address: String,
    output: OutputFormat,
) -> CliResult<String> {
    let address = RecipientAddress::for_channel(channel, address);
    match storage.find_suppression(tenant_id, &address).await? {
        Some(suppression) => output.render(&suppression, || describe(&suppression)),
        // Not an error: "this address is fine to send to" is a perfectly
        // good answer to the question, and exiting non-zero for it would
        // make the command useless in a shell conditional.
        None => output.render(&serde_json::Value::Null, || {
            format!("{} address `{}` is not suppressed\n", channel, address.value())
        }),
    }
}

/// Lifts a suppression, so the address can be sent to again.
///
/// Unlike `add`, a miss is an error. This re-enables sending to someone
/// who at some point asked not to be sent to, and an operator who
/// mistyped the address should be told rather than shown a success.
pub async fn remove(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    channel: ChannelKind,
    address: String,
    output: OutputFormat,
) -> CliResult<String> {
    let address = RecipientAddress::for_channel(channel, address);
    if !storage.delete_suppression(tenant_id, &address).await? {
        return Err(CliError::Invalid(format!(
            "{channel} address `{}` is not suppressed for tenant {tenant_id}",
            address.value()
        )));
    }

    let removed = serde_json::json!({
        "removed": true,
        "channel": channel,
        "address": address.value(),
        "tenant_id": tenant_id,
    });
    output.render(&removed, || format!("lifted suppression on {channel} address `{}`\n", address.value()))
}

fn summary_table(suppressions: &[Suppression]) -> String {
    if suppressions.is_empty() {
        return "no suppressed addresses\n".to_string();
    }

    let rows: Vec<Vec<String>> = suppressions
        .iter()
        .map(|s| {
            vec![
                s.channel().to_string(),
                s.address.value().to_string(),
                s.reason.to_string(),
                s.created_at.to_rfc3339(),
                s.note.clone().unwrap_or_default(),
            ]
        })
        .collect();
    table(&["channel", "address", "reason", "since", "note"], &rows)
}

fn describe(s: &Suppression) -> String {
    let mut out = String::new();
    let mut field = |key: &str, value: String| {
        out.push_str(key);
        out.push_str(": ");
        out.push_str(&value);
        out.push('\n');
    };

    field("tenant", s.tenant_id.to_string());
    field("channel", s.channel().to_string());
    field("address", s.address.value().to_string());
    field("reason", s.reason.to_string());
    field("since", s.created_at.to_rfc3339());
    if let Some(note) = &s.note {
        field("note", note.clone());
    }
    out
}
