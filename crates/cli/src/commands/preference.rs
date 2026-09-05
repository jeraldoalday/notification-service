use chrono::Utc;
use ns_core::{ChannelKind, Preference, PreferenceSetting, Topic};
use ns_storage::{PreferenceRepository, RecipientRepository, SqliteStorage};
use uuid::Uuid;

use crate::error::{CliError, CliResult};
use crate::output::{table, OutputFormat};

/// Records a recipient's answer about one topic.
///
/// Re-running with a different setting replaces the answer rather than
/// failing, matching the HTTP endpoint: somebody changing their mind is
/// the ordinary case, and an operator working through a list of
/// unsubscribe requests shouldn't have to remember which ones they
/// already did.
pub async fn set(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    recipient_id: Uuid,
    topic: Topic,
    channel: Option<ChannelKind>,
    setting: PreferenceSetting,
    output: OutputFormat,
) -> CliResult<String> {
    // The recipient has to exist and belong to this tenant, for the same
    // reason `notification submit` checks: a preference keyed by a
    // recipient id that names nobody -- or names somebody else's
    // recipient -- is a row that can never match a send, and it would sit
    // there looking like the opt-out was recorded.
    //
    // Reported plainly rather than as a "no such recipient", because the
    // audience is an operator on the host rather than a stranger holding
    // an id: telling them it exists but belongs elsewhere is what lets
    // them fix the command.
    let recipient = storage.get_recipient(recipient_id).await?;
    if !recipient.belongs_to(tenant_id) {
        return Err(CliError::Invalid(format!(
            "recipient {} belongs to tenant {}, not {}",
            recipient.id, recipient.tenant_id, tenant_id
        )));
    }

    let preference = Preference::new(tenant_id, recipient_id, topic, channel, setting, Utc::now());
    storage.set_preference(&preference).await?;
    output.render(&preference, || describe(&preference))
}

/// Everything one recipient has said, most recently changed first.
pub async fn list(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    recipient_id: Uuid,
    limit: u32,
    output: OutputFormat,
) -> CliResult<String> {
    let preferences = storage.list_preferences(tenant_id, recipient_id, limit).await?;
    output.render(&preferences, || summary_table(&preferences))
}

/// Answers what would happen to one topic on one channel, and says which
/// preference decided it.
///
/// The "why is this not going out?" command. Resolution runs through the
/// same `PreferenceSet` the worker uses, so this cannot disagree with
/// what the delivery path would actually do -- which is the whole reason
/// it isn't reimplemented here over the listing.
pub async fn check(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    recipient_id: Uuid,
    topic: Topic,
    channel: ChannelKind,
    output: OutputFormat,
) -> CliResult<String> {
    let preferences = storage.preferences_for(tenant_id, recipient_id, &topic).await?;
    let deciding = preferences.deciding(channel);
    let allowed = preferences.allows(channel);

    let rendered = serde_json::json!({
        "recipient_id": recipient_id,
        "topic": topic,
        "channel": channel,
        "setting": preferences.setting_for(channel),
        "allowed": allowed,
        "deciding": deciding,
    });

    output.render(&rendered, || {
        let verdict = if allowed { "would be sent" } else { "would not be sent" };
        let because = match deciding {
            Some(preference) => format!(
                "because of the {} preference for `{}`",
                preference.channel.map(|c| c.to_string()).unwrap_or_else(|| "all-channel".into()),
                preference.topic
            ),
            // Distinguished from an explicit `allowed`, because the two
            // call for different follow-ups: silence can be changed by
            // anyone, an explicit answer is the recipient's own.
            None => "because the recipient has expressed no preference".to_string(),
        };
        format!("{channel} `{topic}` {verdict} to {recipient_id}, {because}\n")
    })
}

/// Forgets one recorded answer.
///
/// Unlike `set`, a miss is an error. Clearing a preference can re-enable
/// sending to somebody who asked not to be sent to, and an operator who
/// mistyped the topic should be told rather than shown a success.
pub async fn clear(
    storage: &SqliteStorage,
    tenant_id: Uuid,
    recipient_id: Uuid,
    topic: Topic,
    channel: Option<ChannelKind>,
    output: OutputFormat,
) -> CliResult<String> {
    if !storage.delete_preference(tenant_id, recipient_id, &topic, channel).await? {
        return Err(CliError::Invalid(format!(
            "recipient {recipient_id} has no {} preference for topic `{topic}`",
            channel_label(channel)
        )));
    }

    let cleared = serde_json::json!({
        "cleared": true,
        "tenant_id": tenant_id,
        "recipient_id": recipient_id,
        "topic": topic,
        "channel": channel,
    });
    output.render(&cleared, || format!("cleared the {} preference for `{topic}`\n", channel_label(channel)))
}

/// How an optional channel reads in a sentence. The stored spelling is
/// `*`, which is right for a key and wrong for a person.
fn channel_label(channel: Option<ChannelKind>) -> String {
    channel.map(|c| c.to_string()).unwrap_or_else(|| "all-channel".into())
}

fn summary_table(preferences: &[Preference]) -> String {
    if preferences.is_empty() {
        return "no preferences recorded\n".to_string();
    }

    let rows: Vec<Vec<String>> = preferences
        .iter()
        .map(|preference| {
            vec![
                preference.topic.to_string(),
                channel_label(preference.channel),
                preference.setting.to_string(),
                preference.updated_at.to_rfc3339(),
            ]
        })
        .collect();
    table(&["topic", "channel", "setting", "updated"], &rows)
}

fn describe(preference: &Preference) -> String {
    let mut out = String::new();
    let mut field = |key: &str, value: String| {
        out.push_str(key);
        out.push_str(": ");
        out.push_str(&value);
        out.push('\n');
    };

    field("tenant", preference.tenant_id.to_string());
    field("recipient", preference.recipient_id.to_string());
    field("topic", preference.topic.to_string());
    field("channel", channel_label(preference.channel));
    field("setting", preference.setting.to_string());
    field("updated", preference.updated_at.to_rfc3339());
    out
}
