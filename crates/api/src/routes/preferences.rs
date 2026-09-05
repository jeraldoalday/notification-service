use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_auth::Scope;
use ns_core::{ChannelKind, Preference, PreferenceSetting, Topic};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Caller;
use crate::error::{ApiError, ApiJson, ApiResult};
use crate::state::{AppState, Repositories};

const DEFAULT_LIST_LIMIT: u32 = 100;

/// A recipient can hold at most one preference per (topic, channel), so
/// this bounds how many topics one response can describe rather than
/// bounding anything unbounded. A preference centre wants all of them in
/// one page; five hundred topics is more than any tenant has.
const MAX_LIST_LIMIT: u32 = 500;

#[derive(Debug, Deserialize)]
pub struct ListPreferencesQuery {
    pub tenant_id: Option<Uuid>,
    pub recipient_id: Uuid,
    pub limit: Option<u32>,
}

/// Identifies one preference. Like a suppression's address, it rides in
/// the query string rather than the path: three fields, one of them
/// optional, do not make a path.
#[derive(Debug, Deserialize)]
pub struct PreferenceQuery {
    pub tenant_id: Option<Uuid>,
    pub recipient_id: Uuid,
    pub topic: Topic,
    /// Omitted means the preference covering every channel -- the same
    /// wildcard `SetPreferenceRequest` writes, so a caller deletes an
    /// answer using exactly the fields it set it with.
    pub channel: Option<ChannelKind>,
}

#[derive(Debug, Deserialize)]
pub struct SetPreferenceRequest {
    pub tenant_id: Option<Uuid>,
    pub recipient_id: Uuid,
    pub topic: Topic,
    /// Omitted covers every channel, including channels this deployment
    /// hasn't added yet. That is the point: somebody who says "nothing
    /// about marketing" today means it about whatever the service learns
    /// to send later.
    pub channel: Option<ChannelKind>,
    pub setting: PreferenceSetting,
}

#[derive(Debug, Serialize)]
pub struct PreferenceResponse {
    pub recipient_id: Uuid,
    pub topic: Topic,
    /// Absent for a preference covering every channel, mirroring the
    /// request that created it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelKind>,
    pub setting: PreferenceSetting,
    pub updated_at: DateTime<Utc>,
}

impl From<Preference> for PreferenceResponse {
    fn from(preference: Preference) -> Self {
        Self {
            recipient_id: preference.recipient_id,
            topic: preference.topic,
            channel: preference.channel,
            setting: preference.setting,
            updated_at: preference.updated_at,
        }
    }
}

/// What the service would do with one topic on one channel, and why.
#[derive(Debug, Serialize)]
pub struct ResolvedPreferenceResponse {
    pub recipient_id: Uuid,
    pub topic: Topic,
    pub channel: ChannelKind,
    pub setting: PreferenceSetting,
    /// The plain answer, so a caller rendering a checkbox doesn't have to
    /// know which settings count as permission.
    pub allowed: bool,
    /// The preference that decided it, or `null` when the recipient has
    /// said nothing and the default applied. Included because "allowed"
    /// alone can't distinguish an explicit yes from silence, and a
    /// preference centre wants to show which of the two it is.
    pub deciding: Option<PreferenceResponse>,
}

/// Records a recipient's answer about one topic.
///
/// `PUT` rather than `POST`, and idempotent: the resource is named by
/// (recipient, topic, channel) rather than by a server-minted id, and
/// sending the same answer twice has to be safe. An unsubscribe link
/// followed twice, or a preference centre saving a form that didn't
/// change, must not be an error.
///
/// `Write` rather than `Admin`, matching suppressions: this changes what
/// the service will send, which is exactly what `write` describes, and
/// requiring `admin` would mean an unsubscribe handler needed a
/// credential that could also mint API keys.
pub async fn set_preference<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    ApiJson(request): ApiJson<SetPreferenceRequest>,
) -> ApiResult<Json<PreferenceResponse>>
where
    R: Repositories,
{
    caller.require(Scope::Write)?;
    let tenant_id = caller.tenant_for(request.tenant_id)?;

    // A preference for a recipient that doesn't exist is a row that can
    // never match a send, so the caller has almost certainly got the id
    // wrong and should hear about it now. 404 for another tenant's
    // recipient rather than 403, for the reason every id lookup here does
    // it: confirming the id names something real is the thing a stranger
    // shouldn't be able to learn.
    let recipient = state.repo.get_recipient(request.recipient_id).await?;
    if caller.tenant_id().is_some() && !recipient.belongs_to(tenant_id) {
        return Err(ApiError::NotFound(format!("recipient {} not found", request.recipient_id)));
    }

    let preference = Preference::new(
        tenant_id,
        request.recipient_id,
        request.topic,
        request.channel,
        request.setting,
        Utc::now(),
    );
    state.repo.set_preference(&preference).await?;
    Ok(Json(preference.into()))
}

/// Everything one recipient has said, most recently changed first.
///
/// This is what a preference centre renders. It reports only what the
/// recipient has actually expressed -- topics they have never had an
/// opinion about are absent rather than listed as allowed, because the
/// service has no list of every topic a tenant might ever send under.
pub async fn list_preferences<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<ListPreferencesQuery>,
) -> ApiResult<Json<Vec<PreferenceResponse>>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT);

    let preferences = state.repo.list_preferences(tenant_id, query.recipient_id, limit).await?;
    Ok(Json(preferences.into_iter().map(PreferenceResponse::from).collect()))
}

/// Answers what would happen to one topic on one channel.
///
/// Worth its own route rather than leaving callers to apply the
/// precedence rule to the listing themselves. A wildcard block and a
/// channel exception only mean something read as a pair, and a client
/// that reimplemented that resolution would eventually disagree with the
/// worker -- which shows up as a message sent to somebody who opted out,
/// not as an error anyone sees.
pub async fn resolve_preference<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<ResolveQuery>,
) -> ApiResult<Json<ResolvedPreferenceResponse>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;

    let preferences = state.repo.preferences_for(tenant_id, query.recipient_id, &query.topic).await?;
    Ok(Json(ResolvedPreferenceResponse {
        recipient_id: query.recipient_id,
        setting: preferences.setting_for(query.channel),
        allowed: preferences.allows(query.channel),
        deciding: preferences.deciding(query.channel).cloned().map(PreferenceResponse::from),
        topic: query.topic,
        channel: query.channel,
    }))
}

/// Resolution needs a concrete channel -- "would this be sent?" is not a
/// question about every channel at once -- so unlike the other routes
/// here the channel is required rather than defaulting to the wildcard.
#[derive(Debug, Deserialize)]
pub struct ResolveQuery {
    pub tenant_id: Option<Uuid>,
    pub recipient_id: Uuid,
    pub topic: Topic,
    pub channel: ChannelKind,
}

/// Forgets one recorded answer, returning the recipient to having said
/// nothing about it.
///
/// 404 when there was nothing to forget, matching the suppression
/// delete. Deleting a preference can re-enable sending to somebody who
/// asked not to be sent to, and a caller that got the topic slightly
/// wrong should find that out rather than be told it worked.
///
/// Note this is not the same as setting `allowed`: that records a
/// decision the recipient made, which is what carves an exception out of
/// a broader block. Deleting removes the decision entirely.
pub async fn delete_preference<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<PreferenceQuery>,
) -> ApiResult<StatusCode>
where
    R: Repositories,
{
    caller.require(Scope::Write)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;

    if state.repo.delete_preference(tenant_id, query.recipient_id, &query.topic, query.channel).await? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!(
            "recipient {} has no {} preference for topic `{}`",
            query.recipient_id,
            query.channel.map(|c| c.to_string()).unwrap_or_else(|| "all-channel".into()),
            query.topic
        )))
    }
}
