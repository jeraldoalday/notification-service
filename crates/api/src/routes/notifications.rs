use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use ns_auth::Scope;
use ns_core::{AttemptResult, ChannelKind, DeliveryAttempt, DeliveryStatus, Notification, Priority};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Caller;
use crate::error::{ApiError, ApiJson, ApiResult};
use crate::state::{AppState, Repositories};

/// Turns "this belongs to another tenant" into "this doesn't exist" for
/// lookups by id.
///
/// A 403 here would confirm that the id names a real notification, which
/// is exactly the thing another tenant shouldn't be able to learn -- and
/// unlike a `tenant_id` the caller supplied explicitly (where a 403 tells
/// a misconfigured client something useful about its own request), an id
/// that resolves to someone else's row is indistinguishable, from the
/// caller's side, from an id that resolves to nothing.
fn hide_other_tenants<T>(result: ApiResult<T>, missing: impl FnOnce() -> String) -> ApiResult<T> {
    match result {
        Err(ApiError::Auth(ns_auth::AuthError::WrongTenant { .. })) => Err(ApiError::NotFound(missing())),
        other => other,
    }
}

/// Applied when a `GET /notifications` request omits `limit`, so an
/// unbounded query can't be used to pull an entire tenant's history in one
/// request.
const DEFAULT_LIST_LIMIT: u32 = 50;

/// Ceiling on an explicitly requested `limit`. Without one, the default
/// above only protects callers who forget the parameter -- `?limit=1000000`
/// still asks the service to deserialize a tenant's entire history into
/// memory and serialize it into one response. Clamping rather than
/// rejecting keeps a caller paging with an over-large limit working, just
/// bounded.
const MAX_LIST_LIMIT: u32 = 500;

/// Attempts are per-notification and far more numerous than
/// notifications, so they get their own, larger, default: a caller asking
/// for one notification's history almost always wants all of it, and
/// `max_attempts` is single digits even before rate-limit skips are
/// counted.
const DEFAULT_ATTEMPT_LIMIT: u32 = 100;
const MAX_ATTEMPT_LIMIT: u32 = 1000;

#[derive(Debug, Deserialize)]
pub struct ListAttemptsQuery {
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ListTenantAttemptsQuery {
    pub tenant_id: Option<Uuid>,
    pub limit: Option<u32>,
}

/// One line of delivery history as the API reports it.
///
/// `tenant_id` is deliberately absent even though the stored record
/// carries one: every route reaching this is already scoped to a single
/// tenant, so echoing it back adds a field to every element of a
/// hundred-item list and tells the caller only what it already said.
#[derive(Debug, Serialize)]
pub struct DeliveryAttemptResponse {
    pub id: Uuid,
    pub notification_id: Uuid,
    pub channel: ChannelKind,
    pub attempt_number: u32,
    pub result: AttemptResult,
    /// True when this attempt actually reached a provider, and so says
    /// something about that provider rather than about this service.
    /// Derived rather than stored, so a client doesn't have to know which
    /// results imply it.
    pub reached_provider: bool,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
    pub occurred_at: DateTime<Utc>,
}

impl From<DeliveryAttempt> for DeliveryAttemptResponse {
    fn from(attempt: DeliveryAttempt) -> Self {
        Self {
            reached_provider: attempt.reached_provider(),
            id: attempt.id,
            notification_id: attempt.notification_id,
            channel: attempt.channel,
            attempt_number: attempt.attempt_number,
            result: attempt.result,
            error: attempt.error,
            duration_ms: attempt.duration_ms,
            occurred_at: attempt.occurred_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SubmitNotificationRequest {
    /// Optional when authenticated: the key already names the tenant, and
    /// supplying one that disagrees is refused rather than overridden.
    pub tenant_id: Option<Uuid>,
    pub recipient_id: Uuid,
    pub channel: ChannelKind,
    #[serde(default)]
    pub priority: Priority,
    pub subject: Option<String>,
    pub body: Option<String>,
    pub template_id: Option<String>,
    #[serde(default)]
    pub template_vars: HashMap<String, String>,
    pub max_attempts: Option<u32>,
    pub scheduled_for: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct NotificationResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub recipient_id: Uuid,
    pub channel: ChannelKind,
    pub priority: Priority,
    pub status: DeliveryStatus,
    pub subject: Option<String>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl From<Notification> for NotificationResponse {
    fn from(n: Notification) -> Self {
        Self {
            id: n.id,
            tenant_id: n.tenant_id,
            recipient_id: n.recipient_id,
            channel: n.channel,
            priority: n.priority,
            status: n.status,
            subject: n.subject,
            attempt_count: n.attempt_count,
            max_attempts: n.max_attempts,
            created_at: n.created_at,
            scheduled_for: n.scheduled_for,
            updated_at: n.updated_at,
        }
    }
}

/// Submits a notification for delivery.
///
/// Both up-front checks exist for the same reason: a notification that
/// can never be delivered should be rejected here, synchronously, rather
/// than being accepted, retried on backoff until its attempts run out and
/// dead-lettered minutes later where nobody is watching. So an unknown
/// recipient is a 404, and a `template_id` is resolved and test-rendered
/// against the supplied `template_vars` before the notification is
/// queued.
///
/// The rendered text is deliberately thrown away rather than stored on
/// the notification: `ns-worker` renders again at send time, so an edit to
/// the template still reaches everything queued behind it. This is purely
/// a validity check.
pub async fn submit_notification<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    ApiJson(request): ApiJson<SubmitNotificationRequest>,
) -> ApiResult<(StatusCode, Json<NotificationResponse>)>
where
    R: Repositories,
{
    caller.require(Scope::Write)?;
    let tenant_id = caller.tenant_for(request.tenant_id)?;

    // Fail fast with a 404 rather than letting a bad recipient_id surface
    // only once the worker tries (and fails) to dispatch the notification.
    // The tenant check rides along here: sending to someone else's
    // recipient is the one way a caller could otherwise reach an address
    // it was never given, and it answers 404 so that guessing recipient
    // ids tells an attacker nothing about which ones exist.
    let recipient = state.repo.get_recipient(request.recipient_id).await?;
    if caller.tenant_id().is_some() && !recipient.belongs_to(tenant_id) {
        return Err(ApiError::NotFound(format!("recipient {} not found", request.recipient_id)));
    }

    // Checked here as well as in the worker, and for the reason every
    // other up-front check in this handler exists: a notification that can
    // never be delivered should be refused while the caller is still on
    // the line. The worker's check is still the authoritative one, since
    // an address can be suppressed after this point and before the send.
    //
    // Only when the recipient actually has an address for this channel --
    // if it doesn't, that is a separate problem the worker already
    // reports, and inventing an address to test would answer the wrong
    // question.
    if let Ok(address) = recipient.address_for(request.channel) {
        if let Some(suppression) = state.repo.find_suppression(tenant_id, address).await? {
            return Err(ApiError::Suppressed { reason: suppression.reason });
        }
    }

    let mut builder =
        Notification::builder(tenant_id, request.recipient_id, request.channel).priority(request.priority);
    if let Some(subject) = request.subject {
        builder = builder.subject(subject);
    }
    if let Some(body) = request.body {
        builder = builder.body(body);
    }
    if let Some(template_id) = request.template_id {
        builder = builder.template(template_id);
    }
    for (key, value) in request.template_vars {
        builder = builder.template_var(key, value);
    }
    if let Some(max_attempts) = request.max_attempts {
        builder = builder.max_attempts(max_attempts);
    }
    if let Some(scheduled_for) = request.scheduled_for {
        builder = builder.scheduled_for(scheduled_for);
    }

    let notification = builder.build()?;

    if let Some(template_id) = &notification.template_id {
        let template = state.repo.get_template(notification.tenant_id, template_id).await?;
        template.render_for(&notification)?;
    }

    let submitted = state.queue.submit(notification, Utc::now()).await?;
    // Counted here rather than inside NotificationQueue::submit so the
    // number means "accepted from a caller", not "written to storage" --
    // the queue's own submit is also what replay uses to re-enqueue work
    // that was already counted once.
    state.metrics.record_submitted(submitted.channel);
    Ok((StatusCode::CREATED, Json(submitted.into())))
}

pub async fn get_notification<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<NotificationResponse>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let notification = state.repo.get_notification(id).await?;
    hide_other_tenants(caller.require_tenant(notification.tenant_id), || {
        format!("notification {id} not found")
    })?;
    Ok(Json(notification.into()))
}

/// Cancels a notification that hasn't started sending yet. `Notification`'s
/// own state machine (`DeliveryStatus::can_transition_to`) is what actually
/// decides whether that's legal -- only `Pending` and `Queued` can move to
/// `Cancelled`, so a notification already `Sending` or terminal returns a
/// 409 via `CoreError::InvalidTransition` rather than silently no-opping.
pub async fn cancel_notification<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<NotificationResponse>>
where
    R: Repositories,
{
    caller.require(Scope::Write)?;
    let mut notification = state.repo.get_notification(id).await?;
    hide_other_tenants(caller.require_tenant(notification.tenant_id), || {
        format!("notification {id} not found")
    })?;
    notification.transition(DeliveryStatus::Cancelled, Utc::now())?;
    state.repo.update_notification(&notification).await?;
    Ok(Json(notification.into()))
}

#[derive(Debug, Deserialize)]
pub struct ListNotificationsQuery {
    pub tenant_id: Option<Uuid>,
    pub status: Option<DeliveryStatus>,
    pub limit: Option<u32>,
}

pub async fn list_notifications<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<ListNotificationsQuery>,
) -> ApiResult<Json<Vec<NotificationResponse>>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT);
    let notifications = state.repo.list_by_tenant(tenant_id, query.status, limit).await?;
    Ok(Json(notifications.into_iter().map(NotificationResponse::from).collect()))
}

/// A notification's delivery history, oldest attempt first.
///
/// The listing this serves is the answer to "why is this notification
/// dead-lettered?", which the notification itself can't give: its row
/// holds a final status and an attempt count, and the interesting part --
/// which provider refused it, what it said, whether the attempts were
/// rejections or rate-limit skips -- only exists here.
///
/// It's a subresource of the notification rather than a filter on a
/// top-level collection because the tenant check is the notification's:
/// an attempt is only reachable through the thing it describes, so an id
/// belonging to someone else answers 404 in exactly the same way a
/// straight GET of that notification would.
pub async fn list_notification_attempts<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Path(id): Path<Uuid>,
    Query(query): Query<ListAttemptsQuery>,
) -> ApiResult<Json<Vec<DeliveryAttemptResponse>>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let notification = state.repo.get_notification(id).await?;
    hide_other_tenants(caller.require_tenant(notification.tenant_id), || {
        format!("notification {id} not found")
    })?;

    let limit = query.limit.unwrap_or(DEFAULT_ATTEMPT_LIMIT).min(MAX_ATTEMPT_LIMIT);
    let attempts = state.repo.list_attempts(id, limit).await?;
    Ok(Json(attempts.into_iter().map(DeliveryAttemptResponse::from).collect()))
}

/// A tenant's most recent attempts across all its notifications,
/// newest-first.
///
/// A different question from the one above, and the reason it gets its own
/// route: "what is failing right now" is answered by scanning recent
/// attempts, not by knowing which notification to ask about. The tenant
/// comes from the key, so this cannot reach across the boundary even
/// though it isn't scoped to a particular id.
pub async fn list_attempts<R>(
    State(state): State<AppState<R>>,
    caller: Caller,
    Query(query): Query<ListTenantAttemptsQuery>,
) -> ApiResult<Json<Vec<DeliveryAttemptResponse>>>
where
    R: Repositories,
{
    caller.require(Scope::Read)?;
    let tenant_id = caller.tenant_for(query.tenant_id)?;
    let limit = query.limit.unwrap_or(DEFAULT_ATTEMPT_LIMIT).min(MAX_ATTEMPT_LIMIT);
    let attempts = state.repo.list_tenant_attempts(tenant_id, limit).await?;
    Ok(Json(attempts.into_iter().map(DeliveryAttemptResponse::from).collect()))
}
